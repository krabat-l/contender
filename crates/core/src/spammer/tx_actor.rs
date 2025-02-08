use std::error::Error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use web3::{
    Web3,
    transports::WebSocket,
    types::{FilterBuilder, Log, H256},
    futures::StreamExt,
};
use serde::{Deserialize, Serialize};
use alloy::primitives::TxHash;

use crate::{
    db::{DbOps, RunTx},
    error::ContenderError,
};
use web3::api::SubscriptionStream;
use web3::types::{BlockHeader, BlockId};

#[derive(Debug, Deserialize)]
struct FragmentResponse {
    payload_id: String,
    block_number: u64,
    index: u64,
    tx_offset: u64,
    log_offset: u64,
    gas_offset: u64,
    timestamp: u64,
    gas_used: u64,
    transactions: Vec<TransactionSigned>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct TransactionSigned {
    pub hash: TxHash,
}

enum TxActorMessage {
    SentRunTx {
        tx_hash: TxHash,
        start_timestamp: usize,
        kind: Option<String>,
        on_receipt: oneshot::Sender<()>,
    },
    FlushCache {
        run_id: u64,
        on_flush: oneshot::Sender<usize>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingRunTx {
    tx_hash: TxHash,
    start_timestamp: usize,
    kind: Option<String>,
}

impl PendingRunTx {
    pub fn new(tx_hash: TxHash, start_timestamp: usize, kind: Option<&str>) -> Self {
        Self {
            tx_hash,
            start_timestamp,
            kind: kind.map(|s| s.to_owned()),
        }
    }
}

struct TxActor<D> where D: DbOps {
    receiver: mpsc::Receiver<TxActorMessage>,
    db: Arc<D>,
    cache: Vec<PendingRunTx>,
    web3: Web3<WebSocket>,
}

impl<D> TxActor<D> where D: DbOps + Send + Sync + 'static {
    pub async fn new(
        receiver: mpsc::Receiver<TxActorMessage>,
        db: Arc<D>,
        ws_url: String,
    ) -> Result<Self, web3::Error> {
        let transport = WebSocket::new(&ws_url).await?;
        let web3 = Web3::new(transport);

        Ok(Self {
            receiver,
            db,
            cache: Vec::new(),
            web3,
        })
    }

    async fn subscribe_to_new_blocks(&self) -> Result<SubscriptionStream<WebSocket, BlockHeader>, Box<dyn Error>> {
        let filter = FilterBuilder::default()
            .build();

        let subscription = self.web3.eth_subscribe()
            .subscribe_new_heads()
            .await?;

        Ok(subscription)
    }

    async fn handle_message(
        &mut self,
        message: TxActorMessage,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match message {
            TxActorMessage::SentRunTx {
                tx_hash,
                start_timestamp,
                kind,
                on_receipt,
            } => {
                let run_tx = PendingRunTx {
                    tx_hash,
                    start_timestamp,
                    kind,
                };
                self.cache.push(run_tx);
                on_receipt.send(()).map_err(|_| {
                    ContenderError::SpamError("failed to join TxActor callback", None)
                })?;
            }
            TxActorMessage::FlushCache { run_id, on_flush } => {
                println!("unconfirmed txs: {}", self.cache.len());

                if self.cache.is_empty() {
                    on_flush.send(0).map_err(|_| {
                        ContenderError::SpamError("failed to join TxActor on_flush", None)
                    })?;
                    return Ok(());
                }

                let mut subscription = self.web3.eth_subscribe()
                    .subscribe_new_heads()
                    .await?;

                while !self.cache.is_empty() {
                    if let Some(block) = subscription.next().await {
                        let block = block?;

                        // Get full block details
                        let block_with_txs = self.web3.eth()
                            .block_with_txs(BlockId::from(block.hash.unwrap()))
                            .await?
                            .unwrap();

                        let confirmed_tx_hashes: Vec<TxHash> = block_with_txs.transactions
                            .iter()
                            .map(|tx| TxHash::from_slice(&tx.hash.0))
                            .collect();

                        let confirmed_txs: Vec<PendingRunTx> = self.cache
                            .iter()
                            .filter(|tx| confirmed_tx_hashes.contains(&tx.tx_hash))
                            .cloned()
                            .collect();

                        self.cache.retain(|tx| !confirmed_tx_hashes.contains(&tx.tx_hash));

                        let run_txs = confirmed_txs.clone().into_iter()
                            .map(|pending_tx| {
                                RunTx {
                                    tx_hash: pending_tx.tx_hash,
                                    start_timestamp: pending_tx.start_timestamp / 1000,
                                    end_timestamp: block_with_txs.timestamp.as_u64() as usize,
                                    block_number: block_with_txs.number.unwrap().as_u64(),
                                    gas_used: block_with_txs.gas_used.as_u128(),
                                    kind: pending_tx.kind,
                                }
                            })
                            .collect::<Vec<_>>();

                        if !run_txs.is_empty() {
                            self.db.insert_run_txs(run_id, run_txs)?;

                            for tx in &confirmed_txs {
                                println!(
                                    "tx landed. hash={}\tblock_num={}",
                                    tx.tx_hash,
                                    block_with_txs.number.unwrap()
                                );
                            }
                        }
                    }

                    if self.cache.is_empty() {
                        break;
                    }
                }

                subscription.unsubscribe().await?;

                on_flush.send(self.cache.len()).map_err(|_| {
                    ContenderError::SpamError("failed to join TxActor on_flush", None)
                })?;
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TxActorHandle {
    sender: mpsc::Sender<TxActorMessage>,
}

impl TxActorHandle {
    pub async fn new<D: DbOps + Send + Sync + 'static>(
        bufsize: usize,
        db: Arc<D>,
        ws_url: String,
    ) -> Result<Self, Box<dyn Error>> {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(receiver, db, ws_url).await?;
        tokio::task::spawn(async move {
            actor.run().await.expect("tx actor crashed");
        });
        Ok(Self { sender })
    }

    pub async fn cache_run_tx(
        &self,
        tx_hash: TxHash,
        start_timestamp: usize,
        kind: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(TxActorMessage::SentRunTx {
                tx_hash,
                start_timestamp,
                kind,
                on_receipt: sender,
            })
            .await?;
        receiver.await?;
        Ok(())
    }

    pub async fn flush_cache(
        &self,
        run_id: u64,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(TxActorMessage::FlushCache {
                run_id,
                on_flush: sender,
            })
            .await?;
        Ok(receiver.await?)
    }
}