use std::{sync::Arc, time::Duration};

use alloy::{network::ReceiptResponse, primitives::TxHash, providers::Provider};
use alloy::pubsub::Subscription;
use futures::stream::Stream;
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::value::RawValue;
use tokio::sync::{mpsc, oneshot};

use crate::{
    db::{DbOps, RunTx},
    error::ContenderError,
    generator::types::AnyProvider,
};

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

#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FragmentResponse {
    id: u64,
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
    /// Transaction hash
    pub hash: TxHash,
}

struct TxActor<D>
where
    D: DbOps,
{
    receiver: mpsc::Receiver<TxActorMessage>,
    db: Arc<D>,
    cache: Vec<PendingRunTx>,
    rpc: Arc<AnyProvider>,
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

impl<D> TxActor<D>
where
    D: DbOps + Send + Sync + 'static,
{
    pub fn new(
        receiver: mpsc::Receiver<TxActorMessage>,
        db: Arc<D>,
        rpc: Arc<AnyProvider>,
    ) -> Self {
        Self {
            receiver,
            db,
            cache: Vec::new(),
            rpc,
        }
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
                self.cache.push(run_tx.to_owned());
                on_receipt.send(()).map_err(|_| {
                    ContenderError::SpamError("failed to join TxActor callback", None)
                })?;
            }
            TxActorMessage::FlushCache { on_flush, run_id } => {
                println!("unconfirmed txs: {}", self.cache.len());

                let subscribe_request = SubscriptionRequest {
                    jsonrpc: "2.0".to_string(),
                    id: 1,
                    method: "eth_subscribe".to_string(),
                    params: vec!["fragments".to_string()],
                };

                // Subscribe to Fragments
                let raw_subscription = self.rpc.subscribe(json!(subscribe_request)).await?;
                let mut subscription: Subscription<Box<RawValue>> = Subscription::from(raw_subscription);
                let mut stream = subscription.into_stream();

                while let Some(result) = stream.next().await {

                    let fragment: FragmentResponse = serde_json::from_str(result.get())?;

                        // Get confirmed transaction hashes from the fragment
                    let confirmed_tx_hashes: Vec<TxHash> = fragment.transactions
                        .iter()
                        .map(|tx| tx.hash)
                        .collect();

                        // Filter cache for confirmed transactions
                    let confirmed_txs: Vec<PendingRunTx> = self.cache
                        .iter()
                        .filter(|tx| confirmed_tx_hashes.contains(&tx.tx_hash))
                        .cloned()
                        .collect();

                    // Update cache to remove confirmed transactions
                    self.cache.retain(|tx| !confirmed_tx_hashes.contains(&tx.tx_hash));

                    // Create RunTx entries for confirmed transactions
                    let run_txs = confirmed_txs.clone()
                        .into_iter()
                        .map(|pending_tx| {
                            let tx = fragment.transactions
                                .iter()
                                .find(|t| t.hash == pending_tx.tx_hash)
                                .expect("Transaction must exist in fragment");

                            RunTx {
                                tx_hash: pending_tx.tx_hash,
                                start_timestamp: pending_tx.start_timestamp / 1000,
                                end_timestamp: fragment.timestamp as usize,
                                block_number: fragment.block_number,
                                gas_used: fragment.gas_used as u128,
                                kind: pending_tx.kind,
                            }
                        })
                        .collect::<Vec<_>>();

                    // Insert confirmed transactions into database
                    if !run_txs.is_empty() {
                        self.db.insert_run_txs(run_id, run_txs)?;

                        for tx in &confirmed_txs {
                            println!(
                                "tx landed. hash={}\tblock_num={}",
                                tx.tx_hash,
                                fragment.block_number
                            );
                        }
                    }

                    // If cache is empty, we can break the subscription loop
                    if self.cache.is_empty() {
                        break;
                    }
                }

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
    pub fn new<D: DbOps + Send + Sync + 'static>(
        bufsize: usize,
        db: Arc<D>,
        rpc: Arc<AnyProvider>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(receiver, db, rpc);
        tokio::task::spawn(async move {
            actor.run().await.expect("tx actor crashed");
        });
        Self { sender }
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