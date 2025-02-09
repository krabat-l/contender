use std::error::Error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use web3::{
    futures::StreamExt,
};
use serde::{Deserialize, Serialize};
use alloy::primitives::TxHash;
use url::Url;
use crate::{
    db::{DbOps, RunTx},
    error::ContenderError,
};
use web3::types::{BlockHeader, BlockId};
use async_tungstenite::tokio::{connect_async, ConnectStream};
use async_tungstenite::WebSocketStream;
use futures::SinkExt;
use futures::stream::SplitStream;
use serde_json::Value;

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
    read: SplitStream<WebSocketStream<ConnectStream>>,
}

impl<D> TxActor<D> where D: DbOps + Send + Sync + 'static {
    pub async fn new(
        receiver: mpsc::Receiver<TxActorMessage>,
        db: Arc<D>,
        ws_url: Url,
    ) -> Self {
        let (ws_stream, _) = connect_async(ws_url).await.expect("failed to connect to WS server");
        let (mut write, mut read) = ws_stream.split();
        let subscribe_message = r#"{"jsonrpc":"2.0","method":"eth_subscribe","params":["fragments"],"id":1}"#;
        write.send(async_tungstenite::tungstenite::Message::Text(subscribe_message.into())).await.expect("failed to send subscribe message");

        Self {
            receiver,
            db,
            cache: Vec::new(),
            read,
        }
    }

    async fn handle_message(
        &mut self,
        message: TxActorMessage,
    ) -> Result<(), Box<dyn Error>> {
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

                while !self.cache.is_empty() {
                    while let Some(message) = self.read.next().await {
                        match message? {
                            async_tungstenite::tungstenite::Message::Text(text) => {
                                // Deserialize the JSON message
                                match serde_json::from_str::<Value>(&text) {
                                    Ok(json) => {
                                        if let Some(result) = json["params"]["result"].as_object() {
                                            let block_number = result["block_number"].as_u64().unwrap_or_default();
                                            let timestamp = result["timestamp"].as_u64().unwrap_or_default();
                                            let gas_used = result["gas_used"].as_u64().unwrap_or_default();
                                            // Extract and print the number of transactions
                                            if let Some(transactions) = result["transactions"].as_array() {
                                                println!("Number of transactions: {}", transactions.len());
                                                let confirmed_tx_hashes: Vec<TxHash> = transactions
                                                    .iter()
                                                    .filter_map(|tx| tx["hash"].as_str())
                                                    .map(|hash_str| TxHash::from_slice(&hex::decode(hash_str).unwrap()))
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
                                                            end_timestamp: timestamp as usize,
                                                            block_number,
                                                            gas_used: u128::from(gas_used),
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
                                                            block_number,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!("Failed to parse JSON: {:?}", e),
                                }
                            }
                            _ => {}
                        }
                        if self.cache.is_empty() {
                            break;
                        }
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
    pub async fn new<D: DbOps + Send + Sync + 'static>(
        bufsize: usize,
        db: Arc<D>,
        ws_url: Url,
    ) -> Result<Self, Box<dyn Error>> {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(receiver, db, ws_url).await;
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