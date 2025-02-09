use std::error::Error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use dashmap::DashMap;
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
    CheckConfirmedCount {
        response: oneshot::Sender<usize>,
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
    cache: Arc<DashMap<TxHash, PendingRunTx>>,
    read: SplitStream<WebSocketStream<ConnectStream>>,
    run_id: Option<u64>,
    expected_tx_count: usize,
    confirmed_count: usize,
}

impl<D> TxActor<D> where D: DbOps + Send + Sync + 'static {
    pub async fn new(
        receiver: mpsc::Receiver<TxActorMessage>,
        db: Arc<D>,
        ws_url: Url,
        run_id: u64,
        expected_tx_count: usize,
    ) -> Self {
        let (ws_stream, _) = connect_async(ws_url).await.expect("failed to connect to WS server");
        let (mut write, read) = ws_stream.split();
        let subscribe_message = r#"{"jsonrpc":"2.0","method":"eth_subscribe","params":["fragments"],"id":1}"#;
        write.send(async_tungstenite::tungstenite::Message::Text(subscribe_message.into()))
            .await
            .expect("failed to send subscribe message");

        Self {
            receiver,
            db,
            cache: Arc::new(DashMap::new()),
            read,
            run_id: Some(run_id),
            expected_tx_count,
            confirmed_count: 0,
        }
    }

    fn calculate_latency_stats(run_txs: &[RunTx]) -> (usize, usize, usize, f64) {
        if run_txs.is_empty() {
            return (0, 0, 0, 0.0);
        }

        let mut latencies: Vec<usize> = run_txs.iter()
            .map(|tx| tx.end_timestamp - tx.start_timestamp)
            .collect();

        latencies.sort_unstable();
        let total_txs = latencies.len();

        let p50_idx = (total_txs as f64 * 0.5) as usize;
        let p99_idx = (total_txs as f64 * 0.99) as usize;

        let p50 = latencies[p50_idx];
        let p99 = latencies[p99_idx];
        let max = latencies[total_txs - 1];

        let first_tx = run_txs.iter().min_by_key(|tx| tx.start_timestamp).unwrap();
        let last_tx = run_txs.iter().max_by_key(|tx| tx.end_timestamp).unwrap();
        let total_time = (last_tx.end_timestamp - first_tx.start_timestamp) as f64;
        let throughput = if total_time > 0.0 {
            total_txs as f64 / total_time
        } else {
            0.0
        };

        (p50, p99, max, throughput * 1000.0)
    }

    fn print_stats(run_txs: &[RunTx]) {
        let (p50, p99, max, throughput) = Self::calculate_latency_stats(run_txs);

        println!("\nTransaction Latency Statistics:");
        println!("--------------------------------");
        println!("Total Transactions: {}", run_txs.len());
        println!("P50 Latency: {} ms", p50);
        println!("P99 Latency: {} ms", p99);
        println!("Max Latency: {} ms", max);
        println!("Throughput: {:.2} tx/s", throughput);
        println!("--------------------------------\n");
    }

    async fn process_ws_message(&mut self, text: String) -> Result<bool, Box<dyn Error>> {
        let mut all_run_txs = Vec::new();
        match serde_json::from_str::<Value>(&text) {
            Ok(json) => {
                if let Some(result) = json["params"]["result"].as_object() {
                    let block_number = result["block_number"].as_u64().unwrap_or_default();
                    let fragment_index = result["index"].as_u64().unwrap_or_default();
                    let gas_used = result["gas_used"].as_u64().unwrap_or_default();
                    if let Some(transactions) = result["transactions"].as_array() {
                        let confirmed_tx_hashes: Vec<TxHash> = transactions
                            .iter()
                            .filter_map(|tx| tx["hash"].as_str())
                            .filter_map(|hash_str| {
                                let hash_str = hash_str.strip_prefix("0x").unwrap_or(hash_str);
                                hex::decode(hash_str)
                                    .ok()
                                    .map(|bytes| TxHash::from_slice(&bytes))
                            })
                            .collect();

                        let mut confirmed_txs = Vec::new();
                        for hash in &confirmed_tx_hashes {
                            if let Some((_, pending_tx)) = self.cache.remove(hash) {
                                confirmed_txs.push(pending_tx);
                            }
                        }

                        if !confirmed_txs.is_empty() {
                            println!("confirmed {} {} txs at block {}, fragments {}, unconfirmed txs count: {}",
                                     confirmed_txs.len(), confirmed_tx_hashes.len(), block_number, fragment_index, self.expected_tx_count - self.confirmed_count - confirmed_txs.len());
                        }

                        if let Some(run_id) = self.run_id {
                            let timestamp_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_millis();

                            let run_txs = confirmed_txs
                                .into_iter()
                                .map(|pending_tx| RunTx {
                                    tx_hash: pending_tx.tx_hash,
                                    start_timestamp: pending_tx.start_timestamp,
                                    end_timestamp: timestamp_ms as usize,
                                    block_number,
                                    gas_used: u128::from(gas_used),
                                    kind: pending_tx.kind,
                                })
                                .collect::<Vec<_>>();

                            if !run_txs.is_empty() {
                                all_run_txs.extend(run_txs.clone());
                                self.db.insert_run_txs(run_id, run_txs.clone())?;
                                self.confirmed_count += run_txs.len();

                                if self.confirmed_count >= self.expected_tx_count {
                                    println!("Reached expected transaction count: {}", self.expected_tx_count);
                                    Self::print_stats(&all_run_txs);
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("Failed to parse JSON: {:?}", e),
        }
        Ok(false)
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
                let start_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis() as usize;
                let run_tx = PendingRunTx {
                    tx_hash,
                    start_timestamp,
                    kind,
                };
                self.cache.insert(tx_hash, run_tx);
                on_receipt.send(()).map_err(|_| {
                    ContenderError::SpamError("failed to join TxActor callback", None)
                })?;
            }
            TxActorMessage::CheckConfirmedCount { response } => {
                response.send(self.expected_tx_count - self.confirmed_count).map_err(|_| {
                    ContenderError::SpamError("failed to send confirmed count", None)
                })?;
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.handle_message(msg).await?;
                }
                Some(Ok(message)) = self.read.next() => {
                    if let async_tungstenite::tungstenite::Message::Text(text) = message {
                        if self.process_ws_message(text).await? {
                            break;
                        }
                    }
                }
                else => break,
            }
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
        run_id: u64,
        expected_tx_count: usize,
    ) -> Result<Self, Box<dyn Error>> {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(receiver, db, ws_url, run_id, expected_tx_count).await;
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

    pub async fn wait_for_confirmations(&self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let (sender, receiver) = oneshot::channel();
            self.sender
                .send(TxActorMessage::CheckConfirmedCount {
                    response: sender,
                })
                .await?;

            let unconfirmed_count = receiver.await?;

            if unconfirmed_count == 0 {
                return Ok(());
            }

            // Sleep for a short duration before checking again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}
