use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use alloy::consensus::TxEnvelope;
use alloy::eips::eip2718::Encodable2718;
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
use chrono::{DateTime, Duration, NaiveDateTime};
use futures::SinkExt;
use futures::stream::SplitStream;
use serde_json::{json, Value};
use crate::generator::types::AnyProvider;
use reqwest::Client;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct TransactionSigned {
    pub hash: TxHash,
}

enum TxActorMessage {
    SentRunTx {
        kind: Option<String>,
        on_receipt: oneshot::Sender<()>,
        signed_tx: TxEnvelope,
        rpc_client: Arc<AnyProvider>,
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

#[derive(Debug, Serialize)]
struct BatchRequest {
    jsonrpc: String,
    method: String,
    params: Vec<String>,
    id: usize,
}

#[derive(Debug)]
struct BatchTxGroup {
    txs: Vec<(TxEnvelope, Option<String>, oneshot::Sender<()>)>,
    timestamp: usize,
}

struct TxActor<D> where D: DbOps {
    receiver: mpsc::Receiver<TxActorMessage>,
    db: Arc<D>,
    pending_txs: Arc<DashMap<TxHash, PendingRunTx>>,
    read: SplitStream<WebSocketStream<ConnectStream>>,
    run_id: Option<u64>,
    expected_tx_count: usize,
    confirmed_count: usize,
    sent_count: Arc<AtomicUsize>,
    all_run_txs: Vec<RunTx>,
    http_client: Client,
    rpc_url: String,
    batch_size: usize,
    current_batch: BatchTxGroup,
}

impl<D> TxActor<D> where D: DbOps + Send + Sync + 'static {
    pub async fn new(
        receiver: mpsc::Receiver<TxActorMessage>,
        db: Arc<D>,
        ws_url: Url,
        http_url: String,
        run_id: u64,
        expected_tx_count: usize,
        batch_size: usize,
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
            pending_txs: Arc::new(DashMap::new()),
            read,
            run_id: Some(run_id),
            expected_tx_count,
            confirmed_count: 0,
            sent_count: Arc::new(AtomicUsize::new(0)),
            all_run_txs: Vec::new(),
            http_client: Client::new(),
            rpc_url: http_url,
            batch_size,
            current_batch: BatchTxGroup {
                txs: Vec::with_capacity(batch_size),
                timestamp: 0,
            },
        }
    }


    fn calculate_latency_stats(run_txs: &[RunTx]) -> (usize, usize, usize, usize, usize, f64) {
        if run_txs.is_empty() {
            return (0, 0, 0, 0, 0, 0.0);
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

        (first_tx.start_timestamp, last_tx.end_timestamp, p50, p99, max, throughput * 1000.0)
    }

    fn print_stats(&self, run_txs: &[RunTx]) {
        let (start, end, p50, p99, max, throughput) = Self::calculate_latency_stats(run_txs);

        let start_time = DateTime::from_timestamp_millis(start as i64);
        let end_time = DateTime::from_timestamp_millis(end as i64);

        println!("\nTransaction Latency Statistics:");
        println!("--------------------------------");
        println!("Total Transactions: {}", run_txs.len());
        println!(
            "Statistics - Sent: {}, Confirmed: {}, Pending: {}, Total: {}",
            self.sent_count.load(Ordering::Relaxed),
            self.confirmed_count,
            self.pending_txs.len(),
            self.expected_tx_count
        );
        println!("Start Time: {:?}", start_time);
        println!("End Time: {:?}", end_time);
        println!("P50 Latency: {} ms", p50);
        println!("P99 Latency: {} ms", p99);
        println!("Max Latency: {} ms", max);
        println!("Throughput: {:.2} tx/s", throughput);
        println!("--------------------------------\n");
    }

    async fn send_batch(&mut self) -> Result<(), Box<dyn Error>> {
        if self.current_batch.txs.is_empty() {
            return Ok(());
        }

        let mut requests = Vec::with_capacity(self.current_batch.txs.len());
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as usize;

        for (idx, (tx, _, _)) in self.current_batch.txs.iter().enumerate() {
            let mut encoded_tx = vec![];
            tx.encode_2718(&mut encoded_tx);
            let rlp_hex = alloy::hex::encode_prefixed(encoded_tx);

            let params = vec![
                rlp_hex,
            ];

            requests.push(BatchRequest {
                jsonrpc: "2.0".to_string(),
                method: "eth_sendRawTransaction".to_string(),
                params,
                id: idx,
            });
        }

        let response = self.http_client
            .post(&self.rpc_url)
            .json(&requests)
            .send()
            .await?;

        let results: Vec<serde_json::Value> = response.json().await?;

        for (result, (_, kind, receipt_sender)) in results.into_iter().zip(self.current_batch.txs.drain(..)) {
            if let Some(hash_str) = result["result"].as_str() {
                let hash_str = hash_str.strip_prefix("0x").unwrap_or(hash_str);
                if let Ok(bytes) = hex::decode(hash_str) {
                    let tx_hash = TxHash::from_slice(&bytes);
                    self.pending_txs.insert(tx_hash, PendingRunTx::new(
                        tx_hash,
                        timestamp,
                        kind.as_ref().map(String::as_str),
                    ));
                    self.sent_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            let _ = receipt_sender.send(());
        }
        Ok(())
    }

    async fn process_ws_message(&mut self, text: String) -> Result<bool, Box<dyn Error>> {
        match serde_json::from_str::<Value>(&text) {
            Ok(json) => {
                if let Some(result) = json["params"]["result"].as_object() {
                    let block_number = result["block_number"].as_u64().unwrap_or_default();
                    let fragment_index = result["index"].as_u64().unwrap_or_default();
                    let tx_offset = result["tx_offset"].as_u64().unwrap_or_default();
                    let gas_used = result["gas_used"].as_u64().unwrap_or_default();

                    if let Some(transactions) = result["transactions"].as_array() {
                        let timestamp_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_millis() as usize;

                        let mut new_confirmed_txs = Vec::new();

                        for tx in transactions {
                            if let Some(hash_str) = tx["hash"].as_str() {
                                let hash_str = hash_str.strip_prefix("0x").unwrap_or(hash_str);
                                if let Ok(bytes) = hex::decode(hash_str) {
                                    let tx_hash = TxHash::from_slice(&bytes);

                                    // Try to match with pending tx
                                    if let Some((_, pending_tx)) = self.pending_txs.remove(&tx_hash) {
                                        new_confirmed_txs.push(RunTx {
                                            tx_hash,
                                            start_timestamp: pending_tx.start_timestamp,
                                            end_timestamp: timestamp_ms,
                                            block_number,
                                            gas_used: u128::from(gas_used),
                                            kind: pending_tx.kind,
                                        });
                                    }
                                }
                            }
                        }

                        if !new_confirmed_txs.is_empty() {
                            println!("confirmed {}/{} txs at fragment {}, block {}, current block tx count: {}, remaining: {}/{}",
                                     new_confirmed_txs.len(), transactions.len(), fragment_index,
                                     block_number, tx_offset as usize + transactions.len(),
                                     self.expected_tx_count - self.confirmed_count - new_confirmed_txs.len(),
                                     self.expected_tx_count);

                            if let Some(run_id) = self.run_id {
                                self.all_run_txs.extend(new_confirmed_txs.clone());
                                self.db.insert_run_txs(run_id, new_confirmed_txs.clone())?;
                                self.confirmed_count += new_confirmed_txs.len();

                                if self.confirmed_count >= self.expected_tx_count {
                                    println!("Reached expected transaction count: {}", self.expected_tx_count);
                                    self.print_stats(&self.all_run_txs);
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
                kind,
                on_receipt,
                signed_tx,
                rpc_client,
            } => {
                self.current_batch.txs.push((signed_tx, kind, on_receipt));
                if self.current_batch.txs.len() >= self.batch_size {
                    self.send_batch().await?;
                }
            }
            TxActorMessage::CheckConfirmedCount { response } => {
                response.send(self.expected_tx_count - self.confirmed_count).map_err(|_| {
                    ContenderError::SpamError("failed to send confirmed count", None)
                })?;
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut batch_interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !self.all_run_txs.is_empty() {
                        self.print_stats(&self.all_run_txs);
                    }
                }
                _ = batch_interval.tick() => {
                    if !self.current_batch.txs.is_empty() {
                        self.send_batch().await?;
                    }
                }
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
        http_url: String,
        run_id: u64,
        expected_tx_count: usize,
    ) -> Result<Self, Box<dyn Error>> {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(
            receiver,
            db,
            ws_url,
            http_url,
            run_id,
            expected_tx_count,
            100,
        ).await;

        tokio::task::spawn(async move {
            actor.run().await.expect("tx actor crashed");
        });

        Ok(Self { sender })
    }

    pub async fn cache_run_tx(
        &self,
        kind: Option<String>,
        signed_tx: TxEnvelope,
        rpc_client: Arc<AnyProvider>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(TxActorMessage::SentRunTx {
                kind,
                on_receipt: sender,
                signed_tx,
                rpc_client,
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
