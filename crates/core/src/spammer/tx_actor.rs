use std::collections::HashMap;
use std::error::Error;
use std::fs::OpenOptions;
use std::sync::{Arc};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use alloy::consensus::{Transaction, TxEnvelope};
use alloy::network::AnyNetwork;
use tokio::sync::{mpsc, oneshot};
use dashmap::DashMap;
use web3::{
    futures::StreamExt,
};
use serde::{Deserialize, Serialize};
use alloy::primitives::{Address, TxHash};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::k256::U256;
use url::Url;
use crate::{
    db::{DbOps, RunTx},
    error::ContenderError,
};
use async_tungstenite::tokio::{connect_async, ConnectStream};
use async_tungstenite::WebSocketStream;
use chrono::{DateTime, Utc};
use futures::SinkExt;
use futures::stream::SplitStream;
use serde_json::Value;
use crate::generator::types::AnyProvider;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct TransactionSigned {
    pub hash: TxHash,
}

pub struct RpcClientPool {
    clients: Vec<Arc<AnyProvider>>,
    pool_size: usize,
}

impl RpcClientPool {
    pub fn new(rpc_url: Url, pool_size: usize) -> Self {
        let clients: Vec<Arc<AnyProvider>> = (0..pool_size)
            .map(|_| {
                Arc::new(
                    ProviderBuilder::new()
                        .network::<AnyNetwork>()
                        .on_http(rpc_url.to_owned())
                )
            })
            .collect();

        Self {
            clients,
            pool_size,
        }
    }

    pub fn get_client_for_address(&self, num: &usize) -> Arc<AnyProvider> {
        let index = num % self.pool_size;
        self.clients[index].clone()
    }
}

enum TxActorMessage {
    SentRunTx {
        // on_receipt: oneshot::Sender<()>,
        tx_groups: HashMap<Address, Vec<TxEnvelope>>,
        // rpc_client: Arc<AnyProvider>,
    },
    CheckConfirmedCount {
        response: oneshot::Sender<usize>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingRunTx {
    tx_hash: TxHash,
    start_timestamp: usize,
}

impl PendingRunTx {
    pub fn new(tx_hash: TxHash, start_timestamp: usize) -> Self {
        Self {
            tx_hash,
            start_timestamp,
        }
    }
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
    queue_count: Arc<AtomicUsize>,
    all_run_txs: Vec<RunTx>,
    client_pool: Arc<RpcClientPool>,
    recent_confirmations: Vec<(usize, usize)>,
    current_second_tx_count: usize,
    current_second_gas_used: u64,
    tps_file: String,
}

impl<D> TxActor<D> where D: DbOps + Send + Sync + 'static {
    pub async fn new(
        receiver: mpsc::Receiver<TxActorMessage>,
        db: Arc<D>,
        rpc_url: Url,
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
        let client_pool = Arc::new(RpcClientPool::new(rpc_url, 200));
        let now = Utc::now();
        let tps_file = format!("tps_data_{}.csv", now.format("%Y%m%d_%H%M%S"));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tps_file)
            .expect("Failed to create TPS file");
        writeln!(file, "timestamp,tps,gasused").expect("Failed to write headers");

        Self {
            receiver,
            db,
            pending_txs: Arc::new(DashMap::new()),
            read,
            run_id: Some(run_id),
            expected_tx_count,
            confirmed_count: 0,
            sent_count: Arc::new(AtomicUsize::new(0)),
            queue_count: Arc::new(AtomicUsize::new(0)),
            all_run_txs: Vec::new(),
            client_pool,
            recent_confirmations: Vec::new(),
            current_second_tx_count: 0,
            current_second_gas_used: 0,
            tps_file,
        }
    }

    fn write_tps_to_file(&mut self, timestamp: i64, tps: usize, gas_used: u64) {
        if let Ok(mut file) = OpenOptions::new()
            .append(true)
            .open(&self.tps_file)
        {
            writeln!(file, "{},{},{}", timestamp, tps, gas_used)
                .expect("Failed to write TPS data");
        }
    }

    fn calculate_realtime_tps(&mut self, current_timestamp: usize) -> f64 {
        let window_start = current_timestamp.saturating_sub(1000);
        self.recent_confirmations.retain(|(ts, _)| *ts >= window_start);

        let total_confirmations: usize = self.recent_confirmations.iter()
            .map(|(_, count)| count)
            .sum();

        let window_duration = if self.recent_confirmations.is_empty() {
            1.0
        } else {
            let oldest_ts = self.recent_confirmations.first()
                .map(|(ts, _)| *ts)
                .unwrap_or(current_timestamp);
            let duration_ms = (current_timestamp - oldest_ts) as f64;
            duration_ms / 1000.0
        };

        if window_duration > 0.0 {
            total_confirmations as f64 / window_duration
        } else {
            0.0
        }
    }


    fn calculate_latency_stats(&self) -> (usize, usize, usize, usize, usize, f64) {
        if self.all_run_txs.is_empty() {
            return (0, 0, 0, 0, 0, 0.0);
        }

        let mut latencies: Vec<usize> = self.all_run_txs.iter()
            .map(|tx| tx.end_timestamp - tx.start_timestamp)
            .collect();

        latencies.sort_unstable();
        let total_txs = latencies.len();

        let p50_idx = (total_txs as f64 * 0.5) as usize;
        let p99_idx = (total_txs as f64 * 0.99) as usize;

        let p50 = latencies[p50_idx];
        let p99 = latencies[p99_idx];
        let max = latencies[total_txs - 1];

        let first_tx = self.all_run_txs.iter().min_by_key(|tx| tx.start_timestamp).unwrap();
        let last_tx = self.all_run_txs.iter().max_by_key(|tx| tx.end_timestamp).unwrap();
        let total_time = (last_tx.end_timestamp - first_tx.start_timestamp) as f64;
        let throughput = if total_time > 0.0 {
            total_txs as f64 / total_time
        } else {
            0.0
        };

        (first_tx.start_timestamp, last_tx.end_timestamp, p50, p99, max, throughput * 1000.0)
    }

    fn print_stats(&mut self) {
        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as usize;

        let (start, end, p50, p99, max, throughput) = self.calculate_latency_stats();
        let realtime_tps = self.calculate_realtime_tps(current_timestamp);

        let start_time = DateTime::from_timestamp_millis(start as i64);
        let end_time = DateTime::from_timestamp_millis(end as i64);

        let sent_count = self.sent_count.load(Ordering::Relaxed);
        let confirmed_count = self.confirmed_count;
        let queued_on_chain_count = sent_count - confirmed_count;
        let queued_count = self.pending_txs.len();
        log::info!("Transaction Latency Statistics:");
        log::info!("--------------------------------");
        log::info!("Sent: {}", sent_count);
        log::info!("Confirmed: {}", confirmed_count);
        log::info!("Queuing on chain: {}", queued_on_chain_count);
        log::info!("Queuing on client: {}", queued_count - queued_on_chain_count);
        log::info!("Queuing: {}", queued_count);
        log::info!("Current: {}", self.expected_tx_count - confirmed_count);
        log::info!("Total: {}", self.expected_tx_count);
        log::info!("Start Time: {:?}", start_time);
        log::info!("End Time: {:?}", end_time);
        log::info!("P50 Latency: {} ms", p50);
        log::info!("P99 Latency: {} ms", p99);
        log::info!("Max Latency: {} ms", max);
        log::info!("Overall Throughput: {:.2} tx/s", throughput);
        log::info!("Realtime TPS (15s): {:.2} tx/s", realtime_tps);
        log::info!("--------------------------------");
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
                                            kind: None,
                                        });
                                    }
                                }
                            }
                        }

                        if !new_confirmed_txs.is_empty() {
                            self.current_second_tx_count += new_confirmed_txs.len();
                            self.current_second_gas_used += gas_used;
                            self.recent_confirmations.push((timestamp_ms, new_confirmed_txs.len()));
                            log::info!("confirmed {}/{} txs at fragment {}, block {}, gas_used: {}, current block tx count: {}, remaining: {}/{}",
                                     new_confirmed_txs.len(), transactions.len(), fragment_index,
                                     block_number, gas_used, tx_offset as usize + transactions.len(),
                                     self.expected_tx_count - self.confirmed_count - new_confirmed_txs.len(),
                                     self.expected_tx_count);

                            if let Some(run_id) = self.run_id {
                                self.all_run_txs.extend(new_confirmed_txs.clone());
                                // self.db.insert_run_txs(run_id, new_confirmed_txs.clone())?;
                                self.confirmed_count += new_confirmed_txs.len();

                                if self.confirmed_count >= self.expected_tx_count {
                                    log::info!("Reached expected transaction count: {}", self.expected_tx_count);
                                    self.print_stats();
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
                tx_groups,
            } => {
                let queue_num = self.queue_count.load(Ordering::Relaxed);
                if queue_num != 0 {
                    log::warn!("There are {} txs not send to client", queue_num);
                }
                let mut client_txs = HashMap::new();
                for (from, txs) in tx_groups.clone() {
                    let addr_bytes = from.to_vec();
                    let prefix = &addr_bytes[0..4];
                    let num = u32::from_be_bytes(prefix.try_into().unwrap());
                    let index = num as usize % 1000;
                    client_txs.entry(index).or_insert_with(Vec::new).extend(txs.clone());
                    self.queue_count.fetch_add(txs.len(), Ordering::Relaxed);
                }

                let tasks: Vec<_> = client_txs.into_iter().map(|(index, txs)| {
                    let sent_count_clone = self.sent_count.clone();
                    let client_pool_clone = self.client_pool.clone();
                    let pending_txs_clone = self.pending_txs.clone();
                    let queue_count = self.queue_count.clone();
                    tokio::spawn(async move{
                        let client = client_pool_clone.get_client_for_address(&index);
                        for tx in txs.clone() {
                            let start_timestamp = SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("time went backwards")
                                .as_millis();
                            let tx_hash = tx.tx_hash();
                            pending_txs_clone.insert(*tx_hash, PendingRunTx {
                                tx_hash: *tx_hash,
                                start_timestamp: start_timestamp as usize,
                            });
                            let _ = client.send_tx_envelope(tx).await;
                            sent_count_clone.fetch_add(1, Ordering::Relaxed);
                            queue_count.fetch_sub(1, Ordering::Relaxed);
                        }
                    })
                }).collect();
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
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let current_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_secs() as i64;
                    self.write_tps_to_file(current_timestamp, self.current_second_tx_count, self.current_second_gas_used);
                    self.current_second_tx_count = 0;
                    self.current_second_gas_used = 0;
                    if !self.all_run_txs.is_empty() {
                        self.print_stats();
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
        rpc_url: Url,
        ws_url: Url,
        run_id: u64,
        expected_tx_count: usize,
    ) -> Result<Self, Box<dyn Error>> {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(receiver, db, rpc_url, ws_url, run_id, expected_tx_count).await;
        tokio::task::spawn(async move {
            actor.run().await.expect("tx actor crashed");
        });
        Ok(Self { sender })
    }

    pub async fn cache_run_tx(
        &self,
        tx_groups: HashMap<Address, Vec<TxEnvelope>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.sender
            .send(TxActorMessage::SentRunTx {
                tx_groups,
            })
            .await?;
        Ok(())
    }

    pub async fn wait_for_confirmations(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut last_count = 0;
        let mut start_time = None;

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

            if unconfirmed_count == last_count {
                if start_time.is_none() {
                    start_time = Some(std::time::Instant::now());
                } else if start_time.unwrap().elapsed().as_secs() >= 3 {
                    return Ok(());
                }
            } else {
                last_count = unconfirmed_count;
                start_time = None;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}
