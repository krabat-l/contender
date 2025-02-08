use std::sync::Arc;
use futures::{StreamExt, SinkExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    WebSocketStream,
    MaybeTlsStream,
};
use tokio::net::TcpStream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use alloy::primitives::TxHash;

use crate::{
    db::{DbOps, RunTx},
    error::ContenderError,
};

// WebSocket 相关的数据结构保持不变...
#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WsResponse {
    jsonrpc: String,
    id: Option<u64>,
    result: Option<String>,
    method: Option<String>,
    params: Option<WsParams>,
}

#[derive(Debug, Deserialize)]
struct WsParams {
    subscription: String,
    result: FragmentResponse,
}

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

// TxActorMessage 和 PendingRunTx 保持不变...
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
    ws_url: String,
}

impl<D> TxActor<D> where D: DbOps + Send + Sync + 'static {
    pub fn new(
        receiver: mpsc::Receiver<TxActorMessage>,
        db: Arc<D>,
        ws_url: String,
    ) -> Self {
        Self {
            receiver,
            db,
            cache: Vec::new(),
            ws_url,
        }
    }

    async fn subscribe_to_fragments(&self) -> Result<impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>, Box<dyn std::error::Error>> {
        // 直接使用字符串 URL
        let (ws_stream, _) = connect_async(&self.ws_url).await?;
        let (mut write, read) = ws_stream.split();

        // 发送 fragments 订阅请求
        let subscribe_req = SubscriptionRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "eth_subscribe".to_string(),
            params: vec!["fragments".to_string()],
        };

        let msg = Message::Text(serde_json::to_string(&subscribe_req)?);
        write.send(msg).await?;

        Ok(read)
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

                let mut ws_stream = self.subscribe_to_fragments().await?;

                while !self.cache.is_empty() {
                    if let Some(msg) = ws_stream.next().await {
                        let msg = msg?;
                        if let Message::Text(text) = msg {
                            let response: WsResponse = serde_json::from_str(&text)?;

                            // 处理订阅确认消息
                            if response.id.is_some() {
                                continue;
                            }

                            if let Some(params) = response.params {
                                let fragment = params.result;

                                // 获取已确认的交易哈希
                                let confirmed_tx_hashes: Vec<TxHash> = fragment.transactions
                                    .iter()
                                    .map(|tx| tx.hash)
                                    .collect();

                                // 从缓存中找出已确认的交易
                                let confirmed_txs: Vec<PendingRunTx> = self.cache
                                    .iter()
                                    .filter(|tx| confirmed_tx_hashes.contains(&tx.tx_hash))
                                    .cloned()
                                    .collect();

                                // 更新缓存，移除已确认的交易
                                self.cache.retain(|tx| !confirmed_tx_hashes.contains(&tx.tx_hash));

                                // 创建RunTx条目
                                let run_txs = confirmed_txs.clone().into_iter()
                                    .map(|pending_tx| {
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

                                // 将确认的交易写入数据库
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
                            }
                        }
                    }

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
        ws_url: String,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(bufsize);
        let mut actor = TxActor::new(receiver, db, ws_url);
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