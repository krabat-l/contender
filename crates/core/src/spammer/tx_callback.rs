use std::{collections::HashMap, sync::Arc};
use alloy::consensus::TxEnvelope;
use alloy::providers::PendingTransactionConfig;
use alloy::rpc::client::RpcClient;
use tokio::task::JoinHandle;

use crate::generator::{types::AnyProvider, NamedTxRequest};

use super::tx_actor::TxActorHandle;

pub trait OnTxSent<K = String, V = String>
where
    K: Eq + std::hash::Hash + AsRef<str>,
    V: AsRef<str>,
{
    fn on_tx_sent(
        &self,
        req: &NamedTxRequest,
        extra: Option<HashMap<K, V>>,
        signed_tx: TxEnvelope,
        tx_handler: Option<Arc<TxActorHandle>>,
    ) -> Option<JoinHandle<()>>;
}

pub struct NilCallback;

pub struct LogCallback {
    pub rpc_provider: Arc<AnyProvider>,
}

impl LogCallback {
    pub fn new(rpc_provider: Arc<AnyProvider>) -> Self {
        Self { rpc_provider }
    }
}

impl OnTxSent for NilCallback {
    fn on_tx_sent(
        &self,
        _req: &NamedTxRequest,
        _extra: Option<HashMap<String, String>>,
        _signed_tx: TxEnvelope,
        _tx_handler: Option<Arc<TxActorHandle>>,
    ) -> Option<JoinHandle<()>> {
        // do nothing
        None
    }
}

impl OnTxSent for LogCallback {
    fn on_tx_sent(
        &self,
        _req: &NamedTxRequest,
        extra: Option<HashMap<String, String>>,
        signed_tx: TxEnvelope,
        tx_actor: Option<Arc<TxActorHandle>>,
    ) -> Option<JoinHandle<()>> {
        let kind = extra
            .as_ref()
            .and_then(|e| e.get("kind").map(|k| k.to_string()));
        let handle = tokio::task::spawn(async move {
            if let Some(tx_actor) = tx_actor {
                tx_actor
                    .cache_run_tx(kind, signed_tx)
                    .await
                    .expect("failed to cache run tx");
            }
        });
        Some(handle)
    }
}
