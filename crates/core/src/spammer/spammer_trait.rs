use std::sync::atomic::AtomicBool;
use std::{pin::Pin, sync::Arc};
use std::collections::HashSet;
use alloy::providers::Provider;
use futures::Stream;
use futures::StreamExt;

use crate::{
    db::DbOps,
    error::ContenderError,
    generator::{seeder::Seeder, templater::Templater, types::AnyProvider, Generator, PlanConfig},
    test_scenario::TestScenario,
    Result,
};

use super::{ExecutionPayload, SpamTrigger};
use super::{tx_actor::TxActorHandle, OnTxSent};

pub trait Spammer<F, D, S, P>
where
    F: OnTxSent + Send + Sync + 'static,
    D: DbOps + Send + Sync + 'static,
    S: Seeder + Send + Sync,
    P: PlanConfig<String> + Templater<String> + Send + Sync,
{
    // fn get_msg_handler(&self, db: Arc<D>, rpc_client: Arc<AnyProvider>) -> TxActorHandle {
    //     TxActorHandle::new(12, db.clone(), rpc_client.clone())
    // }

    fn on_spam(
        &self,
        scenario: &mut TestScenario<D, S, P>,
    ) -> impl std::future::Future<Output = Result<Pin<Box<dyn Stream<Item = SpamTrigger> + Send>>>>;

    fn spam_rpc(
        &self,
        scenario: &mut TestScenario<D, S, P>,
        txs_per_period: usize,
        num_periods: usize,
        sent_tx_callback: Arc<F>,
    ) -> impl std::future::Future<Output = Result<()>> {
        let quit = Arc::new(AtomicBool::default());

        let quit_clone = quit.clone();
        tokio::task::spawn(async move {
            loop {
                let _ = tokio::signal::ctrl_c().await;
                quit_clone.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        });

        async move {
            log::info!("start spamming...");
            log::info!("loading tx requests...");
            let tx_requests = scenario
                .load_txs(crate::generator::PlanType::Spam(
                    txs_per_period * num_periods,
                    |_named_req| Ok(None),
                ))
                .await?;
            let tx_req_chunks = tx_requests.chunks(txs_per_period).collect::<Vec<&[_]>>();
            let block_num = scenario
                .rpc_client
                .get_block_number()
                .await
                .map_err(|e| ContenderError::with_err(e, "failed to get block number"))?;

            log::info!("preparing spam txs...");
            let mut unique_addresses = HashSet::new();
            let mut prepared_payloads = Vec::with_capacity(num_periods);
            for chunk in &tx_req_chunks {
                let prepared_payload = scenario.prepare_spam(chunk).await?;
                prepared_payloads.push(prepared_payload.clone());

                let addresses: Vec<_> = prepared_payload
                    .iter()
                    .flat_map(|ex_payload| match ex_payload {
                        ExecutionPayload::SignedTx(envelope, tx_req) => {
                            vec![tx_req.tx.from.unwrap_or_default()]
                        }
                        ExecutionPayload::SignedTxBundle(_envelopes, tx_reqs) => {todo!()}
                    })
                    .collect();

                for address in addresses {
                    unique_addresses.insert(address);
                }
            }

            let address_count = unique_addresses.len();
            log::info!("address count: {}", address_count);

            let mut tick = 0;

            log::info!("executing spam txs...");
            while tick < num_periods {
                let start_time = std::time::Instant::now();
                log::info!("[{}] start send txs, current datetime: {}", tick, chrono::Local::now().to_string());
                let spam_tasks = scenario
                    .execute_spam(&prepared_payloads[tick], sent_tx_callback.clone())
                    .await?;

                for task in spam_tasks {
                    let res = task.await;
                    if let Err(e) = res {
                        eprintln!("spam task failed: {:?}", e);
                    }
                }

                log::info!("[{}] end send txs, current datetime: {}", tick, chrono::Local::now().to_string());

                let elapsed = start_time.elapsed();
                if elapsed < std::time::Duration::from_secs(1) {
                    tokio::time::sleep(std::time::Duration::from_secs(1) - elapsed).await;
                }

                tick += 1;
            }

            let _ = scenario.msg_handle.wait_for_confirmations().await;

            Ok(())
        }
    }
}
