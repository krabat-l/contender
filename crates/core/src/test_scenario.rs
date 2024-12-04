use crate::agent_controller::AgentStore;
use crate::db::{DbOps, NamedTx};
use crate::error::ContenderError;
use crate::generator::named_txs::ExecutionRequest;
use crate::generator::templater::Templater;
use crate::generator::types::{AnyProvider, EthProvider};
use crate::generator::NamedTxRequest;
use crate::generator::{seeder::Seeder, types::PlanType, Generator, PlanConfig};
use crate::spammer::tx_actor::TxActorHandle;
use crate::spammer::{ExecutionPayload, OnTxSent, SpamTrigger};
use crate::Result;
use alloy::consensus::Transaction;
use alloy::eips::eip2718::Encodable2718;
use alloy::hex::ToHexExt;
use alloy::network::{AnyNetwork, EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, FixedBytes};
use alloy::providers::{PendingTransactionConfig, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::http::reqwest::Url;
use contender_bundle_provider::{BundleClient, EthSendBundle};
use std::collections::HashMap;
use std::sync::Arc;

/// A test scenario can be used to run a test with a specific configuration, database, and RPC provider.
pub struct TestScenario<D, S, P>
where
    D: DbOps + Send + Sync + 'static,
    S: Seeder,
    P: PlanConfig<String> + Templater<String> + Send + Sync,
{
    pub config: P,
    pub db: Arc<D>,
    pub rpc_url: Url,
    pub rpc_client: Arc<AnyProvider>,
    pub eth_client: Arc<EthProvider>,
    pub bundle_client: Option<Arc<BundleClient>>,
    pub builder_rpc_url: Option<Url>,
    pub rand_seed: S,
    /// Wallets explicitly given by the user
    pub wallet_map: HashMap<Address, EthereumWallet>,
    /// Wallets generated by the system
    pub agent_store: AgentStore,
    pub nonces: HashMap<Address, u64>,
    pub chain_id: u64,
    pub gas_limits: HashMap<FixedBytes<4>, u128>,
    pub msg_handle: Arc<TxActorHandle>,
}

impl<D, S, P> TestScenario<D, S, P>
where
    D: DbOps + Send + Sync + 'static,
    S: Seeder + Send + Sync,
    P: PlanConfig<String> + Templater<String> + Send + Sync,
{
    pub async fn new(
        config: P,
        db: Arc<D>,
        rpc_url: Url,
        builder_rpc_url: Option<Url>,
        rand_seed: S,
        signers: &[PrivateKeySigner],
        agent_store: AgentStore,
    ) -> Result<Self> {
        let rpc_client = Arc::new(
            ProviderBuilder::new()
                .network::<AnyNetwork>()
                .on_http(rpc_url.to_owned()),
        );

        let mut wallet_map = HashMap::new();
        let wallets = signers.iter().map(|s| {
            let w = EthereumWallet::new(s.clone());
            (s.address(), w)
        });
        for (addr, wallet) in wallets {
            wallet_map.insert(addr, wallet);
        }
        for (name, signers) in agent_store.all_agents() {
            println!("adding '{}' signers to wallet map", name);
            for signer in signers.signers.iter() {
                wallet_map.insert(signer.address(), EthereumWallet::new(signer.clone()));
            }
        }

        let chain_id = rpc_client
            .get_chain_id()
            .await
            .map_err(|e| ContenderError::with_err(e, "failed to get chain id"))?;

        let mut nonces = HashMap::new();
        let all_addrs = wallet_map.keys().copied().collect::<Vec<Address>>();
        for addr in &all_addrs {
            let nonce = rpc_client
                .get_transaction_count(*addr)
                .await
                .map_err(|e| ContenderError::with_err(e, "failed to retrieve nonce from RPC"))?;
            nonces.insert(*addr, nonce);
        }
        let gas_limits = HashMap::new();

        let bundle_client = builder_rpc_url
            .as_ref()
            .map(|url| Arc::new(BundleClient::new(url.clone())));

        let msg_handle = Arc::new(TxActorHandle::new(12, db.clone(), rpc_client.clone()));

        Ok(Self {
            config,
            db: db.clone(),
            rpc_url: rpc_url.to_owned(),
            rpc_client: rpc_client.clone(),
            eth_client: Arc::new(ProviderBuilder::new().on_http(rpc_url)),
            bundle_client,
            builder_rpc_url,
            rand_seed,
            wallet_map,
            agent_store,
            chain_id,
            nonces,
            gas_limits,
            msg_handle,
        })
    }

    pub async fn sync_nonces(&mut self) -> Result<()> {
        let all_addrs = self.wallet_map.keys().copied().collect::<Vec<Address>>();
        for addr in &all_addrs {
            let nonce = self
                .rpc_client
                .get_transaction_count(*addr)
                .await
                .map_err(|e| ContenderError::with_err(e, "failed to retrieve nonce from RPC"))?;
            self.nonces.insert(*addr, nonce);
        }
        Ok(())
    }

    pub async fn deploy_contracts(&mut self) -> Result<()> {
        let pub_provider = &self.rpc_client;
        let gas_price = pub_provider
            .get_gas_price()
            .await
            .map_err(|e| ContenderError::with_err(e, "failed to get gas price"))?;
        let chain_id = pub_provider
            .get_chain_id()
            .await
            .map_err(|e| ContenderError::with_err(e, "failed to get chain id"))?;

        // we do everything in the callback so no need to actually capture the returned txs
        self.load_txs(PlanType::Create(|tx_req| {
            /* callback */
            // copy data/refs from self before spawning the task
            let db = self.db.clone();
            let from = tx_req.tx.from.as_ref().ok_or(ContenderError::SetupError(
                "failed to get 'from' address",
                None,
            ))?;
            let wallet_conf = self
                .wallet_map
                .get(from)
                .unwrap_or_else(|| panic!("couldn't find wallet for 'from' address {}", from))
                .to_owned();
            let wallet = ProviderBuilder::new()
                // simple_nonce_management is unperformant but it's OK bc we're just deploying
                .with_simple_nonce_management()
                .wallet(wallet_conf)
                .on_http(self.rpc_url.to_owned());

            println!(
                "deploying contract: {:?}",
                tx_req.name.as_ref().unwrap_or(&"".to_string())
            );
            let handle = tokio::task::spawn(async move {
                // estimate gas limit
                let gas_limit = wallet
                    .estimate_gas(&tx_req.tx)
                    .await
                    .expect("failed to estimate gas");

                // inject missing fields into tx_req.tx
                let tx = tx_req
                    .tx
                    .with_gas_price(gas_price)
                    .with_chain_id(chain_id)
                    .with_gas_limit(gas_limit);

                let res = wallet
                    .send_transaction(tx)
                    .await
                    .expect("failed to send tx");
                let receipt = res.get_receipt().await.expect("failed to get receipt");
                println!(
                    "contract address: {}",
                    receipt.contract_address.unwrap_or_default()
                );
                db.insert_named_txs(
                    NamedTx::new(
                        tx_req.name.unwrap_or_default(),
                        receipt.transaction_hash,
                        receipt.contract_address,
                    )
                    .into(),
                )
                .expect("failed to insert tx into db");
            });
            Ok(Some(handle))
        }))
        .await?;

        self.sync_nonces().await?;

        Ok(())
    }

    pub async fn run_setup(&mut self) -> Result<()> {
        self.load_txs(PlanType::Setup(|tx_req| {
            /* callback */
            println!("{}", self.format_setup_log(&tx_req));

            // copy data/refs from self before spawning the task
            let from = tx_req.tx.from.as_ref().ok_or(ContenderError::SetupError(
                "failed to get 'from' address",
                None,
            ))?;
            let wallet = self
                .wallet_map
                .get(from)
                .ok_or(ContenderError::SetupError(
                    "couldn't find private key for address",
                    from.encode_hex().into(),
                ))?
                .to_owned();
            let db = self.db.clone();
            let rpc_url = self.rpc_url.clone();
            let handle = tokio::task::spawn(async move {
                let wallet = ProviderBuilder::new()
                    .with_simple_nonce_management()
                    .wallet(wallet)
                    .on_http(rpc_url);

                let chain_id = wallet.get_chain_id().await.expect("failed to get chain id");
                let gas_price = wallet
                    .get_gas_price()
                    .await
                    .expect("failed to get gas price");
                let gas_limit = wallet
                    .estimate_gas(&tx_req.tx)
                    .await
                    .expect("failed to estimate gas");
                let tx = tx_req
                    .tx
                    .with_gas_price(gas_price)
                    .with_chain_id(chain_id)
                    .with_gas_limit(gas_limit);
                let res = wallet
                    .send_transaction(tx)
                    .await
                    .expect("failed to send tx");

                // get receipt using provider (not wallet) to allow any receipt type (support non-eth chains)
                let receipt = res.get_receipt().await.expect("failed to get receipt");
                if let Some(name) = tx_req.name {
                    db.insert_named_txs(
                        NamedTx::new(name, receipt.transaction_hash, receipt.contract_address)
                            .into(),
                    )
                    .expect("failed to insert tx into db");
                }
            });
            Ok(Some(handle))
        }))
        .await?;

        self.sync_nonces().await?;

        Ok(())
    }

    pub async fn prepare_tx_request(
        &mut self,
        tx_req: &TransactionRequest,
        gas_price: u128,
    ) -> Result<(TransactionRequest, EthereumWallet)> {
        let from = tx_req.from.ok_or(ContenderError::SetupError(
            "missing 'from' address in tx request",
            None,
        ))?;
        let nonce = self
            .nonces
            .get(&from)
            .ok_or(ContenderError::SetupError(
                "missing nonce for 'from' address",
                Some(from.to_string()),
            ))?
            .to_owned();
        self.nonces.insert(from.to_owned(), nonce + 1);
        let fn_sig = FixedBytes::<4>::from_slice(
            tx_req
                .input
                .input
                .to_owned()
                .map(|b| b.split_at(4).0.to_owned())
                .ok_or(ContenderError::SetupError(
                    "invalid function call",
                    Some(format!("{:?}", tx_req.input.input)),
                ))?
                .as_slice(),
        );
        if !self.gas_limits.contains_key(fn_sig.as_slice()) {
            let gas_limit = self
                .eth_client
                .estimate_gas(tx_req)
                .await
                .map_err(|e| ContenderError::with_err(e, "failed to estimate gas for tx"))?;
            self.gas_limits.insert(fn_sig, gas_limit);
        }
        let gas_limit = self
            .gas_limits
            .get(&fn_sig)
            .ok_or(ContenderError::SetupError(
                "failed to lookup gas limit",
                None,
            ))?
            .to_owned();
        let signer = self
            .wallet_map
            .get(&from)
            .ok_or(ContenderError::SetupError(
                "failed to get signer from scenario wallet_map",
                None,
            ))?
            .to_owned();
        let full_tx = tx_req
            .to_owned()
            .with_nonce(nonce)
            .with_max_fee_per_gas(gas_price + (gas_price / 5))
            .with_max_priority_fee_per_gas(gas_price)
            .with_chain_id(self.chain_id)
            .with_gas_limit(gas_limit + (gas_limit / 6));

        Ok((full_tx, signer))
    }

    pub async fn prepare_spam(
        &mut self,
        tx_requests: &[ExecutionRequest],
    ) -> Result<Vec<ExecutionPayload>> {
        let gas_price = self
            .rpc_client
            .get_gas_price()
            .await
            .map_err(|e| ContenderError::with_err(e, "failed to get gas price"))?;
        let mut payloads = vec![];
        for tx in tx_requests {
            let payload = match tx {
                ExecutionRequest::Bundle(reqs) => {
                    if self.bundle_client.is_none() {
                        return Err(ContenderError::SpamError(
                            "Bundle client not found. Specify a builder url to send bundles.",
                            None,
                        ));
                    }

                    // prepare each tx in the bundle (increment nonce, set gas price, etc)
                    let mut bundle_txs = vec![];

                    for req in reqs {
                        let tx_req = req.tx.to_owned();
                        let (tx_req, signer) = self
                            .prepare_tx_request(&tx_req, gas_price)
                            .await
                            .map_err(|e| ContenderError::with_err(e, "failed to prepare tx"))?;

                        println!("bundle tx from {:?}", tx_req.from);
                        // sign tx
                        let tx_envelope = tx_req.build(&signer).await.map_err(|e| {
                            ContenderError::with_err(e, "bad request: failed to build tx")
                        })?;

                        bundle_txs.push(tx_envelope);
                    }
                    ExecutionPayload::SignedTxBundle(bundle_txs, reqs.to_owned())
                }
                ExecutionRequest::Tx(req) => {
                    let tx_req = req.tx.to_owned();

                    let (tx_req, signer) = self
                        .prepare_tx_request(&tx_req, gas_price)
                        .await
                        .map_err(|e| ContenderError::with_err(e, "failed to prepare tx"))?;

                    // sign tx
                    let tx_envelope = tx_req.to_owned().build(&signer).await.map_err(|e| {
                        ContenderError::with_err(e, "bad request: failed to build tx")
                    })?;

                    println!(
                        "sending tx {} from={} to={:?} input={} value={}",
                        tx_envelope.tx_hash(),
                        tx_req.from.map(|s| s.encode_hex()).unwrap_or_default(),
                        tx_envelope.to().to(),
                        tx_req
                            .input
                            .input
                            .as_ref()
                            .map(|s| s.encode_hex())
                            .unwrap_or_default(),
                        tx_req
                            .value
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "0".to_owned())
                    );

                    ExecutionPayload::SignedTx(tx_envelope, req.to_owned())
                }
            };
            payloads.push(payload);
        }
        Ok(payloads)
    }

    pub async fn execute_spam(
        &mut self,
        trigger: SpamTrigger,
        payloads: &[ExecutionPayload],
        callback_handler: Arc<impl OnTxSent + Send + Sync + 'static>,
    ) -> Result<Vec<tokio::task::JoinHandle<()>>> {
        let payloads = payloads.to_owned();

        let mut tasks: Vec<tokio::task::JoinHandle<()>> = vec![];

        for payload in payloads {
            let rpc_client = self.rpc_client.clone();
            let bundle_client = self.bundle_client.clone();
            let callback_handler = callback_handler.clone();
            let tx_handler = self.msg_handle.clone();

            tasks.push(tokio::task::spawn(async move {
                let mut extra = HashMap::new();
                let start_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("time went backwards")
                    .as_millis();
                extra.insert("start_timestamp".to_owned(), start_timestamp.to_string());
                let handles = match payload.to_owned() {
                    ExecutionPayload::SignedTx(signed_tx, req) => {
                        let res = rpc_client
                            .send_tx_envelope(signed_tx.to_owned())
                            .await
                            .expect("failed to send tx envelope");
                        let maybe_handle = callback_handler.on_tx_sent(
                            res.into_inner(),
                            &req,
                            Some(extra),
                            Some(tx_handler.clone()),
                        );
                        vec![maybe_handle]
                    }
                    ExecutionPayload::SignedTxBundle(signed_txs, reqs) => {
                        let mut bundle_txs = vec![];
                        for tx in &signed_txs {
                            let mut raw_tx = vec![];
                            tx.encode_2718(&mut raw_tx);
                            bundle_txs.push(raw_tx);
                        }
                        let block_num = match trigger {
                            SpamTrigger::BlockNumber(n) => n,
                            SpamTrigger::BlockHash(h) => {
                                let block = rpc_client
                                    .get_block_by_hash(
                                        h,
                                        alloy::rpc::types::BlockTransactionsKind::Hashes,
                                    )
                                    .await
                                    .expect("failed to get block")
                                    .expect("block not found");
                                block.header.number
                            }
                            _ => rpc_client
                                .get_block_number()
                                .await
                                .expect("failed to get block number"),
                        };
                        let rpc_bundle = EthSendBundle::new_basic(
                            bundle_txs.into_iter().map(|b| b.into()).collect(),
                            block_num,
                        );
                        if let Some(bundle_client) = bundle_client {
                            println!("spamming bundle: {:?}", rpc_bundle);
                            for i in 1..4 {
                                let mut rpc_bundle = rpc_bundle.clone();
                                rpc_bundle.block_number = block_num + i as u64;

                                let res = rpc_bundle.send_to_builder(&bundle_client).await;
                                if let Err(e) = res {
                                    eprintln!("failed to send bundle: {:?}", e);
                                }
                            }
                        } else {
                            panic!("bundle client not found");
                        }

                        let mut tx_handles = vec![];
                        for (tx, req) in signed_txs.into_iter().zip(reqs) {
                            let maybe_handle = callback_handler.on_tx_sent(
                                PendingTransactionConfig::new(*tx.tx_hash()),
                                &req,
                                Some(extra.clone()),
                                Some(tx_handler.clone()),
                            );
                            tx_handles.push(maybe_handle);
                        }
                        tx_handles
                    }
                };

                for handle in handles.into_iter().flatten() {
                    // ignore None values so we don't attempt to await them
                    handle.await.expect("msg handle failed");
                }
            }));
        }

        Ok(tasks)
    }

    fn format_setup_log(&self, tx_req: &NamedTxRequest) -> String {
        let to_address = tx_req.tx.to.unwrap_or_default();
        let to_address = to_address.to();

        // lookup name of contract if it exists
        let to_name = to_address.map(|a| {
            let named_tx = self.db.get_named_tx_by_address(a);
            named_tx
                .map(|t| t.map(|tt| tt.name).unwrap_or_default())
                .unwrap_or_default()
        });

        format!(
            "running setup: from={} to={} {}",
            tx_req
                .tx
                .from
                .as_ref()
                .map(|a| a.encode_hex())
                .unwrap_or_default(),
            if let Some(to) = to_name {
                to
            } else {
                to_address.map(|a| a.encode_hex()).unwrap_or_default()
            },
            if let Some(kind) = &tx_req.kind {
                format!("kind={}", kind)
            } else {
                "".to_string()
            },
        )
    }
}

impl<D, S, P> Generator<String, D, P> for TestScenario<D, S, P>
where
    D: DbOps + Send + Sync,
    S: Seeder,
    P: PlanConfig<String> + Templater<String> + Send + Sync,
{
    fn get_db(&self) -> &D {
        self.db.as_ref()
    }

    fn get_templater(&self) -> &P {
        &self.config
    }

    fn get_plan_conf(&self) -> &impl PlanConfig<String> {
        &self.config
    }

    fn get_fuzz_seeder(&self) -> &impl Seeder {
        &self.rand_seed
    }

    fn get_agent_store(&self) -> &AgentStore {
        &self.agent_store
    }
}

#[cfg(test)]
pub mod tests {
    use crate::agent_controller::AgentStore;
    use crate::db::MockDb;
    use crate::generator::templater::Templater;
    use crate::generator::types::{
        CreateDefinition, FunctionCallDefinition, FuzzParam, SpamRequest,
    };
    use crate::generator::{types::PlanType, util::test::spawn_anvil, RandSeed};
    use crate::generator::{Generator, PlanConfig};
    use crate::spammer::util::test::get_test_signers;
    use crate::test_scenario::TestScenario;
    use crate::Result;
    use alloy::hex::ToHexExt;
    use alloy::node_bindings::AnvilInstance;
    use alloy::primitives::Address;
    use std::collections::HashMap;

    pub struct MockConfig;

    pub const COUNTER_BYTECODE: &str =
        "0x608060405234801561001057600080fd5b5060f78061001f6000396000f3fe6080604052348015600f57600080fd5b5060043610603c5760003560e01c80633fb5c1cb1460415780638381f58a146053578063d09de08a14606d575b600080fd5b6051604c3660046083565b600055565b005b605b60005481565b60405190815260200160405180910390f35b6051600080549080607c83609b565b9190505550565b600060208284031215609457600080fd5b5035919050565b60006001820160ba57634e487b7160e01b600052601160045260246000fd5b506001019056fea264697066735822122010f3077836fb83a22ad708a23102f2b487523767e1afef5a93c614619001648b64736f6c63430008170033";

    impl PlanConfig<String> for MockConfig {
        fn get_env(&self) -> Result<HashMap<String, String>> {
            Ok(HashMap::<String, String>::from_iter([
                ("test1".to_owned(), "0xbeef".to_owned()),
                ("test2".to_owned(), "0x9001".to_owned()),
            ]))
        }

        fn get_create_steps(&self) -> Result<Vec<CreateDefinition>> {
            Ok(vec![CreateDefinition {
                bytecode: COUNTER_BYTECODE.to_string(),
                name: "test_counter".to_string(),
                from: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_owned(),
            }])
        }

        fn get_setup_steps(&self) -> Result<Vec<FunctionCallDefinition>> {
            Ok(vec![
                FunctionCallDefinition {
                    to: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".to_owned(),
                    from: Some("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_owned()),
                    from_pool: None,
                    value: Some("4096".to_owned()),
                    signature: "swap(uint256 x, uint256 y, address a, bytes b)".to_owned(),
                    args: vec![
                        "1".to_owned(),
                        "2".to_owned(),
                        Address::repeat_byte(0x11).encode_hex(),
                        "0xdead".to_owned(),
                    ]
                    .into(),
                    fuzz: None,
                    kind: None,
                },
                FunctionCallDefinition {
                    to: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".to_owned(),
                    from: Some("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_owned()),
                    from_pool: None,
                    value: Some("0x1000".to_owned()),
                    signature: "swap(uint256 x, uint256 y, address a, bytes b)".to_owned(),
                    args: vec![
                        "1".to_owned(),
                        "2".to_owned(),
                        Address::repeat_byte(0x11).encode_hex(),
                        "0xbeef".to_owned(),
                    ]
                    .into(),
                    fuzz: None,
                    kind: None,
                },
            ])
        }

        fn get_spam_steps(&self) -> Result<Vec<SpamRequest>> {
            let fn_call = |data: &str, from_addr: &str| {
                SpamRequest::Tx(FunctionCallDefinition {
                    to: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".to_owned(),
                    from: Some(from_addr.to_owned()),
                    from_pool: None,
                    value: None,
                    signature: "swap(uint256 x, uint256 y, address a, bytes b)".to_owned(),
                    args: vec![
                        "1".to_owned(),
                        "2".to_owned(),
                        Address::repeat_byte(0x11).encode_hex(),
                        data.to_owned(),
                    ]
                    .into(),
                    fuzz: vec![FuzzParam {
                        param: Some("x".to_string()),
                        value: None,
                        min: None,
                        max: None,
                    }]
                    .into(),
                    kind: None,
                })
            };
            Ok(vec![
                fn_call("0xbeef", "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"),
                fn_call("0xea75", "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"),
                fn_call("0xf00d", "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"),
            ])
        }
    }

    impl Templater<String> for MockConfig {
        fn copy_end(&self, input: &str, _last_end: usize) -> String {
            input.to_owned()
        }
        fn replace_placeholders(
            &self,
            input: &str,
            _placeholder_map: &std::collections::HashMap<String, String>,
        ) -> String {
            input.to_owned()
        }
        fn terminator_start(&self, _input: &str) -> Option<usize> {
            None
        }
        fn terminator_end(&self, _input: &str) -> Option<usize> {
            None
        }
        fn num_placeholders(&self, _input: &str) -> usize {
            0
        }
        fn find_key(&self, _input: &str) -> Option<(String, usize)> {
            None
        }
        fn encode_contract_address(&self, input: &Address) -> String {
            input.encode_hex()
        }
    }

    pub async fn get_test_scenario(
        anvil: &AnvilInstance,
    ) -> TestScenario<MockDb, RandSeed, MockConfig> {
        let seed = RandSeed::seed_from_bytes(&[0x01; 32]);
        let signers = &get_test_signers();

        TestScenario::new(
            MockConfig,
            MockDb.into(),
            anvil.endpoint_url(),
            None,
            seed.to_owned(),
            signers,
            AgentStore::new(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn it_creates_scenarios() {
        let anvil = spawn_anvil();
        let scenario = get_test_scenario(&anvil).await;

        let create_txs = scenario
            .load_txs(PlanType::Create(|tx| {
                println!("create tx callback triggered! {:?}\n", tx);
                Ok(None)
            }))
            .await
            .unwrap();
        assert_eq!(create_txs.len(), 1);

        let setup_txs = scenario
            .load_txs(PlanType::Setup(|tx| {
                println!("setup tx callback triggered! {:?}\n", tx);
                Ok(None)
            }))
            .await
            .unwrap();
        assert_eq!(setup_txs.len(), 2);

        let spam_txs = scenario
            .load_txs(PlanType::Spam(20, |tx| {
                println!("spam tx callback triggered! {:?}\n", tx);
                Ok(None)
            }))
            .await
            .unwrap();
        assert!(spam_txs.len() >= 20);
    }

    #[tokio::test]
    async fn scenario_creates_contracts() {
        let anvil = spawn_anvil();
        let mut scenario = get_test_scenario(&anvil).await;
        let res = scenario.deploy_contracts().await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn scenario_runs_setup() {
        let anvil = spawn_anvil();
        let mut scenario = get_test_scenario(&anvil).await;
        scenario.deploy_contracts().await.unwrap();
        let res = scenario.run_setup().await;
        println!("{:?}", res);
        assert!(res.is_ok());
    }
}
