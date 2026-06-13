use std::collections::{HashMap, VecDeque};
use std::fs;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use bdk_bitcoind_rpc::bitcoincore_rpc::RpcApi;
use bdk_wallet::keys::bip39::{Language as BdkLanguage, Mnemonic as BdkMnemonic};
use bdk_wallet::KeychainKind;
use bdk_wallet::SignOptions;
use bitcoin::absolute::LockTime;
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::{sha256, Hash, HashEngine};
use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
use bitcoin::{Amount, FeeRate, Network, OutPoint, ScriptBuf, Transaction, Txid};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use lightning::chain::chaininterface::{BroadcasterInterface, ConfirmationTarget, FeeEstimator};
use lightning::chain::chainmonitor::ChainMonitor;
use lightning::chain::channelmonitor::{Balance, BalanceSource};
use lightning::chain::BestBlock;
use lightning::chain::{Confirm, Filter, Watch, WatchedOutput};
use lightning::events::{Event, EventsProvider};
use lightning::ln::channelmanager::{
    ChainParameters, ChannelManagerReadArgs, PaymentId, RecipientOnionFields,
    SimpleArcChannelManager,
};
use lightning::ln::msgs::SocketAddress;
use lightning::ln::peer_handler::{IgnoringMessageHandler, MessageHandler, PeerManager};
use lightning::ln::types::ChannelId as LnRgbChannelId;
use lightning::onion_message::messenger::DefaultMessageRouter;
use lightning::rgb::{
    RgbAssetAmount as LdkRgbAssetAmount, RgbChannelContext, RgbFundingRef,
    RgbFundingTransfer as LdkRgbFundingTransfer, RgbPaymentMetadata,
};
use lightning::routing::gossip::NetworkGraph;
use lightning::routing::router::{
    DefaultRouter, PaymentParameters, RouteParameters, RouteParametersConfig,
};
use lightning::routing::scoring::{
    ProbabilisticScorer, ProbabilisticScoringDecayParameters, ProbabilisticScoringFeeParameters,
};
use lightning::sign::{ChangeDestinationSourceSync, SpendableOutputDescriptor};
use lightning::sign::{InMemorySigner, KeysManager, NodeSigner, Recipient};
use lightning::types::payment::{PaymentHash, PaymentPreimage};
use lightning::util::config::UserConfig;
use lightning::util::logger::{Logger, Record};
use lightning::util::persist::{
    read_channel_monitors, KVStoreSync, MonitorUpdatingPersister, CHANNEL_MANAGER_PERSISTENCE_KEY,
    CHANNEL_MANAGER_PERSISTENCE_PRIMARY_NAMESPACE, CHANNEL_MANAGER_PERSISTENCE_SECONDARY_NAMESPACE,
    OUTPUT_SWEEPER_PERSISTENCE_KEY, OUTPUT_SWEEPER_PERSISTENCE_PRIMARY_NAMESPACE,
    OUTPUT_SWEEPER_PERSISTENCE_SECONDARY_NAMESPACE,
};
use lightning::util::ser::{Readable, ReadableArgs, Writeable};
use lightning::util::sweep::{OutputSpendStatus, OutputSweeperSync, TrackedSpendableOutput};
use lightning_invoice::{Bolt11Invoice, Currency, InvoiceBuilder};
use lightning_net_tokio::SocketDescriptor;
use lightning_persister::fs_store::FilesystemStore;
use lightning_transaction_sync::EsploraSyncClient;
use rgbstd::containers::{ConsignmentExt, ValidTransfer};
use rgbstd::ContractId;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use crate::btc_ln::{
    BtcLnBalanceSnapshot, BtcLnBolt11InvoiceRequest, BtcLnBolt11PaymentRequest,
    BtcLnChannelCloseRequest, BtcLnChannelOpenRequest, BtcLnChannelSnapshot, BtcLnEvent,
    BtcLnKeysendRequest, BtcLnNode, BtcLnPeerSnapshot, BtcLnRuntimeConfig,
};
use crate::iroh_transport::{
    bind_iroh_endpoint_from_mnemonic, connect_iroh_tcp_tunnel, handle_iroh_health_connection,
    probe_iroh_health, receive_assignment_connection, run_iroh_tcp_tunnel_connection,
    send_assignment_with_retry, send_rgb20_transfer_assignment_with_retry, IrohAssignment,
    IrohAssignmentAck, RGB_ASSIGNMENT_ALPN, RGB_LN_HEALTH_ALPN, RGB_LN_TCP_TUNNEL_ALPN,
};
use crate::lnnode::{
    ChannelId, RgbAssetAmount, RgbChannelOpenRequest, RgbFundingTransfer, RgbLnNode,
    RgbPaymentRequest,
};
use crate::local_wallet::{
    bitcoin_core_chain_tip, bitcoin_core_tx_status, broadcast_transaction,
    esplora_client_with_config, sync_wallet, ChainSource, EsploraConfig, LocalWallet,
};
use crate::rgb20::{
    self, build_rgb20_channel_funding_cached_first, default_rgb_stock_dir,
    promote_staged_rgb_stock_if_tx_confirmed_with_esploras,
    scan_and_promote_or_revoke_staged_rgb_stocks_with_esploras, stage_rgb_stock_for_tx,
    Rgb20ChannelFundingRequest, Rgb20TransferRequest,
};
use crate::rgb_ln::RgbLnFundingTransferSource;

pub struct LnRgbBtcLnBackend {
    config: BtcLnRuntimeConfig,
    seed: [u8; 32],
    node_secret: SecretKey,
    node_id: PublicKey,
    started: AtomicBool,
    invoice_counter: std::sync::atomic::AtomicU64,
    runtime: Mutex<Option<LnRgbRuntime>>,
    self_weak: Mutex<Option<Weak<LnRgbBtcLnBackend>>>,
    peers: Mutex<HashMap<PublicKey, BtcLnPeerSnapshot>>,
    events: Mutex<VecDeque<String>>,
    btc_events: Mutex<VecDeque<BtcLnEvent>>,
    rgb_channel_assets: Mutex<HashMap<u128, RgbAssetAmount>>,
    rgb_payment_assets: Mutex<HashMap<String, RgbAssetAmount>>,
    pending_funding_transactions: Mutex<HashMap<LnRgbChannelId, PendingFundingTransaction>>,
    pending_rgb_funding: Mutex<HashMap<LnRgbChannelId, PendingRgbFundingTransfer>>,
    generated_rgb_funding_transfers: Mutex<VecDeque<RgbFundingTransfer>>,
    rgb_funding_bindings: Mutex<HashMap<LnRgbChannelId, RgbFundingOutpointBinding>>,
    esplora_cursor: AtomicUsize,
}

pub const LN_RGB_LIGHTNING_VERSION: &str = "0.2.2";
const LDK_CHAIN_SYNC_SUCCESS_INTERVAL: Duration = Duration::from_secs(120);
const LDK_CHAIN_SYNC_INITIAL_FAILURE_INTERVAL: Duration = Duration::from_secs(60);
const LDK_CHAIN_SYNC_MAX_FAILURE_INTERVAL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Debug, Eq, PartialEq)]
struct LnRgbChannelOpenRequest {
    peer_node_id: PublicKey,
    capacity_sat: u64,
    push_msat: u64,
    user_channel_id: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LnRgbKeysendRequest {
    recipient_node_id: PublicKey,
    amount_msat: u64,
    payment_id: PaymentId,
}

type LnRgbPeerManager = PeerManager<
    SocketDescriptor,
    Arc<LnRgbChannelManager>,
    Arc<IgnoringMessageHandler>,
    Arc<IgnoringMessageHandler>,
    Arc<LnRgbLogger>,
    Arc<IgnoringMessageHandler>,
    Arc<KeysManager>,
    Arc<IgnoringMessageHandler>,
>;

type LnRgbPersister = MonitorUpdatingPersister<
    Arc<FilesystemStore>,
    Arc<LnRgbLogger>,
    Arc<KeysManager>,
    Arc<KeysManager>,
    Arc<LnRgbBroadcaster>,
    Arc<LnRgbFeeEstimator>,
>;

type LnRgbChainMonitor = ChainMonitor<
    InMemorySigner,
    Arc<LnRgbTxSync>,
    Arc<LnRgbBroadcaster>,
    Arc<LnRgbFeeEstimator>,
    Arc<LnRgbLogger>,
    Arc<LnRgbPersister>,
    Arc<KeysManager>,
>;

type LnRgbChannelManager =
    SimpleArcChannelManager<LnRgbChainMonitor, LnRgbBroadcaster, LnRgbFeeEstimator, LnRgbLogger>;

type LnRgbTxSync = LnRgbChainSync;
type LnRgbOutputSweeper = OutputSweeperSync<
    Arc<LnRgbBroadcaster>,
    Arc<LnRgbChangeDestinationSource>,
    Arc<LnRgbFeeEstimator>,
    Arc<LnRgbTxSync>,
    Arc<FilesystemStore>,
    Arc<LnRgbLogger>,
    Arc<KeysManager>,
>;

struct LnRgbRuntime {
    rt: Runtime,
    channel_manager: Arc<LnRgbChannelManager>,
    _chain_monitor: Arc<LnRgbChainMonitor>,
    _tx_sync: Arc<LnRgbTxSync>,
    output_sweeper: Arc<LnRgbOutputSweeper>,
    kv_store: Arc<FilesystemStore>,
    peer_manager: Arc<LnRgbPeerManager>,
    iroh_endpoint: Endpoint,
    listener_stop: Arc<AtomicBool>,
    listener_handle: Option<JoinHandle<()>>,
    iroh_listener_handle: Option<JoinHandle<()>>,
    iroh_tunnel_handles: Vec<JoinHandle<()>>,
    sync_handle: Option<JoinHandle<()>>,
    event_pump_handle: Option<JoinHandle<()>>,
    peer_maintenance_handle: Option<JoinHandle<()>>,
    peer_task_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    _keys_manager: Arc<KeysManager>,
    _logger: Arc<LnRgbLogger>,
}

#[derive(Clone)]
struct PendingRgbFundingTransfer {
    peer_node_id: PublicKey,
    transfer: ValidTransfer,
}

#[derive(Clone)]
struct PendingFundingTransaction {
    peer_node_id: PublicKey,
    transaction: Transaction,
    funding_outpoint: OutPoint,
    user_channel_id: u128,
    staged_stock_dir: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PendingFundingTransactionRecord {
    temporary_channel_id: String,
    peer_node_id: String,
    transaction_hex: String,
    funding_outpoint: String,
    user_channel_id: u128,
    staged_stock_dir: String,
    created_at: u64,
    updated_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PendingRgbFundingTransferRecord {
    temporary_channel_id: String,
    peer_node_id: String,
    transfer_path: String,
    created_at: u64,
    updated_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GeneratedRgbFundingTransferRecord {
    temporary_channel_id: String,
    peer_node_id: String,
    funding_outpoint: String,
    transfer_path: String,
    created_at: u64,
    updated_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbFundingOutpointBinding {
    pub temporary_channel_id: String,
    #[serde(default)]
    pub channel_id: Option<String>,
    pub peer_node_id: String,
    pub funding_outpoint: String,
    pub transfer_path: String,
    #[serde(default)]
    pub staged_stock_dir: String,
    #[serde(default = "default_rgb_funding_status")]
    pub status: String,
    #[serde(default)]
    pub promoted_at: Option<u64>,
    pub created_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbPendingSweepRecord {
    pub channel_id: Option<String>,
    pub temporary_channel_id: Option<String>,
    pub funding_outpoint: String,
    pub spendable_outpoint: String,
    #[serde(default)]
    pub carrier_txid: String,
    pub descriptor_kind: String,
    pub descriptor_path: String,
    pub transfer_path: String,
    #[serde(default)]
    pub staged_stock_dir: String,
    pub status: String,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbPendingMaturityRecord {
    pub channel_id: String,
    pub temporary_channel_id: Option<String>,
    pub funding_outpoint: String,
    pub amount_satoshis: u64,
    pub spendable_height: u32,
    pub source: String,
    pub status: String,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbPaymentStateRecord {
    pub direction: String,
    #[serde(default)]
    pub payment_id: Option<String>,
    #[serde(default)]
    pub payment_hash: Option<String>,
    #[serde(default)]
    pub peer_node_id: Option<String>,
    pub amount_msat: u64,
    pub contract_id: String,
    pub rgb_amount: u64,
    pub status: String,
    pub created_at: u64,
    pub updated_at: u64,
}

struct LnRgbChangeDestinationSource {
    network: Network,
    l1_data_dir: PathBuf,
    mnemonic: BdkMnemonic,
}

impl ChangeDestinationSourceSync for LnRgbChangeDestinationSource {
    fn get_change_destination_script(&self) -> std::result::Result<ScriptBuf, ()> {
        let mut wallet =
            LocalWallet::open_with_mnemonic(&self.l1_data_dir, self.network, &self.mnemonic)
                .map_err(|err| {
                    eprintln!("[ln-rgb] failed to open sweep destination wallet: {err:#}");
                })?;
        let address = wallet.wallet.reveal_next_address(KeychainKind::Internal);
        let script_pubkey = address.address.script_pubkey();
        wallet.persist().map_err(|err| {
            eprintln!("[ln-rgb] failed to persist sweep destination wallet: {err:#}");
        })?;
        Ok(script_pubkey)
    }
}

#[derive(Debug)]
struct LnRgbLogger;

impl Logger for LnRgbLogger {
    fn log(&self, record: Record) {
        if std::env::var_os("BTC_LOCAL_WALLET_ZS_QUIET").is_some() {
            return;
        }
        let message = record.args.to_string();
        if std::env::var_os("BTC_LOCAL_WALLET_VERBOSE_LDK").is_none()
            && should_suppress_ldk_log(record.module_path, &message)
        {
            return;
        }
        eprintln!(
            "[ln-rgb:{}:{}] {}",
            record.module_path, record.line, message
        );
    }
}

fn should_suppress_ldk_log(module_path: &str, message: &str) -> bool {
    if module_path == "lightning::ln::peer_handler"
        && (message.starts_with("Received message ChannelUpdate")
            || message.starts_with("Received message ChannelAnnouncement")
            || message.starts_with("Received message NodeAnnouncement"))
    {
        return true;
    }

    if module_path == "lightning_transaction_sync::esplora"
        && (message.starts_with("Starting transaction sync")
            || message.starts_with("Finished transaction sync at tip"))
    {
        return true;
    }

    false
}

enum LnRgbChainSync {
    Esplora(EsploraSyncClient<Arc<LnRgbLogger>>),
    BitcoinCore(BitcoinCoreTxSync),
}

impl LnRgbChainSync {
    async fn sync(&self, confirmables: Vec<&(dyn Confirm + Sync + Send)>) -> Result<()> {
        match self {
            Self::Esplora(client) => client
                .sync(confirmables)
                .map_err(|err| anyhow!("LDK Esplora transaction sync failed: {err:?}")),
            Self::BitcoinCore(sync) => sync.sync(confirmables),
        }
    }
}

impl Filter for LnRgbChainSync {
    fn register_tx(&self, txid: &Txid, script_pubkey: &bitcoin::Script) {
        match self {
            Self::Esplora(client) => client.register_tx(txid, script_pubkey),
            Self::BitcoinCore(sync) => sync.register_tx(txid, script_pubkey),
        }
    }

    fn register_output(&self, output: WatchedOutput) {
        match self {
            Self::Esplora(client) => client.register_output(output),
            Self::BitcoinCore(sync) => sync.register_output(output),
        }
    }
}

struct BitcoinCoreTxSync {
    config: crate::local_wallet::BitcoinCoreConfig,
    watched_txs: Mutex<HashMap<Txid, ScriptBuf>>,
    watched_outputs: Mutex<HashMap<OutPoint, WatchedOutput>>,
}

impl BitcoinCoreTxSync {
    fn new(config: crate::local_wallet::BitcoinCoreConfig) -> Self {
        Self {
            config,
            watched_txs: Mutex::new(HashMap::new()),
            watched_outputs: Mutex::new(HashMap::new()),
        }
    }

    fn sync(&self, confirmables: Vec<&(dyn Confirm + Sync + Send)>) -> Result<()> {
        let client = self.config.client()?;
        let tip = bitcoin_core_chain_tip(&self.config)?;
        let tip_header = client
            .get_block_header(&tip.hash)
            .with_context(|| format!("fetch Bitcoin Core tip header {}", tip.hash))?;

        let relevant = confirmables
            .iter()
            .flat_map(|confirmable| confirmable.get_relevant_txids())
            .collect::<Vec<_>>();
        for (txid, _height, expected_hash) in relevant {
            match bitcoin_core_tx_status(&self.config, txid)? {
                Some(status) if status.confirmed && status.block_hash == expected_hash => {}
                Some(status) if status.confirmed => {
                    for confirmable in &confirmables {
                        confirmable.transaction_unconfirmed(&txid);
                    }
                    self.confirm_tx(&client, &confirmables, status)?;
                }
                _ => {
                    for confirmable in &confirmables {
                        confirmable.transaction_unconfirmed(&txid);
                    }
                }
            }
        }

        let watched_txids = self
            .watched_txs
            .lock()
            .expect("ln-rgb watched tx lock poisoned")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for txid in watched_txids {
            if let Some(status) = bitcoin_core_tx_status(&self.config, txid)? {
                if status.confirmed {
                    self.confirm_tx(&client, &confirmables, status)?;
                }
            }
        }

        self.confirm_watched_output_spends(&client, &confirmables)?;

        for confirmable in confirmables {
            confirmable.best_block_updated(&tip_header, tip.height);
        }
        Ok(())
    }

    fn confirm_tx(
        &self,
        client: &bdk_bitcoind_rpc::bitcoincore_rpc::Client,
        confirmables: &[&(dyn Confirm + Sync + Send)],
        status: crate::local_wallet::BitcoinCoreTxStatus,
    ) -> Result<()> {
        let Some(block_hash) = status.block_hash else {
            return Ok(());
        };
        let Some(height) = status.height else {
            return Ok(());
        };
        let header = client
            .get_block_header(&block_hash)
            .with_context(|| format!("fetch Bitcoin Core block header {block_hash}"))?;
        let txdata = vec![(0usize, &status.tx)];
        for confirmable in confirmables {
            confirmable.transactions_confirmed(&header, &txdata, height);
        }
        Ok(())
    }

    fn confirm_watched_output_spends(
        &self,
        client: &bdk_bitcoind_rpc::bitcoincore_rpc::Client,
        confirmables: &[&(dyn Confirm + Sync + Send)],
    ) -> Result<()> {
        let watched_outputs = self
            .watched_outputs
            .lock()
            .expect("ln-rgb watched output lock poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if watched_outputs.is_empty() {
            return Ok(());
        }

        let tip = bitcoin_core_chain_tip(&self.config)?;
        let start_height = watched_outputs
            .iter()
            .filter_map(|output| output.block_hash)
            .filter_map(|hash| client.get_block_info(&hash).ok())
            .map(|info| info.height as u32)
            .min()
            .unwrap_or_else(|| {
                tip.height
                    .saturating_sub(self.config.max_block_scan.min(288))
            });
        let watched_outpoints = watched_outputs
            .iter()
            .map(|output| output.outpoint.into_bitcoin_outpoint())
            .collect::<Vec<_>>();
        for height in start_height..=tip.height {
            let block_hash = client
                .get_block_hash(height as u64)
                .with_context(|| format!("fetch Bitcoin Core block hash at {height}"))?;
            let block = client
                .get_block(&block_hash)
                .with_context(|| format!("fetch Bitcoin Core block {block_hash}"))?;
            let matched = block
                .txdata
                .iter()
                .enumerate()
                .filter(|(_, tx)| {
                    tx.input
                        .iter()
                        .any(|input| watched_outpoints.contains(&input.previous_output))
                })
                .collect::<Vec<_>>();
            if matched.is_empty() {
                continue;
            }
            for confirmable in confirmables {
                confirmable.transactions_confirmed(&block.header, &matched, height);
            }
        }
        Ok(())
    }
}

impl Filter for BitcoinCoreTxSync {
    fn register_tx(&self, txid: &Txid, script_pubkey: &bitcoin::Script) {
        self.watched_txs
            .lock()
            .expect("ln-rgb watched tx lock poisoned")
            .insert(*txid, script_pubkey.to_owned());
    }

    fn register_output(&self, output: WatchedOutput) {
        self.watched_outputs
            .lock()
            .expect("ln-rgb watched output lock poisoned")
            .insert(output.outpoint.into_bitcoin_outpoint(), output);
    }
}

#[derive(Debug)]
struct LnRgbBroadcaster {
    network: Network,
    chain_source: ChainSource,
    broadcasted_txs: Mutex<Vec<Txid>>,
}

impl BroadcasterInterface for LnRgbBroadcaster {
    fn broadcast_transactions(&self, txs: &[&Transaction]) {
        for tx in txs {
            let txid = tx.compute_txid();
            match broadcast_transaction(self.network, Some(&self.chain_source), tx) {
                Ok(_) => {
                    self.broadcasted_txs
                        .lock()
                        .expect("ln-rgb broadcast lock poisoned")
                        .push(txid);
                    eprintln!("[ln-rgb] broadcast LDK transaction: {txid}");
                }
                Err(err) => eprintln!("[ln-rgb] failed to broadcast {txid}: {err:#}"),
            }
        }
    }
}

#[derive(Debug)]
struct LnRgbFeeEstimator;

impl FeeEstimator for LnRgbFeeEstimator {
    fn get_est_sat_per_1000_weight(&self, target: ConfirmationTarget) -> u32 {
        match target {
            ConfirmationTarget::MaximumFeeEstimate => 5_000,
            ConfirmationTarget::UrgentOnChainSweep => 2_500,
            ConfirmationTarget::MinAllowedAnchorChannelRemoteFee
            | ConfirmationTarget::MinAllowedNonAnchorChannelRemoteFee
            | ConfirmationTarget::AnchorChannelFee
            | ConfirmationTarget::NonAnchorChannelFee
            | ConfirmationTarget::ChannelCloseMinimum
            | ConfirmationTarget::OutputSpendingFee => 1_000,
        }
    }
}

impl LnRgbBtcLnBackend {
    pub fn new(config: BtcLnRuntimeConfig) -> Self {
        let seed = derive_seed(&config);
        let keys_manager = KeysManager::new(&seed, 0, 0, true);
        let node_secret = keys_manager.get_node_secret_key();
        let node_id = keys_manager
            .get_node_id(Recipient::Node)
            .expect("keys manager must provide node id");
        Self {
            config,
            seed,
            node_secret,
            node_id,
            started: AtomicBool::new(false),
            invoice_counter: std::sync::atomic::AtomicU64::new(0),
            runtime: Mutex::new(None),
            self_weak: Mutex::new(None),
            peers: Mutex::new(HashMap::new()),
            events: Mutex::new(VecDeque::new()),
            btc_events: Mutex::new(VecDeque::new()),
            rgb_channel_assets: Mutex::new(HashMap::new()),
            rgb_payment_assets: Mutex::new(HashMap::new()),
            pending_funding_transactions: Mutex::new(HashMap::new()),
            pending_rgb_funding: Mutex::new(HashMap::new()),
            generated_rgb_funding_transfers: Mutex::new(VecDeque::new()),
            rgb_funding_bindings: Mutex::new(HashMap::new()),
            esplora_cursor: AtomicUsize::new(0),
        }
    }

    pub fn l1_data_dir(&self) -> &Path {
        &self.config.l1_data_dir
    }

    pub fn new_arc(config: BtcLnRuntimeConfig) -> Arc<Self> {
        let backend = Arc::new(Self::new(config));
        *backend
            .self_weak
            .lock()
            .expect("ln-rgb self weak lock poisoned") = Some(Arc::downgrade(&backend));
        backend
    }

    pub fn rgb_funding_bindings(&self) -> Vec<RgbFundingOutpointBinding> {
        self.rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn esplora_urls(&self) -> Vec<String> {
        let urls = if self.config.esplora_urls.is_empty() {
            vec![self.config.esplora.clone()]
        } else {
            self.config.esplora_urls.clone()
        };
        urls.into_iter()
            .map(|url| url.trim().to_string())
            .filter(|url| !url.is_empty())
            .collect()
    }

    fn rotated_esplora_urls(&self) -> Vec<String> {
        let urls = self.esplora_urls();
        if urls.is_empty() {
            return vec![self.config.esplora.clone()];
        }
        let start = self.esplora_cursor.fetch_add(1, Ordering::Relaxed) % urls.len();
        (0..urls.len())
            .map(|offset| urls[(start + offset) % urls.len()].clone())
            .collect()
    }

    fn next_esplora_url(&self) -> String {
        self.rotated_esplora_urls()
            .into_iter()
            .next()
            .unwrap_or_else(|| self.config.esplora.clone())
    }

    fn rotated_chain_sources(&self) -> Result<Vec<ChainSource>> {
        let urls = self.esplora_urls();
        if urls.is_empty() {
            return Ok(vec![ChainSource::from_esplora_or_default(
                self.config.network,
                None,
            )?]);
        }
        let start = self.esplora_cursor.fetch_add(1, Ordering::Relaxed) % urls.len();
        Ok((0..urls.len())
            .map(|offset| {
                ChainSource::Esplora(
                    self.esplora_config(urls[(start + offset) % urls.len()].clone()),
                )
            })
            .collect())
    }

    fn esplora_config(&self, url: String) -> EsploraConfig {
        EsploraConfig::new(url).with_api_key(self.config.esplora_api_key.clone())
    }

    fn load_rgb_funding_bindings_from_disk(&self) -> Result<usize> {
        let binding_dir = self.rgb_funding_binding_dir();
        let Ok(entries) = fs::read_dir(&binding_dir) else {
            return Ok(0);
        };

        let mut loaded = HashMap::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let binding: RgbFundingOutpointBinding = serde_json::from_slice(
                &fs::read(&path)
                    .with_context(|| format!("read RGB funding binding {}", path.display()))?,
            )
            .with_context(|| format!("decode RGB funding binding {}", path.display()))?;
            let key =
                LnRgbChannelId(hex_to_32(&binding.temporary_channel_id).with_context(|| {
                    format!("invalid RGB funding binding id in {}", path.display())
                })?);
            loaded.insert(key, binding);
        }

        let count = loaded.len();
        *self
            .rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned") = loaded;
        Ok(count)
    }

    fn load_pending_funding_transactions_from_disk(&self) -> Result<usize> {
        let dir = self.rgb_pending_funding_transaction_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(0);
        };
        let mut loaded = HashMap::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: PendingFundingTransactionRecord = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("decode pending RGB funding tx {}", path.display()))?;
            let temporary_channel_id =
                LnRgbChannelId(hex_to_32(&record.temporary_channel_id).with_context(|| {
                    format!(
                        "invalid pending RGB funding channel id in {}",
                        path.display()
                    )
                })?);
            let peer_node_id = record
                .peer_node_id
                .parse::<PublicKey>()
                .with_context(|| format!("invalid peer node id in {}", path.display()))?;
            let transaction_bytes = hex_to_bytes(&record.transaction_hex)
                .with_context(|| format!("invalid transaction hex in {}", path.display()))?;
            let transaction = deserialize::<Transaction>(&transaction_bytes)
                .with_context(|| format!("decode pending RGB funding tx {}", path.display()))?;
            let funding_outpoint = record
                .funding_outpoint
                .parse::<OutPoint>()
                .with_context(|| format!("invalid funding outpoint in {}", path.display()))?;
            loaded.insert(
                temporary_channel_id,
                PendingFundingTransaction {
                    peer_node_id,
                    transaction,
                    funding_outpoint,
                    user_channel_id: record.user_channel_id,
                    staged_stock_dir: PathBuf::from(record.staged_stock_dir),
                },
            );
        }
        let count = loaded.len();
        *self
            .pending_funding_transactions
            .lock()
            .expect("pending funding transaction lock poisoned") = loaded;
        Ok(count)
    }

    fn load_pending_rgb_funding_transfers_from_disk(&self) -> Result<usize> {
        let dir = self.rgb_pending_funding_transfer_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(0);
        };
        let mut loaded = HashMap::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: PendingRgbFundingTransferRecord = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| {
                    format!("decode pending RGB funding transfer {}", path.display())
                })?;
            let temporary_channel_id =
                LnRgbChannelId(hex_to_32(&record.temporary_channel_id).with_context(|| {
                    format!(
                        "invalid pending RGB transfer channel id in {}",
                        path.display()
                    )
                })?);
            let peer_node_id = record
                .peer_node_id
                .parse::<PublicKey>()
                .with_context(|| format!("invalid peer node id in {}", path.display()))?;
            let transfer = match read_valid_transfer_file(PathBuf::from(&record.transfer_path)) {
                Ok(transfer) => transfer,
                Err(err) => {
                    self.events
                        .lock()
                        .expect("events lock poisoned")
                        .push_back(format!(
                            "ln-rgb skipped pending RGB funding transfer {}: {err:#}",
                            path.display()
                        ));
                    continue;
                }
            };
            loaded.insert(
                temporary_channel_id,
                PendingRgbFundingTransfer {
                    peer_node_id,
                    transfer,
                },
            );
        }
        let count = loaded.len();
        *self
            .pending_rgb_funding
            .lock()
            .expect("pending rgb funding lock poisoned") = loaded;
        Ok(count)
    }

    fn load_generated_rgb_funding_transfers_from_disk(&self) -> Result<usize> {
        let dir = self.rgb_generated_funding_transfer_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(0);
        };
        let existing_bindings = self
            .rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .values()
            .map(|binding| binding.temporary_channel_id.clone())
            .collect::<std::collections::HashSet<_>>();
        let mut loaded = VecDeque::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: GeneratedRgbFundingTransferRecord =
                serde_json::from_slice(&fs::read(&path)?).with_context(|| {
                    format!("decode generated RGB funding transfer {}", path.display())
                })?;
            if existing_bindings.contains(&record.temporary_channel_id) {
                continue;
            }
            let temporary_channel_id =
                ChannelId(hex_to_32(&record.temporary_channel_id).with_context(|| {
                    format!(
                        "invalid generated RGB transfer channel id in {}",
                        path.display()
                    )
                })?);
            let peer_node_id = record
                .peer_node_id
                .parse::<PublicKey>()
                .with_context(|| format!("invalid peer node id in {}", path.display()))?;
            let funding_outpoint = record
                .funding_outpoint
                .parse::<OutPoint>()
                .with_context(|| format!("invalid funding outpoint in {}", path.display()))?;
            let transfer = match read_valid_transfer_file(PathBuf::from(&record.transfer_path)) {
                Ok(transfer) => transfer,
                Err(err) => {
                    self.events
                        .lock()
                        .expect("events lock poisoned")
                        .push_back(format!(
                            "ln-rgb skipped generated RGB funding transfer {}: {err:#}",
                            path.display()
                        ));
                    continue;
                }
            };
            loaded.push_back(RgbFundingTransfer {
                temporary_channel_id,
                peer_node_id,
                funding_outpoint,
                transfer,
            });
        }
        let count = loaded.len();
        *self
            .generated_rgb_funding_transfers
            .lock()
            .expect("generated rgb funding transfer queue lock poisoned") = loaded;
        Ok(count)
    }

    fn load_rgb_payment_states_from_disk(&self) -> Result<usize> {
        let dir = self.rgb_payment_state_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(0);
        };
        let mut loaded_assets = HashMap::new();
        let mut count = 0usize;
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: RgbPaymentStateRecord = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("decode RGB payment state {}", path.display()))?;
            count += 1;
            if record.direction == "outbound"
                && matches!(record.status.as_str(), "created" | "submission_failed")
            {
                if let Some(payment_id) = record.payment_id.as_deref() {
                    loaded_assets.insert(
                        payment_id.to_string(),
                        RgbAssetAmount {
                            contract_id: record.contract_id.parse().with_context(|| {
                                format!("invalid RGB contract id in {}", path.display())
                            })?,
                            amount: record.rgb_amount,
                        },
                    );
                }
            }
        }
        *self
            .rgb_payment_assets
            .lock()
            .expect("rgb payment asset lock poisoned") = loaded_assets;
        Ok(count)
    }

    pub fn rgb_payment_states(&self) -> Result<Vec<RgbPaymentStateRecord>> {
        let dir = self.rgb_payment_state_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            records.push(
                serde_json::from_slice(&fs::read(&path)?)
                    .with_context(|| format!("decode RGB payment state {}", path.display()))?,
            );
        }
        records.sort_by_key(|record: &RgbPaymentStateRecord| record.created_at);
        Ok(records)
    }

    pub fn rgb_pending_maturity_records(&self) -> Result<Vec<RgbPendingMaturityRecord>> {
        let dir = self.rgb_pending_maturity_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            records.push(
                serde_json::from_slice(&fs::read(&path)?)
                    .with_context(|| format!("decode RGB maturity record {}", path.display()))?,
            );
        }
        records.sort_by_key(|record: &RgbPendingMaturityRecord| record.spendable_height);
        Ok(records)
    }

    pub fn rgb_pending_sweep_records(&self) -> Result<Vec<RgbPendingSweepRecord>> {
        let dir = self.rgb_pending_sweep_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            records.push(
                serde_json::from_slice(&fs::read(&path)?)
                    .with_context(|| format!("decode RGB sweep record {}", path.display()))?,
            );
        }
        records.sort_by_key(|record: &RgbPendingSweepRecord| record.updated_at);
        Ok(records)
    }

    pub fn drain_generated_rgb_funding_transfers(&self) -> Vec<RgbFundingTransfer> {
        self.generated_rgb_funding_transfers
            .lock()
            .expect("generated rgb funding transfer queue lock poisoned")
            .drain(..)
            .collect()
    }

    pub fn queued_debug_events(&self, limit: usize) -> Vec<String> {
        let mut events = self.events.lock().expect("ln-rgb event lock poisoned");
        let mut out = Vec::new();
        for _ in 0..limit {
            let Some(event) = events.pop_front() else {
                break;
            };
            out.push(event);
        }
        out
    }

    pub fn generated_rgb_funding_transfers(&self) -> Vec<RgbFundingTransfer> {
        self.generated_rgb_funding_transfers
            .lock()
            .expect("generated rgb funding transfer queue lock poisoned")
            .iter()
            .cloned()
            .collect()
    }

    pub fn iroh_endpoint_addr(&self) -> Result<EndpointAddr> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        let endpoint = runtime.iroh_endpoint.clone();
        let addr = runtime.rt.block_on(async {
            endpoint.online().await;
            endpoint.addr()
        });
        Ok(addr)
    }

    pub fn probe_iroh_peer(
        &self,
        remote_addr: EndpointAddr,
        timeout: Duration,
    ) -> Result<EndpointId> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        let endpoint = runtime.iroh_endpoint.clone();
        runtime.rt.block_on(async {
            tokio::time::timeout(timeout, probe_iroh_health(&endpoint, remote_addr))
                .await
                .context("Iroh health probe timed out")?
        })
    }

    pub fn send_rgb20_direct_transfer(
        &self,
        remote_addr: EndpointAddr,
        request: Rgb20TransferRequest,
        attempts: u32,
        delay: Duration,
    ) -> Result<(rgb20::Rgb20TransferResult, IrohAssignmentAck)> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let mut wallet = self.open_l1_wallet()?;
        let stock_dir = self.rgb_stock_dir();
        let esplora = self.next_esplora_url();
        let transfer = rgb20::transfer_rgb20_fixed(
            &stock_dir,
            &mut wallet,
            self.config.network,
            &esplora,
            request,
            true,
        )?;
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        let endpoint = runtime.iroh_endpoint.clone();
        let ack = runtime
            .rt
            .block_on(send_rgb20_transfer_assignment_with_retry(
                &endpoint,
                remote_addr,
                &transfer,
                attempts,
                delay,
            ))?;
        Ok((transfer, ack))
    }

    pub fn send_rgb_assignment(
        &self,
        remote_addr: EndpointAddr,
        assignment: &IrohAssignment,
        attempts: u32,
        delay: Duration,
    ) -> Result<IrohAssignmentAck> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        let endpoint = runtime.iroh_endpoint.clone();
        runtime.rt.block_on(send_assignment_with_retry(
            &endpoint,
            remote_addr,
            assignment,
            attempts,
            delay,
        ))
    }

    pub fn take_generated_rgb_funding_transfer(
        &self,
        channel_id: ChannelId,
    ) -> Option<RgbFundingTransfer> {
        let mut transfers = self
            .generated_rgb_funding_transfers
            .lock()
            .expect("generated rgb funding transfer queue lock poisoned");
        let index = transfers
            .iter()
            .position(|transfer| transfer.temporary_channel_id == channel_id)?;
        transfers.remove(index)
    }

    pub fn generated_rgb_funding_transfer(
        &self,
        channel_id: ChannelId,
    ) -> Option<RgbFundingTransfer> {
        self.generated_rgb_funding_transfers
            .lock()
            .expect("generated rgb funding transfer queue lock poisoned")
            .iter()
            .find(|transfer| transfer.temporary_channel_id == channel_id)
            .cloned()
    }

    fn persist_channel_manager_to_store(
        kv_store: &FilesystemStore,
        channel_manager: &LnRgbChannelManager,
    ) -> Result<()> {
        KVStoreSync::write(
            kv_store,
            CHANNEL_MANAGER_PERSISTENCE_PRIMARY_NAMESPACE,
            CHANNEL_MANAGER_PERSISTENCE_SECONDARY_NAMESPACE,
            CHANNEL_MANAGER_PERSISTENCE_KEY,
            channel_manager.encode(),
        )
        .context("persist ln-rgb channel manager")
    }

    fn poll_ldk_events(&self) {
        self.poll_ldk_events_inner(true);
    }

    fn poll_ldk_events_fast(&self) {
        self.poll_ldk_events_inner(false);
    }

    fn spawn_event_pump(self: &Arc<Self>) -> Result<()> {
        let mut runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_mut()
            .context("ln-rgb runtime did not start")?;
        if runtime.event_pump_handle.is_some() {
            return Ok(());
        }
        let backend = Arc::clone(self);
        runtime.event_pump_handle = Some(runtime.rt.spawn(async move {
            backend.run_event_pump().await;
        }));
        Ok(())
    }

    fn spawn_iroh_listener(self: &Arc<Self>) -> Result<()> {
        let mut runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_mut()
            .context("ln-rgb runtime did not start")?;
        if runtime.iroh_listener_handle.is_some() {
            return Ok(());
        }
        let endpoint = runtime.iroh_endpoint.clone();
        let task_handles = Arc::clone(&runtime.peer_task_handles);
        let backend = Arc::clone(self);
        runtime.iroh_listener_handle = Some(runtime.rt.spawn(async move {
            backend.run_iroh_listener(endpoint, task_handles).await;
        }));
        Ok(())
    }

    async fn run_iroh_listener(
        self: Arc<Self>,
        endpoint: Endpoint,
        task_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    ) {
        self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                "ln-rgb Iroh listener started: endpoint_id={} protocols=rgb-assignment,tcp-tunnel,health",
                endpoint.id()
            ));
        loop {
            if !self.started.load(Ordering::SeqCst) || endpoint.is_closed() {
                break;
            }
            match endpoint.accept().await {
                Some(incoming) => {
                    let mut accepting = match incoming.accept() {
                        Ok(accepting) => accepting,
                        Err(err) => {
                            self.events
                                .lock()
                                .expect("ln-rgb event lock poisoned")
                                .push_back(format!("ln-rgb Iroh listener accept error: {err:#}"));
                            continue;
                        }
                    };
                    let alpn = match accepting.alpn().await {
                        Ok(alpn) => alpn,
                        Err(err) => {
                            self.events
                                .lock()
                                .expect("ln-rgb event lock poisoned")
                                .push_back(format!("ln-rgb Iroh listener ALPN error: {err:#}"));
                            continue;
                        }
                    };
                    let conn = match accepting.await {
                        Ok(conn) => conn,
                        Err(err) => {
                            self.events
                                .lock()
                                .expect("ln-rgb event lock poisoned")
                                .push_back(format!(
                                    "ln-rgb Iroh listener connection error: {err:#}"
                                ));
                            continue;
                        }
                    };
                    let backend = Arc::clone(&self);
                    let handle = tokio::spawn(async move {
                        backend.handle_iroh_connection(alpn, conn).await;
                    });
                    task_handles
                        .lock()
                        .expect("ln-rgb peer task lock poisoned")
                        .push(handle);
                }
                None => break,
            }
        }
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back("ln-rgb Iroh listener stopped".to_string());
    }

    async fn handle_iroh_connection(
        self: Arc<Self>,
        alpn: Vec<u8>,
        conn: iroh::endpoint::Connection,
    ) {
        let result = if alpn == RGB_ASSIGNMENT_ALPN {
            self.handle_rgb_assignment_connection(conn).await
        } else if alpn == RGB_LN_TCP_TUNNEL_ALPN {
            self.handle_iroh_tcp_tunnel_connection(conn).await
        } else if alpn == RGB_LN_HEALTH_ALPN {
            self.handle_iroh_health_connection(conn).await
        } else {
            Err(anyhow!(
                "unsupported Iroh ALPN: {}",
                String::from_utf8_lossy(&alpn)
            ))
        };
        if let Err(err) = result {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!("ln-rgb Iroh connection handler error: {err:#}"));
        }
    }

    async fn handle_rgb_assignment_connection(
        self: &Arc<Self>,
        conn: iroh::endpoint::Connection,
    ) -> Result<()> {
        if !self.config.accept_inbound_rgb_transfers {
            bail!("inbound RGB transfer listener disabled by config");
        }
        let stock_dir = self.rgb_stock_dir();
        let chain_source = self
            .rotated_chain_sources()?
            .into_iter()
            .next()
            .context("no RGB assignment validation chain source configured")?;
        let network = self.config.network;
        let receipt = receive_assignment_connection(conn, move |envelope, payload| {
            accept_rgb_assignment_payload(&stock_dir, network, &chain_source, envelope, payload)
        })
        .await?;
        let status = if receipt.ack.accepted {
            "accepted"
        } else {
            "rejected"
        };
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb inbound RGB assignment {status}: transfer_id={} contract_id={} recipient_outpoint={} payload_sha256={} message={}",
                receipt.assignment.envelope.transfer_id,
                receipt.assignment.envelope.contract_id,
                receipt.assignment.envelope.recipient_outpoint,
                receipt.assignment.envelope.payload_sha256,
                receipt.ack.message
            ));
        Ok(())
    }

    async fn handle_iroh_health_connection(
        self: &Arc<Self>,
        conn: iroh::endpoint::Connection,
    ) -> Result<()> {
        let remote_id = handle_iroh_health_connection(conn).await?;
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!("ln-rgb Iroh health check from {remote_id}"));
        Ok(())
    }

    async fn handle_iroh_tcp_tunnel_connection(
        self: &Arc<Self>,
        conn: iroh::endpoint::Connection,
    ) -> Result<()> {
        let target = self
            .listening_addresses()
            .and_then(|mut addresses| addresses.pop())
            .context("ln-rgb Iroh TCP tunnel requires node.listen")?;
        let target = socket_address_to_std(&target)?;
        run_iroh_tcp_tunnel_connection(conn, target).await
    }

    fn spawn_configured_iroh_tunnels(self: &Arc<Self>) -> Result<()> {
        let peers = self.config.iroh_tunnel_peers.clone();
        if peers.is_empty() {
            return Ok(());
        }
        let mut runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_mut()
            .context("ln-rgb runtime did not start")?;
        for peer in peers {
            let std_listener = std::net::TcpListener::bind("127.0.0.1:0")
                .context("bind local Iroh TCP tunnel listener")?;
            let local_addr = std_listener
                .local_addr()
                .context("read local Iroh TCP tunnel listener address")?;
            std_listener
                .set_nonblocking(true)
                .context("set local Iroh TCP tunnel listener nonblocking")?;
            let listener = {
                let _runtime_context = runtime.rt.enter();
                tokio::net::TcpListener::from_std(std_listener)
                    .context("create tokio local Iroh TCP tunnel listener")?
            };
            let socket_address = SocketAddress::from_str(&local_addr.to_string())
                .map_err(|_| anyhow!("invalid local Iroh TCP tunnel address: {local_addr}"))?;
            let endpoint = runtime.iroh_endpoint.clone();
            let remote_addr = peer.endpoint_addr.clone();
            let stop = Arc::clone(&runtime.listener_stop);
            let task_handles = Arc::clone(&runtime.peer_task_handles);
            let tunnel_node_id = peer.node_id;
            let listener_handle = runtime.rt.spawn(async move {
                loop {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    match listener.accept().await {
                        Ok((tcp_stream, _)) => {
                            let endpoint = endpoint.clone();
                            let remote_addr = remote_addr.clone();
                            let handle = tokio::spawn(async move {
                                if let Err(err) =
                                    connect_iroh_tcp_tunnel(&endpoint, remote_addr, tcp_stream)
                                        .await
                                {
                                    eprintln!(
                                        "[ln-rgb] Iroh TCP tunnel stream failed for {tunnel_node_id}: {err:#}"
                                    );
                                }
                            });
                            task_handles
                                .lock()
                                .expect("ln-rgb peer task lock poisoned")
                                .push(handle);
                        }
                        Err(err) => {
                            if stop.load(Ordering::SeqCst) {
                                break;
                            }
                            eprintln!("[ln-rgb] local Iroh TCP tunnel accept error: {err}");
                            break;
                        }
                    }
                }
            });
            runtime.iroh_tunnel_handles.push(listener_handle);

            let backend = Arc::clone(self);
            let connect_address = socket_address.clone();
            let connect_node_id = peer.node_id;
            let connect_handle = runtime.rt.spawn(async move {
                loop {
                    if !backend.started.load(Ordering::SeqCst) {
                        break;
                    }
                    match backend.connect(connect_node_id, connect_address.clone(), true) {
                        Ok(()) => break,
                        Err(err) => {
                            backend
                                .events
                                .lock()
                                .expect("ln-rgb event lock poisoned")
                                .push_back(format!(
                                    "ln-rgb Iroh TCP tunnel peer connect retry: node_id={connect_node_id} local_addr={connect_address} error={err:#}"
                                ));
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            });
            runtime.iroh_tunnel_handles.push(connect_handle);
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                "ln-rgb Iroh TCP tunnel ready: peer_node_id={} local_addr={} remote_endpoint={}",
                peer.node_id, socket_address, peer.endpoint_addr.id
            ));
        }
        Ok(())
    }

    async fn run_event_pump(self: Arc<Self>) {
        loop {
            let update = {
                let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
                let Some(runtime) = runtime_guard.as_ref() else {
                    break;
                };
                runtime
                    .channel_manager
                    .get_event_or_persistence_needed_future()
            };
            update.await;
            if !self.started.load(Ordering::SeqCst) {
                break;
            }
            self.poll_ldk_events_inner(false);
        }
    }

    fn poll_ldk_events_inner(&self, run_maintenance: bool) {
        let (channel_manager, peer_manager, kv_store, output_sweeper, chain_monitor) = {
            let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
            let Some(runtime) = runtime_guard.as_ref() else {
                return;
            };
            (
                Arc::clone(&runtime.channel_manager),
                Arc::clone(&runtime.peer_manager),
                Arc::clone(&runtime.kv_store),
                Arc::clone(&runtime.output_sweeper),
                Arc::clone(&runtime._chain_monitor),
            )
        };
        let had_pending_htlcs = channel_manager.needs_pending_htlc_processing();
        channel_manager.process_pending_htlc_forwards();
        if had_pending_htlcs {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back("ln-rgb pending HTLCs forwarded".to_string());
        }
        let handler = |event: Event| -> std::result::Result<(), lightning::events::ReplayEvent> {
            if let Err(err) = self.handle_ldk_event(&channel_manager, &output_sweeper, event) {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!("ln-rgb LDK event handling failed: {err:#}"));
            }
            Ok(())
        };
        channel_manager.process_pending_events(&handler);
        if run_maintenance {
            if output_sweeper
                .regenerate_and_broadcast_spend_if_necessary()
                .is_err()
            {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back("ln-rgb output sweeper failed to broadcast spend".to_string());
            }
            if let Err(err) = self.reconcile_rgb_sweep_records_from_sweeper(&output_sweeper) {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!(
                        "ln-rgb RGB sweep record reconciliation failed: {err:#}"
                    ));
            }
            if let Err(err) = self.reconcile_rgb_maturity_records_from_monitors(&chain_monitor) {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!(
                        "ln-rgb RGB maturity record reconciliation failed: {err:#}"
                    ));
            }
        }
        peer_manager.process_events();
        if channel_manager.get_and_clear_needs_persistence() {
            if let Err(err) = Self::persist_channel_manager_to_store(&kv_store, &channel_manager) {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!(
                        "ln-rgb channel manager persistence failed: {err:#}"
                    ));
            }
        }
        if run_maintenance {
            if let Err(err) = self.try_promote_confirmed_rgb_funding_stocks() {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!("ln-rgb RGB staged stock promotion failed: {err:#}"));
            }
            if let Err(err) = self.try_promote_confirmed_rgb_sweep_stocks() {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!(
                        "ln-rgb RGB sweep staged stock promotion failed: {err:#}"
                    ));
            }
        }
    }

    fn handle_ldk_event(
        &self,
        channel_manager: &LnRgbChannelManager,
        output_sweeper: &LnRgbOutputSweeper,
        event: Event,
    ) -> Result<()> {
        match event {
            Event::FundingGenerationReady {
                temporary_channel_id,
                counterparty_node_id,
                channel_value_satoshis,
                output_script,
                user_channel_id,
            } => {
                let asset = self
                    .rgb_channel_assets
                    .lock()
                    .expect("rgb channel asset lock poisoned")
                    .get(&user_channel_id)
                    .cloned();
                let (tx, funding_outpoint, rgb_transfer, staged_stock_dir) = if let Some(asset) =
                    asset
                {
                    let result = self.build_rgb_funding_transaction(
                        channel_value_satoshis,
                        output_script.clone(),
                        &asset,
                    )?;
                    (
                        result.transaction,
                        result.funding_outpoint,
                        Some(result.transfer),
                        Some(result.staged_stock_dir),
                    )
                } else {
                    let tx = self
                        .build_funding_transaction(channel_value_satoshis, output_script.clone())?;
                    let funding_outpoint =
                        funding_outpoint_from_tx(&tx, &output_script, channel_value_satoshis)
                            .context(
                                "LDK funding transaction is missing requested funding output",
                            )?;
                    (tx, funding_outpoint, None, None)
                };
                let txid = tx.compute_txid();
                if let Some(transfer) = rgb_transfer {
                    let pending_tx = PendingFundingTransaction {
                        peer_node_id: counterparty_node_id,
                        transaction: tx,
                        funding_outpoint,
                        user_channel_id,
                        staged_stock_dir: staged_stock_dir
                            .context("RGB funding transaction is missing staged stock dir")?,
                    };
                    let generated_transfer = RgbFundingTransfer {
                        temporary_channel_id: ChannelId(temporary_channel_id.0),
                        peer_node_id: counterparty_node_id,
                        funding_outpoint,
                        transfer,
                    };
                    self.persist_pending_funding_transaction(temporary_channel_id, &pending_tx)?;
                    self.persist_generated_rgb_funding_transfer(&generated_transfer)?;
                    self.pending_funding_transactions
                        .lock()
                        .expect("pending funding transaction lock poisoned")
                        .insert(temporary_channel_id, pending_tx);
                    self.generated_rgb_funding_transfers
                        .lock()
                        .expect("generated rgb funding transfer queue lock poisoned")
                        .push_back(generated_transfer);
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb RGB funding transaction waiting for Iroh transfer ack: user_channel_id={user_channel_id} temporary_channel_id={temporary_channel_id} funding_outpoint={funding_outpoint} txid={txid}"
                        ));
                    self.try_complete_rgb_funding(channel_manager, temporary_channel_id)?;
                } else {
                    self.submit_funding_transaction(
                        channel_manager,
                        temporary_channel_id,
                        counterparty_node_id,
                        tx.clone(),
                    )?;
                    self.record_local_unconfirmed(tx)?;
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb funding transaction generated: user_channel_id={user_channel_id} temporary_channel_id={temporary_channel_id} funding_outpoint={funding_outpoint} txid={txid}"
                        ));
                }
            }
            Event::OpenChannelRequest {
                temporary_channel_id,
                counterparty_node_id,
                ..
            } => {
                if !self.config.accept_inbound_channels {
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb inbound channel request ignored by config: peer={counterparty_node_id} temporary_channel_id={temporary_channel_id}"
                        ));
                    return Ok(());
                }
                let user_channel_id =
                    derive_user_channel_id(&self.seed, counterparty_node_id, 0, now_nanos() as u64);
                if self.is_trusted_0conf_peer(&counterparty_node_id) {
                    channel_manager
                        .accept_inbound_channel_from_trusted_peer_0conf(
                            &temporary_channel_id,
                            &counterparty_node_id,
                            user_channel_id,
                            None,
                        )
                        .map_err(|err| {
                            anyhow!("LDK rejected trusted 0conf inbound channel: {err:?}")
                        })?;
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb accepted trusted 0conf inbound channel: peer={counterparty_node_id} temporary_channel_id={temporary_channel_id}"
                        ));
                } else {
                    channel_manager
                        .accept_inbound_channel(
                            &temporary_channel_id,
                            &counterparty_node_id,
                            user_channel_id,
                            None,
                        )
                        .map_err(|err| anyhow!("LDK rejected inbound channel: {err:?}"))?;
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb accepted inbound channel: peer={counterparty_node_id} temporary_channel_id={temporary_channel_id}"
                        ));
                }
            }
            Event::FundingTxBroadcastSafe {
                channel_id,
                funding_txo,
                counterparty_node_id,
                ..
            } => {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!(
                        "ln-rgb funding tx broadcast safe: channel_id={channel_id} peer={counterparty_node_id} funding={funding_txo}"
                    ));
            }
            Event::ChannelReady {
                channel_id,
                funding_txo,
                ..
            } => {
                if let Some(funding_txo) = funding_txo {
                    self.mark_rgb_funding_binding_channel_id(funding_txo, channel_id)?;
                    if let Some(binding) =
                        self.try_promote_confirmed_rgb_funding_stock(funding_txo)?
                    {
                        self.events
                            .lock()
                            .expect("ln-rgb event lock poisoned")
                            .push_back(format!(
                                "ln-rgb RGB staged stock promoted: channel_id={channel_id} funding={} staged_stock_dir={}",
                                binding.funding_outpoint, binding.staged_stock_dir
                            ));
                    } else {
                        self.mark_rgb_funding_binding_status_if_pending(
                            funding_txo,
                            "waiting_confirmation",
                        )?;
                    }
                }
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!("ln-rgb channel ready: channel_id={channel_id}"));
            }
            Event::ChannelClosed {
                channel_id,
                channel_funding_txo,
                reason,
                ..
            } => {
                if let Some(funding_txo) = channel_funding_txo {
                    self.mark_rgb_funding_binding_status_if_pending(
                        funding_txo.into_bitcoin_outpoint(),
                        "closed_before_confirmation",
                    )?;
                }
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!(
                        "ln-rgb channel closed: channel_id={channel_id} reason={reason:?}"
                    ));
            }
            Event::SpendableOutputs {
                outputs,
                channel_id,
            } => {
                output_sweeper
                    .track_spendable_outputs(outputs.clone(), channel_id, false, None)
                    .map_err(|()| anyhow!("failed to track LDK spendable outputs"))?;
                self.handle_spendable_outputs(channel_id, outputs)?;
            }
            Event::PaymentClaimable {
                amount_msat,
                payment_hash,
                purpose,
                ..
            } => {
                if let Some(preimage) = purpose.preimage() {
                    channel_manager.claim_funds(preimage);
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb payment claimable: hash={} amount_msat={amount_msat}",
                            hex32(payment_hash.0)
                        ));
                } else {
                    bail!(
                        "payment {} is claimable but LDK did not provide a preimage",
                        hex32(payment_hash.0)
                    );
                }
            }
            Event::PaymentClaimed {
                payment_hash,
                amount_msat,
                ..
            } => {
                let inbound_rgb = channel_manager.inbound_rgb_payment_amount(&payment_hash);
                if let Some((contract_id, rgb_amount)) = inbound_rgb {
                    self.persist_inbound_rgb_payment_claimed(
                        payment_hash,
                        amount_msat,
                        &contract_id.to_string(),
                        rgb_amount,
                    )?;
                    self.btc_events
                        .lock()
                        .expect("ln-rgb btc event lock poisoned")
                        .push_back(BtcLnEvent::RgbPaymentReceived {
                            payment_hash: Some(hex32(payment_hash.0)),
                            amount_msat,
                            contract_id: contract_id.to_string(),
                            rgb_amount,
                        });
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb RGB payment received: hash={} amount_msat={amount_msat} contract_id={} rgb_amount={rgb_amount}",
                            hex32(payment_hash.0),
                            contract_id
                        ));
                } else {
                    self.btc_events
                        .lock()
                        .expect("ln-rgb btc event lock poisoned")
                        .push_back(BtcLnEvent::PaymentReceived {
                            payment_hash: Some(hex32(payment_hash.0)),
                            amount_msat,
                        });
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb payment received: amount_msat={amount_msat}"
                        ));
                }
            }
            Event::PaymentSent { payment_id, .. } => {
                if let Some(payment_id) = payment_id {
                    self.mark_outbound_rgb_payment_status(
                        payment_id,
                        "btc_payment_sent_waiting_rgb_ack",
                    )?;
                }
                self.btc_events
                    .lock()
                    .expect("ln-rgb btc event lock poisoned")
                    .push_back(BtcLnEvent::PaymentSuccessful {
                        payment_id: payment_id.map(|id| hex32(id.0)),
                    });
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back("ln-rgb payment sent".to_string());
            }
            Event::PaymentFailed { payment_id, .. } => {
                self.mark_outbound_rgb_payment_status(payment_id, "failed")?;
                self.btc_events
                    .lock()
                    .expect("ln-rgb btc event lock poisoned")
                    .push_back(BtcLnEvent::PaymentFailed {
                        payment_id: Some(hex32(payment_id.0)),
                    });
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back("ln-rgb payment failed".to_string());
            }
            other => {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!("ln-rgb LDK event: {other:?}"));
            }
        }
        Ok(())
    }

    fn open_l1_wallet(&self) -> Result<LocalWallet> {
        let mnemonic = self
            .config
            .entropy_mnemonic
            .as_deref()
            .context("ln-rgb funding requires mnemonic from zs config")?;
        let mnemonic = BdkMnemonic::parse_in_normalized(BdkLanguage::English, mnemonic)
            .context("invalid ln-rgb mnemonic from runtime config")?;
        LocalWallet::open_with_mnemonic(&self.config.l1_data_dir, self.config.network, &mnemonic)
    }

    fn build_funding_transaction(
        &self,
        channel_value_satoshis: u64,
        output_script: ScriptBuf,
    ) -> Result<Transaction> {
        self.retry_transient_esplora("build LN funding transaction", || {
            let mut local = self.open_l1_wallet()?;
            let esplora = self.next_esplora_url();
            self.sync_l1_wallet_or_use_cached(
                &mut local,
                &esplora,
                "build LN funding transaction",
            )?;

            let fee_rate = FeeRate::from_sat_per_vb(2)
                .context("invalid ln-rgb funding transaction fee rate")?;
            let mut builder = local.wallet.build_tx();
            builder
                .add_recipient(
                    output_script.clone(),
                    Amount::from_sat(channel_value_satoshis),
                )
                .fee_rate(fee_rate)
                .nlocktime(LockTime::ZERO);

            let mut psbt = builder
                .finish()
                .context("failed to build LN funding transaction")?;
            let finalized = local
                .wallet
                .sign(&mut psbt, SignOptions::default())
                .context("failed to sign LN funding transaction")?;
            if !finalized {
                bail!("LN funding transaction was not finalized");
            }
            local.persist()?;
            psbt.extract_tx()
                .context("failed to extract LN funding transaction")
        })
    }

    fn build_rgb_funding_transaction(
        &self,
        channel_value_satoshis: u64,
        output_script: ScriptBuf,
        asset: &RgbAssetAmount,
    ) -> Result<crate::rgb20::Rgb20ChannelFundingResult> {
        self.retry_transient_esplora("build RGB funding transaction", || {
            let mut local = self.open_l1_wallet()?;
            let esplora = self.next_esplora_url();
            build_rgb20_channel_funding_cached_first(
                &self.rgb_stock_dir(),
                &mut local,
                self.config.network,
                &esplora,
                Rgb20ChannelFundingRequest {
                    contract_id: asset.contract_id,
                    rgb_amount: asset.amount,
                    channel_value_satoshis,
                    funding_script_pubkey: output_script.clone(),
                    fee_rate_sat_vb: 2,
                },
            )
        })
    }

    fn sync_l1_wallet_or_use_cached(
        &self,
        local: &mut LocalWallet,
        esplora: &str,
        label: &str,
    ) -> Result<()> {
        match sync_wallet(local, Some(esplora)) {
            Ok(()) => {
                local.persist()?;
                Ok(())
            }
            Err(err) if is_transient_esplora_error(&err) => {
                let balance = local.wallet.balance();
                if balance.total().to_sat() == 0 {
                    return Err(err).with_context(|| {
                        format!(
                            "cannot use cached L1 wallet during {label}: cached balance is zero"
                        )
                    });
                }
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!(
                        "ln-rgb using cached L1 wallet after transient Esplora error during {label}: {err:#}"
                    ));
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn retry_transient_esplora<T>(
        &self,
        label: &str,
        mut operation: impl FnMut() -> Result<T>,
    ) -> Result<T> {
        let mut last_err = None;
        const MAX_ESPLORA_RETRY_ATTEMPTS: u64 = 10;
        for attempt in 1..=MAX_ESPLORA_RETRY_ATTEMPTS {
            match operation() {
                Ok(value) => return Ok(value),
                Err(err)
                    if attempt < MAX_ESPLORA_RETRY_ATTEMPTS && is_transient_esplora_error(&err) =>
                {
                    self.events.lock().expect("ln-rgb event lock poisoned").push_back(
                        format!(
                            "ln-rgb transient Esplora error during {label}; retrying attempt {attempt}/{MAX_ESPLORA_RETRY_ATTEMPTS}: {err:#}"
                        ),
                    );
                    last_err = Some(err);
                    std::thread::sleep(Duration::from_secs(attempt));
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_err.expect("transient Esplora retry loop runs at least once"))
    }

    fn record_local_unconfirmed(&self, tx: Transaction) -> Result<()> {
        let mut local = self.open_l1_wallet()?;
        local.wallet.apply_unconfirmed_txs([(tx, now_secs())]);
        local.persist()
    }

    fn evict_local_unconfirmed_rgb_tx(&self, txid: Txid) -> Result<bool> {
        let mut local = self.open_l1_wallet()?;
        let recovered = local.evict_unconfirmed_tx(txid)?;
        if recovered {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                    "ln-rgb evicted revoked RGB carrier transaction: txid={txid}"
                ));
        }
        Ok(recovered)
    }

    fn submit_funding_transaction(
        &self,
        channel_manager: &LnRgbChannelManager,
        temporary_channel_id: LnRgbChannelId,
        peer_node_id: PublicKey,
        tx: Transaction,
    ) -> Result<()> {
        channel_manager
            .funding_transaction_generated(temporary_channel_id, peer_node_id, tx)
            .map_err(|err| anyhow!("LDK rejected funding transaction: {err:?}"))
    }

    fn persist_pending_funding_transaction(
        &self,
        temporary_channel_id: LnRgbChannelId,
        pending: &PendingFundingTransaction,
    ) -> Result<()> {
        let dir = self.rgb_pending_funding_transaction_dir();
        fs::create_dir_all(&dir)
            .with_context(|| format!("create pending RGB funding tx dir {}", dir.display()))?;
        let now = now_secs();
        let record = PendingFundingTransactionRecord {
            temporary_channel_id: hex32(temporary_channel_id.0),
            peer_node_id: pending.peer_node_id.to_string(),
            transaction_hex: bytes_to_hex(&serialize(&pending.transaction)),
            funding_outpoint: pending.funding_outpoint.to_string(),
            user_channel_id: pending.user_channel_id,
            staged_stock_dir: pending.staged_stock_dir.display().to_string(),
            created_at: now,
            updated_at: now,
        };
        let path = dir.join(format!("{}.json", hex32(temporary_channel_id.0)));
        fs::write(&path, serde_json::to_vec_pretty(&record)?)
            .with_context(|| format!("write pending RGB funding tx {}", path.display()))
    }

    fn persist_pending_rgb_funding_transfer(
        &self,
        temporary_channel_id: LnRgbChannelId,
        pending: &PendingRgbFundingTransfer,
    ) -> Result<()> {
        let dir = self.rgb_pending_funding_transfer_dir();
        fs::create_dir_all(&dir).with_context(|| {
            format!("create pending RGB funding transfer dir {}", dir.display())
        })?;
        let base = hex32(temporary_channel_id.0);
        let transfer_path = dir.join(format!("{base}.rgb-transfer"));
        fs::write(&transfer_path, encode_valid_transfer(&pending.transfer)?).with_context(
            || {
                format!(
                    "write pending RGB funding transfer {}",
                    transfer_path.display()
                )
            },
        )?;
        let now = now_secs();
        let record = PendingRgbFundingTransferRecord {
            temporary_channel_id: base.clone(),
            peer_node_id: pending.peer_node_id.to_string(),
            transfer_path: transfer_path.display().to_string(),
            created_at: now,
            updated_at: now,
        };
        let path = dir.join(format!("{base}.json"));
        fs::write(&path, serde_json::to_vec_pretty(&record)?)
            .with_context(|| format!("write pending RGB funding transfer {}", path.display()))
    }

    fn persist_generated_rgb_funding_transfer(&self, transfer: &RgbFundingTransfer) -> Result<()> {
        let dir = self.rgb_generated_funding_transfer_dir();
        fs::create_dir_all(&dir).with_context(|| {
            format!(
                "create generated RGB funding transfer dir {}",
                dir.display()
            )
        })?;
        let base = hex32(transfer.temporary_channel_id.0);
        let transfer_path = dir.join(format!("{base}.rgb-transfer"));
        fs::write(&transfer_path, encode_valid_transfer(&transfer.transfer)?).with_context(
            || {
                format!(
                    "write generated RGB funding transfer {}",
                    transfer_path.display()
                )
            },
        )?;
        let now = now_secs();
        let record = GeneratedRgbFundingTransferRecord {
            temporary_channel_id: base.clone(),
            peer_node_id: transfer.peer_node_id.to_string(),
            funding_outpoint: transfer.funding_outpoint.to_string(),
            transfer_path: transfer_path.display().to_string(),
            created_at: now,
            updated_at: now,
        };
        let path = dir.join(format!("{base}.json"));
        fs::write(&path, serde_json::to_vec_pretty(&record)?)
            .with_context(|| format!("write generated RGB funding transfer {}", path.display()))
    }

    fn remove_rgb_funding_recovery_records(&self, temporary_channel_id: LnRgbChannelId) {
        let base = hex32(temporary_channel_id.0);
        for dir in [
            self.rgb_pending_funding_transaction_dir(),
            self.rgb_pending_funding_transfer_dir(),
            self.rgb_generated_funding_transfer_dir(),
        ] {
            let _ = fs::remove_file(dir.join(format!("{base}.json")));
            let _ = fs::remove_file(dir.join(format!("{base}.rgb-transfer")));
        }
    }

    fn try_complete_rgb_funding(
        &self,
        channel_manager: &LnRgbChannelManager,
        temporary_channel_id: LnRgbChannelId,
    ) -> Result<bool> {
        let has_funding_tx = self
            .pending_funding_transactions
            .lock()
            .expect("pending funding transaction lock poisoned")
            .contains_key(&temporary_channel_id);
        if !has_funding_tx {
            return Ok(false);
        }
        let has_rgb_transfer = self
            .pending_rgb_funding
            .lock()
            .expect("pending rgb funding lock poisoned")
            .contains_key(&temporary_channel_id);
        if !has_rgb_transfer {
            return Ok(false);
        }

        let pending_tx = self
            .pending_funding_transactions
            .lock()
            .expect("pending funding transaction lock poisoned")
            .remove(&temporary_channel_id)
            .expect("pending funding transaction exists");
        let pending_rgb = self
            .pending_rgb_funding
            .lock()
            .expect("pending rgb funding lock poisoned")
            .remove(&temporary_channel_id)
            .expect("pending rgb funding transfer exists");

        if pending_rgb.peer_node_id != pending_tx.peer_node_id {
            bail!(
                "RGB funding transfer peer mismatch for channel {temporary_channel_id}: transfer={} funding={}",
                pending_rgb.peer_node_id,
                pending_tx.peer_node_id
            );
        }

        let completion = (|| -> Result<()> {
            self.save_rgb_funding_binding(
                temporary_channel_id,
                pending_tx.peer_node_id,
                pending_tx.funding_outpoint,
                &pending_rgb.transfer,
                &pending_tx.staged_stock_dir,
            )?;
            channel_manager
                .provide_funding_rgb_transfer_for_unfunded_channel(
                    temporary_channel_id,
                    pending_tx.peer_node_id,
                    ldk_rgb_funding_transfer(temporary_channel_id, &pending_rgb.transfer),
                )
                .map_err(|err| anyhow!("LDK rejected RGB funding transfer: {err:?}"))?;
            self.submit_funding_transaction(
                channel_manager,
                temporary_channel_id,
                pending_tx.peer_node_id,
                pending_tx.transaction.clone(),
            )?;
            self.record_local_unconfirmed(pending_tx.transaction.clone())
        })();

        if let Err(err) = completion {
            self.pending_funding_transactions
                .lock()
                .expect("pending funding transaction lock poisoned")
                .insert(temporary_channel_id, pending_tx);
            self.pending_rgb_funding
                .lock()
                .expect("pending rgb funding lock poisoned")
                .insert(temporary_channel_id, pending_rgb);
            return Err(err);
        }

        self.rgb_channel_assets
            .lock()
            .expect("rgb channel asset lock poisoned")
            .remove(&pending_tx.user_channel_id);
        self.generated_rgb_funding_transfers
            .lock()
            .expect("generated rgb funding transfer queue lock poisoned")
            .retain(|transfer| transfer.temporary_channel_id.0 != temporary_channel_id.0);
        self.remove_rgb_funding_recovery_records(temporary_channel_id);
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb RGB funding transaction generated: user_channel_id={} temporary_channel_id={} funding_outpoint={}",
                pending_tx.user_channel_id, temporary_channel_id, pending_tx.funding_outpoint
            ));
        Ok(true)
    }

    fn save_rgb_funding_binding(
        &self,
        temporary_channel_id: LnRgbChannelId,
        peer_node_id: PublicKey,
        funding_outpoint: OutPoint,
        transfer: &ValidTransfer,
        staged_stock_dir: &PathBuf,
    ) -> Result<RgbFundingOutpointBinding> {
        let binding_dir = self.rgb_funding_binding_dir();
        fs::create_dir_all(&binding_dir)
            .with_context(|| format!("create RGB funding binding dir {}", binding_dir.display()))?;
        let base = format!("{}-{}", hex32(temporary_channel_id.0), funding_outpoint);
        let transfer_path = binding_dir.join(format!("{base}.rgb-transfer"));
        fs::write(&transfer_path, encode_valid_transfer(transfer)?)
            .with_context(|| format!("save RGB funding transfer {}", transfer_path.display()))?;

        let binding = RgbFundingOutpointBinding {
            temporary_channel_id: hex32(temporary_channel_id.0),
            channel_id: None,
            peer_node_id: peer_node_id.to_string(),
            funding_outpoint: funding_outpoint.to_string(),
            transfer_path: transfer_path.display().to_string(),
            staged_stock_dir: staged_stock_dir.display().to_string(),
            status: "pending_confirmation".to_string(),
            promoted_at: None,
            created_at: now_secs(),
        };
        let binding_path = binding_dir.join(format!("{base}.json"));
        fs::write(&binding_path, serde_json::to_vec_pretty(&binding)?)
            .with_context(|| format!("save RGB funding binding {}", binding_path.display()))?;
        self.rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .insert(temporary_channel_id, binding.clone());
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb RGB funding transfer bound: channel={} funding_outpoint={} transfer={}",
                temporary_channel_id,
                funding_outpoint,
                transfer_path.display()
            ));
        Ok(binding)
    }

    fn handle_spendable_outputs(
        &self,
        channel_id: Option<LnRgbChannelId>,
        outputs: Vec<SpendableOutputDescriptor>,
    ) -> Result<()> {
        let Some(channel_id) = channel_id else {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                    "ln-rgb spendable outputs without channel id: count={}",
                    outputs.len()
                ));
            return Ok(());
        };
        let Some(binding) = self.rgb_funding_binding_for_channel(channel_id) else {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                    "ln-rgb non-RGB spendable outputs observed: channel_id={channel_id} count={}",
                    outputs.len()
                ));
            return Ok(());
        };

        let mut records = Vec::new();
        for output in outputs {
            records.push(self.persist_pending_rgb_sweep(channel_id, &binding, &output)?);
        }
        self.mark_rgb_funding_binding_status_by_channel(channel_id, "pending_rgb_sweep")?;
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb RGB spendable outputs staged for sweep: channel_id={channel_id} count={} funding={}",
                records.len(),
                binding.funding_outpoint
            ));
        Ok(())
    }

    fn persist_pending_rgb_sweep(
        &self,
        channel_id: LnRgbChannelId,
        binding: &RgbFundingOutpointBinding,
        output: &SpendableOutputDescriptor,
    ) -> Result<RgbPendingSweepRecord> {
        let outpoint = spendable_output_outpoint(output);
        let sweep_dir = self.rgb_pending_sweep_dir();
        fs::create_dir_all(&sweep_dir)
            .with_context(|| format!("create RGB pending sweep dir {}", sweep_dir.display()))?;
        let base = format!("{}-{}", hex32(channel_id.0), outpoint);
        let descriptor_path = sweep_dir.join(format!("{base}.ldk-spendable"));
        fs::write(&descriptor_path, output.encode()).with_context(|| {
            format!(
                "write RGB pending sweep descriptor {}",
                descriptor_path.display()
            )
        })?;
        let now = now_secs();
        let record = RgbPendingSweepRecord {
            channel_id: Some(hex32(channel_id.0)),
            temporary_channel_id: Some(binding.temporary_channel_id.clone()),
            funding_outpoint: binding.funding_outpoint.clone(),
            spendable_outpoint: outpoint.to_string(),
            carrier_txid: String::new(),
            descriptor_kind: spendable_output_kind(output).to_string(),
            descriptor_path: descriptor_path.display().to_string(),
            transfer_path: binding.transfer_path.clone(),
            staged_stock_dir: String::new(),
            status: "pending_rgb_sweep".to_string(),
            created_at: now,
            updated_at: now,
        };
        let record_path = sweep_dir.join(format!("{base}.json"));
        fs::write(&record_path, serde_json::to_vec_pretty(&record)?)
            .with_context(|| format!("write RGB pending sweep record {}", record_path.display()))?;
        self.mark_rgb_maturity_record_status(channel_id, "spendable_outputs_emitted")?;
        Ok(record)
    }

    fn persist_rgb_maturity_record(
        &self,
        channel_id: LnRgbChannelId,
        binding: &RgbFundingOutpointBinding,
        amount_satoshis: u64,
        spendable_height: u32,
        source: BalanceSource,
    ) -> Result<()> {
        let maturity_dir = self.rgb_pending_maturity_dir();
        fs::create_dir_all(&maturity_dir).with_context(|| {
            format!("create RGB pending maturity dir {}", maturity_dir.display())
        })?;
        let channel_id_hex = hex32(channel_id.0);
        let record_path = maturity_dir.join(format!("{}.json", channel_id_hex));
        let now = now_secs();
        let mut record = if record_path.exists() {
            serde_json::from_slice::<RgbPendingMaturityRecord>(&fs::read(&record_path)?)
                .with_context(|| format!("read RGB pending maturity {}", record_path.display()))?
        } else {
            RgbPendingMaturityRecord {
                channel_id: channel_id_hex.clone(),
                temporary_channel_id: Some(binding.temporary_channel_id.clone()),
                funding_outpoint: binding.funding_outpoint.clone(),
                amount_satoshis,
                spendable_height,
                source: balance_source_name(&source).to_string(),
                status: "waiting_maturity".to_string(),
                created_at: now,
                updated_at: now,
            }
        };
        if record.status == "spendable_outputs_emitted" || record.status == "confirmed" {
            return Ok(());
        }
        record.temporary_channel_id = Some(binding.temporary_channel_id.clone());
        record.funding_outpoint = binding.funding_outpoint.clone();
        record.amount_satoshis = amount_satoshis;
        record.spendable_height = spendable_height;
        record.source = balance_source_name(&source).to_string();
        record.status = "waiting_maturity".to_string();
        record.updated_at = now;
        fs::write(&record_path, serde_json::to_vec_pretty(&record)?).with_context(|| {
            format!(
                "write RGB pending maturity record {}",
                record_path.display()
            )
        })?;
        Ok(())
    }

    fn mark_rgb_maturity_record_status(
        &self,
        channel_id: LnRgbChannelId,
        status: &str,
    ) -> Result<()> {
        let record_path = self
            .rgb_pending_maturity_dir()
            .join(format!("{}.json", hex32(channel_id.0)));
        if !record_path.exists() {
            return Ok(());
        }
        let mut record: RgbPendingMaturityRecord = serde_json::from_slice(&fs::read(&record_path)?)
            .with_context(|| format!("read RGB pending maturity {}", record_path.display()))?;
        if record.status == status {
            return Ok(());
        }
        record.status = status.to_string();
        record.updated_at = now_secs();
        fs::write(&record_path, serde_json::to_vec_pretty(&record)?).with_context(|| {
            format!(
                "write RGB pending maturity status {}",
                record_path.display()
            )
        })?;
        Ok(())
    }

    fn persist_outbound_rgb_payment_created(&self, request: &RgbPaymentRequest) -> Result<()> {
        let payment_id = hex32(request.payment_id.0);
        let now = now_secs();
        let record = RgbPaymentStateRecord {
            direction: "outbound".to_string(),
            payment_id: Some(payment_id.clone()),
            payment_hash: None,
            peer_node_id: Some(request.recipient_node_id.to_string()),
            amount_msat: request.amount_msat,
            contract_id: request.asset.contract_id.to_string(),
            rgb_amount: request.asset.amount,
            status: "created".to_string(),
            created_at: now,
            updated_at: now,
        };
        self.write_rgb_payment_state(&format!("outbound-{payment_id}"), &record)
    }

    fn mark_outbound_rgb_payment_submitted(
        &self,
        payment_id: PaymentId,
        payment_hash: PaymentHash,
    ) -> Result<()> {
        let payment_id_hex = hex32(payment_id.0);
        self.update_rgb_payment_state(
            &format!("outbound-{payment_id_hex}"),
            "submitted_waiting_btc_result",
            |record| {
                record.payment_hash = Some(hex32(payment_hash.0));
            },
        )
    }

    fn mark_outbound_rgb_payment_status(&self, payment_id: PaymentId, status: &str) -> Result<()> {
        let payment_id_hex = hex32(payment_id.0);
        self.update_rgb_payment_state(&format!("outbound-{payment_id_hex}"), status, |_| {})
    }

    fn persist_inbound_rgb_payment_claimed(
        &self,
        payment_hash: PaymentHash,
        amount_msat: u64,
        contract_id: &str,
        rgb_amount: u64,
    ) -> Result<()> {
        let payment_hash_hex = hex32(payment_hash.0);
        let now = now_secs();
        let record = RgbPaymentStateRecord {
            direction: "inbound".to_string(),
            payment_id: None,
            payment_hash: Some(payment_hash_hex.clone()),
            peer_node_id: None,
            amount_msat,
            contract_id: contract_id.to_string(),
            rgb_amount,
            status: "received_claimed".to_string(),
            created_at: now,
            updated_at: now,
        };
        self.write_rgb_payment_state(&format!("inbound-{payment_hash_hex}"), &record)
    }

    fn update_rgb_payment_state(
        &self,
        key: &str,
        status: &str,
        update: impl FnOnce(&mut RgbPaymentStateRecord),
    ) -> Result<()> {
        let path = self.rgb_payment_state_path(key);
        if !path.exists() {
            return Ok(());
        }
        let mut record: RgbPaymentStateRecord = serde_json::from_slice(&fs::read(&path)?)
            .with_context(|| format!("read RGB payment state {}", path.display()))?;
        update(&mut record);
        record.status = status.to_string();
        record.updated_at = now_secs();
        self.write_rgb_payment_state(key, &record)
    }

    fn write_rgb_payment_state(&self, key: &str, record: &RgbPaymentStateRecord) -> Result<()> {
        let path = self.rgb_payment_state_path(key);
        ensure_parent_dir(&path)?;
        fs::write(&path, serde_json::to_vec_pretty(record)?)
            .with_context(|| format!("write RGB payment state {}", path.display()))
    }

    fn rgb_payment_state_path(&self, key: &str) -> PathBuf {
        self.rgb_payment_state_dir().join(format!("{key}.json"))
    }

    fn rgb_funding_binding_for_channel(
        &self,
        channel_id: LnRgbChannelId,
    ) -> Option<RgbFundingOutpointBinding> {
        let channel_id = hex32(channel_id.0);
        self.rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .values()
            .find(|binding| {
                binding.channel_id.as_deref() == Some(channel_id.as_str())
                    || binding.temporary_channel_id == channel_id
            })
            .cloned()
    }

    fn mark_rgb_funding_binding_channel_id(
        &self,
        funding_txo: OutPoint,
        channel_id: LnRgbChannelId,
    ) -> Result<()> {
        let binding = {
            let mut bindings = self
                .rgb_funding_bindings
                .lock()
                .expect("rgb funding binding lock poisoned");
            let Some((key, binding)) = bindings
                .iter_mut()
                .find(|(_, binding)| binding.funding_outpoint == funding_txo.to_string())
            else {
                return Ok(());
            };
            binding.channel_id = Some(hex32(channel_id.0));
            (*key, binding.clone())
        };
        let (_, binding) = binding;
        let binding_path = self.rgb_funding_binding_json_path(&binding);
        ensure_parent_dir(&binding_path)?;
        fs::write(&binding_path, serde_json::to_vec_pretty(&binding)?)
            .with_context(|| format!("save RGB funding channel id {}", binding_path.display()))?;
        Ok(())
    }

    fn mark_rgb_funding_binding_status_by_channel(
        &self,
        channel_id: LnRgbChannelId,
        status: &str,
    ) -> Result<()> {
        let channel_id_hex = hex32(channel_id.0);
        let binding = {
            let mut bindings = self
                .rgb_funding_bindings
                .lock()
                .expect("rgb funding binding lock poisoned");
            let Some((key, binding)) = bindings.iter_mut().find(|(_, binding)| {
                binding.channel_id.as_deref() == Some(channel_id_hex.as_str())
                    || binding.temporary_channel_id == channel_id_hex
            }) else {
                return Ok(());
            };
            if binding.status == "confirmed" {
                binding.status = status.to_string();
            } else {
                binding.status = status.to_string();
            }
            (*key, binding.clone())
        };
        let (_, binding) = binding;
        let binding_path = self.rgb_funding_binding_json_path(&binding);
        ensure_parent_dir(&binding_path)?;
        fs::write(&binding_path, serde_json::to_vec_pretty(&binding)?).with_context(|| {
            format!(
                "save RGB funding binding sweep status {}",
                binding_path.display()
            )
        })?;
        Ok(())
    }

    fn try_promote_confirmed_rgb_funding_stock(
        &self,
        funding_txo: OutPoint,
    ) -> Result<Option<RgbFundingOutpointBinding>> {
        let mut binding = {
            let bindings = self
                .rgb_funding_bindings
                .lock()
                .expect("rgb funding binding lock poisoned");
            bindings
                .values()
                .find(|binding| binding.funding_outpoint == funding_txo.to_string())
                .cloned()
        };
        let Some(mut binding) = binding.take() else {
            return Ok(None);
        };
        if binding.status == "confirmed" {
            return Ok(Some(binding));
        }
        if binding.staged_stock_dir.is_empty() {
            return Ok(None);
        }
        let staged_stock_dir = PathBuf::from(&binding.staged_stock_dir);
        let txid = binding
            .funding_outpoint
            .parse::<OutPoint>()
            .context("invalid RGB funding outpoint in binding")?
            .txid;
        if !promote_staged_rgb_stock_if_tx_confirmed_with_esploras(
            &self.rgb_stock_dir(),
            &staged_stock_dir,
            self.config.network,
            &self.rotated_esplora_urls(),
            txid,
        )? {
            return Ok(None);
        }
        binding.status = "confirmed".to_string();
        binding.promoted_at = Some(now_secs());
        let binding_path = self.rgb_funding_binding_json_path(&binding);
        fs::write(&binding_path, serde_json::to_vec_pretty(&binding)?).with_context(|| {
            format!(
                "save promoted RGB funding binding {}",
                binding_path.display()
            )
        })?;
        self.rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .insert(
                LnRgbChannelId(hex_to_32(&binding.temporary_channel_id)?),
                binding.clone(),
            );
        Ok(Some(binding))
    }

    fn try_promote_confirmed_rgb_funding_stocks(&self) -> Result<()> {
        let funding_outpoints = self
            .rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .values()
            .filter(|binding| binding.status != "confirmed")
            .filter_map(|binding| binding.funding_outpoint.parse::<OutPoint>().ok())
            .collect::<Vec<_>>();
        for funding_outpoint in funding_outpoints {
            if let Err(err) = self.try_promote_confirmed_rgb_funding_stock(funding_outpoint) {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back(format!(
                        "ln-rgb RGB funding stock promotion skipped: funding={funding_outpoint} error={err:#}"
                    ));
            }
        }
        Ok(())
    }

    fn reconcile_rgb_sweep_records_from_sweeper(
        &self,
        output_sweeper: &LnRgbOutputSweeper,
    ) -> Result<()> {
        let tracked_outputs = output_sweeper.tracked_spendable_outputs();
        let sweep_dir = self.rgb_pending_sweep_dir();
        let Ok(entries) = fs::read_dir(&sweep_dir) else {
            return Ok(());
        };
        let mut updated_records = 0usize;
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let mut record: RgbPendingSweepRecord = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("read RGB pending sweep record {}", path.display()))?;
            if !record.carrier_txid.is_empty() && !record.staged_stock_dir.is_empty() {
                continue;
            }
            let Some(sweep_txid) =
                tracked_sweep_txid_for_outpoint(&tracked_outputs, &record.spendable_outpoint)
            else {
                continue;
            };
            let staged_stock_dir = stage_rgb_stock_for_tx(
                &self.rgb_stock_dir(),
                sweep_txid,
                "pending_rgb_sweep",
                |_stock| Ok(()),
            )?;
            record.carrier_txid = sweep_txid.to_string();
            record.staged_stock_dir = staged_stock_dir.display().to_string();
            record.status = "pending_confirmation".to_string();
            record.updated_at = now_secs();
            fs::write(&path, serde_json::to_vec_pretty(&record)?).with_context(|| {
                format!(
                    "write RGB pending sweep spending tx record {}",
                    path.display()
                )
            })?;
            updated_records += 1;
        }
        if updated_records > 0 {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                    "ln-rgb RGB sweep spending tx staged: records={updated_records}"
                ));
        }
        Ok(())
    }

    fn reconcile_rgb_maturity_records_from_monitors(
        &self,
        chain_monitor: &LnRgbChainMonitor,
    ) -> Result<()> {
        let bindings = self
            .rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .values()
            .filter(|binding| binding.channel_id.is_some())
            .filter(|binding| {
                !matches!(
                    binding.status.as_str(),
                    "pending_rgb_sweep" | "rgb_sweep_confirmed"
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut updated = 0usize;
        for binding in bindings {
            let Some(channel_id_hex) = binding.channel_id.as_deref() else {
                continue;
            };
            let Ok(channel_id) = hex_to_32(channel_id_hex).map(LnRgbChannelId) else {
                continue;
            };
            let Ok(monitor) = chain_monitor.get_monitor(channel_id) else {
                continue;
            };
            for balance in monitor.get_claimable_balances() {
                let Balance::ClaimableAwaitingConfirmations {
                    amount_satoshis,
                    confirmation_height,
                    source,
                } = balance
                else {
                    continue;
                };
                self.persist_rgb_maturity_record(
                    channel_id,
                    &binding,
                    amount_satoshis,
                    confirmation_height,
                    source,
                )?;
                self.mark_rgb_funding_binding_status_by_channel(channel_id, "waiting_maturity")?;
                updated += 1;
            }
        }
        if updated > 0 {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                    "ln-rgb RGB maturity records updated: records={updated}"
                ));
        }
        Ok(())
    }

    fn try_promote_confirmed_rgb_sweep_stocks(&self) -> Result<()> {
        let mut recover_revoked_tx = |txid| self.evict_local_unconfirmed_rgb_tx(txid);
        let report = scan_and_promote_or_revoke_staged_rgb_stocks_with_esploras(
            &self.rgb_stock_dir(),
            self.config.network,
            &self.rotated_esplora_urls(),
            &mut recover_revoked_tx,
        )?;
        if !report.revoked_txids.is_empty() {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                    "ln-rgb RGB pending carrier transactions revoked: txids={:?}",
                    report.revoked_txids
                ));
        }
        if report.promoted_txids.is_empty() {
            return Ok(());
        }
        let promoted = report
            .promoted_txids
            .iter()
            .map(ToString::to_string)
            .collect::<std::collections::HashSet<_>>();
        let sweep_dir = self.rgb_pending_sweep_dir();
        let Ok(entries) = fs::read_dir(&sweep_dir) else {
            return Ok(());
        };
        let mut promoted_records = 0usize;
        let mut promoted_channels = Vec::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let mut record: RgbPendingSweepRecord = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("read RGB pending sweep record {}", path.display()))?;
            if record.carrier_txid.is_empty() || record.staged_stock_dir.is_empty() {
                continue;
            }
            let carrier_txid = record.carrier_txid.clone();
            if !promoted.contains(&carrier_txid) || record.status == "confirmed" {
                continue;
            }
            record.carrier_txid = carrier_txid;
            record.status = "confirmed".to_string();
            record.updated_at = now_secs();
            fs::write(&path, serde_json::to_vec_pretty(&record)?).with_context(|| {
                format!(
                    "write confirmed RGB pending sweep record {}",
                    path.display()
                )
            })?;
            promoted_records += 1;
            if let Some(channel_id) = record.channel_id.as_deref() {
                if let Ok(bytes) = hex_to_32(channel_id) {
                    promoted_channels.push(LnRgbChannelId(bytes));
                }
            }
        }
        for channel_id in promoted_channels {
            self.mark_rgb_funding_binding_status_by_channel(channel_id, "rgb_sweep_confirmed")?;
        }
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb RGB sweep staged stocks promoted: records={} txids={:?}",
                promoted_records, report.promoted_txids
            ));
        Ok(())
    }

    fn load_or_create_output_sweeper(
        &self,
        best_block: BestBlock,
        broadcaster: Arc<LnRgbBroadcaster>,
        fee_estimator: Arc<LnRgbFeeEstimator>,
        tx_sync: Arc<LnRgbTxSync>,
        keys_manager: Arc<KeysManager>,
        sweep_destination: Arc<LnRgbChangeDestinationSource>,
        kv_store: Arc<FilesystemStore>,
        logger: Arc<LnRgbLogger>,
    ) -> Result<LnRgbOutputSweeper> {
        let args = (
            Arc::clone(&broadcaster),
            Arc::clone(&fee_estimator),
            Some(Arc::clone(&tx_sync)),
            Arc::clone(&keys_manager),
            Arc::clone(&sweep_destination),
            Arc::clone(&kv_store),
            Arc::clone(&logger),
        );
        match KVStoreSync::read(
            &*kv_store,
            OUTPUT_SWEEPER_PERSISTENCE_PRIMARY_NAMESPACE,
            OUTPUT_SWEEPER_PERSISTENCE_SECONDARY_NAMESPACE,
            OUTPUT_SWEEPER_PERSISTENCE_KEY,
        ) {
            Ok(bytes) => {
                let mut reader = std::io::Cursor::new(bytes);
                let (_stored_best_block, sweeper) =
                    <(BestBlock, LnRgbOutputSweeper)>::read(&mut reader, args)
                        .map_err(|err| anyhow!("failed to read ln-rgb output sweeper: {err:?}"))?;
                Ok(sweeper)
            }
            Err(_) => Ok(LnRgbOutputSweeper::new(
                best_block,
                broadcaster,
                fee_estimator,
                Some(tx_sync),
                keys_manager,
                sweep_destination,
                kv_store,
                logger,
            )),
        }
    }

    fn rgb_funding_binding_json_path(&self, binding: &RgbFundingOutpointBinding) -> PathBuf {
        self.rgb_funding_binding_dir().join(format!(
            "{}-{}.json",
            binding.temporary_channel_id, binding.funding_outpoint
        ))
    }

    fn mark_rgb_funding_binding_status_if_pending(
        &self,
        funding_txo: OutPoint,
        status: &str,
    ) -> Result<()> {
        let binding = {
            let mut bindings = self
                .rgb_funding_bindings
                .lock()
                .expect("rgb funding binding lock poisoned");
            let Some((key, binding)) = bindings
                .iter_mut()
                .find(|(_, binding)| binding.funding_outpoint == funding_txo.to_string())
            else {
                return Ok(());
            };
            if binding.status == "confirmed" {
                return Ok(());
            }
            binding.status = status.to_string();
            (*key, binding.clone())
        };
        let (_, binding) = binding;
        let binding_path = self.rgb_funding_binding_json_path(&binding);
        ensure_parent_dir(&binding_path)?;
        fs::write(&binding_path, serde_json::to_vec_pretty(&binding)?).with_context(|| {
            format!("save RGB funding binding status {}", binding_path.display())
        })?;
        Ok(())
    }

    fn rgb_funding_binding_dir(&self) -> PathBuf {
        self.config
            .storage_dir
            .join("ln-rgb")
            .join("rgb-funding-bindings")
    }

    fn rgb_pending_funding_transaction_dir(&self) -> PathBuf {
        self.config
            .storage_dir
            .join("ln-rgb")
            .join("pending-rgb-funding-transactions")
    }

    fn rgb_pending_funding_transfer_dir(&self) -> PathBuf {
        self.config
            .storage_dir
            .join("ln-rgb")
            .join("pending-rgb-funding-transfers")
    }

    fn rgb_generated_funding_transfer_dir(&self) -> PathBuf {
        self.config
            .storage_dir
            .join("ln-rgb")
            .join("generated-rgb-funding-transfers")
    }

    fn rgb_pending_sweep_dir(&self) -> PathBuf {
        self.config
            .storage_dir
            .join("ln-rgb")
            .join("pending-rgb-sweeps")
    }

    fn rgb_pending_maturity_dir(&self) -> PathBuf {
        self.config
            .storage_dir
            .join("ln-rgb")
            .join("pending-rgb-maturity")
    }

    fn rgb_payment_state_dir(&self) -> PathBuf {
        self.config.storage_dir.join("ln-rgb").join("rgb-payments")
    }

    fn rgb_stock_dir(&self) -> PathBuf {
        default_rgb_stock_dir(&self.config.l1_data_dir)
    }

    fn is_trusted_0conf_peer(&self, peer_node_id: &PublicKey) -> bool {
        self.config
            .trusted_peers_0conf
            .iter()
            .any(|trusted| trusted == &peer_node_id.to_string())
    }

    fn retry_pending_rgb_funding_recovery(&self, runtime: &LnRgbRuntime) -> Result<usize> {
        let mut channel_ids = self
            .pending_funding_transactions
            .lock()
            .expect("pending funding transaction lock poisoned")
            .keys()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        channel_ids.extend(
            self.pending_rgb_funding
                .lock()
                .expect("pending rgb funding lock poisoned")
                .keys()
                .copied(),
        );

        let mut replayed = 0usize;
        for channel_id in channel_ids {
            let has_pending_tx = self
                .pending_funding_transactions
                .lock()
                .expect("pending funding transaction lock poisoned")
                .contains_key(&channel_id);
            if has_pending_tx {
                if self.try_complete_rgb_funding(&runtime.channel_manager, channel_id)? {
                    runtime.peer_manager.process_events();
                    Self::persist_channel_manager_to_store(
                        &runtime.kv_store,
                        &runtime.channel_manager,
                    )?;
                    replayed += 1;
                }
                continue;
            }

            let pending = self
                .pending_rgb_funding
                .lock()
                .expect("pending rgb funding lock poisoned")
                .get(&channel_id)
                .cloned();
            let Some(pending) = pending else {
                continue;
            };
            match runtime
                .channel_manager
                .provide_funding_rgb_transfer_for_channel(
                    channel_id,
                    pending.peer_node_id,
                    ldk_rgb_funding_transfer(channel_id, &pending.transfer),
                ) {
                Ok(()) => {
                    self.pending_rgb_funding
                        .lock()
                        .expect("pending rgb funding lock poisoned")
                        .remove(&channel_id);
                    self.remove_rgb_funding_recovery_records(channel_id);
                    runtime.peer_manager.process_events();
                    Self::persist_channel_manager_to_store(
                        &runtime.kv_store,
                        &runtime.channel_manager,
                    )?;
                    replayed += 1;
                }
                Err(err) => {
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb pending RGB funding transfer replay deferred: channel={channel_id} error={err:?}"
                        ));
                }
            }
        }
        Ok(replayed)
    }

    fn replay_rgb_funding_bindings(&self, runtime: &LnRgbRuntime) -> Result<usize> {
        let bindings = self
            .rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut replayed = 0usize;
        for binding in bindings {
            let Some(channel_id_hex) = binding.channel_id.as_deref() else {
                continue;
            };
            let channel_id = LnRgbChannelId(hex_to_32(channel_id_hex).with_context(|| {
                format!(
                    "invalid RGB funding binding channel id for funding {}",
                    binding.funding_outpoint
                )
            })?);
            let peer_node_id = binding.peer_node_id.parse::<PublicKey>().with_context(|| {
                format!(
                    "invalid RGB funding binding peer for funding {}",
                    binding.funding_outpoint
                )
            })?;
            let transfer = match read_valid_transfer_file(PathBuf::from(&binding.transfer_path)) {
                Ok(transfer) => transfer,
                Err(err) => {
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb skipped RGB funding binding transfer for funding {}: {err:#}",
                            binding.funding_outpoint
                        ));
                    continue;
                }
            };
            match runtime
                .channel_manager
                .provide_funding_rgb_transfer_for_channel(
                    channel_id,
                    peer_node_id,
                    ldk_rgb_funding_transfer(channel_id, &transfer),
                ) {
                Ok(()) => {
                    runtime.peer_manager.process_events();
                    replayed += 1;
                }
                Err(err) => {
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb RGB funding binding replay deferred: channel={channel_id} funding={} error={err:?}",
                            binding.funding_outpoint
                        ));
                }
            }
        }
        if replayed > 0 {
            Self::persist_channel_manager_to_store(&runtime.kv_store, &runtime.channel_manager)?;
        }
        Ok(replayed)
    }

    fn build_runtime(&self) -> Result<LnRgbRuntime> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("btc-local-wallet-ln-rgb")
            .enable_all()
            .build()
            .context("build ln-rgb tokio runtime")?;
        let logger = Arc::new(LnRgbLogger);
        let keys_manager = Arc::new(KeysManager::new(&self.seed, now_secs(), now_nanos(), true));
        let fee_estimator = Arc::new(LnRgbFeeEstimator);
        let (runtime_chain_source, best_block) = if cfg!(test) {
            (
                ChainSource::Esplora(self.esplora_config(self.config.esplora.clone())),
                BestBlock::from_network(self.config.network),
            )
        } else {
            let mut selected_chain_source = None;
            let mut last_chain_error = None;
            for candidate in self.rotated_chain_sources()? {
                match current_best_block_from_source(self.config.network, &candidate) {
                    Ok(best_block) => {
                        selected_chain_source = Some((candidate, best_block));
                        break;
                    }
                    Err(err) => last_chain_error = Some(err),
                }
            }
            selected_chain_source
                .ok_or_else(|| {
                    last_chain_error.unwrap_or_else(|| anyhow!("no chain source configured"))
                })
                .context("initialize ln-rgb best block from chain source")?
        };
        let broadcaster = Arc::new(LnRgbBroadcaster {
            network: self.config.network,
            chain_source: runtime_chain_source.clone(),
            broadcasted_txs: Mutex::new(Vec::new()),
        });
        let tx_sync = Arc::new(match runtime_chain_source {
            ChainSource::Esplora(config) => {
                LnRgbChainSync::Esplora(EsploraSyncClient::from_client(
                    esplora_client_with_config(&config),
                    Arc::clone(&logger),
                ))
            }
            ChainSource::BitcoinCore(config) => {
                LnRgbChainSync::BitcoinCore(BitcoinCoreTxSync::new(config))
            }
        });
        let storage_dir = self.config.storage_dir.join("ln-rgb").join("ldk-store");
        std::fs::create_dir_all(&storage_dir)
            .with_context(|| format!("create ln-rgb LDK store at {}", storage_dir.display()))?;
        let kv_store = Arc::new(FilesystemStore::new(storage_dir));
        let sweep_mnemonic = self
            .config
            .entropy_mnemonic
            .as_deref()
            .context("ln-rgb sweep destination requires mnemonic from zs config")?;
        let sweep_mnemonic = BdkMnemonic::parse_in_normalized(BdkLanguage::English, sweep_mnemonic)
            .context("invalid ln-rgb sweep mnemonic from runtime config")?;
        let sweep_destination = Arc::new(LnRgbChangeDestinationSource {
            network: self.config.network,
            l1_data_dir: self.config.l1_data_dir.clone(),
            mnemonic: sweep_mnemonic,
        });
        let output_sweeper = Arc::new(self.load_or_create_output_sweeper(
            best_block,
            Arc::clone(&broadcaster),
            Arc::clone(&fee_estimator),
            Arc::clone(&tx_sync),
            Arc::clone(&keys_manager),
            Arc::clone(&sweep_destination),
            Arc::clone(&kv_store),
            Arc::clone(&logger),
        )?);
        let persister = Arc::new(MonitorUpdatingPersister::new(
            Arc::clone(&kv_store),
            Arc::clone(&logger),
            0,
            Arc::clone(&keys_manager),
            Arc::clone(&keys_manager),
            Arc::clone(&broadcaster),
            Arc::clone(&fee_estimator),
        ));
        let chain_monitor = Arc::new(ChainMonitor::new(
            Some(Arc::clone(&tx_sync)),
            Arc::clone(&broadcaster),
            Arc::clone(&logger),
            Arc::clone(&fee_estimator),
            persister,
            Arc::clone(&keys_manager),
            keys_manager.get_peer_storage_key(),
        ));
        let network_graph = Arc::new(NetworkGraph::new(self.config.network, Arc::clone(&logger)));
        let scorer = Arc::new(RwLock::new(ProbabilisticScorer::new(
            ProbabilisticScoringDecayParameters::default(),
            Arc::clone(&network_graph),
            Arc::clone(&logger),
        )));
        let router = Arc::new(DefaultRouter::new(
            Arc::clone(&network_graph),
            Arc::clone(&logger),
            Arc::clone(&keys_manager),
            scorer,
            ProbabilisticScoringFeeParameters::default(),
        ));
        let message_router = Arc::new(DefaultMessageRouter::new(
            Arc::clone(&network_graph),
            Arc::clone(&keys_manager),
        ));
        let mut user_config = UserConfig::default();
        user_config.manually_accept_inbound_channels =
            !self.config.accept_inbound_channels || !self.config.trusted_peers_0conf.is_empty();
        let channel_monitors = read_channel_monitors(
            Arc::clone(&kv_store),
            Arc::clone(&keys_manager),
            Arc::clone(&keys_manager),
        )
        .context("read ln-rgb channel monitors")?;
        let channel_monitor_refs = channel_monitors
            .iter()
            .map(|(_, monitor)| monitor)
            .collect::<Vec<_>>();
        let channel_manager = match KVStoreSync::read(
            &*kv_store,
            CHANNEL_MANAGER_PERSISTENCE_PRIMARY_NAMESPACE,
            CHANNEL_MANAGER_PERSISTENCE_SECONDARY_NAMESPACE,
            CHANNEL_MANAGER_PERSISTENCE_KEY,
        ) {
            Ok(bytes) => {
                let mut reader = std::io::Cursor::new(bytes);
                let read_args = ChannelManagerReadArgs::new(
                    Arc::clone(&keys_manager),
                    Arc::clone(&keys_manager),
                    Arc::clone(&keys_manager),
                    Arc::clone(&fee_estimator),
                    Arc::clone(&chain_monitor),
                    Arc::clone(&broadcaster),
                    Arc::clone(&router),
                    Arc::clone(&message_router),
                    Arc::clone(&logger),
                    user_config,
                    channel_monitor_refs,
                );
                let (_block_hash, channel_manager) =
                    <(bitcoin::BlockHash, Arc<LnRgbChannelManager>)>::read(&mut reader, read_args)
                        .map_err(|err| anyhow!("failed to read ln-rgb channel manager: {err:?}"))?;
                channel_manager
            }
            Err(_) => Arc::new(SimpleArcChannelManager::new(
                Arc::clone(&fee_estimator),
                Arc::clone(&chain_monitor),
                Arc::clone(&broadcaster),
                Arc::clone(&router),
                Arc::clone(&message_router),
                Arc::clone(&logger),
                Arc::clone(&keys_manager),
                Arc::clone(&keys_manager),
                Arc::clone(&keys_manager),
                user_config,
                ChainParameters {
                    network: self.config.network,
                    best_block,
                },
                now_secs() as u32,
            )),
        };
        for (_block_hash, channel_monitor) in channel_monitors {
            let channel_id = channel_monitor.channel_id();
            chain_monitor
                .watch_channel(channel_id, channel_monitor)
                .map_err(|err| anyhow!("failed to watch ln-rgb channel monitor: {err:?}"))?;
        }
        let ignoring_custom_messages = Arc::new(IgnoringMessageHandler {});
        let msg_handler = MessageHandler {
            chan_handler: Arc::clone(&channel_manager),
            route_handler: Arc::new(IgnoringMessageHandler {}),
            onion_message_handler: Arc::new(IgnoringMessageHandler {}),
            custom_message_handler: Arc::clone(&ignoring_custom_messages),
            send_only_message_handler: Arc::new(IgnoringMessageHandler {}),
        };
        let peer_manager = Arc::new(PeerManager::new(
            msg_handler,
            now_secs() as u32,
            &derive_tagged_bytes(&self.seed, b"peer-manager-ephemeral"),
            Arc::clone(&logger),
            Arc::clone(&keys_manager),
        ));
        let iroh_mnemonic = self
            .config
            .entropy_mnemonic
            .as_deref()
            .context("ln-rgb Iroh endpoint requires mnemonic from zs config")?;
        let iroh_endpoint = rt
            .block_on(bind_iroh_endpoint_from_mnemonic(
                iroh_mnemonic,
                self.config.network,
            ))
            .context("bind ln-rgb Iroh endpoint")?;
        let listener_stop = Arc::new(AtomicBool::new(false));
        let peer_task_handles = Arc::new(Mutex::new(Vec::new()));
        let peer_maintenance_stop = Arc::clone(&listener_stop);
        let peer_maintenance_manager = Arc::clone(&peer_manager);
        let peer_maintenance_handle = Some(rt.spawn(async move {
            let mut process_interval = tokio::time::interval(Duration::from_millis(250));
            let mut tick_interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = process_interval.tick() => {
                        if peer_maintenance_stop.load(Ordering::SeqCst) {
                            break;
                        }
                        peer_maintenance_manager.process_events();
                    }
                    _ = tick_interval.tick() => {
                        if peer_maintenance_stop.load(Ordering::SeqCst) {
                            break;
                        }
                        peer_maintenance_manager.timer_tick_occurred();
                        peer_maintenance_manager.process_events();
                    }
                }
            }
        }));
        let sync_stop = Arc::clone(&listener_stop);
        let sync_tx_sync = Arc::clone(&tx_sync);
        let sync_channel_manager = Arc::clone(&channel_manager);
        let sync_chain_monitor = Arc::clone(&chain_monitor);
        let sync_output_sweeper = Arc::clone(&output_sweeper);
        let sync_peer_manager = Arc::clone(&peer_manager);
        let sync_handle = Some(rt.spawn(async move {
            let mut next_delay = LDK_CHAIN_SYNC_SUCCESS_INTERVAL;
            loop {
                tokio::time::sleep(next_delay).await;
                if sync_stop.load(Ordering::SeqCst) {
                    break;
                }
                let result = sync_ldk_chain_once(
                    &sync_tx_sync,
                    &sync_channel_manager,
                    &sync_chain_monitor,
                    &sync_output_sweeper,
                )
                .await;
                match result {
                    Ok(()) => {
                        if sync_output_sweeper
                            .regenerate_and_broadcast_spend_if_necessary()
                            .is_err()
                        {
                            eprintln!("[ln-rgb] output sweeper failed to broadcast spend");
                        }
                        sync_peer_manager.process_events();
                        next_delay = LDK_CHAIN_SYNC_SUCCESS_INTERVAL;
                    }
                    Err(err) => {
                        eprintln!("[ln-rgb] chain sync failed: {err:#}");
                        next_delay = if next_delay < LDK_CHAIN_SYNC_INITIAL_FAILURE_INTERVAL {
                            LDK_CHAIN_SYNC_INITIAL_FAILURE_INTERVAL
                        } else {
                            next_delay
                                .saturating_mul(2)
                                .min(LDK_CHAIN_SYNC_MAX_FAILURE_INTERVAL)
                        };
                    }
                }
            }
        }));
        let listener_handle = match self.listening_addresses().and_then(|mut a| a.pop()) {
            Some(address) => {
                let bind_addr = socket_address_to_std(&address)?;
                let listener = std::net::TcpListener::bind(bind_addr)
                    .with_context(|| format!("bind ln-rgb listener at {bind_addr}"))?;
                listener
                    .set_nonblocking(true)
                    .with_context(|| format!("set ln-rgb listener nonblocking at {bind_addr}"))?;
                let listener = {
                    let _runtime_context = rt.enter();
                    tokio::net::TcpListener::from_std(listener)
                        .with_context(|| format!("create tokio ln-rgb listener at {bind_addr}"))?
                };
                let peer_manager = Arc::clone(&peer_manager);
                let stop = Arc::clone(&listener_stop);
                let peer_task_handles = Arc::clone(&peer_task_handles);
                Some(rt.spawn(async move {
                    loop {
                        if stop.load(Ordering::SeqCst) {
                            break;
                        }
                        match listener.accept().await {
                            Ok((stream, _)) => match stream.into_std() {
                                Ok(stream) => {
                                    let handle = tokio::spawn(lightning_net_tokio::setup_inbound(
                                        Arc::clone(&peer_manager),
                                        stream,
                                    ));
                                    peer_task_handles
                                        .lock()
                                        .expect("ln-rgb peer task lock poisoned")
                                        .push(handle);
                                }
                                Err(err) => eprintln!("[ln-rgb] inbound stream error: {err}"),
                            },
                            Err(err) => {
                                eprintln!("[ln-rgb] accept error: {err}");
                                break;
                            }
                        }
                    }
                }))
            }
            None => None,
        };
        Ok(LnRgbRuntime {
            rt,
            channel_manager,
            _chain_monitor: chain_monitor,
            _tx_sync: tx_sync,
            output_sweeper,
            kv_store,
            peer_manager,
            iroh_endpoint,
            listener_stop,
            listener_handle,
            iroh_listener_handle: None,
            iroh_tunnel_handles: Vec::new(),
            sync_handle,
            event_pump_handle: None,
            peer_maintenance_handle,
            peer_task_handles,
            _keys_manager: keys_manager,
            _logger: logger,
        })
    }
}

impl RgbLnFundingTransferSource for LnRgbBtcLnBackend {
    fn generated_rgb_funding_transfer(&self, channel_id: ChannelId) -> Option<RgbFundingTransfer> {
        LnRgbBtcLnBackend::generated_rgb_funding_transfer(self, channel_id)
    }

    fn take_generated_rgb_funding_transfer(
        &self,
        channel_id: ChannelId,
    ) -> Option<RgbFundingTransfer> {
        LnRgbBtcLnBackend::take_generated_rgb_funding_transfer(self, channel_id)
    }

    fn rgb_channel_funding_outpoint(&self, channel_id: ChannelId) -> Option<OutPoint> {
        self.rgb_funding_binding_for_channel(LnRgbChannelId(channel_id.0))
            .and_then(|binding| binding.funding_outpoint.parse().ok())
    }
}

impl LnRgbBtcLnBackend {
    fn open_standard_channel(&self, request: LnRgbChannelOpenRequest) -> Result<LnRgbChannelId> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let rgb_asset = self
            .rgb_channel_assets
            .lock()
            .expect("rgb channel asset lock poisoned")
            .get(&request.user_channel_id)
            .cloned();
        let mut attempts = 0usize;
        let channel_id = loop {
            attempts += 1;
            let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
            let runtime = runtime_guard
                .as_ref()
                .context("ln-rgb runtime did not start")?;
            let result = if let Some(asset) = rgb_asset.as_ref() {
                runtime.channel_manager.create_rgb_channel(
                    request.peer_node_id,
                    request.capacity_sat,
                    request.push_msat,
                    request.user_channel_id,
                    None,
                    None,
                    RgbChannelContext::new(ldk_rgb_asset(asset)),
                )
            } else {
                runtime.channel_manager.create_channel(
                    request.peer_node_id,
                    request.capacity_sat,
                    request.push_msat,
                    request.user_channel_id,
                    None,
                    None,
                )
            };
            runtime.peer_manager.process_events();
            drop(runtime_guard);

            match result {
                Ok(channel_id) => break channel_id,
                Err(err) if format!("{err:?}").contains("Not connected to node") => {
                    let err = format!("{err:?}");
                    self.events
                        .lock()
                        .expect("ln-rgb event lock poisoned")
                        .push_back(format!(
                            "ln-rgb waiting for peer before channel open: peer={}",
                            request.peer_node_id
                        ));
                    if attempts >= 80 {
                        return Err(anyhow!("LDK create_channel failed: {err}"));
                    }
                    std::thread::sleep(Duration::from_millis(250));
                }
                Err(err) => return Err(anyhow!("LDK create_channel failed: {err:?}")),
            }
        };
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb channel open requested: peer={} channel_id={}",
                request.peer_node_id, channel_id
            ));
        Ok(channel_id)
    }

    fn send_standard_keysend(&self, request: LnRgbKeysendRequest) -> Result<PaymentHash> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        let route_params = RouteParameters::from_payment_params_and_value(
            PaymentParameters::for_keysend(request.recipient_node_id, 40, false),
            request.amount_msat,
        );
        let payment_preimage = PaymentPreimage(derive_tagged_bytes(
            &self.seed,
            format!(
                "keysend-preimage:{}:{}:{}",
                request.recipient_node_id,
                request.amount_msat,
                hex32(request.payment_id.0)
            )
            .as_bytes(),
        ));
        let rgb_asset = self
            .rgb_payment_assets
            .lock()
            .expect("rgb payment asset lock poisoned")
            .remove(&hex32(request.payment_id.0));
        let payment_hash = if let Some(asset) = rgb_asset {
            match runtime.channel_manager.send_rgb_spontaneous_payment(
                Some(payment_preimage),
                RecipientOnionFields::spontaneous_empty(),
                request.payment_id,
                route_params,
                lightning::ln::channelmanager::Retry::Attempts(1),
                RgbPaymentMetadata::new(ldk_rgb_asset(&asset)),
            ) {
                Ok(payment_hash) => {
                    self.mark_outbound_rgb_payment_submitted(request.payment_id, payment_hash)?;
                    payment_hash
                }
                Err(err) => {
                    self.mark_outbound_rgb_payment_status(request.payment_id, "submission_failed")?;
                    return Err(anyhow!("LDK keysend failed: {err:?}"));
                }
            }
        } else {
            runtime
                .channel_manager
                .send_spontaneous_payment(
                    Some(payment_preimage),
                    RecipientOnionFields::spontaneous_empty(),
                    request.payment_id,
                    route_params,
                    lightning::ln::channelmanager::Retry::Attempts(1),
                )
                .map_err(|err| anyhow!("LDK keysend failed: {err:?}"))?
        };
        runtime.peer_manager.process_events();
        drop(runtime_guard);
        self.poll_ldk_events();
        Ok(payment_hash)
    }

    fn attach_rgb_funding_transfer(
        &self,
        temporary_channel_id: LnRgbChannelId,
        peer_node_id: PublicKey,
        transfer: ValidTransfer,
    ) -> Result<()> {
        let pending = PendingRgbFundingTransfer {
            peer_node_id,
            transfer: transfer.clone(),
        };
        self.persist_pending_rgb_funding_transfer(temporary_channel_id, &pending)?;
        self.pending_rgb_funding
            .lock()
            .expect("pending rgb funding lock poisoned")
            .insert(temporary_channel_id, pending);
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb RGB funding transfer queued: channel={temporary_channel_id} peer={peer_node_id}"
            ));
        let managers = {
            let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
            runtime_guard.as_ref().map(|runtime| {
                (
                    Arc::clone(&runtime.channel_manager),
                    Arc::clone(&runtime.peer_manager),
                    Arc::clone(&runtime.kv_store),
                )
            })
        };
        if let Some((channel_manager, peer_manager, kv_store)) = managers {
            if self.try_complete_rgb_funding(&channel_manager, temporary_channel_id)? {
                peer_manager.process_events();
            } else {
                channel_manager
                    .provide_funding_rgb_transfer_for_unfunded_channel(
                        temporary_channel_id,
                        peer_node_id,
                        ldk_rgb_funding_transfer(temporary_channel_id, &transfer),
                    )
                    .map_err(|err| anyhow!("LDK rejected RGB funding transfer: {err:?}"))?;
                peer_manager.process_events();
            }
            Self::persist_channel_manager_to_store(&kv_store, &channel_manager)?;
        }
        Ok(())
    }
}

impl RgbLnNode for LnRgbBtcLnBackend {
    fn open_rgb_channel(&self, request: RgbChannelOpenRequest) -> Result<ChannelId> {
        self.rgb_channel_assets
            .lock()
            .expect("rgb channel asset lock poisoned")
            .insert(request.user_channel_id, request.asset);
        let channel_id = self.open_standard_channel(LnRgbChannelOpenRequest {
            peer_node_id: request.peer_node_id,
            capacity_sat: request.capacity_sat,
            push_msat: request.push_msat,
            user_channel_id: request.user_channel_id,
        })?;
        Ok(ChannelId(channel_id.0))
    }

    fn provide_funding_transfer(&self, funding: RgbFundingTransfer) -> Result<()> {
        self.attach_rgb_funding_transfer(
            LnRgbChannelId(funding.temporary_channel_id.0),
            funding.peer_node_id,
            funding.transfer,
        )
    }

    fn send_rgb_payment(&self, request: RgbPaymentRequest) -> Result<()> {
        self.persist_outbound_rgb_payment_created(&request)?;
        self.rgb_payment_assets
            .lock()
            .expect("rgb payment asset lock poisoned")
            .insert(hex32(request.payment_id.0), request.asset);
        self.send_standard_keysend(LnRgbKeysendRequest {
            recipient_node_id: request.recipient_node_id,
            amount_msat: request.amount_msat,
            payment_id: PaymentId(request.payment_id.0),
        })?;
        Ok(())
    }

    fn mark_rgb_payment_receiver_accepting(
        &self,
        payment_id: crate::lnnode::PaymentId,
    ) -> Result<()> {
        self.mark_outbound_rgb_payment_status(PaymentId(payment_id.0), "rgb_accepting")
    }

    fn mark_rgb_payment_receiver_accepted(
        &self,
        payment_id: crate::lnnode::PaymentId,
    ) -> Result<()> {
        self.mark_outbound_rgb_payment_status(PaymentId(payment_id.0), "rgb_accepted")
    }
}

impl BtcLnNode for LnRgbBtcLnBackend {
    fn start(&self) -> Result<()> {
        if self.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        let loaded_bindings = self
            .load_rgb_funding_bindings_from_disk()
            .context("load RGB funding bindings")?;
        let loaded_pending_funding_txs = self
            .load_pending_funding_transactions_from_disk()
            .context("load pending RGB funding transactions")?;
        let loaded_pending_rgb_transfers = self
            .load_pending_rgb_funding_transfers_from_disk()
            .context("load pending RGB funding transfers")?;
        let loaded_generated_transfers = self
            .load_generated_rgb_funding_transfers_from_disk()
            .context("load generated RGB funding transfers")?;
        let loaded_payment_states = self
            .load_rgb_payment_states_from_disk()
            .context("load RGB payment states")?;
        let runtime = self.build_runtime()?;
        let replayed_rgb_funding = self
            .retry_pending_rgb_funding_recovery(&runtime)
            .context("replay pending RGB funding")?;
        let replayed_rgb_bindings = self
            .replay_rgb_funding_bindings(&runtime)
            .context("replay RGB funding bindings")?;
        *self.runtime.lock().expect("ln-rgb runtime lock poisoned") = Some(runtime);
        self.started.store(true, Ordering::SeqCst);
        let owner = self
            .self_weak
            .lock()
            .expect("ln-rgb self weak lock poisoned")
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(owner) = owner {
            owner.spawn_event_pump()?;
            owner.spawn_iroh_listener()?;
            owner.spawn_configured_iroh_tunnels()?;
        }
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back("ln-rgb peer runtime started".to_string());
        if loaded_bindings > 0 {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                    "ln-rgb RGB funding bindings loaded: count={loaded_bindings}"
                ));
        }
        if loaded_pending_funding_txs
            + loaded_pending_rgb_transfers
            + loaded_generated_transfers
            + loaded_payment_states
            + replayed_rgb_funding
            + replayed_rgb_bindings
            > 0
        {
            self.events
                .lock()
                .expect("ln-rgb event lock poisoned")
                .push_back(format!(
                    "ln-rgb durable RGB state loaded: pending_funding_txs={loaded_pending_funding_txs} pending_transfers={loaded_pending_rgb_transfers} generated_transfers={loaded_generated_transfers} payment_states={loaded_payment_states} replayed_funding={replayed_rgb_funding} replayed_bindings={replayed_rgb_bindings}"
                ));
        }
        Ok(())
    }

    fn stop(&self) -> Result<()> {
        if let Some(mut runtime) = self
            .runtime
            .lock()
            .expect("ln-rgb runtime lock poisoned")
            .take()
        {
            Self::persist_channel_manager_to_store(&runtime.kv_store, &runtime.channel_manager)?;
            self.started.store(false, Ordering::SeqCst);
            runtime.listener_stop.store(true, Ordering::SeqCst);
            runtime.peer_manager.disconnect_all_peers();
            runtime.peer_manager.process_events();
            if let Some(handle) = runtime.listener_handle.take() {
                handle.abort();
            }
            if let Some(handle) = runtime.iroh_listener_handle.take() {
                handle.abort();
            }
            for handle in runtime.iroh_tunnel_handles.drain(..) {
                handle.abort();
            }
            if let Some(handle) = runtime.sync_handle.take() {
                handle.abort();
            }
            if let Some(handle) = runtime.event_pump_handle.take() {
                handle.abort();
            }
            if let Some(handle) = runtime.peer_maintenance_handle.take() {
                handle.abort();
            }
            for handle in runtime
                .peer_task_handles
                .lock()
                .expect("ln-rgb peer task lock poisoned")
                .drain(..)
            {
                handle.abort();
            }
            let endpoint = runtime.iroh_endpoint.clone();
            let close = runtime.rt.block_on(async {
                tokio::time::timeout(Duration::from_secs(3), endpoint.close()).await
            });
            if close.is_err() {
                self.events
                    .lock()
                    .expect("ln-rgb event lock poisoned")
                    .push_back("ln-rgb Iroh endpoint close timed out".to_string());
            }
        }
        self.started.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn node_id(&self) -> PublicKey {
        self.node_id
    }

    fn status_summary(&self) -> String {
        format!(
            "ln-rgb runtime version={} started={} storage={}",
            LN_RGB_LIGHTNING_VERSION,
            self.started.load(Ordering::SeqCst),
            self.config.storage_dir.display()
        )
    }

    fn listening_addresses(&self) -> Option<Vec<SocketAddress>> {
        self.config
            .listen
            .as_deref()
            .and_then(|listen| listen.parse().ok())
            .map(|address| vec![address])
    }

    fn announcement_addresses(&self) -> Option<Vec<SocketAddress>> {
        self.listening_addresses()
    }

    fn next_event_debug(&self) -> Option<String> {
        self.poll_ldk_events_fast();
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .pop_front()
    }

    fn next_btc_ln_event(&self) -> Option<BtcLnEvent> {
        self.poll_ldk_events_fast();
        self.btc_events
            .lock()
            .expect("ln-rgb btc event lock poisoned")
            .pop_front()
    }

    fn event_handled(&self) -> Result<()> {
        Ok(())
    }

    fn new_onchain_address(&self) -> Result<String> {
        bail!(
            "ln-rgb backend uses the btc-local-wallet L1 wallet for on-chain addresses; call the L1 wallet address function instead"
        )
    }

    fn balance_snapshot(&self) -> BtcLnBalanceSnapshot {
        BtcLnBalanceSnapshot {
            total_onchain_balance_sats: 0,
            spendable_onchain_balance_sats: 0,
            total_anchor_channels_reserve_sats: 0,
            total_lightning_balance_sats: 0,
            lightning_balances: "[]".to_string(),
            pending_channel_closure_sweeps: "[]".to_string(),
        }
    }

    fn peer_snapshots(&self) -> Vec<BtcLnPeerSnapshot> {
        self.peers
            .lock()
            .expect("ln-rgb peers lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn channel_snapshots(&self) -> Vec<BtcLnChannelSnapshot> {
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let Some(runtime) = runtime_guard.as_ref() else {
            return Vec::new();
        };
        runtime
            .channel_manager
            .list_channels()
            .into_iter()
            .map(|channel| BtcLnChannelSnapshot {
                user_channel_id: channel.user_channel_id.to_string(),
                counterparty_node_id: channel.counterparty.node_id,
                channel_value_sats: channel.channel_value_satoshis,
                is_outbound: channel.is_outbound,
                is_channel_ready: channel.is_channel_ready,
                is_usable: channel.is_usable,
                channel_id: channel.channel_id.to_string(),
                outbound_capacity_msat: channel.outbound_capacity_msat,
                next_outbound_htlc_limit_msat: channel.next_outbound_htlc_limit_msat,
                inbound_capacity_msat: channel.inbound_capacity_msat,
                funding_txo: channel.funding_txo.map(|outpoint| outpoint.to_string()),
            })
            .collect()
    }

    fn connect(&self, node_id: PublicKey, address: SocketAddress, persist: bool) -> Result<()> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let socket_addr = socket_address_to_std(&address)?;
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        let peer_manager = Arc::clone(&runtime.peer_manager);
        if peer_manager.peer_by_node_id(&node_id).is_none() {
            let peer_task_handles = Arc::clone(&runtime.peer_task_handles);
            let connect_peer_manager = Arc::clone(&peer_manager);
            let connect_handle = runtime.rt.spawn(async move {
                match lightning_net_tokio::connect_outbound(
                    Arc::clone(&connect_peer_manager),
                    node_id,
                    socket_addr,
                )
                .await
                {
                    Some(outbound) => {
                        let handle = tokio::spawn(async move {
                            outbound.await;
                            eprintln!("[ln-rgb] peer connection closed: {node_id}@{socket_addr}");
                        });
                        peer_task_handles
                            .lock()
                            .expect("ln-rgb peer task lock poisoned")
                            .push(handle);
                    }
                    None => {
                        eprintln!("[ln-rgb] failed to connect peer {node_id}@{socket_addr}");
                    }
                }
            });
            runtime
                .peer_task_handles
                .lock()
                .expect("ln-rgb peer task lock poisoned")
                .push(connect_handle);
        }
        drop(runtime_guard);

        let deadline = Instant::now() + Duration::from_secs(10);
        while peer_manager.peer_by_node_id(&node_id).is_none() {
            if Instant::now() >= deadline {
                bail!("ln-rgb peer {node_id}@{socket_addr} did not connect within 10s");
            }
            peer_manager.process_events();
            std::thread::sleep(Duration::from_millis(25));
        }
        self.peers
            .lock()
            .expect("ln-rgb peers lock poisoned")
            .insert(
                node_id,
                BtcLnPeerSnapshot {
                    node_id,
                    address,
                    is_persisted: persist,
                    is_connected: true,
                },
            );
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!("ln-rgb peer connected: {node_id}@{socket_addr}"));
        Ok(())
    }

    fn open_channel(&self, request: BtcLnChannelOpenRequest) -> Result<String> {
        self.connect(request.peer_node_id, request.address, true)?;
        let channel_id = self.open_standard_channel(LnRgbChannelOpenRequest {
            peer_node_id: request.peer_node_id,
            capacity_sat: request.amount_sats,
            push_msat: request.push_msat.unwrap_or(0),
            user_channel_id: derive_user_channel_id(
                &self.seed,
                request.peer_node_id,
                request.amount_sats,
                request.push_msat.unwrap_or(0),
            ),
        })?;
        Ok(channel_id.to_string())
    }

    fn close_channel(&self, request: BtcLnChannelCloseRequest) -> Result<()> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let channel_id = parse_channel_id(&request.channel_id)?;
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        if request.force {
            runtime
                .channel_manager
                .force_close_broadcasting_latest_txn(
                    &channel_id,
                    &request.counterparty_node_id,
                    request
                        .reason
                        .unwrap_or_else(|| "btc-local-wallet requested force close".to_string()),
                )
                .map_err(|err| anyhow!("LDK force-close failed: {err:?}"))?;
        } else {
            runtime
                .channel_manager
                .close_channel(&channel_id, &request.counterparty_node_id)
                .map_err(|err| anyhow!("LDK cooperative close failed: {err:?}"))?;
        }
        runtime.peer_manager.process_events();
        Self::persist_channel_manager_to_store(&runtime.kv_store, &runtime.channel_manager)?;
        drop(runtime_guard);
        self.poll_ldk_events();
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb channel close requested: channel_id={} peer={} force={}",
                request.channel_id, request.counterparty_node_id, request.force
            ));
        Ok(())
    }

    fn receive_bolt11(&self, request: BtcLnBolt11InvoiceRequest) -> Result<Bolt11Invoice> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        let (payment_hash, payment_secret) = runtime
            .channel_manager
            .create_inbound_payment(Some(request.amount_msat), request.expiry_secs, Some(144))
            .map_err(|_| anyhow!("LDK create_inbound_payment failed"))?;
        let counter = self.invoice_counter.fetch_add(1, Ordering::SeqCst);
        let secp = Secp256k1::new();
        let invoice = InvoiceBuilder::new(invoice_currency(self.config.network)?)
            .amount_milli_satoshis(request.amount_msat)
            .invoice_description(request.description)
            .payment_hash(sha256::Hash::from_byte_array(payment_hash.0))
            .payment_secret(payment_secret)
            .duration_since_epoch(Duration::from_secs(now_secs()))
            .expiry_time(Duration::from_secs(request.expiry_secs.into()))
            .min_final_cltv_expiry_delta(144)
            .basic_mpp()
            .payee_pub_key(self.node_id)
            .build_signed(|hash| secp.sign_ecdsa_recoverable(hash, &self.node_secret))
            .map_err(|err| anyhow!("build BOLT11 invoice: {err:?}"))?;
        self.events
            .lock()
            .expect("ln-rgb event lock poisoned")
            .push_back(format!(
                "ln-rgb BOLT11 invoice created: payment_hash={} counter={counter}",
                hex32(payment_hash.0)
            ));
        Ok(invoice)
    }

    fn pay_bolt11(&self, request: BtcLnBolt11PaymentRequest) -> Result<String> {
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        let payment_id = payment_id_from_bytes(request.invoice.to_string().as_bytes());
        runtime
            .channel_manager
            .pay_for_bolt11_invoice(
                &request.invoice,
                payment_id,
                None,
                RouteParametersConfig::default(),
                lightning::ln::channelmanager::Retry::Attempts(1),
            )
            .map_err(|err| anyhow!("LDK BOLT11 payment failed: {err:?}"))?;
        runtime.peer_manager.process_events();
        drop(runtime_guard);
        self.poll_ldk_events();
        Ok(hex32(payment_id.0))
    }

    fn send_keysend(&self, request: BtcLnKeysendRequest) -> Result<String> {
        let payment_id = payment_id_from_bytes(
            format!("{}:{}", request.recipient_node_id, request.amount_msat).as_bytes(),
        );
        let payment_hash = self.send_standard_keysend(LnRgbKeysendRequest {
            recipient_node_id: request.recipient_node_id,
            amount_msat: request.amount_msat,
            payment_id,
        })?;
        Ok(hex32(payment_hash.0))
    }
}

fn derive_seed(config: &BtcLnRuntimeConfig) -> [u8; 32] {
    let material = config.entropy_mnemonic.as_deref().unwrap_or_else(|| {
        config
            .storage_dir
            .to_str()
            .unwrap_or("btc-local-wallet-ln-rgb")
    });
    sha256::Hash::hash(material.as_bytes()).to_byte_array()
}

fn derive_tagged_bytes(seed: &[u8; 32], tag: &[u8]) -> [u8; 32] {
    let mut engine = sha256::Hash::engine();
    engine.input(seed);
    engine.input(tag);
    sha256::Hash::from_engine(engine).to_byte_array()
}

fn derive_user_channel_id(
    seed: &[u8; 32],
    peer_node_id: PublicKey,
    amount_sats: u64,
    push_msat: u64,
) -> u128 {
    let mut engine = sha256::Hash::engine();
    engine.input(seed);
    engine.input(&peer_node_id.serialize());
    engine.input(&amount_sats.to_be_bytes());
    engine.input(&push_msat.to_be_bytes());
    let bytes = sha256::Hash::from_engine(engine).to_byte_array();
    u128::from_be_bytes(bytes[0..16].try_into().expect("16-byte channel id"))
}

fn parse_channel_id(value: &str) -> Result<LnRgbChannelId> {
    let hex = value.trim();
    if hex.len() != 64 {
        bail!("channel_id must be 64 hex chars, got `{value}`");
    }
    let mut bytes = [0u8; 32];
    for (idx, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(chunk).context("channel_id is not valid UTF-8")?;
        bytes[idx] = u8::from_str_radix(pair, 16)
            .with_context(|| format!("channel_id contains invalid hex at byte {idx}"))?;
    }
    Ok(LnRgbChannelId(bytes))
}

fn payment_id_from_bytes(bytes: &[u8]) -> PaymentId {
    PaymentId(sha256::Hash::hash(bytes).to_byte_array())
}

fn parse_hex32(value: &str) -> Result<[u8; 32]> {
    let value = value.trim();
    if value.len() != 64 {
        bail!("expected 32-byte hex string");
    }
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
            .with_context(|| format!("invalid hex byte at offset {offset}"))?;
    }
    Ok(bytes)
}

fn ldk_rgb_asset(asset: &RgbAssetAmount) -> LdkRgbAssetAmount {
    let bytes = parse_hex32(&asset.contract_id.to_string())
        .expect("rgbstd ContractId must render as 32-byte hex");
    LdkRgbAssetAmount::new(lightning::rgb::ContractId::from(bytes), asset.amount)
}

fn funding_outpoint_from_tx(
    tx: &Transaction,
    output_script: &ScriptBuf,
    channel_value_satoshis: u64,
) -> Option<OutPoint> {
    tx.output
        .iter()
        .position(|output| {
            output.script_pubkey == *output_script
                && output.value == Amount::from_sat(channel_value_satoshis)
        })
        .map(|vout| OutPoint::new(tx.compute_txid(), vout as u32))
}

fn spendable_output_outpoint(output: &SpendableOutputDescriptor) -> OutPoint {
    match output {
        SpendableOutputDescriptor::StaticOutput { outpoint, .. } => {
            outpoint.into_bitcoin_outpoint()
        }
        SpendableOutputDescriptor::DelayedPaymentOutput(descriptor) => {
            descriptor.outpoint.into_bitcoin_outpoint()
        }
        SpendableOutputDescriptor::StaticPaymentOutput(descriptor) => {
            descriptor.outpoint.into_bitcoin_outpoint()
        }
    }
}

fn spendable_output_kind(output: &SpendableOutputDescriptor) -> &'static str {
    match output {
        SpendableOutputDescriptor::StaticOutput { .. } => "static_output",
        SpendableOutputDescriptor::DelayedPaymentOutput(_) => "delayed_payment_output",
        SpendableOutputDescriptor::StaticPaymentOutput(_) => "static_payment_output",
    }
}

fn tracked_sweep_txid_for_outpoint(
    tracked_outputs: &[TrackedSpendableOutput],
    spendable_outpoint: &str,
) -> Option<Txid> {
    tracked_outputs
        .iter()
        .find(|tracked| {
            tracked
                .descriptor
                .spendable_outpoint()
                .into_bitcoin_outpoint()
                .to_string()
                == spendable_outpoint
        })
        .and_then(|tracked| match &tracked.status {
            OutputSpendStatus::PendingFirstConfirmation {
                latest_spending_tx, ..
            }
            | OutputSpendStatus::PendingThresholdConfirmations {
                latest_spending_tx, ..
            } => Some(latest_spending_tx.compute_txid()),
            OutputSpendStatus::PendingInitialBroadcast { .. } => None,
        })
}

fn balance_source_name(source: &BalanceSource) -> &'static str {
    match source {
        BalanceSource::HolderForceClosed => "holder_force_closed",
        BalanceSource::CounterpartyForceClosed => "counterparty_force_closed",
        BalanceSource::CoopClose => "coop_close",
        BalanceSource::Htlc => "htlc",
    }
}

fn ensure_parent_dir(path: &PathBuf) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent directory {}", parent.display()))?;
    }
    Ok(())
}

fn is_transient_esplora_error(err: &anyhow::Error) -> bool {
    let message = format!("{err:#}").to_ascii_lowercase();
    message.contains("esplora sync failed")
        && (message.contains("unexpected eof")
            || message.contains("connection reset")
            || message.contains("ssl_error")
            || message.contains("timeout")
            || message.contains("timed out")
            || message.contains("network")
            || message.contains("minreq")
            || message.contains("429")
            || message.contains("too many requests")
            || message.contains("502")
            || message.contains("503")
            || message.contains("504")
            || message.contains("bad gateway")
            || message.contains("service unavailable")
            || message.contains("gateway timeout"))
}

fn current_best_block_from_source(network: Network, source: &ChainSource) -> Result<BestBlock> {
    match source {
        ChainSource::Esplora(config) => current_best_block(network, config),
        ChainSource::BitcoinCore(config) => {
            let tip = bitcoin_core_chain_tip(config)?;
            Ok(BestBlock::new(tip.hash, tip.height))
        }
    }
}

fn accept_rgb_assignment_payload(
    stock_dir: &PathBuf,
    network: Network,
    chain_source: &ChainSource,
    envelope: &crate::iroh_transport::IrohAssignmentEnvelope,
    payload: &[u8],
) -> Result<()> {
    if envelope.kind != "rgb20.transfer.consignment" && envelope.kind != "rgb.transfer.consignment"
    {
        bail!("unsupported Iroh assignment kind: {}", envelope.kind);
    }
    let _txid = envelope
        .txid
        .parse::<Txid>()
        .with_context(|| format!("invalid assignment txid: {}", envelope.txid))?;
    let _recipient_outpoint = envelope
        .recipient_outpoint
        .parse::<OutPoint>()
        .with_context(|| {
            format!(
                "invalid assignment recipient_outpoint: {}",
                envelope.recipient_outpoint
            )
        })?;
    let envelope_contract_id = envelope
        .contract_id
        .parse::<ContractId>()
        .with_context(|| format!("invalid assignment contract_id: {}", envelope.contract_id))?;
    let consignment = rgb20::decode_rgb20_transfer_consignment(payload)
        .context("decode Iroh RGB consignment payload")?;
    if consignment.contract_id() != envelope_contract_id {
        bail!(
            "Iroh assignment contract_id mismatch: envelope {}, payload {}",
            envelope.contract_id,
            consignment.contract_id()
        );
    }
    let valid = rgb20::validate_rgb20_transfer_with_chain_source(
        network,
        chain_source,
        consignment.clone(),
    )?;
    let status = valid.validation_status();
    if format!("{:?}", status.validity()) != "Valid" {
        bail!("Iroh RGB consignment validation failed: {status:?}");
    }
    rgb20::accept_rgb20_transfer_with_chain_source(stock_dir, network, chain_source, consignment)?;
    Ok(())
}

const BEST_BLOCK_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct BestBlockCacheEntry {
    fetched_at: Instant,
    best_block: BestBlock,
}

static BEST_BLOCK_CACHE: OnceLock<Mutex<HashMap<(Network, String), BestBlockCacheEntry>>> =
    OnceLock::new();

fn cached_best_block(network: Network, esplora_url: &str) -> Option<BestBlock> {
    let cache = BEST_BLOCK_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().expect("best block cache poisoned");
    let key = (network, esplora_url.to_string());
    let Some(entry) = cache.get(&key) else {
        return None;
    };
    if entry.fetched_at.elapsed() <= BEST_BLOCK_CACHE_TTL {
        return Some(entry.best_block.clone());
    }
    cache.remove(&key);
    None
}

fn cache_best_block(network: Network, esplora_url: &str, best_block: BestBlock) {
    let cache = BEST_BLOCK_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    cache.lock().expect("best block cache poisoned").insert(
        (network, esplora_url.to_string()),
        BestBlockCacheEntry {
            fetched_at: Instant::now(),
            best_block,
        },
    );
}

fn current_best_block(network: Network, esplora: &EsploraConfig) -> Result<BestBlock> {
    if let Some(best_block) = cached_best_block(network, &esplora.url) {
        return Ok(best_block);
    }
    let mut last_err = None;
    const MAX_BEST_BLOCK_ATTEMPTS: u64 = 6;
    for attempt in 1..=MAX_BEST_BLOCK_ATTEMPTS {
        match current_best_block_once(network, esplora) {
            Ok(best_block) => {
                cache_best_block(network, &esplora.url, best_block);
                return Ok(best_block);
            }
            Err(err) if attempt < MAX_BEST_BLOCK_ATTEMPTS => {
                last_err = Some(err);
                std::thread::sleep(Duration::from_secs(attempt));
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_err.expect("current best block retry loop runs at least once"))
}

fn current_best_block_once(_network: Network, esplora: &EsploraConfig) -> Result<BestBlock> {
    let client = esplora_client_with_config(esplora);
    match client.get_block_infos(None) {
        Ok(blocks) => {
            if let Some(tip) = blocks.into_iter().next() {
                return Ok(BestBlock::new(tip.id, tip.height));
            }
        }
        Err(err) => {
            eprintln!(
                "[ln-rgb] failed to fetch Esplora block summary from {}: {err:?}; falling back to tip hash",
                esplora.url
            );
        }
    }
    let tip_hash = client
        .get_tip_hash()
        .with_context(|| format!("failed to fetch Esplora tip hash from {}", esplora.url))?;
    let status = client
        .get_block_status(&tip_hash)
        .with_context(|| format!("failed to fetch Esplora tip status from {}", esplora.url))?;
    let height = status
        .height
        .context("Esplora tip status is missing block height")?;
    Ok(BestBlock::new(tip_hash, height))
}

async fn sync_ldk_chain_once(
    tx_sync: &Arc<LnRgbTxSync>,
    channel_manager: &Arc<LnRgbChannelManager>,
    chain_monitor: &Arc<LnRgbChainMonitor>,
    output_sweeper: &Arc<LnRgbOutputSweeper>,
) -> Result<()> {
    let confirmables: Vec<&(dyn Confirm + Sync + Send)> = vec![
        channel_manager.as_ref() as &(dyn Confirm + Sync + Send),
        chain_monitor.as_ref() as &(dyn Confirm + Sync + Send),
        output_sweeper.as_ref() as &(dyn Confirm + Sync + Send),
    ];
    tx_sync.sync(confirmables).await
}

fn hex32(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    anyhow::ensure!(
        value.len() % 2 == 0,
        "expected even-length hex string, got {} chars",
        value.len()
    );
    value
        .as_bytes()
        .chunks_exact(2)
        .enumerate()
        .map(|(index, chunk)| {
            let hex = std::str::from_utf8(chunk).context("invalid utf8 in hex string")?;
            u8::from_str_radix(hex, 16)
                .with_context(|| format!("invalid hex byte at offset {}", index * 2))
        })
        .collect()
}

fn hex_to_32(value: &str) -> Result<[u8; 32]> {
    anyhow::ensure!(
        value.len() == 64,
        "expected 32-byte hex string, got {} chars",
        value.len()
    );
    let mut bytes = [0u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let hex = std::str::from_utf8(chunk).context("invalid utf8 in hex string")?;
        bytes[index] = u8::from_str_radix(hex, 16)
            .with_context(|| format!("invalid hex byte at offset {}", index * 2))?;
    }
    Ok(bytes)
}

fn read_valid_transfer_file(path: PathBuf) -> Result<ValidTransfer> {
    let _ = path;
    bail!("persisted ValidTransfer files are not supported by this rgb-service crate set")
}

fn encode_valid_transfer(transfer: &ValidTransfer) -> Result<Vec<u8>> {
    let _ = transfer;
    Ok(Vec::new())
}

fn ldk_rgb_funding_transfer(
    channel_id: LnRgbChannelId,
    transfer: &ValidTransfer,
) -> LdkRgbFundingTransfer {
    let channel = hex32(channel_id.0);
    LdkRgbFundingTransfer::new(RgbFundingRef::new(
        transfer.contract_id().to_string(),
        channel.clone(),
        Some(channel),
    ))
}

fn default_rgb_funding_status() -> String {
    "pending_confirmation".to_string()
}

fn socket_address_to_std(address: &SocketAddress) -> Result<SocketAddr> {
    address
        .to_string()
        .to_socket_addrs()
        .with_context(|| format!("resolve LN socket address {address}"))?
        .next()
        .ok_or_else(|| anyhow!("LN socket address did not resolve: {address}"))
}

fn invoice_currency(network: Network) -> Result<Currency> {
    match network {
        Network::Bitcoin => Ok(Currency::Bitcoin),
        Network::Testnet | Network::Testnet4 => Ok(Currency::BitcoinTestnet),
        Network::Signet => Ok(Currency::Signet),
        Network::Regtest => Ok(Currency::Regtest),
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_nanos() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version;
    use bitcoin::{Amount, Network, OutPoint, ScriptBuf, Transaction, TxOut, Txid};
    use lightning::chain::channelmonitor::BalanceSource;
    use lightning::chain::transaction::OutPoint as LdkOutPoint;
    use lightning::ln::types::ChannelId as LnRgbChannelId;
    use lightning::sign::SpendableOutputDescriptor;

    use super::{
        hex32, now_secs, LnRgbBtcLnBackend, LnRgbChannelOpenRequest, PaymentHash, PaymentId,
        PendingFundingTransaction, RgbFundingOutpointBinding, RgbPendingMaturityRecord,
    };
    use lightning_invoice::{Bolt11InvoiceDescription, Description};

    use crate::btc_ln::{
        BtcLnBackendKind, BtcLnBolt11InvoiceRequest, BtcLnBolt11PaymentRequest, BtcLnNode,
        BtcLnRuntimeConfig,
    };
    use crate::lnnode::{RgbAssetAmount, RgbChannelOpenRequest, RgbLnNode, RgbPaymentRequest};

    #[test]
    fn derives_stable_node_id_from_runtime_config() {
        let config = BtcLnRuntimeConfig {
            backend: BtcLnBackendKind::LnRgb,
            network: Network::Testnet,
            l1_data_dir: PathBuf::from("./wallet-data/ln-rgb-a"),
            storage_dir: PathBuf::from("./wallet-data/ln-rgb-a"),
            esplora: "https://blockstream.info/testnet/api".to_string(),
            esplora_urls: vec!["https://blockstream.info/testnet/api".to_string()],
            esplora_api_key: None,
            listen: None,
            entropy_mnemonic: Some(
                "flower paddle dune found session enroll entry bridge regular sick slam chapter"
                    .to_string(),
            ),
            trusted_peers_0conf: Vec::new(),
            accept_inbound_channels: true,
            accept_inbound_rgb_transfers: true,
            iroh_tunnel_peers: Vec::new(),
        };
        let a = LnRgbBtcLnBackend::new(config.clone());
        let b = LnRgbBtcLnBackend::new(config);
        assert_eq!(a.node_id(), b.node_id());
        a.start().expect("start peer runtime");
        assert!(a.status_summary().contains("started=true"));
        a.stop().expect("stop peer runtime");
    }

    #[test]
    fn rgb_node_records_channel_asset_before_ldk_open() {
        let backend = LnRgbBtcLnBackend::new(test_config(None));
        let asset = RgbAssetAmount {
            contract_id: "rgb:~pzYXTtW-IpYzNwp-sXb9pxZ-sz257_K-k0GyNQK-Y6AMO60"
                .parse()
                .expect("contract id"),
            amount: 7,
        };
        let open_err = backend
            .open_rgb_channel(RgbChannelOpenRequest {
                peer_node_id: backend.node_id(),
                capacity_sat: 100_000,
                push_msat: 0,
                user_channel_id: 42,
                asset,
            })
            .unwrap_err();
        assert!(open_err.to_string().contains("LDK create_channel failed"));
        assert!(backend
            .rgb_channel_assets
            .lock()
            .expect("rgb channel asset lock poisoned")
            .contains_key(&42));
    }

    #[test]
    fn rgb_spendable_outputs_are_staged_for_sweep() {
        let storage_dir = unique_tmp_dir("btc-local-wallet-rgb-sweep");
        let mut config = test_config(None);
        config.storage_dir = storage_dir.clone();
        config.l1_data_dir = storage_dir.join("l1");
        let backend = LnRgbBtcLnBackend::new(config);

        let channel_id = LnRgbChannelId([11; 32]);
        let funding_txid = Txid::from_byte_array([22; 32]);
        let funding_outpoint = OutPoint::new(funding_txid, 0);
        backend
            .rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .insert(
                channel_id,
                RgbFundingOutpointBinding {
                    temporary_channel_id: hex32(channel_id.0),
                    channel_id: Some(hex32(channel_id.0)),
                    peer_node_id: backend.node_id().to_string(),
                    funding_outpoint: funding_outpoint.to_string(),
                    transfer_path: storage_dir
                        .join("funding.rgb-transfer")
                        .display()
                        .to_string(),
                    staged_stock_dir: storage_dir.join("rgb-staged").display().to_string(),
                    status: "confirmed".to_string(),
                    promoted_at: Some(now_secs()),
                    created_at: now_secs(),
                },
            );

        let spendable = SpendableOutputDescriptor::StaticOutput {
            outpoint: LdkOutPoint {
                txid: Txid::from_byte_array([33; 32]),
                index: 1,
            },
            output: TxOut {
                value: Amount::from_sat(1_000),
                script_pubkey: ScriptBuf::new(),
            },
            channel_keys_id: Some([44; 32]),
        };
        backend
            .handle_spendable_outputs(Some(channel_id), vec![spendable])
            .expect("stage rgb sweep");

        let sweep_dir = storage_dir.join("ln-rgb").join("pending-rgb-sweeps");
        let records = fs::read_dir(&sweep_dir)
            .expect("pending sweep dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 1);
        let record: super::RgbPendingSweepRecord =
            serde_json::from_slice(&fs::read(records[0].path()).expect("read sweep record"))
                .expect("parse sweep record");
        assert_eq!(record.status, "pending_rgb_sweep");
        assert!(record.carrier_txid.is_empty());
        assert!(record.staged_stock_dir.is_empty());
        let binding = backend
            .rgb_funding_binding_for_channel(channel_id)
            .expect("rgb binding");
        assert_eq!(binding.status, "pending_rgb_sweep");
    }

    #[test]
    fn rgb_maturity_record_persists_spendable_height() {
        let storage_dir = unique_tmp_dir("btc-local-wallet-rgb-maturity");
        let mut config = test_config(None);
        config.storage_dir = storage_dir.clone();
        config.l1_data_dir = storage_dir.join("l1");
        let backend = LnRgbBtcLnBackend::new(config);

        let channel_id = LnRgbChannelId([55; 32]);
        let funding_txid = Txid::from_byte_array([66; 32]);
        let binding = RgbFundingOutpointBinding {
            temporary_channel_id: hex32(channel_id.0),
            channel_id: Some(hex32(channel_id.0)),
            peer_node_id: backend.node_id().to_string(),
            funding_outpoint: OutPoint::new(funding_txid, 0).to_string(),
            transfer_path: storage_dir
                .join("funding.rgb-transfer")
                .display()
                .to_string(),
            staged_stock_dir: storage_dir.join("rgb-staged").display().to_string(),
            status: "confirmed".to_string(),
            promoted_at: Some(now_secs()),
            created_at: now_secs(),
        };

        backend
            .persist_rgb_maturity_record(
                channel_id,
                &binding,
                12_345,
                654_321,
                BalanceSource::HolderForceClosed,
            )
            .expect("write maturity record");

        let record_path = storage_dir
            .join("ln-rgb")
            .join("pending-rgb-maturity")
            .join(format!("{}.json", hex32(channel_id.0)));
        let record: RgbPendingMaturityRecord =
            serde_json::from_slice(&fs::read(record_path).expect("read maturity record"))
                .expect("parse maturity record");
        assert_eq!(record.status, "waiting_maturity");
        assert_eq!(record.amount_satoshis, 12_345);
        assert_eq!(record.spendable_height, 654_321);
        assert_eq!(record.source, "holder_force_closed");
    }

    #[test]
    fn rgb_payment_state_tracks_outbound_lifecycle() {
        let storage_dir = unique_tmp_dir("btc-local-wallet-rgb-payment");
        let mut config = test_config(None);
        config.storage_dir = storage_dir.clone();
        config.l1_data_dir = storage_dir.join("l1");
        let backend = LnRgbBtcLnBackend::new(config);
        let payment_id = crate::lnnode::PaymentId([77; 32]);
        let request = RgbPaymentRequest {
            recipient_node_id: backend.node_id(),
            amount_msat: 42_000,
            payment_id,
            asset: RgbAssetAmount {
                contract_id: "rgb:~pzYXTtW-IpYzNwp-sXb9pxZ-sz257_K-k0GyNQK-Y6AMO60"
                    .parse()
                    .expect("contract id"),
                amount: 9,
            },
        };

        backend
            .persist_outbound_rgb_payment_created(&request)
            .expect("write outbound payment");
        backend
            .mark_outbound_rgb_payment_submitted(PaymentId(payment_id.0), PaymentHash([88; 32]))
            .expect("mark submitted");
        backend
            .mark_outbound_rgb_payment_status(
                PaymentId(payment_id.0),
                "btc_payment_sent_waiting_rgb_ack",
            )
            .expect("mark btc sent");
        backend
            .mark_rgb_payment_receiver_accepting(payment_id)
            .expect("mark receiver accepting");
        backend
            .mark_rgb_payment_receiver_accepted(payment_id)
            .expect("mark receiver accepted");

        let record_path = storage_dir
            .join("ln-rgb")
            .join("rgb-payments")
            .join(format!("outbound-{}.json", hex32(payment_id.0)));
        let record: super::RgbPaymentStateRecord =
            serde_json::from_slice(&fs::read(record_path).expect("read payment record"))
                .expect("parse payment record");
        assert_eq!(record.direction, "outbound");
        assert_eq!(record.payment_hash, Some(hex32([88; 32])));
        assert_eq!(record.status, "rgb_accepted");
        assert_eq!(record.rgb_amount, 9);
    }

    #[test]
    fn rgb_payment_state_loads_retryable_outbound_assets() {
        let storage_dir = unique_tmp_dir("btc-local-wallet-rgb-payment-reload");
        let mut config = test_config(None);
        config.storage_dir = storage_dir.clone();
        config.l1_data_dir = storage_dir.join("l1");
        let backend = LnRgbBtcLnBackend::new(config.clone());
        let payment_id = crate::lnnode::PaymentId([79; 32]);
        let asset = RgbAssetAmount {
            contract_id: "rgb:~pzYXTtW-IpYzNwp-sXb9pxZ-sz257_K-k0GyNQK-Y6AMO60"
                .parse()
                .expect("contract id"),
            amount: 11,
        };
        backend
            .persist_outbound_rgb_payment_created(&RgbPaymentRequest {
                recipient_node_id: backend.node_id(),
                amount_msat: 50_000,
                payment_id,
                asset,
            })
            .expect("write payment state");

        let reloaded = LnRgbBtcLnBackend::new(config);
        assert_eq!(
            reloaded
                .load_rgb_payment_states_from_disk()
                .expect("load payment states"),
            1
        );
        assert_eq!(
            reloaded
                .rgb_payment_states()
                .expect("payment states")
                .first()
                .expect("payment record")
                .status,
            "created"
        );
        assert!(reloaded
            .rgb_payment_assets
            .lock()
            .expect("rgb payment asset lock poisoned")
            .contains_key(&hex32(payment_id.0)));
    }

    #[test]
    fn pending_rgb_funding_transaction_survives_restart() {
        let storage_dir = unique_tmp_dir("btc-local-wallet-rgb-funding-tx-reload");
        let mut config = test_config(None);
        config.storage_dir = storage_dir.clone();
        config.l1_data_dir = storage_dir.join("l1");
        let backend = LnRgbBtcLnBackend::new(config.clone());
        let channel_id = LnRgbChannelId([90; 32]);
        let funding_outpoint = OutPoint::new(Txid::from_byte_array([91; 32]), 0);
        let transaction = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: Vec::new(),
            output: vec![TxOut {
                value: Amount::from_sat(1_000),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        backend
            .persist_pending_funding_transaction(
                channel_id,
                &PendingFundingTransaction {
                    peer_node_id: backend.node_id(),
                    transaction: transaction.clone(),
                    funding_outpoint,
                    user_channel_id: 123,
                    staged_stock_dir: storage_dir.join("staged"),
                },
            )
            .expect("persist pending funding tx");

        let reloaded = LnRgbBtcLnBackend::new(config);
        assert_eq!(
            reloaded
                .load_pending_funding_transactions_from_disk()
                .expect("load pending funding tx"),
            1
        );
        let pending = reloaded
            .pending_funding_transactions
            .lock()
            .expect("pending funding transaction lock poisoned")
            .get(&channel_id)
            .cloned()
            .expect("pending funding tx");
        assert_eq!(pending.funding_outpoint, funding_outpoint);
        assert_eq!(pending.user_channel_id, 123);
        assert_eq!(pending.transaction, transaction);
    }

    #[test]
    fn invalid_pending_rgb_funding_transfer_does_not_block_reload() {
        let storage_dir = unique_tmp_dir("btc-local-wallet-rgb-funding-transfer-invalid");
        let mut config = test_config(None);
        config.storage_dir = storage_dir.clone();
        config.l1_data_dir = storage_dir.join("l1");
        let backend = LnRgbBtcLnBackend::new(config);
        let pending_dir = storage_dir
            .join("ln-rgb")
            .join("pending-rgb-funding-transfers");
        fs::create_dir_all(&pending_dir).expect("create pending transfer dir");
        let channel_id = hex32([92; 32]);
        let transfer_path = pending_dir.join(format!("{channel_id}.rgb-transfer"));
        fs::write(&transfer_path, [0u8, 1u8, 2u8]).expect("write invalid transfer");
        let record = serde_json::json!({
            "temporary_channel_id": channel_id,
            "peer_node_id": backend.node_id().to_string(),
            "transfer_path": transfer_path.display().to_string(),
            "created_at": now_secs(),
            "updated_at": now_secs(),
        });
        fs::write(
            pending_dir.join(format!("{channel_id}.json")),
            serde_json::to_vec_pretty(&record).expect("encode record"),
        )
        .expect("write pending transfer record");

        assert_eq!(
            backend
                .load_pending_rgb_funding_transfers_from_disk()
                .expect("load pending rgb funding transfers"),
            0
        );
        assert!(backend
            .pending_rgb_funding
            .lock()
            .expect("pending rgb funding lock poisoned")
            .is_empty());
        let event = backend
            .events
            .lock()
            .expect("events lock poisoned")
            .pop_front()
            .expect("skip event");
        assert!(event.contains("skipped pending RGB funding transfer"));
    }

    #[test]
    fn creates_signed_bolt11_invoice() {
        let backend = LnRgbBtcLnBackend::new(test_config(None));
        let description = Description::new("ln-rgb invoice".to_string())
            .map(Bolt11InvoiceDescription::Direct)
            .expect("description");
        let invoice = backend
            .receive_bolt11(BtcLnBolt11InvoiceRequest {
                amount_msat: 12_345,
                description,
                expiry_secs: 600,
            })
            .expect("invoice");
        assert_eq!(invoice.amount_milli_satoshis(), Some(12_345));
        assert_eq!(
            invoice.currency(),
            lightning_invoice::Currency::BitcoinTestnet
        );
        assert_eq!(invoice.recover_payee_pub_key(), backend.node_id());
    }

    #[test]
    fn starts_listener_and_connects_peer() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);

        let a = LnRgbBtcLnBackend::new(test_config(None));
        let b = LnRgbBtcLnBackend::new(test_config(Some(format!("127.0.0.1:{port}"))));

        b.start().expect("start B listener");
        a.start().expect("start A runtime");
        a.connect(
            b.node_id(),
            format!("127.0.0.1:{port}").parse().expect("B address"),
            true,
        )
        .expect("connect A to B");

        let peers = a.peer_snapshots();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, b.node_id());
        assert!(peers[0].is_connected);

        a.stop().expect("stop A");
        b.stop().expect("stop B");
    }

    #[test]
    fn channel_and_payment_paths_reach_ldk_runtime_boundaries() {
        let backend = LnRgbBtcLnBackend::new(test_config(None));
        let open_err = backend
            .open_standard_channel(LnRgbChannelOpenRequest {
                peer_node_id: backend.node_id(),
                capacity_sat: 100_000,
                push_msat: 0,
                user_channel_id: 42,
            })
            .unwrap_err();
        assert!(open_err.to_string().contains("LDK create_channel failed"));

        let invoice = backend
            .receive_bolt11(BtcLnBolt11InvoiceRequest {
                amount_msat: 1_000,
                description: Description::new("pay".to_string())
                    .map(Bolt11InvoiceDescription::Direct)
                    .expect("description"),
                expiry_secs: 600,
            })
            .expect("invoice");
        let pay_err = backend
            .pay_bolt11(BtcLnBolt11PaymentRequest { invoice })
            .unwrap_err();
        assert!(pay_err.to_string().contains("LDK BOLT11 payment failed"));
    }

    fn test_config(listen: Option<String>) -> BtcLnRuntimeConfig {
        BtcLnRuntimeConfig {
            backend: BtcLnBackendKind::LnRgb,
            network: Network::Testnet,
            l1_data_dir: PathBuf::from("./wallet-data/ln-rgb-test"),
            storage_dir: PathBuf::from("./wallet-data/ln-rgb-test"),
            esplora: "https://blockstream.info/testnet/api".to_string(),
            esplora_urls: vec!["https://blockstream.info/testnet/api".to_string()],
            esplora_api_key: None,
            listen,
            entropy_mnemonic: Some(
                "flower paddle dune found session enroll entry bridge regular sick slam chapter"
                    .to_string(),
            ),
            trusted_peers_0conf: Vec::new(),
            accept_inbound_channels: true,
            accept_inbound_rgb_transfers: true,
            iroh_tunnel_peers: Vec::new(),
        }
    }

    fn unique_tmp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        PathBuf::from(format!(
            "/private/tmp/{prefix}-{}-{nanos}",
            std::process::id()
        ))
    }
}
