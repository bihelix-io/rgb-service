use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure, Context, Result};
use bdk_bitcoind_rpc::bitcoincore_rpc::RpcApi;
use bdk_wallet::keys::bip39::{Language as BdkLanguage, Mnemonic as BdkMnemonic};
use bdk_wallet::SignOptions;
use bdk_wallet::{KeychainKind, TxOrdering};
use bitcoin::absolute::LockTime;
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::{sha256, Hash, HashEngine};
use bitcoin::psbt::Psbt;
use bitcoin::secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
use bitcoin::{
    Amount, BlockHash, FeeRate, Network, OutPoint, ScriptBuf, Transaction, TxOut, Txid, Weight,
};
use lightning::chain::chaininterface::{BroadcasterInterface, ConfirmationTarget, FeeEstimator};
use lightning::chain::chainmonitor::ChainMonitor;
use lightning::chain::channelmonitor::{Balance, BalanceSource};
use lightning::chain::BestBlock;
use lightning::chain::{Confirm, Filter, Watch, WatchedOutput};
use lightning::events::{Event, EventsProvider};
use lightning::ln::channelmanager::{
    Bolt11InvoiceParameters, ChainParameters, ChannelManagerReadArgs, PaymentId,
    RecipientOnionFields, SimpleArcChannelManager,
};
use lightning::ln::funding::{FundingTxInput, SpliceContribution};
use lightning::ln::msgs::SocketAddress;
use lightning::ln::peer_handler::{IgnoringMessageHandler, MessageHandler, PeerManager};
use lightning::ln::types::ChannelId as LnRgbChannelId;
use lightning::onion_message::messenger::DefaultMessageRouter;
use lightning::rgb::{
    init_rgb_ln_tx_composer, AllocationStatus, AssetLayer, AssetSpendAuthorization,
    AssetSpendPurpose, BalanceRequest, BalanceScope, CommitTransferRequest, IssueAssetRequest,
    IssueAssetResponse, ListAssetsRequest, ListAssetsResponse, LnChannelFundingRefRequest,
    LnChannelOpenPrepareRequest, LnPaymentClaimRequest, PrepareTransferRequest, RequestSignature,
    RgbAssetAmount as LdkRgbAssetAmount, RgbBalance, RgbChannelContext, RgbDaemonLnTxComposer,
    RgbFundingRef, RgbFundingTransfer as LdkRgbFundingTransfer, RgbLnTxComposer,
    RgbPaymentMetadata, RgbServiceClient, RgbServiceClientError, RgbServiceSigner, SignatureScheme,
    TrackedUtxo,
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
use lightning::util::ser::{ReadableArgs, Writeable};
use lightning::util::sweep::{OutputSpendStatus, OutputSweeperSync, TrackedSpendableOutput};
use lightning_invoice::Bolt11Invoice;
use lightning_net_tokio::SocketDescriptor;
use lightning_persister::fs_store::FilesystemStore;
use lightning_transaction_sync::EsploraSyncClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use crate::btc_ln::{
    BtcLnBalanceSnapshot, BtcLnBolt11InvoiceRequest, BtcLnBolt11PaymentRequest,
    BtcLnChannelCloseRequest, BtcLnChannelOpenRequest, BtcLnChannelSnapshot,
    BtcLnChannelSpliceRequest, BtcLnEvent, BtcLnKeysendRequest, BtcLnNode, BtcLnPeerSnapshot,
    BtcLnRuntimeConfig,
};
use crate::lnnode::{
    ChannelId, RgbAssetAmount, RgbChannelOpenRequest, RgbFundingTransfer, RgbLnNode,
    RgbPaymentRequest,
};
use crate::local_wallet::{
    bitcoin_core_chain_tip, bitcoin_core_tx_status, broadcast_transaction, electrum_block_header,
    electrum_chain_tip, electrum_get_transaction, electrum_script_history,
    electrum_transaction_status, esplora_client_with_config, sync_wallet, ChainSource,
    ElectrumConfig, EsploraConfig, LocalWallet,
};

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
    btc_events: Mutex<VecDeque<BtcLnEvent>>,
    rgb_channel_assets: Mutex<HashMap<u128, RgbAssetAmount>>,
    rgb_payment_assets: Mutex<HashMap<String, RgbAssetAmount>>,
    pending_funding_transactions: Mutex<HashMap<LnRgbChannelId, PendingFundingTransaction>>,
    pending_rgb_funding: Mutex<HashMap<LnRgbChannelId, PendingRgbFundingTransfer>>,
    pending_inbound_rgb_channels: Mutex<HashMap<LnRgbChannelId, PublicKey>>,
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
    listener_stop: Arc<AtomicBool>,
    listener_handle: Option<JoinHandle<()>>,
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
    funding_outpoint: OutPoint,
    funding_ref: RgbFundingRef,
}

#[derive(Clone)]
struct PendingFundingTransaction {
    peer_node_id: PublicKey,
    transaction: Transaction,
    funding_outpoint: OutPoint,
    user_channel_id: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PendingFundingTransactionRecord {
    temporary_channel_id: String,
    peer_node_id: String,
    transaction_hex: String,
    funding_outpoint: String,
    user_channel_id: u128,
    created_at: u64,
    updated_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PendingRgbFundingTransferRecord {
    temporary_channel_id: String,
    peer_node_id: String,
    funding_outpoint: String,
    funding_ref: RgbFundingRef,
    created_at: u64,
    updated_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GeneratedRgbFundingTransferRecord {
    temporary_channel_id: String,
    peer_node_id: String,
    funding_outpoint: String,
    funding_ref: RgbFundingRef,
    created_at: u64,
    updated_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedPeerRecord {
    node_id: String,
    address: String,
    created_at: u64,
    updated_at: u64,
    last_connected_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbFundingOutpointBinding {
    pub temporary_channel_id: String,
    #[serde(default)]
    pub channel_id: Option<String>,
    pub peer_node_id: String,
    pub funding_outpoint: String,
    pub funding_ref: RgbFundingRef,
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
    pub funding_ref: RgbFundingRef,
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
                    ln_rgb_log_line(&format!(
                        "[ln-rgb] failed to open sweep destination wallet: {err:#}"
                    ));
                })?;
        let address = wallet.wallet.reveal_next_address(KeychainKind::Internal);
        let script_pubkey = address.address.script_pubkey();
        wallet.persist().map_err(|err| {
            ln_rgb_log_line(&format!(
                "[ln-rgb] failed to persist sweep destination wallet: {err:#}"
            ));
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
        ln_rgb_log_line(&format!(
            "[ln-rgb:{}:{}] {}",
            record.module_path, record.line, message
        ));
    }
}

fn should_suppress_ldk_log(module_path: &str, message: &str) -> bool {
    if module_path == "lightning::ln::peer_handler"
        && (message.starts_with("Received message ChannelUpdate")
            || message.starts_with("Received message ChannelAnnouncement")
            || message.starts_with("Received message NodeAnnouncement")
            || message.starts_with("Received message Ping")
            || message.starts_with("Received message Pong")
            || message.starts_with("Enqueueing message Ping")
            || message.starts_with("Enqueueing message Pong"))
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

fn ln_rgb_log_line(message: &str) {
    let path = std::env::var("LN_RGB_LOG_PATH")
        .unwrap_or_else(|_| "/data/logs/super_bazaar/ln-rgb.log".to_string());
    let path = PathBuf::from(path);
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{message}");
    }
}

fn ln_rgb_log_signed_btc_tx(operation: &str, tx: &Transaction) {
    let txid = tx.compute_txid();
    let raw_tx = bytes_to_hex(&serialize(tx));
    ln_rgb_log_line(&format!(
        "[ln-rgb] signed BTC transaction {}",
        json!({
            "operation": operation,
            "txid": txid.to_string(),
            "raw_tx": raw_tx
        })
    ));
}

enum LnRgbChainSync {
    Esplora(EsploraSyncClient<Arc<LnRgbLogger>>),
    Electrum(ElectrumTxSync),
    BitcoinCore(BitcoinCoreTxSync),
}

impl LnRgbChainSync {
    async fn sync(&self, confirmables: Vec<&(dyn Confirm + Sync + Send)>) -> Result<()> {
        match self {
            Self::Esplora(client) => client
                .sync(confirmables)
                .map_err(|err| anyhow!("LDK Esplora transaction sync failed: {err:?}")),
            Self::Electrum(sync) => sync.sync(confirmables),
            Self::BitcoinCore(sync) => sync.sync(confirmables),
        }
    }
}

impl Filter for LnRgbChainSync {
    fn register_tx(&self, txid: &Txid, script_pubkey: &bitcoin::Script) {
        match self {
            Self::Esplora(client) => client.register_tx(txid, script_pubkey),
            Self::Electrum(sync) => sync.register_tx(txid, script_pubkey),
            Self::BitcoinCore(sync) => sync.register_tx(txid, script_pubkey),
        }
    }

    fn register_output(&self, output: WatchedOutput) {
        match self {
            Self::Esplora(client) => client.register_output(output),
            Self::Electrum(sync) => sync.register_output(output),
            Self::BitcoinCore(sync) => sync.register_output(output),
        }
    }
}

struct ElectrumTxSync {
    config: ElectrumConfig,
    watched_txs: Mutex<HashMap<Txid, ScriptBuf>>,
    watched_outputs: Mutex<HashMap<OutPoint, WatchedOutput>>,
    confirmed_txs: Mutex<HashMap<Txid, (u32, BlockHash)>>,
}

impl ElectrumTxSync {
    fn new(config: ElectrumConfig) -> Self {
        Self {
            config,
            watched_txs: Mutex::new(HashMap::new()),
            watched_outputs: Mutex::new(HashMap::new()),
            confirmed_txs: Mutex::new(HashMap::new()),
        }
    }

    fn mark_confirmation_seen(&self, txid: Txid, height: u32, block_hash: BlockHash) -> bool {
        let mut confirmed_txs = self
            .confirmed_txs
            .lock()
            .expect("ln-rgb confirmed tx lock poisoned");
        match confirmed_txs.get(&txid) {
            Some((seen_height, seen_hash))
                if *seen_height == height && *seen_hash == block_hash =>
            {
                false
            }
            _ => {
                confirmed_txs.insert(txid, (height, block_hash));
                true
            }
        }
    }

    fn sync(&self, confirmables: Vec<&(dyn Confirm + Sync + Send)>) -> Result<()> {
        let tip = electrum_chain_tip(&self.config)?;
        let tip_header = electrum_block_header(&self.config, tip.height)
            .with_context(|| format!("fetch Electrum tip header at {}", tip.height))?;

        let mut seen_confirmations = HashSet::new();
        let relevant = confirmables
            .iter()
            .flat_map(|confirmable| confirmable.get_relevant_txids())
            .filter(|(txid, height, _)| seen_confirmations.insert((*txid, *height)))
            .collect::<Vec<_>>();
        for (txid, height, expected_hash) in relevant {
            if self.confirm_relevant_tx(&confirmables, txid, height, expected_hash)? {
                continue;
            }
            match electrum_transaction_status(&self.config, txid) {
                Ok(Some(status)) if status.confirmed && status.block_hash == expected_hash => {}
                Ok(Some(status)) if status.confirmed => {
                    for confirmable in &confirmables {
                        confirmable.transaction_unconfirmed(&txid);
                    }
                    self.confirm_tx(&confirmables, status)?;
                }
                Ok(_) => ln_rgb_log_line(&format!(
                    "[ln-rgb] Electrum could not verify relevant tx {txid} at height {height}; keeping existing LDK confirmation state"
                )),
                Err(err) => ln_rgb_log_line(&format!(
                    "[ln-rgb] Electrum status lookup failed for relevant tx {txid} at height {height}: {err:#}; keeping existing LDK confirmation state"
                )),
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
            let script_pubkey = self
                .watched_txs
                .lock()
                .expect("ln-rgb watched tx lock poisoned")
                .get(&txid)
                .cloned();
            let mut confirmed_from_history = false;
            if let Some(script_pubkey) = script_pubkey {
                for entry in electrum_script_history(&self.config, &script_pubkey)? {
                    if entry.txid != txid {
                        continue;
                    }
                    if let Some(height) = entry.height {
                        if !seen_confirmations.insert((txid, height)) {
                            confirmed_from_history = true;
                            break;
                        }
                        if self.confirm_relevant_tx(&confirmables, txid, height, None)? {
                            confirmed_from_history = true;
                            break;
                        }
                    }
                }
            }
            if confirmed_from_history {
                continue;
            }
            match electrum_transaction_status(&self.config, txid) {
                Ok(Some(status)) if status.confirmed => {
                    if status
                        .height
                        .map(|height| seen_confirmations.insert((txid, height)))
                        .unwrap_or(false)
                    {
                        self.confirm_tx(&confirmables, status)?;
                    }
                }
                Ok(_) => {}
                Err(err) => ln_rgb_log_line(&format!(
                    "[ln-rgb] Electrum status lookup failed for watched tx {txid}: {err:#}"
                )),
            }
        }

        self.confirm_watched_output_spends(&confirmables, &mut seen_confirmations)?;

        for confirmable in confirmables {
            confirmable.best_block_updated(&tip_header, tip.height);
        }
        Ok(())
    }

    fn confirm_tx(
        &self,
        confirmables: &[&(dyn Confirm + Sync + Send)],
        status: crate::local_wallet::ElectrumTxStatus,
    ) -> Result<()> {
        let Some(height) = status.height else {
            return Ok(());
        };
        let header = electrum_block_header(&self.config, height)
            .with_context(|| format!("fetch Electrum block header at {height}"))?;
        let txid = status.tx.compute_txid();
        if !self.mark_confirmation_seen(txid, height, header.block_hash()) {
            return Ok(());
        }
        let position = status.position.unwrap_or_default();
        let txdata = vec![(position, &status.tx)];
        for confirmable in confirmables {
            confirmable.transactions_confirmed(&header, &txdata, height);
        }
        Ok(())
    }

    fn confirm_relevant_tx(
        &self,
        confirmables: &[&(dyn Confirm + Sync + Send)],
        txid: Txid,
        height: u32,
        expected_hash: Option<BlockHash>,
    ) -> Result<bool> {
        if height == 0 {
            return Ok(false);
        }
        let header = electrum_block_header(&self.config, height)
            .with_context(|| format!("fetch Electrum block header at {height}"))?;
        if let Some(expected_hash) = expected_hash {
            if header.block_hash() != expected_hash {
                return Ok(false);
            }
        }
        let tx = match electrum_get_transaction(&self.config, txid) {
            Ok(tx) => tx,
            Err(err) => {
                ln_rgb_log_line(&format!(
                    "[ln-rgb] Electrum could not fetch relevant tx {txid} at height {height}: {err:#}"
                ));
                return Ok(false);
            }
        };
        if tx.compute_txid() != txid {
            return Ok(false);
        }
        if !self.mark_confirmation_seen(txid, height, header.block_hash()) {
            return Ok(true);
        }
        let position =
            crate::local_wallet::electrum_transaction_position(&self.config, txid, height)
                .unwrap_or_default();
        let txdata = vec![(position, &tx)];
        for confirmable in confirmables {
            confirmable.transactions_confirmed(&header, &txdata, height);
        }
        Ok(true)
    }

    fn confirm_watched_output_spends(
        &self,
        confirmables: &[&(dyn Confirm + Sync + Send)],
        seen_confirmations: &mut HashSet<(Txid, u32)>,
    ) -> Result<()> {
        let watched_outputs = self
            .watched_outputs
            .lock()
            .expect("ln-rgb watched output lock poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for output in watched_outputs {
            for entry in electrum_script_history(&self.config, &output.script_pubkey)? {
                let Some(height) = entry.height else {
                    continue;
                };
                let tx = electrum_get_transaction(&self.config, entry.txid)?;
                if !tx
                    .input
                    .iter()
                    .any(|input| input.previous_output == output.outpoint.into_bitcoin_outpoint())
                {
                    continue;
                }
                if !seen_confirmations.insert((entry.txid, height)) {
                    continue;
                }
                let header = electrum_block_header(&self.config, height)
                    .with_context(|| format!("fetch Electrum block header at {height}"))?;
                if !self.mark_confirmation_seen(entry.txid, height, header.block_hash()) {
                    continue;
                }
                let position = crate::local_wallet::electrum_transaction_position(
                    &self.config,
                    entry.txid,
                    height,
                )
                .unwrap_or_default();
                let txdata = vec![(position, &tx)];
                for confirmable in confirmables {
                    confirmable.transactions_confirmed(&header, &txdata, height);
                }
            }
        }
        Ok(())
    }
}

impl Filter for ElectrumTxSync {
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
            ln_rgb_log_signed_btc_tx("ldk_broadcast_transaction", tx);
            match broadcast_transaction(self.network, Some(&self.chain_source), tx) {
                Ok(_) => {
                    self.broadcasted_txs
                        .lock()
                        .expect("ln-rgb broadcast lock poisoned")
                        .push(txid);
                    ln_rgb_log_line(&format!("[ln-rgb] broadcast LDK transaction: {txid}"));
                }
                Err(err) => {
                    ln_rgb_log_line(&format!("[ln-rgb] failed to broadcast {txid}: {err:#}"))
                }
            }
        }
    }
}

#[derive(Debug)]
struct LnRgbFeeEstimator;

const MIN_REMOTE_CHANNEL_FEERATE_SAT_PER_1000_WEIGHT: u32 = 253;

impl FeeEstimator for LnRgbFeeEstimator {
    fn get_est_sat_per_1000_weight(&self, target: ConfirmationTarget) -> u32 {
        match target {
            ConfirmationTarget::MaximumFeeEstimate => 5_000,
            ConfirmationTarget::UrgentOnChainSweep => 2_500,
            ConfirmationTarget::MinAllowedAnchorChannelRemoteFee
            | ConfirmationTarget::MinAllowedNonAnchorChannelRemoteFee => {
                MIN_REMOTE_CHANNEL_FEERATE_SAT_PER_1000_WEIGHT
            }
            ConfirmationTarget::AnchorChannelFee
            | ConfirmationTarget::NonAnchorChannelFee
            | ConfirmationTarget::ChannelCloseMinimum
            | ConfirmationTarget::OutputSpendingFee => 1_000,
        }
    }
}

struct BackendRgbServiceSigner {
    node_id: PublicKey,
    node_secret: SecretKey,
}

#[derive(Serialize)]
struct BackendUnsignedAssetSpendAuthorization<'a> {
    asset_id: &'a str,
    amount: u64,
    purpose: &'a AssetSpendPurpose,
    recipient: Option<&'a str>,
    anchor_psbt: Option<&'a str>,
    expires_at_ms: u64,
}

impl RgbServiceSigner for BackendRgbServiceSigner {
    fn sign_rgb_service_payload(
        &self,
        purpose: &str,
        payload: &[u8],
    ) -> std::result::Result<RequestSignature, RgbServiceClientError> {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).map_err(|err| {
            RgbServiceClientError::Compose(format!("generate LN RGB authorization nonce: {err}"))
        })?;
        let nonce = bytes_to_hex(&nonce);
        let timestamp_ms = now_millis();
        let mut engine = sha256::Hash::engine();
        engine.input(b"bihelix-ln-rgb-auth-v1");
        engine.input(purpose.as_bytes());
        engine.input(nonce.as_bytes());
        engine.input(&timestamp_ms.to_be_bytes());
        engine.input(payload);
        let message = Message::from_digest(sha256::Hash::from_engine(engine).to_byte_array());
        let secp = Secp256k1::new();
        let signature = secp.sign_ecdsa(&message, &self.node_secret);
        Ok(RequestSignature {
            signer_id: self.node_id.to_string(),
            public_key: self.node_id.to_string(),
            scheme: SignatureScheme::Ecdsa,
            nonce,
            timestamp_ms,
            signature: bytes_to_hex(&signature.serialize_der()),
        })
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
            btc_events: Mutex::new(VecDeque::new()),
            rgb_channel_assets: Mutex::new(HashMap::new()),
            rgb_payment_assets: Mutex::new(HashMap::new()),
            pending_funding_transactions: Mutex::new(HashMap::new()),
            pending_rgb_funding: Mutex::new(HashMap::new()),
            pending_inbound_rgb_channels: Mutex::new(HashMap::new()),
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

    pub fn account_id(&self) -> &str {
        &self.config.account_id
    }

    pub fn sign_node_message(&self, message: &[u8]) -> Result<String> {
        let runtime = self
            .runtime
            .lock()
            .map_err(|_| anyhow!("LN RGB runtime lock poisoned"))?;
        let keys_manager = Arc::clone(
            &runtime
                .as_ref()
                .context("LN RGB node is not running")?
                ._keys_manager,
        );
        drop(runtime);
        keys_manager
            .sign_message(message)
            .map_err(|_| anyhow!("LDK node signer could not sign message"))
    }

    pub fn list_rgb_assets(&self) -> Result<ListAssetsResponse> {
        let client = RgbServiceClient::new(
            self.config.rgb_service_url.clone(),
            Arc::new(BackendRgbServiceSigner {
                node_id: self.node_id,
                node_secret: self.node_secret,
            }),
        )
        .map_err(|err| anyhow!("{err}"))?;
        client
            .list_assets(ListAssetsRequest {
                account_id: self.config.account_id.clone(),
            })
            .map_err(|err| anyhow!("{err}"))
    }

    pub fn issue_rgb_asset(
        &self,
        ticker: String,
        name: String,
        precision: u8,
        supply: u64,
        allocation_outpoint: String,
        utxos: Vec<TrackedUtxo>,
    ) -> Result<IssueAssetResponse> {
        let client = RgbServiceClient::new(
            self.config.rgb_service_url.clone(),
            Arc::new(BackendRgbServiceSigner {
                node_id: self.node_id,
                node_secret: self.node_secret,
            }),
        )
        .map_err(|err| anyhow!("{err}"))?;
        client
            .issue_asset(IssueAssetRequest {
                account_id: self.config.account_id.clone(),
                ticker,
                name,
                precision,
                supply,
                allocation_outpoint,
                utxos,
            })
            .map_err(|err| anyhow!("{err}"))
    }

    pub fn rgb_balance(&self, asset_id: String) -> Result<RgbBalance> {
        let client = RgbServiceClient::new(
            self.config.rgb_service_url.clone(),
            Arc::new(BackendRgbServiceSigner {
                node_id: self.node_id,
                node_secret: self.node_secret,
            }),
        )
        .map_err(|err| anyhow!("{err}"))?;
        client
            .balance(BalanceRequest {
                account_id: self.config.account_id.clone(),
                asset_id,
                scope: BalanceScope::All,
            })
            .map_err(|err| anyhow!("{err}"))
    }

    fn rgb_service_client(&self) -> Result<RgbServiceClient> {
        RgbServiceClient::new(
            self.config.rgb_service_url.clone(),
            Arc::new(BackendRgbServiceSigner {
                node_id: self.node_id,
                node_secret: self.node_secret,
            }),
        )
        .map_err(|err| anyhow!("{err}"))
    }

    fn rgb_asset_spend_authorization(
        &self,
        asset_id: &str,
        amount: u64,
        recipient: Option<&str>,
        anchor_psbt: Option<&str>,
    ) -> Result<AssetSpendAuthorization> {
        let purpose = AssetSpendPurpose::L1Transfer;
        let expires_at_ms = now_millis().saturating_add(300_000);
        let unsigned = BackendUnsignedAssetSpendAuthorization {
            asset_id,
            amount,
            purpose: &purpose,
            recipient,
            anchor_psbt,
            expires_at_ms,
        };
        let payload = serde_json::to_vec(&unsigned).context("encode RGB asset authorization")?;
        let signer = BackendRgbServiceSigner {
            node_id: self.node_id,
            node_secret: self.node_secret,
        };
        let signature = signer
            .sign_rgb_service_payload("asset_spend", &payload)
            .map_err(|err| anyhow!("{err}"))?;
        Ok(AssetSpendAuthorization {
            asset_id: asset_id.to_string(),
            amount,
            purpose,
            recipient: recipient.map(str::to_string),
            anchor_psbt: anchor_psbt.map(str::to_string),
            expires_at_ms,
            signature,
        })
    }

    fn l1_utxos_response(
        &self,
        esplora: impl Into<String>,
        balance_source: &'static str,
        utxos: Vec<serde_json::Value>,
    ) -> serde_json::Value {
        json!({
            "module": "ln_rgb",
            "operation": "utxos",
            "wallet": "ln_hot_wallet",
            "network": self.config.network.to_string(),
            "esplora": esplora.into(),
            "balance_source": balance_source,
            "utxos": utxos
        })
    }

    fn local_l1_utxos(local: &LocalWallet) -> Vec<serde_json::Value> {
        local
            .wallet
            .list_unspent()
            .map(|utxo| {
                json!({
                    "outpoint": utxo.outpoint.to_string(),
                    "txid": utxo.outpoint.txid.to_string(),
                    "vout": utxo.outpoint.vout,
                    "value": utxo.txout.value.to_sat(),
                    "amount_sat": utxo.txout.value.to_sat(),
                    "confirmed": utxo.chain_position.is_confirmed(),
                    "status": {
                        "confirmed": utxo.chain_position.is_confirmed()
                    },
                    "script_pubkey": bytes_to_hex(utxo.txout.script_pubkey.as_bytes())
                })
            })
            .collect::<Vec<_>>()
    }

    pub fn l1_utxos_json(&self) -> Result<serde_json::Value> {
        match self.try_open_l1_wallet()? {
            Some(local) => {
                let utxos = Self::local_l1_utxos(&local);
                Ok(self.l1_utxos_response(self.config.esplora.clone(), "cached", utxos))
            }
            None => Ok(json!({
                "module": "ln_rgb",
                "operation": "utxos",
                "wallet": "ln_hot_wallet",
                "network": self.config.network.to_string(),
                "esplora": self.config.esplora.clone(),
                "balance_source": "cached",
                "wallet_busy": true,
                "utxos": []
            })),
        }
    }

    pub fn sync_l1_utxos_json(&self) -> Result<serde_json::Value> {
        self.retry_transient_esplora("list LN hot wallet L1 UTXOs", || {
            let mut local = self.open_l1_wallet()?;
            let esplora = self.next_esplora_url();
            self.sync_l1_wallet_or_use_cached(&mut local, &esplora, "list LN hot wallet L1 UTXOs")?;
            let utxos = Self::local_l1_utxos(&local);
            Ok(self.l1_utxos_response(esplora, "synced", utxos))
        })
    }

    pub fn transfer_l1_with_inputs_json(
        &self,
        recipient: &str,
        amount_sats: u64,
        fee_rate_sat_vb: u64,
        input_outpoints: &str,
    ) -> Result<serde_json::Value> {
        ensure!(amount_sats > 0, "amount_sats must be greater than zero");
        let recipient = recipient.trim();
        ensure!(!recipient.is_empty(), "recipient address must not be empty");
        let requested = input_outpoints.trim();
        ensure!(
            !requested.is_empty(),
            "input_outpoints must include at least one outpoint"
        );
        let recipient_address = bitcoin::Address::from_str(recipient)
            .with_context(|| format!("invalid recipient BTC address: {recipient}"))?
            .require_network(self.config.network)
            .with_context(|| format!("recipient address is not for {:?}", self.config.network))?;
        let fee_rate_sat_vb = fee_rate_sat_vb.max(1);
        let fee_rate = FeeRate::from_sat_per_vb(fee_rate_sat_vb)
            .context("invalid LN hot wallet withdrawal fee rate")?;

        self.retry_transient_esplora("LN hot wallet L1 transfer", || {
            let mut local = self.open_l1_wallet()?;
            let esplora = self.next_esplora_url();
            self.sync_l1_wallet_or_use_cached(&mut local, &esplora, "LN hot wallet L1 transfer")?;
            let available = local
                .wallet
                .list_unspent()
                .filter(|utxo| utxo.chain_position.is_confirmed())
                .map(|utxo| (utxo.outpoint, utxo.txout.value.to_sat()))
                .collect::<HashMap<_, _>>();
            let mut selected = Vec::new();
            let mut selected_sats = 0u64;
            for token in requested
                .split(|ch: char| ch == ',' || ch == '\n' || ch == '\r' || ch == '\t' || ch == ' ')
                .map(str::trim)
                .filter(|token| !token.is_empty())
            {
                let outpoint = OutPoint::from_str(token)
                    .with_context(|| format!("invalid selected LN hot wallet outpoint: {token}"))?;
                ensure!(
                    !selected.iter().any(|(existing, _)| *existing == outpoint),
                    "duplicate selected LN hot wallet outpoint: {outpoint}"
                );
                let value = *available.get(&outpoint).with_context(|| {
                    format!(
                        "selected LN hot wallet outpoint is not confirmed/available: {outpoint}"
                    )
                })?;
                selected_sats = selected_sats.saturating_add(value);
                selected.push((outpoint, value));
            }
            ensure!(
                !selected.is_empty(),
                "no selected LN hot wallet UTXO provided"
            );

            let mut builder = local.wallet.build_tx();
            builder
                .add_recipient(
                    recipient_address.script_pubkey(),
                    Amount::from_sat(amount_sats),
                )
                .fee_rate(fee_rate)
                .nlocktime(LockTime::ZERO)
                .manually_selected_only();
            for (outpoint, _) in &selected {
                builder
                    .add_utxo(*outpoint)
                    .with_context(|| format!("add LN hot wallet UTXO {outpoint}"))?;
            }

            let mut psbt = builder
                .finish()
                .context("build LN hot wallet withdrawal PSBT")?;
            let unsigned_psbt = psbt.to_string();
            let finalized = local
                .wallet
                .sign(&mut psbt, SignOptions::default())
                .context("sign LN hot wallet withdrawal PSBT")?;
            ensure!(finalized, "LN hot wallet withdrawal PSBT was not finalized");
            local.persist()?;

            let signed_psbt = psbt.to_string();
            let tx = psbt
                .extract_tx()
                .context("extract LN hot wallet withdrawal transaction")?;
            let output_sats = tx
                .output
                .iter()
                .map(|output| output.value.to_sat())
                .sum::<u64>();
            let fee_sats = selected_sats.saturating_sub(output_sats);
            let chain_source =
                ChainSource::from_esplora_or_default(self.config.network, Some(&esplora))?;
            ln_rgb_log_signed_btc_tx("transfer_with_inputs", &tx);
            let txid = broadcast_transaction(self.config.network, Some(&chain_source), &tx)?;
            Ok(json!({
                "module": "ln_rgb",
                "operation": "transfer_with_inputs",
                "wallet": "ln_hot_wallet",
                "to": recipient_address.to_string(),
                "amount_sats": amount_sats,
                "fee_rate_sat_vb": fee_rate_sat_vb,
                "fee_sats": fee_sats,
                "selected_sats": selected_sats,
                "change_sats": selected_sats.saturating_sub(amount_sats).saturating_sub(fee_sats),
                "inputs": selected
                    .iter()
                    .map(|(outpoint, value)| json!({
                        "outpoint": outpoint.to_string(),
                        "value": value
                    }))
                    .collect::<Vec<_>>(),
                "txid": txid.to_string(),
                "raw_tx": bytes_to_hex(&serialize(&tx)),
                "unsigned_psbt": unsigned_psbt,
                "signed_psbt": signed_psbt,
                "esplora": esplora
            }))
        })
    }

    pub fn transfer_l1_batch_with_inputs_json(
        &self,
        outputs: &str,
        fee_rate_sat_vb: u64,
        input_outpoints: &str,
    ) -> Result<serde_json::Value> {
        let requested = input_outpoints.trim();
        ensure!(
            !requested.is_empty(),
            "input_outpoints must include at least one outpoint"
        );
        let fee_rate_sat_vb = fee_rate_sat_vb.max(1);
        let fee_rate = FeeRate::from_sat_per_vb(fee_rate_sat_vb)
            .context("invalid LN hot wallet withdrawal fee rate")?;
        let mut recipients = Vec::new();
        let mut amount_sats = 0u64;
        for entry in outputs
            .split(';')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            let (address, amount) = entry
                .split_once('=')
                .with_context(|| format!("invalid withdrawal batch output: {entry}"))?;
            let address = address.trim();
            let amount = amount
                .trim()
                .parse::<u64>()
                .with_context(|| format!("invalid withdrawal batch amount: {entry}"))?;
            ensure!(amount > 0, "withdrawal batch amount must be positive");
            let address = bitcoin::Address::from_str(address)
                .with_context(|| format!("invalid recipient BTC address: {address}"))?
                .require_network(self.config.network)
                .with_context(|| {
                    format!("recipient address is not for {:?}", self.config.network)
                })?;
            amount_sats = amount_sats.saturating_add(amount);
            recipients.push((address, amount));
        }
        ensure!(
            !recipients.is_empty(),
            "withdrawal batch outputs must not be empty"
        );

        self.retry_transient_esplora("LN hot wallet L1 batch transfer", || {
            let mut local = self.open_l1_wallet()?;
            let esplora = self.next_esplora_url();
            self.sync_l1_wallet_or_use_cached(
                &mut local,
                &esplora,
                "LN hot wallet L1 batch transfer",
            )?;
            let available = local
                .wallet
                .list_unspent()
                .filter(|utxo| utxo.chain_position.is_confirmed())
                .map(|utxo| (utxo.outpoint, utxo.txout.value.to_sat()))
                .collect::<HashMap<_, _>>();
            let mut selected = Vec::new();
            let mut selected_sats = 0u64;
            for token in requested
                .split(|ch: char| ch == ',' || ch == '\n' || ch == '\r' || ch == '\t' || ch == ' ')
                .map(str::trim)
                .filter(|token| !token.is_empty())
            {
                let outpoint = OutPoint::from_str(token)
                    .with_context(|| format!("invalid selected LN hot wallet outpoint: {token}"))?;
                ensure!(
                    !selected.iter().any(|(existing, _)| *existing == outpoint),
                    "duplicate selected LN hot wallet outpoint: {outpoint}"
                );
                let value = *available.get(&outpoint).with_context(|| {
                    format!(
                        "selected LN hot wallet outpoint is not confirmed/available: {outpoint}"
                    )
                })?;
                selected_sats = selected_sats.saturating_add(value);
                selected.push((outpoint, value));
            }
            ensure!(
                !selected.is_empty(),
                "no selected LN hot wallet UTXO provided"
            );

            let mut builder = local.wallet.build_tx();
            builder
                .fee_rate(fee_rate)
                .nlocktime(LockTime::ZERO)
                .manually_selected_only();
            for (address, amount) in &recipients {
                builder.add_recipient(address.script_pubkey(), Amount::from_sat(*amount));
            }
            for (outpoint, _) in &selected {
                builder
                    .add_utxo(*outpoint)
                    .with_context(|| format!("add LN hot wallet UTXO {outpoint}"))?;
            }

            let mut psbt = builder
                .finish()
                .context("build LN hot wallet batch withdrawal PSBT")?;
            let unsigned_psbt = psbt.to_string();
            let finalized = local
                .wallet
                .sign(&mut psbt, SignOptions::default())
                .context("sign LN hot wallet batch withdrawal PSBT")?;
            ensure!(
                finalized,
                "LN hot wallet batch withdrawal PSBT was not finalized"
            );
            local.persist()?;

            let signed_psbt = psbt.to_string();
            let tx = psbt
                .extract_tx()
                .context("extract LN hot wallet batch withdrawal transaction")?;
            let output_sats = tx
                .output
                .iter()
                .map(|output| output.value.to_sat())
                .sum::<u64>();
            let fee_sats = selected_sats.saturating_sub(output_sats);
            let chain_source =
                ChainSource::from_esplora_or_default(self.config.network, Some(&esplora))?;
            ln_rgb_log_signed_btc_tx("transfer_batch_with_inputs", &tx);
            let txid = broadcast_transaction(self.config.network, Some(&chain_source), &tx)?;
            Ok(json!({
                "module": "ln_rgb",
                "operation": "transfer_batch_with_inputs",
                "wallet": "ln_hot_wallet",
                "outputs": recipients
                    .iter()
                    .map(|(address, amount)| json!({
                        "to": address.to_string(),
                        "amount_sats": amount
                    }))
                    .collect::<Vec<_>>(),
                "amount_sats": amount_sats,
                "fee_rate_sat_vb": fee_rate_sat_vb,
                "fee_sats": fee_sats,
                "selected_sats": selected_sats,
                "change_sats": selected_sats.saturating_sub(amount_sats).saturating_sub(fee_sats),
                "inputs": selected
                    .iter()
                    .map(|(outpoint, value)| json!({
                        "outpoint": outpoint.to_string(),
                        "value": value
                    }))
                    .collect::<Vec<_>>(),
                "txid": txid.to_string(),
                "raw_tx": bytes_to_hex(&serialize(&tx)),
                "unsigned_psbt": unsigned_psbt,
                "signed_psbt": signed_psbt,
                "esplora": esplora
            }))
        })
    }

    pub fn transfer_rgb_l1_json(
        &self,
        asset_id: &str,
        amount: u64,
        recipient: &str,
        fee_rate_sat_vb: u64,
    ) -> Result<serde_json::Value> {
        const RGB_CONTAINER_SATS: u64 = 1_000;

        let asset_id = asset_id.trim();
        let recipient = recipient.trim();
        ensure!(!asset_id.is_empty(), "RGB asset_id must not be empty");
        ensure!(amount > 0, "RGB amount must be greater than zero");
        ensure!(!recipient.is_empty(), "RGB recipient must not be empty");
        let recipient_address = bitcoin::Address::from_str(recipient)
            .with_context(|| format!("invalid RGB recipient BTC address: {recipient}"))?
            .require_network(self.config.network)
            .with_context(|| {
                format!("RGB recipient address is not for {:?}", self.config.network)
            })?;
        let fee_rate_sat_vb = fee_rate_sat_vb.max(1);
        let fee_rate = FeeRate::from_sat_per_vb(fee_rate_sat_vb)
            .context("invalid LN hot wallet RGB withdrawal fee rate")?;

        let mut local = self.open_l1_wallet()?;
        let esplora = self.next_esplora_url();
        self.sync_l1_wallet_or_use_cached(
            &mut local,
            &esplora,
            "build LN hot wallet RGB withdrawal",
        )?;
        let confirmed_wallet_utxos = local
            .wallet
            .list_unspent()
            .filter(|utxo| utxo.chain_position.is_confirmed())
            .map(|utxo| (utxo.outpoint, utxo.txout.value.to_sat()))
            .collect::<HashMap<_, _>>();

        let client = self.rgb_service_client()?;
        let assets = client
            .list_assets(ListAssetsRequest {
                account_id: self.config.account_id.clone(),
            })
            .map_err(|err| anyhow!("{err}"))?;
        let mut all_rgb_outpoints = HashSet::new();
        let mut selected_rgb_outpoints = Vec::new();
        let mut selected_rgb_amount = 0u64;
        for (outpoint_text, allocations) in assets.utxo_assets {
            let outpoint = OutPoint::from_str(&outpoint_text)
                .with_context(|| format!("invalid RGB allocation outpoint: {outpoint_text}"))?;
            if !allocations.is_empty() {
                all_rgb_outpoints.insert(outpoint);
            }
            if selected_rgb_amount >= amount || !confirmed_wallet_utxos.contains_key(&outpoint) {
                continue;
            }
            let available = allocations
                .iter()
                .filter(|allocation| {
                    allocation.asset_id == asset_id
                        && allocation.layer == AssetLayer::L1
                        && allocation.status == AllocationStatus::Available
                })
                .map(|allocation| allocation.amount)
                .sum::<u64>();
            if available > 0 {
                selected_rgb_amount = selected_rgb_amount.saturating_add(available);
                selected_rgb_outpoints.push(outpoint);
            }
        }
        ensure!(
            selected_rgb_amount >= amount,
            "insufficient confirmed LN hot wallet RGB allocation: asset={asset_id} need={amount} available={selected_rgb_amount}"
        );

        let change_address = local
            .wallet
            .reveal_next_address(KeychainKind::Internal)
            .address;
        let change_script = change_address.script_pubkey();
        let recipient_script = recipient_address.script_pubkey();
        let mut builder = local.wallet.build_tx();
        builder
            .ordering(TxOrdering::Untouched)
            .fee_rate(fee_rate)
            .add_recipient(
                recipient_script.clone(),
                Amount::from_sat(RGB_CONTAINER_SATS),
            )
            .add_recipient(change_script.clone(), Amount::from_sat(RGB_CONTAINER_SATS))
            .add_data(&[0; 32])
            .drain_to(change_script.clone());
        for outpoint in &selected_rgb_outpoints {
            builder
                .add_utxo(*outpoint)
                .with_context(|| format!("add LN hot wallet RGB UTXO {outpoint}"))?;
        }
        let selected_set = selected_rgb_outpoints
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let protected_rgb_outpoints = all_rgb_outpoints
            .into_iter()
            .filter(|outpoint| !selected_set.contains(outpoint))
            .collect::<Vec<_>>();
        if !protected_rgb_outpoints.is_empty() {
            builder.unspendable(protected_rgb_outpoints);
        }

        let psbt = builder
            .finish()
            .context("build unsigned LN hot wallet RGB withdrawal PSBT")?;
        let recipient_vout = psbt
            .unsigned_tx
            .output
            .iter()
            .position(|output| {
                output.script_pubkey == recipient_script
                    && output.value == Amount::from_sat(RGB_CONTAINER_SATS)
            })
            .context("RGB withdrawal PSBT is missing recipient container output")?
            as u32;
        let change_vout = psbt
            .unsigned_tx
            .output
            .iter()
            .enumerate()
            .find(|(vout, output)| {
                *vout as u32 != recipient_vout
                    && output.script_pubkey == change_script
                    && output.value == Amount::from_sat(RGB_CONTAINER_SATS)
            })
            .map(|(vout, _)| vout as u32)
            .context("RGB withdrawal PSBT is missing sender change container output")?;
        let unsigned_anchor_psbt = bytes_to_hex(&psbt.serialize());
        let prepare_authorization = self.rgb_asset_spend_authorization(
            asset_id,
            amount,
            Some(recipient),
            Some(&unsigned_anchor_psbt),
        )?;
        let prepared = client
            .prepare_transfer(PrepareTransferRequest {
                account_id: self.config.account_id.clone(),
                asset_id: asset_id.to_string(),
                amount,
                recipient: recipient.to_string(),
                fee_rate_sat_vb: Some(fee_rate_sat_vb),
                unsigned_anchor_psbt: Some(unsigned_anchor_psbt.clone()),
                change_vout: Some(change_vout),
                recipient_vout: Some(recipient_vout),
                asset_authorization: prepare_authorization,
            })
            .map_err(|err| anyhow!("{err}"))?;
        let prepared_anchor_psbt = prepared
            .anchor_psbt
            .as_deref()
            .context("RGB prepare response missing anchor_psbt")?;
        let prepared_anchor_psbt_bytes = hex_to_bytes(prepared_anchor_psbt)
            .context("decode prepared RGB withdrawal anchor PSBT")?;
        let mut prepared_psbt = Psbt::deserialize(&prepared_anchor_psbt_bytes)
            .context("deserialize prepared RGB withdrawal anchor PSBT")?;
        let finalized = local
            .wallet
            .sign(&mut prepared_psbt, SignOptions::default())
            .context("sign LN hot wallet RGB withdrawal PSBT")?;
        ensure!(
            finalized,
            "LN hot wallet RGB withdrawal PSBT was not finalized"
        );
        local.persist()?;
        let signed_anchor_psbt = prepared_psbt.to_string();
        let tx = prepared_psbt
            .extract_tx()
            .context("extract LN hot wallet RGB withdrawal transaction")?;
        let chain_source =
            ChainSource::from_esplora_or_default(self.config.network, Some(&esplora))?;
        let txid = broadcast_transaction(self.config.network, Some(&chain_source), &tx)?;
        let commit_authorization =
            self.rgb_asset_spend_authorization(asset_id, amount, None, None)?;
        let commit = client.commit_transfer(CommitTransferRequest {
            account_id: self.config.account_id.clone(),
            transfer_id: prepared.transfer_id.clone(),
            txid: txid.to_string(),
            signed_anchor_psbt: Some(signed_anchor_psbt),
            utxos: vec![TrackedUtxo {
                outpoint: format!("{txid}:{recipient_vout}"),
                address: Some(recipient_address.to_string()),
                confirmed: false,
            }],
            asset_authorization: commit_authorization,
        });
        let commit = match commit {
            Ok(commit) => commit,
            Err(err) => {
                return Ok(json!({
                    "ok": false,
                    "module": "ln_rgb",
                    "operation": "transfer_rgb_l1",
                    "stage": "commit",
                    "wallet": "ln_hot_wallet",
                    "account_id": self.config.account_id,
                    "asset_id": asset_id,
                    "amount": amount,
                    "recipient": recipient_address.to_string(),
                    "transfer_id": prepared.transfer_id,
                    "txid": txid.to_string(),
                    "error": err.to_string()
                }));
            }
        };
        Ok(json!({
            "ok": true,
            "module": "ln_rgb",
            "operation": "transfer_rgb_l1",
            "status": "committed",
            "wallet": "ln_hot_wallet",
            "account_id": self.config.account_id,
            "asset_id": asset_id,
            "amount": amount,
            "recipient": recipient_address.to_string(),
            "recipient_vout": recipient_vout,
            "change_vout": change_vout,
            "selected_rgb_amount": selected_rgb_amount,
            "selected_rgb_outpoints": selected_rgb_outpoints
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "transfer_id": prepared.transfer_id,
            "txid": txid.to_string(),
            "commit": commit
        }))
    }

    pub fn prepare_external_rgb_l1_sweep_json(
        &self,
        asset_id: &str,
        amount: u64,
        source_outpoint: &str,
        source_address: &str,
        fee_rate_sat_vb: u64,
    ) -> Result<serde_json::Value> {
        const RGB_CONTAINER_SATS: u64 = 1_000;
        const P2WPKH_MAX_SATISFACTION_WEIGHT_WU: u64 = 112;

        let asset_id = asset_id.trim();
        let source_outpoint = source_outpoint.trim();
        let source_address = source_address.trim();
        ensure!(!asset_id.is_empty(), "RGB asset_id must not be empty");
        ensure!(amount > 0, "RGB amount must be greater than zero");
        ensure!(
            !source_outpoint.is_empty(),
            "RGB source_outpoint must not be empty"
        );
        ensure!(
            !source_address.is_empty(),
            "RGB source_address must not be empty"
        );
        let source_outpoint = OutPoint::from_str(source_outpoint)
            .with_context(|| format!("invalid RGB source outpoint: {source_outpoint}"))?;
        let source_address = bitcoin::Address::from_str(source_address)
            .with_context(|| format!("invalid RGB source address: {source_address}"))?
            .require_network(self.config.network)
            .with_context(|| format!("RGB source address is not for {:?}", self.config.network))?;
        ensure!(
            source_address.script_pubkey().is_p2wpkh(),
            "external RGB sweep currently requires a P2WPKH source"
        );
        let recipient_address = bitcoin::Address::from_str(&self.config.account_id)
            .with_context(|| {
                format!(
                    "invalid LN hot wallet RGB account address: {}",
                    self.config.account_id
                )
            })?
            .require_network(self.config.network)
            .with_context(|| {
                format!(
                    "LN hot wallet RGB account address is not for {:?}",
                    self.config.network
                )
            })?;
        let fee_rate_sat_vb = fee_rate_sat_vb.max(1);
        let fee_rate = FeeRate::from_sat_per_vb(fee_rate_sat_vb)
            .context("invalid external RGB sweep fee rate")?;

        let mut local = self.open_l1_wallet()?;
        let esplora = self.next_esplora_url();
        self.sync_l1_wallet_or_use_cached(
            &mut local,
            &esplora,
            "prepare external RGB sweep with LN hot wallet fee input",
        )?;
        let confirmed_wallet_utxos = local
            .wallet
            .list_unspent()
            .filter(|utxo| utxo.chain_position.is_confirmed())
            .map(|utxo| (utxo.outpoint, utxo.txout.clone()))
            .collect::<HashMap<_, _>>();
        ensure!(
            !confirmed_wallet_utxos.is_empty(),
            "LN hot wallet has no confirmed BTC UTXO for external RGB sweep fees"
        );

        let client = self.rgb_service_client()?;
        let source_assets = client
            .list_assets(ListAssetsRequest {
                account_id: source_address.to_string(),
            })
            .map_err(|err| anyhow!("query source RGB allocations: {err}"))?;
        let source_allocations = source_assets
            .utxo_assets
            .get(&source_outpoint.to_string())
            .cloned()
            .unwrap_or_default();
        ensure!(
            !source_allocations.is_empty(),
            "source outpoint has no RGB allocation in daemon account {}",
            source_address
        );
        ensure!(
            source_allocations.iter().all(|allocation| {
                allocation.asset_id == asset_id
                    && allocation.layer == AssetLayer::L1
                    && allocation.status == AllocationStatus::Available
            }),
            "source outpoint contains another or unavailable RGB allocation; batch custody sweep is required"
        );
        let source_asset_amount =
            source_allocations
                .iter()
                .try_fold(0u64, |total, allocation| {
                    total
                        .checked_add(allocation.amount)
                        .context("source RGB allocation amount overflow")
                })?;
        ensure!(
            source_asset_amount == amount,
            "external RGB sweep must move the complete allocation: requested={amount} available={source_asset_amount}"
        );

        let chain_source =
            ChainSource::from_esplora_or_default(self.config.network, Some(&esplora))?;
        let source_prev_tx = match &chain_source {
            ChainSource::Esplora(config) => esplora_client_with_config(config)
                .get_tx(&source_outpoint.txid)
                .with_context(|| {
                    format!("fetch external RGB source transaction {source_outpoint}")
                })?
                .with_context(|| {
                    format!("external RGB source transaction not found: {source_outpoint}")
                })?,
            ChainSource::Electrum(config) => electrum_get_transaction(config, source_outpoint.txid)
                .with_context(|| {
                    format!("fetch external RGB source transaction {source_outpoint}")
                })?,
            ChainSource::BitcoinCore(config) => config
                .client()?
                .get_raw_transaction(&source_outpoint.txid, None)
                .with_context(|| {
                    format!("fetch external RGB source transaction {source_outpoint}")
                })?,
        };
        let source_txout = source_prev_tx
            .output
            .get(source_outpoint.vout as usize)
            .cloned()
            .with_context(|| format!("external RGB source output not found: {source_outpoint}"))?;
        ensure!(
            source_txout.script_pubkey == source_address.script_pubkey(),
            "external RGB source outpoint does not pay the declared source address"
        );
        ensure!(
            source_txout.value.to_sat() == RGB_CONTAINER_SATS,
            "external RGB source container must contain {RGB_CONTAINER_SATS} sats"
        );

        let rgb_change_address = local
            .wallet
            .reveal_next_address(KeychainKind::Internal)
            .address;
        let btc_change_address = local
            .wallet
            .reveal_next_address(KeychainKind::Internal)
            .address;
        let recipient_script = recipient_address.script_pubkey();
        let rgb_change_script = rgb_change_address.script_pubkey();
        let btc_change_script = btc_change_address.script_pubkey();
        let foreign_input = bitcoin::psbt::Input {
            witness_utxo: Some(source_txout.clone()),
            non_witness_utxo: Some(source_prev_tx),
            ..Default::default()
        };

        let target_assets = client
            .list_assets(ListAssetsRequest {
                account_id: self.config.account_id.clone(),
            })
            .map_err(|err| anyhow!("query LN hot wallet RGB allocations: {err}"))?;
        let protected_rgb_outpoints = target_assets
            .utxo_assets
            .into_iter()
            .filter_map(|(outpoint, allocations)| {
                let outpoint = OutPoint::from_str(&outpoint).ok()?;
                (!allocations.is_empty() && confirmed_wallet_utxos.contains_key(&outpoint))
                    .then_some(outpoint)
            })
            .collect::<Vec<_>>();

        let mut builder = local.wallet.build_tx();
        builder
            .ordering(TxOrdering::Untouched)
            .fee_rate(fee_rate)
            .add_recipient(
                recipient_script.clone(),
                Amount::from_sat(RGB_CONTAINER_SATS),
            )
            .add_recipient(
                rgb_change_script.clone(),
                Amount::from_sat(RGB_CONTAINER_SATS),
            )
            .add_data(&[0; 32])
            .drain_to(btc_change_script.clone())
            .add_foreign_utxo(
                source_outpoint,
                foreign_input,
                Weight::from_wu(P2WPKH_MAX_SATISFACTION_WEIGHT_WU),
            )
            .with_context(|| format!("add external RGB source UTXO {source_outpoint}"))?;
        if !protected_rgb_outpoints.is_empty() {
            builder.unspendable(protected_rgb_outpoints.clone());
        }
        let psbt = builder
            .finish()
            .context("build unsigned external RGB custody sweep PSBT")?;
        let recipient_vout = psbt
            .unsigned_tx
            .output
            .iter()
            .position(|output| {
                output.script_pubkey == recipient_script
                    && output.value == Amount::from_sat(RGB_CONTAINER_SATS)
            })
            .context("external RGB sweep PSBT is missing recipient container output")?
            as u32;
        let change_vout = psbt
            .unsigned_tx
            .output
            .iter()
            .position(|output| {
                output.script_pubkey == rgb_change_script
                    && output.value == Amount::from_sat(RGB_CONTAINER_SATS)
            })
            .context("external RGB sweep PSBT is missing blank-state change output")?
            as u32;
        ensure!(
            recipient_vout != change_vout,
            "external RGB sweep recipient and change outputs must differ"
        );
        let unsigned_anchor_psbt = bytes_to_hex(&psbt.serialize());
        let prepare_authorization = self.rgb_asset_spend_authorization(
            asset_id,
            amount,
            Some(&self.config.account_id),
            Some(&unsigned_anchor_psbt),
        )?;
        let prepared = client
            .prepare_transfer(PrepareTransferRequest {
                account_id: source_address.to_string(),
                asset_id: asset_id.to_string(),
                amount,
                recipient: self.config.account_id.clone(),
                fee_rate_sat_vb: Some(fee_rate_sat_vb),
                unsigned_anchor_psbt: Some(unsigned_anchor_psbt),
                change_vout: Some(change_vout),
                recipient_vout: Some(recipient_vout),
                asset_authorization: prepare_authorization,
            })
            .map_err(|err| anyhow!("prepare external RGB custody sweep: {err}"))?;
        let prepared_anchor_psbt = prepared
            .anchor_psbt
            .as_deref()
            .context("RGB prepare response missing anchor_psbt")?;
        let prepared_anchor_psbt_bytes = hex_to_bytes(prepared_anchor_psbt)
            .context("decode prepared external RGB sweep anchor PSBT")?;
        let mut prepared_psbt = Psbt::deserialize(&prepared_anchor_psbt_bytes)
            .context("deserialize prepared external RGB sweep anchor PSBT")?;
        let source_input_index = prepared_psbt
            .unsigned_tx
            .input
            .iter()
            .position(|input| input.previous_output == source_outpoint)
            .context("prepared external RGB sweep PSBT lost the source input")?;
        let _finalized = local
            .wallet
            .sign(
                &mut prepared_psbt,
                SignOptions {
                    trust_witness_utxo: true,
                    try_finalize: false,
                    ..SignOptions::default()
                },
            )
            .context("partially sign external RGB sweep with LN hot wallet")?;
        let source_input = prepared_psbt
            .inputs
            .get(source_input_index)
            .context("prepared external RGB source input metadata is missing")?;
        ensure!(
            source_input.partial_sigs.is_empty()
                && source_input.tap_key_sig.is_none()
                && source_input.final_script_sig.is_none()
                && source_input.final_script_witness.is_none(),
            "LN hot wallet unexpectedly signed or finalized the external RGB source input"
        );
        let signed_ln_input_count = prepared_psbt
            .inputs
            .iter()
            .enumerate()
            .filter(|(index, input)| {
                *index != source_input_index
                    && (!input.partial_sigs.is_empty()
                        || input.tap_key_sig.is_some()
                        || input.final_script_sig.is_some()
                        || input.final_script_witness.is_some())
            })
            .count();
        ensure!(
            signed_ln_input_count > 0,
            "LN hot wallet did not sign any external RGB sweep fee input"
        );
        local.persist()?;

        let input_sats =
            prepared_psbt
                .unsigned_tx
                .input
                .iter()
                .try_fold(0u64, |total, input| {
                    let value = if input.previous_output == source_outpoint {
                        source_txout.value.to_sat()
                    } else {
                        confirmed_wallet_utxos
                            .get(&input.previous_output)
                            .with_context(|| {
                                format!(
                                    "prepared external RGB sweep selected unknown LN input {}",
                                    input.previous_output
                                )
                            })?
                            .value
                            .to_sat()
                    };
                    total
                        .checked_add(value)
                        .context("sweep input value overflow")
                })?;
        let output_sats =
            prepared_psbt
                .unsigned_tx
                .output
                .iter()
                .try_fold(0u64, |total, output| {
                    total
                        .checked_add(output.value.to_sat())
                        .context("sweep output value overflow")
                })?;
        ensure!(
            input_sats >= output_sats,
            "external RGB sweep fee underflow"
        );
        let fee_sats = input_sats - output_sats;
        let local_input_outpoints = prepared_psbt
            .unsigned_tx
            .input
            .iter()
            .filter(|input| input.previous_output != source_outpoint)
            .map(|input| input.previous_output.to_string())
            .collect::<Vec<_>>();
        Ok(json!({
            "ok": true,
            "module": "ln_rgb",
            "operation": "prepare_external_rgb_l1_sweep",
            "status": "awaiting_external_signature",
            "broadcasted": false,
            "committed": false,
            "source_account_id": source_address.to_string(),
            "source_address": source_address.to_string(),
            "source_outpoint": source_outpoint.to_string(),
            "source_input_index": source_input_index,
            "source_container_sats": source_txout.value.to_sat(),
            "asset_id": asset_id,
            "amount": amount,
            "recipient_account_id": self.config.account_id,
            "recipient_address": recipient_address.to_string(),
            "recipient_vout": recipient_vout,
            "rgb_change_address": rgb_change_address.to_string(),
            "change_vout": change_vout,
            "btc_change_address": btc_change_address.to_string(),
            "fee_rate_sat_vb": fee_rate_sat_vb,
            "fee_sats": fee_sats,
            "input_sats": input_sats,
            "output_sats": output_sats,
            "ln_fee_inputs": local_input_outpoints,
            "ln_signed_input_count": signed_ln_input_count,
            "protected_ln_rgb_outpoints": protected_rgb_outpoints
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "transfer_id": prepared.transfer_id,
            "unsigned_txid": prepared_psbt.unsigned_tx.compute_txid().to_string(),
            "partially_signed_psbt": prepared_psbt.to_string(),
            "prepared_anchor_psbt": bytes_to_hex(&prepared_psbt.serialize())
        }))
    }

    pub fn commit_rgb_l1_transfer_json(
        &self,
        asset_id: &str,
        amount: u64,
        transfer_id: &str,
        txid: &str,
    ) -> Result<serde_json::Value> {
        self.commit_rgb_l1_transfer_for_account_json(
            &self.config.account_id,
            asset_id,
            amount,
            transfer_id,
            txid,
            "commit_rgb_l1_transfer",
        )
    }

    pub fn commit_external_rgb_l1_sweep_json(
        &self,
        source_account_id: &str,
        asset_id: &str,
        amount: u64,
        transfer_id: &str,
        txid: &str,
    ) -> Result<serde_json::Value> {
        ensure!(
            !source_account_id.trim().is_empty(),
            "external RGB source account_id must not be empty"
        );
        self.commit_rgb_l1_transfer_for_account_json(
            source_account_id,
            asset_id,
            amount,
            transfer_id,
            txid,
            "commit_external_rgb_l1_sweep",
        )
    }

    fn commit_rgb_l1_transfer_for_account_json(
        &self,
        account_id: &str,
        asset_id: &str,
        amount: u64,
        transfer_id: &str,
        txid: &str,
        operation: &str,
    ) -> Result<serde_json::Value> {
        ensure!(
            !asset_id.trim().is_empty(),
            "RGB asset_id must not be empty"
        );
        ensure!(amount > 0, "RGB amount must be greater than zero");
        ensure!(
            !transfer_id.trim().is_empty(),
            "RGB transfer_id must not be empty"
        );
        Txid::from_str(txid).with_context(|| format!("invalid RGB withdrawal txid: {txid}"))?;
        let client = self.rgb_service_client()?;
        let asset_authorization =
            self.rgb_asset_spend_authorization(asset_id, amount, None, None)?;
        let commit = client
            .commit_transfer(CommitTransferRequest {
                account_id: account_id.to_string(),
                transfer_id: transfer_id.to_string(),
                txid: txid.to_string(),
                signed_anchor_psbt: None,
                utxos: Vec::new(),
                asset_authorization,
            })
            .map_err(|err| anyhow!("{err}"))?;
        Ok(json!({
            "ok": true,
            "module": "ln_rgb",
            "operation": operation,
            "status": "committed",
            "wallet": "ln_hot_wallet",
            "account_id": account_id,
            "asset_id": asset_id,
            "amount": amount,
            "transfer_id": transfer_id,
            "txid": txid,
            "commit": commit
        }))
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
                let url = urls[(start + offset) % urls.len()].clone();
                ChainSource::from_esplora_or_default(self.config.network, Some(&url))
            })
            .collect::<Result<Vec<_>>>()?)
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
            let funding_outpoint = record
                .funding_outpoint
                .parse::<OutPoint>()
                .with_context(|| format!("invalid funding outpoint in {}", path.display()))?;
            loaded.insert(
                temporary_channel_id,
                PendingRgbFundingTransfer {
                    peer_node_id,
                    funding_outpoint,
                    funding_ref: record.funding_ref,
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
            loaded.push_back(RgbFundingTransfer {
                temporary_channel_id,
                peer_node_id,
                funding_outpoint,
                funding_ref: record.funding_ref,
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

    fn restore_rgb_payment_metadata(&self, channel_manager: &LnRgbChannelManager) -> Result<usize> {
        let mut restored = 0usize;
        let payment_assets = self
            .rgb_payment_assets
            .lock()
            .expect("rgb payment asset lock poisoned")
            .clone();

        for (payment_id_hex, asset) in payment_assets {
            let payment_id = PaymentId(hex_to_32(&payment_id_hex)?);
            channel_manager.restore_rgb_payment_metadata(
                payment_id,
                RgbPaymentMetadata::new(ldk_rgb_asset(&asset)),
            );
            restored += 1;
        }

        Ok(restored)
    }

    fn load_persisted_peers_from_disk(&self) -> Result<Vec<(PublicKey, SocketAddress)>> {
        let dir = self.peer_record_dir();
        let Ok(entries) = fs::read_dir(&dir) else {
            return Ok(Vec::new());
        };
        let mut peers = Vec::new();
        for entry in entries.filter_map(|entry| entry.ok()) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: PersistedPeerRecord = serde_json::from_slice(
                &fs::read(&path).with_context(|| format!("read peer record {}", path.display()))?,
            )
            .with_context(|| format!("decode peer record {}", path.display()))?;
            let node_id = record
                .node_id
                .parse::<PublicKey>()
                .with_context(|| format!("invalid peer node id in {}", path.display()))?;
            let address = SocketAddress::from_str(&record.address)
                .map_err(|_| anyhow!("invalid peer address in {}", path.display()))?;
            peers.push((node_id, address));
        }
        Ok(peers)
    }

    fn persist_connected_peer(&self, node_id: PublicKey, address: &SocketAddress) -> Result<()> {
        let path = self.peer_record_path(&node_id);
        let now = now_secs();
        let created_at = fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<PersistedPeerRecord>(&bytes).ok())
            .map(|record| record.created_at)
            .unwrap_or(now);
        let record = PersistedPeerRecord {
            node_id: node_id.to_string(),
            address: address.to_string(),
            created_at,
            updated_at: now,
            last_connected_at: now,
        };
        ensure_parent_dir(&path)?;
        fs::write(&path, serde_json::to_vec_pretty(&record)?)
            .with_context(|| format!("save peer record {}", path.display()))?;
        Ok(())
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

    fn log_event(&self, message: impl AsRef<str>) {
        ln_rgb_log_line(message.as_ref());
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

    pub fn retry_pending_sweeps(&self) {
        self.poll_ldk_events();
        let output_sweeper = {
            let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
            let Some(runtime) = runtime_guard.as_ref() else {
                return;
            };
            Arc::clone(&runtime.output_sweeper)
        };
        if output_sweeper
            .force_regenerate_and_broadcast_spend()
            .is_err()
        {
            self.log_event("ln-rgb forced output sweeper broadcast failed".to_string());
        }
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
            self.log_event("ln-rgb pending HTLCs forwarded".to_string());
        }
        let handler = |event: Event| -> std::result::Result<(), lightning::events::ReplayEvent> {
            if let Err(err) = self.handle_ldk_event(&channel_manager, &output_sweeper, event) {
                self.log_event(format!("ln-rgb LDK event handling failed: {err:#}"));
            }
            Ok(())
        };
        channel_manager.process_pending_events(&handler);
        chain_monitor.process_pending_events(&handler);
        if run_maintenance {
            if output_sweeper
                .regenerate_and_broadcast_spend_if_necessary()
                .is_err()
            {
                self.log_event("ln-rgb output sweeper failed to broadcast spend".to_string());
            }
            if let Err(err) = self.reconcile_rgb_sweep_records_from_sweeper(&output_sweeper) {
                self.log_event(format!(
                    "ln-rgb RGB sweep record reconciliation failed: {err:#}"
                ));
            }
            if let Err(err) = self.reconcile_rgb_maturity_records_from_monitors(&chain_monitor) {
                self.log_event(format!(
                    "ln-rgb RGB maturity record reconciliation failed: {err:#}"
                ));
            }
        }
        peer_manager.process_events();
        if channel_manager.get_and_clear_needs_persistence() {
            if let Err(err) = Self::persist_channel_manager_to_store(&kv_store, &channel_manager) {
                self.log_event(format!(
                    "ln-rgb channel manager persistence failed: {err:#}"
                ));
            }
        }
        if run_maintenance {
            if let Err(err) = self.try_confirm_rgb_funding_refs() {
                self.log_event(format!(
                    "ln-rgb RGB funding ref confirmation failed: {err:#}"
                ));
            }
            if let Err(err) = self.try_attach_inbound_rgb_funding_refs() {
                self.log_event(format!(
                    "ln-rgb inbound RGB funding ref lookup failed: {err:#}"
                ));
            }
            if let Err(err) = self.try_confirm_rgb_sweep_carriers() {
                self.log_event(format!(
                    "ln-rgb RGB sweep carrier confirmation failed: {err:#}"
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
                if let Some(asset) = asset {
                    let channel = hex32(temporary_channel_id.0);
                    let (tx, funding_outpoint, funding_ref) = self
                        .build_rgb_funding_transaction(
                            channel_value_satoshis,
                            output_script.clone(),
                            &channel,
                            &asset,
                        )
                        .context("build RGB LN funding transaction")?;
                    let txid = tx.compute_txid();
                    let generated_transfer = RgbFundingTransfer {
                        temporary_channel_id: ChannelId(temporary_channel_id.0),
                        peer_node_id: counterparty_node_id,
                        funding_outpoint,
                        funding_ref: funding_ref.clone(),
                    };
                    self.persist_generated_rgb_funding_transfer(&generated_transfer)?;
                    channel_manager
                        .provide_funding_rgb_transfer_for_unfunded_channel(
                            temporary_channel_id,
                            counterparty_node_id,
                            ldk_rgb_funding_transfer(funding_ref.clone()),
                        )
                        .map_err(|err| anyhow!("LDK rejected RGB funding ref: {err:?}"))?;
                    self.save_rgb_funding_binding(
                        temporary_channel_id,
                        counterparty_node_id,
                        funding_outpoint,
                        funding_ref,
                    )?;
                    self.submit_funding_transaction(
                        channel_manager,
                        temporary_channel_id,
                        counterparty_node_id,
                        tx.clone(),
                    )?;
                    self.record_local_unconfirmed(tx)?;
                    self.rgb_channel_assets
                        .lock()
                        .expect("rgb channel asset lock poisoned")
                        .remove(&user_channel_id);
                    self.log_event(format!(
                            "ln-rgb RGB funding transaction generated: user_channel_id={user_channel_id} temporary_channel_id={temporary_channel_id} channel_id={channel} funding_outpoint={funding_outpoint} txid={txid}"
                        ));
                } else {
                    let tx = self
                        .build_funding_transaction(channel_value_satoshis, output_script.clone())?;
                    let funding_outpoint =
                        funding_outpoint_from_tx(&tx, &output_script, channel_value_satoshis)
                            .context(
                                "LDK funding transaction is missing requested funding output",
                            )?;
                    let txid = tx.compute_txid();
                    self.submit_funding_transaction(
                        channel_manager,
                        temporary_channel_id,
                        counterparty_node_id,
                        tx.clone(),
                    )?;
                    self.record_local_unconfirmed(tx)?;
                    self.log_event(format!(
                            "ln-rgb funding transaction generated: user_channel_id={user_channel_id} temporary_channel_id={temporary_channel_id} funding_outpoint={funding_outpoint} txid={txid}"
                        ));
                }
            }
            Event::FundingTransactionReadyForSigning {
                channel_id,
                counterparty_node_id,
                user_channel_id,
                unsigned_transaction,
            } => {
                let txid = unsigned_transaction.compute_txid();
                let signed_transaction =
                    self.sign_interactive_funding_transaction(unsigned_transaction)?;
                channel_manager
                    .funding_transaction_signed(
                        &channel_id,
                        &counterparty_node_id,
                        signed_transaction,
                    )
                    .map_err(|err| {
                        anyhow!("LDK rejected signed splice funding transaction: {err:?}")
                    })?;
                self.log_event(format!(
                        "ln-rgb interactive funding transaction signed: user_channel_id={user_channel_id} channel_id={channel_id} peer={counterparty_node_id} txid={txid}"
                    ));
            }
            Event::SplicePending {
                channel_id,
                user_channel_id,
                counterparty_node_id,
                new_funding_txo,
                ..
            } => {
                self.log_event(format!(
                        "ln-rgb splice pending: user_channel_id={user_channel_id} channel_id={channel_id} peer={counterparty_node_id} new_funding_txo={new_funding_txo}"
                    ));
            }
            Event::SpliceFailed {
                channel_id,
                user_channel_id,
                counterparty_node_id,
                abandoned_funding_txo,
                contributed_inputs,
                ..
            } => {
                self.log_event(format!(
                        "ln-rgb splice failed: user_channel_id={user_channel_id} channel_id={channel_id} peer={counterparty_node_id} abandoned_funding_txo={abandoned_funding_txo:?} contributed_inputs={contributed_inputs:?}"
                    ));
            }
            Event::DiscardFunding { channel_id, .. } => {
                self.log_event(format!("ln-rgb discard funding: channel_id={channel_id}"));
            }
            Event::OpenChannelRequest {
                temporary_channel_id,
                counterparty_node_id,
                ..
            } => {
                if !self.config.accept_inbound_channels {
                    self.log_event(format!(
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
                    self.log_event(format!(
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
                    self.log_event(format!(
                            "ln-rgb accepted inbound channel: peer={counterparty_node_id} temporary_channel_id={temporary_channel_id}"
                        ));
                }
                self.pending_inbound_rgb_channels
                    .lock()
                    .expect("pending inbound rgb channel lock poisoned")
                    .insert(temporary_channel_id, counterparty_node_id);
            }
            Event::FundingTxBroadcastSafe {
                channel_id,
                funding_txo,
                counterparty_node_id,
                ..
            } => {
                self.log_event(format!(
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
                    if let Some(binding) = self.try_confirm_rgb_funding_ref(funding_txo)? {
                        self.log_event(format!(
                                "ln-rgb RGB funding ref confirmed: channel_id={channel_id} funding={} transfer_id={}",
                                binding.funding_outpoint, binding.funding_ref.transfer_id
                            ));
                    } else {
                        self.mark_rgb_funding_binding_status_if_pending(
                            funding_txo,
                            "waiting_confirmation",
                        )?;
                    }
                }
                self.log_event(format!("ln-rgb channel ready: channel_id={channel_id}"));
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
                self.log_event(format!(
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
                    self.log_event(format!(
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
                    if let Err(err) = self.claim_inbound_rgb_payment(
                        payment_hash,
                        amount_msat,
                        &contract_id.to_string(),
                        rgb_amount,
                    ) {
                        self.log_event(format!("ln-rgb daemon RGB payment claim failed: {err:#}"));
                    }
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
                    self.log_event(format!(
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
                    self.log_event(format!(
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
                self.log_event("ln-rgb payment sent".to_string());
            }
            Event::PaymentFailed { payment_id, .. } => {
                self.mark_outbound_rgb_payment_status(payment_id, "failed")?;
                self.btc_events
                    .lock()
                    .expect("ln-rgb btc event lock poisoned")
                    .push_back(BtcLnEvent::PaymentFailed {
                        payment_id: Some(hex32(payment_id.0)),
                    });
                self.log_event("ln-rgb payment failed".to_string());
            }
            other => {
                self.log_event(format!("ln-rgb LDK event: {other:?}"));
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

    fn try_open_l1_wallet(&self) -> Result<Option<LocalWallet>> {
        let mnemonic = self
            .config
            .entropy_mnemonic
            .as_deref()
            .context("ln-rgb funding requires mnemonic from zs config")?;
        let mnemonic = BdkMnemonic::parse_in_normalized(BdkLanguage::English, mnemonic)
            .context("invalid ln-rgb mnemonic from runtime config")?;
        LocalWallet::try_open_with_mnemonic(
            &self.config.l1_data_dir,
            self.config.network,
            &mnemonic,
        )
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
        channel_id: &str,
        asset: &RgbAssetAmount,
    ) -> Result<(Transaction, OutPoint, RgbFundingRef)> {
        self.retry_transient_esplora("build RGB LN funding transaction", || {
            let mut local = self.open_l1_wallet()?;
            let esplora = self.next_esplora_url();
            self.sync_l1_wallet_or_use_cached(
                &mut local,
                &esplora,
                "build RGB LN funding transaction",
            )?;

            let fee_rate = FeeRate::from_sat_per_vb(2)
                .context("invalid RGB LN funding transaction fee rate")?;
            let mut builder = local.wallet.build_tx();
            builder
                .ordering(TxOrdering::Untouched)
                .add_recipient(
                    output_script.clone(),
                    Amount::from_sat(channel_value_satoshis),
                )
                .add_data(&[0; 32])
                .fee_rate(fee_rate)
                .nlocktime(LockTime::ZERO);

            let psbt = builder
                .finish()
                .context("failed to build unsigned RGB LN funding PSBT")?;
            let funding_vout = psbt
                .unsigned_tx
                .output
                .iter()
                .position(|output| {
                    output.script_pubkey == output_script
                        && output.value == Amount::from_sat(channel_value_satoshis)
                })
                .context("unsigned RGB LN funding PSBT is missing funding output")?
                as u32;
            let change_vout = psbt
                .unsigned_tx
                .output
                .iter()
                .enumerate()
                .find(|(vout, output)| {
                    *vout as u32 != funding_vout
                        && !output.script_pubkey.is_op_return()
                        && output.value > Amount::ZERO
                })
                .map(|(vout, _)| vout as u32)
                .context("unsigned RGB LN funding PSBT is missing RGB change output")?;
            let unsigned_anchor_psbt = bytes_to_hex(&psbt.serialize());
            let expires_at_ms = now_secs().saturating_mul(1000).saturating_add(300_000);
            let contract_id = asset.contract_id.to_string();
            let purpose = AssetSpendPurpose::L2Reserve;
            let authorization_payload = json!({
                "asset_id": contract_id,
                "amount": asset.amount,
                "purpose": purpose,
                "recipient": null,
                "anchor_psbt": unsigned_anchor_psbt,
                "expires_at_ms": expires_at_ms
            });
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).context("generate LN RGB asset authorization nonce")?;
            let nonce = bytes_to_hex(&nonce);
            let timestamp_ms = now_millis();
            let authorization_payload_bytes = serde_json::to_vec(&authorization_payload)
                .context("encode LN RGB asset authorization payload")?;
            let mut engine = sha256::Hash::engine();
            engine.input(b"bihelix-ln-rgb-auth-v1");
            engine.input(b"l2_reserve");
            engine.input(nonce.as_bytes());
            engine.input(&timestamp_ms.to_be_bytes());
            engine.input(&authorization_payload_bytes);
            let message = Message::from_digest(sha256::Hash::from_engine(engine).to_byte_array());
            let secp = Secp256k1::new();
            let signature = secp.sign_ecdsa(&message, &self.node_secret);
            let signature = RequestSignature {
                signer_id: self.node_id.to_string(),
                public_key: self.node_id.to_string(),
                scheme: SignatureScheme::Ecdsa,
                nonce,
                timestamp_ms,
                signature: bytes_to_hex(&signature.serialize_der()),
            };
            let asset_authorization = AssetSpendAuthorization {
                asset_id: contract_id.clone(),
                amount: asset.amount,
                purpose,
                recipient: None,
                anchor_psbt: Some(unsigned_anchor_psbt.clone()),
                expires_at_ms,
                signature,
            };
            let client = RgbServiceClient::new(
                self.config.rgb_service_url.clone(),
                Arc::new(BackendRgbServiceSigner {
                    node_id: self.node_id,
                    node_secret: self.node_secret,
                }),
            )
            .map_err(|err| anyhow!("{err}"))?;
            let prepared = client
                .prepare_ln_channel_open(LnChannelOpenPrepareRequest {
                    account_id: self.config.account_id.clone(),
                    channel_id: channel_id.to_string(),
                    contract_id,
                    unsigned_anchor_psbt,
                    change_vout,
                    funding_vout,
                    funding_rgb: asset.amount,
                    to_local_rgb: asset.amount,
                    to_remote_rgb: 0,
                    asset_authorization,
                })
                .map_err(|err| anyhow!("{err}"))?;
            let prepared_anchor_psbt = hex_to_bytes(&prepared.anchor_psbt)
                .context("decode RGB LN prepared anchor PSBT hex")?;
            let mut prepared_psbt = Psbt::deserialize(&prepared_anchor_psbt)
                .context("deserialize RGB LN prepared anchor PSBT")?;
            let finalized = local
                .wallet
                .sign(&mut prepared_psbt, SignOptions::default())
                .context("failed to sign RGB LN funding transaction")?;
            if !finalized {
                bail!("RGB LN funding transaction was not finalized");
            }
            local.persist()?;
            let tx = prepared_psbt
                .extract_tx()
                .context("failed to extract RGB LN funding transaction")?;
            let funding_outpoint = OutPoint::from_str(&prepared.funding_outpoint)
                .context("RGB service returned invalid funding_outpoint")?;
            ensure!(
                tx.compute_txid() == funding_outpoint.txid,
                "RGB LN funding txid mismatch: signed={} daemon={}",
                tx.compute_txid(),
                funding_outpoint.txid
            );
            ensure!(
                tx.output
                    .get(funding_outpoint.vout as usize)
                    .is_some_and(|output| output.script_pubkey == output_script),
                "RGB LN funding output script changed"
            );
            Ok((tx, funding_outpoint, prepared.funding_ref))
        })
    }

    fn build_splice_in_contribution(
        &self,
        amount_sats: u64,
        funding_feerate_per_kw: u32,
    ) -> Result<SpliceContribution> {
        self.retry_transient_esplora("select LN splice inputs", || {
            let mut local = self.open_l1_wallet()?;
            let esplora = self.next_esplora_url();
            self.sync_l1_wallet_or_use_cached(&mut local, &esplora, "select LN splice inputs")?;
            let client = esplora_client_with_config(&self.esplora_config(esplora));
            let mut candidates = local
                .wallet
                .list_unspent()
                .filter(|utxo| utxo.chain_position.is_confirmed())
                .filter(|utxo| {
                    utxo.txout.script_pubkey.is_p2wpkh() || utxo.txout.script_pubkey.is_p2tr()
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|utxo| utxo.txout.value.to_sat());

            let fee_rate_sat_vb = u64::from(funding_feerate_per_kw)
                .saturating_add(249)
                .checked_div(250)
                .unwrap_or(1)
                .max(1);
            let mut selected_sats = 0u64;
            let mut inputs = Vec::new();
            for utxo in candidates {
                let outpoint = utxo.outpoint;
                let prevtx = client
                    .get_tx(&outpoint.txid)
                    .with_context(|| format!("fetch splice input prevtx {outpoint}"))?
                    .with_context(|| format!("splice input prevtx not found: {outpoint}"))?;
                let input = if utxo.txout.script_pubkey.is_p2wpkh() {
                    FundingTxInput::new_p2wpkh(prevtx, outpoint.vout)
                } else {
                    FundingTxInput::new_p2tr_key_spend(prevtx, outpoint.vout)
                }
                .map_err(|()| anyhow!("unsupported splice input script for {outpoint}"))?;
                selected_sats = selected_sats.saturating_add(utxo.txout.value.to_sat());
                inputs.push(input);

                let estimated_vbytes = 250u64.saturating_add((inputs.len() as u64) * 110);
                let required_sats =
                    amount_sats.saturating_add(fee_rate_sat_vb.saturating_mul(estimated_vbytes));
                if selected_sats >= required_sats {
                    break;
                }
            }

            let estimated_vbytes = 250u64.saturating_add((inputs.len() as u64) * 110);
            let required_sats =
                amount_sats.saturating_add(fee_rate_sat_vb.saturating_mul(estimated_vbytes));
            ensure!(
                selected_sats >= required_sats,
                "insufficient confirmed P2WPKH/P2TR L1 funds for splice: need about {required_sats} sats, selected {selected_sats} sats"
            );
            let change_script = local
                .wallet
                .reveal_next_address(KeychainKind::Internal)
                .address
                .script_pubkey();
            local.persist()?;
            Ok(SpliceContribution::SpliceIn {
                value: Amount::from_sat(amount_sats),
                inputs,
                change_script: Some(change_script),
            })
        })
    }

    fn build_splice_out_contribution(&self, amount_sats: u64) -> Result<SpliceContribution> {
        let mut local = self.open_l1_wallet()?;
        let address = local
            .wallet
            .reveal_next_address(KeychainKind::External)
            .address;
        let script_pubkey = address.script_pubkey();
        local.persist()?;
        Ok(SpliceContribution::SpliceOut {
            outputs: vec![TxOut {
                value: Amount::from_sat(amount_sats),
                script_pubkey,
            }],
        })
    }

    fn sign_interactive_funding_transaction(
        &self,
        unsigned_transaction: Transaction,
    ) -> Result<Transaction> {
        self.retry_transient_esplora("sign LN interactive funding transaction", || {
            let unsigned_txid = unsigned_transaction.compute_txid();
            let mut local = self.open_l1_wallet()?;
            let esplora = self.next_esplora_url();
            self.sync_l1_wallet_or_use_cached(
                &mut local,
                &esplora,
                "sign LN interactive funding transaction",
            )?;
            let known_utxos = local
                .wallet
                .list_unspent()
                .map(|utxo| (utxo.outpoint, utxo.txout))
                .collect::<HashMap<_, _>>();
            let mut psbt = Psbt::from_unsigned_tx(unsigned_transaction.clone())
                .context("build splice funding PSBT")?;
            for (index, txin) in psbt.unsigned_tx.input.iter().enumerate() {
                if let Some(txout) = known_utxos.get(&txin.previous_output) {
                    psbt.inputs[index].witness_utxo = Some(txout.clone());
                }
            }
            local
                .wallet
                .sign(
                    &mut psbt,
                    SignOptions {
                        trust_witness_utxo: true,
                        try_finalize: true,
                        ..SignOptions::default()
                    },
                )
                .context("sign LN interactive funding PSBT")?;
            let signed_transaction = psbt.extract_tx_unchecked_fee_rate();
            ensure!(
                signed_transaction.compute_txid() == unsigned_txid,
                "LDK interactive funding txid changed while signing"
            );
            ensure!(
                signed_transaction
                    .input
                    .iter()
                    .any(|input| !input.witness.is_empty()),
                "BDK did not sign any local splice input"
            );
            local.persist()?;
            Ok(signed_transaction)
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
                self.log_event(format!(
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
                    self.log_event(
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
            self.log_event(format!(
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
        let now = now_secs();
        let record = PendingRgbFundingTransferRecord {
            temporary_channel_id: base.clone(),
            peer_node_id: pending.peer_node_id.to_string(),
            funding_outpoint: pending.funding_outpoint.to_string(),
            funding_ref: pending.funding_ref.clone(),
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
        let now = now_secs();
        let record = GeneratedRgbFundingTransferRecord {
            temporary_channel_id: base.clone(),
            peer_node_id: transfer.peer_node_id.to_string(),
            funding_outpoint: transfer.funding_outpoint.to_string(),
            funding_ref: transfer.funding_ref.clone(),
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
            let transfer_peer = pending_rgb.peer_node_id.to_string();
            let funding_peer = pending_tx.peer_node_id.to_string();
            self.pending_funding_transactions
                .lock()
                .expect("pending funding transaction lock poisoned")
                .insert(temporary_channel_id, pending_tx);
            self.pending_rgb_funding
                .lock()
                .expect("pending rgb funding lock poisoned")
                .insert(temporary_channel_id, pending_rgb);
            bail!(
                "RGB funding ref peer mismatch for channel {temporary_channel_id}: transfer={} funding={}",
                transfer_peer,
                funding_peer
            );
        }

        let completion = (|| -> Result<()> {
            if pending_rgb.funding_outpoint != pending_tx.funding_outpoint {
                bail!(
                    "RGB funding ref outpoint mismatch for channel {temporary_channel_id}: ref={} funding={}",
                    pending_rgb.funding_outpoint,
                    pending_tx.funding_outpoint
                );
            }
            channel_manager
                .provide_funding_rgb_transfer_for_unfunded_channel(
                    temporary_channel_id,
                    pending_tx.peer_node_id,
                    ldk_rgb_funding_transfer(pending_rgb.funding_ref.clone()),
                )
                .map_err(|err| anyhow!("LDK rejected RGB funding ref: {err:?}"))?;
            self.save_rgb_funding_binding(
                temporary_channel_id,
                pending_tx.peer_node_id,
                pending_tx.funding_outpoint,
                pending_rgb.funding_ref.clone(),
            )?;
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
        self.log_event(format!(
                "ln-rgb RGB funding transaction generated: user_channel_id={} temporary_channel_id={} funding_outpoint={} transfer_id={}",
                pending_tx.user_channel_id,
                temporary_channel_id,
                pending_tx.funding_outpoint,
                pending_rgb.funding_ref.transfer_id
            ));
        Ok(true)
    }

    fn save_rgb_funding_binding(
        &self,
        temporary_channel_id: LnRgbChannelId,
        peer_node_id: PublicKey,
        funding_outpoint: OutPoint,
        funding_ref: RgbFundingRef,
    ) -> Result<RgbFundingOutpointBinding> {
        let binding_dir = self.rgb_funding_binding_dir();
        fs::create_dir_all(&binding_dir)
            .with_context(|| format!("create RGB funding binding dir {}", binding_dir.display()))?;
        let base = format!("{}-{}", hex32(temporary_channel_id.0), funding_outpoint);

        let binding = RgbFundingOutpointBinding {
            temporary_channel_id: hex32(temporary_channel_id.0),
            channel_id: None,
            peer_node_id: peer_node_id.to_string(),
            funding_outpoint: funding_outpoint.to_string(),
            funding_ref,
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
        self.log_event(format!(
            "ln-rgb RGB funding ref bound: channel={} funding_outpoint={}",
            temporary_channel_id, funding_outpoint
        ));
        Ok(binding)
    }

    fn handle_spendable_outputs(
        &self,
        channel_id: Option<LnRgbChannelId>,
        outputs: Vec<SpendableOutputDescriptor>,
    ) -> Result<()> {
        let Some(channel_id) = channel_id else {
            self.log_event(format!(
                "ln-rgb spendable outputs without channel id: count={}",
                outputs.len()
            ));
            return Ok(());
        };
        let Some(binding) = self.rgb_funding_binding_for_channel(channel_id) else {
            self.log_event(format!(
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
        self.log_event(format!(
                "ln-rgb RGB spendable outputs recorded for sweep: channel_id={channel_id} count={} funding={}",
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
            funding_ref: binding.funding_ref.clone(),
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

    fn claim_inbound_rgb_payment(
        &self,
        payment_hash: PaymentHash,
        amount_msat: u64,
        contract_id: &str,
        rgb_amount: u64,
    ) -> Result<()> {
        let client = RgbServiceClient::new(
            self.config.rgb_service_url.clone(),
            Arc::new(BackendRgbServiceSigner {
                node_id: self.node_id,
                node_secret: self.node_secret,
            }),
        )
        .map_err(|err| anyhow!("{err}"))?;
        client
            .claim_ln_payment(LnPaymentClaimRequest {
                account_id: self.config.account_id.clone(),
                channel_id: None,
                payment_hash: hex32(payment_hash.0),
                contract_id: contract_id.to_string(),
                amount_msat,
                rgb_amount,
            })
            .map(|_| ())
            .map_err(|err| anyhow!("{err}"))
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

    fn try_confirm_rgb_funding_ref(
        &self,
        funding_txo: OutPoint,
    ) -> Result<Option<RgbFundingOutpointBinding>> {
        let binding = {
            let bindings = self
                .rgb_funding_bindings
                .lock()
                .expect("rgb funding binding lock poisoned");
            bindings
                .values()
                .find(|binding| binding.funding_outpoint == funding_txo.to_string())
                .cloned()
        };
        let Some(mut binding) = binding else {
            return Ok(None);
        };
        if binding.status == "confirmed" {
            return Ok(Some(binding));
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

    fn try_confirm_rgb_funding_refs(&self) -> Result<()> {
        let funding_outpoints = self
            .rgb_funding_bindings
            .lock()
            .expect("rgb funding binding lock poisoned")
            .values()
            .filter(|binding| binding.status != "confirmed")
            .filter_map(|binding| binding.funding_outpoint.parse::<OutPoint>().ok())
            .collect::<Vec<_>>();
        for funding_outpoint in funding_outpoints {
            if let Err(err) = self.try_confirm_rgb_funding_ref(funding_outpoint) {
                self.log_event(format!(
                        "ln-rgb RGB funding stock promotion skipped: funding={funding_outpoint} error={err:#}"
                    ));
            }
        }
        Ok(())
    }

    fn try_attach_inbound_rgb_funding_refs(&self) -> Result<()> {
        let pending = self
            .pending_inbound_rgb_channels
            .lock()
            .expect("pending inbound rgb channel lock poisoned")
            .iter()
            .map(|(channel_id, peer)| (*channel_id, *peer))
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }
        let client = RgbServiceClient::new(
            self.config.rgb_service_url.clone(),
            Arc::new(BackendRgbServiceSigner {
                node_id: self.node_id,
                node_secret: self.node_secret,
            }),
        )
        .map_err(|err| anyhow!("{err}"))?;
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
        let Some((channel_manager, peer_manager, kv_store)) = managers else {
            return Ok(());
        };
        for (temporary_channel_id, peer_node_id) in pending {
            let channel = hex32(temporary_channel_id.0);
            let response = match client.ln_channel_funding_ref(LnChannelFundingRefRequest {
                account_id: self.config.account_id.clone(),
                channel_id: channel.clone(),
            }) {
                Ok(response) => response,
                Err(err) => {
                    self.log_event(format!(
                            "ln-rgb inbound RGB funding ref lookup deferred: channel={channel} error={err}"
                        ));
                    continue;
                }
            };
            let Some(funding_ref) = response.funding_ref else {
                continue;
            };
            channel_manager
                .provide_funding_rgb_transfer_for_unfunded_channel(
                    temporary_channel_id,
                    peer_node_id,
                    ldk_rgb_funding_transfer(funding_ref.clone()),
                )
                .map_err(|err| anyhow!("LDK rejected inbound RGB funding ref: {err:?}"))?;
            peer_manager.process_events();
            Self::persist_channel_manager_to_store(&kv_store, &channel_manager)?;
            self.pending_inbound_rgb_channels
                .lock()
                .expect("pending inbound rgb channel lock poisoned")
                .remove(&temporary_channel_id);
            self.log_event(format!(
                "ln-rgb inbound RGB funding ref attached: channel={channel} transfer_id={}",
                funding_ref.transfer_id
            ));
        }
        Ok(())
    }

    fn reconcile_rgb_sweep_records_from_sweeper(
        &self,
        output_sweeper: &LnRgbOutputSweeper,
    ) -> Result<()> {
        let tracked_outputs = output_sweeper.tracked_spendable_outputs();
        let mut records = self.rgb_pending_sweep_records()?;
        let mut updated = 0usize;
        for record in records.iter_mut() {
            let Ok(spendable_outpoint) = record.spendable_outpoint.parse::<OutPoint>() else {
                continue;
            };
            let Some(tracked) = tracked_outputs.iter().find(|tracked| {
                spendable_output_outpoint(&tracked.descriptor) == spendable_outpoint
            }) else {
                continue;
            };
            let (carrier_txid, status) = match &tracked.status {
                OutputSpendStatus::PendingInitialBroadcast { .. } => {
                    (String::new(), "pending_rgb_sweep")
                }
                OutputSpendStatus::PendingFirstConfirmation {
                    latest_spending_tx, ..
                } => (
                    latest_spending_tx.compute_txid().to_string(),
                    "pending_confirmation",
                ),
                OutputSpendStatus::PendingThresholdConfirmations {
                    latest_spending_tx, ..
                } => (latest_spending_tx.compute_txid().to_string(), "confirmed"),
            };
            if record.carrier_txid == carrier_txid && record.status == status {
                continue;
            }
            record.carrier_txid = carrier_txid;
            record.status = status.to_string();
            record.updated_at = now_secs();
            let descriptor_path = PathBuf::from(&record.descriptor_path);
            let Some(base) = descriptor_path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let record_path = self.rgb_pending_sweep_dir().join(format!("{base}.json"));
            fs::write(&record_path, serde_json::to_vec_pretty(record)?).with_context(|| {
                format!("write RGB pending sweep record {}", record_path.display())
            })?;
            updated += 1;
        }
        if updated > 0 {
            self.log_event(format!(
                "ln-rgb RGB sweep records updated: records={updated}"
            ));
        }
        Ok(())
    }

    fn try_confirm_rgb_sweep_carriers(&self) -> Result<()> {
        let mut updated = 0usize;
        for mut record in self.rgb_pending_sweep_records()? {
            if record.carrier_txid.is_empty() || record.status == "rgb_sweep_confirmed" {
                continue;
            }
            let txid = record.carrier_txid.parse::<Txid>().with_context(|| {
                format!("invalid RGB sweep carrier txid {}", record.carrier_txid)
            })?;
            let mut confirmed = false;
            for source in self.rotated_chain_sources()? {
                match source {
                    ChainSource::Esplora(config) => {
                        let client = esplora_client_with_config(&config);
                        let status = client
                            .get_tx_status(&txid)
                            .with_context(|| format!("fetch RGB sweep tx status {txid}"))?;
                        if status.confirmed {
                            confirmed = true;
                            break;
                        }
                    }
                    ChainSource::Electrum(config) => {
                        if electrum_transaction_status(&config, txid)?
                            .map(|status| status.confirmed)
                            .unwrap_or(false)
                        {
                            confirmed = true;
                            break;
                        }
                    }
                    ChainSource::BitcoinCore(config) => {
                        if bitcoin_core_tx_status(&config, txid)?
                            .map(|status| status.confirmed)
                            .unwrap_or(false)
                        {
                            confirmed = true;
                            break;
                        }
                    }
                }
            }
            if !confirmed {
                continue;
            }
            record.status = "rgb_sweep_confirmed".to_string();
            record.updated_at = now_secs();
            let descriptor_path = PathBuf::from(&record.descriptor_path);
            let Some(base) = descriptor_path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let record_path = self.rgb_pending_sweep_dir().join(format!("{base}.json"));
            fs::write(&record_path, serde_json::to_vec_pretty(&record)?).with_context(|| {
                format!("write confirmed RGB sweep record {}", record_path.display())
            })?;
            if let Some(channel_id) = record
                .channel_id
                .as_deref()
                .and_then(|channel_id| hex_to_32(channel_id).ok())
                .map(LnRgbChannelId)
            {
                self.mark_rgb_funding_binding_status_by_channel(channel_id, "rgb_sweep_confirmed")?;
                self.mark_rgb_maturity_record_status(channel_id, "confirmed")?;
            }
            updated += 1;
        }
        if updated > 0 {
            self.log_event(format!(
                "ln-rgb RGB sweep carrier transactions confirmed: records={updated}"
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
            self.log_event(format!(
                "ln-rgb RGB maturity records updated: records={updated}"
            ));
        }
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

    fn peer_record_dir(&self) -> PathBuf {
        self.config.storage_dir.join("ln-rgb").join("peers")
    }

    fn peer_record_path(&self, node_id: &PublicKey) -> PathBuf {
        self.peer_record_dir().join(format!("{node_id}.json"))
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
                    ldk_rgb_funding_transfer(pending.funding_ref.clone()),
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
                    self.log_event(format!(
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
            match runtime
                .channel_manager
                .provide_funding_rgb_transfer_for_channel(
                    channel_id,
                    peer_node_id,
                    ldk_rgb_funding_transfer(binding.funding_ref.clone()),
                ) {
                Ok(()) => {
                    runtime.peer_manager.process_events();
                    replayed += 1;
                }
                Err(err) => {
                    self.log_event(format!(
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
            ChainSource::Electrum(config) => LnRgbChainSync::Electrum(ElectrumTxSync::new(config)),
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
        user_config.channel_handshake_config.announce_for_forwarding =
            self.config.announce_for_forwarding;
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
                            ln_rgb_log_line("[ln-rgb] output sweeper failed to broadcast spend");
                        }
                        sync_peer_manager.process_events();
                        next_delay = LDK_CHAIN_SYNC_SUCCESS_INTERVAL;
                    }
                    Err(err) => {
                        ln_rgb_log_line(&format!("[ln-rgb] chain sync failed: {err:#}"));
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
                                Err(err) => ln_rgb_log_line(&format!(
                                    "[ln-rgb] inbound stream error: {err}"
                                )),
                            },
                            Err(err) => {
                                ln_rgb_log_line(&format!("[ln-rgb] accept error: {err}"));
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
            listener_stop,
            listener_handle,
            sync_handle,
            event_pump_handle: None,
            peer_maintenance_handle,
            peer_task_handles,
            _keys_manager: keys_manager,
            _logger: logger,
        })
    }
}

impl LnRgbBtcLnBackend {
    pub fn cached_balance_snapshot(&self) -> BtcLnBalanceSnapshot {
        self.balance_snapshot_internal(false)
    }

    fn balance_snapshot_internal(&self, sync_l1_wallet: bool) -> BtcLnBalanceSnapshot {
        let mut total_onchain_balance_sats = 0;
        let mut spendable_onchain_balance_sats = 0;
        match self.open_l1_wallet() {
            Ok(mut local) => {
                if sync_l1_wallet {
                    let esplora = self.next_esplora_url();
                    if let Err(err) =
                        self.sync_l1_wallet_or_use_cached(&mut local, &esplora, "read LN balance")
                    {
                        self.log_event(format!("ln-rgb L1 balance sync failed: {err:#}"));
                    }
                }
                let balance = local.wallet.balance();
                total_onchain_balance_sats = balance.total().to_sat();
                spendable_onchain_balance_sats = balance.trusted_spendable().to_sat();
            }
            Err(err) => {
                self.log_event(format!("ln-rgb L1 wallet balance unavailable: {err:#}"));
            }
        }

        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let (total_lightning_balance_sats, lightning_balances, pending_channel_closure_sweeps) =
            if let Some(runtime) = runtime_guard.as_ref() {
                let channels = runtime.channel_manager.list_channels();
                let total_outbound_msat = channels
                    .iter()
                    .map(|channel| channel.outbound_capacity_msat)
                    .sum::<u64>();
                let lightning_balances = json!(channels
                    .iter()
                    .map(|channel| json!({
                        "channel_id": channel.channel_id.to_string(),
                        "counterparty_node_id": channel.counterparty.node_id.to_string(),
                        "is_channel_ready": channel.is_channel_ready,
                        "is_usable": channel.is_usable,
                        "channel_value_sats": channel.channel_value_satoshis,
                        "outbound_capacity_msat": channel.outbound_capacity_msat,
                        "outbound_balance_sats": channel.outbound_capacity_msat / 1000,
                        "inbound_capacity_msat": channel.inbound_capacity_msat,
                        "funding_txo": channel.funding_txo.map(|outpoint| outpoint.to_string()),
                    }))
                    .collect::<Vec<_>>())
                .to_string();
                let pending_channel_closure_sweeps = json!(
                    runtime
                        .output_sweeper
                        .tracked_spendable_outputs()
                        .iter()
                        .map(|output| {
                            let txid = match &output.status {
                                OutputSpendStatus::PendingFirstConfirmation { latest_spending_tx, .. }
                                | OutputSpendStatus::PendingThresholdConfirmations { latest_spending_tx, .. } => {
                                    Some(latest_spending_tx.compute_txid().to_string())
                                }
                                OutputSpendStatus::PendingInitialBroadcast { .. } => None,
                            };
                            json!({
                                "spendable_outpoint": output.descriptor.spendable_outpoint().to_string(),
                                "channel_id": output.channel_id.map(|channel_id| channel_id.to_string()),
                                "status": format!("{:?}", output.status),
                                "latest_spending_txid": txid,
                            })
                        })
                        .collect::<Vec<_>>()
                )
                .to_string();
                (
                    total_outbound_msat / 1000,
                    lightning_balances,
                    pending_channel_closure_sweeps,
                )
            } else {
                (0, "[]".to_string(), "[]".to_string())
            };

        BtcLnBalanceSnapshot {
            total_onchain_balance_sats,
            spendable_onchain_balance_sats,
            total_anchor_channels_reserve_sats: 0,
            total_lightning_balance_sats,
            lightning_balances,
            pending_channel_closure_sweeps,
        }
    }

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
                    self.log_event(format!(
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
        self.log_event(format!(
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
        funding_outpoint: OutPoint,
        funding_ref: RgbFundingRef,
    ) -> Result<()> {
        let pending = PendingRgbFundingTransfer {
            peer_node_id,
            funding_outpoint,
            funding_ref: funding_ref.clone(),
        };
        self.persist_pending_rgb_funding_transfer(temporary_channel_id, &pending)?;
        self.pending_rgb_funding
            .lock()
            .expect("pending rgb funding lock poisoned")
            .insert(temporary_channel_id, pending);
        self.log_event(format!(
            "ln-rgb RGB funding ref queued: channel={temporary_channel_id} peer={peer_node_id}"
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
                        ldk_rgb_funding_transfer(funding_ref),
                    )
                    .map_err(|err| anyhow!("LDK rejected RGB funding ref: {err:?}"))?;
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
            funding.funding_outpoint,
            funding.funding_ref,
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
        let persisted_peers = self
            .load_persisted_peers_from_disk()
            .context("load persisted LN peers")?;
        let signer: Arc<dyn RgbServiceSigner + Send + Sync> = Arc::new(BackendRgbServiceSigner {
            node_id: self.node_id,
            node_secret: self.node_secret,
        });
        let client = RgbServiceClient::new(self.config.rgb_service_url.clone(), signer)
            .map_err(|err| anyhow!("{err}"))?;
        let composer: Arc<dyn RgbLnTxComposer + Send + Sync> = Arc::new(
            RgbDaemonLnTxComposer::new(client, self.config.account_id.clone(), 300000)
                .map_err(|err| anyhow!("{err}"))?,
        );
        init_rgb_ln_tx_composer(composer);
        let runtime = self.build_runtime()?;
        let restored_payment_metadata = self
            .restore_rgb_payment_metadata(&runtime.channel_manager)
            .context("restore RGB payment metadata")?;
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
        }
        self.log_event("ln-rgb peer runtime started".to_string());
        for (peer_node_id, address) in persisted_peers.iter().cloned() {
            match self.connect(peer_node_id, address.clone(), true) {
                Ok(()) => {
                    self.log_event(format!(
                        "ln-rgb persisted peer reconnected: {peer_node_id}@{address}"
                    ));
                }
                Err(err) => {
                    self.log_event(format!(
                        "ln-rgb persisted peer reconnect failed: {peer_node_id}@{address}: {err:#}"
                    ));
                }
            }
        }
        if restored_payment_metadata > 0 {
            self.log_event(format!(
                "ln-rgb RGB payment metadata restored from disk: count={restored_payment_metadata}"
            ));
        }
        if loaded_bindings > 0 {
            self.log_event(format!(
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
            self.log_event(format!(
                    "ln-rgb durable RGB state loaded: pending_funding_txs={loaded_pending_funding_txs} pending_transfers={loaded_pending_rgb_transfers} generated_transfers={loaded_generated_transfers} payment_states={loaded_payment_states} replayed_funding={replayed_rgb_funding} replayed_bindings={replayed_rgb_bindings}"
                ));
        }
        if !persisted_peers.is_empty() {
            self.log_event(format!(
                "ln-rgb persisted peers loaded: count={}",
                persisted_peers.len()
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

    fn next_btc_ln_event(&self) -> Option<BtcLnEvent> {
        self.poll_ldk_events_fast();
        self.btc_events
            .lock()
            .expect("ln-rgb btc event lock poisoned")
            .pop_front()
    }

    fn balance_snapshot(&self) -> BtcLnBalanceSnapshot {
        self.balance_snapshot_internal(true)
    }

    fn peer_snapshots(&self) -> Vec<BtcLnPeerSnapshot> {
        let mut snapshots = self
            .peers
            .lock()
            .expect("ln-rgb peers lock poisoned")
            .values()
            .cloned()
            .map(|mut peer| {
                peer.is_connected = false;
                (peer.node_id, peer)
            })
            .collect::<HashMap<_, _>>();

        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        if let Some(runtime) = runtime_guard.as_ref() {
            runtime.peer_manager.process_events();
            for peer in runtime.peer_manager.list_peers() {
                let node_id = peer.counterparty_node_id;
                let is_persisted = snapshots
                    .get(&node_id)
                    .map(|snapshot| snapshot.is_persisted)
                    .unwrap_or(false);
                let address = peer.socket_address.clone().or_else(|| {
                    snapshots
                        .get(&node_id)
                        .map(|snapshot| snapshot.address.clone())
                });
                if let Some(address) = address {
                    snapshots.insert(
                        node_id,
                        BtcLnPeerSnapshot {
                            node_id,
                            address,
                            is_persisted,
                            is_connected: true,
                        },
                    );
                }
            }
        }

        snapshots.into_values().collect()
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
                            ln_rgb_log_line(&format!(
                                "[ln-rgb] peer connection closed: {node_id}@{socket_addr}"
                            ));
                        });
                        peer_task_handles
                            .lock()
                            .expect("ln-rgb peer task lock poisoned")
                            .push(handle);
                    }
                    None => {
                        ln_rgb_log_line(&format!(
                            "[ln-rgb] failed to connect peer {node_id}@{socket_addr}"
                        ));
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
        if persist {
            self.persist_connected_peer(node_id, &address)
                .context("persist connected LN peer")?;
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
        self.log_event(format!("ln-rgb peer connected: {node_id}@{socket_addr}"));
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
        self.log_event(format!(
            "ln-rgb channel close requested: channel_id={} peer={} force={}",
            request.channel_id, request.counterparty_node_id, request.force
        ));
        Ok(())
    }

    fn splice_channel(&self, request: BtcLnChannelSpliceRequest) -> Result<()> {
        ensure!(request.amount_sats != 0, "amount_sats must not be zero");
        if !self.started.load(Ordering::SeqCst) {
            self.start()?;
        }
        let channel_id = parse_channel_id(&request.channel_id)?;
        let contribution = if request.amount_sats > 0 {
            self.build_splice_in_contribution(
                request.amount_sats as u64,
                request.funding_feerate_per_kw,
            )?
        } else {
            let amount_sats = request
                .amount_sats
                .checked_abs()
                .and_then(|amount| u64::try_from(amount).ok())
                .context("amount_sats is too small")?;
            self.build_splice_out_contribution(amount_sats)?
        };
        let direction = if request.amount_sats > 0 { "in" } else { "out" };
        let runtime_guard = self.runtime.lock().expect("ln-rgb runtime lock poisoned");
        let runtime = runtime_guard
            .as_ref()
            .context("ln-rgb runtime did not start")?;
        runtime
            .channel_manager
            .splice_channel(
                &channel_id,
                &request.counterparty_node_id,
                contribution,
                request.funding_feerate_per_kw,
                request.locktime,
            )
            .map_err(|err| anyhow!("LDK splice-in failed: {err:?}"))?;
        runtime.peer_manager.process_events();
        Self::persist_channel_manager_to_store(&runtime.kv_store, &runtime.channel_manager)?;
        drop(runtime_guard);
        self.poll_ldk_events();
        self.log_event(format!(
                "ln-rgb BTC splice-{direction} requested: channel_id={} peer={} amount_sats={} funding_feerate_per_kw={} locktime={:?}",
                request.channel_id,
                request.counterparty_node_id,
                request.amount_sats,
                request.funding_feerate_per_kw,
                request.locktime
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
        let counter = self.invoice_counter.fetch_add(1, Ordering::SeqCst);
        let invoice = runtime
            .channel_manager
            .create_bolt11_invoice(Bolt11InvoiceParameters {
                amount_msats: Some(request.amount_msat),
                description: request.description,
                invoice_expiry_delta_secs: Some(request.expiry_secs),
                min_final_cltv_expiry_delta: Some(144),
                payment_hash: None,
            })
            .map_err(|err| anyhow!("build BOLT11 invoice: {err:?}"))?;
        self.log_event(format!(
            "ln-rgb BOLT11 invoice created: payment_hash={} counter={counter}",
            invoice.payment_hash()
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

fn ldk_rgb_asset(asset: &RgbAssetAmount) -> LdkRgbAssetAmount {
    let bytes = asset.contract_id.to_byte_array();
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
        ChainSource::Electrum(config) => {
            let tip = electrum_chain_tip(config)?;
            Ok(BestBlock::new(tip.hash, tip.height))
        }
        ChainSource::BitcoinCore(config) => {
            let tip = bitcoin_core_chain_tip(config)?;
            Ok(BestBlock::new(tip.hash, tip.height))
        }
    }
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
            ln_rgb_log_line(&format!(
                "[ln-rgb] failed to fetch Esplora block summary from {}: {err:?}; falling back to tip hash",
                esplora.url
            ));
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

fn ldk_rgb_funding_transfer(funding_ref: RgbFundingRef) -> LdkRgbFundingTransfer {
    LdkRgbFundingTransfer::new(funding_ref)
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

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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
        PendingFundingTransaction, RgbFundingOutpointBinding, RgbFundingRef,
        RgbPendingMaturityRecord,
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
            rgb_service_url: "http://127.0.0.1:8091".to_string(),
            account_id: "test-account".to_string(),
            listen: None,
            entropy_mnemonic: Some(
                "flower paddle dune found session enroll entry bridge regular sick slam chapter"
                    .to_string(),
            ),
            trusted_peers_0conf: Vec::new(),
            accept_inbound_channels: true,
            announce_for_forwarding: false,
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
                    funding_ref: RgbFundingRef::new(
                        "test-transfer",
                        "test-operation",
                        Some(hex32(channel_id.0)),
                    ),
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
        assert_eq!(record.funding_ref.transfer_id, "test-transfer");
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
            funding_ref: RgbFundingRef::new(
                "test-transfer",
                "test-operation",
                Some(hex32(channel_id.0)),
            ),
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
    fn pending_rgb_funding_ref_survives_restart() {
        let storage_dir = unique_tmp_dir("btc-local-wallet-rgb-funding-transfer-reload");
        let mut config = test_config(None);
        config.storage_dir = storage_dir.clone();
        config.l1_data_dir = storage_dir.join("l1");
        let backend = LnRgbBtcLnBackend::new(config);
        let pending_dir = storage_dir
            .join("ln-rgb")
            .join("pending-rgb-funding-transfers");
        fs::create_dir_all(&pending_dir).expect("create pending transfer dir");
        let channel_id_bytes = [92; 32];
        let channel_id = hex32(channel_id_bytes);
        let funding_outpoint = OutPoint::new(Txid::from_byte_array([93; 32]), 0);
        let record = serde_json::json!({
            "temporary_channel_id": channel_id,
            "peer_node_id": backend.node_id().to_string(),
            "funding_outpoint": funding_outpoint.to_string(),
            "funding_ref": RgbFundingRef::new("test-transfer", "test-operation", Some(channel_id.clone())),
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
            1
        );
        let pending = backend
            .pending_rgb_funding
            .lock()
            .expect("pending rgb funding lock poisoned")
            .get(&LnRgbChannelId(channel_id_bytes))
            .cloned()
            .expect("pending rgb funding ref");
        assert_eq!(pending.funding_outpoint, funding_outpoint);
        assert_eq!(pending.funding_ref.transfer_id, "test-transfer");
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
            rgb_service_url: "http://127.0.0.1:8091".to_string(),
            account_id: "test-account".to_string(),
            listen,
            entropy_mnemonic: Some(
                "flower paddle dune found session enroll entry bridge regular sick slam chapter"
                    .to_string(),
            ),
            trusted_peers_0conf: Vec::new(),
            accept_inbound_channels: true,
            announce_for_forwarding: false,
        }
    }

    fn unique_tmp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}
