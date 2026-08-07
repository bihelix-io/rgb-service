use std::{
    collections::HashMap,
    env, fs,
    io::{BufRead, BufReader, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use bdk_bitcoind_rpc::{
    bitcoincore_rpc::{
        bitcoincore_rpc_json::ScanTxOutRequest,
        jsonrpc::{client::Client as CoreJsonRpcClient, simple_http::Builder as CoreHttpBuilder},
        Auth, Client as BitcoinCoreClient, RpcApi,
    },
    Emitter,
};
use bdk_esplora::EsploraExt;
use bdk_wallet::bitcoin::{
    bip32::{ChildNumber, DerivationPath},
    block::Header,
    consensus::encode,
    hashes::{sha256, Hash},
    Address, Amount, BlockHash, Network, OutPoint, ScriptBuf, Transaction, TxOut, Txid,
};
use bdk_wallet::chain::{BlockId, CheckPoint, ConfirmationBlockTime, TxUpdate};
use bdk_wallet::descriptor::IntoWalletDescriptor;
use bdk_wallet::file_store;
use bdk_wallet::keys::bip39::Language;
use bdk_wallet::keys::{DerivableKey, DescriptorKey};
use bdk_wallet::miniscript;
use bdk_wallet::Update;
use bdk_wallet::{ChangeSet, KeychainKind, PersistedWallet, Wallet};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const MAGIC: &[u8] = b"LocalBtcWallet";
pub const CONFIG_FILE: &str = "wallet.json";
pub const BDK_FILE: &str = "bdk_wallet";
const BDK_SYNC_CACHE_FILE: &str = "bdk_sync_cache.json";
const BITCOIN_CORE_DESCRIPTOR_SCAN_RANGE: u32 = 1000;
const BITCOIN_CORE_MAX_BLOCK_SCAN: u32 = 10_000;
const BITCOIN_CORE_RPC_TIMEOUT_SECS: u64 = 300;
const RGB_MAINNET_COIN_TYPE: u32 = 827_166;
const RGB_TESTNET_COIN_TYPE: u32 = 827_167;
static BDK_STORE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, &'static RwLock<()>>>> = OnceLock::new();

fn bdk_store_lock_for_path(store_path: &Path) -> &'static RwLock<()> {
    let key = store_path
        .parent()
        .and_then(|parent| fs::canonicalize(parent).ok())
        .map(|parent| parent.join(BDK_FILE))
        .unwrap_or_else(|| store_path.to_path_buf());
    let mut locks = BDK_STORE_LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("BDK store lock map poisoned");
    if let Some(lock) = locks.get(&key) {
        return lock;
    }
    let lock = Box::leak(Box::new(RwLock::new(())));
    locks.insert(key, lock);
    lock
}

#[derive(Serialize, Deserialize)]
pub struct WalletConfig {
    pub network: String,
    pub mnemonic: String,
}

impl WalletConfig {
    pub fn read(data_dir: &Path) -> Result<Self> {
        let config_path = data_dir.join(CONFIG_FILE);
        serde_json::from_slice(
            &fs::read(&config_path)
                .with_context(|| format!("wallet is not initialized: {}", config_path.display()))?,
        )
        .context("failed to parse wallet config")
    }
}

#[derive(Clone, Debug)]
pub struct LocalAddressUtxo {
    pub outpoint: OutPoint,
    pub value: Amount,
    pub confirmed: bool,
    pub keychain: KeychainKind,
    pub derivation_index: u32,
}

pub struct LocalWallet {
    pub wallet: PersistedWallet<file_store::Store<bdk_wallet::ChangeSet>>,
    pub data_dir: PathBuf,
    db: file_store::Store<bdk_wallet::ChangeSet>,
    _store_guard: LocalWalletStoreGuard,
}

#[allow(dead_code)]
enum LocalWalletStoreGuard {
    Read(RwLockReadGuard<'static, ()>),
    Write(RwLockWriteGuard<'static, ()>),
}

#[derive(Clone, Copy)]
enum LocalWalletOpenMode {
    ReadOnly,
    TryReadOnly,
    Write,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChainSource {
    Esplora(EsploraConfig),
    Electrum(ElectrumConfig),
    BitcoinCore(BitcoinCoreConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EsploraConfig {
    pub url: String,
    pub api_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElectrumConfig {
    pub url: String,
}

#[derive(Clone, Debug)]
pub struct ElectrumTxStatus {
    pub tx: Transaction,
    pub confirmed: bool,
    pub block_hash: Option<BlockHash>,
    pub height: Option<u32>,
    pub block_time: Option<u32>,
    pub position: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitcoinCoreConfig {
    pub rpc_url: String,
    pub cookie_file: Option<PathBuf>,
    pub rpc_user: Option<String>,
    pub rpc_password: Option<String>,
    pub descriptor_scan_range: u32,
    pub max_block_scan: u32,
    pub rpc_timeout_secs: u64,
}

#[derive(Clone, Debug)]
pub struct BitcoinCoreTxStatus {
    pub tx: Transaction,
    pub confirmed: bool,
    pub block_hash: Option<BlockHash>,
    pub height: Option<u32>,
    pub block_time: Option<u32>,
}

impl ChainSource {
    pub fn from_esplora_or_default(network: Network, override_url: Option<&str>) -> Result<Self> {
        if let Some(url) = override_url {
            if is_electrum_chain_source(url) {
                return Ok(Self::Electrum(ElectrumConfig::new(url.to_string())));
            }
            return Ok(Self::Esplora(EsploraConfig::new(url.to_string())));
        }
        Ok(Self::Esplora(EsploraConfig::new(
            default_esplora(network)?.to_string(),
        )))
    }

    pub fn cache_key(&self) -> String {
        match self {
            Self::Esplora(config) => format!("esplora:{}", config.url),
            Self::Electrum(config) => format!("electrum:{}", config.url),
            Self::BitcoinCore(config) => format!("bitcoin-core:{}", config.rpc_url),
        }
    }
}

impl EsploraConfig {
    pub fn new(url: String) -> Self {
        Self { url, api_key: None }
    }

    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        self.api_key = api_key.filter(|value| !value.trim().is_empty());
        self
    }

    pub fn builder(&self) -> bdk_esplora::esplora_client::Builder {
        let mut builder = bdk_esplora::esplora_client::Builder::new(&self.url).timeout(10);
        if let Some(api_key) = self.api_key.as_deref().filter(|value| !value.is_empty()) {
            builder = builder.header("api-key", api_key);
        }
        builder
    }
}

impl ElectrumConfig {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl BitcoinCoreConfig {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            cookie_file: None,
            rpc_user: None,
            rpc_password: None,
            descriptor_scan_range: BITCOIN_CORE_DESCRIPTOR_SCAN_RANGE,
            max_block_scan: BITCOIN_CORE_MAX_BLOCK_SCAN,
            rpc_timeout_secs: BITCOIN_CORE_RPC_TIMEOUT_SECS,
        }
    }

    pub fn with_datadir(network: Network, datadir: impl Into<PathBuf>) -> Self {
        let mut config = Self::new(default_bitcoin_core_rpc_url(network).to_string());
        config.cookie_file = Some(bitcoin_core_cookie_file(&datadir.into(), network));
        config
    }

    pub(crate) fn client(&self) -> Result<Arc<BitcoinCoreClient>> {
        let auth = match (&self.rpc_user, &self.rpc_password, &self.cookie_file) {
            (Some(user), Some(password), _) => Auth::UserPass(user.clone(), password.clone()),
            (_, _, Some(cookie_file)) => Auth::CookieFile(cookie_file.clone()),
            _ => Auth::None,
        };
        let (user, pass) = auth
            .get_user_pass()
            .context("failed to load Bitcoin Core RPC credentials")?;
        let mut builder = CoreHttpBuilder::new()
            .timeout(Duration::from_secs(self.rpc_timeout_secs))
            .url(&self.rpc_url)
            .with_context(|| {
                format!("failed to configure Bitcoin Core RPC URL {}", self.rpc_url)
            })?;
        if let Some(user) = user {
            builder = builder.auth(user, pass);
        }
        let client =
            BitcoinCoreClient::from_jsonrpc(CoreJsonRpcClient::with_transport(builder.build()));
        Ok(Arc::new(client))
    }
}

pub fn default_bitcoin_core_config(network: Network) -> Option<BitcoinCoreConfig> {
    detect_default_bitcoin_core(network)
}

impl LocalWallet {
    pub fn open(data_dir: &Path, network: Network) -> Result<Self> {
        let cfg = WalletConfig::read(data_dir)?;

        if cfg.network != network.to_string() {
            bail!(
                "wallet was initialized for {}, but command requested {}",
                cfg.network,
                network
            );
        }

        let mnemonic =
            bdk_wallet::bip39::Mnemonic::parse_in_normalized(Language::English, &cfg.mnemonic)
                .context("invalid mnemonic in wallet config")?;
        Self::open_with_mnemonic(data_dir, network, &mnemonic)
    }

    pub fn open_with_mnemonic(
        data_dir: &Path,
        network: Network,
        mnemonic: &bdk_wallet::bip39::Mnemonic,
    ) -> Result<Self> {
        Self::open_with_mnemonic_inner(data_dir, network, mnemonic, LocalWalletOpenMode::Write)?
            .ok_or_else(|| anyhow!("BDK store open lock is busy"))
    }

    pub fn try_open_with_mnemonic(
        data_dir: &Path,
        network: Network,
        mnemonic: &bdk_wallet::bip39::Mnemonic,
    ) -> Result<Option<Self>> {
        Self::open_with_mnemonic_inner(
            data_dir,
            network,
            mnemonic,
            LocalWalletOpenMode::TryReadOnly,
        )
    }

    pub fn open_read_only_with_mnemonic(
        data_dir: &Path,
        network: Network,
        mnemonic: &bdk_wallet::bip39::Mnemonic,
    ) -> Result<Self> {
        Self::open_with_mnemonic_inner(data_dir, network, mnemonic, LocalWalletOpenMode::ReadOnly)?
            .ok_or_else(|| anyhow!("BDK store open lock is busy"))
    }

    fn open_with_mnemonic_inner(
        data_dir: &Path,
        network: Network,
        mnemonic: &bdk_wallet::bip39::Mnemonic,
        mode: LocalWalletOpenMode,
    ) -> Result<Option<Self>> {
        fs::create_dir_all(data_dir)
            .with_context(|| format!("failed to create {}", data_dir.display()))?;

        let external = descriptor_from_mnemonic(mnemonic, network, 0)?;
        let internal = descriptor_from_mnemonic(mnemonic, network, 1)?;

        let store_path = data_dir.join(BDK_FILE);
        let store_lock = bdk_store_lock_for_path(&store_path);
        let store_guard = match mode {
            LocalWalletOpenMode::TryReadOnly => match store_lock.try_read() {
                Ok(guard) => LocalWalletStoreGuard::Read(guard),
                Err(TryLockError::WouldBlock) => return Ok(None),
                Err(TryLockError::Poisoned(err)) => {
                    return Err(anyhow!("BDK store read lock poisoned: {err}"));
                }
            },
            LocalWalletOpenMode::ReadOnly => LocalWalletStoreGuard::Read(
                store_lock
                    .read()
                    .map_err(|err| anyhow!("BDK store read lock poisoned: {err}"))?,
            ),
            LocalWalletOpenMode::Write => LocalWalletStoreGuard::Write(
                store_lock
                    .write()
                    .map_err(|err| anyhow!("BDK store write lock poisoned: {err}"))?,
            ),
        };
        let (mut db, _) = load_bdk_store(&store_path)?;
        let wallet = match Wallet::load()
            .descriptor(KeychainKind::External, Some(external.clone()))
            .descriptor(KeychainKind::Internal, Some(internal.clone()))
            .extract_keys()
            .check_network(network)
            .load_wallet(&mut db)
            .with_context(|| {
                format!(
                    "failed to load BDK wallet from {}; wallet store is unreadable or incompatible and must be explicitly cleaned/rebuilt",
                    data_dir.join(BDK_FILE).display()
                )
            })?
        {
            Some(wallet) => wallet,
            None if matches!(mode, LocalWalletOpenMode::Write) => Wallet::create(external, internal)
                .network(network)
                .create_wallet(&mut db)
                .context("failed to create BDK wallet")?,
            None => bail!(
                "BDK wallet is not initialized at {}; read-only open cannot create it",
                store_path.display()
            ),
        };

        Ok(Some(Self {
            wallet,
            data_dir: data_dir.to_path_buf(),
            db,
            _store_guard: store_guard,
        }))
    }

    pub fn persist(&mut self) -> Result<()> {
        if self.wallet.staged().is_some() {
            self.wallet
                .persist(&mut self.db)
                .context("failed to persist wallet")?;
        }
        Ok(())
    }

    pub fn list_address_utxos(
        &self,
        network: Network,
        address: &str,
    ) -> Result<Vec<LocalAddressUtxo>> {
        list_local_address_utxos(&self.wallet, network, address)
    }

    pub fn evict_unconfirmed_tx(&mut self, txid: Txid) -> Result<bool> {
        let was_confirmed = self
            .wallet
            .get_tx(txid)
            .is_some_and(|tx| tx.chain_position.is_confirmed());
        if was_confirmed {
            return Ok(false);
        }

        self.wallet.apply_evicted_txs([(txid, now_secs())]);
        self.persist()?;

        Ok(!self
            .wallet
            .get_tx(txid)
            .is_some_and(|tx| tx.chain_position.is_unconfirmed()))
    }
}

fn load_bdk_store(path: &Path) -> Result<(file_store::Store<ChangeSet>, Option<ChangeSet>)> {
    file_store::Store::load_or_create(MAGIC, path).with_context(|| {
        format!(
            "failed to open BDK store {}; wallet store must be explicitly cleaned/rebuilt",
            path.display()
        )
    })
}

pub fn init_wallet(data_dir: &Path, network: Network, mnemonic: Option<String>) -> Result<()> {
    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create {}", data_dir.display()))?;

    let config_path = data_dir.join(CONFIG_FILE);
    if config_path.exists() {
        bail!("wallet already exists at {}", config_path.display());
    }

    let mnemonic = match mnemonic {
        Some(words) => bdk_wallet::bip39::Mnemonic::parse_in_normalized(Language::English, &words)
            .context("invalid mnemonic")?,
        None => {
            let mut entropy = [0u8; 16];
            getrandom::fill(&mut entropy).context("generate wallet mnemonic entropy")?;
            bdk_wallet::bip39::Mnemonic::from_entropy_in(Language::English, &entropy)
                .map_err(|err| anyhow!("failed to generate mnemonic: {err:?}"))?
        }
    };

    let cfg = WalletConfig {
        network: network.to_string(),
        mnemonic: mnemonic.to_string(),
    };
    fs::write(&config_path, serde_json::to_string_pretty(&cfg)?)
        .with_context(|| format!("failed to write {}", config_path.display()))?;

    let mut local = LocalWallet::open(data_dir, network)?;
    let address = local.wallet.reveal_next_address(KeychainKind::External);
    local.persist()?;

    println!("wallet: {}", data_dir.display());
    println!("network: {network}");
    println!("mnemonic: {}", cfg.mnemonic);
    println!("first_address: {}", address.address);
    Ok(())
}

pub fn list_local_address_utxos(
    wallet: &PersistedWallet<file_store::Store<bdk_wallet::ChangeSet>>,
    network: Network,
    address: &str,
) -> Result<Vec<LocalAddressUtxo>> {
    let address = Address::from_str(address)
        .with_context(|| format!("invalid address: {address}"))?
        .require_network(network)
        .with_context(|| format!("address is not for {network:?}: {address}"))?;
    let script_pubkey = address.script_pubkey();

    Ok(wallet
        .list_unspent()
        .filter(|utxo| utxo.txout.script_pubkey == script_pubkey)
        .map(|utxo| LocalAddressUtxo {
            outpoint: utxo.outpoint,
            value: utxo.txout.value,
            confirmed: utxo.chain_position.is_confirmed(),
            keychain: utxo.keychain,
            derivation_index: utxo.derivation_index,
        })
        .collect())
}

pub fn sync_wallet(local: &mut LocalWallet, esplora: Option<&str>) -> Result<()> {
    let source = ChainSource::from_esplora_or_default(local.wallet.network(), esplora)?;
    sync_wallet_with_chain_source(local, &source)
}

pub fn sync_wallet_with_chain_source(local: &mut LocalWallet, source: &ChainSource) -> Result<()> {
    match source {
        ChainSource::Esplora(config) => sync_wallet_esplora(local, config),
        ChainSource::Electrum(config) => sync_wallet_electrum(local, config),
        ChainSource::BitcoinCore(config) => sync_wallet_bitcoin_core(local, config),
    }
}

fn sync_wallet_esplora(local: &mut LocalWallet, esplora: &EsploraConfig) -> Result<()> {
    let client = esplora_client_with_config(esplora);
    let request = local.wallet.start_sync_with_revealed_spks().build();
    let update = client.sync(request, 2).context("esplora sync failed")?;
    local
        .wallet
        .apply_update(update)
        .context("failed to apply sync update")?;
    Ok(())
}

fn sync_wallet_electrum(local: &mut LocalWallet, electrum: &ElectrumConfig) -> Result<()> {
    let mut request = local.wallet.start_sync_with_revealed_spks().build();
    let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
    let mut checkpoint_blocks = Vec::new();

    while let Some(item) = request.next_spk_with_expected_txids() {
        let history = match electrum_script_history(electrum, &item.spk) {
            Ok(history) => history,
            Err(err) if is_electrum_history_limit_error(&err) => {
                electrum_script_unspent_history(electrum, &item.spk).with_context(|| {
                    format!(
                        "Electrum listunspent fallback failed after history limit: {err:#}"
                    )
                })?
            }
            Err(err) => return Err(err),
        };
        let mut seen = std::collections::HashSet::new();
        for entry in history {
            seen.insert(entry.txid);
            electrum_add_tx_update(electrum, &mut tx_update, &mut checkpoint_blocks, entry)?;
        }
        for txid in item.expected_txids {
            if !seen.contains(&txid) {
                tx_update.evicted_ats.insert((txid, request.start_time()));
            }
        }
    }

    while let Some(txid) = request.next_txid() {
        if let Some(status) = electrum_transaction_status(electrum, txid)? {
            electrum_add_status_update(electrum, &mut tx_update, &mut checkpoint_blocks, status)?;
        }
    }

    while let Some(outpoint) = request.next_outpoint() {
        if let Some(status) = electrum_transaction_status(electrum, outpoint.txid)? {
            if let Some(txout) = status.tx.output.get(outpoint.vout as usize) {
                tx_update.txouts.insert(outpoint, txout.clone());
            }
            electrum_add_status_update(electrum, &mut tx_update, &mut checkpoint_blocks, status)?;
        }
    }

    checkpoint_blocks.push(electrum_chain_tip(electrum)?);
    let checkpoint = extend_latest_checkpoint(local.wallet.latest_checkpoint(), checkpoint_blocks)?;
    local
        .wallet
        .apply_update(Update {
            tx_update,
            chain: Some(checkpoint),
            ..Default::default()
        })
        .context("failed to apply Electrum wallet update")?;
    Ok(())
}

pub fn sync_wallet_cached(
    local: &mut LocalWallet,
    esplora: Option<&str>,
    min_interval: Duration,
) -> Result<bool> {
    let source = ChainSource::from_esplora_or_default(local.wallet.network(), esplora)?;
    sync_wallet_cached_with_chain_source(local, &source, min_interval)
}

pub fn sync_wallet_cached_with_chain_source(
    local: &mut LocalWallet,
    source: &ChainSource,
    min_interval: Duration,
) -> Result<bool> {
    let source_key = source.cache_key();
    let cache_path = local.data_dir.join(BDK_SYNC_CACHE_FILE);
    if let Ok(bytes) = fs::read(&cache_path) {
        if let Ok(cache) = serde_json::from_slice::<BdkSyncCache>(&bytes) {
            if cache.source_key() == source_key
                && now_secs().saturating_sub(cache.synced_at_secs) < min_interval.as_secs()
            {
                return Ok(false);
            }
        }
    }

    sync_wallet_with_chain_source(local, source)?;
    fs::write(
        &cache_path,
        serde_json::to_vec_pretty(&BdkSyncCache {
            chain_source: source_key,
            esplora: String::new(),
            synced_at_secs: now_secs(),
        })?,
    )
    .with_context(|| format!("write BDK sync cache {}", cache_path.display()))?;
    Ok(true)
}

pub fn full_scan_wallet(
    local: &mut LocalWallet,
    esplora: Option<&str>,
    stop_gap: usize,
) -> Result<()> {
    let client = esplora_client(local.wallet.network(), esplora)?;
    let request = local.wallet.start_full_scan().build();
    let update = client
        .full_scan(request, stop_gap, 2)
        .context("esplora full scan failed")?;
    local
        .wallet
        .apply_update(update)
        .context("failed to apply full scan update")?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct BdkSyncCache {
    #[serde(default)]
    chain_source: String,
    #[serde(default)]
    esplora: String,
    synced_at_secs: u64,
}

impl BdkSyncCache {
    fn source_key(&self) -> String {
        if !self.chain_source.is_empty() {
            self.chain_source.clone()
        } else {
            format!("esplora:{}", self.esplora)
        }
    }
}

pub fn broadcast_transaction(
    network: Network,
    source: Option<&ChainSource>,
    tx: &Transaction,
) -> Result<Txid> {
    let source = match source {
        Some(source) => source.clone(),
        None => ChainSource::from_esplora_or_default(network, None)?,
    };
    match source {
        ChainSource::Esplora(config) => {
            let client = esplora_client_with_config(&config);
            client
                .broadcast(tx)
                .context("failed to broadcast BTC transaction")?;
        }
        ChainSource::Electrum(config) => {
            let tx_hex = bytes_to_hex(&encode::serialize(tx));
            if let Err(electrum_err) =
                electrum_rpc(&config, "blockchain.transaction.broadcast", json!([tx_hex]))
                    .context("failed to broadcast BTC transaction through Electrum")
            {
                broadcast_transaction_esplora_fallback(network, tx, electrum_err)?;
            }
        }
        ChainSource::BitcoinCore(config) => {
            let client = config.client()?;
            client
                .send_raw_transaction(tx)
                .context("failed to broadcast BTC transaction through Bitcoin Core")?;
        }
    }
    Ok(tx.compute_txid())
}

fn broadcast_transaction_esplora_fallback(
    network: Network,
    tx: &Transaction,
    electrum_err: anyhow::Error,
) -> Result<()> {
    let tx_hex = bytes_to_hex(&encode::serialize(tx));
    let mut urls = Vec::new();
    if let Ok(url) = default_esplora(network) {
        urls.push(url.to_string());
    }
    if network == Network::Bitcoin {
        urls.push("https://mempool.space/api".to_string());
    }

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("build Esplora broadcast fallback HTTP client")?;
    let mut errors = Vec::new();
    for url in urls {
        let base = url.trim_end_matches('/');
        let endpoint = format!("{base}/tx");
        let response = match client
            .post(&endpoint)
            .header("content-type", "text/plain")
            .body(tx_hex.clone())
            .send()
        {
            Ok(response) => response,
            Err(err) => {
                errors.push(format!("{base}: POST failed: {err:#}"));
                continue;
            }
        };
        let status = response.status();
        let body = response
            .text()
            .unwrap_or_else(|err| format!("failed to read response body: {err:#}"));
        if status.is_success() {
            return Ok(());
        }
        errors.push(format!("{base}: HTTP {status}: {body}"));
    }

    bail!(
        "failed to broadcast BTC transaction through Electrum: {electrum_err:#}; Esplora fallback failed: {}",
        errors.join(" | ")
    )
}

#[derive(Clone, Copy, Debug)]
pub struct ElectrumHistoryEntry {
    pub txid: Txid,
    pub height: Option<u32>,
}

pub fn is_electrum_chain_source(source: &str) -> bool {
    let source = source.trim();
    source.starts_with("electrum://") || source.starts_with("tcp://")
}

fn electrum_endpoint(source: &str) -> Result<String> {
    let source = source.trim().trim_end_matches('/');
    let source = source
        .strip_prefix("electrum://")
        .or_else(|| source.strip_prefix("tcp://"))
        .unwrap_or(source);
    if source.starts_with("ssl://") || source.starts_with("tls://") {
        bail!("Electrum TLS endpoints are not supported here; use plaintext tcp/electrum");
    }
    let authority = source.split('/').next().unwrap_or_default();
    if !authority.contains(':') {
        bail!("Electrum source must include host:port");
    }
    Ok(authority.to_string())
}

pub fn electrum_rpc(config: &ElectrumConfig, method: &str, params: Value) -> Result<Value> {
    let endpoint = electrum_endpoint(&config.url)?;
    let address = endpoint
        .to_socket_addrs()
        .with_context(|| format!("resolve Electrum endpoint {endpoint}"))?
        .next()
        .with_context(|| format!("Electrum endpoint {endpoint} resolved no addresses"))?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
        .with_context(|| format!("connect Electrum endpoint {endpoint}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .context("set Electrum read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(15)))
        .context("set Electrum write timeout")?;
    let request = json!({
        "id": now_secs(),
        "method": method,
        "params": params.as_array().cloned().unwrap_or_default()
    });
    let request_line = serde_json::to_string(&request).context("encode Electrum request")? + "\n";
    stream
        .write_all(request_line.as_bytes())
        .with_context(|| format!("write Electrum request {method} to {endpoint}"))?;
    stream
        .flush()
        .with_context(|| format!("flush Electrum request {method} to {endpoint}"))?;
    let mut reader = BufReader::new(stream);
    let mut response_line = String::new();
    reader
        .read_line(&mut response_line)
        .with_context(|| format!("read Electrum response {method} from {endpoint}"))?;
    if response_line.trim().is_empty() {
        bail!("empty Electrum response for {method} from {endpoint}");
    }
    let response: Value = serde_json::from_str(&response_line)
        .with_context(|| format!("decode Electrum response for {method}: {response_line}"))?;
    if let Some(error) = response.get("error").filter(|error| !error.is_null()) {
        bail!("Electrum {method} failed: {error}");
    }
    response
        .get("result")
        .cloned()
        .context("Electrum response missing result")
}

pub fn electrum_script_hash_hex(script: &ScriptBuf) -> String {
    let digest = sha256::Hash::hash(script.as_bytes());
    let mut bytes = digest.to_byte_array();
    bytes.reverse();
    bytes_to_hex(&bytes)
}

pub fn electrum_script_history(
    config: &ElectrumConfig,
    script: &ScriptBuf,
) -> Result<Vec<ElectrumHistoryEntry>> {
    let script_hash = electrum_script_hash_hex(script);
    let history = electrum_rpc(
        config,
        "blockchain.scripthash.get_history",
        json!([script_hash]),
    )?;
    let mut entries = Vec::new();
    for item in history.as_array().cloned().unwrap_or_default() {
        let Some(txid_text) = item.get("tx_hash").and_then(Value::as_str) else {
            continue;
        };
        let txid = Txid::from_str(txid_text)
            .with_context(|| format!("decode Electrum tx_hash {txid_text}"))?;
        let height = item
            .get("height")
            .and_then(Value::as_i64)
            .filter(|height| *height > 0)
            .map(|height| height as u32);
        entries.push(ElectrumHistoryEntry { txid, height });
    }
    Ok(entries)
}

fn is_electrum_history_limit_error(error: &anyhow::Error) -> bool {
    format!("{error:#}")
        .to_ascii_lowercase()
        .contains("too many history entries")
}

fn electrum_script_unspent_history(
    config: &ElectrumConfig,
    script: &ScriptBuf,
) -> Result<Vec<ElectrumHistoryEntry>> {
    let script_hash = electrum_script_hash_hex(script);
    let unspents = electrum_rpc(
        config,
        "blockchain.scripthash.listunspent",
        json!([script_hash]),
    )?;
    let mut entries = Vec::new();
    for item in unspents.as_array().cloned().unwrap_or_default() {
        let Some(txid_text) = item.get("tx_hash").and_then(Value::as_str) else {
            continue;
        };
        let txid = Txid::from_str(txid_text)
            .with_context(|| format!("decode Electrum listunspent tx_hash {txid_text}"))?;
        let height = item
            .get("height")
            .and_then(Value::as_i64)
            .filter(|height| *height > 0)
            .map(|height| height as u32);
        entries.push(ElectrumHistoryEntry { txid, height });
    }
    Ok(entries)
}

pub fn electrum_get_transaction(config: &ElectrumConfig, txid: Txid) -> Result<Transaction> {
    let raw = electrum_rpc(
        config,
        "blockchain.transaction.get",
        json!([txid.to_string()]),
    )?;
    let raw_hex = raw
        .as_str()
        .with_context(|| format!("Electrum transaction.get returned non-string for {txid}"))?;
    let raw_tx = hex_to_bytes(raw_hex)?;
    encode::deserialize(&raw_tx).with_context(|| format!("decode Electrum raw transaction {txid}"))
}

pub fn electrum_transaction_status(
    config: &ElectrumConfig,
    txid: Txid,
) -> Result<Option<ElectrumTxStatus>> {
    let tx = electrum_get_transaction(config, txid)?;
    let verbose = electrum_rpc(
        config,
        "blockchain.transaction.get",
        json!([txid.to_string(), true]),
    )
    .ok();
    let tip = electrum_chain_tip(config).ok();
    let confirmations = verbose
        .as_ref()
        .and_then(|value| value.get("confirmations"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let block_hash = verbose
        .as_ref()
        .and_then(|value| value.get("blockhash"))
        .and_then(Value::as_str)
        .and_then(|value| BlockHash::from_str(value).ok());
    let height = match (tip, confirmations) {
        (Some(tip), confirmations) if confirmations > 0 => Some(
            tip.height
                .saturating_add(1)
                .saturating_sub(confirmations as u32),
        ),
        _ => None,
    };
    let block_time = verbose
        .as_ref()
        .and_then(|value| value.get("blocktime"))
        .and_then(Value::as_u64)
        .map(|value| value as u32);
    let position = match height {
        Some(height) => electrum_transaction_position(config, txid, height).ok(),
        None => None,
    };
    Ok(Some(ElectrumTxStatus {
        tx,
        confirmed: height.is_some(),
        block_hash,
        height,
        block_time,
        position,
    }))
}

pub fn electrum_chain_tip(config: &ElectrumConfig) -> Result<BlockId> {
    let tip = electrum_rpc(config, "blockchain.headers.subscribe", json!([]))?;
    let height = tip
        .get("height")
        .and_then(Value::as_u64)
        .context("Electrum header subscription missing height")? as u32;
    let header_hex = tip
        .get("hex")
        .and_then(Value::as_str)
        .context("Electrum header subscription missing hex")?;
    let header = decode_header_hex(header_hex)?;
    Ok(BlockId {
        height,
        hash: header.block_hash(),
    })
}

pub fn electrum_block_header(config: &ElectrumConfig, height: u32) -> Result<Header> {
    let header_hex = electrum_rpc(config, "blockchain.block.header", json!([height]))?;
    let header_hex = header_hex
        .as_str()
        .with_context(|| format!("Electrum block.header returned non-string at {height}"))?;
    decode_header_hex(header_hex)
}

pub fn electrum_transaction_position(
    config: &ElectrumConfig,
    txid: Txid,
    height: u32,
) -> Result<usize> {
    let merkle = electrum_rpc(
        config,
        "blockchain.transaction.get_merkle",
        json!([txid.to_string(), height]),
    )?;
    Ok(merkle
        .get("pos")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize)
}

fn electrum_add_tx_update(
    config: &ElectrumConfig,
    tx_update: &mut TxUpdate<ConfirmationBlockTime>,
    checkpoint_blocks: &mut Vec<BlockId>,
    entry: ElectrumHistoryEntry,
) -> Result<()> {
    let status = electrum_transaction_status(config, entry.txid)?;
    let Some(mut status) = status else {
        return Ok(());
    };
    if status.height.is_none() {
        status.height = entry.height;
    }
    electrum_add_status_update(config, tx_update, checkpoint_blocks, status)
}

fn electrum_add_status_update(
    config: &ElectrumConfig,
    tx_update: &mut TxUpdate<ConfirmationBlockTime>,
    checkpoint_blocks: &mut Vec<BlockId>,
    status: ElectrumTxStatus,
) -> Result<()> {
    let txid = status.tx.compute_txid();
    tx_update.txs.push(Arc::new(status.tx.clone()));
    if let Some(height) = status.height {
        let header = electrum_block_header(config, height)?;
        let block_id = BlockId {
            height,
            hash: header.block_hash(),
        };
        tx_update.anchors.insert((
            ConfirmationBlockTime {
                block_id,
                confirmation_time: status.block_time.unwrap_or(header.time) as u64,
            },
            txid,
        ));
        checkpoint_blocks.push(block_id);
    } else {
        tx_update.seen_ats.insert((txid, now_secs()));
    }
    Ok(())
}

fn decode_header_hex(header_hex: &str) -> Result<Header> {
    let bytes = hex_to_bytes(header_hex)?;
    encode::deserialize(&bytes).context("decode Electrum block header")
}

fn hex_to_bytes(input: &str) -> Result<Vec<u8>> {
    let input = input.trim();
    if input.len() % 2 != 0 {
        bail!("hex string has odd length");
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    for index in (0..bytes.len()).step_by(2) {
        let high = hex_value(bytes[index])?;
        let low = hex_value(bytes[index + 1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex byte {}", byte as char),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn bitcoin_core_chain_tip(config: &BitcoinCoreConfig) -> Result<BlockId> {
    let client = config.client()?;
    let info = client
        .get_blockchain_info()
        .context("failed to fetch Bitcoin Core chain tip")?;
    Ok(BlockId {
        height: info.blocks as u32,
        hash: info.best_block_hash,
    })
}

pub fn bitcoin_core_tx_status(
    config: &BitcoinCoreConfig,
    txid: Txid,
) -> Result<Option<BitcoinCoreTxStatus>> {
    let client = config.client()?;
    if let Ok(info) = client.get_raw_transaction_info(&txid, None) {
        let tx = encode::deserialize(&info.hex)
            .with_context(|| format!("failed to decode Bitcoin Core transaction {txid}"))?;
        let mut height = None;
        let mut block_time = info.blocktime.and_then(|time| time.try_into().ok());
        if let Some(block_hash) = info.blockhash {
            if let Ok(block_info) = client.get_block_info(&block_hash) {
                height = Some(block_info.height as u32);
                block_time = block_time.or_else(|| block_info.time.try_into().ok());
            }
        }
        return Ok(Some(BitcoinCoreTxStatus {
            tx,
            confirmed: info.confirmations.unwrap_or_default() > 0 && info.blockhash.is_some(),
            block_hash: info.blockhash,
            height,
            block_time,
        }));
    }

    let info = client
        .get_blockchain_info()
        .context("failed to fetch Bitcoin Core chain tip for tx lookup")?;
    let tip = info.blocks as u32;
    let start_height = tip.saturating_sub(config.max_block_scan);
    for height in (start_height..=tip).rev() {
        let block_hash = client
            .get_block_hash(height as u64)
            .with_context(|| format!("failed to fetch block hash at height {height}"))?;
        let block_info = client
            .get_block_info(&block_hash)
            .with_context(|| format!("failed to fetch block info {block_hash}"))?;
        if !block_info.tx.contains(&txid) {
            continue;
        }
        let tx = client
            .get_raw_transaction(&txid, Some(&block_hash))
            .with_context(|| format!("failed to fetch transaction {txid} in block {block_hash}"))?;
        return Ok(Some(BitcoinCoreTxStatus {
            tx,
            confirmed: true,
            block_hash: Some(block_hash),
            height: Some(height),
            block_time: block_info.time.try_into().ok(),
        }));
    }
    Ok(None)
}

pub fn refresh_known_wallet_outpoints_with_bitcoin_core(
    local: &mut LocalWallet,
    config: &BitcoinCoreConfig,
) -> Result<()> {
    let client = config.client()?;
    let info = client
        .get_blockchain_info()
        .context("failed to fetch Bitcoin Core blockchain info")?;
    let wallet_network = local.wallet.network();
    if info.chain != wallet_network {
        bail!(
            "Bitcoin Core RPC is on {:?}, but wallet is on {:?}",
            info.chain,
            wallet_network
        );
    }
    if info.initial_block_download {
        bail!("Bitcoin Core is still in initial block download");
    }

    let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
    let mut checkpoint_blocks = vec![BlockId {
        height: 0,
        hash: client
            .get_block_hash(0)
            .context("failed to fetch Bitcoin Core genesis hash")?,
    }];

    for utxo in local.wallet.list_unspent() {
        let outpoint = utxo.outpoint;
        let Some(txout) = client
            .get_tx_out(&outpoint.txid, outpoint.vout, Some(true))
            .with_context(|| format!("failed to fetch Bitcoin Core txout {outpoint}"))?
        else {
            continue;
        };
        tx_update.txouts.insert(
            outpoint,
            TxOut {
                value: txout.value,
                script_pubkey: txout.script_pub_key.script().with_context(|| {
                    format!("failed to decode Bitcoin Core txout script {outpoint}")
                })?,
            },
        );
        if txout.confirmations == 0 {
            continue;
        }
        let height = (info.blocks as u32)
            .saturating_add(1)
            .saturating_sub(txout.confirmations);
        let hash = client
            .get_block_hash(height as u64)
            .with_context(|| format!("failed to fetch block hash at height {height}"))?;
        let header = client
            .get_block_header_info(&hash)
            .with_context(|| format!("failed to fetch block header {hash}"))?;
        tx_update.anchors.insert((
            ConfirmationBlockTime {
                block_id: BlockId { height, hash },
                confirmation_time: header.time as u64,
            },
            outpoint.txid,
        ));
        checkpoint_blocks.push(BlockId { height, hash });
    }

    checkpoint_blocks.push(BlockId {
        height: info.blocks as u32,
        hash: info.best_block_hash,
    });
    let checkpoint = extend_latest_checkpoint(local.wallet.latest_checkpoint(), checkpoint_blocks)?;
    local
        .wallet
        .apply_update(Update {
            tx_update,
            chain: Some(checkpoint),
            ..Default::default()
        })
        .context("failed to apply Bitcoin Core known outpoint update")?;
    Ok(())
}

pub fn esplora_client(
    network: Network,
    override_url: Option<&str>,
) -> Result<bdk_esplora::esplora_client::blocking::BlockingClient> {
    let url = effective_esplora_url(network, override_url)?;
    Ok(esplora_client_with_config(&EsploraConfig::new(url)))
}

pub fn esplora_client_with_config(
    config: &EsploraConfig,
) -> bdk_esplora::esplora_client::blocking::BlockingClient {
    config.builder().build_blocking()
}

pub fn effective_esplora_url(network: Network, override_url: Option<&str>) -> Result<String> {
    Ok(match override_url {
        Some(url) => url.to_string(),
        None => default_esplora(network)?.to_string(),
    })
}

fn sync_wallet_bitcoin_core(local: &mut LocalWallet, config: &BitcoinCoreConfig) -> Result<()> {
    let client = config.client()?;
    let info = client
        .get_blockchain_info()
        .context("failed to fetch Bitcoin Core blockchain info")?;
    let wallet_network = local.wallet.network();
    if info.chain != wallet_network {
        bail!(
            "Bitcoin Core RPC is on {:?}, but wallet is on {:?}",
            info.chain,
            wallet_network
        );
    }
    if info.initial_block_download {
        bail!("Bitcoin Core is still in initial block download");
    }

    let mut checkpoint_blocks = vec![BlockId {
        height: 0,
        hash: client
            .get_block_hash(0)
            .context("failed to fetch Bitcoin Core genesis hash")?,
    }];
    let tx_update = scan_bitcoin_core_utxos(local, &client, config, &mut checkpoint_blocks)
        .context("failed to scan Bitcoin Core UTXO set")?;

    let current_height = local.wallet.latest_checkpoint().height();
    let tip_height = info.blocks as u32;
    let blocks_to_tip = tip_height.saturating_sub(current_height);
    if current_height > 0 && current_height < tip_height && blocks_to_tip <= config.max_block_scan {
        let expected_mempool = local
            .wallet
            .transactions()
            .filter(|tx| tx.chain_position.is_unconfirmed())
            .map(|tx| tx.tx_node.tx.clone())
            .collect::<Vec<_>>();
        let mut emitter = Emitter::new(
            Arc::clone(&client),
            local.wallet.latest_checkpoint(),
            current_height.saturating_add(1),
            expected_mempool,
        );
        while let Some(emission) = emitter
            .next_block()
            .context("failed to fetch Bitcoin Core block")?
        {
            local
                .wallet
                .apply_block_connected_to(
                    &emission.block,
                    emission.block_height(),
                    emission.connected_to(),
                )
                .context("failed to apply Bitcoin Core block")?;
        }
        let mempool = emitter
            .mempool()
            .context("failed to fetch Bitcoin Core mempool")?;
        local.wallet.apply_unconfirmed_txs(mempool.update);
        local.wallet.apply_evicted_txs(mempool.evicted);
    }

    checkpoint_blocks.push(BlockId {
        height: info.blocks as u32,
        hash: info.best_block_hash,
    });
    let checkpoint = extend_latest_checkpoint(local.wallet.latest_checkpoint(), checkpoint_blocks)?;
    local
        .wallet
        .apply_update(Update {
            tx_update,
            chain: Some(checkpoint),
            ..Default::default()
        })
        .context("failed to apply Bitcoin Core wallet update")?;
    Ok(())
}

fn extend_latest_checkpoint(
    mut checkpoint: CheckPoint,
    mut block_ids: Vec<BlockId>,
) -> Result<CheckPoint> {
    block_ids.sort_by_key(|block| block.height);
    let mut deduped: Vec<BlockId> = Vec::with_capacity(block_ids.len());
    for block_id in block_ids {
        if let Some(last) = deduped.last() {
            if last.height == block_id.height {
                if last.hash != block_id.hash {
                    bail!(
                        "Bitcoin Core returned conflicting block hashes at height {}",
                        block_id.height
                    );
                }
                continue;
            }
        }
        deduped.push(block_id);
    }

    for block_id in deduped {
        checkpoint = checkpoint.insert(block_id);
    }
    Ok(checkpoint)
}

fn scan_bitcoin_core_utxos(
    local: &LocalWallet,
    client: &Arc<BitcoinCoreClient>,
    config: &BitcoinCoreConfig,
    checkpoint_blocks: &mut Vec<BlockId>,
) -> Result<TxUpdate<ConfirmationBlockTime>> {
    let range = (0_u64, config.descriptor_scan_range.saturating_sub(1) as u64);
    let requests = [
        ScanTxOutRequest::Extended {
            desc: local
                .wallet
                .public_descriptor(KeychainKind::External)
                .to_string(),
            range,
        },
        ScanTxOutRequest::Extended {
            desc: local
                .wallet
                .public_descriptor(KeychainKind::Internal)
                .to_string(),
            range,
        },
    ];
    let scan = client
        .scan_tx_out_set_blocking(&requests)
        .context("Bitcoin Core scantxoutset failed")?;
    if scan.success == Some(false) {
        bail!("Bitcoin Core scantxoutset did not complete successfully");
    }

    let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
    for utxo in scan.unspents {
        let height = utxo.height as u32;
        let hash = client
            .get_block_hash(utxo.height)
            .with_context(|| format!("failed to fetch block hash at height {}", utxo.height))?;
        let header = client
            .get_block_header_info(&hash)
            .with_context(|| format!("failed to fetch block header {hash}"))?;
        let block = client
            .get_block(&hash)
            .with_context(|| format!("failed to fetch block {hash}"))?;
        let tx = block
            .txdata
            .iter()
            .find(|tx| tx.compute_txid() == utxo.txid)
            .with_context(|| format!("failed to find transaction {} in block {hash}", utxo.txid))?
            .clone();
        let outpoint = OutPoint {
            txid: utxo.txid,
            vout: utxo.vout,
        };
        tx_update.txs.push(Arc::new(tx));
        tx_update.txouts.insert(
            outpoint,
            TxOut {
                value: utxo.amount,
                script_pubkey: utxo.script_pub_key,
            },
        );
        tx_update.anchors.insert((
            ConfirmationBlockTime {
                block_id: BlockId { height, hash },
                confirmation_time: header.time as u64,
            },
            utxo.txid,
        ));
        checkpoint_blocks.push(BlockId { height, hash });
    }
    Ok(tx_update)
}

fn detect_default_bitcoin_core(network: Network) -> Option<BitcoinCoreConfig> {
    if env_flag("BTC_LOCAL_WALLET_DISABLE_BITCOIN_CORE") {
        return None;
    }

    let rpc_url = env::var("BITCOIN_RPC_URL")
        .or_else(|_| env::var("BTC_LOCAL_WALLET_BITCOIN_RPC_URL"))
        .unwrap_or_else(|_| default_bitcoin_core_rpc_url(network).to_string());
    let mut config = BitcoinCoreConfig::new(rpc_url);
    config.rpc_user = env::var("BITCOIN_RPC_USER")
        .or_else(|_| env::var("BTC_LOCAL_WALLET_BITCOIN_RPC_USER"))
        .ok();
    config.rpc_password = env::var("BITCOIN_RPC_PASSWORD")
        .or_else(|_| env::var("BTC_LOCAL_WALLET_BITCOIN_RPC_PASSWORD"))
        .ok();
    config.cookie_file = env::var_os("BITCOIN_RPC_COOKIE")
        .or_else(|| env::var_os("BITCOIN_COOKIE_FILE"))
        .or_else(|| env::var_os("BTC_LOCAL_WALLET_BITCOIN_RPC_COOKIE"))
        .map(PathBuf::from)
        .or_else(|| {
            bitcoin_core_datadir_candidates(network)
                .into_iter()
                .map(|datadir| bitcoin_core_cookie_file(&datadir, network))
                .find(|cookie| cookie.exists())
        });
    config.descriptor_scan_range = env::var("BTC_LOCAL_WALLET_BITCOIN_DESCRIPTOR_SCAN_RANGE")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(BITCOIN_CORE_DESCRIPTOR_SCAN_RANGE);
    config.max_block_scan = env::var("BTC_LOCAL_WALLET_BITCOIN_MAX_BLOCK_SCAN")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(BITCOIN_CORE_MAX_BLOCK_SCAN);
    config.rpc_timeout_secs = env::var("BTC_LOCAL_WALLET_BITCOIN_RPC_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(BITCOIN_CORE_RPC_TIMEOUT_SECS);

    if config.rpc_user.is_some() && config.rpc_password.is_some() || config.cookie_file.is_some() {
        Some(config)
    } else {
        None
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn bitcoin_core_datadir_candidates(network: Network) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(datadir) =
        env::var_os("BITCOIN_DATADIR").or_else(|| env::var_os("BTC_LOCAL_WALLET_BITCOIN_DATADIR"))
    {
        candidates.push(PathBuf::from(datadir));
    }
    if let Ok(cwd) = env::current_dir() {
        push_network_datadir_candidates(&mut candidates, &cwd, network);
        if let Some(parent) = cwd.parent() {
            push_network_datadir_candidates(&mut candidates, parent, network);
        }
    }
    if let Some(home) = env::var_os("HOME") {
        candidates.push(PathBuf::from(home).join(".bitcoin"));
    }
    candidates
}

fn push_network_datadir_candidates(candidates: &mut Vec<PathBuf>, base: &Path, network: Network) {
    match network {
        Network::Bitcoin => candidates.push(base.join("bitcoin")),
        Network::Testnet => candidates.push(base.join("bitcoin-testnet3")),
        Network::Testnet4 => candidates.push(base.join("bitcoin-testnet4")),
        Network::Signet => candidates.push(base.join("bitcoin-signet")),
        Network::Regtest => candidates.push(base.join("bitcoin-regtest")),
    }
    candidates.push(base.join(format!("bitcoin-{network}")));
}

fn bitcoin_core_cookie_file(datadir: &Path, network: Network) -> PathBuf {
    match network {
        Network::Bitcoin => datadir.join(".cookie"),
        Network::Testnet => datadir.join("testnet3").join(".cookie"),
        Network::Testnet4 => datadir.join("testnet4").join(".cookie"),
        Network::Signet => datadir.join("signet").join(".cookie"),
        Network::Regtest => datadir.join("regtest").join(".cookie"),
    }
}

fn default_bitcoin_core_rpc_url(network: Network) -> &'static str {
    match network {
        Network::Bitcoin => "http://127.0.0.1:8332",
        Network::Testnet => "http://127.0.0.1:18332",
        Network::Testnet4 => "http://127.0.0.1:48332",
        Network::Signet => "http://127.0.0.1:38332",
        Network::Regtest => "http://127.0.0.1:18443",
    }
}

fn descriptor_from_mnemonic(
    mnemonic: &bdk_wallet::bip39::Mnemonic,
    network: Network,
    branch: u32,
) -> Result<impl IntoWalletDescriptor + Clone + use<>> {
    let path = rgb_bip84_derivation_path(network, branch);
    let key: DescriptorKey<bdk_wallet::miniscript::Segwitv0> = mnemonic
        .clone()
        .into_descriptor_key(None, path)
        .context("failed to derive descriptor key")?;
    Ok(bdk_wallet::descriptor!(wpkh(key)).context("failed to build descriptor")?)
}

fn rgb_bip84_derivation_path(network: Network, branch: u32) -> DerivationPath {
    DerivationPath::from_iter([
        ChildNumber::Hardened { index: 84 },
        ChildNumber::Hardened {
            index: rgb_bip84_coin_type(network),
        },
        ChildNumber::Hardened { index: 0 },
        ChildNumber::Normal { index: branch },
    ])
}

fn rgb_bip84_coin_type(network: Network) -> u32 {
    if network == Network::Bitcoin {
        RGB_MAINNET_COIN_TYPE
    } else {
        RGB_TESTNET_COIN_TYPE
    }
}

fn default_esplora(network: Network) -> Result<&'static str> {
    match network {
        Network::Bitcoin => Ok("https://blockstream.info/api"),
        Network::Testnet => Ok("https://blockstream.info/testnet/api"),
        Network::Testnet4 => Ok("https://mempool.space/testnet4/api"),
        Network::Signet => Ok("https://blockstream.info/signet/api"),
        Network::Regtest => bail!("regtest requires --esplora http://host:port"),
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rgb_bip84_derivation_paths_use_rgb_coin_types() {
        assert_eq!(
            rgb_bip84_derivation_path(Network::Bitcoin, 0).to_string(),
            "84'/827166'/0'/0"
        );
        assert_eq!(
            rgb_bip84_derivation_path(Network::Bitcoin, 1).to_string(),
            "84'/827166'/0'/1"
        );

        for network in [
            Network::Testnet,
            Network::Testnet4,
            Network::Signet,
            Network::Regtest,
        ] {
            assert_eq!(
                rgb_bip84_derivation_path(network, 0).to_string(),
                "84'/827167'/0'/0"
            );
            assert_eq!(
                rgb_bip84_derivation_path(network, 1).to_string(),
                "84'/827167'/0'/1"
            );
        }
    }
}
