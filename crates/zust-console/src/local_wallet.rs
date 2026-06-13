use std::{
    env, fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
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
    consensus::encode,
    Address, Amount, BlockHash, Network, OutPoint, Transaction, TxOut, Txid,
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

pub const MAGIC: &[u8] = b"LocalBtcWallet";
pub const CONFIG_FILE: &str = "wallet.json";
pub const BDK_FILE: &str = "bdk_wallet";
const BDK_SYNC_CACHE_FILE: &str = "bdk_sync_cache.json";
const BITCOIN_CORE_DESCRIPTOR_SCAN_RANGE: u32 = 1000;
const BITCOIN_CORE_MAX_BLOCK_SCAN: u32 = 10_000;
const BITCOIN_CORE_RPC_TIMEOUT_SECS: u64 = 300;
const RGB_MAINNET_COIN_TYPE: u32 = 827_166;
const RGB_TESTNET_COIN_TYPE: u32 = 827_167;
static BDK_STORE_OPEN_LOCK: Mutex<()> = Mutex::new(());

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChainSource {
    Esplora(EsploraConfig),
    BitcoinCore(BitcoinCoreConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EsploraConfig {
    pub url: String,
    pub api_key: Option<String>,
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
            return Ok(Self::Esplora(EsploraConfig::new(url.to_string())));
        }
        Ok(Self::Esplora(EsploraConfig::new(
            default_esplora(network)?.to_string(),
        )))
    }

    pub fn cache_key(&self) -> String {
        match self {
            Self::Esplora(config) => format!("esplora:{}", config.url),
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
        fs::create_dir_all(data_dir)
            .with_context(|| format!("failed to create {}", data_dir.display()))?;

        let external = descriptor_from_mnemonic(mnemonic, network, 0)?;
        let internal = descriptor_from_mnemonic(mnemonic, network, 1)?;

        let _bdk_store_guard = BDK_STORE_OPEN_LOCK
            .lock()
            .map_err(|err| anyhow!("BDK store open lock poisoned: {err}"))?;
        let (mut db, _) = load_bdk_store(&data_dir.join(BDK_FILE))?;
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
            None => Wallet::create(external, internal)
                .network(network)
                .create_wallet(&mut db)
                .context("failed to create BDK wallet")?,
        };

        Ok(Self {
            wallet,
            data_dir: data_dir.to_path_buf(),
            db,
        })
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
        ChainSource::BitcoinCore(config) => {
            let client = config.client()?;
            client
                .send_raw_transaction(tx)
                .context("failed to broadcast BTC transaction through Bitcoin Core")?;
        }
    }
    Ok(tx.compute_txid())
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
