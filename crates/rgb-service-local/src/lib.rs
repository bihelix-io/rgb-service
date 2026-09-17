use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::{Duration, Instant},
};

use amplify::confinement::{Confined, U32 as U32MAX};
use anyhow::{anyhow, Context, Result};
use bitcoin::{Amount, Network, OutPoint, Psbt, ScriptBuf, Transaction, Txid};
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};
use nonasync::persistence::CloneNoPersistence;
use psrgbt::{RgbOutExt, RgbPsbtExt};
use rgb_schemata::NonInflatableAsset;
use rgbstd::{
    containers::{
        BuilderSeal, Consignment, ConsignmentExt, Fascia, FileContent, Transfer, ValidTransfer,
    },
    contract::{AllocatedState, ContractBuilder, FilterIncludeAll, IssuerWrapper},
    indexers::{esplora_blocking::esplora_client, AnyResolver},
    persistence::{fjall::FjallBinStore, fs::FsBinStore, StashReadProvider, Stock},
    stl::{AssetSpec, ContractTerms, Name, Ticker},
    txout::CloseMethod,
    validation::{
        ResolveWitness, ValidationConfig, WitnessOrdProvider, WitnessResolverError, WitnessStatus,
    },
    vm::WitnessOrd,
    ContractId, GenesisSeal, Identity, Operation, Opout, OutputSeal, Transition, Txid as RgbTxid,
};
use serde::{Deserialize, Serialize};
use strict_types::{StrictDeserialize, StrictSerialize};

pub use rgbstd;

const RGB_STOCK_PARTITION: &str = "rgb_stock";
const RGB_PENDING_OPS_PARTITION: &str = "rgb_pending_ops";
const RGB_PENDING_STATUS_PARTITION: &str = "rgb_pending_status";
const SHARED_RGB_ACCOUNT_MARKER: &str = ".rgb-accounts";

static LOCAL_STORES: OnceLock<Mutex<HashMap<PathBuf, Arc<LocalRgbStoreInner>>>> = OnceLock::new();
static TX_CONFIRMATION_CACHE: OnceLock<Mutex<HashMap<(Network, Txid), TxConfirmationCacheEntry>>> =
    OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChainSource {
    Esplora(EsploraConfig),
    Electrum(ElectrumConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EsploraConfig {
    pub url: String,
    pub api_key: Option<String>,
}

impl EsploraConfig {
    pub fn new(url: String) -> Self {
        Self { url, api_key: None }
    }

    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        self.api_key = api_key.filter(|value| !value.trim().is_empty());
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElectrumConfig {
    pub url: String,
}

impl ElectrumConfig {
    pub fn new(url: String) -> Self {
        Self {
            url: normalize_electrum_url(&url),
        }
    }
}

#[derive(Clone)]
pub struct LocalRgbStore {
    inner: Arc<LocalRgbStoreInner>,
    key_prefix: Vec<u8>,
    logical_name: String,
}

struct LocalRgbStoreInner {
    path: PathBuf,
    db: SingleWriterTxDatabase,
    rgb_stock_locks: Mutex<HashMap<Vec<u8>, Arc<RwLock<()>>>>,
}

impl LocalRgbStore {
    /// Registers an already-open database so account stock handles share the
    /// daemon's single Fjall engine instead of opening the same directory a
    /// second time.
    pub fn register_database(store_dir: &Path, db: SingleWriterTxDatabase) -> Result<()> {
        fs::create_dir_all(store_dir)
            .with_context(|| format!("create local RGB store {}", store_dir.display()))?;
        let path = fs::canonicalize(store_dir)
            .with_context(|| format!("canonicalize local RGB store {}", store_dir.display()))?;
        let mut stores = LOCAL_STORES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|err| anyhow!("local RGB store registry lock poisoned: {err}"))?;
        stores.entry(path.clone()).or_insert_with(|| {
            Arc::new(LocalRgbStoreInner {
                path,
                db,
                rgb_stock_locks: Mutex::new(HashMap::new()),
            })
        });
        Ok(())
    }

    pub fn open(store_locator: &Path) -> Result<Self> {
        let (store_dir, namespace, logical_name) =
            if let Some(account_id) = shared_rgb_store_account_id(store_locator) {
                let marker_dir = store_locator
                    .parent()
                    .context("shared RGB account locator has no marker directory")?;
                let store_dir = marker_dir
                    .parent()
                    .context("shared RGB account locator has no database directory")?;
                (
                    store_dir.to_path_buf(),
                    account_id.as_bytes().to_vec(),
                    account_id,
                )
            } else {
                (
                    store_locator.to_path_buf(),
                    Vec::new(),
                    store_locator.display().to_string(),
                )
            };
        fs::create_dir_all(&store_dir)
            .with_context(|| format!("create local RGB store {}", store_dir.display()))?;
        let path = fs::canonicalize(&store_dir)
            .with_context(|| format!("canonicalize local RGB store {}", store_dir.display()))?;
        let mut stores = LOCAL_STORES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|err| anyhow!("local RGB store registry lock poisoned: {err}"))?;
        let inner = if let Some(existing) = stores.get(&path) {
            Arc::clone(existing)
        } else {
            let db = SingleWriterTxDatabase::builder(&path)
                .open()
                .with_context(|| format!("open local RGB store {}", path.display()))?;
            let inner = Arc::new(LocalRgbStoreInner {
                path: path.clone(),
                db,
                rgb_stock_locks: Mutex::new(HashMap::new()),
            });
            stores.insert(path, Arc::clone(&inner));
            inner
        };
        let mut key_prefix = namespace;
        if !key_prefix.is_empty() {
            key_prefix.push(0);
        }
        Ok(Self {
            inner,
            key_prefix,
            logical_name,
        })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn rgb_stock_store(&self) -> Result<FjallBinStore> {
        Ok(FjallBinStore::with_database_prefix(
            self.inner.path.clone(),
            self.inner.db.clone(),
            RGB_STOCK_PARTITION,
            &self.key_prefix,
        )
        .context("open RGB stock partition")?)
    }

    pub fn rgb_stock_has_data(&self) -> Result<bool> {
        self.rgb_stock_store()?
            .has_data()
            .map_err(|err| anyhow!("check RGB stock partition: {err:?}"))
    }

    pub fn with_rgb_stock_write_lock<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let mut locks = self
            .inner
            .rgb_stock_locks
            .lock()
            .map_err(|err| anyhow!("local RGB stock lock registry poisoned: {err}"))?;
        let lock = Arc::clone(
            locks
                .entry(self.key_prefix.clone())
                .or_insert_with(|| Arc::new(RwLock::new(()))),
        );
        drop(locks);
        let _guard = lock
            .write()
            .map_err(|err| anyhow!("local RGB stock write lock poisoned: {err}"))?;
        f()
    }

    pub fn logical_name(&self) -> &str {
        &self.logical_name
    }

    fn namespaced_key(&self, suffix: impl AsRef<[u8]>) -> Vec<u8> {
        let suffix = suffix.as_ref();
        let mut key = Vec::with_capacity(self.key_prefix.len() + suffix.len());
        key.extend_from_slice(&self.key_prefix);
        key.extend_from_slice(suffix);
        key
    }

    fn pending_ops_keyspace(&self) -> Result<fjall::SingleWriterTxKeyspace> {
        self.inner
            .db
            .keyspace(RGB_PENDING_OPS_PARTITION, KeyspaceCreateOptions::default)
            .context("open RGB pending ops partition")
    }

    fn pending_status_keyspace(&self) -> Result<fjall::SingleWriterTxKeyspace> {
        self.inner
            .db
            .keyspace(RGB_PENDING_STATUS_PARTITION, KeyspaceCreateOptions::default)
            .context("open RGB pending status partition")
    }

    pub fn put_pending_operation(
        &self,
        txid: impl std::fmt::Display,
        op: &[u8],
        status: &RgbPendingStockStatus,
    ) -> Result<()> {
        let txid = txid.to_string();
        let key = self.namespaced_key(txid.as_bytes());
        let ops = self.pending_ops_keyspace()?;
        let statuses = self.pending_status_keyspace()?;
        let status = serde_json::to_vec(status).context("encode RGB pending status")?;
        let mut tx = self.inner.db.write_tx();
        tx.insert(&ops, &key, op);
        tx.insert(&statuses, key, status);
        tx.commit()
            .with_context(|| format!("commit RGB pending operation {txid}"))?;
        self.persist()
    }

    pub fn put_pending_status(
        &self,
        txid: impl std::fmt::Display,
        status: &RgbPendingStockStatus,
    ) -> Result<()> {
        let txid = txid.to_string();
        let keyspace = self.pending_status_keyspace()?;
        let bytes = serde_json::to_vec(status).context("encode RGB pending status")?;
        let mut tx = self.inner.db.write_tx();
        tx.insert(&keyspace, self.namespaced_key(txid.as_bytes()), bytes);
        tx.commit()
            .with_context(|| format!("commit RGB pending status {txid}"))?;
        self.persist()
    }

    pub fn pending_status(
        &self,
        txid: impl std::fmt::Display,
    ) -> Result<Option<RgbPendingStockStatus>> {
        let txid = txid.to_string();
        let keyspace = self.pending_status_keyspace()?;
        keyspace
            .get(self.namespaced_key(txid.as_bytes()))
            .with_context(|| format!("read RGB pending status {txid}"))?
            .map(|bytes| {
                serde_json::from_slice(bytes.as_ref())
                    .with_context(|| format!("decode RGB pending status {txid}"))
            })
            .transpose()
    }

    pub fn pending_statuses(&self) -> Result<Vec<RgbPendingStockStatus>> {
        let keyspace = self.pending_status_keyspace()?;
        let mut statuses = Vec::new();
        for item in keyspace.as_ref().prefix(&self.key_prefix) {
            let bytes = item.value().context("read RGB pending status value")?;
            statuses
                .push(serde_json::from_slice(bytes.as_ref()).context("decode RGB pending status")?);
        }
        Ok(statuses)
    }

    pub fn pending_txids(&self) -> Result<Vec<Txid>> {
        let keyspace = self.pending_ops_keyspace()?;
        let mut txids = Vec::new();
        for item in keyspace.as_ref().prefix(&self.key_prefix) {
            let key = item.key().context("read RGB pending op key")?;
            let suffix = key
                .as_ref()
                .strip_prefix(self.key_prefix.as_slice())
                .context("RGB pending op key has wrong namespace")?;
            txids.push(
                std::str::from_utf8(suffix)
                    .context("RGB pending op key is not UTF-8")?
                    .parse()
                    .context("RGB pending op key is not a txid")?,
            );
        }
        txids.sort();
        Ok(txids)
    }

    pub fn has_active_pending(&self) -> Result<bool> {
        for txid in self.pending_txids()? {
            if self
                .pending_status(txid)?
                .is_none_or(|status| !is_terminal_pending_stock_status(&status.status))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn confirmed_pending_txids(&self) -> Result<BTreeSet<Txid>> {
        Ok(self
            .pending_statuses()?
            .into_iter()
            .filter(|status| status.status == "confirmed")
            .filter_map(|status| status.txid.parse().ok())
            .collect())
    }

    pub fn copy_pending_from(&self, source: &LocalRgbStore) -> Result<usize> {
        let mut copied = 0;
        for txid in source.pending_txids()? {
            if self.get_pending_op(txid)?.is_some() {
                continue;
            }
            let Some(op) = source.get_pending_op(txid)? else {
                continue;
            };
            let source_status = source.pending_status(txid)?;
            let status_name = source_status
                .as_ref()
                .map(|status| status.status.as_str())
                .unwrap_or("pending");
            let status = pending_stock_status(
                txid,
                status_name,
                self.logical_name(),
                source_status
                    .as_ref()
                    .and_then(|status| status.confirmed_at),
                source_status.as_ref().and_then(|status| status.promoted_at),
            );
            self.put_pending_operation(txid, &op, &status)?;
            copied += 1;
        }
        Ok(copied)
    }

    pub fn get_pending_op(&self, txid: impl std::fmt::Display) -> Result<Option<Vec<u8>>> {
        let keyspace = self.pending_ops_keyspace()?;
        Ok(keyspace
            .get(self.namespaced_key(txid.to_string().as_bytes()))
            .with_context(|| format!("read RGB pending op {txid}"))?
            .map(|bytes| bytes.as_ref().to_vec()))
    }

    pub fn put_pending_op(&self, txid: impl std::fmt::Display, op: &[u8]) -> Result<()> {
        let keyspace = self.pending_ops_keyspace()?;
        let mut tx = self.inner.db.write_tx();
        tx.insert(
            &keyspace,
            self.namespaced_key(txid.to_string().as_bytes()),
            op,
        );
        tx.commit()
            .with_context(|| format!("commit RGB pending op {txid}"))?;
        self.persist()
    }

    pub fn remove_pending_op(&self, txid: impl std::fmt::Display) -> Result<()> {
        let keyspace = self.pending_ops_keyspace()?;
        let statuses = self.pending_status_keyspace()?;
        let mut tx = self.inner.db.write_tx();
        let key = self.namespaced_key(txid.to_string().as_bytes());
        tx.remove(&keyspace, &key);
        tx.remove(&statuses, key);
        tx.commit()
            .with_context(|| format!("remove RGB pending op {txid}"))?;
        self.persist()
    }

    pub fn persist(&self) -> Result<()> {
        self.inner
            .db
            .persist(PersistMode::SyncAll)
            .context("persist local RGB store")
    }
}

/// Returns a logical account-stock locator backed by the shared Fjall database
/// at `database_dir`. The marker path is never created on disk; it only carries
/// the account namespace through the existing local RGB APIs.
pub fn shared_rgb_store_locator(database_dir: &Path, account_id: &str) -> PathBuf {
    database_dir
        .join(SHARED_RGB_ACCOUNT_MARKER)
        .join(hex_encode(account_id.as_bytes()))
}

/// Extracts the account id from a shared database stock locator.
pub fn shared_rgb_store_account_id(locator: &Path) -> Option<String> {
    let encoded = locator.file_name()?.to_str()?;
    let marker = locator.parent()?.file_name()?.to_str()?;
    if marker != SHARED_RGB_ACCOUNT_MARKER {
        return None;
    }
    String::from_utf8(hex_decode(encoded)?).ok()
}

/// Lists accounts with a persisted RGB stock in the shared database.
pub fn shared_rgb_stock_account_ids(db: &SingleWriterTxDatabase) -> Result<BTreeSet<String>> {
    shared_rgb_account_ids(db, RGB_STOCK_PARTITION)
}

/// Lists accounts with an active persisted RGB operation in the shared
/// database.
pub fn shared_rgb_pending_account_ids(db: &SingleWriterTxDatabase) -> Result<BTreeSet<String>> {
    shared_rgb_account_ids(db, RGB_PENDING_OPS_PARTITION)
}

fn shared_rgb_account_ids(
    db: &SingleWriterTxDatabase,
    keyspace_name: &str,
) -> Result<BTreeSet<String>> {
    let keyspace = db
        .keyspace(keyspace_name, KeyspaceCreateOptions::default)
        .with_context(|| format!("open {keyspace_name} partition"))?;
    let mut accounts = BTreeSet::new();
    for item in keyspace.as_ref().prefix(b"") {
        let key = item
            .key()
            .with_context(|| format!("read {keyspace_name} key"))?;
        let Some(delimiter) = key.as_ref().iter().position(|byte| *byte == 0) else {
            // Unprefixed records belong to the old one-database-per-account
            // layout and are not shared account records.
            continue;
        };
        let account_id = std::str::from_utf8(&key.as_ref()[..delimiter])
            .with_context(|| format!("decode {keyspace_name} account id"))?;
        accounts.insert(account_id.to_string());
    }
    Ok(accounts)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(encoded: &str) -> Option<Vec<u8>> {
    if encoded.len() % 2 != 0 {
        return None;
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some(((high << 4) | low) as u8)
        })
        .collect()
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RgbAccountStoreMigrationReport {
    pub stock_migrated: bool,
    pub pending_migrated: usize,
}

/// Copies an old per-account Fjall stock and its pending operations into the
/// account namespace of the shared database. The operation is idempotent and
/// does not delete the source directory.
pub fn migrate_rgb_account_store(
    source_store_dir: &Path,
    target_store_locator: &Path,
) -> Result<RgbAccountStoreMigrationReport> {
    let source = LocalRgbStore::open(source_store_dir)?;
    let target = LocalRgbStore::open(target_store_locator)?;
    let mut report = RgbAccountStoreMigrationReport::default();

    if source.rgb_stock_has_data()? && !target.rgb_stock_has_data()? {
        target.with_rgb_stock_write_lock(|| {
            let source_provider = source.rgb_stock_store()?;
            let mut stock: Stock = Stock::load(source_provider, true)
                .map_err(|err| anyhow!("load per-account RGB stock: {err:?}"))?;
            stock
                .make_persistent(target.rgb_stock_store()?, true)
                .map_err(|err| anyhow!("attach shared RGB stock persistence: {err:?}"))?;
            stock
                .store()
                .map_err(|err| anyhow!("store shared RGB stock: {err:?}"))
        })?;
        report.stock_migrated = true;
    }

    for txid in source.pending_txids()? {
        if target.get_pending_op(txid)?.is_some() {
            continue;
        }
        let Some(operation) = source.get_pending_op(txid)? else {
            continue;
        };
        let source_status = source.pending_status(txid)?.or_else(|| {
            legacy_pending_stock_status(source_store_dir, txid)
                .ok()
                .flatten()
        });
        let status_name = source_status
            .as_ref()
            .map(|status| status.status.as_str())
            .unwrap_or("pending");
        let status = pending_stock_status(
            txid,
            status_name,
            target.logical_name(),
            source_status
                .as_ref()
                .and_then(|status| status.confirmed_at),
            source_status.as_ref().and_then(|status| status.promoted_at),
        );
        target.put_pending_operation(txid, &operation, &status)?;
        report.pending_migrated += 1;
    }
    Ok(report)
}

pub struct Rgb20IssueRequest {
    pub ticker: String,
    pub name: String,
    pub amount: u64,
    pub precision: u8,
    pub utxo: OutPoint,
}

pub struct Rgb20IssueResult {
    pub contract_id: ContractId,
    pub utxo: OutPoint,
    pub stock_dir: String,
}

#[derive(Clone, Debug)]
pub struct Rgb20ContractInfo {
    pub contract_id: ContractId,
    pub ticker: String,
    pub name: String,
    pub precision: u8,
}

#[derive(Clone, Debug)]
pub struct Rgb20TrackedUtxo {
    pub outpoint: OutPoint,
    pub address: Option<String>,
    pub confirmed: bool,
}

#[derive(Clone, Debug)]
pub struct Rgb20AssetAllocation {
    pub contract_id: ContractId,
    pub outpoint: OutPoint,
    pub address: Option<String>,
    pub amount: rgbstd::Amount,
    pub amount_raw: u64,
    pub amount_display: String,
    pub amount_encoding: String,
    pub ticker: String,
    pub name: String,
    pub precision: u8,
    pub witness: Option<RgbTxid>,
    pub confirmed: bool,
}

#[derive(Clone, Debug)]
pub struct Rgb20PsbtAssignment {
    pub contract_id: ContractId,
    pub amount: u64,
    pub vout: u32,
}

pub struct PreparedRgb20Psbt {
    pub fascia: Fascia,
    pub psbt: Psbt,
}

#[derive(Clone, Debug, Default)]
pub struct RgbPendingStockScanReport {
    pub scanned: usize,
    pub promoted: usize,
    pub pending: usize,
    pub skipped: usize,
    pub promoted_txids: Vec<Txid>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbPendingStockStatus {
    pub txid: String,
    pub status: String,
    pub main_stock_dir: String,
    pub staged_stock_dir: String,
    pub updated_at: u64,
    #[serde(default)]
    pub confirmed_at: Option<u64>,
    #[serde(default)]
    pub promoted_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RgbPendingOperation {
    SenderFascia { txid: String, fascia: Vec<u8> },
    ReceiverTransfer { txid: String, consignment: Vec<u8> },
}

#[derive(Clone, Copy, Debug)]
struct TentativeWitnessOrd;

impl WitnessOrdProvider for TentativeWitnessOrd {
    fn witness_ord(&self, _witness_id: RgbTxid) -> Result<WitnessOrd, WitnessResolverError> {
        Ok(WitnessOrd::Tentative)
    }
}

struct LocalWitnessResolver {
    inner: Box<dyn ResolveWitness + Send>,
    local_txs: HashMap<RgbTxid, Transaction>,
}

impl LocalWitnessResolver {
    fn new(
        inner: impl ResolveWitness + Send + 'static,
        local_txs: impl IntoIterator<Item = Transaction>,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            local_txs: local_txs
                .into_iter()
                .map(|tx| (txid_to_rgb(tx.compute_txid()), tx))
                .collect(),
        }
    }
}

impl ResolveWitness for LocalWitnessResolver {
    fn resolve_witness(&self, witness_id: RgbTxid) -> Result<WitnessStatus, WitnessResolverError> {
        if let Some(tx) = self.local_txs.get(&witness_id) {
            return Ok(WitnessStatus::Resolved(tx.clone(), WitnessOrd::Tentative));
        }
        self.inner.resolve_witness(witness_id)
    }

    fn check_chain_net(&self, chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        self.inner.check_chain_net(chain_net)
    }
}

pub fn issue_rgb20_fixed_with_chain_source(
    stock_dir: &Path,
    network: Network,
    chain_source: &ChainSource,
    request: Rgb20IssueRequest,
) -> Result<Rgb20IssueResult> {
    let ticker: Ticker = request
        .ticker
        .try_into()
        .map_err(|err| anyhow!("invalid RGB ticker: {err:?}"))?;
    let name: Name = request
        .name
        .try_into()
        .map_err(|err| anyhow!("invalid RGB asset name: {err:?}"))?;
    let precision = rgbstd::Precision::try_from(request.precision)
        .map_err(|err| anyhow!("invalid RGB precision: {err:?}"))?;

    let spec = AssetSpec {
        ticker,
        name,
        details: None,
        precision,
    };
    let genesis_outpoint = outpoint_to_rgb(request.utxo);
    let genesis_seal = GenesisSeal::new_random(genesis_outpoint.txid, genesis_outpoint.vout);

    let contract = ContractBuilder::with(
        Identity::default(),
        NonInflatableAsset::schema(),
        NonInflatableAsset::types(),
        NonInflatableAsset::scripts(),
        network_to_rgb(network),
    )
    .add_global_state("spec", spec)
    .context("failed to add RGB asset spec")?
    .add_global_state(
        "terms",
        ContractTerms {
            text: Default::default(),
            media: None,
        },
    )
    .context("failed to add RGB contract terms")?
    .add_global_state("issuedSupply", rgbstd::Amount::from(request.amount))
    .context("failed to add RGB issued supply")?
    .add_fungible_state(
        "assetOwner",
        BuilderSeal::Revealed(genesis_seal),
        request.amount,
    )
    .context("failed to add RGB owner state")?
    .issue_contract()
    .map_err(|err| anyhow!("failed to issue RGB contract: {err:?}"))?;

    let contract_id = contract.contract_id();
    let resolver = rgb_resolver(network, chain_source, [])?;
    with_rgb_stock_write_lock(stock_dir, || {
        let mut stock = open_or_create_stock(stock_dir)?;
        stock
            .import_contract(contract, resolver)
            .map_err(|err| anyhow!("failed to import RGB contract into stock: {err:?}"))?;
        stock
            .store()
            .map_err(|err| anyhow!("failed to persist RGB stock: {err:?}"))?;
        Ok(())
    })?;

    Ok(Rgb20IssueResult {
        contract_id,
        utxo: request.utxo,
        stock_dir: stock_dir.display().to_string(),
    })
}

// wallet-service-v2 migration: read legacy fs-backed RGB stocks and persist
// them into the account namespace of the daemon's shared Fjall database.
pub fn import_rgb20_stock_from_fs(source_stock_dir: &Path, target_stock_dir: &Path) -> Result<()> {
    let source_provider = FsBinStore::new(source_stock_dir.to_path_buf())
        .with_context(|| format!("open legacy RGB stock {}", source_stock_dir.display()))?;
    let mut stock: Stock = Stock::load(source_provider, true).map_err(|err| {
        anyhow!(
            "load legacy RGB stock {}: {err:?}",
            source_stock_dir.display()
        )
    })?;
    let target_store = LocalRgbStore::open(target_stock_dir)?;
    let target_provider = target_store.rgb_stock_store()?;
    stock
        .make_persistent(target_provider, true)
        .map_err(|err| {
            anyhow!(
                "persist migrated RGB stock {}: {err:?}",
                target_stock_dir.display()
            )
        })?;
    stock.store().map_err(|err| {
        anyhow!(
            "store migrated RGB stock {}: {err:?}",
            target_stock_dir.display()
        )
    })
}

// wallet-service-v2 migration: same as `import_rgb20_stock_from_fs` but also
// returns every outpoint carrying a fungible RGB allocation recorded in the
// stock (FilterIncludeAll). Loading the stock only once avoids re-reading the
// legacy stock for allocation extraction, which matters for large accounts.
pub fn import_rgb20_stock_and_allocations(
    source_stock_dir: &Path,
    target_stock_dir: &Path,
) -> Result<Vec<OutPoint>> {
    use rgbstd::contract::FilterIncludeAll;
    let source_provider = FsBinStore::new(source_stock_dir.to_path_buf())
        .with_context(|| format!("open legacy RGB stock {}", source_stock_dir.display()))?;
    let mut stock: Stock = Stock::load(source_provider, true).map_err(|err| {
        anyhow!(
            "load legacy RGB stock {}: {err:?}",
            source_stock_dir.display()
        )
    })?;

    // extract allocation outpoints while the stock is loaded in memory
    let mut allocation_outpoints = Vec::new();
    let contracts = stock
        .contracts()
        .map_err(|err| anyhow!("list contracts: {err:?}"))?;
    for info in contracts {
        let Ok(contract_data) = stock.contract_data(info.id) else {
            continue;
        };
        let Ok(allocations) = contract_data.fungible("assetOwner", FilterIncludeAll) else {
            continue;
        };
        for allocation in allocations {
            allocation_outpoints.push(allocation.seal.to_outpoint());
        }
    }

    let target_store = LocalRgbStore::open(target_stock_dir)?;
    let target_provider = target_store.rgb_stock_store()?;
    stock
        .make_persistent(target_provider, true)
        .map_err(|err| {
            anyhow!(
                "persist migrated RGB stock {}: {err:?}",
                target_stock_dir.display()
            )
        })?;
    stock.store().map_err(|err| {
        anyhow!(
            "store migrated RGB stock {}: {err:?}",
            target_stock_dir.display()
        )
    })?;
    Ok(allocation_outpoints)
}

pub fn import_selected_rgb20_contracts_from_fs(
    source_stock_dir: &Path,
    target_stock_dir: &Path,
    network: Network,
    chain_source: &ChainSource,
    contract_ids: impl IntoIterator<Item = ContractId>,
) -> Result<Vec<Rgb20ContractInfo>> {
    let source_provider = FsBinStore::new(source_stock_dir.to_path_buf())
        .with_context(|| format!("open legacy RGB stock {}", source_stock_dir.display()))?;
    let source_stock: Stock = Stock::load(source_provider, true).map_err(|err| {
        anyhow!(
            "load legacy RGB stock {}: {err:?}",
            source_stock_dir.display()
        )
    })?;

    let mut imported = Vec::new();
    with_rgb_stock_write_lock(target_stock_dir, || {
        let mut target_stock = open_or_create_stock(target_stock_dir)?;
        for contract_id in contract_ids {
            let Ok(contract_data) = source_stock.contract_data(contract_id) else {
                continue;
            };

            let spec = contract_data
                .global("spec")
                .next()
                .map(|strict_val| AssetSpec::from_strict_val_unchecked(&strict_val));
            let (ticker, name, precision) = spec
                .map(|spec| {
                    (
                        spec.ticker.to_string(),
                        spec.name.to_string(),
                        spec.precision.decimals(),
                    )
                })
                .unwrap_or_else(|| (String::new(), String::new(), 0));

            let opids = contract_data
                .fungible("assetOwner", FilterIncludeAll)
                .map_err(|err| anyhow!("load fungible state for {contract_id}: {err:?}"))?
                .map(|allocation| allocation.opout.op)
                .collect::<BTreeSet<_>>();

            if opids.is_empty() {
                let contract = source_stock
                    .export_contract(contract_id)
                    .map_err(|err| anyhow!("export contract {contract_id}: {err:?}"))?;
                let resolver = rgb_resolver_with_consignment(
                    network,
                    chain_source,
                    &contract,
                    std::iter::empty::<Transaction>(),
                )?;
                let validation = ValidationConfig {
                    chain_net: network_to_rgb(network),
                    trusted_typesystem: contract.types.clone(),
                    ..Default::default()
                };
                let valid = contract
                    .validate(&resolver, &validation)
                    .map_err(|err| anyhow!("validate contract {contract_id}: {err:?}"))?;
                target_stock
                    .import_contract(valid, resolver)
                    .map_err(|err| anyhow!("import contract {contract_id}: {err:?}"))?;
            } else {
                let transfer = source_stock
                    .transfer(contract_id, [], [], opids.iter().copied(), None)
                    .map_err(|err| anyhow!("export transfer branch {contract_id}: {err:?}"))?;
                let resolver = rgb_resolver_with_consignment(
                    network,
                    chain_source,
                    &transfer,
                    std::iter::empty::<Transaction>(),
                )?;
                let validation = ValidationConfig {
                    chain_net: network_to_rgb(network),
                    trusted_typesystem: transfer.types.clone(),
                    ..Default::default()
                };
                let valid = transfer
                    .validate(&resolver, &validation)
                    .map_err(|err| anyhow!("validate transfer {contract_id}: {err:?}"))?;
                target_stock
                    .accept_transfer(valid, resolver)
                    .map_err(|err| anyhow!("accept transfer {contract_id}: {err:?}"))?;
            }

            imported.push(Rgb20ContractInfo {
                contract_id,
                ticker,
                name,
                precision,
            });
        }

        target_stock
            .store()
            .map_err(|err| anyhow!("persist target RGB stock: {err:?}"))?;
        Ok(())
    })?;

    Ok(imported)
}

// wallet-service-v2 migration: inspect allocations directly from legacy stock
// files for dry-run balance reconciliation before importing.
pub fn list_legacy_rgb20_assets_for_utxos(
    source_stock_dir: &Path,
    utxos: impl IntoIterator<Item = Rgb20TrackedUtxo>,
) -> Result<Vec<Rgb20AssetAllocation>> {
    let source_provider = FsBinStore::new(source_stock_dir.to_path_buf())
        .with_context(|| format!("open legacy RGB stock {}", source_stock_dir.display()))?;
    let stock: Stock = Stock::load(source_provider, true).map_err(|err| {
        anyhow!(
            "load legacy RGB stock {}: {err:?}",
            source_stock_dir.display()
        )
    })?;
    list_rgb20_assets_from_stock(&stock, utxos)
}

pub fn list_legacy_rgb20_assets_for_contracts(
    source_stock_dir: &Path,
    contract_ids: impl IntoIterator<Item = ContractId>,
) -> Result<Vec<Rgb20AssetAllocation>> {
    let selected = contract_ids.into_iter().collect::<HashSet<ContractId>>();
    if selected.is_empty() {
        return Ok(Vec::new());
    }

    let source_provider = FsBinStore::new(source_stock_dir.to_path_buf())
        .with_context(|| format!("open legacy RGB stock {}", source_stock_dir.display()))?;
    let stock: Stock = Stock::load(source_provider, true).map_err(|err| {
        anyhow!(
            "load legacy RGB stock {}: {err:?}",
            source_stock_dir.display()
        )
    })?;

    let mut assets = Vec::new();
    for contract in stock
        .contracts()
        .map_err(|err| anyhow!("failed to list RGB contracts: {err:?}"))?
    {
        if !selected.contains(&contract.id) {
            continue;
        }

        let contract_data = stock
            .contract_data(contract.id)
            .map_err(|err| anyhow!("failed to load RGB contract data: {err:?}"))?;

        let spec = contract_data
            .global("spec")
            .next()
            .map(|strict_val| AssetSpec::from_strict_val_unchecked(&strict_val));

        let Ok(allocations) = contract_data.fungible("assetOwner", FilterIncludeAll) else {
            continue;
        };

        for allocation in allocations {
            let amount_raw = allocation.state.value();
            let (ticker, name, precision) = spec
                .as_ref()
                .map(|spec| {
                    (
                        spec.ticker.to_string(),
                        spec.name.to_string(),
                        spec.precision,
                    )
                })
                .unwrap_or_else(|| (String::new(), String::new(), rgbstd::Precision::Indivisible));

            assets.push(Rgb20AssetAllocation {
                contract_id: contract.id,
                outpoint: allocation.seal.to_outpoint(),
                address: None,
                amount: allocation.state,
                amount_raw,
                amount_display: rgbstd::CoinAmount::new(allocation.state, precision).to_string(),
                amount_encoding: allocation.state.to_string(),
                ticker,
                name,
                precision: precision.decimals(),
                witness: allocation.witness,
                confirmed: true,
            });
        }
    }

    Ok(assets)
}

pub fn select_rgb20_inputs(
    stock_dir: &Path,
    wallet_outpoints: impl IntoIterator<Item = OutPoint>,
    contract_id: ContractId,
    amount: u64,
) -> Result<Vec<OutPoint>> {
    let stock = open_or_create_stock(stock_dir)?;
    let available = wallet_outpoints.into_iter().collect::<Vec<_>>();
    let available_rgb = available
        .iter()
        .copied()
        .map(outpoint_to_rgb)
        .collect::<HashSet<_>>();
    let wallet_outpoints = available
        .iter()
        .map(|outpoint| (outpoint_to_rgb(*outpoint), *outpoint))
        .collect::<HashMap<_, _>>();
    let contract = stock
        .contract_data(contract_id)
        .map_err(|err| anyhow!("failed to load RGB contract data: {err:?}"))?;
    let mut state = contract
        .fungible("assetOwner", &available_rgb)
        .map_err(|err| anyhow!("failed to list RGB assetOwner state: {err:?}"))?
        .fold(
            BTreeMap::<_, Vec<rgbstd::Amount>>::new(),
            |mut map, allocation| {
                map.entry(allocation.seal)
                    .or_default()
                    .push(allocation.state);
                map
            },
        )
        .into_iter()
        .map(|(seal, amounts)| {
            (
                amounts.into_iter().sum::<rgbstd::Amount>(),
                seal.to_outpoint(),
            )
        })
        .collect::<Vec<_>>();
    state.sort_by_key(|(sum, _)| std::cmp::Reverse(*sum));

    let amount = rgbstd::Amount::from(amount);
    let mut selected = BTreeSet::new();
    let mut collected = rgbstd::Amount::ZERO;
    for (value, seal) in state {
        if collected >= amount {
            break;
        }
        collected += value;
        selected.insert(seal);
    }
    anyhow::ensure!(
        collected >= amount,
        "not enough RGB20 balance for contract {contract_id}: need {}, have {}",
        amount,
        collected
    );

    selected
        .into_iter()
        .map(|rgb_outpoint| {
            wallet_outpoints
                .get(&rgb_outpoint)
                .copied()
                .with_context(|| format!("RGB UTXO is not in wallet: {rgb_outpoint}"))
        })
        .collect()
}

pub fn prepare_rgb20_psbt(
    stock_dir: &Path,
    psbt: Psbt,
    change_vout: u32,
    assignments: impl IntoIterator<Item = Rgb20PsbtAssignment>,
) -> Result<PreparedRgb20Psbt> {
    let mut stock = open_or_create_stock(stock_dir)?;
    let assignments = assignments.into_iter().map(|assignment| {
        (
            assignment.contract_id,
            rgbstd::Amount::from(assignment.amount),
            RgbSeal::Vout(assignment.vout),
        )
    });
    let (fascia, psbt) = prepare_rgb20_psbt_inner(&mut stock, psbt, change_vout, assignments)?;
    Ok(PreparedRgb20Psbt { fascia, psbt })
}

/// UTEXO's deployed validator requires the first DBC-capable output to match
/// the OP_RETURN proof method. Preserve legacy validation semantics, but reject
/// an incompatible external carrier before building or signing any transition.
/// Output indexes belong to the caller; this function never silently reorders them.
pub fn validate_rgb20_external_carrier(psbt: &Psbt) -> Result<()> {
    let outputs = &psbt.unsigned_tx.output;
    let opret = outputs
        .iter()
        .position(|o| o.script_pubkey.is_op_return())
        .context("external RGB carrier requires OP_RETURN")?;
    anyhow::ensure!(
        outputs
            .iter()
            .filter(|o| o.script_pubkey.is_op_return())
            .count()
            == 1,
        "external RGB carrier requires exactly one OP_RETURN"
    );
    anyhow::ensure!(
        !outputs[..opret].iter().any(|o| o.script_pubkey.is_p2tr()),
        "external RGB OP_RETURN must precede every Taproot output for UTEXO compatibility"
    );
    Ok(())
}

pub fn prepare_rgb20_external_psbt(
    stock_dir: &Path,
    psbt: Psbt,
    change_vout: u32,
    assignments: impl IntoIterator<Item = Rgb20PsbtAssignment>,
) -> Result<PreparedRgb20Psbt> {
    validate_rgb20_external_carrier(&psbt)?;
    prepare_rgb20_psbt(stock_dir, psbt, change_vout, assignments)
}

pub fn build_rgb20_transfer_consignment(
    stock_dir: &Path,
    fascia: Fascia,
    contract_id: ContractId,
    txid: Txid,
    recipient_vout: u32,
) -> Result<Transfer> {
    let stock = open_or_create_stock(stock_dir)?;
    let mut staged_stock = stock.clone_no_persistence();
    staged_stock
        .consume_fascia(fascia.clone(), TentativeWitnessOrd)
        .map_err(|err| anyhow!("failed to consume staged sender RGB fascia: {err:?}"))?;
    staged_stock
        .transfer(
            contract_id,
            [OutputSeal::with(txid_to_rgb(txid), recipient_vout)],
            [],
            transfer_opids(&fascia, contract_id),
            Some(txid_to_rgb(txid)),
        )
        .map_err(|err| anyhow!("failed to build RGB transfer consignment: {err:?}"))
}

pub fn external_rgb_schema(stock_dir: &Path, contract: ContractId) -> Result<rgbstd::SchemaId> {
    let stock = open_or_create_stock(stock_dir)?;
    let schema = stock
        .as_stash_provider()
        .contract_schema(contract)
        .map_err(|e| anyhow!("read external contract schema: {e:?}"))?;
    Ok(schema.schema_id())
}

/// External recipients retain their actual concealed or witness beneficiary.
#[derive(Clone, Copy, Debug)]
pub enum ExternalRgbSeal {
    Witness(u32),
    Blind(rgbstd::SecretSeal),
}

pub fn prepare_external_rgb_transfer(
    stock_dir: &Path,
    psbt: Psbt,
    change_vout: u32,
    contract: ContractId,
    amount: u64,
    recipient: ExternalRgbSeal,
) -> Result<(PreparedRgb20Psbt, Transfer)> {
    anyhow::ensure!(amount > 0, "amount must be positive");
    validate_rgb20_external_carrier(&psbt)?;
    let mut stock = open_or_create_stock(stock_dir)?;
    let seal = match recipient {
        ExternalRgbSeal::Witness(v) => {
            anyhow::ensure!(v != change_vout, "recipient and change must differ");
            anyhow::ensure!(
                (v as usize) < psbt.unsigned_tx.output.len(),
                "missing recipient output"
            );
            RgbSeal::Vout(v)
        }
        ExternalRgbSeal::Blind(s) => RgbSeal::Blind(s),
    };
    let (fascia, psbt) = prepare_rgb20_psbt_inner(
        &mut stock,
        psbt,
        change_vout,
        [(contract, amount.into(), seal)],
    )?;
    let txid = psbt.unsigned_tx.compute_txid();
    let mut staged = stock.clone_no_persistence();
    staged
        .consume_fascia(fascia.clone(), TentativeWitnessOrd)
        .map_err(|e| anyhow!("stage external fascia: {e:?}"))?;
    let (outputs, secrets) = match recipient {
        ExternalRgbSeal::Witness(v) => (vec![OutputSeal::with(txid_to_rgb(txid), v)], vec![]),
        ExternalRgbSeal::Blind(s) => (vec![], vec![s]),
    };
    let transfer = staged
        .transfer(
            contract,
            outputs,
            secrets,
            transfer_opids(&fascia, contract),
            Some(txid_to_rgb(txid)),
        )
        .map_err(|e| anyhow!("build external consignment: {e:?}"))?;
    Ok((PreparedRgb20Psbt { fascia, psbt }, transfer))
}

/// Validate in a non-persistent stock before acknowledging or crediting a receive.
/// Only the exact current candidate transaction may be tentative; historical
/// witnesses still resolve through the configured chain source.
pub fn preview_external_rgb_receive(
    stock_dir: &Path,
    network: Network,
    source: &ChainSource,
    transfer: Transfer,
    candidate: Transaction,
    outpoint: OutPoint,
    secret: Option<rgbstd::GraphSeal>,
) -> Result<Vec<Rgb20AssetAllocation>> {
    let resolver = rgb_resolver_with_consignment(network, source, &transfer, [candidate.clone()])?;
    let validation = ValidationConfig {
        chain_net: network_to_rgb(network),
        trusted_typesystem: transfer.types.clone(),
        ..Default::default()
    };
    let valid = transfer
        .validate(&resolver, &validation)
        .map_err(|s| anyhow!("invalid external transfer: {s:?}"))?;
    let resolver = rgb_resolver_with_consignment(network, source, &valid, [candidate])?;
    let mut stock = open_or_create_stock(stock_dir)?.clone_no_persistence();
    if let Some(secret) = secret {
        stock
            .store_secret_seal(secret)
            .map_err(|e| anyhow!("store receive seal: {e:?}"))?;
    }
    stock
        .accept_transfer(valid, resolver)
        .map_err(|e| anyhow!("preview receive: {e:?}"))?;
    list_rgb20_assets_from_stock(
        &stock,
        [Rgb20TrackedUtxo {
            outpoint,
            address: None,
            confirmed: false,
        }],
    )
}

pub fn store_external_rgb_receive_secret(
    stock_dir: &Path,
    secret: rgbstd::GraphSeal,
) -> Result<()> {
    with_rgb_stock_write_lock(stock_dir, || {
        let mut stock = open_or_create_stock(stock_dir)?;
        stock
            .store_secret_seal(secret)
            .map_err(|e| anyhow!("store receive seal: {e:?}"))?;
        stock
            .store()
            .map_err(|e| anyhow!("persist receive seal: {e:?}"))
    })
}

pub fn accept_rgb20_transfer_with_chain_source(
    receiver_stock_dir: &Path,
    network: Network,
    chain_source: &ChainSource,
    consignment: Transfer,
) -> Result<()> {
    let valid = validate_rgb20_transfer_with_chain_source(network, chain_source, consignment)?;
    let resolver = rgb_resolver_with_consignment(network, chain_source, &valid, [])?;
    with_rgb_stock_write_lock(receiver_stock_dir, || {
        let mut stock = open_or_create_stock(receiver_stock_dir)?;
        stock
            .accept_transfer(valid, resolver)
            .map_err(|err| anyhow!("failed to accept RGB transfer: {err:?}"))?;
        stock
            .store()
            .map_err(|err| anyhow!("failed to persist receiver RGB stock: {err:?}"))?;
        Ok(())
    })
}

pub fn validate_rgb20_transfer_with_chain_source(
    network: Network,
    chain_source: &ChainSource,
    consignment: Transfer,
) -> Result<ValidTransfer> {
    let resolver = rgb_resolver_with_consignment(network, chain_source, &consignment, [])?;
    let validation = ValidationConfig {
        chain_net: network_to_rgb(network),
        trusted_typesystem: consignment.types.clone(),
        ..Default::default()
    };
    consignment
        .validate(&resolver, &validation)
        .map_err(|status| anyhow!("invalid RGB transfer consignment: {status:?}"))
}

pub fn export_rgb20_transfer_consignment(
    stock_dir: &Path,
    contract_id: ContractId,
    recipient_outpoint: OutPoint,
) -> Result<Transfer> {
    let stock = open_or_create_stock(stock_dir)?;
    stock
        .transfer(
            contract_id,
            [OutputSeal::with(
                txid_to_rgb(recipient_outpoint.txid),
                recipient_outpoint.vout,
            )],
            [],
            [],
            Some(txid_to_rgb(recipient_outpoint.txid)),
        )
        .map_err(|err| anyhow!("failed to export RGB transfer consignment: {err:?}"))
}

pub fn encode_rgb20_transfer_consignment(consignment: &Transfer) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    consignment
        .save(&mut bytes)
        .context("failed to encode RGB transfer consignment")?;
    Ok(bytes)
}

pub fn decode_rgb20_transfer_consignment(bytes: &[u8]) -> Result<Transfer> {
    Transfer::load(bytes).context("failed to decode RGB transfer consignment")
}

pub fn list_rgb20_assets_for_address(
    stock_dir: &Path,
    address: &str,
    utxos: impl IntoIterator<Item = Rgb20TrackedUtxo>,
) -> Result<Vec<Rgb20AssetAllocation>> {
    list_rgb20_assets_for_utxos(
        stock_dir,
        utxos
            .into_iter()
            .filter(|utxo| utxo.address.as_deref() == Some(address)),
    )
}

pub fn list_rgb20_contracts(stock_dir: &Path) -> Result<Vec<Rgb20ContractInfo>> {
    let stock = open_or_create_stock(stock_dir)?;
    let mut contracts = Vec::new();
    for contract in stock
        .contracts()
        .map_err(|err| anyhow!("failed to list RGB contracts: {err:?}"))?
    {
        let contract_data = stock
            .contract_data(contract.id)
            .map_err(|err| anyhow!("failed to load RGB contract data: {err:?}"))?;
        let spec = contract_data
            .global("spec")
            .next()
            .map(|strict_val| AssetSpec::from_strict_val_unchecked(&strict_val));
        let (ticker, name, precision) = spec
            .map(|spec| {
                (
                    spec.ticker.to_string(),
                    spec.name.to_string(),
                    spec.precision,
                )
            })
            .unwrap_or_else(|| (String::new(), String::new(), rgbstd::Precision::Indivisible));
        contracts.push(Rgb20ContractInfo {
            contract_id: contract.id,
            ticker,
            name,
            precision: precision.decimals(),
        });
    }
    Ok(contracts)
}

pub fn list_rgb20_assets_for_utxos(
    stock_dir: &Path,
    utxos: impl IntoIterator<Item = Rgb20TrackedUtxo>,
) -> Result<Vec<Rgb20AssetAllocation>> {
    let stock = open_or_create_stock(stock_dir)?;
    list_rgb20_assets_from_stock(&stock, utxos)
}

fn list_rgb20_assets_from_stock(
    stock: &Stock,
    utxos: impl IntoIterator<Item = Rgb20TrackedUtxo>,
) -> Result<Vec<Rgb20AssetAllocation>> {
    let utxos = utxos.into_iter().collect::<Vec<_>>();
    let rgb_to_wallet = utxos
        .iter()
        .map(|utxo| (outpoint_to_rgb(utxo.outpoint), utxo))
        .collect::<HashMap<_, _>>();
    let filter = rgb_to_wallet.keys().copied().collect::<HashSet<_>>();

    let mut assets = Vec::new();
    for contract in stock
        .contracts()
        .map_err(|err| anyhow!("failed to list RGB contracts: {err:?}"))?
    {
        let contract_data = stock
            .contract_data(contract.id)
            .map_err(|err| anyhow!("failed to load RGB contract data: {err:?}"))?;
        let Ok(allocations) = contract_data.fungible("assetOwner", &filter) else {
            continue;
        };
        let spec = contract_data
            .global("spec")
            .next()
            .map(|strict_val| AssetSpec::from_strict_val_unchecked(&strict_val));

        for allocation in allocations {
            let rgb_outpoint = allocation.seal.to_outpoint();
            let Some(utxo) = rgb_to_wallet.get(&rgb_outpoint) else {
                continue;
            };
            let amount_raw = allocation.state.value();
            let (ticker, name, precision) = spec
                .as_ref()
                .map(|spec| {
                    (
                        spec.ticker.to_string(),
                        spec.name.to_string(),
                        spec.precision,
                    )
                })
                .unwrap_or_else(|| (String::new(), String::new(), rgbstd::Precision::Indivisible));
            assets.push(Rgb20AssetAllocation {
                contract_id: contract.id,
                outpoint: utxo.outpoint,
                address: utxo.address.clone(),
                amount: allocation.state,
                amount_raw,
                amount_display: rgbstd::CoinAmount::new(allocation.state, precision).to_string(),
                amount_encoding: allocation.state.to_string(),
                ticker,
                name,
                precision: precision.decimals(),
                witness: allocation.witness,
                confirmed: utxo.confirmed,
            });
        }
    }

    Ok(assets)
}

pub fn stage_sender_fascia(stock_dir: &Path, txid: Txid, fascia: &Fascia) -> Result<()> {
    store_pending_rgb_operation(
        stock_dir,
        txid,
        "pending_sender_transfer",
        RgbPendingOperation::SenderFascia {
            txid: txid.to_string(),
            fascia: encode_fascia(fascia)?,
        },
    )
}

pub fn stage_receiver_transfer(stock_dir: &Path, txid: Txid, consignment: &Transfer) -> Result<()> {
    store_pending_rgb_operation(
        stock_dir,
        txid,
        "pending_receiver_transfer",
        RgbPendingOperation::ReceiverTransfer {
            txid: txid.to_string(),
            consignment: encode_rgb20_transfer_consignment(consignment)?,
        },
    )
}

pub fn scan_and_promote_confirmed_staged_rgb_stocks(
    stock_dir: &Path,
    network: Network,
    esplora_urls: &[String],
) -> Result<RgbPendingStockScanReport> {
    let store = LocalRgbStore::open(stock_dir)?;
    let mut report = RgbPendingStockScanReport::default();
    for (index, txid) in store.pending_txids()?.into_iter().enumerate() {
        let existing_status = store.pending_status(txid)?;
        if existing_status
            .as_ref()
            .is_some_and(|status| is_terminal_pending_stock_status(&status.status))
        {
            report.skipped += 1;
            continue;
        }
        report.scanned += 1;
        let Some(tx_confirmed) =
            fetch_tx_confirmation_consensus(network, esplora_urls, txid, index)?
        else {
            report.skipped += 1;
            continue;
        };
        if !tx_confirmed {
            let status = pending_stock_status(txid, "pending", store.logical_name(), None, None);
            store.put_pending_status(txid, &status)?;
            report.pending += 1;
            continue;
        }
        let chain_source = chain_source_from_esplora_urls(network, esplora_urls)?;
        replay_pending_rgb_operation(stock_dir, network, &chain_source, txid)?;
        let now = now();
        let status = pending_stock_status(
            txid,
            "confirmed",
            store.logical_name(),
            Some(now),
            Some(now),
        );
        store.put_pending_status(txid, &status)?;
        report.promoted += 1;
        report.promoted_txids.push(txid);
    }
    Ok(report)
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RgbSeal {
    Vout(u32),
    Blind(rgbstd::SecretSeal),
}

fn prepare_rgb20_psbt_inner(
    stock: &mut Stock,
    mut psbt: Psbt,
    change_vout: u32,
    rgb_assign: impl IntoIterator<Item = (ContractId, rgbstd::Amount, RgbSeal)>,
) -> Result<(Fascia, Psbt)> {
    anyhow::ensure!(
        psbt.unsigned_tx
            .output
            .get(change_vout as usize)
            .is_some_and(|output| output.value > Amount::ZERO),
        "RGB sender change output is missing or zero"
    );

    let rgb_assign = rgb_assign.into_iter().fold(
        HashMap::<ContractId, HashMap<RgbSeal, rgbstd::Amount>>::new(),
        |mut map, (contract_id, amount, seal)| {
            map.entry(contract_id)
                .or_default()
                .entry(seal)
                .and_modify(|existing| existing.saturating_add_assign(amount))
                .or_insert(amount);
            map
        },
    );

    let (opret_vout, _) = psbt
        .unsigned_tx
        .output
        .iter()
        .enumerate()
        .find(|(_, output)| output.script_pubkey.is_op_return())
        .context("RGB carrier transaction has no OP_RETURN output")?;
    psbt.unsigned_tx.output[opret_vout].script_pubkey = ScriptBuf::new_op_return([]);

    let mut rgb_psbt = psbt;
    let prev_outputs = rgb_psbt
        .unsigned_tx()
        .input
        .iter()
        .map(|input| input.previous_output)
        .collect::<Vec<_>>();
    let assignment_name = "assetOwner";
    let transfer_name = "transfer";
    let transfer_contract_ids = rgb_assign.keys().copied().collect::<HashSet<_>>();
    let mut all_transitions = HashMap::<ContractId, Vec<Transition>>::new();
    let mut blank_state = HashMap::<ContractId, Vec<(Opout, AllocatedState)>>::new();

    for (contract_id, assign) in rgb_assign {
        let mut total_input = rgbstd::Amount::ZERO;
        let mut builder = stock
            .transition_builder(contract_id, transfer_name)
            .map_err(|err| anyhow!("failed to create RGB transition builder: {err:?}"))?;

        for (_seal, opout_state_map) in stock
            .contract_assignments_for(contract_id, prev_outputs.clone())
            .map_err(|err| anyhow!("failed to load RGB input assignments: {err:?}"))?
        {
            for (opout, state) in opout_state_map {
                match state {
                    AllocatedState::Amount(value) => total_input += value.as_u64().into(),
                    _ => {
                        blank_state
                            .entry(contract_id)
                            .or_default()
                            .push((opout, state.clone()));
                        continue;
                    }
                }
                builder = builder
                    .add_input(opout, state)
                    .map_err(|err| anyhow!("failed to add RGB transition input: {err:?}"))?;
            }
        }

        let mut change_amount = total_input;
        for (seal, amount) in assign {
            anyhow::ensure!(change_amount >= amount, "insufficient RGB input amount");
            change_amount = change_amount.saturating_sub(amount);
            builder = builder
                .add_fungible_state(assignment_name, builder_seal(seal, contract_id), amount)
                .map_err(|err| anyhow!("failed to add RGB recipient state: {err:?}"))?;
        }
        if change_amount > rgbstd::Amount::ZERO {
            builder = builder
                .add_fungible_state(
                    assignment_name,
                    BuilderSeal::Revealed(rgbstd::GraphSeal::new_random_vout(change_vout)),
                    change_amount,
                )
                .map_err(|err| anyhow!("failed to add RGB change state: {err:?}"))?;
        }

        let transition = builder
            .complete_transition()
            .map_err(|err| anyhow!("failed to complete RGB transition: {err:?}"))?;
        all_transitions
            .entry(contract_id)
            .or_default()
            .push(transition.clone());
        rgb_psbt
            .push_rgb_transition(transition)
            .map_err(|err| anyhow!("failed to push RGB transition into PSBT: {err:?}"))?;
    }

    for utxo in prev_outputs {
        for contract_id in stock
            .contracts_assigning([utxo])
            .map_err(|err| anyhow!("failed to find RGB blank-state contracts: {err:?}"))?
        {
            if transfer_contract_ids.contains(&contract_id) {
                continue;
            }
            for (_seal, assignments) in stock
                .contract_assignments_for(contract_id, [utxo])
                .map_err(|err| anyhow!("failed to load RGB blank-state inputs: {err:?}"))?
            {
                blank_state
                    .entry(contract_id)
                    .or_default()
                    .extend(assignments);
            }
        }
    }

    for (contract_id, opouts) in blank_state {
        let schema = stock
            .as_stash_provider()
            .contract_schema(contract_id)
            .map_err(|err| anyhow!("failed to load RGB schema: {err:?}"))?;
        for (opout, state) in opouts {
            let transition_type = schema.default_transition_for_assignment(&opout.ty);
            let transition = stock
                .transition_builder_raw(contract_id, transition_type)
                .map_err(|err| anyhow!("failed to build RGB blank transition: {err:?}"))?
                .add_input(opout, state.clone())
                .map_err(|err| anyhow!("failed to add RGB blank input: {err:?}"))?
                .add_owned_state_raw(
                    opout.ty,
                    rgbstd::GraphSeal::new_random_vout(change_vout),
                    state,
                )
                .map_err(|err| anyhow!("failed to add RGB blank change: {err:?}"))?
                .complete_transition()
                .map_err(|err| anyhow!("failed to complete RGB blank transition: {err:?}"))?;
            all_transitions
                .entry(contract_id)
                .or_default()
                .push(transition.clone());
            rgb_psbt
                .push_rgb_transition(transition)
                .map_err(|err| anyhow!("failed to push RGB blank transition: {err:?}"))?;
        }
    }

    let (opret_vout, _) = rgb_psbt
        .unsigned_tx()
        .output
        .iter()
        .enumerate()
        .find(|(_, output)| output.script_pubkey.is_op_return())
        .context("RGB PSBT has no OP_RETURN output")?;
    rgb_psbt.outputs[opret_vout].set_opret_host();

    for (contract_id, transitions) in &all_transitions {
        for transition in transitions {
            for opout in transition.inputs() {
                rgb_psbt
                    .set_rgb_contract_consumer(*contract_id, opout, transition.id())
                    .map_err(|err| anyhow!("failed to set RGB contract consumer: {err:?}"))?;
            }
        }
    }

    rgb_psbt.set_rgb_close_method(CloseMethod::OpretFirst);
    let fascia = rgb_psbt
        .rgb_commit()
        .map_err(|err| anyhow!("failed to commit RGB PSBT: {err:?}"))?;
    Ok((fascia, rgb_psbt))
}

fn builder_seal(seal: RgbSeal, contract_id: ContractId) -> BuilderSeal<rgbstd::GraphSeal> {
    match seal {
        RgbSeal::Blind(seal) => BuilderSeal::Concealed(seal),
        RgbSeal::Vout(vout) => {
            let mut hasher = std::hash::DefaultHasher::new();
            contract_id.hash(&mut hasher);
            BuilderSeal::Revealed(rgbstd::GraphSeal::with_blinded_vout(vout, hasher.finish()))
        }
    }
}

fn rgb_resolver(
    network: Network,
    chain_source: &ChainSource,
    local_txs: impl IntoIterator<Item = Transaction>,
) -> Result<LocalWitnessResolver> {
    let resolver = chain_source_resolver(chain_source)?;
    let resolver = LocalWitnessResolver::new(resolver, local_txs);
    resolver
        .check_chain_net(network_to_rgb(network))
        .map_err(|err| anyhow!("RGB witness resolver chain check failed: {err:?}"))?;
    Ok(resolver)
}

fn rgb_resolver_with_consignment<const TYPE: bool>(
    network: Network,
    chain_source: &ChainSource,
    consignment: &Consignment<TYPE>,
    local_txs: impl IntoIterator<Item = Transaction>,
) -> Result<LocalWitnessResolver> {
    let mut resolver = chain_source_resolver(chain_source)?;
    resolver.add_consignment_txes(consignment);
    let resolver = LocalWitnessResolver::new(resolver, local_txs);
    resolver
        .check_chain_net(network_to_rgb(network))
        .map_err(|err| anyhow!("RGB witness resolver chain check failed: {err:?}"))?;
    Ok(resolver)
}

fn esplora_builder(config: &EsploraConfig) -> esplora_client::Builder {
    let mut builder = esplora_client::Builder::new(&config.url).timeout(10);
    if let Some(api_key) = config.api_key.as_deref().filter(|value| !value.is_empty()) {
        builder = builder.header("api-key", api_key);
    }
    builder
}

fn open_or_create_stock(stock_dir: &Path) -> Result<Stock> {
    let store = LocalRgbStore::open(stock_dir)?;
    let provider = store.rgb_stock_store()?;
    let stock_is_empty = !store.rgb_stock_has_data()?;

    match Stock::load(provider.clone(), true) {
        Ok(stock) => Ok(stock),
        Err(_err) if stock_is_empty => {
            let mut stock = Stock::in_memory();
            stock
                .make_persistent(provider, true)
                .map_err(|err| anyhow!("failed to initialize RGB stock store: {err:?}"))?;
            Ok(stock)
        }
        Err(err) => Err(anyhow!("failed to load RGB stock store: {err:?}")),
    }
}

fn with_rgb_stock_write_lock<T>(stock_dir: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    LocalRgbStore::open(stock_dir)?.with_rgb_stock_write_lock(f)
}

fn transfer_opids(fascia: &Fascia, contract_id: ContractId) -> Vec<rgbstd::OpId> {
    fascia
        .clone()
        .into_bundles()
        .into_iter()
        .find_map(|(id, bundle)| (id == contract_id).then(|| bundle.known_transitions_opids()))
        .map(|set| set.into_iter().collect())
        .unwrap_or_default()
}

fn store_pending_rgb_operation(
    stock_dir: &Path,
    txid: Txid,
    status: &str,
    op: RgbPendingOperation,
) -> Result<()> {
    let bytes = serde_json::to_vec(&op).context("encode RGB pending operation")?;
    let store = LocalRgbStore::open(stock_dir)?;
    let status = pending_stock_status(txid, status, store.logical_name(), None, None);
    store.put_pending_operation(txid, &bytes, &status)
}

fn replay_pending_rgb_operation(
    stock_dir: &Path,
    network: Network,
    chain_source: &ChainSource,
    txid: Txid,
) -> Result<()> {
    let store = LocalRgbStore::open(stock_dir)?;
    let bytes = store
        .get_pending_op(txid)?
        .with_context(|| format!("RGB pending operation not found for {txid}"))?;
    let op: RgbPendingOperation =
        serde_json::from_slice(&bytes).context("decode RGB pending operation")?;
    match op {
        RgbPendingOperation::SenderFascia { fascia, .. } => {
            let fascia = decode_fascia(&fascia)?;
            with_rgb_stock_write_lock(stock_dir, || {
                let mut stock = open_or_create_stock(stock_dir)?;
                stock
                    .consume_fascia(fascia, TentativeWitnessOrd)
                    .map_err(|err| anyhow!("failed to replay sender RGB fascia: {err:?}"))?;
                stock
                    .store()
                    .map_err(|err| anyhow!("failed to persist replayed sender RGB stock: {err:?}"))
            })?;
        }
        RgbPendingOperation::ReceiverTransfer { consignment, .. } => {
            accept_rgb20_transfer_with_chain_source(
                stock_dir,
                network,
                chain_source,
                decode_rgb20_transfer_consignment(&consignment)?,
            )?;
        }
    }
    store.remove_pending_op(txid)?;
    Ok(())
}

fn chain_source_from_esplora_urls(
    network: Network,
    esplora_urls: &[String],
) -> Result<ChainSource> {
    let url = normalized_esplora_urls(esplora_urls)
        .into_iter()
        .next()
        .with_context(|| format!("no chain source URL configured for {network:?}"))?;
    Ok(chain_source_from_url(url))
}

pub fn chain_source_from_url(url: String) -> ChainSource {
    if is_electrum_url(&url) {
        ChainSource::Electrum(ElectrumConfig::new(url))
    } else {
        ChainSource::Esplora(EsploraConfig::new(url))
    }
}

pub fn is_electrum_url(url: &str) -> bool {
    let url = url.trim();
    url.starts_with("electrum://") || url.starts_with("tcp://") || url.starts_with("ssl://")
}

pub fn normalize_electrum_url(url: &str) -> String {
    url.trim()
        .strip_prefix("electrum://")
        .map(|rest| format!("tcp://{rest}"))
        .unwrap_or_else(|| url.trim().to_string())
}

fn normalized_esplora_urls(esplora_urls: &[String]) -> Vec<String> {
    esplora_urls
        .iter()
        .map(|url| url.trim())
        .filter(|url| !url.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn rotate_esplora_urls(esplora_urls: &[String], start: usize) -> Vec<String> {
    let urls = normalized_esplora_urls(esplora_urls);
    if urls.is_empty() {
        return urls;
    }
    (0..urls.len())
        .map(|offset| urls[(start + offset) % urls.len()].clone())
        .collect()
}

const RGB_TX_STATUS_CONFIRMED_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const RGB_TX_STATUS_UNCONFIRMED_CACHE_TTL: Duration = Duration::from_secs(60);
const RGB_TX_STATUS_ERROR_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct TxConfirmationCacheEntry {
    fetched_at: Instant,
    result: std::result::Result<Option<bool>, String>,
}

fn fetch_tx_confirmation_consensus(
    network: Network,
    esplora_urls: &[String],
    txid: Txid,
    start: usize,
) -> Result<Option<bool>> {
    let cache_key = (network, txid);
    if let Some(cached) = cached_tx_confirmation(&cache_key) {
        return cached.map_err(anyhow::Error::msg);
    }
    let fetched = fetch_tx_confirmation_consensus_uncached(esplora_urls, txid, start)
        .map_err(|err| format!("{err:#}"));
    cache_tx_confirmation(cache_key, fetched.clone());
    fetched.map_err(anyhow::Error::msg)
}

fn cached_tx_confirmation(
    cache_key: &(Network, Txid),
) -> Option<std::result::Result<Option<bool>, String>> {
    let cache = TX_CONFIRMATION_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().expect("RGB tx confirmation cache poisoned");
    let Some(entry) = cache.get(cache_key) else {
        return None;
    };
    let ttl = match entry.result {
        Ok(Some(true)) => RGB_TX_STATUS_CONFIRMED_CACHE_TTL,
        Ok(Some(false)) | Ok(None) => RGB_TX_STATUS_UNCONFIRMED_CACHE_TTL,
        Err(_) => RGB_TX_STATUS_ERROR_CACHE_TTL,
    };
    if entry.fetched_at.elapsed() <= ttl {
        return Some(entry.result.clone());
    }
    cache.remove(cache_key);
    None
}

fn cache_tx_confirmation(
    cache_key: (Network, Txid),
    result: std::result::Result<Option<bool>, String>,
) {
    let cache = TX_CONFIRMATION_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    cache
        .lock()
        .expect("RGB tx confirmation cache poisoned")
        .insert(
            cache_key,
            TxConfirmationCacheEntry {
                fetched_at: Instant::now(),
                result,
            },
        );
}

fn fetch_tx_confirmation_consensus_uncached(
    esplora_urls: &[String],
    txid: Txid,
    start: usize,
) -> Result<Option<bool>> {
    let rotated_urls = rotate_esplora_urls(esplora_urls, start);
    anyhow::ensure!(
        !rotated_urls.is_empty(),
        "no Esplora URL configured for RGB carrier tx status"
    );
    let mut saw_confirmed = false;
    let mut last_error = None;
    for url in rotated_urls {
        if is_electrum_url(&url) {
            match fetch_tx_confirmation_electrum(&url, txid) {
                Ok(Some(true)) => saw_confirmed = true,
                Ok(other) => return Ok(other),
                Err(err) => last_error = Some(err),
            }
        } else {
            let client = esplora_client::Builder::new(&url)
                .timeout(10)
                .build_blocking();
            match client.get_tx_status(&txid) {
                Ok(status) if status.confirmed => saw_confirmed = true,
                Ok(_) => return Ok(Some(false)),
                Err(err) => last_error = Some(anyhow!("{err:?}")),
            }
        }
    }
    if saw_confirmed {
        return Ok(Some(true));
    }
    if let Some(err) = last_error {
        return Err(err).with_context(|| format!("failed to fetch RGB carrier tx status: {txid}"));
    }
    Ok(None)
}

fn chain_source_resolver(chain_source: &ChainSource) -> Result<AnyResolver> {
    match chain_source {
        ChainSource::Esplora(config) => AnyResolver::esplora_blocking(esplora_builder(config))
            .map_err(|err| anyhow!("failed to create RGB Esplora resolver: {err}")),
        ChainSource::Electrum(config) => AnyResolver::electrum_blocking(&config.url, None)
            .map_err(|err| anyhow!("failed to create RGB Electrum resolver: {err}")),
    }
}

fn fetch_tx_confirmation_electrum(url: &str, txid: Txid) -> Result<Option<bool>> {
    let source = ChainSource::Electrum(ElectrumConfig::new(url.to_string()));
    let resolver = chain_source_resolver(&source)?;
    match resolver
        .resolve_witness(txid)
        .map_err(|err| anyhow!("failed to resolve Electrum witness {txid}: {err:?}"))?
    {
        WitnessStatus::Unresolved => Ok(None),
        WitnessStatus::Resolved(_, WitnessOrd::Mined(_)) => Ok(Some(true)),
        WitnessStatus::Resolved(_, WitnessOrd::Tentative) => Ok(Some(false)),
        WitnessStatus::Resolved(_, WitnessOrd::Ignored | WitnessOrd::Archived) => Ok(None),
    }
}

fn encode_fascia(fascia: &Fascia) -> Result<Vec<u8>> {
    Ok(fascia
        .to_strict_serialized::<U32MAX>()
        .context("encode RGB fascia")?
        .release())
}

fn decode_fascia(bytes: &[u8]) -> Result<Fascia> {
    let confined = Confined::try_from(bytes.to_vec()).context("confine RGB fascia bytes")?;
    Fascia::from_strict_serialized::<U32MAX>(confined).context("decode RGB fascia")
}

pub fn encode_fascia_bytes(fascia: &Fascia) -> Result<Vec<u8>> {
    encode_fascia(fascia)
}

pub fn decode_fascia_bytes(bytes: &[u8]) -> Result<Fascia> {
    decode_fascia(bytes)
}

fn pending_rgb_stock_root(stock_dir: &Path) -> PathBuf {
    let stock_name = stock_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("stock");
    stock_dir
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{stock_name}_pending"))
}

fn is_terminal_pending_stock_status(status: &str) -> bool {
    matches!(status, "confirmed" | "invalid")
}

fn pending_stock_status(
    txid: Txid,
    status: &str,
    logical_store: &str,
    confirmed_at: Option<u64>,
    promoted_at: Option<u64>,
) -> RgbPendingStockStatus {
    RgbPendingStockStatus {
        txid: txid.to_string(),
        status: status.to_string(),
        main_stock_dir: logical_store.to_string(),
        staged_stock_dir: format!("{logical_store}:pending:{txid}"),
        updated_at: now(),
        confirmed_at,
        promoted_at,
    }
}

fn legacy_pending_stock_status(
    stock_dir: &Path,
    txid: Txid,
) -> Result<Option<RgbPendingStockStatus>> {
    let path = pending_rgb_stock_root(stock_dir)
        .join(txid.to_string())
        .join("pending-status.json");
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(
        serde_json::from_slice(&fs::read(&path)?)
            .with_context(|| format!("read legacy RGB pending status {}", path.display()))?,
    ))
}

fn outpoint_to_rgb(outpoint: OutPoint) -> rgbstd::Outpoint {
    rgbstd::Outpoint::from_str(&outpoint.to_string())
        .expect("bitcoin outpoint is valid RGB outpoint")
}

fn txid_to_rgb(txid: Txid) -> RgbTxid {
    RgbTxid::from_str(&txid.to_string()).expect("bitcoin txid is valid RGB txid")
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn network_to_rgb(network: Network) -> rgbstd::ChainNet {
    match network {
        Network::Bitcoin => rgbstd::ChainNet::BitcoinMainnet,
        Network::Testnet => rgbstd::ChainNet::BitcoinTestnet3,
        Network::Testnet4 => rgbstd::ChainNet::BitcoinTestnet4,
        Network::Signet => rgbstd::ChainNet::BitcoinSignet,
        Network::Regtest => rgbstd::ChainNet::BitcoinRegtest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    fn temp_path(label: &str) -> PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rgb-service-local-{label}-{suffix}"))
    }

    fn initialize_empty_stock(store: &LocalRgbStore) {
        let provider = store.rgb_stock_store().unwrap();
        let mut stock = Stock::in_memory();
        stock.make_persistent(provider, true).unwrap();
        stock.store().unwrap();
    }

    #[test]
    fn shared_database_isolates_account_stocks_and_pending_state() {
        let database_dir = temp_path("shared");
        let alice_locator = shared_rgb_store_locator(&database_dir, "alice");
        let bob_locator = shared_rgb_store_locator(&database_dir, "bob");
        let alice = LocalRgbStore::open(&alice_locator).unwrap();
        let bob = LocalRgbStore::open(&bob_locator).unwrap();
        initialize_empty_stock(&alice);
        initialize_empty_stock(&bob);

        let txid = Txid::from_byte_array([7; 32]);
        let status = pending_stock_status(txid, "pending", alice.logical_name(), None, None);
        alice
            .put_pending_operation(txid, b"alice-pending", &status)
            .unwrap();

        assert!(alice.rgb_stock_has_data().unwrap());
        assert!(bob.rgb_stock_has_data().unwrap());
        assert_eq!(
            alice.get_pending_op(txid).unwrap().unwrap(),
            b"alice-pending"
        );
        assert!(bob.get_pending_op(txid).unwrap().is_none());
        assert_eq!(
            shared_rgb_stock_account_ids(&alice.inner.db).unwrap(),
            BTreeSet::from(["alice".to_string(), "bob".to_string()])
        );
        assert_eq!(
            shared_rgb_pending_account_ids(&alice.inner.db).unwrap(),
            BTreeSet::from(["alice".to_string()])
        );
        assert!(!database_dir.join(SHARED_RGB_ACCOUNT_MARKER).exists());
    }

    #[test]
    fn per_account_store_migration_is_idempotent() {
        let source_dir = temp_path("source");
        let source = LocalRgbStore::open(&source_dir).unwrap();
        initialize_empty_stock(&source);
        let txid = Txid::from_byte_array([9; 32]);
        let status = pending_stock_status(txid, "pending", source.logical_name(), None, None);
        source
            .put_pending_operation(txid, b"pending", &status)
            .unwrap();

        let database_dir = temp_path("target");
        let target_locator = shared_rgb_store_locator(&database_dir, "account-1");
        let first = migrate_rgb_account_store(&source_dir, &target_locator).unwrap();
        assert!(first.stock_migrated);
        assert_eq!(first.pending_migrated, 1);
        let target = LocalRgbStore::open(&target_locator).unwrap();
        assert!(target.rgb_stock_has_data().unwrap());
        assert_eq!(target.get_pending_op(txid).unwrap().unwrap(), b"pending");

        let second = migrate_rgb_account_store(&source_dir, &target_locator).unwrap();
        assert_eq!(second, RgbAccountStoreMigrationReport::default());
    }
    #[test]
    fn external_blind_assignment_preserves_value_and_rejects_overspend() {
        use rgbstd::rgbcore::commit_verify::Conceal;
        struct Resolver(Transaction);
        impl ResolveWitness for Resolver {
            fn check_chain_net(&self, _: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
                Ok(())
            }
            fn resolve_witness(&self, id: RgbTxid) -> Result<WitnessStatus, WitnessResolverError> {
                Ok(if id == self.0.compute_txid() {
                    WitnessStatus::Resolved(self.0.clone(), WitnessOrd::Tentative)
                } else {
                    WitnessStatus::Unresolved
                })
            }
        }
        let funding = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![],
            output: vec![bitcoin::TxOut {
                value: Amount::from_sat(5000),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let outpoint = OutPoint {
            txid: funding.compute_txid(),
            vout: 0,
        };
        let contract = ContractBuilder::with(
            Identity::default(),
            NonInflatableAsset::schema(),
            NonInflatableAsset::types(),
            NonInflatableAsset::scripts(),
            network_to_rgb(Network::Signet),
        )
        .add_global_state(
            "spec",
            AssetSpec {
                ticker: "BLIND".try_into().unwrap(),
                name: "Blind fixture".try_into().unwrap(),
                details: None,
                precision: rgbstd::Precision::Indivisible,
            },
        )
        .unwrap()
        .add_global_state(
            "terms",
            ContractTerms {
                text: Default::default(),
                media: None,
            },
        )
        .unwrap()
        .add_global_state("issuedSupply", rgbstd::Amount::from(10u64))
        .unwrap()
        .add_fungible_state(
            "assetOwner",
            BuilderSeal::Revealed(GenesisSeal::new_random(outpoint.txid, outpoint.vout)),
            10u64,
        )
        .unwrap()
        .issue_contract()
        .unwrap();
        let id = contract.contract_id();
        let mut stock = Stock::in_memory();
        stock.import_contract(contract, Resolver(funding)).unwrap();
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: outpoint,
                script_sig: ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![
                bitcoin::TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::new_op_return([]),
                },
                bitcoin::TxOut {
                    value: Amount::from_sat(4500),
                    script_pubkey: ScriptBuf::new(),
                },
            ],
        };
        let psbt = Psbt::from_unsigned_tx(tx).unwrap();
        let secret = rgbstd::GraphSeal::new_random(Txid::from_byte_array([4; 32]), 0u32).conceal();
        let error = prepare_rgb20_psbt_inner(
            &mut stock,
            psbt.clone(),
            1,
            [(id, 11u64.into(), RgbSeal::Blind(secret))],
        )
        .unwrap_err();
        assert!(error.to_string().contains("insufficient RGB input amount"));
        let (fascia, psbt) = prepare_rgb20_psbt_inner(
            &mut stock,
            psbt,
            1,
            [(id, 4u64.into(), RgbSeal::Blind(secret))],
        )
        .unwrap();
        let txid = psbt.unsigned_tx.compute_txid();
        let opids = transfer_opids(&fascia, id);
        stock.consume_fascia(fascia, TentativeWitnessOrd).unwrap();
        let proof = stock.transfer(id, [], [secret], opids, Some(txid)).unwrap();
        assert!(proof.terminals.values().any(|s| s.contains(&secret)));
        let allocations = list_rgb20_assets_from_stock(
            &stock,
            [Rgb20TrackedUtxo {
                outpoint: OutPoint { txid, vout: 1 },
                address: None,
                confirmed: false,
            }],
        )
        .unwrap();
        assert_eq!(allocations.iter().map(|a| a.amount_raw).sum::<u64>(), 6);
        let encoded = encode_rgb20_transfer_consignment(&proof).unwrap();
        assert_eq!(
            decode_rgb20_transfer_consignment(&encoded)
                .unwrap()
                .contract_id(),
            id
        );
    }
}
