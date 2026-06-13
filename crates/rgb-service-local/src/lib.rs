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
use anyhow::{Context, Result, anyhow};
use bitcoin::{Amount, Network, OutPoint, Psbt, ScriptBuf, Transaction, Txid};
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};
use nonasync::persistence::CloneNoPersistence;
use psrgbt::{RgbOutExt, RgbPsbtExt};
use rgb_schemata::NonInflatableAsset;
use rgbstd::{
    ContractId, GenesisSeal, Identity, Operation, Opout, OutputSeal, Transition, Txid as RgbTxid,
    containers::{
        BuilderSeal, Consignment, ConsignmentExt, Fascia, FileContent, Transfer, ValidTransfer,
    },
    contract::{AllocatedState, ContractBuilder, IssuerWrapper},
    indexers::{AnyResolver, esplora_blocking::esplora_client},
    persistence::{StashReadProvider, Stock, fjall::FjallBinStore},
    stl::{AssetSpec, ContractTerms, Name, Ticker},
    txout::CloseMethod,
    validation::{
        ResolveWitness, ValidationConfig, WitnessOrdProvider, WitnessResolverError, WitnessStatus,
    },
    vm::WitnessOrd,
};
use serde::{Deserialize, Serialize};
use strict_types::{StrictDeserialize, StrictSerialize};

pub use rgbstd;

const RGB_STOCK_PARTITION: &str = "rgb_stock";
const RGB_PENDING_OPS_PARTITION: &str = "rgb_pending_ops";

static LOCAL_STORES: OnceLock<Mutex<HashMap<PathBuf, Arc<LocalRgbStoreInner>>>> = OnceLock::new();
static TX_CONFIRMATION_CACHE: OnceLock<Mutex<HashMap<(Network, Txid), TxConfirmationCacheEntry>>> =
    OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChainSource {
    Esplora(EsploraConfig),
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

#[derive(Clone)]
pub struct LocalRgbStore {
    inner: Arc<LocalRgbStoreInner>,
}

struct LocalRgbStoreInner {
    path: PathBuf,
    db: SingleWriterTxDatabase,
    rgb_stock_lock: RwLock<()>,
}

impl LocalRgbStore {
    pub fn open(store_dir: &Path) -> Result<Self> {
        fs::create_dir_all(store_dir)
            .with_context(|| format!("create local RGB store {}", store_dir.display()))?;
        let path = fs::canonicalize(store_dir)
            .with_context(|| format!("canonicalize local RGB store {}", store_dir.display()))?;
        let mut stores = LOCAL_STORES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|err| anyhow!("local RGB store registry lock poisoned: {err}"))?;
        if let Some(existing) = stores.get(&path) {
            return Ok(Self {
                inner: Arc::clone(existing),
            });
        }
        let db = SingleWriterTxDatabase::builder(&path)
            .open()
            .with_context(|| format!("open local RGB store {}", path.display()))?;
        let inner = Arc::new(LocalRgbStoreInner {
            path: path.clone(),
            db,
            rgb_stock_lock: RwLock::new(()),
        });
        stores.insert(path, Arc::clone(&inner));
        Ok(Self { inner })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn rgb_stock_store(&self) -> Result<FjallBinStore> {
        Ok(FjallBinStore::with_database(
            self.inner.path.clone(),
            self.inner.db.clone(),
            RGB_STOCK_PARTITION,
        )
        .context("open RGB stock partition")?)
    }

    pub fn rgb_stock_has_data(&self) -> Result<bool> {
        self.rgb_stock_store()?
            .has_data()
            .map_err(|err| anyhow!("check RGB stock partition: {err:?}"))
    }

    pub fn with_rgb_stock_write_lock<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let _guard = self
            .inner
            .rgb_stock_lock
            .write()
            .map_err(|err| anyhow!("local RGB stock write lock poisoned: {err}"))?;
        f()
    }

    pub fn get_pending_op(&self, txid: impl std::fmt::Display) -> Result<Option<Vec<u8>>> {
        let keyspace = self
            .inner
            .db
            .keyspace(RGB_PENDING_OPS_PARTITION, KeyspaceCreateOptions::default)
            .context("open RGB pending ops partition")?;
        Ok(keyspace
            .get(txid.to_string().as_bytes())
            .with_context(|| format!("read RGB pending op {txid}"))?
            .map(|bytes| bytes.as_ref().to_vec()))
    }

    pub fn put_pending_op(&self, txid: impl std::fmt::Display, op: &[u8]) -> Result<()> {
        let keyspace = self
            .inner
            .db
            .keyspace(RGB_PENDING_OPS_PARTITION, KeyspaceCreateOptions::default)
            .context("open RGB pending ops partition")?;
        let mut tx = self.inner.db.write_tx();
        tx.insert(&keyspace, txid.to_string().as_bytes(), op);
        tx.commit()
            .with_context(|| format!("commit RGB pending op {txid}"))?;
        self.persist()
    }

    pub fn remove_pending_op(&self, txid: impl std::fmt::Display) -> Result<()> {
        let keyspace = self
            .inner
            .db
            .keyspace(RGB_PENDING_OPS_PARTITION, KeyspaceCreateOptions::default)
            .context("open RGB pending ops partition")?;
        let mut tx = self.inner.db.write_tx();
        tx.remove(&keyspace, txid.to_string().as_bytes());
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
        .fold(BTreeMap::<_, Vec<rgbstd::Amount>>::new(), |mut map, allocation| {
            map.entry(allocation.seal)
                .or_default()
                .push(allocation.state);
            map
        })
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
            .unwrap_or_else(|| {
                (
                    String::new(),
                    String::new(),
                    rgbstd::Precision::Indivisible,
                )
            });
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
                .unwrap_or_else(|| {
                    (
                        String::new(),
                        String::new(),
                        rgbstd::Precision::Indivisible,
                    )
                });
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

pub fn stage_sender_fascia(stock_dir: &Path, txid: Txid, fascia: &Fascia) -> Result<PathBuf> {
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

pub fn stage_receiver_transfer(
    stock_dir: &Path,
    txid: Txid,
    consignment: &Transfer,
) -> Result<PathBuf> {
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
    let pending_root = pending_rgb_stock_root(stock_dir);
    let mut report = RgbPendingStockScanReport::default();
    let Ok(entries) = fs::read_dir(&pending_root) else {
        return Ok(report);
    };
    let mut staged_dirs = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            entry
                .file_type()
                .ok()
                .filter(|file_type| file_type.is_dir())
                .map(|_| entry.path())
        })
        .collect::<Vec<_>>();
    staged_dirs.sort();

    for (index, staged_stock_dir) in staged_dirs.into_iter().enumerate() {
        let Some(txid) = staged_stock_dir
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<Txid>().ok())
        else {
            report.skipped += 1;
            continue;
        };
        let status_path = pending_stock_status_path(&staged_stock_dir);
        let existing_status = read_pending_stock_status(&status_path).ok();
        if existing_status
            .as_ref()
            .is_some_and(|status| is_terminal_pending_stock_status(&status.status))
        {
            report.skipped += 1;
            continue;
        }
        if LocalRgbStore::open(stock_dir)
            .and_then(|store| Ok(store.get_pending_op(txid)?.is_some()))
            .unwrap_or(false)
            == false
        {
            write_pending_stock_status(
                &status_path,
                pending_stock_status(txid, "invalid", stock_dir, &staged_stock_dir, None, None),
            )?;
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
            write_pending_stock_status(
                &status_path,
                pending_stock_status(txid, "pending", stock_dir, &staged_stock_dir, None, None),
            )?;
            report.pending += 1;
            continue;
        }
        let chain_source = chain_source_from_esplora_urls(network, esplora_urls)?;
        replay_pending_rgb_operation(stock_dir, network, &chain_source, txid)?;
        let now = now();
        write_pending_stock_status(
            &status_path,
            pending_stock_status(
                txid,
                "confirmed",
                stock_dir,
                &staged_stock_dir,
                Some(now),
                Some(now),
            ),
        )?;
        report.promoted += 1;
        report.promoted_txids.push(txid);
    }
    Ok(report)
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RgbSeal {
    Vout(u32),
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
    let ChainSource::Esplora(config) = chain_source;
    let resolver = AnyResolver::esplora_blocking(esplora_builder(config))
        .map_err(|err| anyhow!("failed to create RGB Esplora resolver: {err}"))?;
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
    let ChainSource::Esplora(config) = chain_source;
    let mut resolver = AnyResolver::esplora_blocking(esplora_builder(config))
        .map_err(|err| anyhow!("failed to create RGB Esplora resolver: {err}"))?;
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
) -> Result<PathBuf> {
    let staged_stock_dir = staged_rgb_stock_dir(stock_dir, txid);
    fs::create_dir_all(&staged_stock_dir).with_context(|| {
        format!(
            "create staged RGB operation marker {}",
            staged_stock_dir.display()
        )
    })?;
    let bytes = serde_json::to_vec(&op).context("encode RGB pending operation")?;
    LocalRgbStore::open(stock_dir)?.put_pending_op(txid, &bytes)?;
    write_pending_stock_status(
        &pending_stock_status_path(&staged_stock_dir),
        pending_stock_status(txid, status, stock_dir, &staged_stock_dir, None, None),
    )?;
    Ok(staged_stock_dir)
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

fn chain_source_from_esplora_urls(network: Network, esplora_urls: &[String]) -> Result<ChainSource> {
    let url = normalized_esplora_urls(esplora_urls)
        .into_iter()
        .next()
        .with_context(|| format!("no Esplora URL configured for {network:?}"))?;
    Ok(ChainSource::Esplora(EsploraConfig::new(url)))
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
        let client = esplora_client::Builder::new(&url)
            .timeout(10)
            .build_blocking();
        match client.get_tx_status(&txid) {
            Ok(status) if status.confirmed => saw_confirmed = true,
            Ok(_) => return Ok(Some(false)),
            Err(err) => last_error = Some(anyhow!("{err:?}")),
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

fn staged_rgb_stock_dir(stock_dir: &Path, txid: Txid) -> PathBuf {
    pending_rgb_stock_root(stock_dir).join(txid.to_string())
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

fn pending_stock_status_path(staged_stock_dir: &Path) -> PathBuf {
    staged_stock_dir.join("pending-status.json")
}

fn is_terminal_pending_stock_status(status: &str) -> bool {
    matches!(status, "confirmed" | "invalid")
}

fn pending_stock_status(
    txid: Txid,
    status: &str,
    stock_dir: &Path,
    staged_stock_dir: &Path,
    confirmed_at: Option<u64>,
    promoted_at: Option<u64>,
) -> RgbPendingStockStatus {
    RgbPendingStockStatus {
        txid: txid.to_string(),
        status: status.to_string(),
        main_stock_dir: stock_dir.display().to_string(),
        staged_stock_dir: staged_stock_dir.display().to_string(),
        updated_at: now(),
        confirmed_at,
        promoted_at,
    }
}

fn read_pending_stock_status(path: &Path) -> Result<RgbPendingStockStatus> {
    serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("read RGB pending stock status {}", path.display()))
}

fn write_pending_stock_status(path: &Path, status: RgbPendingStockStatus) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(&status)?)
        .with_context(|| format!("write RGB pending stock status {}", path.display()))
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
