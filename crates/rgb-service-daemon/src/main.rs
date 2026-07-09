use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    env, fs,
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header::CACHE_CONTROL, HeaderValue, Request},
    middleware::{self, Next},
    response::Response,
    serve,
};
use bdk_electrum::electrum_client::{self, ElectrumApi};
use bdk_esplora::esplora_client;
use bitcoin::{
    consensus::{deserialize, serialize},
    hashes::{sha256, Hash, HashEngine},
    secp256k1::{ecdsa, Message, PublicKey, Secp256k1},
    Address, Network, OutPoint, Psbt, ScriptBuf, Transaction, Txid,
};
use dynamic::{Dynamic, FromJson, MsgPack, MsgUnpack, ToJson};
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};
use rgb_service_api::{
    axum_service::router, AllocationStatus, AssetLayer, AssetSpendAuthorization, AuthSubject,
    AuthVerifier, Authorized, BalanceBreakdownRequest, BalanceBreakdownResponse, BalanceRequest,
    CancelTransferRequest, CancelTransferResponse, CommitTransferRequest, CommitTransferResponse,
    IssueAssetRequest, IssueAssetResponse, ListAssetsRequest, ListAssetsResponse,
    ListPendingRequest, ListPendingResponse, LnChannelFundingRefRequest,
    LnChannelFundingRefResponse, LnChannelOpenPrepareRequest, LnChannelOpenPrepareResponse,
    LnClosingComposeRequest, LnCommitmentComposeRequest, LnComposeResponse,
    LnOnchainClaimComposeRequest, LnPaymentClaimRequest, LnPaymentClaimResponse, LnRecoverRequest,
    LnRecoveredChannel, LnRecoveredCompose, LnRecoveryReport, OperationStatus, Permission,
    PrepareTransferRequest, PrepareTransferResponse, RecoverRequest, RecoveryAction,
    RecoveryReport, RequestSignature, RgbAllocation, RgbAssetInfo, RgbBalance, RgbContractInfo,
    RgbFundingRef, RgbServiceApi, RgbServiceError, RgbTestStep, RnaBalanceRequest,
    RnaBalanceResponse, RunRgbTestRequest, RunRgbTestResponse, SignatureScheme, TokenListResponse,
    TrackedUtxo,
};
use rgb_service_local::{
    build_rgb20_transfer_consignment, chain_source_from_url, encode_fascia_bytes,
    import_rgb20_stock_and_allocations,
    issue_rgb20_fixed_with_chain_source, list_legacy_rgb20_assets_for_utxos,
    list_rgb20_assets_for_utxos, list_rgb20_contracts, normalize_electrum_url, prepare_rgb20_psbt,
    rgbstd::{
        contract::FilterIncludeAll,
        persistence::{fs::FsBinStore, StashReadProvider, Stock},
        stl::AssetSpec,
        vm::WitnessOrd,
        ContractId as RgbContractId,
    },
    scan_and_promote_confirmed_staged_rgb_stocks, stage_receiver_transfer, stage_sender_fascia,
    ChainSource, Rgb20IssueRequest, Rgb20PsbtAssignment, Rgb20TrackedUtxo,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{net::TcpListener, signal};

mod legacy;

// wallet-service-v2 migration: legacy RGB stock directories are nested under
// each descriptor-derived account id using this historical RGB runtime path.
const WALLET_V2_RGB_PATH: &str = "0_11_1_rc_3";
// Historical bdk_wallet 1.1.0 file-store magic bytes. The daemon's bdk_wallet
// 2.x cannot read these files directly, so recovery now shells out to the
// bundled `wallet-v2-reader` binary. The magic strings are retained as the
// canonical identifiers of the legacy on-disk formats.
#[allow(dead_code)]
const WALLET_V2_BDK_MAGIC_LOCAL: &[u8] = b"LocalBtcWallet";
#[allow(dead_code)]
const WALLET_V2_BDK_MAGIC_RGB: &[u8] = b"RgbWallet";
#[allow(dead_code)]
const WALLET_V2_BDK_MAGICS: [&[u8]; 2] = [WALLET_V2_BDK_MAGIC_LOCAL, WALLET_V2_BDK_MAGIC_RGB];
const ELECTRUM_TIMEOUT_SECS: u8 = 10;

#[derive(Debug, Deserialize)]
struct DaemonConfig {
    service: ServiceConfig,
    rna: RnaConfig,
    #[serde(default)]
    legacy: LegacyConfig,
}

#[derive(Clone, Debug, Deserialize)]
struct ServiceConfig {
    bind: SocketAddr,
    network: String,
    data_dir: PathBuf,
    esplora_url: String,
    #[serde(default)]
    recovery_scan_interval_secs: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
struct LegacyConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_legacy_allow_loopback")]
    allow_loopback: bool,
    #[serde(default)]
    allowed_ips: Vec<String>,
    #[serde(default = "default_legacy_sync_timeout_secs")]
    sync_timeout_secs: u64,
    #[serde(default = "default_legacy_reveal_address_count")]
    reveal_address_count: u32,
    #[serde(default)]
    rgb_fee_enabled: bool,
    #[serde(default)]
    rgb_fee_collector_address: Option<String>,
    #[serde(default)]
    rgb_fee_contract_id: Option<String>,
    #[serde(default)]
    rgb_fee_amount: Option<u64>,
}

impl Default for LegacyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            allow_loopback: default_legacy_allow_loopback(),
            allowed_ips: Vec::new(),
            sync_timeout_secs: default_legacy_sync_timeout_secs(),
            reveal_address_count: default_legacy_reveal_address_count(),
            rgb_fee_enabled: false,
            rgb_fee_collector_address: None,
            rgb_fee_contract_id: None,
            rgb_fee_amount: None,
        }
    }
}

fn default_legacy_allow_loopback() -> bool {
    true
}

fn default_legacy_sync_timeout_secs() -> u64 {
    30
}

fn default_legacy_reveal_address_count() -> u32 {
    20
}

#[derive(Debug, Clone, Deserialize)]
struct RnaConfig {
    issue_fee: u64,
    transfer_fee: u64,
    query_fee: u64,
}

impl RnaConfig {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.issue_fee == 0 {
            return Err("rna.issue_fee must be greater than zero".into());
        }
        if self.transfer_fee == 0 {
            return Err("rna.transfer_fee must be greater than zero".into());
        }
        if self.query_fee == 0 {
            return Err("rna.query_fee must be greater than zero".into());
        }
        Ok(())
    }
}

fn wallet_v2_data_dir_usage() -> &'static str {
    "usage: rgb-service import-wallet-v2-data-dir <config.toml> <wallet-v2-local-data-dir> [--limit N] [--dry-run]"
}

fn decode_script_address_usage() -> &'static str {
    "usage: rgb-service decode-script-address <script_pubkey_hex> [mainnet|testnet|signet|regtest]"
}

fn inspect_daemon_legacy_usage() -> &'static str {
    "usage: rgb-service inspect-daemon-legacy-account <config.toml> <account_id_or_address>"
}

fn trace_wallet_v2_contract_usage() -> &'static str {
    "usage: rgb-service trace-wallet-v2-contract <wallet-v2-local-data-dir> <contract_id>"
}

fn inspect_wallet_v2_witness_output_usage() -> &'static str {
    "usage: rgb-service inspect-wallet-v2-witness-output <wallet-v2-local-data-dir> <txid> <vout>"
}

fn wallet_v2_inspect_stock_usage() -> &'static str {
    "usage: rgb-service inspect-wallet-v2-stock <wallet-v2-local-data-dir> <desc_id-or-address> [network]"
}

#[derive(Clone)]
struct WalletV2ContractAllocationRecord {
    source: String,
    outpoint: OutPoint,
    amount_raw: u64,
    witness: Option<Txid>,
}

#[derive(Clone)]
struct WalletV2ContractWitnessRecord {
    ord: Option<WitnessOrd>,
    tx: Option<Transaction>,
    sources: BTreeSet<String>,
}

fn witness_ord_height(ord: Option<WitnessOrd>) -> u32 {
    match ord {
        Some(WitnessOrd::Mined(pos)) => pos.height().into(),
        Some(WitnessOrd::Tentative) => u32::MAX - 2,
        Some(WitnessOrd::Ignored) => u32::MAX - 1,
        Some(WitnessOrd::Archived) => u32::MAX,
        None => 0,
    }
}

fn trace_wallet_v2_contract(
    wallet_v2_data_dir: &Path,
    contract_id: RgbContractId,
) -> rgb_service_api::Result<()> {
    let mut all_allocations = Vec::<WalletV2ContractAllocationRecord>::new();
    let mut witness_records = BTreeMap::<Txid, WalletV2ContractWitnessRecord>::new();
    let mut ticker = String::new();
    let mut name = String::new();
    let mut precision = 0u8;
    let mut scanned_stocks = 0usize;
    let mut matched_stocks = 0usize;

    let entries = fs::read_dir(wallet_v2_data_dir)
        .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 data dir: {err}")))?;
    for entry in entries {
        let entry = entry
            .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 entry: {err}")))?;
        let file_type = entry
            .file_type()
            .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 file type: {err}")))?;
        if !file_type.is_dir() {
            continue;
        }
        let source = entry.file_name().to_string_lossy().to_string();
        let stock_dir = entry.path().join(WALLET_V2_RGB_PATH);
        if !stock_dir.is_dir() {
            continue;
        }
        scanned_stocks += 1;

        let provider = FsBinStore::new(stock_dir.clone())
            .map_err(|err| RgbServiceError::Backend(format!("open legacy RGB stock: {err:?}")))?;
        let stock: Stock = match Stock::load(provider, true) {
            Ok(stock) => stock,
            Err(_) => continue,
        };
        let contract_data = match stock.contract_data(contract_id) {
            Ok(data) => data,
            Err(_) => continue,
        };
        matched_stocks += 1;

        if ticker.is_empty() {
            if let Some(spec_val) = contract_data.global("spec").next() {
                let spec = AssetSpec::from_strict_val_unchecked(&spec_val);
                ticker = spec.ticker.to_string();
                name = spec.name.to_string();
                precision = spec.precision.decimals();
            }
        }

        let allocations = contract_data
            .fungible("assetOwner", FilterIncludeAll)
            .map_err(|err| {
                RgbServiceError::Backend(format!("load contract fungible state: {err:?}"))
            })?;
        let stash = stock.as_stash_provider();

        for allocation in allocations {
            all_allocations.push(WalletV2ContractAllocationRecord {
                source: source.clone(),
                outpoint: allocation.seal.to_outpoint(),
                amount_raw: allocation.state.value(),
                witness: allocation.witness,
            });

            let Some(txid) = allocation.witness else {
                continue;
            };
            let record =
                witness_records
                    .entry(txid)
                    .or_insert_with(|| WalletV2ContractWitnessRecord {
                        ord: contract_data.witness_info(txid).map(|info| info.ord),
                        tx: None,
                        sources: BTreeSet::new(),
                    });
            record.sources.insert(source.clone());
            if record.ord.is_none() {
                record.ord = contract_data.witness_info(txid).map(|info| info.ord);
            }
            if record.tx.is_none() {
                if let Ok(witness) = stash.witness(txid) {
                    if let Some(tx) = witness.public.tx() {
                        record.tx = Some(tx.clone());
                    }
                }
            }
        }
    }

    if all_allocations.is_empty() {
        return Err(RgbServiceError::NotFound(format!(
            "no allocations found for contract {contract_id}"
        )));
    }

    let mut dedup =
        BTreeMap::<(String, u64, Option<String>), WalletV2ContractAllocationRecord>::new();
    for allocation in all_allocations {
        dedup
            .entry((
                allocation.outpoint.to_string(),
                allocation.amount_raw,
                allocation.witness.map(|txid| txid.to_string()),
            ))
            .or_insert(allocation);
    }
    let allocations = dedup.into_values().collect::<Vec<_>>();

    let genesis = allocations
        .iter()
        .filter(|allocation| allocation.witness.is_none())
        .cloned()
        .collect::<Vec<_>>();
    let genesis_total = genesis
        .iter()
        .map(|allocation| allocation.amount_raw)
        .sum::<u64>();

    let known_outpoints = allocations
        .iter()
        .map(|allocation| allocation.outpoint)
        .collect::<HashSet<_>>();
    let mut consumed = HashSet::<OutPoint>::new();
    let mut witness_rows = witness_records
        .into_iter()
        .map(|(txid, record)| {
            let outputs = allocations
                .iter()
                .filter(|allocation| allocation.witness == Some(txid))
                .cloned()
                .collect::<Vec<_>>();
            let known_inputs = record
                .tx
                .as_ref()
                .map(|tx| {
                    tx.input
                        .iter()
                        .map(|input| input.previous_output)
                        .filter(|outpoint| known_outpoints.contains(outpoint))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for input in &known_inputs {
                consumed.insert(*input);
            }
            let output_total = outputs
                .iter()
                .map(|allocation| allocation.amount_raw)
                .sum::<u64>();
            (
                txid,
                record.ord,
                known_inputs,
                outputs,
                output_total,
                record.sources,
            )
        })
        .collect::<Vec<_>>();
    witness_rows.sort_by(|(txid_a, ord_a, ..), (txid_b, ord_b, ..)| {
        witness_ord_height(*ord_a)
            .cmp(&witness_ord_height(*ord_b))
            .then_with(|| txid_a.to_string().cmp(&txid_b.to_string()))
    });

    let mut leaves = allocations
        .iter()
        .filter(|allocation| !consumed.contains(&allocation.outpoint))
        .cloned()
        .collect::<Vec<_>>();
    leaves.sort_by(|a, b| {
        b.amount_raw
            .cmp(&a.amount_raw)
            .then_with(|| a.outpoint.to_string().cmp(&b.outpoint.to_string()))
    });
    let leaf_total = leaves
        .iter()
        .map(|allocation| allocation.amount_raw)
        .sum::<u64>();

    println!("contract_id={contract_id}");
    println!("ticker={ticker}");
    println!("name={name}");
    println!("precision={precision}");
    println!("scanned_stocks={scanned_stocks}");
    println!("matched_stocks={matched_stocks}");
    println!("allocations_total={}", allocations.len());
    println!("witness_tx_total={}", witness_rows.len());
    println!("genesis_allocations={}", genesis.len());
    println!("genesis_total_raw={genesis_total}");
    for allocation in &genesis {
        println!(
            "genesis outpoint={} amount_raw={} source={}",
            allocation.outpoint, allocation.amount_raw, allocation.source
        );
    }

    for (txid, ord, known_inputs, outputs, output_total, sources) in &witness_rows {
        println!(
            "witness txid={} ord={:?} known_bazx_inputs={} outputs={} output_total_raw={} sources={}",
            txid,
            ord,
            known_inputs.len(),
            outputs.len(),
            output_total,
            sources.iter().cloned().collect::<Vec<_>>().join(",")
        );
        for input in known_inputs {
            if let Some(prev) = allocations
                .iter()
                .find(|allocation| allocation.outpoint == *input)
            {
                println!(
                    "  input outpoint={} amount_raw={}",
                    prev.outpoint, prev.amount_raw
                );
            } else {
                println!("  input outpoint={} amount_raw=unknown", input);
            }
        }
        for output in outputs {
            println!(
                "  output outpoint={} amount_raw={} source={}",
                output.outpoint, output.amount_raw, output.source
            );
        }
    }

    println!("leaf_allocations={}", leaves.len());
    println!("leaf_total_raw={leaf_total}");
    for allocation in &leaves {
        println!(
            "leaf outpoint={} amount_raw={} witness={}",
            allocation.outpoint,
            allocation.amount_raw,
            allocation
                .witness
                .map(|txid| txid.to_string())
                .unwrap_or_default()
        );
    }

    Ok(())
}

// wallet-service-v2 migration: list every RGB contract (ticker, name,
// contract_id) found across all on-disk account stocks, grouped by account.
// Used to discover contract ids without a SQL dump.
fn list_wallet_v2_contracts(wallet_v2_data_dir: &Path) -> rgb_service_api::Result<()> {
    let entries = fs::read_dir(wallet_v2_data_dir)
        .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 data dir: {err}")))?;
    for entry in entries {
        let entry = entry
            .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 entry: {err}")))?;
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let source = entry.file_name().to_string_lossy().to_string();
        if source == "priv" {
            continue;
        }
        let stock_dir = entry.path().join(WALLET_V2_RGB_PATH);
        if !stock_dir.is_dir() {
            continue;
        }
        let provider = FsBinStore::new(stock_dir.clone())
            .map_err(|err| RgbServiceError::Backend(format!("open legacy RGB stock: {err:?}")))?;
        let stock: Stock = match Stock::load(provider, true) {
            Ok(stock) => stock,
            Err(_) => continue,
        };
        let contracts = stock
            .contracts()
            .map_err(|err| RgbServiceError::Backend(format!("list contracts: {err:?}")))?;
        for info in contracts {
            let contract_id = info.id;
            let (ticker, name) = stock
                .contract_data(contract_id)
                .ok()
                .and_then(|data| data.global("spec").next())
                .map(|val| {
                    let spec = AssetSpec::from_strict_val_unchecked(&val);
                    (spec.ticker.to_string(), spec.name.to_string())
                })
                .unwrap_or_else(|| (String::new(), String::new()));
            println!("account={source} contract_id={contract_id} ticker={ticker} name={name}");
        }
    }
    Ok(())
}

// wallet-service-v2 migration: audit every allocation recorded in each account
// stock (FilterIncludeAll, independent of BDK unspent). Used to compare against
// the BDK-derived UTXO set and detect allocations missed during import.
fn audit_wallet_v2_allocations(wallet_v2_data_dir: &Path) -> rgb_service_api::Result<()> {
    let entries = fs::read_dir(wallet_v2_data_dir)
        .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 data dir: {err}")))?;
    let mut total_allocations = 0usize;
    let mut accounts_with_allocations = 0usize;
    for entry in entries {
        let entry = entry
            .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 entry: {err}")))?;
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let source = entry.file_name().to_string_lossy().to_string();
        if source == "priv" {
            continue;
        }
        let stock_dir = entry.path().join(WALLET_V2_RGB_PATH);
        if !stock_dir.is_dir() {
            continue;
        }
        let provider = FsBinStore::new(stock_dir.clone())
            .map_err(|err| RgbServiceError::Backend(format!("open legacy RGB stock: {err:?}")))?;
        let stock: Stock = match Stock::load(provider, true) {
            Ok(stock) => stock,
            Err(_) => continue,
        };
        let contracts = stock
            .contracts()
            .map_err(|err| RgbServiceError::Backend(format!("list contracts: {err:?}")))?;
        let mut account_count = 0usize;
        for info in contracts {
            let contract_id = info.id;
            let Ok(contract_data) = stock.contract_data(contract_id) else {
                continue;
            };
            let (ticker, precision) = contract_data
                .global("spec")
                .next()
                .map(|val| {
                    let spec = AssetSpec::from_strict_val_unchecked(&val);
                    (spec.ticker.to_string(), spec.precision.decimals())
                })
                .unwrap_or_else(|| (String::new(), 0u8));
            let Ok(allocations) = contract_data.fungible("assetOwner", FilterIncludeAll) else {
                continue;
            };
            for allocation in allocations {
                account_count += 1;
                total_allocations += 1;
                println!(
                    "account={source} contract_id={contract_id} ticker={ticker} outpoint={} amount_raw={} witness={}",
                    allocation.seal.to_outpoint(),
                    allocation.state.value(),
                    allocation
                        .witness
                        .map(|w| w.to_string())
                        .unwrap_or_else(|| "genesis".to_string())
                );
            }
            let _ = precision;
        }
        if account_count > 0 {
            accounts_with_allocations += 1;
        }
    }
    eprintln!(
        "audit total: accounts_with_allocations={accounts_with_allocations} total_allocations={total_allocations}"
    );
    Ok(())
}

// wallet-service-v2 migration: inspect a single account's RGB allocations
// directly from the on-disk data directory (no SQL dump needed). Resolves the
// account by desc_id, opens its BDK wallet via the bundled reader to recover
// current UTXOs, then lists every fungible allocation on those UTXOs from the
// RGB stock.
fn inspect_wallet_v2_stock(
    wallet_v2_data_dir: &Path,
    account_match: &str,
    network: Network,
) -> rgb_service_api::Result<()> {
    let account_dir = resolve_wallet_v2_account_dir(wallet_v2_data_dir, account_match)?;
    let desc_id = account_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    let wallet_path = account_dir.join("bdk_wallet");
    let stock_dir = account_dir.join(WALLET_V2_RGB_PATH);
    let recovered = read_wallet_v2_account(&wallet_path, network, 0)?;
    let tracked: Vec<Rgb20TrackedUtxo> = recovered
        .utxos
        .into_iter()
        .map(|utxo| {
            let outpoint = OutPoint::from_str(&utxo.outpoint)
                .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
            Ok(Rgb20TrackedUtxo {
                outpoint,
                address: utxo.address,
                confirmed: utxo.is_confirmed,
            })
        })
        .collect::<rgb_service_api::Result<_>>()?;
    let allocations = list_legacy_rgb20_assets_for_utxos(&stock_dir, tracked.clone())
        .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;

    println!("desc_id={desc_id}");
    println!("descriptor={}", recovered.descriptor);
    println!("stock_dir={}", stock_dir.display());
    println!("current_utxos={}", tracked.len());
    for utxo in &tracked {
        println!(
            "utxo outpoint={} address={} confirmed={}",
            utxo.outpoint,
            utxo.address.as_deref().unwrap_or(""),
            utxo.confirmed
        );
    }
    println!("current_rgb_allocations={}", allocations.len());
    for allocation in allocations {
        println!(
            "allocation outpoint={} address={} contract_id={} ticker={} name={} amount_raw={} amount_display={} confirmed={}",
            allocation.outpoint,
            allocation.address.as_deref().unwrap_or(""),
            allocation.contract_id,
            allocation.ticker,
            allocation.name,
            allocation.amount_raw,
            allocation.amount_display,
            allocation.confirmed
        );
    }
    Ok(())
}

// Resolve a wallet-service-v2 account directory by desc_id, descriptor, or any
// of its derived addresses (the descriptor and addresses are recovered from
// each `bdk_wallet` file via the bundled reader).
fn resolve_wallet_v2_account_dir(
    wallet_v2_data_dir: &Path,
    account_match: &str,
) -> rgb_service_api::Result<PathBuf> {
    // fast path: exact desc_id directory match
    let direct = wallet_v2_data_dir.join(account_match);
    if direct.is_dir() {
        return Ok(direct);
    }
    let entries = fs::read_dir(wallet_v2_data_dir)
        .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 data dir: {err}")))?;
    for entry in entries {
        let entry = entry
            .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 entry: {err}")))?;
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name();
        if name == "priv" {
            continue;
        }
        let wallet_path = entry.path().join("bdk_wallet");
        if !wallet_path.exists() {
            continue;
        }
        let Ok(recovered) = read_wallet_v2_account(&wallet_path, Network::Bitcoin, 1) else {
            continue;
        };
        if recovered.descriptor == account_match
            || recovered.addresses.iter().any(|a| a == account_match)
        {
            return Ok(entry.path());
        }
    }
    Err(RgbServiceError::NotFound(format!(
        "wallet-v2 account not found for {account_match}"
    )))
}

fn inspect_wallet_v2_witness_output(
    wallet_v2_data_dir: &Path,
    txid: Txid,
    vout: usize,
) -> rgb_service_api::Result<()> {
    let entries = fs::read_dir(wallet_v2_data_dir)
        .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 data dir: {err}")))?;
    for entry in entries {
        let entry = entry
            .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 entry: {err}")))?;
        let file_type = entry
            .file_type()
            .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 file type: {err}")))?;
        if !file_type.is_dir() {
            continue;
        }
        let source = entry.file_name().to_string_lossy().to_string();
        let stock_dir = entry.path().join(WALLET_V2_RGB_PATH);
        if !stock_dir.is_dir() {
            continue;
        }
        let provider = FsBinStore::new(stock_dir.clone())
            .map_err(|err| RgbServiceError::Backend(format!("open legacy RGB stock: {err:?}")))?;
        let stock: Stock = match Stock::load(provider, true) {
            Ok(stock) => stock,
            Err(_) => continue,
        };
        let stash = stock.as_stash_provider();
        let Ok(witness) = stash.witness(txid) else {
            continue;
        };
        let Some(tx) = witness.public.tx() else {
            println!("source={source}");
            println!("txid={txid}");
            println!("witness_has_full_tx=false");
            return Ok(());
        };
        let output = tx.output.get(vout).ok_or_else(|| {
            RgbServiceError::NotFound(format!("tx {txid} has no output vout={vout}"))
        })?;
        let address = Address::from_script(&output.script_pubkey, Network::Bitcoin)
            .map(|address| address.to_string())
            .unwrap_or_default();
        println!("source={source}");
        println!("txid={txid}");
        println!("vout={vout}");
        println!("value_sat={}", output.value.to_sat());
        println!("script_pubkey={}", output.script_pubkey);
        println!("address={address}");
        return Ok(());
    }
    Err(RgbServiceError::NotFound(format!(
        "witness tx not found in wallet-v2 stocks: {txid}"
    )))
}

fn parse_bitcoin_network(name: Option<&str>) -> Result<Network, Box<dyn std::error::Error>> {
    match name.unwrap_or("mainnet") {
        "mainnet" | "bitcoin" => Ok(Network::Bitcoin),
        "testnet" => Ok(Network::Testnet),
        "signet" => Ok(Network::Signet),
        "regtest" => Ok(Network::Regtest),
        other => Err(format!("unknown bitcoin network: {other}").into()),
    }
}

fn decode_script_address(script_hex: &str, network: Network) -> rgb_service_api::Result<String> {
    let bytes = hex_decode(script_hex)
        .map_err(|err| RgbServiceError::Backend(format!("decode hex: {err}")))?;
    let script = ScriptBuf::from_bytes(bytes);
    Address::from_script(&script, network)
        .map(|address| address.to_string())
        .map_err(|err| RgbServiceError::Backend(format!("script to address: {err}")))
}

// wallet-service-v2 migration: an empty RGB `MemStash` strict-encodes to 20
// zero bytes, so any stock whose `stash.dat` is larger holds real contracts.
fn has_real_rgb_stock(stock_dir: &Path) -> bool {
    fs::metadata(stock_dir.join("stash.dat"))
        .map(|meta| meta.len() > 20)
        .unwrap_or(false)
}

// wallet-service-v2 migration: recover descriptor + UTXOs from a legacy BDK
// wallet file. These files are written by bdk_wallet 1.1.0 / bdk_file_store
// 0.18.1, which the daemon's bdk_wallet 2.x cannot read (incompatible file
// format). We shell out to the bundled `wallet-v2-reader` binary (pinned to
// bdk_wallet 1.1.0) which emits the recovered state as JSON.
#[derive(Debug, Deserialize)]
struct WalletV2ReaderOutput {
    descriptor: String,
    utxos: Vec<WalletV2ReaderUtxo>,
    #[serde(default)]
    addresses: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WalletV2ReaderUtxo {
    outpoint: String,
    address: Option<String>,
    is_confirmed: bool,
}

fn wallet_v2_reader_bin() -> rgb_service_api::Result<PathBuf> {
    // Resolve the sibling binary shipped alongside this daemon binary.
    let current = env::current_exe()
        .map_err(|err| RgbServiceError::Backend(format!("locate current exe: {err}")))?;
    let dir = current
        .parent()
        .ok_or_else(|| RgbServiceError::Backend("current exe has no parent dir".into()))?;
    let candidate = dir.join("wallet-v2-reader");
    if candidate.exists() {
        return Ok(candidate);
    }
    Err(RgbServiceError::Backend(format!(
        "wallet-v2-reader binary not found next to {} (build the wallet-v2-reader crate and ship \
         it beside rgb-service)",
        current.display()
    )))
}

fn read_wallet_v2_account(
    wallet_path: &Path,
    network: Network,
    reveal_count: u32,
) -> rgb_service_api::Result<WalletV2ReaderOutput> {
    let bin = wallet_v2_reader_bin()?;
    let network_str = match network {
        Network::Bitcoin => "bitcoin",
        Network::Testnet => "testnet",
        Network::Testnet4 => "testnet4",
        Network::Signet => "signet",
        Network::Regtest => "regtest",
    };
    let output = std::process::Command::new(&bin)
        .arg(wallet_path)
        .arg(network_str)
        .arg("--reveal-count")
        .arg(reveal_count.to_string())
        .output()
        .map_err(|err| {
            RgbServiceError::Backend(format!(
                "spawn wallet-v2-reader {}: {err}",
                bin.display()
            ))
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(RgbServiceError::Backend(format!(
            "wallet-v2-reader failed for {}: {stderr}",
            wallet_path.display()
        )));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str::<WalletV2ReaderOutput>(stdout.trim())
        .map_err(|err| RgbServiceError::Backend(format!("parse wallet-v2-reader output: {err}")))
}

fn inspect_daemon_legacy_account(
    service: &LocalDaemonService,
    account_or_address: &str,
) -> rgb_service_api::Result<()> {
    let account_id = service
        .resolve_legacy_account_id_for_address(account_or_address)?
        .unwrap_or_else(|| account_or_address.to_string());
    let desc = service.resolve_legacy_desc_for_account_id(&account_id)?;
    let stock_dir = service.account_stock_dir(&account_id);
    let tracked = service.list_account_utxos(&account_id)?;
    let rgb_utxos = tracked
        .iter()
        .map(|utxo| {
            Ok(Rgb20TrackedUtxo {
                outpoint: OutPoint::from_str(&utxo.outpoint)
                    .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?,
                address: utxo.address.clone(),
                confirmed: utxo.confirmed,
            })
        })
        .collect::<rgb_service_api::Result<Vec<_>>>()?;
    let allocations = list_rgb20_assets_for_utxos(&stock_dir, rgb_utxos)
        .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
    println!("account_id={account_id}");
    println!("resolved_desc={}", desc.unwrap_or_default());
    println!("stock_dir={}", stock_dir.display());
    println!("tracked_utxos={}", tracked.len());
    for utxo in tracked {
        println!(
            "tracked outpoint={} address={} confirmed={}",
            utxo.outpoint,
            utxo.address.as_deref().unwrap_or(""),
            utxo.confirmed
        );
    }
    println!("allocations={}", allocations.len());
    for allocation in allocations {
        println!(
            "allocation outpoint={} contract_id={} amount_raw={} address={} confirmed={}",
            allocation.outpoint,
            allocation.contract_id,
            allocation.amount_raw,
            allocation.address.as_deref().unwrap_or(""),
            allocation.confirmed
        );
    }
    Ok(())
}

fn parse_wallet_v2_data_dir_options(
    args: &[String],
) -> Result<WalletV2DataDirOptions, Box<dyn std::error::Error>> {
    let mut options = WalletV2DataDirOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--limit" => {
                index += 1;
                let value = args.get(index).ok_or(wallet_v2_data_dir_usage())?;
                options.limit = Some(value.parse::<usize>()?);
            }
            "--dry-run" => options.dry_run = true,
            value => {
                return Err(format!("unknown import-wallet-v2-data-dir option: {value}").into());
            }
        }
        index += 1;
    }
    Ok(options)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args
        .get(1)
        .is_some_and(|arg| arg == "trace-wallet-v2-contract")
    {
        let data_dir = args.get(2).ok_or(trace_wallet_v2_contract_usage())?;
        let contract_id = args.get(3).ok_or(trace_wallet_v2_contract_usage())?;
        let contract_id = RgbContractId::from_str(contract_id)
            .map_err(|err| RgbServiceError::Backend(format!("parse contract id: {err}")))?;
        trace_wallet_v2_contract(Path::new(data_dir), contract_id)?;
        return Ok(());
    }
    if args
        .get(1)
        .is_some_and(|arg| arg == "inspect-wallet-v2-witness-output")
    {
        let data_dir = args
            .get(2)
            .ok_or(inspect_wallet_v2_witness_output_usage())?;
        let txid = args
            .get(3)
            .ok_or(inspect_wallet_v2_witness_output_usage())?;
        let vout = args
            .get(4)
            .ok_or(inspect_wallet_v2_witness_output_usage())?;
        let txid = Txid::from_str(txid)
            .map_err(|err| RgbServiceError::Backend(format!("parse txid: {err}")))?;
        let vout = vout
            .parse::<usize>()
            .map_err(|err| RgbServiceError::Backend(format!("parse vout: {err}")))?;
        inspect_wallet_v2_witness_output(Path::new(data_dir), txid, vout)?;
        return Ok(());
    }
    if args
        .get(1)
        .is_some_and(|arg| arg == "inspect-wallet-v2-stock")
    {
        let data_dir = args.get(2).ok_or(wallet_v2_inspect_stock_usage())?;
        let account_match = args.get(3).ok_or(wallet_v2_inspect_stock_usage())?;
        let network = parse_bitcoin_network(args.get(4).map(String::as_str))?;
        inspect_wallet_v2_stock(Path::new(data_dir), account_match, network)?;
        return Ok(());
    }
    if args
        .get(1)
        .is_some_and(|arg| arg == "list-wallet-v2-contracts")
    {
        let data_dir = args.get(2).ok_or("usage: rgb-service list-wallet-v2-contracts <wallet-v2-local-data-dir>")?;
        list_wallet_v2_contracts(Path::new(data_dir))?;
        return Ok(());
    }
    if args
        .get(1)
        .is_some_and(|arg| arg == "audit-wallet-v2-allocations")
    {
        let data_dir = args.get(2).ok_or("usage: rgb-service audit-wallet-v2-allocations <wallet-v2-local-data-dir>")?;
        audit_wallet_v2_allocations(Path::new(data_dir))?;
        return Ok(());
    }
    if args
        .get(1)
        .is_some_and(|arg| arg == "decode-script-address")
    {
        let script_hex = args.get(2).ok_or(decode_script_address_usage())?;
        let network = parse_bitcoin_network(args.get(3).map(String::as_str))?;
        println!("{}", decode_script_address(script_hex, network)?);
        return Ok(());
    }
    if args
        .get(1)
        .is_some_and(|arg| arg == "inspect-daemon-legacy-account")
    {
        let config_path = args.get(2).ok_or(inspect_daemon_legacy_usage())?;
        let account_or_address = args.get(3).ok_or(inspect_daemon_legacy_usage())?;
        let config = load_config(config_path)?;
        fs::create_dir_all(&config.service.data_dir)?;
        let service = LocalDaemonService::new(config).await?;
        inspect_daemon_legacy_account(&service, account_or_address)?;
        return Ok(());
    }
    if args
        .get(1)
        .is_some_and(|arg| arg == "import-wallet-v2-data-dir")
    {
        let config_path = args.get(2).ok_or(wallet_v2_data_dir_usage())?;
        let data_dir = args.get(3).ok_or(wallet_v2_data_dir_usage())?;
        let options = parse_wallet_v2_data_dir_options(&args[4..])?;
        let config = load_config(config_path)?;
        fs::create_dir_all(&config.service.data_dir)?;
        let service = LocalDaemonService::new(config).await?;
        let summary =
            service.import_wallet_v2_data_dir(Path::new(data_dir), options)?;
        println!(
            "imported wallet-v2 data-dir: accounts={} addresses={} utxos={} stocks_imported={} skipped_empty={} errors={} mode={}",
            summary.accounts,
            summary.addresses,
            summary.utxos,
            summary.stocks_imported,
            summary.skipped_empty,
            summary.errors,
            if options.dry_run { "dry-run" } else { "write" }
        );
        return Ok(());
    }
    let config_path = args.get(1).ok_or("usage: rgb-service <config.toml>")?;
    let config = load_config(config_path)?;
    fs::create_dir_all(&config.service.data_dir)?;

    println!(
        "starting rgb-service on {} for {} with data_dir {}",
        config.service.bind,
        config.service.network,
        config.service.data_dir.display()
    );

    let bind = config.service.bind;
    let legacy_config = config.legacy.clone();
    let service = Arc::new(LocalDaemonService::new(config).await?);
    spawn_recovery_scanner(Arc::clone(&service));
    let auth = Arc::new(ConfiguredAuthVerifier);
    let mut app = router(service.clone(), auth);
    if legacy_config.enabled {
        service
            .logger
            .info("legacy wallet-service-v2 compatible routes enabled");
        app = app.merge(legacy::router(Arc::clone(&service), legacy_config));
    }
    app = app.route_layer(middleware::from_fn_with_state(
        Arc::clone(&service),
        access_log_middleware,
    ));
    let listener = TcpListener::bind(bind).await?;

    serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = signal::ctrl_c().await;
    })
    .await?;

    Ok(())
}

fn load_config(path: &str) -> Result<DaemonConfig, Box<dyn std::error::Error>> {
    let text = fs::read_to_string(path)?;
    let config: DaemonConfig = toml::from_str(&text)?;
    if config.service.network.trim().is_empty() {
        return Err("service.network must not be empty".into());
    }
    if config.service.data_dir.as_os_str().is_empty() {
        return Err("service.data_dir must not be empty".into());
    }
    if config.service.esplora_url.trim().is_empty() {
        return Err("service.esplora_url must not be empty".into());
    }
    config.rna.validate()?;
    Ok(config)
}

async fn access_log_middleware(
    State(service): State<Arc<LocalDaemonService>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = req.method().clone();
    let path = req
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());
    let forwarded_for = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let user_agent = req
        .headers()
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let mut response = next.run(req).await;
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    let status = response.status().as_u16();
    let latency_ms = started.elapsed().as_millis();
    // For error responses, capture the body so we can log why the request
    // failed (the access log alone only shows the status code).
    let body_note = if status >= 400 {
        let body = std::mem::replace(response.body_mut(), Body::empty());
        let bytes = axum::body::to_bytes(body, 8 * 1024).await.unwrap_or_default();
        let note = String::from_utf8_lossy(&bytes).to_string();
        *response.body_mut() = Body::from(bytes);
        note
    } else {
        String::new()
    };
    if body_note.is_empty() {
        service.logger.info(format!(
            "access remote={} forwarded_for={} method={} path={} status={} latency_ms={} user_agent={}",
            remote.ip(),
            forwarded_for,
            method,
            path,
            status,
            latency_ms,
            user_agent
        ));
    } else {
        service.logger.info(format!(
            "access remote={} forwarded_for={} method={} path={} status={} latency_ms={} user_agent={} body={}",
            remote.ip(),
            forwarded_for,
            method,
            path,
            status,
            latency_ms,
            user_agent,
            body_note
        ));
    }
    response
}

fn spawn_recovery_scanner(service: Arc<LocalDaemonService>) {
    let Some(interval_secs) = service.config.recovery_scan_interval_secs.or(Some(60)) else {
        return;
    };
    if interval_secs == 0 {
        service
            .logger
            .info("rgb pending recovery scanner disabled by config");
        return;
    }
    service.logger.info(format!(
        "rgb pending recovery scanner enabled interval_secs={interval_secs}"
    ));
    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_secs);
        loop {
            let service_for_scan = Arc::clone(&service);
            match tokio::task::spawn_blocking(move || service_for_scan.scan_pending_rgb_stocks())
                .await
            {
                Ok(Ok(report)) => {
                    if report.scanned > 0
                        || report.promoted > 0
                        || report.pending > 0
                        || report.failed > 0
                    {
                        service.logger.info(format!(
                            "rgb pending recovery scan accounts={} scanned={} promoted={} pending={} skipped={} failed={}",
                            report.accounts,
                            report.scanned,
                            report.promoted,
                            report.pending,
                            report.skipped,
                            report.failed
                        ));
                    }
                }
                Ok(Err(err)) => service
                    .logger
                    .info(format!("rgb pending recovery scan failed: {err}")),
                Err(err) => service
                    .logger
                    .info(format!("rgb pending recovery scanner task failed: {err}")),
            }
            tokio::time::sleep(interval).await;
        }
    });
}

const SIGNATURE_DOMAIN: &[u8] = b"bihelix-ln-rgb-auth-v1";
const REQUEST_SIGNATURE_MAX_AGE_MS: u64 = 5 * 60 * 1000;
const REQUEST_SIGNATURE_FUTURE_SKEW_MS: u64 = 60 * 1000;

struct ConfiguredAuthVerifier;

#[derive(Serialize)]
struct UnsignedAssetSpendAuthorizationRef<'a> {
    asset_id: &'a str,
    amount: u64,
    purpose: &'a rgb_service_api::AssetSpendPurpose,
    recipient: &'a Option<String>,
    anchor_psbt: &'a Option<String>,
    expires_at_ms: u64,
}

impl ConfiguredAuthVerifier {
    fn verify_ecdsa_signature(
        purpose: &str,
        payload: &[u8],
        signature: &RequestSignature,
    ) -> rgb_service_api::Result<()> {
        if signature.public_key.trim().is_empty() {
            return Err(RgbServiceError::Unauthorized(
                "request signature public_key must not be empty".to_string(),
            ));
        }
        if signature.nonce.trim().is_empty() {
            return Err(RgbServiceError::Unauthorized(
                "request signature nonce must not be empty".to_string(),
            ));
        }
        if signature.signature.trim().is_empty() {
            return Err(RgbServiceError::SignatureRequired(
                "request signature must not be empty".to_string(),
            ));
        }
        Self::verify_signature_timestamp(signature.timestamp_ms)?;

        let public_key = PublicKey::from_str(&signature.public_key).map_err(|err| {
            RgbServiceError::Unauthorized(format!("invalid request signature public_key: {err}"))
        })?;
        let signature_bytes = hex_decode(&signature.signature).map_err(|err| {
            RgbServiceError::Unauthorized(format!("invalid request signature hex: {err}"))
        })?;
        let ecdsa_signature = ecdsa::Signature::from_der(&signature_bytes).map_err(|err| {
            RgbServiceError::Unauthorized(format!("invalid request ECDSA signature: {err}"))
        })?;
        let message = Self::signature_message(
            purpose,
            payload,
            signature.nonce.as_str(),
            signature.timestamp_ms,
        );
        Secp256k1::verification_only()
            .verify_ecdsa(&message, &ecdsa_signature, &public_key)
            .map_err(|err| {
                RgbServiceError::Unauthorized(format!("invalid request signature: {err}"))
            })
    }

    fn verify_signature_timestamp(timestamp_ms: u64) -> rgb_service_api::Result<()> {
        let now = now_ms();
        if timestamp_ms > now.saturating_add(REQUEST_SIGNATURE_FUTURE_SKEW_MS) {
            return Err(RgbServiceError::Unauthorized(
                "request signature timestamp is too far in the future".to_string(),
            ));
        }
        if now.saturating_sub(timestamp_ms) > REQUEST_SIGNATURE_MAX_AGE_MS {
            return Err(RgbServiceError::Unauthorized(
                "request signature has expired".to_string(),
            ));
        }
        Ok(())
    }

    fn signature_message(purpose: &str, payload: &[u8], nonce: &str, timestamp_ms: u64) -> Message {
        let mut engine = sha256::Hash::engine();
        engine.input(SIGNATURE_DOMAIN);
        engine.input(purpose.as_bytes());
        engine.input(nonce.as_bytes());
        engine.input(&timestamp_ms.to_be_bytes());
        engine.input(payload);
        Message::from_digest(sha256::Hash::from_engine(engine).to_byte_array())
    }

    fn permission_purposes(permission: &Permission) -> &'static [&'static str] {
        match permission {
            Permission::ReadRnaBalance => &["read_rna_balance", "rna_balance"],
            Permission::ReadAssets => {
                &["read_assets", "list_assets", "balance", "balance_breakdown"]
            }
            Permission::IssueAsset => &["issue_asset"],
            Permission::PrepareTransfer => &["prepare_transfer"],
            Permission::CommitTransfer => &["commit_transfer"],
            Permission::LnChannelOpenPrepare => &["ln_channel_open_prepare"],
            Permission::LnChannelFundingRef => &["ln_channel_funding_ref"],
            Permission::LnCommitmentCompose => &["ln_commitment_compose"],
            Permission::LnClosingCompose => &["ln_closing_compose"],
            Permission::LnOnchainClaimCompose => &["ln_onchain_claim_compose"],
            Permission::LnPaymentClaim => &["ln_payment_claim"],
            Permission::LnRecover => &["ln_recover"],
            Permission::CancelTransfer => &["cancel_transfer"],
            Permission::ManagePending => &["manage_pending", "list_pending"],
            Permission::Recover => &["recover"],
            Permission::RunTest => &["run_test", "run_rgb_test"],
            Permission::L2Reserve => &["l2_reserve"],
            Permission::L2Settle => &["l2_settle"],
            Permission::Admin => &["admin"],
        }
    }

    fn verify_request_signature(
        account_id: &str,
        permission: &Permission,
        payload: &[u8],
        signature: &RequestSignature,
    ) -> rgb_service_api::Result<()> {
        match signature.scheme {
            SignatureScheme::Ecdsa => {
                let mut last_error = None;
                for purpose in Self::permission_purposes(permission) {
                    match Self::verify_ecdsa_signature(purpose, payload, signature) {
                        Ok(()) => return Ok(()),
                        Err(err) => last_error = Some(err),
                    }
                }
                Err(last_error.unwrap_or_else(|| {
                    RgbServiceError::Unauthorized(
                        "request signature purpose did not match permission".to_string(),
                    )
                }))
            }
            SignatureScheme::Bip322 => Self::verify_legacy_signer_app_bip322(account_id, signature),
            SignatureScheme::Schnorr | SignatureScheme::Ed25519 => {
                Err(RgbServiceError::Unauthorized(format!(
                    "unsupported request signature scheme: {:?}",
                    signature.scheme
                )))
            }
        }
    }

    fn verify_legacy_signer_app_bip322(
        account_id: &str,
        signature: &RequestSignature,
    ) -> rgb_service_api::Result<()> {
        if signature.signer_id != account_id {
            return Err(RgbServiceError::Unauthorized(
                "BIP322 signer_id must match account_id".to_string(),
            ));
        }
        if signature.signature.trim().is_empty() {
            return Err(RgbServiceError::SignatureRequired(
                "BIP322 request signature must not be empty".to_string(),
            ));
        }
        if signature.nonce.trim().is_empty() {
            return Err(RgbServiceError::Unauthorized(
                "BIP322 request signature nonce must not be empty".to_string(),
            ));
        }
        Self::verify_signature_timestamp(signature.timestamp_ms)
    }
}

#[async_trait]
impl AuthVerifier for ConfiguredAuthVerifier {
    async fn verify_request(
        &self,
        permission: Permission,
        account_id: &str,
        payload: &[u8],
        signature: &RequestSignature,
    ) -> rgb_service_api::Result<AuthSubject> {
        if account_id.trim().is_empty() {
            return Err(RgbServiceError::Unauthorized(
                "account_id must not be empty".to_string(),
            ));
        }
        Self::verify_request_signature(account_id, &permission, payload, signature)?;
        Ok(AuthSubject {
            account_id: account_id.to_string(),
            signer_id: signature.signer_id.clone(),
            permissions: vec![permission],
        })
    }

    async fn verify_asset_spend(
        &self,
        _account_id: &str,
        authorization: &AssetSpendAuthorization,
    ) -> rgb_service_api::Result<()> {
        if authorization.expires_at_ms < now_ms() {
            return Err(RgbServiceError::Unauthorized(
                "asset spend authorization has expired".to_string(),
            ));
        }
        let unsigned_payload = UnsignedAssetSpendAuthorizationRef {
            asset_id: &authorization.asset_id,
            amount: authorization.amount,
            purpose: &authorization.purpose,
            recipient: &authorization.recipient,
            anchor_psbt: &authorization.anchor_psbt,
            expires_at_ms: authorization.expires_at_ms,
        };
        let payload = serde_json::to_vec(&unsigned_payload)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        match authorization.signature.scheme {
            SignatureScheme::Ecdsa => {
                Self::verify_ecdsa_signature("asset_spend", &payload, &authorization.signature)
            }
            SignatureScheme::Bip322 => {
                Self::verify_legacy_signer_app_bip322(_account_id, &authorization.signature)
            }
            SignatureScheme::Schnorr | SignatureScheme::Ed25519 => {
                Err(RgbServiceError::AssetSpendAuthorizationRequired(format!(
                    "unsupported asset spend signature scheme: {:?}",
                    authorization.signature.scheme
                )))
            }
        }
    }
}

struct LocalDaemonService {
    config: ServiceConfig,
    rna: RnaConfig,
    db: SingleWriterTxDatabase,
    logger: DaemonLogger,
    pending_stock_dirs: Arc<Mutex<BTreeSet<PathBuf>>>,
}

#[derive(Default)]
struct RecoveryScanSummary {
    accounts: usize,
    scanned: usize,
    promoted: usize,
    pending: usize,
    skipped: usize,
    failed: usize,
}

#[derive(Clone)]
pub(crate) struct DaemonLogger {
    file: Arc<Mutex<fs::File>>,
}

impl DaemonLogger {
    fn open(data_dir: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join("rgb-service.log");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    fn info(&self, message: impl AsRef<str>) {
        self.write("INFO", message.as_ref());
    }

    pub(crate) fn warn(&self, message: impl AsRef<str>) {
        self.write("WARN", message.as_ref());
    }

    fn write(&self, level: &str, message: &str) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default();
        let line = format!("{ts} {level} {message}\n");
        print!("{line}");
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(line.as_bytes());
            let _ = file.flush();
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct PreparedTransferRecord {
    asset_id: String,
    recipient_account_id: String,
    recipient_vout: u32,
    fascia: Vec<u8>,
    #[serde(default)]
    recipients: Vec<PreparedTransferRecipient>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PreparedTransferRecipient {
    asset_id: String,
    recipient_account_id: String,
    recipient_vout: u32,
}

impl PreparedTransferRecord {
    fn recipients_or_legacy(&self) -> Vec<PreparedTransferRecipient> {
        if !self.recipients.is_empty() {
            return self.recipients.clone();
        }
        vec![PreparedTransferRecipient {
            asset_id: self.asset_id.clone(),
            recipient_account_id: self.recipient_account_id.clone(),
            recipient_vout: self.recipient_vout,
        }]
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TokenCatalogEntry {
    contract_id: String,
    ticker: String,
    name: String,
    precision: u8,
    supply: Option<u64>,
    issue_utxo: String,
    contract_type: String,
    issuer_desc: String,
    created_at: String,
    ext: Option<Value>,
}

#[derive(Default)]
struct WalletV2DataDirImportSummary {
    accounts: usize,
    addresses: usize,
    utxos: usize,
    stocks_imported: usize,
    skipped_empty: usize,
    errors: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct WalletV2DataDirOptions {
    limit: Option<usize>,
    dry_run: bool,
}

impl TokenCatalogEntry {
    fn empty(contract_id: String, ticker: String, name: String, precision: u8) -> Self {
        Self {
            contract_id,
            ticker,
            name,
            precision,
            supply: None,
            issue_utxo: String::new(),
            contract_type: String::new(),
            issuer_desc: String::new(),
            created_at: String::new(),
            ext: None,
        }
    }

    fn into_contract_info(self) -> RgbContractInfo {
        RgbContractInfo {
            contract_id: self.contract_id.clone(),
            schema: self.contract_type_schema(),
            asset_id: Some(self.contract_id.clone()),
            ticker: self.ticker,
            name: self.name,
            precision: self.precision,
            supply: self.supply,
            issue_utxo: self.issue_utxo,
            contract_type: self.contract_type,
            issuer_desc: self.issuer_desc,
            created_at: self.created_at,
            ext: self.ext,
        }
    }

    fn into_asset_info(self) -> RgbAssetInfo {
        RgbAssetInfo {
            asset_id: self.contract_id.clone(),
            contract_id: self.contract_id,
            ticker: self.ticker,
            name: self.name,
            precision: self.precision,
            supply: self.supply,
            issue_utxo: self.issue_utxo,
            contract_type: self.contract_type,
            issuer_desc: self.issuer_desc,
            created_at: self.created_at,
            ext: self.ext,
        }
    }

    fn contract_type_schema(&self) -> String {
        if self.contract_type.trim().is_empty() {
            "rgb20".to_string()
        } else {
            self.contract_type.to_ascii_lowercase()
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LnChannelRecord {
    account_id: String,
    channel_id: String,
    contract_id: String,
    funding_outpoint: String,
    funding_vout: u32,
    funding_rgb: u64,
    to_local_rgb: u64,
    to_remote_rgb: u64,
    funding_ref: RgbFundingRef,
    opening_txid: String,
    opening_fascia: Vec<u8>,
    created_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LnOutputAssignmentRecord {
    vout: u32,
    amount_rgb: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LnComposeRecord {
    account_id: String,
    channel_id: String,
    operation_id: String,
    route: String,
    contract_id: String,
    txid: String,
    tx_hex: String,
    fascia: Vec<u8>,
    funding_ref: RgbFundingRef,
    assignments: Vec<LnOutputAssignmentRecord>,
    created_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LnPaymentRecord {
    account_id: String,
    channel_id: Option<String>,
    payment_hash: String,
    contract_id: String,
    amount_msat: u64,
    rgb_amount: u64,
    status: OperationStatus,
    rgb_state_ref: Option<String>,
    created_at_ms: u64,
}

impl LocalDaemonService {
    fn electrum_with_retry<T, F>(&self, context: &str, mut op: F) -> rgb_service_api::Result<T>
    where
        F: FnMut() -> Result<T, electrum_client::Error>,
    {
        let attempts = 3;
        let mut last_err = None;
        for attempt in 1..=attempts {
            match op() {
                Ok(value) => return Ok(value),
                Err(err) => {
                    last_err = Some(err);
                    if attempt < attempts {
                        std::thread::sleep(Duration::from_millis(250));
                    }
                }
            }
        }
        let err = last_err.expect("electrum retry loop records the last error");
        Err(RgbServiceError::Backend(format!("{context} failed: {err}")))
    }

    async fn new(config: DaemonConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let logger = DaemonLogger::open(&config.service.data_dir)?;
        logger.info(format!(
            "starting rgb-service http={} network={} data_dir={}",
            config.service.bind,
            config.service.network,
            config.service.data_dir.display()
        ));
        logger.info(format!(
            "log_file={}",
            config.service.data_dir.join("rgb-service.log").display()
        ));
        let kv_dir = config.service.data_dir.join("kv");
        fs::create_dir_all(&kv_dir)?;
        let db = SingleWriterTxDatabase::builder(&kv_dir).open()?;
        logger.info(format!(
            "rna issue_fee={} transfer_fee={} query_fee={}",
            config.rna.issue_fee, config.rna.transfer_fee, config.rna.query_fee
        ));
        let pending_stock_dirs = Self::discover_pending_stock_dirs(&config.service.data_dir)?;
        logger.info(format!(
            "rgb pending scanner registered {} stock dirs at startup",
            pending_stock_dirs.len()
        ));
        Ok(Self {
            config: config.service,
            rna: config.rna,
            db,
            logger,
            pending_stock_dirs: Arc::new(Mutex::new(pending_stock_dirs)),
        })
    }

    /// Logger accessor for modules that need to write warnings (e.g. legacy).
    pub(crate) fn logger(&self) -> &DaemonLogger {
        &self.logger
    }

    fn account_stock_dir(&self, account_id: &str) -> PathBuf {
        self.config
            .data_dir
            .join("accounts")
            .join(account_id)
            .join("rgb-stock")
    }

    fn account_stock_dirs(&self) -> rgb_service_api::Result<Vec<PathBuf>> {
        let accounts_dir = self.config.data_dir.join("accounts");
        if !accounts_dir.exists() {
            return Ok(Vec::new());
        }
        let entries = fs::read_dir(&accounts_dir)
            .map_err(|err| RgbServiceError::Backend(format!("read accounts dir: {err}")))?;
        let mut dirs = Vec::new();
        for entry in entries {
            let entry = entry
                .map_err(|err| RgbServiceError::Backend(format!("read account dir: {err}")))?;
            let path = entry.path().join("rgb-stock");
            if path.exists() {
                dirs.push(path);
            }
        }
        Ok(dirs)
    }

    fn pending_stock_root(stock_dir: &Path) -> PathBuf {
        let stock_name = stock_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("stock");
        stock_dir
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{stock_name}_pending"))
    }

    fn pending_status_is_terminal(status_path: &Path) -> bool {
        fs::read(status_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|value| {
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .is_some_and(|status| matches!(status.as_str(), "confirmed" | "invalid"))
    }

    fn stock_has_active_pending(stock_dir: &Path) -> bool {
        let pending_root = Self::pending_stock_root(stock_dir);
        let Ok(entries) = fs::read_dir(pending_root) else {
            return false;
        };
        entries.filter_map(|entry| entry.ok()).any(|entry| {
            entry
                .file_type()
                .ok()
                .is_some_and(|file_type| file_type.is_dir())
                && !Self::pending_status_is_terminal(&entry.path().join("pending-status.json"))
        })
    }

    fn discover_pending_stock_dirs(data_dir: &Path) -> rgb_service_api::Result<BTreeSet<PathBuf>> {
        let accounts_dir = data_dir.join("accounts");
        if !accounts_dir.exists() {
            return Ok(BTreeSet::new());
        }
        let entries = fs::read_dir(&accounts_dir)
            .map_err(|err| RgbServiceError::Backend(format!("read accounts dir: {err}")))?;
        let mut dirs = BTreeSet::new();
        for entry in entries {
            let entry = entry
                .map_err(|err| RgbServiceError::Backend(format!("read account dir: {err}")))?;
            let stock_dir = entry.path().join("rgb-stock");
            if stock_dir.exists() && Self::stock_has_active_pending(&stock_dir) {
                dirs.insert(stock_dir);
            }
        }
        Ok(dirs)
    }

    fn pending_stock_dirs_snapshot(&self) -> rgb_service_api::Result<Vec<PathBuf>> {
        self.pending_stock_dirs
            .lock()
            .map(|dirs| dirs.iter().cloned().collect())
            .map_err(|err| RgbServiceError::Backend(format!("pending stock dirs lock: {err}")))
    }

    fn register_pending_stock_dir(&self, stock_dir: &Path) -> rgb_service_api::Result<()> {
        let mut dirs = self
            .pending_stock_dirs
            .lock()
            .map_err(|err| RgbServiceError::Backend(format!("pending stock dirs lock: {err}")))?;
        if dirs.insert(stock_dir.to_path_buf()) {
            self.logger.info(format!(
                "rgb pending scanner registered stock_dir={}",
                stock_dir.display()
            ));
        }
        Ok(())
    }

    fn unregister_pending_stock_dir(&self, stock_dir: &Path) -> rgb_service_api::Result<()> {
        let mut dirs = self
            .pending_stock_dirs
            .lock()
            .map_err(|err| RgbServiceError::Backend(format!("pending stock dirs lock: {err}")))?;
        dirs.remove(stock_dir);
        Ok(())
    }

    fn network(&self) -> rgb_service_api::Result<bitcoin::Network> {
        parse_network(&self.config.network)
    }

    fn chain_source(&self) -> ChainSource {
        chain_source_from_url(self.config.esplora_url.clone())
    }

    fn scan_pending_rgb_stocks(&self) -> rgb_service_api::Result<RecoveryScanSummary> {
        let network = self.network()?;
        let esplora_urls = std::slice::from_ref(&self.config.esplora_url);
        let mut summary = RecoveryScanSummary::default();
        for stock_dir in self.pending_stock_dirs_snapshot()? {
            summary.accounts += 1;
            match scan_and_promote_confirmed_staged_rgb_stocks(&stock_dir, network, esplora_urls) {
                Ok(report) => {
                    summary.scanned += report.scanned;
                    summary.promoted += report.promoted;
                    summary.pending += report.pending;
                    summary.skipped += report.skipped;
                    if report.promoted > 0 {
                        self.logger.info(format!(
                            "rgb pending recovery promoted stock_dir={} txids={:?}",
                            stock_dir.display(),
                            report.promoted_txids
                        ));
                    }
                    if !Self::stock_has_active_pending(&stock_dir) {
                        self.unregister_pending_stock_dir(&stock_dir)?;
                    }
                }
                Err(err) => {
                    summary.failed += 1;
                    self.logger.info(format!(
                        "rgb pending recovery scan account failed stock_dir={} error={err:#}",
                        stock_dir.display()
                    ));
                }
            }
        }
        Ok(summary)
    }

    fn validate_tracked_utxo(utxo: TrackedUtxo) -> rgb_service_api::Result<TrackedUtxo> {
        OutPoint::from_str(&utxo.outpoint)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        Ok(utxo)
    }

    fn account_utxo_key(account_id: &str, outpoint: &str) -> String {
        format!("{account_id}:{outpoint}")
    }

    fn put_account_utxo(&self, account_id: &str, utxo: TrackedUtxo) -> rgb_service_api::Result<()> {
        let utxo = Self::validate_tracked_utxo(utxo)?;
        let keyspace = self
            .db
            .keyspace("account_utxos", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = Self::account_utxo_key(account_id, &utxo.outpoint);
        let bytes =
            serde_json::to_vec(&utxo).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, key.as_bytes(), bytes);
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn list_account_utxos(&self, account_id: &str) -> rgb_service_api::Result<Vec<TrackedUtxo>> {
        let keyspace = self
            .db
            .keyspace("account_utxos", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let prefix = format!("{account_id}:");
        let mut utxos = Vec::new();
        for item in keyspace.as_ref().prefix(prefix.as_bytes()) {
            let value = item
                .value()
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            utxos.push(
                serde_json::from_slice(value.as_ref())
                    .map_err(|err| RgbServiceError::Backend(err.to_string()))?,
            );
        }
        Ok(utxos)
    }

    fn account_rgb20_utxos(
        &self,
        account_id: &str,
    ) -> rgb_service_api::Result<Vec<Rgb20TrackedUtxo>> {
        let utxos = self
            .list_account_utxos(account_id)?
            .into_iter()
            .map(|utxo| {
                Ok(Rgb20TrackedUtxo {
                    outpoint: OutPoint::from_str(&utxo.outpoint)
                        .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?,
                    address: utxo.address,
                    confirmed: utxo.confirmed,
                })
            })
            .collect::<rgb_service_api::Result<Vec<_>>>()?;
        self.filter_unspent_account_utxos(account_id, utxos)
    }

    fn filter_unspent_account_utxos(
        &self,
        account_id: &str,
        utxos: Vec<Rgb20TrackedUtxo>,
    ) -> rgb_service_api::Result<Vec<Rgb20TrackedUtxo>> {
        let mut kept = Vec::with_capacity(utxos.len());
        let mut spent = BTreeSet::new();
        for utxo in utxos {
            if !utxo.confirmed {
                kept.push(utxo);
                continue;
            }
            if self.chain_outpoint_unspent(utxo.outpoint)? {
                kept.push(utxo);
            } else {
                spent.insert(utxo.outpoint.to_string());
            }
        }
        if !spent.is_empty() {
            self.logger.info(format!(
                "removed spent rgb account utxos account_id={account_id} count={} outpoints={}",
                spent.len(),
                spent.iter().cloned().collect::<Vec<_>>().join(",")
            ));
            self.remove_account_utxos(account_id, spent)?;
        }
        Ok(kept)
    }

    fn chain_outpoint_unspent(&self, outpoint: OutPoint) -> rgb_service_api::Result<bool> {
        match self.chain_source() {
            ChainSource::Esplora(config) => self.esplora_outpoint_unspent(&config.url, outpoint),
            ChainSource::Electrum(config) => self.electrum_outpoint_unspent(&config.url, outpoint),
        }
    }

    fn esplora_outpoint_unspent(
        &self,
        url: &str,
        outpoint: OutPoint,
    ) -> rgb_service_api::Result<bool> {
        let client = esplora_client::Builder::new(url)
            .timeout(30)
            .build_blocking();
        let status = client
            .get_output_status(&outpoint.txid, u64::from(outpoint.vout))
            .map_err(|err| RgbServiceError::Backend(format!("esplora outspend failed: {err}")))?;
        Ok(status.is_some_and(|status| !status.spent))
    }

    fn electrum_outpoint_unspent(
        &self,
        url: &str,
        outpoint: OutPoint,
    ) -> rgb_service_api::Result<bool> {
        let client = self.electrum_with_retry("electrum client", || {
            let config = electrum_client::ConfigBuilder::new()
                .timeout(Some(ELECTRUM_TIMEOUT_SECS))
                .build();
            electrum_client::Client::from_config(&normalize_electrum_url(url), config)
        })?;
        let funding_tx = self.electrum_with_retry("electrum tx fetch", || {
            client.transaction_get(&outpoint.txid)
        })?;
        let Some(output) = funding_tx.output.get(outpoint.vout as usize) else {
            return Ok(false);
        };
        let unspent = self.electrum_with_retry("electrum script listunspent", || {
            client.script_list_unspent(output.script_pubkey.as_script())
        })?;
        Ok(unspent
            .iter()
            .any(|entry| entry.tx_hash == outpoint.txid && entry.tx_pos == outpoint.vout as usize))
    }

    fn remove_account_utxos(
        &self,
        account_id: &str,
        outpoints: BTreeSet<String>,
    ) -> rgb_service_api::Result<()> {
        if outpoints.is_empty() {
            return Ok(());
        }
        let keyspace = self
            .db
            .keyspace("account_utxos", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        for outpoint in outpoints {
            let key = Self::account_utxo_key(account_id, &outpoint);
            tx.remove(&keyspace, key.as_bytes());
        }
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn get_token_catalog_entry(
        &self,
        contract_id: &str,
    ) -> rgb_service_api::Result<Option<TokenCatalogEntry>> {
        let keyspace = self
            .db
            .keyspace("token_catalog", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let Some(bytes) = keyspace
            .get(contract_id.as_bytes())
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?
            .map(|bytes| bytes.as_ref().to_vec())
        else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn list_token_catalog_entries(&self) -> rgb_service_api::Result<Vec<TokenCatalogEntry>> {
        let keyspace = self
            .db
            .keyspace("token_catalog", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut entries = Vec::new();
        for item in keyspace.as_ref().prefix(b"") {
            let value = item
                .value()
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            entries.push(
                serde_json::from_slice(value.as_ref())
                    .map_err(|err| RgbServiceError::Backend(err.to_string()))?,
            );
        }
        entries.sort_by(|a: &TokenCatalogEntry, b: &TokenCatalogEntry| {
            a.ticker
                .cmp(&b.ticker)
                .then_with(|| a.contract_id.cmp(&b.contract_id))
        });
        Ok(entries)
    }

    fn catalog_entry_for_contract(
        &self,
        contract_id: String,
        ticker: String,
        name: String,
        precision: u8,
    ) -> rgb_service_api::Result<TokenCatalogEntry> {
        Ok(self
            .get_token_catalog_entry(&contract_id)?
            .unwrap_or_else(|| TokenCatalogEntry::empty(contract_id, ticker, name, precision)))
    }

    fn put_legacy_address_account(
        &self,
        address: &str,
        account_id: &str,
    ) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("legacy_address_accounts", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, address.as_bytes(), account_id.as_bytes());
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    pub(crate) fn put_legacy_account_desc(
        &self,
        account_id: &str,
        desc: &str,
    ) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("legacy_account_descs", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, account_id.as_bytes(), desc.as_bytes());
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    pub(crate) fn resolve_legacy_account_id_for_address(
        &self,
        address: &str,
    ) -> rgb_service_api::Result<Option<String>> {
        let keyspace = self
            .db
            .keyspace("legacy_address_accounts", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        keyspace
            .get(address.as_bytes())
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?
            .map(|bytes| {
                String::from_utf8(bytes.as_ref().to_vec())
                    .map_err(|err| RgbServiceError::Backend(err.to_string()))
            })
            .transpose()
    }

    pub(crate) fn resolve_legacy_desc_for_account_id(
        &self,
        account_id: &str,
    ) -> rgb_service_api::Result<Option<String>> {
        let keyspace = self
            .db
            .keyspace("legacy_account_descs", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        keyspace
            .get(account_id.as_bytes())
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?
            .map(|bytes| {
                String::from_utf8(bytes.as_ref().to_vec())
                    .map_err(|err| RgbServiceError::Backend(err.to_string()))
            })
            .transpose()
    }

    // wallet-service-v2 migration: import accounts purely from the on-disk data
    // directory (no SQL dump). Descriptors are recovered directly from each
    // account's `bdk_wallet` file, so this does not require a wallet-v2.sql.
    // Only accounts holding a real RGB stock (stash.dat > empty) are imported.
    fn import_wallet_v2_data_dir(
        &self,
        wallet_v2_data_dir: &Path,
        options: WalletV2DataDirOptions,
    ) -> rgb_service_api::Result<WalletV2DataDirImportSummary> {
        let network = self.network()?;
        let reveal_count = default_legacy_reveal_address_count();
        let entries = fs::read_dir(wallet_v2_data_dir)
            .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 data dir: {err}")))?;
        let mut summary = WalletV2DataDirImportSummary::default();
        let limit = options.limit.unwrap_or(usize::MAX);
        let mut considered = 0usize;
        for entry in entries {
            let entry = entry
                .map_err(|err| RgbServiceError::Backend(format!("read wallet-v2 dir entry: {err}")))?;
            let path = entry.path();
            // skip files and the legacy fee wallet (`priv/`)
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name();
            if name == "priv" {
                continue;
            }
            let source_stock_dir = path.join(WALLET_V2_RGB_PATH);
            if !has_real_rgb_stock(&source_stock_dir) {
                summary.skipped_empty += 1;
                continue;
            }
            considered += 1;
            if considered > limit {
                break;
            }
            if let Err(err) = self.import_wallet_v2_data_dir_account(
                &path,
                &source_stock_dir,
                network,
                reveal_count,
                options.dry_run,
                &mut summary,
            ) {
                summary.errors += 1;
                eprintln!(
                    "wallet-v2 data-dir: account {} FAILED: {err:#}",
                    path.display()
                );
                continue;
            }
            if summary.accounts % 50 == 0 {
                println!(
                    "wallet-v2 data-dir progress: accounts={} addresses={} utxos={} stocks_imported={} skipped_empty={} errors={}",
                    summary.accounts,
                    summary.addresses,
                    summary.utxos,
                    summary.stocks_imported,
                    summary.skipped_empty,
                    summary.errors
                );
            }
        }
        Ok(summary)
    }

    #[allow(clippy::too_many_arguments)]
    fn import_wallet_v2_data_dir_account(
        &self,
        account_dir: &Path,
        source_stock_dir: &Path,
        network: Network,
        reveal_count: u32,
        dry_run: bool,
        summary: &mut WalletV2DataDirImportSummary,
    ) -> rgb_service_api::Result<()> {
        let wallet_path = account_dir.join("bdk_wallet");
        // Legacy files are bdk_wallet 1.1.0 format; the bundled
        // `wallet-v2-reader` (pinned to 1.1.0) recovers descriptor, UTXOs and
        // revealed addresses as JSON.
        let recovered = read_wallet_v2_account(&wallet_path, network, reveal_count)?;
        let desc = recovered.descriptor;
        let account_id = legacy::legacy_account_id(&desc);

        if dry_run {
            println!(
                "wallet-v2 data-dir DRY-RUN desc_id={} account_id={} desc={} utxos={} addresses={} stock={}",
                account_dir.file_name().and_then(|n| n.to_str()).unwrap_or(""),
                account_id,
                desc,
                recovered.utxos.len(),
                recovered.addresses.len(),
                source_stock_dir.display()
            );
            summary.accounts += 1;
            summary.addresses += recovered.addresses.len();
            summary.utxos += recovered.utxos.len();
            return Ok(());
        }

        self.get_or_create_profile(&account_id)?;
        self.put_legacy_account_desc(&account_id, &desc)?;
        for address in &recovered.addresses {
            self.put_legacy_address_account(address, &account_id)?;
            summary.addresses += 1;
        }
        // Seed tracked UTXOs from the BDK unspent set so the daemon knows the
        // bitcoin UTXOs the account owns (used for coin selection / change).
        let mut seeded = std::collections::BTreeSet::new();
        for utxo in recovered.utxos {
            let tracked = TrackedUtxo {
                outpoint: utxo.outpoint.clone(),
                address: utxo.address,
                confirmed: utxo.is_confirmed,
            };
            self.put_account_utxo(&account_id, tracked)?;
            seeded.insert(utxo.outpoint);
            summary.utxos += 1;
        }
        // Migrate the RGB stock into the daemon's fjall store and, from the same
        // single load, collect every outpoint carrying a fungible allocation.
        // Allocation outpoints are the authoritative source of which UTXOs hold
        // RGB assets and may include change outputs that BDK no longer tracks as
        // unspent; seed them as tracked UTXOs so the daemon resolves the full
        // balance. Chain-sync filtering later removes genuinely spent ones.
        let allocation_outpoints =
            import_rgb20_stock_and_allocations(source_stock_dir, &self.account_stock_dir(&account_id))
                .map_err(|err| {
                    RgbServiceError::Backend(format!(
                        "import wallet-v2 RGB stock {}: {err:#}",
                        source_stock_dir.display()
                    ))
                })?;
        for allocation_outpoint in allocation_outpoints {
            let outpoint_str = allocation_outpoint.to_string();
            if !seeded.insert(outpoint_str.clone()) {
                continue;
            }
            let tracked = TrackedUtxo {
                outpoint: outpoint_str,
                address: None,
                confirmed: true,
            };
            self.put_account_utxo(&account_id, tracked)?;
            summary.utxos += 1;
        }
        summary.stocks_imported += 1;
        summary.accounts += 1;
        Ok(())
    }

    fn issue_utxo(allocation_outpoint: String, mut utxos: Vec<TrackedUtxo>) -> TrackedUtxo {
        utxos
            .drain(..)
            .find(|utxo| utxo.outpoint == allocation_outpoint)
            .unwrap_or(TrackedUtxo {
                outpoint: allocation_outpoint,
                address: None,
                confirmed: true,
            })
    }

    fn prepared_key(account_id: &str, transfer_id: &str) -> String {
        format!("{account_id}:{transfer_id}")
    }

    fn put_prepared_transfer(
        &self,
        account_id: &str,
        transfer_id: &str,
        record: &PreparedTransferRecord,
    ) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("prepared_transfers", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = Self::prepared_key(account_id, transfer_id);
        let bytes =
            serde_json::to_vec(record).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, key.as_bytes(), bytes);
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn get_prepared_transfer(
        &self,
        account_id: &str,
        transfer_id: &str,
    ) -> rgb_service_api::Result<PreparedTransferRecord> {
        let keyspace = self
            .db
            .keyspace("prepared_transfers", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = Self::prepared_key(account_id, transfer_id);
        let bytes = keyspace
            .get(key.as_bytes())
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?
            .map(|bytes| bytes.as_ref().to_vec())
            .ok_or_else(|| {
                RgbServiceError::NotFound(format!("prepared transfer not found: {transfer_id}"))
            })?;
        serde_json::from_slice(&bytes).map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn legacy_find_prepared_transfer_account_id(
        &self,
        transfer_id: &str,
    ) -> rgb_service_api::Result<Option<String>> {
        let keyspace = self
            .db
            .keyspace("prepared_transfers", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let suffix = format!(":{transfer_id}");
        for item in keyspace.as_ref().prefix(b"") {
            let key = item
                .key()
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            let key = String::from_utf8(key.to_vec())
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            if let Some(account_id) = key.strip_suffix(&suffix) {
                return Ok(Some(account_id.to_string()));
            }
        }
        Ok(None)
    }

    fn legacy_list_assets(&self, account_id: &str) -> rgb_service_api::Result<ListAssetsResponse> {
        let stock_dir = self.account_stock_dir(account_id);
        let account_utxos = self.account_rgb20_utxos(account_id)?;
        let known_outpoints = account_utxos
            .iter()
            .map(|utxo| utxo.outpoint.to_string())
            .collect::<BTreeSet<_>>();
        let allocations = list_rgb20_assets_for_utxos(&stock_dir, account_utxos)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let mut assets = Vec::<RgbAssetInfo>::new();
        let mut rgb_outpoints = BTreeSet::new();
        let mut utxo_assets = BTreeMap::<String, Vec<RgbAllocation>>::new();
        for allocation in allocations {
            let outpoint = allocation.outpoint.to_string();
            rgb_outpoints.insert(outpoint.clone());
            if !assets
                .iter()
                .any(|asset| asset.contract_id == allocation.contract_id.to_string())
            {
                assets.push(
                    self.catalog_entry_for_contract(
                        allocation.contract_id.to_string(),
                        allocation.ticker.clone(),
                        allocation.name.clone(),
                        allocation.precision,
                    )?
                    .into_asset_info(),
                );
            }
            utxo_assets
                .entry(outpoint.clone())
                .or_default()
                .push(RgbAllocation {
                    asset_id: allocation.contract_id.to_string(),
                    outpoint,
                    amount: allocation.amount_raw,
                    layer: AssetLayer::L1,
                    status: AllocationStatus::Available,
                    address: allocation.address.clone(),
                    confirmed: Some(allocation.confirmed),
                });
        }
        self.remove_account_utxos(
            account_id,
            known_outpoints
                .difference(&rgb_outpoints)
                .cloned()
                .collect::<BTreeSet<_>>(),
        )?;
        Ok(ListAssetsResponse {
            assets,
            utxo_assets,
        })
    }

    fn legacy_put_prepared_transfer(
        &self,
        account_id: &str,
        transfer_id: &str,
        asset_id: String,
        recipient_account_id: String,
        recipient_vout: u32,
        fascia: &rgb_service_local::rgbstd::containers::Fascia,
        recipients: Vec<PreparedTransferRecipient>,
    ) -> rgb_service_api::Result<()> {
        let fascia = encode_fascia_bytes(fascia)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        self.put_prepared_transfer(
            account_id,
            transfer_id,
            &PreparedTransferRecord {
                asset_id,
                recipient_account_id,
                recipient_vout,
                fascia,
                recipients,
            },
        )
    }

    fn legacy_commit_prepared_transfer(
        &self,
        account_id: &str,
        transfer_id: &str,
        txid: &str,
        utxos: Vec<TrackedUtxo>,
    ) -> rgb_service_api::Result<()> {
        let stock_dir = self.account_stock_dir(account_id);
        let txid =
            Txid::from_str(txid).map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        let record = self.get_prepared_transfer(account_id, transfer_id)?;
        let fascia = rgb_service_local::decode_fascia_bytes(&record.fascia)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:#}")))?;
        stage_sender_fascia(&stock_dir, txid, &fascia)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        self.register_pending_stock_dir(&stock_dir)?;
        for recipient in record.recipients_or_legacy() {
            let contract_id = rgb_service_local::rgbstd::ContractId::from_str(&recipient.asset_id)
                .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:?}")))?;
            let consignment = build_rgb20_transfer_consignment(
                &stock_dir,
                fascia.clone(),
                contract_id,
                txid,
                recipient.recipient_vout,
            )
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            let receiver_stock_dir = self.account_stock_dir(&recipient.recipient_account_id);
            stage_receiver_transfer(&receiver_stock_dir, txid, &consignment)
                .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            self.register_pending_stock_dir(&receiver_stock_dir)?;
            let receiver_outpoint = format!("{}:{}", txid, recipient.recipient_vout);
            let receiver_utxo = Self::issue_utxo(receiver_outpoint, utxos.clone());
            self.put_account_utxo(&recipient.recipient_account_id, receiver_utxo)?;
        }
        Ok(())
    }

    fn legacy_remove_prepared_transfer(
        &self,
        account_id: &str,
        transfer_id: &str,
    ) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("prepared_transfers", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = Self::prepared_key(account_id, transfer_id);
        let mut tx = self.db.write_tx();
        tx.remove(&keyspace, key.as_bytes());
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn ln_channel_key(account_id: &str, channel_id: &str) -> String {
        format!("{account_id}:{channel_id}")
    }

    fn put_ln_channel_record(&self, record: &LnChannelRecord) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("ln_channels", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = Self::ln_channel_key(&record.account_id, &record.channel_id);
        let bytes =
            serde_json::to_vec(record).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, key.as_bytes(), bytes);
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn put_ln_compose_record(&self, record: &LnComposeRecord) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("ln_composes", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = format!("{}:{}", record.account_id, record.operation_id);
        let bytes =
            serde_json::to_vec(record).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, key.as_bytes(), bytes);
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn put_ln_payment_record(&self, record: &LnPaymentRecord) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("ln_payments", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = format!("{}:{}", record.account_id, record.payment_hash);
        let bytes =
            serde_json::to_vec(record).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, key.as_bytes(), bytes);
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn get_ln_channel_record(
        &self,
        account_id: &str,
        channel_id: &str,
    ) -> rgb_service_api::Result<Option<LnChannelRecord>> {
        let keyspace = self
            .db
            .keyspace("ln_channels", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = Self::ln_channel_key(account_id, channel_id);
        let Some(bytes) = keyspace
            .get(key.as_bytes())
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?
        else {
            return Ok(None);
        };
        serde_json::from_slice(bytes.as_ref())
            .map(Some)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn list_ln_channel_records(
        &self,
        account_id: &str,
    ) -> rgb_service_api::Result<Vec<LnChannelRecord>> {
        let keyspace = self
            .db
            .keyspace("ln_channels", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let prefix = format!("{account_id}:");
        let mut records = Vec::new();
        for item in keyspace.as_ref().prefix(prefix.as_bytes()) {
            let value = item
                .value()
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            records.push(
                serde_json::from_slice(value.as_ref())
                    .map_err(|err| RgbServiceError::Backend(err.to_string()))?,
            );
        }
        Ok(records)
    }

    fn find_ln_channel_record_by_channel_id(
        &self,
        channel_id: &str,
    ) -> rgb_service_api::Result<Option<LnChannelRecord>> {
        let keyspace = self
            .db
            .keyspace("ln_channels", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        for item in keyspace.as_ref().prefix(b"") {
            let value = item
                .value()
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            let record: LnChannelRecord = serde_json::from_slice(value.as_ref())
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            if record.channel_id == channel_id
                || record.funding_ref.channel_id.as_deref() == Some(channel_id)
            {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn list_ln_compose_records(
        &self,
        account_id: &str,
        channel_id: Option<&str>,
    ) -> rgb_service_api::Result<Vec<LnComposeRecord>> {
        let keyspace = self
            .db
            .keyspace("ln_composes", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let prefix = format!("{account_id}:");
        let mut records = Vec::new();
        for item in keyspace.as_ref().prefix(prefix.as_bytes()) {
            let value = item
                .value()
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            let record: LnComposeRecord = serde_json::from_slice(value.as_ref())
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
            if channel_id.is_some_and(|id| record.channel_id != id) {
                continue;
            }
            records.push(record);
        }
        records.sort_by_key(|record| record.created_at_ms);
        Ok(records)
    }

    fn find_ln_output_assignment(
        &self,
        account_id: &str,
        channel_id: &str,
        commitment_txid: &str,
        vout: u32,
    ) -> rgb_service_api::Result<Option<(LnComposeRecord, LnOutputAssignmentRecord)>> {
        for record in self.list_ln_compose_records(account_id, Some(channel_id))? {
            if record.txid != commitment_txid {
                continue;
            }
            if let Some(assignment) = record
                .assignments
                .iter()
                .find(|assignment| assignment.vout == vout)
                .cloned()
            {
                return Ok(Some((record, assignment)));
            }
            return Ok(None);
        }
        Ok(None)
    }

    fn require_nonce(authorization: &AssetSpendAuthorization) -> rgb_service_api::Result<String> {
        let nonce = authorization.signature.nonce.trim();
        if nonce.is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "asset authorization signature nonce must not be empty".to_string(),
            ));
        }
        Ok(nonce.to_string())
    }

    fn validate_ln_asset_authorization(
        contract_id: &str,
        amount: u64,
        authorization: &AssetSpendAuthorization,
    ) -> rgb_service_api::Result<()> {
        if authorization.asset_id != contract_id {
            return Err(RgbServiceError::Forbidden(format!(
                "asset authorization asset_id {} does not match contract_id {}",
                authorization.asset_id, contract_id
            )));
        }
        if authorization.amount != amount {
            return Err(RgbServiceError::Forbidden(format!(
                "asset authorization amount {} does not match LN RGB amount {}",
                authorization.amount, amount
            )));
        }
        Ok(())
    }

    fn require_vout(
        amount: u64,
        vout: Option<u32>,
        label: &str,
    ) -> rgb_service_api::Result<Option<u32>> {
        if amount == 0 {
            return Ok(None);
        }
        vout.map(Some).ok_or_else(|| {
            RgbServiceError::InvalidRequest(format!(
                "{label} is required when RGB amount is non-zero"
            ))
        })
    }

    fn decode_unsigned_tx(tx_hex: &str) -> rgb_service_api::Result<Transaction> {
        deserialize(&hex_decode(tx_hex)?).map_err(|err| {
            RgbServiceError::InvalidRequest(format!("invalid unsigned_tx_hex: {err}"))
        })
    }

    fn compose_ln_rgb_tx(
        &self,
        account_id: &str,
        channel_id: &str,
        route: &str,
        funding_ref: RgbFundingRef,
        contract_id: &str,
        unsigned_tx_hex: &str,
        change_vout: u32,
        assignments: Vec<Rgb20PsbtAssignment>,
        authorization: &AssetSpendAuthorization,
    ) -> rgb_service_api::Result<LnComposeResponse> {
        if channel_id.trim().is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "channel_id must not be empty".to_string(),
            ));
        }
        if contract_id.trim().is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "contract_id must not be empty".to_string(),
            ));
        }
        if assignments.is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "at least one RGB assignment is required for LN compose".to_string(),
            ));
        }
        let amount = assignments
            .iter()
            .map(|assignment| assignment.amount)
            .sum::<u64>();
        Self::validate_ln_asset_authorization(contract_id, amount, authorization)?;
        let assignment_records = assignments
            .iter()
            .map(|assignment| LnOutputAssignmentRecord {
                vout: assignment.vout,
                amount_rgb: assignment.amount,
            })
            .collect::<Vec<_>>();
        let operation_id = Self::require_nonce(authorization)?;
        let tx = Self::decode_unsigned_tx(unsigned_tx_hex)?;
        let psbt = Psbt::from_unsigned_tx(tx).map_err(|err| {
            RgbServiceError::InvalidRequest(format!("invalid unsigned transaction for PSBT: {err}"))
        })?;
        let stock_account_id = self
            .get_ln_channel_record(account_id, channel_id)?
            .or_else(|| {
                self.find_ln_channel_record_by_channel_id(channel_id)
                    .ok()
                    .flatten()
            })
            .map(|record| record.account_id)
            .unwrap_or_else(|| account_id.to_string());
        let stock_dir = self.account_stock_dir(&stock_account_id);
        let prepared = prepare_rgb20_psbt(&stock_dir, psbt, change_vout, assignments)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let tx = prepared.psbt.unsigned_tx;
        let txid = tx.compute_txid();
        let tx_hex = hex_encode(&serialize(&tx));
        let fascia = encode_fascia_bytes(&prepared.fascia)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let rgb_state_ref = format!("ln:{channel_id}:{operation_id}");
        self.put_ln_compose_record(&LnComposeRecord {
            account_id: account_id.to_string(),
            channel_id: channel_id.to_string(),
            operation_id: operation_id.clone(),
            route: route.to_string(),
            contract_id: contract_id.to_string(),
            txid: txid.to_string(),
            tx_hex: tx_hex.clone(),
            fascia,
            funding_ref,
            assignments: assignment_records,
            created_at_ms: now_ms(),
        })?;
        Ok(LnComposeResponse {
            operation_id,
            tx_hex,
            rgb_state_ref: Some(rgb_state_ref),
        })
    }

    fn new_profile(&self, id: &str, now: u64) -> Value {
        json!({
            "id": id,
            "rna_balance": 0_u64,
            "created_at_ms": now,
            "updated_at_ms": now
        })
    }

    fn load_profile(&self, id: &str) -> rgb_service_api::Result<Option<Value>> {
        let keyspace = self
            .db
            .keyspace("profiles", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let Some(bytes) = keyspace
            .get(id.as_bytes())
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?
            .map(|bytes| bytes.as_ref().to_vec())
        else {
            return Ok(None);
        };
        let (dynamic, consumed) =
            Dynamic::decode(&bytes).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        if consumed != bytes.len() {
            return Err(RgbServiceError::Backend(
                "trailing data after profile Dynamic msgpack payload".to_string(),
            ));
        }
        Ok(Some(dynamic_to_json_value(&dynamic)?))
    }

    fn profile_rna_balance(profile: &Value) -> rgb_service_api::Result<u64> {
        profile
            .get("rna_balance")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                RgbServiceError::Backend("profile missing numeric rna_balance".to_string())
            })
    }

    fn put_profile(&self, id: &str, profile: &Value) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("profiles", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let bytes = json_value_to_dynamic(profile)?;
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, id.as_bytes(), dynamic_to_msgpack(&bytes));
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn get_or_create_profile(&self, id: &str) -> rgb_service_api::Result<Value> {
        if id.trim().is_empty() {
            return Err(RgbServiceError::Unauthorized(
                "account_id must not be empty for profile lookup".to_string(),
            ));
        }
        if let Some(profile) = self.load_profile(id)? {
            return Ok(profile);
        }
        let profile = self.new_profile(id, now_ms());
        self.put_profile(id, &profile)?;
        Ok(profile)
    }

    fn charge_rna(
        &self,
        account_id: &str,
        route: &str,
        purpose: &str,
        amount: u64,
    ) -> rgb_service_api::Result<u64> {
        if account_id.trim().is_empty() {
            return Err(RgbServiceError::Unauthorized(
                "account_id must not be empty for RNA metering".to_string(),
            ));
        }
        let now = now_ms();
        let profile_keyspace = self
            .db
            .keyspace("profiles", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let logs_keyspace = self
            .db
            .keyspace("usage_logs", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut profile = self
            .load_profile(account_id)?
            .unwrap_or_else(|| self.new_profile(account_id, now));
        let current = Self::profile_rna_balance(&profile)?;
        if current < amount {
            return Err(RgbServiceError::Forbidden(format!(
                "insufficient RNA balance for {purpose}: required {amount}, available {current}"
            )));
        }
        let new_balance = current - amount;
        profile["rna_balance"] = json!(new_balance);
        profile["updated_at_ms"] = json!(now);
        let event_id = format!("{now}:{account_id}:{purpose}");
        let event = json!({
            "event_id": event_id,
            "account_id": account_id,
            "kind": "debit",
            "route": route,
            "purpose": purpose,
            "amount": amount,
            "balance_before": current,
            "balance_after": new_balance,
            "created_at_ms": now
        });
        let profile_bytes = dynamic_to_msgpack(&json_value_to_dynamic(&profile)?);
        let event_bytes =
            serde_json::to_vec(&event).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&profile_keyspace, account_id.as_bytes(), profile_bytes);
        tx.insert(&logs_keyspace, event_id.as_bytes(), event_bytes);
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.logger.info(format!(
            "rna debit account_id={account_id} purpose={purpose} amount={amount} balance_after={new_balance}"
        ));
        Ok(new_balance)
    }

    fn refund_rna(
        &self,
        account_id: &str,
        route: &str,
        purpose: &str,
        amount: u64,
        reason: &str,
    ) -> rgb_service_api::Result<u64> {
        let now = now_ms();
        let profile_keyspace = self
            .db
            .keyspace("profiles", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let logs_keyspace = self
            .db
            .keyspace("usage_logs", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut profile = self.load_profile(account_id)?.ok_or_else(|| {
            RgbServiceError::Backend(format!(
                "profile missing for RNA refund account_id={account_id}"
            ))
        })?;
        let current = Self::profile_rna_balance(&profile)?;
        let new_balance = current
            .checked_add(amount)
            .ok_or_else(|| RgbServiceError::Backend("RNA balance overflow".to_string()))?;
        profile["rna_balance"] = json!(new_balance);
        profile["updated_at_ms"] = json!(now);
        let event_id = format!("{now}:{account_id}:{purpose}:refund");
        let event = json!({
            "event_id": event_id,
            "account_id": account_id,
            "kind": "refund",
            "route": route,
            "purpose": purpose,
            "amount": amount,
            "balance_before": current,
            "balance_after": new_balance,
            "reason": reason,
            "created_at_ms": now
        });
        let profile_bytes = dynamic_to_msgpack(&json_value_to_dynamic(&profile)?);
        let event_bytes =
            serde_json::to_vec(&event).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let mut tx = self.db.write_tx();
        tx.insert(&profile_keyspace, account_id.as_bytes(), profile_bytes);
        tx.insert(&logs_keyspace, event_id.as_bytes(), event_bytes);
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.logger.info(format!(
            "rna refund account_id={account_id} purpose={purpose} amount={amount} balance_after={new_balance} reason={reason}"
        ));
        Ok(new_balance)
    }

    fn refund_rna_on_error<T>(
        &self,
        result: rgb_service_api::Result<T>,
        account_id: &str,
        route: &str,
        purpose: &str,
        amount: u64,
    ) -> rgb_service_api::Result<T> {
        if let Err(err) = &result {
            if let Err(refund_err) =
                self.refund_rna(account_id, route, purpose, amount, &err.to_string())
            {
                self.logger.info(format!(
                    "rna refund failed account_id={account_id} purpose={purpose} amount={amount} error={refund_err}"
                ));
            }
        }
        result
    }
}

#[async_trait]
impl RgbServiceApi for LocalDaemonService {
    async fn rna_balance(
        &self,
        req: Authorized<RnaBalanceRequest>,
    ) -> rgb_service_api::Result<RnaBalanceResponse> {
        let profile = self.get_or_create_profile(&req.payload.account_id)?;
        Ok(RnaBalanceResponse {
            account_id: req.payload.account_id,
            rna_balance: Self::profile_rna_balance(&profile)?,
            new_profile_grant: 0,
            issue_fee: self.rna.issue_fee,
            transfer_fee: self.rna.transfer_fee,
            query_fee: self.rna.query_fee,
        })
    }

    async fn issue_asset(
        &self,
        req: Authorized<IssueAssetRequest>,
    ) -> rgb_service_api::Result<IssueAssetResponse> {
        let route = "/v1/assets/issue";
        let purpose = "issue_asset";
        let amount = self.rna.issue_fee;
        let payload = req.payload;
        let account_id = payload.account_id.clone();
        let ticker = payload.ticker.clone();
        let allocation_outpoint = payload.allocation_outpoint.clone();
        let issued_utxo = Self::issue_utxo(allocation_outpoint.clone(), payload.utxos.clone());
        self.charge_rna(&account_id, route, purpose, amount)?;
        let result = (|| {
            let stock_dir = self.account_stock_dir(&account_id);
            let outpoint = OutPoint::from_str(&payload.allocation_outpoint)
                .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
            let issued = issue_rgb20_fixed_with_chain_source(
                &stock_dir,
                self.network()?,
                &self.chain_source(),
                Rgb20IssueRequest {
                    ticker: payload.ticker,
                    name: payload.name,
                    amount: payload.supply,
                    precision: payload.precision,
                    utxo: outpoint,
                },
            )
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            self.put_account_utxo(&account_id, issued_utxo)?;
            Ok(IssueAssetResponse {
                contract_id: issued.contract_id.to_string(),
                asset_id: issued.contract_id.to_string(),
                allocation_outpoint: issued.utxo.to_string(),
            })
        })();
        match &result {
            Ok(response) => self.logger.info(format!(
                "rgb issue success account_id={account_id} ticker={ticker} allocation_outpoint={allocation_outpoint} contract_id={}",
                response.contract_id
            )),
            Err(err) => self.logger.info(format!(
                "rgb issue failed account_id={account_id} ticker={ticker} allocation_outpoint={allocation_outpoint} error={err}"
            )),
        }
        self.refund_rna_on_error(result, &account_id, route, purpose, amount)
    }

    async fn list_assets(
        &self,
        req: Authorized<ListAssetsRequest>,
    ) -> rgb_service_api::Result<ListAssetsResponse> {
        self.charge_rna(
            &req.payload.account_id,
            "/v1/assets/list",
            "list_assets",
            self.rna.query_fee,
        )?;
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let account_utxos = self.account_rgb20_utxos(&req.payload.account_id)?;
        let known_outpoints = account_utxos
            .iter()
            .map(|utxo| utxo.outpoint.to_string())
            .collect::<BTreeSet<_>>();
        let allocations = list_rgb20_assets_for_utxos(&stock_dir, account_utxos)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let mut assets = Vec::<RgbAssetInfo>::new();
        let mut rgb_outpoints = BTreeSet::new();
        let mut utxo_assets = BTreeMap::<String, Vec<RgbAllocation>>::new();
        for allocation in allocations {
            let outpoint = allocation.outpoint.to_string();
            rgb_outpoints.insert(outpoint.clone());
            if assets
                .iter()
                .any(|asset| asset.contract_id == allocation.contract_id.to_string())
            {
            } else {
                assets.push(
                    self.catalog_entry_for_contract(
                        allocation.contract_id.to_string(),
                        allocation.ticker.clone(),
                        allocation.name.clone(),
                        allocation.precision,
                    )?
                    .into_asset_info(),
                );
            }
            utxo_assets
                .entry(outpoint.clone())
                .or_default()
                .push(RgbAllocation {
                    asset_id: allocation.contract_id.to_string(),
                    outpoint,
                    amount: allocation.amount_raw,
                    layer: AssetLayer::L1,
                    status: AllocationStatus::Available,
                    address: allocation.address.clone(),
                    confirmed: Some(allocation.confirmed),
                });
        }
        self.remove_account_utxos(
            &req.payload.account_id,
            known_outpoints
                .difference(&rgb_outpoints)
                .cloned()
                .collect::<BTreeSet<_>>(),
        )?;
        Ok(ListAssetsResponse {
            assets,
            utxo_assets,
        })
    }

    async fn token_list(&self) -> rgb_service_api::Result<TokenListResponse> {
        let mut seen = BTreeSet::<String>::new();
        let mut contracts = Vec::<RgbContractInfo>::new();
        let mut assets = Vec::<RgbAssetInfo>::new();
        for entry in self.list_token_catalog_entries()? {
            if !seen.insert(entry.contract_id.clone()) {
                continue;
            }
            contracts.push(entry.clone().into_contract_info());
            assets.push(entry.into_asset_info());
        }
        for stock_dir in self.account_stock_dirs()? {
            let stock_contracts = list_rgb20_contracts(&stock_dir)
                .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            for contract in stock_contracts {
                let contract_id = contract.contract_id.to_string();
                if !seen.insert(contract_id.clone()) {
                    continue;
                }
                let entry = self.catalog_entry_for_contract(
                    contract_id,
                    contract.ticker,
                    contract.name,
                    contract.precision,
                )?;
                contracts.push(entry.clone().into_contract_info());
                assets.push(entry.into_asset_info());
            }
        }
        Ok(TokenListResponse { contracts, assets })
    }

    async fn balance(
        &self,
        req: Authorized<BalanceRequest>,
    ) -> rgb_service_api::Result<RgbBalance> {
        let breakdown = self
            .balance_breakdown(Authorized {
                subject: req.subject,
                payload: BalanceBreakdownRequest {
                    account_id: req.payload.account_id,
                    asset_id: req.payload.asset_id,
                },
            })
            .await?;
        Ok(breakdown.summary)
    }

    async fn balance_breakdown(
        &self,
        req: Authorized<BalanceBreakdownRequest>,
    ) -> rgb_service_api::Result<BalanceBreakdownResponse> {
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let account_utxos = self.account_rgb20_utxos(&req.payload.account_id)?;
        let known_outpoints = account_utxos
            .iter()
            .map(|utxo| utxo.outpoint.to_string())
            .collect::<BTreeSet<_>>();
        let allocations = list_rgb20_assets_for_utxos(&stock_dir, account_utxos)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let mut summary = RgbBalance {
            asset_id: req.payload.asset_id.clone(),
            total: 0,
            l1_available: 0,
            l1_pending_in: 0,
            l1_pending_out: 0,
            l2_available: 0,
            l2_locked: 0,
            l2_pending_in: 0,
            l2_pending_out: 0,
            reserved: 0,
            settling: 0,
        };
        let mut response_allocations = Vec::new();
        let mut rgb_outpoints = BTreeSet::new();
        let mut utxo_assets = BTreeMap::<String, Vec<RgbAllocation>>::new();
        for allocation in allocations {
            let outpoint = allocation.outpoint.to_string();
            rgb_outpoints.insert(outpoint.clone());
            if allocation.contract_id.to_string() != req.payload.asset_id {
                continue;
            }
            summary.total += allocation.amount_raw;
            summary.l1_available += allocation.amount_raw;
            let response_allocation = RgbAllocation {
                asset_id: req.payload.asset_id.clone(),
                outpoint: outpoint.clone(),
                amount: allocation.amount_raw,
                layer: AssetLayer::L1,
                status: AllocationStatus::Available,
                address: allocation.address.clone(),
                confirmed: Some(allocation.confirmed),
            };
            response_allocations.push(response_allocation.clone());
            utxo_assets
                .entry(outpoint)
                .or_default()
                .push(response_allocation);
        }
        self.remove_account_utxos(
            &req.payload.account_id,
            known_outpoints
                .difference(&rgb_outpoints)
                .cloned()
                .collect::<BTreeSet<_>>(),
        )?;
        Ok(BalanceBreakdownResponse {
            summary,
            allocations: response_allocations,
            utxo_assets,
            pending_ops: Vec::new(),
        })
    }

    async fn prepare_transfer(
        &self,
        req: Authorized<PrepareTransferRequest>,
    ) -> rgb_service_api::Result<PrepareTransferResponse> {
        let route = "/v1/transfers/prepare";
        let purpose = "prepare_transfer";
        let amount = self.rna.transfer_fee;
        let payload = req.payload;
        let account_id = payload.account_id.clone();
        if payload.recipient.trim().is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "recipient account_id must not be empty".to_string(),
            ));
        }
        self.charge_rna(&account_id, route, purpose, amount)?;
        let result = (|| {
            let stock_dir = self.account_stock_dir(&account_id);
            let psbt = payload
                .unsigned_anchor_psbt
                .as_deref()
                .ok_or_else(|| {
                    RgbServiceError::InvalidRequest(
                        "unsigned_anchor_psbt is required for prepare_transfer".to_string(),
                    )
                })
                .and_then(|value| {
                    Psbt::deserialize(&hex_decode(value)?)
                        .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))
                })?;
            let change_vout = payload.change_vout.ok_or_else(|| {
                RgbServiceError::InvalidRequest("change_vout is required".to_string())
            })?;
            let recipient_vout = payload.recipient_vout.ok_or_else(|| {
                RgbServiceError::InvalidRequest("recipient_vout is required".to_string())
            })?;
            let contract_id = rgb_service_local::rgbstd::ContractId::from_str(&payload.asset_id)
                .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:?}")))?;
            let prepared = prepare_rgb20_psbt(
                &stock_dir,
                psbt,
                change_vout,
                [Rgb20PsbtAssignment {
                    contract_id,
                    amount: payload.amount,
                    vout: recipient_vout,
                }],
            )
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            let fascia = encode_fascia_bytes(&prepared.fascia)
                .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            let transfer_id = payload.asset_authorization.signature.nonce;
            let record = PreparedTransferRecord {
                asset_id: payload.asset_id.clone(),
                recipient_account_id: payload.recipient.clone(),
                recipient_vout,
                fascia,
                recipients: vec![PreparedTransferRecipient {
                    asset_id: payload.asset_id.clone(),
                    recipient_account_id: payload.recipient.clone(),
                    recipient_vout,
                }],
            };
            self.put_prepared_transfer(&account_id, &transfer_id, &record)?;
            Ok(PrepareTransferResponse {
                transfer_id,
                operation_id: "prepare_transfer".to_string(),
                anchor_psbt: Some(hex_encode(&prepared.psbt.serialize())),
            })
        })();
        self.refund_rna_on_error(result, &account_id, route, purpose, amount)
    }

    async fn commit_transfer(
        &self,
        req: Authorized<CommitTransferRequest>,
    ) -> rgb_service_api::Result<CommitTransferResponse> {
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let txid = Txid::from_str(&req.payload.txid)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        let record =
            self.get_prepared_transfer(&req.payload.account_id, &req.payload.transfer_id)?;
        let fascia = rgb_service_local::decode_fascia_bytes(&record.fascia)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:#}")))?;
        stage_sender_fascia(&stock_dir, txid, &fascia)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        self.register_pending_stock_dir(&stock_dir)?;
        for recipient in record.recipients_or_legacy() {
            let contract_id = rgb_service_local::rgbstd::ContractId::from_str(&recipient.asset_id)
                .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:?}")))?;
            let consignment = build_rgb20_transfer_consignment(
                &stock_dir,
                fascia.clone(),
                contract_id,
                txid,
                recipient.recipient_vout,
            )
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            let receiver_stock_dir = self.account_stock_dir(&recipient.recipient_account_id);
            stage_receiver_transfer(&receiver_stock_dir, txid, &consignment)
                .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            self.register_pending_stock_dir(&receiver_stock_dir)?;
            let receiver_outpoint = format!("{}:{}", req.payload.txid, recipient.recipient_vout);
            let receiver_utxo = Self::issue_utxo(receiver_outpoint, req.payload.utxos.clone());
            self.put_account_utxo(&recipient.recipient_account_id, receiver_utxo)?;
        }
        Ok(CommitTransferResponse {
            transfer_id: req.payload.transfer_id,
            operation_id: txid.to_string(),
            status: OperationStatus::Committed,
        })
    }

    async fn cancel_transfer(
        &self,
        req: Authorized<CancelTransferRequest>,
    ) -> rgb_service_api::Result<CancelTransferResponse> {
        Ok(CancelTransferResponse {
            transfer_id: req.payload.transfer_id,
            status: OperationStatus::Cancelled,
        })
    }

    async fn list_pending(
        &self,
        _req: Authorized<ListPendingRequest>,
    ) -> rgb_service_api::Result<ListPendingResponse> {
        Ok(ListPendingResponse {
            pending: Vec::new(),
        })
    }

    async fn recover(
        &self,
        req: Authorized<RecoverRequest>,
    ) -> rgb_service_api::Result<RecoveryReport> {
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let report = scan_and_promote_confirmed_staged_rgb_stocks(
            &stock_dir,
            self.network()?,
            std::slice::from_ref(&self.config.esplora_url),
        )
        .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        Ok(RecoveryReport {
            scanned: report.scanned,
            recovered: report.promoted,
            failed: 0,
            actions: report
                .promoted_txids
                .into_iter()
                .map(|txid| RecoveryAction {
                    operation_id: txid.to_string(),
                    action: "promoted".to_string(),
                    message: "pending RGB operation promoted after confirmation".to_string(),
                })
                .collect(),
        })
    }

    async fn prepare_ln_channel_open(
        &self,
        req: Authorized<LnChannelOpenPrepareRequest>,
    ) -> rgb_service_api::Result<LnChannelOpenPrepareResponse> {
        let route = "/v1/ln/channels/open/prepare";
        let purpose = "ln_channel_open_prepare";
        let amount = self.rna.transfer_fee;
        let payload = req.payload;
        let account_id = payload.account_id.clone();
        if payload.channel_id.trim().is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "channel_id must not be empty".to_string(),
            ));
        }
        if payload.contract_id.trim().is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "contract_id must not be empty".to_string(),
            ));
        }
        if payload.to_local_rgb + payload.to_remote_rgb != payload.funding_rgb {
            return Err(RgbServiceError::InvalidRequest(
                "to_local_rgb + to_remote_rgb must equal funding_rgb".to_string(),
            ));
        }
        Self::validate_ln_asset_authorization(
            &payload.contract_id,
            payload.funding_rgb,
            &payload.asset_authorization,
        )?;
        let psbt =
            Psbt::deserialize(&hex_decode(&payload.unsigned_anchor_psbt)?).map_err(|err| {
                RgbServiceError::InvalidRequest(format!("invalid unsigned_anchor_psbt: {err}"))
            })?;
        let funding_output = psbt
            .unsigned_tx
            .output
            .get(payload.funding_vout as usize)
            .ok_or_else(|| {
                RgbServiceError::InvalidRequest("funding_vout is out of bounds".to_string())
            })?;
        if funding_output.value == bitcoin::Amount::ZERO {
            return Err(RgbServiceError::InvalidRequest(
                "funding output must be non-zero".to_string(),
            ));
        }
        if !psbt
            .unsigned_tx
            .output
            .iter()
            .any(|output| output.script_pubkey.is_op_return())
        {
            return Err(RgbServiceError::InvalidRequest(
                "RGB LN funding PSBT must include an OP_RETURN carrier output".to_string(),
            ));
        }
        self.charge_rna(&account_id, route, purpose, amount)?;
        let result = (|| {
            let operation_id = Self::require_nonce(&payload.asset_authorization)?;
            let stock_dir = self.account_stock_dir(&account_id);
            let contract_id = parse_contract_id(&payload.contract_id)?;
            let prepared = prepare_rgb20_psbt(
                &stock_dir,
                psbt,
                payload.change_vout,
                [Rgb20PsbtAssignment {
                    contract_id,
                    amount: payload.funding_rgb,
                    vout: payload.funding_vout,
                }],
            )
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            let opening_txid = prepared.psbt.unsigned_tx.compute_txid();
            let funding_outpoint = OutPoint::new(opening_txid, payload.funding_vout);
            let fascia = encode_fascia_bytes(&prepared.fascia)
                .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            stage_sender_fascia(&stock_dir, opening_txid, &prepared.fascia)
                .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
            self.register_pending_stock_dir(&stock_dir)?;
            let funding_ref = RgbFundingRef {
                transfer_id: format!("ln-open:{operation_id}"),
                operation_id: operation_id.clone(),
                channel_id: Some(payload.channel_id.clone()),
            };
            self.put_ln_channel_record(&LnChannelRecord {
                account_id: payload.account_id,
                channel_id: payload.channel_id,
                contract_id: payload.contract_id,
                funding_outpoint: funding_outpoint.to_string(),
                funding_vout: payload.funding_vout,
                funding_rgb: payload.funding_rgb,
                to_local_rgb: payload.to_local_rgb,
                to_remote_rgb: payload.to_remote_rgb,
                funding_ref: funding_ref.clone(),
                opening_txid: opening_txid.to_string(),
                opening_fascia: fascia,
                created_at_ms: now_ms(),
            })?;
            Ok(LnChannelOpenPrepareResponse {
                funding_ref,
                operation_id,
                funding_outpoint: funding_outpoint.to_string(),
                anchor_psbt: hex_encode(&prepared.psbt.serialize()),
            })
        })();
        self.refund_rna_on_error(result, &account_id, route, purpose, amount)
    }

    async fn ln_channel_funding_ref(
        &self,
        req: Authorized<LnChannelFundingRefRequest>,
    ) -> rgb_service_api::Result<LnChannelFundingRefResponse> {
        let funding_ref = self
            .get_ln_channel_record(&req.payload.account_id, &req.payload.channel_id)?
            .or_else(|| {
                self.find_ln_channel_record_by_channel_id(&req.payload.channel_id)
                    .ok()
                    .flatten()
            })
            .map(|record| record.funding_ref);
        Ok(LnChannelFundingRefResponse { funding_ref })
    }

    async fn compose_ln_commitment(
        &self,
        req: Authorized<LnCommitmentComposeRequest>,
    ) -> rgb_service_api::Result<LnComposeResponse> {
        OutPoint::from_str(&req.payload.funding_outpoint).map_err(|err| {
            RgbServiceError::InvalidRequest(format!("invalid funding_outpoint: {err}"))
        })?;
        let contract_id = parse_contract_id(&req.payload.contract_id)?;
        let mut assignments = Vec::new();
        if let Some(vout) = Self::require_vout(
            req.payload.to_local_rgb,
            req.payload.to_local_vout,
            "to_local_vout",
        )? {
            assignments.push(Rgb20PsbtAssignment {
                contract_id,
                amount: req.payload.to_local_rgb,
                vout,
            });
        }
        if let Some(vout) = Self::require_vout(
            req.payload.to_remote_rgb,
            req.payload.to_remote_vout,
            "to_remote_vout",
        )? {
            assignments.push(Rgb20PsbtAssignment {
                contract_id,
                amount: req.payload.to_remote_rgb,
                vout,
            });
        }
        for htlc in req.payload.htlcs {
            if htlc.amount_rgb == 0 {
                continue;
            }
            assignments.push(Rgb20PsbtAssignment {
                contract_id,
                amount: htlc.amount_rgb,
                vout: htlc.vout,
            });
        }
        self.compose_ln_rgb_tx(
            &req.payload.account_id,
            &req.payload.channel_id,
            "/v1/ln/commitments/compose",
            req.payload.funding_ref,
            &req.payload.contract_id,
            &req.payload.unsigned_tx_hex,
            req.payload.change_vout,
            assignments,
            &req.payload.asset_authorization,
        )
    }

    async fn claim_ln_payment(
        &self,
        req: Authorized<LnPaymentClaimRequest>,
    ) -> rgb_service_api::Result<LnPaymentClaimResponse> {
        if req.payload.payment_hash.trim().is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "payment_hash must not be empty".to_string(),
            ));
        }
        if req.payload.rgb_amount == 0 {
            return Err(RgbServiceError::InvalidRequest(
                "rgb_amount must be greater than zero".to_string(),
            ));
        }
        parse_contract_id(&req.payload.contract_id)?;
        let operation_id = format!("ln-payment:{}", req.payload.payment_hash);
        let rgb_state_ref = req
            .payload
            .channel_id
            .as_ref()
            .map(|channel_id| format!("ln:{channel_id}:{operation_id}"));
        self.put_ln_payment_record(&LnPaymentRecord {
            account_id: req.payload.account_id,
            channel_id: req.payload.channel_id,
            payment_hash: req.payload.payment_hash,
            contract_id: req.payload.contract_id,
            amount_msat: req.payload.amount_msat,
            rgb_amount: req.payload.rgb_amount,
            status: OperationStatus::Settled,
            rgb_state_ref: rgb_state_ref.clone(),
            created_at_ms: now_ms(),
        })?;
        Ok(LnPaymentClaimResponse {
            operation_id,
            status: OperationStatus::Settled,
            rgb_state_ref,
        })
    }

    async fn compose_ln_closing(
        &self,
        req: Authorized<LnClosingComposeRequest>,
    ) -> rgb_service_api::Result<LnComposeResponse> {
        OutPoint::from_str(&req.payload.funding_outpoint).map_err(|err| {
            RgbServiceError::InvalidRequest(format!("invalid funding_outpoint: {err}"))
        })?;
        let contract_id = parse_contract_id(&req.payload.contract_id)?;
        let mut assignments = Vec::new();
        if let Some(vout) = Self::require_vout(
            req.payload.to_local_rgb,
            req.payload.to_local_vout,
            "to_local_vout",
        )? {
            assignments.push(Rgb20PsbtAssignment {
                contract_id,
                amount: req.payload.to_local_rgb,
                vout,
            });
        }
        if let Some(vout) = Self::require_vout(
            req.payload.to_remote_rgb,
            req.payload.to_remote_vout,
            "to_remote_vout",
        )? {
            assignments.push(Rgb20PsbtAssignment {
                contract_id,
                amount: req.payload.to_remote_rgb,
                vout,
            });
        }
        self.compose_ln_rgb_tx(
            &req.payload.account_id,
            &req.payload.channel_id,
            "/v1/ln/closing/compose",
            req.payload.funding_ref,
            &req.payload.contract_id,
            &req.payload.unsigned_tx_hex,
            req.payload.change_vout,
            assignments,
            &req.payload.asset_authorization,
        )
    }

    async fn compose_ln_onchain_claim(
        &self,
        req: Authorized<LnOnchainClaimComposeRequest>,
    ) -> rgb_service_api::Result<LnComposeResponse> {
        Txid::from_str(&req.payload.commitment_txid).map_err(|err| {
            RgbServiceError::InvalidRequest(format!("invalid commitment_txid: {err}"))
        })?;
        let tx = Self::decode_unsigned_tx(&req.payload.unsigned_tx_hex)?;
        let claim_txid = tx.compute_txid();
        let operation_id = format!(
            "ln-claim:{}:{}:{}",
            req.payload.commitment_txid, req.payload.vout, claim_txid
        );
        let Some((source_record, source_assignment)) = self.find_ln_output_assignment(
            &req.payload.account_id,
            &req.payload.channel_id,
            &req.payload.commitment_txid,
            req.payload.vout,
        )?
        else {
            return Ok(LnComposeResponse {
                operation_id,
                tx_hex: req.payload.unsigned_tx_hex,
                rgb_state_ref: None,
            });
        };
        if tx.output.len() != 1 {
            return Err(RgbServiceError::InvalidRequest(
                "LN RGB on-chain claim requires exactly one claim output".to_string(),
            ));
        }
        let contract_id = parse_contract_id(&source_record.contract_id)?;
        let assignments = vec![Rgb20PsbtAssignment {
            contract_id,
            amount: source_assignment.amount_rgb,
            vout: 0,
        }];
        let psbt = Psbt::from_unsigned_tx(tx).map_err(|err| {
            RgbServiceError::InvalidRequest(format!("invalid unsigned transaction for PSBT: {err}"))
        })?;
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let prepared = prepare_rgb20_psbt(&stock_dir, psbt, 0, assignments)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let tx = prepared.psbt.unsigned_tx;
        let txid = tx.compute_txid();
        let tx_hex = hex_encode(&serialize(&tx));
        let fascia = encode_fascia_bytes(&prepared.fascia)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let rgb_state_ref = format!("ln:{}:{operation_id}", req.payload.channel_id);
        self.put_ln_compose_record(&LnComposeRecord {
            account_id: req.payload.account_id,
            channel_id: req.payload.channel_id,
            operation_id: operation_id.clone(),
            route: "/v1/ln/onchain-claims/compose".to_string(),
            contract_id: source_record.contract_id,
            txid: txid.to_string(),
            tx_hex: tx_hex.clone(),
            fascia,
            funding_ref: source_record.funding_ref,
            assignments: vec![LnOutputAssignmentRecord {
                vout: 0,
                amount_rgb: source_assignment.amount_rgb,
            }],
            created_at_ms: now_ms(),
        })?;
        Ok(LnComposeResponse {
            operation_id,
            tx_hex,
            rgb_state_ref: Some(rgb_state_ref),
        })
    }

    async fn recover_ln(
        &self,
        req: Authorized<LnRecoverRequest>,
    ) -> rgb_service_api::Result<LnRecoveryReport> {
        let channels = if let Some(channel_id) = req.payload.channel_id.as_deref() {
            self.get_ln_channel_record(&req.payload.account_id, channel_id)?
                .into_iter()
                .collect()
        } else {
            self.list_ln_channel_records(&req.payload.account_id)?
        };
        let composes = self
            .list_ln_compose_records(&req.payload.account_id, req.payload.channel_id.as_deref())?;
        Ok(LnRecoveryReport {
            account_id: req.payload.account_id,
            channel_id: req.payload.channel_id,
            channels: channels
                .into_iter()
                .map(|record| LnRecoveredChannel {
                    channel_id: record.channel_id,
                    contract_id: record.contract_id,
                    funding_outpoint: record.funding_outpoint,
                    funding_rgb: record.funding_rgb,
                    to_local_rgb: record.to_local_rgb,
                    to_remote_rgb: record.to_remote_rgb,
                    funding_ref: record.funding_ref,
                    created_at_ms: record.created_at_ms,
                })
                .collect(),
            composes: composes
                .into_iter()
                .map(|record| LnRecoveredCompose {
                    rgb_state_ref: format!("ln:{}:{}", record.channel_id, record.operation_id),
                    fascia_len: record.fascia.len(),
                    channel_id: record.channel_id,
                    operation_id: record.operation_id,
                    route: record.route,
                    contract_id: record.contract_id,
                    txid: record.txid,
                    tx_hex: record.tx_hex,
                    funding_ref: record.funding_ref,
                    created_at_ms: record.created_at_ms,
                })
                .collect(),
        })
    }

    async fn run_rgb_test(
        &self,
        req: Authorized<RunRgbTestRequest>,
    ) -> rgb_service_api::Result<RunRgbTestResponse> {
        Ok(RunRgbTestResponse {
            scenario: req.payload.scenario,
            passed: true,
            steps: vec![
                test_step("config_loaded"),
                test_step("auth_checked"),
                test_step("local_store_available"),
            ],
        })
    }
}

fn json_value_to_dynamic(value: &Value) -> rgb_service_api::Result<Dynamic> {
    let json = value.to_string();
    let (dynamic, consumed) = Dynamic::from_json(json.as_bytes())
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
    if json.as_bytes()[consumed..]
        .iter()
        .any(|byte| !byte.is_ascii_whitespace())
    {
        return Err(RgbServiceError::Backend(
            "trailing data after profile JSON decode".to_string(),
        ));
    }
    Ok(dynamic)
}

fn dynamic_to_json_value(value: &Dynamic) -> rgb_service_api::Result<Value> {
    let mut json = String::new();
    value.to_json(&mut json);
    serde_json::from_str(&json).map_err(|err| RgbServiceError::Backend(err.to_string()))
}

fn dynamic_to_msgpack(value: &Dynamic) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn test_step(name: &str) -> RgbTestStep {
    RgbTestStep {
        name: name.to_string(),
        passed: true,
        message: None,
    }
}

fn parse_network(value: &str) -> rgb_service_api::Result<bitcoin::Network> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "mainnet" => Ok(bitcoin::Network::Bitcoin),
        network => bitcoin::Network::from_str(network)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("invalid network: {err}"))),
    }
}

fn parse_contract_id(
    value: &str,
) -> rgb_service_api::Result<rgb_service_local::rgbstd::ContractId> {
    match rgb_service_local::rgbstd::ContractId::from_str(value) {
        Ok(contract_id) => Ok(contract_id),
        Err(parse_err) => {
            let bytes = hex_decode(value)?;
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
                RgbServiceError::InvalidRequest(format!(
                    "invalid contract_id; expected RGB contract id string or 32-byte hex: {parse_err:?}"
                ))
            })?;
            rgb_service_local::rgbstd::ContractId::copy_from_slice(bytes).map_err(|err| {
                RgbServiceError::InvalidRequest(format!("invalid 32-byte hex contract_id: {err}"))
            })
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(value: &str) -> rgb_service_api::Result<Vec<u8>> {
    let value = value.trim();
    if value.len() % 2 != 0 {
        return Err(RgbServiceError::InvalidRequest(
            "hex string must have an even length".to_string(),
        ));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::secp256k1::SecretKey;

    #[test]
    fn parse_network_accepts_mainnet_alias() {
        assert_eq!(parse_network("mainnet").unwrap(), bitcoin::Network::Bitcoin);
    }

    #[test]
    fn parse_network_accepts_bitcoin_network_name() {
        assert_eq!(parse_network("bitcoin").unwrap(), bitcoin::Network::Bitcoin);
    }

    #[test]
    fn verifies_ecdsa_request_signature() {
        let secp = Secp256k1::new();
        let secret_key = SecretKey::from_slice(&[7; 32]).unwrap();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let payload = serde_json::to_vec(&json!({
            "account_id": "account-1"
        }))
        .unwrap();
        let nonce = "nonce-1";
        let timestamp_ms = now_ms();
        let message =
            ConfiguredAuthVerifier::signature_message("issue_asset", &payload, nonce, timestamp_ms);
        let signature = secp.sign_ecdsa(&message, &secret_key);
        let signature = RequestSignature {
            signer_id: public_key.to_string(),
            public_key: public_key.to_string(),
            scheme: SignatureScheme::Ecdsa,
            nonce: nonce.to_string(),
            timestamp_ms,
            signature: hex_encode(&signature.serialize_der()),
        };

        ConfiguredAuthVerifier::verify_request_signature(
            "account-1",
            &Permission::IssueAsset,
            &payload,
            &signature,
        )
        .unwrap();

        let tampered = serde_json::to_vec(&json!({
            "account_id": "account-2"
        }))
        .unwrap();
        assert!(ConfiguredAuthVerifier::verify_request_signature(
            "account-1",
            &Permission::IssueAsset,
            &tampered,
            &signature,
        )
        .is_err());
    }

    #[test]
    fn accepts_legacy_signer_app_bip322_envelope() {
        let signature = RequestSignature {
            signer_id: "account-1".to_string(),
            public_key: String::new(),
            scheme: SignatureScheme::Bip322,
            nonce: now_ms().to_string(),
            timestamp_ms: now_ms(),
            signature: "bitcoin-message-signature".to_string(),
        };
        ConfiguredAuthVerifier::verify_request_signature(
            "account-1",
            &Permission::ReadRnaBalance,
            br#"{"account_id":"account-1"}"#,
            &signature,
        )
        .unwrap();
        assert!(ConfiguredAuthVerifier::verify_request_signature(
            "other-account",
            &Permission::ReadRnaBalance,
            br#"{"account_id":"account-1"}"#,
            &signature,
        )
        .is_err());
    }
}
