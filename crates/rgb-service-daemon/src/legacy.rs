use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use axum::{
    body::Body,
    extract::{ConnectInfo, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use bdk_electrum::electrum_client::{self, ElectrumApi};
use bdk_electrum::BdkElectrumClient;
use bdk_esplora::EsploraExt;
use bdk_wallet::{
    bitcoin::{
        consensus::deserialize,
        hashes::{sha256, Hash},
        Address, Amount, FeeRate, OutPoint, Psbt, ScriptBuf,
    },
    descriptor::ExtendedDescriptor,
    file_store, ChangeSet, KeychainKind, PersistedWallet, TxOrdering, Wallet,
};
use rgb_service_api::{RgbAssetInfo, RgbServiceError, TrackedUtxo};
use rgb_service_local::{
    is_electrum_url, list_rgb20_assets_for_utxos, normalize_electrum_url, prepare_rgb20_psbt,
    select_rgb20_inputs, Rgb20PsbtAssignment, Rgb20TrackedUtxo,
};
use serde::{Deserialize, Serialize};

use crate::{LegacyConfig, LocalDaemonService, PreparedTransferRecipient};

const LEGACY_BDK_MAGIC: &[u8] = b"RgbDaemonLegacyBdk";
const BDK_FILE: &str = "bdk_wallet";
const DEFAULT_RGB_DUST_SATS: u64 = 1000;

#[derive(Clone)]
struct LegacyState {
    service: Arc<LocalDaemonService>,
    config: LegacyConfig,
}

pub(crate) fn router(service: Arc<LocalDaemonService>, config: LegacyConfig) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/account/create", put(create_account))
        .route("/asset/list", get(asset_list))
        .route("/asset", get(query_asset))
        .route("/asset/internal/issue", post(issue_asset))
        .route("/estimate/gas", get(estimate_gas))
        .route("/get_fee", get(get_mempool_info))
        .route("/transfer/psbt", post(transfer_psbt))
        .route("/transfer/callback", post(transfer_callback))
        .route("/transfer/cancel", post(transfer_cancel))
        .route_layer(middleware::from_fn_with_state(
            config.clone(),
            legacy_allowlist_middleware,
        ))
        .with_state(LegacyState { service, config })
}

async fn legacy_allowlist_middleware(
    State(config): State<LegacyConfig>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if !config.enabled {
        return Err(StatusCode::NOT_FOUND);
    }
    if legacy_ip_allowed(&config, remote.ip()) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

fn legacy_ip_allowed(config: &LegacyConfig, ip: IpAddr) -> bool {
    if config.allowed_ips.is_empty() {
        return true;
    }
    if config.allow_loopback && ip.is_loopback() {
        return true;
    }
    config
        .allowed_ips
        .iter()
        .filter_map(|value| value.parse::<IpAddr>().ok())
        .any(|allowed| allowed == ip)
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Deserialize)]
struct CreateAccountReq {
    desc: String,
}

async fn create_account(
    State(state): State<LegacyState>,
    Json(req): Json<CreateAccountReq>,
) -> Result<impl IntoResponse, LegacyHttpError> {
    let account_id = legacy_account_id(&req.desc);
    let _wallet = open_legacy_wallet(&state.service, &state.config, &req.desc)?;
    state.service.get_or_create_profile(&account_id)?;
    state
        .service
        .put_legacy_account_desc(&account_id, &req.desc)?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct AssetListQuery {
    contract_id: Option<String>,
    address: Option<String>,
    desc: Option<String>,
}

#[derive(Serialize)]
struct LegacyAssetInfo {
    ticker: String,
    name: Option<String>,
    precision: i16,
    contract_id: String,
    supply: i64,
    utxo: String,
    contract_type: String,
    ext: Option<serde_json::Value>,
    ext_uptime: Option<String>,
}

async fn asset_list(
    State(state): State<LegacyState>,
    Query(query): Query<AssetListQuery>,
) -> Result<Json<Vec<LegacyAssetInfo>>, LegacyHttpError> {
    let issuer_desc = legacy_asset_list_issuer_desc(&state.service, &query)?;
    let mut entries = state.service.list_token_catalog_entries()?;
    entries.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let assets = entries
        .into_iter()
        .filter(|entry| {
            query
                .contract_id
                .as_deref()
                .is_none_or(|contract_id| contract_id == entry.contract_id)
                && issuer_desc
                    .as_deref()
                    .is_none_or(|desc| desc == entry.issuer_desc)
        })
        .map(|entry| entry.into_asset_info())
        .map(legacy_asset_info)
        .collect();
    Ok(Json(assets))
}

fn legacy_asset_list_issuer_desc(
    service: &LocalDaemonService,
    query: &AssetListQuery,
) -> Result<Option<String>, LegacyHttpError> {
    if let Some(desc) = query.desc.clone() {
        return Ok(Some(desc));
    }
    let Some(address) = query.address.as_deref() else {
        return Ok(None);
    };
    let account_id = service
        .resolve_legacy_account_id_for_address(address)?
        .ok_or_else(|| RgbServiceError::NotFound(format!("legacy address not found: {address}")))?;
    service
        .resolve_legacy_desc_for_account_id(&account_id)?
        .map(Some)
        .ok_or_else(|| {
            RgbServiceError::NotFound(format!("legacy desc not found for account: {account_id}"))
                .into()
        })
}

fn legacy_asset_info(asset: RgbAssetInfo) -> LegacyAssetInfo {
    LegacyAssetInfo {
        ticker: asset.ticker,
        name: Some(asset.name),
        precision: i16::from(asset.precision),
        contract_id: asset.contract_id,
        supply: asset.supply.unwrap_or_default() as i64,
        utxo: asset.issue_utxo,
        contract_type: asset.contract_type,
        ext: asset.ext,
        ext_uptime: None,
    }
}

#[derive(Deserialize)]
struct QueryAssetReq {
    desc: Option<String>,
    address: Option<String>,
    contract_id: Option<String>,
}

#[derive(Serialize)]
struct LegacyAllocation {
    contract_id: String,
    ticker: Option<String>,
    rgb_amount: u64,
    address: Option<String>,
    status: String,
    decimal: Option<i16>,
    txid: Option<String>,
}

async fn query_asset(
    State(state): State<LegacyState>,
    Query(query): Query<QueryAssetReq>,
) -> Result<Json<BTreeMap<String, Vec<LegacyAllocation>>>, LegacyHttpError> {
    let account_ids = legacy_query_account_ids(&state, &query)?;
    let mut result = BTreeMap::<String, Vec<LegacyAllocation>>::new();
    let mut seen = HashSet::<(String, String, Option<String>)>::new();
    for account_id in account_ids {
        let list = state.service.legacy_list_assets(&account_id)?;
        let metadata = list
            .assets
            .into_iter()
            .map(|asset| (asset.contract_id.clone(), asset))
            .collect::<HashMap<_, _>>();
        for (outpoint, allocations) in list.utxo_assets {
            for allocation in allocations {
                if query
                    .address
                    .as_deref()
                    .is_some_and(|address| allocation.address.as_deref() != Some(address))
                {
                    continue;
                }
                if query
                    .contract_id
                    .as_deref()
                    .is_some_and(|contract_id| contract_id != allocation.asset_id)
                {
                    continue;
                }
                let Some(asset) = metadata.get(&allocation.asset_id) else {
                    continue;
                };
                if !seen.insert((
                    outpoint.clone(),
                    allocation.asset_id.clone(),
                    allocation.address.clone(),
                )) {
                    continue;
                }
                result
                    .entry(outpoint.clone())
                    .or_default()
                    .push(LegacyAllocation {
                        contract_id: asset.contract_id.clone(),
                        ticker: Some(asset.ticker.clone()),
                        rgb_amount: allocation.amount,
                        address: allocation.address,
                        status: if allocation.confirmed.unwrap_or(true) {
                            "Confirmed".to_string()
                        } else {
                            "Pending".to_string()
                        },
                        decimal: Some(i16::from(asset.precision)),
                        txid: outpoint.split(':').next().map(ToString::to_string),
                    });
            }
        }
    }
    Ok(Json(result))
}

#[derive(Serialize)]
struct LegacyFeeInfo {
    ticker: &'static str,
    contract_id: String,
    amount: u64,
    precision: u8,
}

async fn estimate_gas(
    State(state): State<LegacyState>,
) -> Result<Json<Option<LegacyFeeInfo>>, LegacyHttpError> {
    let amount = legacy_rgb_fee_amount(&state);
    let fee = state
        .service
        .list_token_catalog_entries()?
        .into_iter()
        .find(|entry| entry.ticker == "RNA")
        .map(|entry| LegacyFeeInfo {
            ticker: "RNA",
            contract_id: entry.contract_id,
            amount,
            precision: entry.precision,
        });
    Ok(Json(fee))
}

async fn get_mempool_info() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "fastestFee": 1,
        "halfHourFee": 1,
        "hourFee": 1,
        "economyFee": 1,
        "minimumFee": 1
    }))
}

fn legacy_query_account_ids(
    state: &LegacyState,
    query: &QueryAssetReq,
) -> Result<Vec<String>, LegacyHttpError> {
    if let Some(desc) = query.desc.as_deref() {
        let mut account_ids = Vec::<String>::new();
        let mut seen = HashSet::<String>::new();
        let desc_account_id = legacy_account_id(desc);
        seen.insert(desc_account_id.clone());
        account_ids.push(desc_account_id);

        let descriptor = ExtendedDescriptor::from_str(desc)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        let wallet = Wallet::create_single(descriptor)
            .network(state.service.network()?)
            .create_wallet_no_persist()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        for index in 0..state.config.reveal_address_count.max(1) {
            let address = wallet
                .peek_address(KeychainKind::External, index)
                .address
                .to_string();
            if let Some(account_id) = state.service.resolve_legacy_account_id_for_address(&address)?
            {
                if seen.insert(account_id.clone()) {
                    account_ids.push(account_id);
                }
            }
        }
        return Ok(account_ids);
    }
    if let Some(address) = query.address.as_deref() {
        if let Some(account_id) = state
            .service
            .resolve_legacy_account_id_for_address(address)?
        {
            return Ok(vec![account_id]);
        }
        return Ok(vec![address.to_string()]);
    }
    Err(RgbServiceError::InvalidRequest("desc or address is required".to_string()).into())
}

#[derive(Deserialize)]
struct IssueAssetReq {
    desc: String,
    ticker: String,
    name: String,
    amount: u64,
    precision: u8,
    utxo: Option<String>,
    terms: Option<String>,
    issuer: Option<String>,
}

#[derive(Serialize)]
struct IssueAssetResp {
    contract_id: String,
}

async fn issue_asset(
    State(state): State<LegacyState>,
    Json(req): Json<IssueAssetReq>,
) -> Result<Json<IssueAssetResp>, LegacyHttpError> {
    let _ = (&req.terms, &req.issuer);
    let account_id = legacy_account_id(&req.desc);
    let mut wallet = open_legacy_wallet(&state.service, &state.config, &req.desc)?;
    sync_legacy_wallet(&state.service, &state.config, &mut wallet)?;
    let allocation_outpoint = match req.utxo {
        Some(utxo) => OutPoint::from_str(&utxo)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?,
        None => wallet
            .wallet
            .list_unspent()
            .next()
            .map(|utxo| utxo.outpoint)
            .ok_or_else(|| {
                RgbServiceError::InvalidRequest(
                    "utxo is required when the descriptor wallet has no synced UTXO".to_string(),
                )
            })?,
    };
    let issued = rgb_service_local::issue_rgb20_fixed_with_chain_source(
        &state.service.account_stock_dir(&account_id),
        state.service.network()?,
        &state.service.chain_source(),
        rgb_service_local::Rgb20IssueRequest {
            ticker: req.ticker,
            name: req.name,
            amount: req.amount,
            precision: req.precision,
            utxo: allocation_outpoint,
        },
    )
    .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
    state.service.put_account_utxo(
        &account_id,
        tracked_utxo_for_wallet(&wallet, allocation_outpoint),
    )?;
    wallet.persist()?;
    Ok(Json(IssueAssetResp {
        contract_id: issued.contract_id.to_string(),
    }))
}

#[derive(Deserialize)]
struct TransferReq {
    desc: Option<String>,
    assign: Vec<TransferAssign>,
    #[serde(alias = "feeRate")]
    fee_rate: u64,
}

#[derive(Clone, Deserialize)]
struct TransferAssign {
    address: String,
    sats: Option<u64>,
    #[serde(default)]
    rgb_assign: HashMap<String, u64>,
}

#[derive(Serialize)]
struct TransferPsbtResp {
    psbt: String,
}

#[derive(Clone)]
struct LegacyParsedRgbAssignment {
    recipient_index: usize,
    contract_id: rgb_service_local::rgbstd::ContractId,
    amount: u64,
}

async fn transfer_psbt(
    State(state): State<LegacyState>,
    Json(mut req): Json<TransferReq>,
) -> Result<Json<TransferPsbtResp>, LegacyHttpError> {
    if req.assign.is_empty() {
        return Err(RgbServiceError::InvalidRequest("assign is empty".to_string()).into());
    }
    let desc = req
        .desc
        .as_deref()
        .ok_or_else(|| RgbServiceError::InvalidRequest("desc is required".to_string()))?;
    let account_id = legacy_account_id(desc);

    maybe_add_legacy_rgb_fee_assignment(&state, &mut req.assign)?;

    let rgb_assignments = parse_legacy_rgb_assignments(&req.assign)?;
    if rgb_assignments.is_empty() {
        let mut wallet = open_legacy_wallet(&state.service, &state.config, desc)?;
        sync_legacy_wallet(&state.service, &state.config, &mut wallet)?;
        let psbt = build_legacy_btc_only_psbt(&state, &account_id, &mut wallet, &req)?;
        wallet.persist()?;
        return Ok(Json(TransferPsbtResp {
            psbt: psbt.to_string(),
        }));
    }

    let mut wallet = open_legacy_wallet(&state.service, &state.config, desc)?;
    sync_legacy_wallet(&state.service, &state.config, &mut wallet)?;

    let wallet_outpoints = wallet
        .wallet
        .list_unspent()
        .map(|utxo| utxo.outpoint)
        .collect::<Vec<_>>();
    let rgb_totals = aggregate_legacy_rgb_amounts(&rgb_assignments)?;
    let mut rgb_inputs = BTreeSet::new();
    for (contract_id, amount) in &rgb_totals {
        let have = legacy_rgb_balance(&state.service, &account_id, contract_id)?;
        if have < *amount {
            return Err(legacy_rgb_insufficient_error(contract_id, *amount, have));
        }
        for outpoint in select_rgb20_inputs(
            &state.service.account_stock_dir(&account_id),
            wallet_outpoints.clone(),
            *contract_id,
            *amount,
        )
        .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?
        {
            rgb_inputs.insert(outpoint);
        }
    }

    let fee_rate = FeeRate::from_sat_per_vb(req.fee_rate)
        .ok_or_else(|| RgbServiceError::InvalidRequest("invalid fee_rate".to_string()))?;
    let change_address = wallet.wallet.reveal_next_address(KeychainKind::External);
    let change_script = change_address.script_pubkey();
    let mut builder = wallet.wallet.build_tx();
    builder
        .ordering(TxOrdering::Untouched)
        .fee_rate(fee_rate)
        .drain_to(change_script.clone())
        .add_data(&[0; 32]);
    for outpoint in rgb_inputs {
        builder
            .add_utxo(outpoint)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
    }

    let network = state.service.network()?;
    let mut recipient_scripts = Vec::new();
    for assign in &req.assign {
        let address = Address::from_str(&assign.address)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?
            .require_network(network)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        let script = address.script_pubkey();
        let mut sats = assign.sats.unwrap_or_else(|| {
            if assign.rgb_assign.is_empty() {
                0
            } else {
                DEFAULT_RGB_DUST_SATS
            }
        });
        if sats == 0 && !assign.rgb_assign.is_empty() {
            sats = DEFAULT_RGB_DUST_SATS;
        }
        if sats > 0 {
            builder.add_recipient(script.clone(), Amount::from_sat(sats));
        }
        recipient_scripts.push((script, sats));
    }

    let psbt = builder
        .finish()
        .map_err(|err| RgbServiceError::Backend(format!("failed to build BTC PSBT: {err}")))?;
    let recipient_vouts = find_recipient_vouts(&psbt, &recipient_scripts)?;
    let rgb_recipient_vouts = rgb_assignments
        .iter()
        .map(|assignment| {
            recipient_vouts[assignment.recipient_index].ok_or_else(|| {
                RgbServiceError::Backend("built PSBT is missing RGB recipient output".to_string())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let change_vout = find_change_vout(&psbt, &change_script, &rgb_recipient_vouts)?;
    let psbt_assignments =
        rgb_assignments
            .iter()
            .zip(rgb_recipient_vouts.iter())
            .map(|(assignment, vout)| Rgb20PsbtAssignment {
                contract_id: assignment.contract_id,
                amount: assignment.amount,
                vout: *vout,
            });
    let prepared = prepare_rgb20_psbt(
        &state.service.account_stock_dir(&account_id),
        psbt,
        change_vout,
        psbt_assignments,
    )
    .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;

    let transfer_id = legacy_transfer_id(&prepared.psbt);
    let recipients = rgb_assignments
        .iter()
        .zip(rgb_recipient_vouts.iter())
        .map(|(assignment, vout)| PreparedTransferRecipient {
            asset_id: assignment.contract_id.to_string(),
            recipient_account_id: legacy_account_id_from_recipient(
                &req.assign[assignment.recipient_index].address,
            ),
            recipient_vout: *vout,
        })
        .collect::<Vec<_>>();
    let first_recipient = recipients.first().ok_or_else(|| {
        RgbServiceError::InvalidRequest("legacy transfer/psbt requires recipient".to_string())
    })?;
    state.service.legacy_put_prepared_transfer(
        &account_id,
        &transfer_id,
        first_recipient.asset_id.clone(),
        first_recipient.recipient_account_id.clone(),
        first_recipient.recipient_vout,
        &prepared.fascia,
        recipients,
    )?;
    state
        .service
        .logger()
        .info(format!("legacy prepared transfer stored account_id={account_id} transfer_id={transfer_id}"));
    wallet.persist()?;
    Ok(Json(TransferPsbtResp {
        psbt: prepared.psbt.to_string(),
    }))
}

fn build_legacy_btc_only_psbt(
    state: &LegacyState,
    account_id: &str,
    wallet: &mut LegacyWallet,
    req: &TransferReq,
) -> Result<Psbt, LegacyHttpError> {
    let fee_rate = FeeRate::from_sat_per_vb(req.fee_rate)
        .ok_or_else(|| RgbServiceError::InvalidRequest("invalid fee_rate".to_string()))?;
    let rgb_outpoints = legacy_rgb_allocated_outpoints(state, account_id, wallet)?;
    let change_address = wallet.wallet.reveal_next_address(KeychainKind::External);
    let mut builder = wallet.wallet.build_tx();
    builder
        .ordering(TxOrdering::Untouched)
        .fee_rate(fee_rate)
        .drain_to(change_address.script_pubkey());

    let network = state.service.network()?;
    let mut has_recipient = false;
    for assign in &req.assign {
        if !assign.rgb_assign.is_empty() {
            continue;
        }
        let sats = assign.sats.ok_or_else(|| {
            RgbServiceError::InvalidRequest("BTC-only transfer requires sats".to_string())
        })?;
        if sats == 0 {
            return Err(RgbServiceError::InvalidRequest(
                "BTC-only transfer requires non-zero sats".to_string(),
            )
            .into());
        }
        let address = Address::from_str(&assign.address)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?
            .require_network(network)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        builder.add_recipient(address.script_pubkey(), Amount::from_sat(sats));
        has_recipient = true;
    }
    if !has_recipient {
        return Err(RgbServiceError::InvalidRequest("assign is empty".to_string()).into());
    }

    if !rgb_outpoints.is_empty() {
        builder.unspendable(rgb_outpoints);
    }
    builder
        .finish()
        .map_err(|err| RgbServiceError::Backend(format!("failed to build BTC PSBT: {err}")).into())
}

fn legacy_rgb_allocated_outpoints(
    state: &LegacyState,
    account_id: &str,
    wallet: &LegacyWallet,
) -> Result<Vec<OutPoint>, LegacyHttpError> {
    let wallet_utxos = wallet
        .wallet
        .list_unspent()
        .map(|utxo| Rgb20TrackedUtxo {
            outpoint: utxo.outpoint,
            address: None,
            confirmed: utxo.chain_position.is_confirmed(),
        })
        .collect::<Vec<_>>();
    let allocations =
        list_rgb20_assets_for_utxos(&state.service.account_stock_dir(account_id), wallet_utxos)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
    Ok(allocations
        .into_iter()
        .map(|allocation| allocation.outpoint)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

fn maybe_add_legacy_rgb_fee_assignment(
    state: &LegacyState,
    assignments: &mut Vec<TransferAssign>,
) -> Result<(), LegacyHttpError> {
    if !state.config.rgb_fee_enabled {
        return Ok(());
    }
    if assignments
        .iter()
        .all(|assign| assign.rgb_assign.is_empty())
    {
        return Ok(());
    }
    let collector = state
        .config
        .rgb_fee_collector_address
        .as_deref()
        .filter(|address| !address.trim().is_empty())
        .ok_or_else(|| {
            RgbServiceError::InvalidRequest(
                "legacy RGB fee collector address is not configured".to_string(),
            )
        })?;
    let contract_id = legacy_rna_fee_contract_id(state)?;
    let amount = legacy_rgb_fee_amount(state);
    assignments.push(TransferAssign {
        address: collector.to_string(),
        sats: Some(DEFAULT_RGB_DUST_SATS),
        rgb_assign: HashMap::from([(contract_id, amount)]),
    });
    Ok(())
}

fn legacy_rna_fee_contract_id(state: &LegacyState) -> Result<String, LegacyHttpError> {
    if let Some(contract_id) = state
        .config
        .rgb_fee_contract_id
        .as_deref()
        .filter(|contract_id| !contract_id.trim().is_empty())
    {
        rgb_service_local::rgbstd::ContractId::from_str(contract_id)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:?}")))?;
        return Ok(contract_id.to_string());
    }
    state
        .service
        .list_token_catalog_entries()?
        .into_iter()
        .find(|entry| entry.ticker == "RNA")
        .map(|entry| entry.contract_id)
        .ok_or_else(|| {
            RgbServiceError::NotFound("RNA asset not found in catalog".to_string()).into()
        })
}

fn legacy_rgb_fee_amount(state: &LegacyState) -> u64 {
    state
        .config
        .rgb_fee_amount
        .unwrap_or(state.service.rna.transfer_fee)
}

fn parse_legacy_rgb_assignments(
    assignments: &[TransferAssign],
) -> Result<Vec<LegacyParsedRgbAssignment>, LegacyHttpError> {
    assignments
        .iter()
        .enumerate()
        .flat_map(|(recipient_index, assign)| {
            assign
                .rgb_assign
                .iter()
                .map(move |(contract_id, amount)| (recipient_index, contract_id, amount))
        })
        .map(|(recipient_index, contract_id, amount)| {
            let contract_id = rgb_service_local::rgbstd::ContractId::from_str(contract_id)
                .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:?}")))?;
            Ok(LegacyParsedRgbAssignment {
                recipient_index,
                contract_id,
                amount: *amount,
            })
        })
        .collect()
}

fn aggregate_legacy_rgb_amounts(
    assignments: &[LegacyParsedRgbAssignment],
) -> Result<HashMap<rgb_service_local::rgbstd::ContractId, u64>, LegacyHttpError> {
    let mut totals = HashMap::new();
    for assignment in assignments {
        totals
            .entry(assignment.contract_id)
            .and_modify(|amount: &mut u64| {
                *amount = amount.saturating_add(assignment.amount);
            })
            .or_insert(assignment.amount);
    }
    Ok(totals)
}

fn legacy_rgb_balance(
    service: &LocalDaemonService,
    account_id: &str,
    contract_id: &rgb_service_local::rgbstd::ContractId,
) -> Result<u64, LegacyHttpError> {
    let list = service.legacy_list_assets(account_id)?;
    let contract_id = contract_id.to_string();
    Ok(list
        .utxo_assets
        .values()
        .flat_map(|allocations| allocations.iter())
        .filter(|allocation| allocation.asset_id == contract_id)
        .map(|allocation| allocation.amount)
        .sum())
}

fn legacy_rgb_insufficient_error(
    contract_id: &rgb_service_local::rgbstd::ContractId,
    need: u64,
    have: u64,
) -> LegacyHttpError {
    LegacyHttpError::with_code(
        RgbServiceError::InvalidRequest(format!(
            "insufficient rgb balance: contract {contract_id}, need {need}, have {have}"
        )),
        22,
    )
}

fn find_recipient_vouts(
    psbt: &Psbt,
    recipients: &[(ScriptBuf, u64)],
) -> Result<Vec<Option<u32>>, LegacyHttpError> {
    let mut used = HashSet::new();
    recipients
        .iter()
        .map(|(script, sats)| {
            if *sats == 0 {
                return Ok(None);
            }
            let vout = psbt
                .unsigned_tx
                .output
                .iter()
                .enumerate()
                .find(|(vout, output)| {
                    !used.contains(vout)
                        && output.script_pubkey == *script
                        && output.value == Amount::from_sat(*sats)
                })
                .map(|(vout, _)| vout)
                .ok_or_else(|| {
                    RgbServiceError::Backend("built PSBT is missing recipient output".to_string())
                })?;
            used.insert(vout);
            Ok(Some(vout as u32))
        })
        .collect::<Result<Vec<_>, RgbServiceError>>()
        .map_err(Into::into)
}

fn find_change_vout(
    psbt: &Psbt,
    change_script: &ScriptBuf,
    recipient_vouts: &[u32],
) -> Result<u32, LegacyHttpError> {
    psbt.unsigned_tx
        .output
        .iter()
        .enumerate()
        .find(|(vout, output)| {
            !recipient_vouts.contains(&(*vout as u32))
                && output.script_pubkey == *change_script
                && output.value > Amount::ZERO
        })
        .map(|(vout, _)| vout as u32)
        .ok_or_else(|| {
            RgbServiceError::Backend("built PSBT is missing RGB change output".to_string()).into()
        })
}

#[derive(Deserialize)]
struct TransferCallbackReq {
    tx: Option<String>,
    txid: Option<String>,
    transfer_id: Option<String>,
    desc: Option<String>,
    utxos: Option<Vec<TrackedUtxo>>,
}

async fn transfer_callback(
    State(state): State<LegacyState>,
    Json(req): Json<TransferCallbackReq>,
) -> Result<impl IntoResponse, LegacyHttpError> {
    // Parse the signed transaction (the caller submits the fully-signed tx;
    // the daemon is responsible for broadcasting it, mirroring wallet-v2).
    let tx: bdk_wallet::bitcoin::Transaction = match (req.txid.clone(), req.tx.clone()) {
        (_, Some(tx_hex)) => deserialize(
            &crate::hex_decode(&tx_hex)
                .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?,
        )
        .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?,
        (Some(txid_str), None) => {
            return Err(RgbServiceError::InvalidRequest(format!(
                "transfer callback requires signed tx to broadcast (txid={txid_str})"
            ))
            .into());
        }
        (None, None) => {
            return Err(
                RgbServiceError::InvalidRequest("tx is required".to_string()).into(),
            );
        }
    };
    let txid = tx.compute_txid().to_string();

    let has_transfer_id_field = req.transfer_id.is_some();
    let has_desc = req.desc.is_some();
    let transfer_id = req.transfer_id.unwrap_or_else(|| txid.clone());
    let desc_account_id = req.desc.as_deref().map(legacy_account_id);
    state.service.logger().info(format!(
        "legacy transfer callback txid={txid} transfer_id={transfer_id} has_desc={has_desc} has_transfer_id_field={has_transfer_id_field}"
    ));
    let account_id = state
        .service
        .legacy_find_prepared_transfer_account_id(&transfer_id)?
        .or(desc_account_id);
    // RGB commit first: stage the fascia/consignment into stock so that RGB
    // state is persisted before we attempt broadcast. If broadcast fails the
    // RGB transition is already recorded and the tx can be re-broadcast later.
    if let Some(account_id) = &account_id {
        state
            .service
            .legacy_commit_prepared_transfer(
                account_id,
                &transfer_id,
                &txid,
                req.utxos.unwrap_or_default(),
            )
            .or_else(|err| match err {
                RgbServiceError::NotFound(_) if req.desc.is_some() => Ok(()),
                err => Err(err),
            })?;
    } else {
        // No prepared transfer and no desc: BTC-only transfer, nothing to commit.
        state.service.logger().info(format!(
            "legacy transfer callback btc-only (no rgb commit) txid={txid}"
        ));
    }
    // Broadcast the signed transaction via the configured chain backend.
    broadcast_legacy_tx(&state.service, &tx, &txid)?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct TransferCancelReq {
    desc: String,
    transfer_id: Option<String>,
}

async fn transfer_cancel(
    State(state): State<LegacyState>,
    Json(req): Json<TransferCancelReq>,
) -> Result<impl IntoResponse, LegacyHttpError> {
    let account_id = legacy_account_id(&req.desc);
    if let Some(transfer_id) = req.transfer_id {
        state
            .service
            .legacy_remove_prepared_transfer(&account_id, &transfer_id)?;
    }
    Ok(StatusCode::OK)
}

struct LegacyWallet {
    wallet: PersistedWallet<file_store::Store<ChangeSet>>,
    db: file_store::Store<ChangeSet>,
}

impl LegacyWallet {
    fn persist(&mut self) -> Result<(), LegacyHttpError> {
        if self.wallet.staged().is_some() {
            self.wallet
                .persist(&mut self.db)
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        }
        Ok(())
    }
}

fn open_legacy_wallet(
    service: &LocalDaemonService,
    config: &LegacyConfig,
    desc: &str,
) -> Result<LegacyWallet, LegacyHttpError> {
    let network = service.network()?;
    let descriptor = ExtendedDescriptor::from_str(desc)
        .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
    let dir = legacy_wallet_dir(&service.config.data_dir, desc);
    std::fs::create_dir_all(&dir).map_err(|err| RgbServiceError::Backend(err.to_string()))?;
    let (mut db, _) = file_store::Store::load_or_create(LEGACY_BDK_MAGIC, dir.join(BDK_FILE))
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
    let mut wallet = match Wallet::load()
        .descriptor(KeychainKind::External, Some(descriptor.clone()))
        .check_network(network)
        .load_wallet(&mut db)
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?
    {
        Some(wallet) => wallet,
        None => Wallet::create_single(descriptor)
            .network(network)
            .create_wallet(&mut db)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?,
    };
    let reveal_count = config.reveal_address_count.max(1);
    let _ = wallet.reveal_addresses_to(KeychainKind::External, reveal_count);
    Ok(LegacyWallet { wallet, db })
}

fn sync_legacy_wallet(
    service: &LocalDaemonService,
    config: &LegacyConfig,
    wallet: &mut LegacyWallet,
) -> Result<(), LegacyHttpError> {
    if is_electrum_url(&service.config.esplora_url) {
        let url = normalize_electrum_url(&service.config.esplora_url);
        let client = electrum_client::Client::new(&url)
            .map_err(|err| RgbServiceError::Backend(format!("electrum client failed: {err}")))?;
        let client = BdkElectrumClient::new(client);
        let request = wallet.wallet.start_sync_with_revealed_spks().build();
        // `fetch_prev_txouts=false`: legacy transfers don't need fee calculation
        // from the chain sync. Some electrs deployments reject the
        // `blockchain.block.txids` calls that anchors fetching triggers, so we
        // skip it. If sync still fails (e.g. the server lacks a method), we log
        // and continue with the persisted BDK state rather than failing the
        // whole request — the stock allocation is the source of truth for RGB
        // balances, and the BDK wallet already carries the last-known UTXO set.
        match client.sync(request, 10, false) {
            Ok(update) => {
                if let Err(err) = wallet.wallet.apply_update(update) {
                    service
                        .logger()
                        .warn(format!("legacy electrum apply_update failed: {err}"));
                }
            }
            Err(err) => {
                service.logger().warn(format!(
                    "legacy electrum sync failed (continuing with persisted state): {err}"
                ));
            }
        }
        return Ok(());
    }

    let client = bdk_esplora::esplora_client::Builder::new(&service.config.esplora_url)
        .timeout(config.sync_timeout_secs.max(1))
        .build_blocking();
    let request = wallet.wallet.start_sync_with_revealed_spks().build();
    match client.sync(request, 2) {
        Ok(update) => {
            if let Err(err) = wallet.wallet.apply_update(update) {
                service
                    .logger()
                    .warn(format!("legacy esplora apply_update failed: {err}"));
            }
        }
        Err(err) => {
            service.logger().warn(format!(
                "legacy esplora sync failed (continuing with persisted state): {err}"
            ));
        }
    }
    Ok(())
}

// Broadcast a fully-signed transaction through the configured chain backend
// (electrum protocol or esplora HTTP). Logs and propagates errors so the
// caller (transfer/callback) returns a non-200 to the client for retry.
fn broadcast_legacy_tx(
    service: &LocalDaemonService,
    tx: &bdk_wallet::bitcoin::Transaction,
    txid: &str,
) -> Result<(), LegacyHttpError> {
    if is_electrum_url(&service.config.esplora_url) {
        let url = normalize_electrum_url(&service.config.esplora_url);
        let result = service.electrum_with_retry("electrum broadcast", || {
            let config = electrum_client::ConfigBuilder::new()
                .timeout(Some(crate::ELECTRUM_TIMEOUT_SECS))
                .build();
            let client = electrum_client::Client::from_config(&url, config)?;
            client.transaction_broadcast(tx)?;
            Ok(())
        });
        if let Err(err) = result {
            service.logger().warn(format!(
                "legacy broadcast failed txid={txid} (electrum): {err}"
            ));
            return Err(RgbServiceError::Backend(format!("broadcast failed: {err}")).into());
        }
    } else {
        let client = bdk_esplora::esplora_client::Builder::new(&service.config.esplora_url)
            .timeout(30)
            .build_blocking();
        if let Err(err) = client.broadcast(tx) {
            service.logger().warn(format!(
                "legacy broadcast failed txid={txid} (esplora): {err}"
            ));
            return Err(RgbServiceError::Backend(format!("broadcast failed: {err}")).into());
        }
    }
    service
        .logger()
        .info(format!("legacy broadcast ok txid={txid}"));
    Ok(())
}

fn tracked_utxo_for_wallet(wallet: &LegacyWallet, outpoint: OutPoint) -> TrackedUtxo {
    let confirmed = wallet
        .wallet
        .get_utxo(outpoint)
        .is_some_and(|utxo| utxo.chain_position.is_confirmed());
    TrackedUtxo {
        outpoint: outpoint.to_string(),
        address: None,
        confirmed,
    }
}

fn legacy_wallet_dir(data_dir: &Path, desc: &str) -> PathBuf {
    data_dir
        .join("legacy-wallets")
        .join(legacy_account_hash(desc))
}

pub(crate) fn legacy_account_id(desc: &str) -> String {
    format!("legacy-desc:{}", legacy_account_hash(desc))
}

fn legacy_account_id_from_recipient(recipient: &str) -> String {
    recipient.to_string()
}

fn legacy_account_hash(desc: &str) -> String {
    sha256::Hash::hash(desc.as_bytes()).to_string()
}

fn legacy_transfer_id(psbt: &Psbt) -> String {
    psbt.unsigned_tx.compute_txid().to_string()
}

struct LegacyHttpError {
    err: RgbServiceError,
    legacy_code: Option<u8>,
}

impl LegacyHttpError {
    fn with_code(err: RgbServiceError, legacy_code: u8) -> Self {
        Self {
            err,
            legacy_code: Some(legacy_code),
        }
    }
}

impl From<RgbServiceError> for LegacyHttpError {
    fn from(err: RgbServiceError) -> Self {
        Self {
            err,
            legacy_code: None,
        }
    }
}

impl IntoResponse for LegacyHttpError {
    fn into_response(self) -> Response {
        let status = match &self.err {
            RgbServiceError::Unauthorized(_) | RgbServiceError::SignatureRequired(_) => {
                StatusCode::UNAUTHORIZED
            }
            RgbServiceError::Forbidden(_) | RgbServiceError::AssetSpendAuthorizationRequired(_) => {
                StatusCode::FORBIDDEN
            }
            RgbServiceError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            RgbServiceError::NotFound(_) => StatusCode::NOT_FOUND,
            RgbServiceError::Conflict(_) => StatusCode::CONFLICT,
            RgbServiceError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            RgbServiceError::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = self.err.to_string();
        let body = match self.legacy_code {
            Some(code) => serde_json::json!({ "code": code, "message": message }),
            None => serde_json::json!({ "message": message }),
        };
        (status, Json(body)).into_response()
    }
}
