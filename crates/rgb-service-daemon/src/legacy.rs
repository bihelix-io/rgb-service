use std::{
    collections::{BTreeMap, HashMap},
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
use rgb_service_api::{RgbServiceApi, RgbServiceError, TrackedUtxo};
use rgb_service_local::{prepare_rgb20_psbt, select_rgb20_inputs, Rgb20PsbtAssignment};
use serde::{Deserialize, Serialize};

use crate::{hex_encode, LegacyConfig, LocalDaemonService};

const LEGACY_BDK_MAGIC: &[u8] = b"RgbDaemonLegacyBdk";
const BDK_FILE: &str = "bdk_wallet";
const REVEAL_ADDRESS_COUNT: u32 = 1000;
const DEFAULT_RGB_DUST_SATS: u64 = 1000;

#[derive(Clone)]
struct LegacyState {
    service: Arc<LocalDaemonService>,
}

pub(crate) fn router(service: Arc<LocalDaemonService>, config: LegacyConfig) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/account/create", put(create_account))
        .route("/asset/list", get(asset_list))
        .route("/asset", get(query_asset))
        .route("/asset/internal/issue", post(issue_asset))
        .route("/transfer/psbt", post(transfer_psbt))
        .route("/transfer/callback", post(transfer_callback))
        .route("/transfer/cancel", post(transfer_cancel))
        .route_layer(middleware::from_fn_with_state(
            config,
            legacy_allowlist_middleware,
        ))
        .with_state(LegacyState { service })
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
    let _wallet = open_legacy_wallet(&state.service, &req.desc)?;
    state.service.get_or_create_profile(&account_id)?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct AssetListQuery {
    contract_id: Option<String>,
}

#[derive(Serialize)]
struct LegacyAssetInfo {
    id: u64,
    ticker: String,
    name: String,
    precision: u8,
    contract_id: String,
    supply: String,
}

async fn asset_list(
    State(state): State<LegacyState>,
    Query(query): Query<AssetListQuery>,
) -> Result<Json<Vec<LegacyAssetInfo>>, LegacyHttpError> {
    let response = state.service.token_list().await?;
    let mut id = 1u64;
    let assets = response
        .assets
        .into_iter()
        .filter(|asset| {
            query
                .contract_id
                .as_deref()
                .is_none_or(|contract_id| contract_id == asset.contract_id)
        })
        .map(|asset| {
            let item = LegacyAssetInfo {
                id,
                ticker: asset.ticker,
                name: asset.name,
                precision: asset.precision,
                contract_id: asset.contract_id,
                supply: "0".to_string(),
            };
            id += 1;
            item
        })
        .collect();
    Ok(Json(assets))
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
    ticker: String,
    name: String,
    rgb_amount: u64,
    address: Option<String>,
    status: String,
    decimal: u8,
    txid: String,
}

async fn query_asset(
    State(state): State<LegacyState>,
    Query(query): Query<QueryAssetReq>,
) -> Result<Json<BTreeMap<String, Vec<LegacyAllocation>>>, LegacyHttpError> {
    let account_id = legacy_query_account_id(&query)?;
    let list = state.service.legacy_list_assets(&account_id)?;
    let metadata = list
        .assets
        .into_iter()
        .map(|asset| (asset.contract_id.clone(), asset))
        .collect::<HashMap<_, _>>();
    let mut result = BTreeMap::<String, Vec<LegacyAllocation>>::new();
    for (outpoint, allocations) in list.utxo_assets {
        for allocation in allocations {
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
            result
                .entry(outpoint.clone())
                .or_default()
                .push(LegacyAllocation {
                    contract_id: asset.contract_id.clone(),
                    ticker: asset.ticker.clone(),
                    name: asset.name.clone(),
                    rgb_amount: allocation.amount,
                    address: None,
                    status: format!("{:?}", allocation.status),
                    decimal: asset.precision,
                    txid: outpoint.split(':').next().unwrap_or_default().to_string(),
                });
        }
    }
    Ok(Json(result))
}

fn legacy_query_account_id(query: &QueryAssetReq) -> Result<String, LegacyHttpError> {
    if let Some(desc) = query.desc.as_deref() {
        return Ok(legacy_account_id(desc));
    }
    if let Some(address) = query.address.as_deref() {
        return Ok(address.to_string());
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
    let mut wallet = open_legacy_wallet(&state.service, &req.desc)?;
    sync_legacy_wallet(&state.service, &mut wallet)?;
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
    desc: String,
    assign: Vec<TransferAssign>,
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
    transfer_id: String,
}

async fn transfer_psbt(
    State(state): State<LegacyState>,
    Json(req): Json<TransferReq>,
) -> Result<Json<TransferPsbtResp>, LegacyHttpError> {
    if req.assign.is_empty() {
        return Err(RgbServiceError::InvalidRequest("assign is empty".to_string()).into());
    }
    let account_id = legacy_account_id(&req.desc);
    let mut wallet = open_legacy_wallet(&state.service, &req.desc)?;
    sync_legacy_wallet(&state.service, &mut wallet)?;

    let rgb_assignments =
        req.assign
            .iter()
            .enumerate()
            .flat_map(|(recipient_index, assign)| {
                assign.rgb_assign.iter().map(move |(contract_id, amount)| {
                    (recipient_index, contract_id.clone(), *amount)
                })
            })
            .collect::<Vec<_>>();
    if rgb_assignments.len() != 1 {
        return Err(RgbServiceError::InvalidRequest(
            "legacy transfer/psbt currently supports exactly one RGB assignment".to_string(),
        )
        .into());
    }
    let (rgb_recipient_index, contract_id, rgb_amount) = rgb_assignments[0].clone();
    let contract_id = rgb_service_local::rgbstd::ContractId::from_str(&contract_id)
        .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:?}")))?;

    let wallet_outpoints = wallet
        .wallet
        .list_unspent()
        .map(|utxo| utxo.outpoint)
        .collect::<Vec<_>>();
    let rgb_inputs = select_rgb20_inputs(
        &state.service.account_stock_dir(&account_id),
        wallet_outpoints,
        contract_id,
        rgb_amount,
    )
    .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;

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
    let recipient_vout = find_recipient_vout(
        &psbt,
        &recipient_scripts[rgb_recipient_index].0,
        recipient_scripts[rgb_recipient_index].1,
    )?;
    let change_vout = find_change_vout(&psbt, &change_script, recipient_vout)?;
    let prepared = prepare_rgb20_psbt(
        &state.service.account_stock_dir(&account_id),
        psbt,
        change_vout,
        [Rgb20PsbtAssignment {
            contract_id,
            amount: rgb_amount,
            vout: recipient_vout,
        }],
    )
    .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;

    let transfer_id = legacy_transfer_id(&prepared.psbt);
    state.service.legacy_put_prepared_transfer(
        &account_id,
        &transfer_id,
        contract_id.to_string(),
        legacy_account_id_from_recipient(&req.assign[rgb_recipient_index].address),
        recipient_vout,
        &prepared.fascia,
    )?;
    wallet.persist()?;
    Ok(Json(TransferPsbtResp {
        psbt: hex_encode(&prepared.psbt.serialize()),
        transfer_id,
    }))
}

fn find_recipient_vout(psbt: &Psbt, script: &ScriptBuf, sats: u64) -> Result<u32, LegacyHttpError> {
    psbt.unsigned_tx
        .output
        .iter()
        .enumerate()
        .find(|(_, output)| {
            output.script_pubkey == *script && (sats == 0 || output.value == Amount::from_sat(sats))
        })
        .map(|(vout, _)| vout as u32)
        .ok_or_else(|| {
            RgbServiceError::Backend("built PSBT is missing recipient output".to_string()).into()
        })
}

fn find_change_vout(
    psbt: &Psbt,
    change_script: &ScriptBuf,
    recipient_vout: u32,
) -> Result<u32, LegacyHttpError> {
    psbt.unsigned_tx
        .output
        .iter()
        .enumerate()
        .find(|(vout, output)| {
            *vout as u32 != recipient_vout
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
    let txid = match (req.txid, req.tx) {
        (Some(txid), _) => txid,
        (None, Some(tx_hex)) => {
            let tx: bdk_wallet::bitcoin::Transaction = deserialize(
                &crate::hex_decode(&tx_hex)
                    .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?,
            )
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
            tx.compute_txid().to_string()
        }
        (None, None) => {
            return Err(
                RgbServiceError::InvalidRequest("tx or txid is required".to_string()).into(),
            );
        }
    };
    let transfer_id = req.transfer_id.unwrap_or_else(|| txid.clone());
    let account_id = req
        .desc
        .as_deref()
        .map(legacy_account_id)
        .ok_or_else(|| RgbServiceError::InvalidRequest("desc is required".to_string()))?;
    state.service.legacy_commit_prepared_transfer(
        &account_id,
        &transfer_id,
        &txid,
        req.utxos.unwrap_or_default(),
    )?;
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
    let _ = wallet.reveal_addresses_to(KeychainKind::External, REVEAL_ADDRESS_COUNT);
    Ok(LegacyWallet { wallet, db })
}

fn sync_legacy_wallet(
    service: &LocalDaemonService,
    wallet: &mut LegacyWallet,
) -> Result<(), LegacyHttpError> {
    let client = bdk_esplora::esplora_client::Builder::new(&service.config.esplora_url)
        .timeout(10)
        .build_blocking();
    let request = wallet.wallet.start_sync_with_revealed_spks().build();
    let update = client
        .sync(request, 2)
        .map_err(|err| RgbServiceError::Backend(format!("esplora sync failed: {err}")))?;
    wallet
        .wallet
        .apply_update(update)
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
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

fn legacy_account_id(desc: &str) -> String {
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

struct LegacyHttpError(RgbServiceError);

impl From<RgbServiceError> for LegacyHttpError {
    fn from(err: RgbServiceError) -> Self {
        Self(err)
    }
}

impl IntoResponse for LegacyHttpError {
    fn into_response(self) -> Response {
        let status = match self.0 {
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
        (
            status,
            Json(serde_json::json!({ "message": self.0.to_string() })),
        )
            .into_response()
    }
}
