use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
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
        absolute,
        bip32::ChildNumber,
        consensus::deserialize,
        hashes::{sha256, Hash},
        secp256k1::Secp256k1,
        transaction, Address, Amount, FeeRate, OutPoint, Psbt, ScriptBuf, Sequence, Transaction,
        TxIn, TxOut, Txid, Witness,
    },
    chain::{ChainPosition, ConfirmationBlockTime, Merge},
    descriptor::ExtendedDescriptor,
    file_store, ChangeSet, KeychainKind, PersistedWallet, TxOrdering, Wallet, WalletPersister,
};
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase, SingleWriterTxKeyspace};
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
        .route("/utxo", get(utxo))
        .route("/asset/internal/issue", post(issue_asset))
        .route("/estimate/gas", get(estimate_gas))
        .route("/get_fee", get(get_mempool_info))
        .route("/stake/address", get(generate_stake_address))
        .route("/stake/split", post(stake_split))
        .route("/redeem/list", get(redeem_list))
        .route("/redeem/psbt", post(redeem_psbt))
        .route("/redeem/callback", post(redeem_callback))
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
struct GenerateStakeAddressQuery {
    height: std::num::NonZeroU16,
    public_key: bdk_wallet::bitcoin::PublicKey,
}

#[derive(Debug, Serialize)]
struct GenerateStakeAddressResponse {
    address: String,
    script: String,
}

async fn generate_stake_address(
    State(state): State<LegacyState>,
    Query(query): Query<GenerateStakeAddressQuery>,
) -> Result<Json<GenerateStakeAddressResponse>, LegacyHttpError> {
    let script = make_stake_script(query.public_key, query.height.get());
    let address = Address::p2wsh(&script, state.service.network()?);
    Ok(Json(GenerateStakeAddressResponse {
        address: address.to_string(),
        script: crate::hex_encode(script.as_bytes()),
    }))
}

/// Reproduce wallet-service-v2's CSV stake witness script exactly.
pub(crate) fn make_stake_script(
    public_key: bdk_wallet::bitcoin::PublicKey,
    height: u16,
) -> ScriptBuf {
    use bdk_wallet::bitcoin::{opcodes::all, script::Builder};

    Builder::new()
        .push_int(height as i64)
        .push_opcode(all::OP_CSV)
        .push_opcode(all::OP_DROP)
        .push_key(&public_key)
        .push_opcode(all::OP_CHECKSIG)
        .into_script()
}

#[derive(Deserialize)]
struct StakeSplitReq {
    desc: String,
    split_to: Vec<StakeSplitAssign>,
    fee_rate: u64,
}

#[derive(Deserialize)]
struct StakeSplitAssign {
    address: String,
    sats: u64,
    contract_id: String,
    rgb_amount: u64,
}

async fn stake_split(
    State(state): State<LegacyState>,
    Json(req): Json<StakeSplitReq>,
) -> Result<Json<TransferPsbtResp>, LegacyHttpError> {
    let assign = req
        .split_to
        .into_iter()
        .map(|split| TransferAssign {
            address: split.address,
            sats: Some(split.sats),
            rgb_assign: HashMap::from([(split.contract_id, split.rgb_amount)]),
        })
        .collect();
    transfer_psbt(
        State(state),
        Json(TransferReq {
            desc: Some(req.desc),
            assign,
            fee_rate: req.fee_rate,
        }),
    )
    .await
}

#[derive(Deserialize)]
struct RedeemListQuery {
    public_key: bdk_wallet::bitcoin::PublicKey,
    contract_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RedeemListResp {
    spend_txid: Option<String>,
    outpoint: String,
    sats: u64,
    height: u16,
    public_key: String,
    status: i16,
    assign_map: Option<(String, u64)>,
    create_time: String,
    confirm_height: Option<u64>,
}

async fn redeem_list(
    State(state): State<LegacyState>,
    Query(query): Query<RedeemListQuery>,
) -> Result<Json<Vec<RedeemListResp>>, LegacyHttpError> {
    let public_key = query.public_key.to_string();
    let records = state
        .service
        .list_stake_redeems_by_public_key(&public_key)?;
    let response = records
        .into_iter()
        .filter_map(|record| {
            let assignment = match query.contract_id.as_deref() {
                Some(contract_id) => record
                    .rgb_assignments
                    .get(contract_id)
                    .copied()
                    .map(|amount| (contract_id.to_string(), amount)),
                None => record
                    .rgb_assignments
                    .iter()
                    .next()
                    .map(|(contract_id, amount)| (contract_id.clone(), *amount)),
            };
            if query.contract_id.is_some() && assignment.is_none() {
                return None;
            }
            Some(RedeemListResp {
                spend_txid: record.redeem_spend_txid,
                outpoint: record.stake_outpoint,
                sats: record.sats,
                height: record.csv_height,
                public_key: record.public_key,
                status: record.status,
                assign_map: assignment,
                create_time: record.created_at,
                confirm_height: record.confirm_height,
            })
        })
        .collect();
    Ok(Json(response))
}

#[derive(Deserialize)]
struct RedeemPsbtReq {
    outpoint: OutPoint,
    fee_rate: u64,
    address: String,
}

#[derive(Serialize)]
struct RedeemPsbtResp {
    psbt: String,
}

async fn redeem_psbt(
    State(state): State<LegacyState>,
    Json(req): Json<RedeemPsbtReq>,
) -> Result<Json<RedeemPsbtResp>, LegacyHttpError> {
    run_legacy_blocking("legacy redeem/psbt", move || {
        let outpoint = req.outpoint.to_string();
        let record = state.service.get_stake_redeem(&outpoint)?.ok_or_else(|| {
            RgbServiceError::NotFound(format!("stake redeem not found: {outpoint}"))
        })?;
        if record.status != 0 {
            return Err(RgbServiceError::Conflict(format!(
                "stake outpoint {outpoint} is already redeemed"
            ))
            .into());
        }

        let public_key = bdk_wallet::bitcoin::PublicKey::from_str(&record.public_key)
            .map_err(|err| RgbServiceError::Backend(format!("invalid stored public key: {err}")))?;
        let funding_tx = fetch_legacy_transaction(&state.service, &req.outpoint.txid)?;
        let funding_output = funding_tx
            .output
            .get(req.outpoint.vout as usize)
            .cloned()
            .ok_or_else(|| {
                RgbServiceError::NotFound(format!("stake output not found: {outpoint}"))
            })?;
        let witness_script = make_stake_script(public_key, record.csv_height);
        if funding_output.script_pubkey != witness_script.to_p2wsh() {
            return Err(RgbServiceError::Conflict(format!(
                "stake output script does not match stored CSV terms: {outpoint}"
            ))
            .into());
        }
        if funding_output.value.to_sat() != record.sats {
            return Err(RgbServiceError::Conflict(format!(
                "stake output amount does not match stored sats: {outpoint}"
            ))
            .into());
        }

        let target = Address::from_str(&req.address)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?
            .require_network(state.service.network()?)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        let target_script = target.script_pubkey();
        let mut unsigned_tx = Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: req.outpoint,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::from_height(record.csv_height),
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: funding_output.value,
                script_pubkey: target_script.clone(),
            }],
        };
        let fee_rate = FeeRate::from_sat_per_vb(req.fee_rate)
            .ok_or_else(|| RgbServiceError::InvalidRequest("invalid fee_rate".to_string()))?;
        let fee = fee_rate
            .checked_mul_by_weight(unsigned_tx.weight())
            .ok_or_else(|| RgbServiceError::InvalidRequest("fee_rate is too high".to_string()))?;
        unsigned_tx.output[0].value =
            unsigned_tx.output[0]
                .value
                .checked_sub(fee)
                .ok_or_else(|| {
                    RgbServiceError::InvalidRequest("fee exceeds stake output".to_string())
                })?;
        if unsigned_tx.output[0].value < target_script.minimal_non_dust() {
            return Err(
                RgbServiceError::InvalidRequest("redeem output would be dust".to_string()).into(),
            );
        }

        let mut psbt = Psbt::from_unsigned_tx(unsigned_tx)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        psbt.inputs[0].witness_script = Some(witness_script);
        psbt.inputs[0].witness_utxo = Some(funding_output);
        psbt.inputs[0].non_witness_utxo = Some(funding_tx);
        if let Some(desc) = record.owner_desc.as_deref() {
            add_redeem_bip32_derivation(&mut psbt, desc, public_key)?;
        }
        Ok(Json(RedeemPsbtResp {
            psbt: psbt.to_string(),
        }))
    })
    .await
}

#[derive(Deserialize)]
struct RedeemCallbackReq {
    psbt: String,
}

async fn redeem_callback(
    State(state): State<LegacyState>,
    Json(req): Json<RedeemCallbackReq>,
) -> Result<StatusCode, LegacyHttpError> {
    run_legacy_blocking("legacy redeem/callback", move || {
        let mut psbt = Psbt::from_str(&req.psbt)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("invalid PSBT: {err}")))?;
        if psbt.inputs.len() != 1
            || psbt.unsigned_tx.input.len() != 1
            || psbt.unsigned_tx.output.len() != 1
        {
            return Err(RgbServiceError::InvalidRequest(
                "redeem PSBT must contain exactly one input and one output".to_string(),
            )
            .into());
        }
        let outpoint = psbt.unsigned_tx.input[0].previous_output.to_string();
        let record = state.service.get_stake_redeem(&outpoint)?.ok_or_else(|| {
            RgbServiceError::NotFound(format!("stake redeem not found: {outpoint}"))
        })?;
        let public_key = bdk_wallet::bitcoin::PublicKey::from_str(&record.public_key)
            .map_err(|err| RgbServiceError::Backend(format!("invalid stored public key: {err}")))?;
        let expected_script = make_stake_script(public_key, record.csv_height);
        let input = &psbt.inputs[0];
        if input.final_script_witness.is_some() {
            return Err(RgbServiceError::InvalidRequest(
                "redeem PSBT is already finalized".to_string(),
            )
            .into());
        }
        if input.witness_script.as_ref() != Some(&expected_script) {
            return Err(RgbServiceError::InvalidRequest(
                "redeem PSBT witness_script does not match stake record".to_string(),
            )
            .into());
        }
        if psbt.unsigned_tx.input[0].sequence != Sequence::from_height(record.csv_height) {
            return Err(RgbServiceError::InvalidRequest(
                "redeem PSBT sequence does not match stake CSV height".to_string(),
            )
            .into());
        }
        let signature = input
            .partial_sigs
            .get(&public_key)
            .cloned()
            .ok_or_else(|| {
                RgbServiceError::InvalidRequest(
                    "redeem PSBT is missing the stake public key signature".to_string(),
                )
            })?;
        let spend_txid = psbt.unsigned_tx.compute_txid().to_string();
        if record.status == 1 {
            if record.redeem_spend_txid.as_deref() == Some(spend_txid.as_str()) {
                return Ok(StatusCode::OK);
            }
            return Err(RgbServiceError::Conflict(format!(
                "stake outpoint {outpoint} was already redeemed"
            ))
            .into());
        }
        psbt.inputs[0].final_script_witness = Some(Witness::from_slice(&[
            signature.to_vec(),
            expected_script.to_bytes(),
        ]));
        let tx = psbt.extract_tx().map_err(|err| {
            RgbServiceError::InvalidRequest(format!("invalid signed PSBT: {err}"))
        })?;
        broadcast_legacy_tx(&state.service, &tx, &spend_txid)?;
        state.service.mark_stake_redeemed(&outpoint, &spend_txid)?;
        Ok(StatusCode::OK)
    })
    .await
}

fn fetch_legacy_transaction(
    service: &LocalDaemonService,
    txid: &Txid,
) -> Result<Transaction, LegacyHttpError> {
    if is_electrum_url(&service.config.esplora_url) {
        let url = normalize_electrum_url(&service.config.esplora_url);
        return service
            .electrum_with_retry("electrum transaction_get", || {
                let config = electrum_client::ConfigBuilder::new()
                    .timeout(Some(crate::ELECTRUM_TIMEOUT_SECS))
                    .build();
                let client = electrum_client::Client::from_config(&url, config)?;
                client.transaction_get(txid)
            })
            .map_err(Into::into);
    }
    let client = bdk_esplora::esplora_client::Builder::new(&service.config.esplora_url)
        .timeout(30)
        .build_blocking();
    client
        .get_tx(txid)
        .map_err(|err| RgbServiceError::Backend(format!("fetch stake transaction: {err}")))?
        .ok_or_else(|| RgbServiceError::NotFound(format!("transaction not found: {txid}")))
        .map_err(Into::into)
}

fn add_redeem_bip32_derivation(
    psbt: &mut Psbt,
    desc: &str,
    public_key: bdk_wallet::bitcoin::PublicKey,
) -> Result<(), LegacyHttpError> {
    use bdk_wallet::miniscript::{
        descriptor::{DescriptorPublicKey, Wildcard},
        Descriptor,
    };

    let descriptor = ExtendedDescriptor::from_str(desc).map_err(|err| {
        RgbServiceError::Backend(format!("invalid stored owner descriptor: {err}"))
    })?;
    let Descriptor::Wpkh(wpkh) = &descriptor else {
        return Ok(());
    };
    let DescriptorPublicKey::XPub(xpub) = wpkh.as_inner() else {
        return Ok(());
    };
    let secp = Secp256k1::verification_only();
    let derived = descriptor
        .derived_descriptor(&secp, 0)
        .map_err(|err| RgbServiceError::Backend(format!("derive owner descriptor: {err}")))?;
    let Descriptor::Wpkh(derived_wpkh) = derived else {
        return Ok(());
    };
    if derived_wpkh.as_inner() != &public_key {
        return Ok(());
    }
    let Some((fingerprint, origin_path)) = xpub.origin.as_ref() else {
        return Ok(());
    };
    let mut derivation_path = origin_path.extend(&xpub.derivation_path);
    derivation_path = match xpub.wildcard {
        Wildcard::None => derivation_path,
        Wildcard::Unhardened => derivation_path.child(ChildNumber::Normal { index: 0 }),
        Wildcard::Hardened => return Ok(()),
    };
    psbt.inputs[0]
        .bip32_derivation
        .insert(public_key.inner, (*fingerprint, derivation_path));
    Ok(())
}

const LEGACY_BLOCKING_MAX_CONCURRENCY: usize = 4;
const LEGACY_BLOCKING_QUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const LEGACY_BLOCKING_EXECUTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

fn legacy_blocking_semaphore() -> &'static Arc<tokio::sync::Semaphore> {
    static SEMAPHORE: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();
    SEMAPHORE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(LEGACY_BLOCKING_MAX_CONCURRENCY)))
}

async fn run_legacy_blocking<T, F>(operation: &'static str, task: F) -> Result<T, LegacyHttpError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, LegacyHttpError> + Send + 'static,
{
    let permit = tokio::time::timeout(
        LEGACY_BLOCKING_QUEUE_TIMEOUT,
        Arc::clone(legacy_blocking_semaphore()).acquire_owned(),
    )
    .await
    .map_err(|_| {
        LegacyHttpError::with_status(
            RgbServiceError::Backend(format!(
                "{operation} blocking queue timed out after {} seconds",
                LEGACY_BLOCKING_QUEUE_TIMEOUT.as_secs()
            )),
            StatusCode::SERVICE_UNAVAILABLE,
        )
    })?
    .map_err(|_| {
        LegacyHttpError::with_status(
            RgbServiceError::Backend(format!("{operation} blocking executor is closed")),
            StatusCode::SERVICE_UNAVAILABLE,
        )
    })?;

    let handle = tokio::task::spawn_blocking(move || {
        // Keep the permit until the synchronous work actually exits. A timed-out
        // spawn_blocking task cannot be cancelled safely, so releasing it when the
        // HTTP timeout fires would allow detached work to grow without a bound.
        let _permit = permit;
        task()
    });

    match tokio::time::timeout(LEGACY_BLOCKING_EXECUTION_TIMEOUT, handle).await {
        Ok(Ok(result)) => result,
        Ok(Err(err)) => Err(RgbServiceError::Backend(format!(
            "{operation} blocking worker failed: {err}"
        ))
        .into()),
        Err(_) => Err(LegacyHttpError::with_status(
            RgbServiceError::Backend(format!(
                "{operation} timed out after {} seconds",
                LEGACY_BLOCKING_EXECUTION_TIMEOUT.as_secs()
            )),
            StatusCode::GATEWAY_TIMEOUT,
        )),
    }
}

#[derive(Deserialize)]
struct CreateAccountReq {
    desc: String,
}

async fn create_account(
    State(state): State<LegacyState>,
    Json(req): Json<CreateAccountReq>,
) -> Result<impl IntoResponse, LegacyHttpError> {
    run_legacy_blocking("legacy account/create", move || {
        let account_id = legacy_account_id(&req.desc);
        let wallet = open_legacy_wallet(&state.service, &state.config, &req.desc)?;
        state.service.get_or_create_profile(&account_id)?;
        state
            .service
            .put_legacy_account_desc(&account_id, &req.desc)?;
        for index in 0..=state.config.reveal_address_count.max(1) {
            let address = wallet
                .wallet
                .peek_address(KeychainKind::External, index)
                .address
                .to_string();
            state
                .service
                .put_legacy_address_account(&address, &account_id)?;
        }
        Ok(StatusCode::OK)
    })
    .await
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
    run_legacy_blocking("legacy asset/list", move || {
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
    })
    .await
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

#[derive(Deserialize)]
struct LegacyUtxoQuery {
    address: Option<String>,
    desc: Option<String>,
}

#[derive(Serialize)]
struct LegacyLocalOutput {
    outpoint: OutPoint,
    txout: TxOut,
    keychain: KeychainKind,
    is_spent: bool,
    derivation_index: u32,
    chain_position: LegacyChainPosition,
}

#[derive(Serialize)]
enum LegacyChainPosition {
    Confirmed {
        anchor: ConfirmationBlockTime,
        transitively: Option<Txid>,
    },
    Unconfirmed {
        last_seen: Option<u64>,
    },
}

impl From<bdk_wallet::LocalOutput> for LegacyLocalOutput {
    fn from(output: bdk_wallet::LocalOutput) -> Self {
        let chain_position = match output.chain_position {
            ChainPosition::Confirmed {
                anchor,
                transitively,
            } => LegacyChainPosition::Confirmed {
                anchor,
                transitively,
            },
            ChainPosition::Unconfirmed { last_seen, .. } => {
                LegacyChainPosition::Unconfirmed { last_seen }
            }
        };
        Self {
            outpoint: output.outpoint,
            txout: output.txout,
            keychain: output.keychain,
            is_spent: output.is_spent,
            derivation_index: output.derivation_index,
            chain_position,
        }
    }
}

async fn utxo(
    State(state): State<LegacyState>,
    Query(query): Query<LegacyUtxoQuery>,
) -> Result<Json<Vec<LegacyLocalOutput>>, LegacyHttpError> {
    run_legacy_blocking("legacy utxo", move || {
        // Match wallet-service-v2's QueryListReq behavior: when both are present,
        // `address` wins and is expanded to its registered descriptor wallet.
        let desc = if let Some(address) = query.address.as_deref() {
            let account_id = state
                .service
                .resolve_legacy_account_id_for_address(address)?
                .ok_or_else(|| {
                    RgbServiceError::NotFound(format!("legacy address not found: {address}"))
                })?;
            state
                .service
                .resolve_legacy_desc_for_account_id(&account_id)?
                .ok_or_else(|| {
                    RgbServiceError::NotFound(format!(
                        "legacy desc not found for account: {account_id}"
                    ))
                })?
        } else if let Some(desc) = query.desc {
            desc
        } else {
            return Err(
                RgbServiceError::InvalidRequest("desc or address is required".to_string()).into(),
            );
        };

        let mut wallet = open_legacy_wallet(&state.service, &state.config, &desc)?;
        sync_legacy_wallet(&state.service, &state.config, &mut wallet)?;
        let utxos = wallet
            .wallet
            .list_unspent()
            .map(LegacyLocalOutput::from)
            .collect::<Vec<_>>();
        wallet.persist()?;
        Ok(Json(utxos))
    })
    .await
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
    run_legacy_blocking("legacy asset", move || {
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
                    // `address` identifies the legacy descriptor wallet; it is not
                    // an allocation filter. Change can live on another revealed
                    // address of the same descriptor, and legacy wallet balances
                    // must include those sibling-address allocations as well.
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
    })
    .await
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

// Resolve all legacy account_ids that the given descriptor can reach:
//   1. The desc's own account (`legacy-desc:<sha256(desc)>`).
//   2. Every other legacy account that owns a revealed address of the
//      descriptor's wallet (the same xpub may have been imported under a
//      different desc in the wallet-v2 migration, leaving the stock under
//      a different account_id; the address mapping bridges them).
//
// This mirrors the resolution used by `legacy_query_account_ids` for the
// desc case so that `/asset?desc=...` and `/transfer/psbt` agree on
// which account holds the RGB balance. The first entry is always the
// desc's own account_id and is used as the canonical primary key for
// non-RGB state (e.g. `legacy_put_prepared_transfer`).
fn legacy_desc_account_ids(
    state: &LegacyState,
    desc: &str,
) -> Result<Vec<String>, LegacyHttpError> {
    legacy_desc_account_ids_for_service(&state.service, state.config.reveal_address_count, desc)
        .map_err(Into::into)
}

fn legacy_desc_account_ids_for_service(
    service: &LocalDaemonService,
    reveal_address_count: u32,
    desc: &str,
) -> rgb_service_api::Result<Vec<String>> {
    let mut account_ids = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    let desc_account_id = legacy_account_id(desc);
    seen.insert(desc_account_id.clone());
    account_ids.push(desc_account_id);

    let descriptor = ExtendedDescriptor::from_str(desc)
        .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
    let wallet = Wallet::create_single(descriptor)
        .network(service.network()?)
        .create_wallet_no_persist()
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
    for index in 0..=reveal_address_count.max(1) {
        let address = wallet
            .peek_address(KeychainKind::External, index)
            .address
            .to_string();
        // Receiver consignments are stored under their address account before
        // a legacy wallet registers its descriptor. Keep that address-owned
        // stock reachable after `/account/create` installs the address -> desc
        // mapping; otherwise registration makes an existing balance disappear.
        if service.account_stock_has_data(&address) && seen.insert(address.clone()) {
            account_ids.push(address.clone());
        }
        if let Some(account_id) = service.resolve_legacy_account_id_for_address(&address)? {
            if seen.insert(account_id.clone()) {
                account_ids.push(account_id);
            }
        }
    }
    Ok(account_ids)
}

pub(crate) fn legacy_account_ids_for_address(
    service: &LocalDaemonService,
    reveal_address_count: u32,
    address: &str,
) -> rgb_service_api::Result<Vec<String>> {
    if let Some(account_id) = service.resolve_legacy_account_id_for_address(address)? {
        if let Some(desc) = service.resolve_legacy_desc_for_account_id(&account_id)? {
            return legacy_desc_account_ids_for_service(service, reveal_address_count, &desc);
        }
        return Ok(vec![account_id]);
    }
    Ok(vec![address.to_string()])
}

fn legacy_query_account_ids(
    state: &LegacyState,
    query: &QueryAssetReq,
) -> Result<Vec<String>, LegacyHttpError> {
    if let Some(desc) = query.desc.as_deref() {
        return legacy_desc_account_ids(state, desc);
    }
    if let Some(address) = query.address.as_deref() {
        return legacy_account_ids_for_address(
            &state.service,
            state.config.reveal_address_count,
            address,
        )
        .map_err(Into::into);
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
    run_legacy_blocking("legacy asset/internal/issue", move || {
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
                        "utxo is required when the descriptor wallet has no synced UTXO"
                            .to_string(),
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
    })
    .await
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
    Json(req): Json<TransferReq>,
) -> Result<Json<TransferPsbtResp>, LegacyHttpError> {
    run_legacy_blocking("legacy transfer/psbt", move || {
    let mut req = req;
    if req.assign.is_empty() {
        return Err(RgbServiceError::InvalidRequest("assign is empty".to_string()).into());
    }
    let desc = req
        .desc
        .as_deref()
        .ok_or_else(|| RgbServiceError::InvalidRequest("desc is required".to_string()))?;
    // Resolve every legacy account_id reachable from this descriptor so RGB
    // balance checks and input selection stay consistent with `/asset?desc=`
    // (which already aggregates across the same set of accounts). The first
    // entry is the desc's own account_id and remains the canonical primary
    // key for prepared-transfer storage.
    let account_ids = legacy_desc_account_ids(&state, desc)?;
    let account_id = account_ids[0].clone();
    // `transfer_account_id` tracks which resolved account actually owns the
    // RGB stock that will be moved; we use it for `prepare_rgb20_psbt` and
    // `legacy_put_prepared_transfer` so the fascia is staged into the same
    // stock that backs the consumed allocations.
    let mut transfer_account_id = account_id.clone();

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
        // Sum balance across every reachable account_id. The query path
        // already does this, so the transfer's availability check matches
        // what the frontend sees from `/asset?desc=`.
        let have: u64 = account_ids
            .iter()
            .map(|aid| {
                legacy_rgb_balance(&state.service, aid, contract_id).unwrap_or_default()
            })
            .sum();
        if have < *amount {
            return Err(legacy_rgb_insufficient_error(contract_id, *amount, have));
        }
        // Try each reachable account's stock in order; the first that holds
        // the required amount wins. (Split allocations across accounts are
        // not yet supported — the common case is a single stock per wallet.)
        let mut selected = false;
        for aid in &account_ids {
            match select_rgb20_inputs(
                &state.service.account_stock_dir(aid),
                wallet_outpoints.clone(),
                *contract_id,
                *amount,
            ) {
                Ok(inputs) => {
                    if !selected {
                        transfer_account_id = aid.clone();
                        selected = true;
                    }
                    rgb_inputs.extend(inputs);
                    break;
                }
                Err(_) => continue,
            }
        }
        if !selected {
            return Err(RgbServiceError::Backend(format!(
                "no reachable account holds contract {contract_id}"
            ))
            .into());
        }
    }

    let fee_rate = FeeRate::from_sat_per_vb(req.fee_rate)
        .ok_or_else(|| RgbServiceError::InvalidRequest("invalid fee_rate".to_string()))?;
    let change_script = legacy_change_script(&wallet.wallet);
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
        &state.service.account_stock_dir(&transfer_account_id),
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
        &transfer_account_id,
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
        .info(format!("legacy prepared transfer stored account_id={transfer_account_id} (desc_account_id={account_id}) transfer_id={transfer_id}"));
    wallet.persist()?;
    Ok(Json(TransferPsbtResp {
        psbt: prepared.psbt.to_string(),
    }))
    })
    .await
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
    let change_script = legacy_change_script(&wallet.wallet);
    let mut builder = wallet.wallet.build_tx();
    builder
        .ordering(TxOrdering::Untouched)
        .fee_rate(fee_rate)
        .drain_to(change_script);

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
        .unwrap_or(state.service.daemon_rna.transfer_fee)
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

/// Preserve wallet-service-v2's change-address semantics.
///
/// Legacy clients identify a wallet by the first address derived from their
/// single external descriptor and may display BTC balance for that address
/// rather than scanning every subsequently revealed address. With a
/// single-descriptor BDK wallet, `Internal` maps to `External`; peeking index
/// zero therefore returns the stable legacy change address without advancing
/// the derivation index. Using `reveal_next_address` here strands change on a
/// fresh address from the client's point of view and makes its displayed
/// balance too low.
fn legacy_change_script(wallet: &Wallet) -> ScriptBuf {
    wallet
        .peek_address(KeychainKind::Internal, 0)
        .script_pubkey()
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
    run_legacy_blocking("legacy transfer/callback", move || {
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
    let request_desc = req.desc.clone();
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
    let wallet_desc = match request_desc {
        Some(desc) => Some(desc),
        None => match account_id.as_deref() {
            Some(account_id) => state
                .service
                .resolve_legacy_desc_for_account_id(account_id)?,
            None => None,
        },
    };
    if let Some(desc) = wallet_desc.as_deref() {
        apply_legacy_signed_tx(&state, &desc, account_id.as_deref(), &tx)?;
    }
    Ok(StatusCode::OK)
    })
    .await
}

fn apply_legacy_signed_tx(
    state: &LegacyState,
    desc: &str,
    rgb_account_id: Option<&str>,
    tx: &bdk_wallet::bitcoin::Transaction,
) -> Result<(), LegacyHttpError> {
    let mut wallet = open_legacy_wallet(&state.service, &state.config, desc)?;
    let seen_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?
        .as_secs();
    wallet.wallet.apply_unconfirmed_txs([(tx.clone(), seen_at)]);

    let canonical_account_id = legacy_account_id(desc);
    state
        .service
        .put_legacy_account_desc(&canonical_account_id, desc)?;
    for (vout, output) in tx.output.iter().enumerate() {
        if !wallet.wallet.is_mine(output.script_pubkey.clone()) {
            continue;
        }
        let address = Address::from_script(&output.script_pubkey, state.service.network()?)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?
            .to_string();
        state
            .service
            .put_legacy_address_account(&address, &canonical_account_id)?;
        if let Some(account_id) = rgb_account_id {
            state.service.put_account_utxo(
                account_id,
                TrackedUtxo {
                    outpoint: OutPoint::new(tx.compute_txid(), vout as u32).to_string(),
                    address: Some(address),
                    confirmed: false,
                },
            )?;
        }
    }
    wallet.persist()
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
    run_legacy_blocking("legacy transfer/cancel", move || {
        let account_id = legacy_account_id(&req.desc);
        if let Some(transfer_id) = req.transfer_id {
            state
                .service
                .legacy_remove_prepared_transfer(&account_id, &transfer_id)?;
        }
        Ok(StatusCode::OK)
    })
    .await
}

struct LegacyWallet {
    wallet: PersistedWallet<LegacyWalletDb>,
    db: LegacyWalletDb,
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

struct LegacyWalletDb {
    db: SingleWriterTxDatabase,
    wallets: SingleWriterTxKeyspace,
    account_hash: String,
}

impl LegacyWalletDb {
    fn new(db: SingleWriterTxDatabase, account_hash: impl Into<String>) -> Result<Self, String> {
        let wallets = db
            .keyspace("legacy_wallets", KeyspaceCreateOptions::default)
            .map_err(|err| err.to_string())?;
        Ok(Self {
            db,
            wallets,
            account_hash: account_hash.into(),
        })
    }

    fn load_changeset(&self) -> Result<ChangeSet, String> {
        self.wallets
            .get(self.account_hash.as_bytes())
            .map_err(|err| err.to_string())?
            .map(|bytes| bincode::deserialize(bytes.as_ref()).map_err(|err| err.to_string()))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    fn store_changeset(&self, changeset: &ChangeSet) -> Result<(), String> {
        let bytes = bincode::serialize(changeset).map_err(|err| err.to_string())?;
        let mut tx = self.db.write_tx();
        tx.insert(&self.wallets, self.account_hash.as_bytes(), bytes);
        tx.commit().map_err(|err| err.to_string())?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| err.to_string())
    }
}

impl WalletPersister for LegacyWalletDb {
    type Error = String;

    fn initialize(persister: &mut Self) -> Result<ChangeSet, Self::Error> {
        persister.load_changeset()
    }

    fn persist(persister: &mut Self, changeset: &ChangeSet) -> Result<(), Self::Error> {
        let mut aggregate = persister.load_changeset()?;
        aggregate.merge(changeset.clone());
        persister.store_changeset(&aggregate)
    }
}

fn migrate_one_legacy_wallet_file(
    service: &LocalDaemonService,
    account_hash: &str,
    target: &mut LegacyWalletDb,
) -> Result<bool, LegacyHttpError> {
    if !target
        .load_changeset()
        .map_err(RgbServiceError::Backend)?
        .is_empty()
    {
        return Ok(false);
    }
    let source = service
        .config
        .data_dir
        .join("legacy-wallets")
        .join(account_hash)
        .join(BDK_FILE);
    if !source.is_file() {
        return Ok(false);
    }
    let (_, changeset) =
        file_store::Store::<ChangeSet>::load(LEGACY_BDK_MAGIC, &source).map_err(|err| {
            RgbServiceError::Backend(format!(
                "load legacy wallet file {}: {err}",
                source.display()
            ))
        })?;
    let Some(changeset) = changeset else {
        return Ok(false);
    };
    target
        .store_changeset(&changeset)
        .map_err(RgbServiceError::Backend)?;
    Ok(true)
}

pub(crate) fn migrate_legacy_wallets_to_database(
    service: &LocalDaemonService,
) -> Result<usize, RgbServiceError> {
    const MIGRATION: &[u8] = b"legacy_wallet_files_to_shared_v1";
    let migrations = service
        .db
        .keyspace("schema_migrations", KeyspaceCreateOptions::default)
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
    if migrations
        .get(MIGRATION)
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?
        .is_some()
    {
        return Ok(0);
    }

    let legacy_dir = service.config.data_dir.join("legacy-wallets");
    let mut migrated = 0usize;
    if legacy_dir.is_dir() {
        let mut entries = std::fs::read_dir(&legacy_dir)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            if !entry
                .file_type()
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?
                .is_dir()
            {
                continue;
            }
            let account_hash = entry.file_name().to_string_lossy().to_string();
            let mut target = LegacyWalletDb::new(service.db.clone(), &account_hash)
                .map_err(RgbServiceError::Backend)?;
            if migrate_one_legacy_wallet_file(service, &account_hash, &mut target)
                .map_err(|err| err.err)?
            {
                migrated += 1;
            }
        }
    }

    let mut tx = service.db.write_tx();
    tx.insert(
        &migrations,
        MIGRATION,
        format!("migrated={migrated}").as_bytes(),
    );
    tx.commit()
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
    service
        .db
        .persist(PersistMode::SyncAll)
        .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
    Ok(migrated)
}

fn open_legacy_wallet(
    service: &LocalDaemonService,
    config: &LegacyConfig,
    desc: &str,
) -> Result<LegacyWallet, LegacyHttpError> {
    let network = service.network()?;
    let descriptor = ExtendedDescriptor::from_str(desc)
        .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
    let account_hash = legacy_account_hash(desc);
    let mut db =
        LegacyWalletDb::new(service.db.clone(), &account_hash).map_err(RgbServiceError::Backend)?;
    migrate_one_legacy_wallet_file(service, &account_hash, &mut db)?;
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
    status: Option<StatusCode>,
}

impl LegacyHttpError {
    fn with_code(err: RgbServiceError, legacy_code: u8) -> Self {
        Self {
            err,
            legacy_code: Some(legacy_code),
            status: None,
        }
    }

    fn with_status(err: RgbServiceError, status: StatusCode) -> Self {
        Self {
            err,
            legacy_code: None,
            status: Some(status),
        }
    }
}

impl From<RgbServiceError> for LegacyHttpError {
    fn from(err: RgbServiceError) -> Self {
        Self {
            err,
            legacy_code: None,
            status: None,
        }
    }
}

impl IntoResponse for LegacyHttpError {
    fn into_response(self) -> Response {
        let status = self.status.unwrap_or_else(|| match &self.err {
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
        });
        let message = self.err.to_string();
        let body = match self.legacy_code {
            Some(code) => serde_json::json!({ "code": code, "message": message }),
            None => serde_json::json!({ "message": message }),
        };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bdk_wallet::bitcoin::{Network, PublicKey};

    const LEGACY_DESCRIPTOR_KEY: &str = "[a49cd98b/84'/827166'/0']xpub6Bz49QXuN7g57fzNQJA8sbQKu8ihjcaPKCwYUq3HXXn5LXNn6ejuXEUSmcHgAFAdtyBgxFyumSNivxp5gtwbN7XkUTEMh4vuLTBfW3ff82T/0/*";

    #[test]
    fn stake_address_script_matches_wallet_service_v2_contract() {
        let public_key = PublicKey::from_str(
            "03b9175c5dbb731da31fec7b9b0d04d5c4a7d098e06d1e2cbaacaef58d687e963b",
        )
        .unwrap();
        let script = make_stake_script(public_key, 10);

        assert_eq!(
            crate::hex_encode(script.as_bytes()),
            "5ab2752103b9175c5dbb731da31fec7b9b0d04d5c4a7d098e06d1e2cbaacaef58d687e963bac"
        );
        assert!(Address::p2wsh(&script, Network::Bitcoin)
            .to_string()
            .starts_with("bc1q"));
    }

    #[test]
    fn redeem_list_keeps_wallet_service_v2_assignment_tuple_shape() {
        let response = RedeemListResp {
            spend_txid: None,
            outpoint: format!("{}:0", Txid::all_zeros()),
            sats: 100_000,
            height: 10,
            public_key: "03b9175c5dbb731da31fec7b9b0d04d5c4a7d098e06d1e2cbaacaef58d687e963b"
                .to_string(),
            status: 0,
            assign_map: Some(("rgb:test".to_string(), 42)),
            create_time: "2026-08-16T12:00:00+08:00".to_string(),
            confirm_height: Some(100),
        };

        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["assign_map"], serde_json::json!(["rgb:test", 42]));
        assert_eq!(value["height"], 10);
        assert_eq!(value["sats"], 100_000);
    }

    #[test]
    fn redeem_psbt_restores_owner_bip32_derivation() {
        use bdk_wallet::miniscript::Descriptor;

        let descriptor =
            ExtendedDescriptor::from_str(&format!("wpkh({LEGACY_DESCRIPTOR_KEY})")).unwrap();
        let derived = descriptor
            .derived_descriptor(&Secp256k1::verification_only(), 0)
            .unwrap();
        let Descriptor::Wpkh(wpkh) = derived else {
            panic!("test descriptor must be wpkh");
        };
        let public_key = *wpkh.as_inner();
        let tx = Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1_000),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let mut psbt = Psbt::from_unsigned_tx(tx).unwrap();

        let result = add_redeem_bip32_derivation(
            &mut psbt,
            &format!("wpkh({LEGACY_DESCRIPTOR_KEY})"),
            public_key,
        );
        assert!(result.is_ok());

        let (fingerprint, path) = psbt.inputs[0]
            .bip32_derivation
            .get(&public_key.inner)
            .unwrap();
        assert_eq!(fingerprint.to_string(), "a49cd98b");
        assert_eq!(path.to_string(), "84'/827166'/0'/0/0");
    }

    #[test]
    fn legacy_change_address_stays_at_index_zero_for_all_supported_script_types() {
        let descriptors = [
            format!("pkh({LEGACY_DESCRIPTOR_KEY})"),
            format!("sh(wpkh({LEGACY_DESCRIPTOR_KEY}))"),
            format!("wpkh({LEGACY_DESCRIPTOR_KEY})"),
            format!("tr({LEGACY_DESCRIPTOR_KEY})"),
        ];

        for descriptor in descriptors {
            let descriptor = ExtendedDescriptor::from_str(&descriptor).unwrap();
            let mut wallet = Wallet::create_single(descriptor)
                .network(Network::Bitcoin)
                .create_wallet_no_persist()
                .unwrap();
            let _ = wallet.reveal_addresses_to(KeychainKind::External, 20);

            let first_external = wallet
                .peek_address(KeychainKind::External, 0)
                .script_pubkey();
            assert_eq!(legacy_change_script(&wallet), first_external);

            let next_external = wallet
                .reveal_next_address(KeychainKind::External)
                .script_pubkey();
            assert_ne!(next_external, first_external);
            assert_eq!(legacy_change_script(&wallet), first_external);
        }
    }

    #[test]
    fn legacy_utxo_json_keeps_bdk_v1_chain_position_shape() {
        let output = LegacyLocalOutput {
            outpoint: OutPoint::null(),
            txout: TxOut {
                value: Amount::from_sat(9_985_779),
                script_pubkey: ScriptBuf::new(),
            },
            keychain: KeychainKind::External,
            is_spent: false,
            derivation_index: 21,
            chain_position: LegacyChainPosition::Unconfirmed {
                last_seen: Some(42),
            },
        };

        let value = serde_json::to_value(output).unwrap();
        assert_eq!(value["derivation_index"], 21);
        assert_eq!(
            value["chain_position"],
            serde_json::json!({"Unconfirmed": {"last_seen": 42}})
        );
        assert!(value["chain_position"]["Unconfirmed"]
            .get("first_seen")
            .is_none());
    }

    #[test]
    fn legacy_wallet_state_roundtrips_through_shared_fjall_database() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rgb-legacy-wallet-db-{suffix}"));
        std::fs::create_dir_all(&path).unwrap();
        let database = SingleWriterTxDatabase::builder(&path).open().unwrap();
        let descriptor =
            ExtendedDescriptor::from_str(&format!("wpkh({LEGACY_DESCRIPTOR_KEY})")).unwrap();
        let mut persister = LegacyWalletDb::new(database.clone(), "account-a").unwrap();
        let mut wallet = Wallet::create_single(descriptor.clone())
            .network(Network::Bitcoin)
            .create_wallet(&mut persister)
            .unwrap();
        let revealed = wallet.reveal_next_address(KeychainKind::External).address;
        wallet.persist(&mut persister).unwrap();

        let mut reopened = LegacyWalletDb::new(database, "account-a").unwrap();
        let wallet = Wallet::load()
            .descriptor(KeychainKind::External, Some(descriptor))
            .check_network(Network::Bitcoin)
            .load_wallet(&mut reopened)
            .unwrap()
            .unwrap();
        assert_eq!(
            wallet.peek_address(KeychainKind::External, 0).address,
            revealed
        );
    }
}
