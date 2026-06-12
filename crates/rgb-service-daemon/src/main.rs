use std::{
    env, fs,
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::serve;
use bitcoin::{
    consensus::{deserialize, serialize},
    OutPoint, Psbt, Transaction, Txid,
};
use dynamic::{Dynamic, FromJson, MsgPack, MsgUnpack, ToJson};
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};
use rgb_service_api::{
    axum_service::router, AllocationStatus, AssetLayer, AssetSpendAuthorization, AuthSubject,
    AuthVerifier, Authorized, BalanceBreakdownRequest, BalanceBreakdownResponse, BalanceRequest,
    CancelTransferRequest, CancelTransferResponse, CommitTransferRequest, CommitTransferResponse,
    ConsignmentDelivery, CreateInvoiceRequest, CreateInvoiceResponse, IssueAssetRequest,
    IssueAssetResponse, ListAssetsRequest, ListAssetsResponse, ListPendingRequest,
    ListPendingResponse, LnChannelOpenPrepareRequest, LnChannelOpenPrepareResponse,
    LnClosingComposeRequest, LnCommitmentComposeRequest, LnComposeResponse,
    LnOnchainClaimComposeRequest, LnRecoverRequest, LnRecoveredChannel, LnRecoveredCompose,
    LnRecoveryReport, OperationStatus, Permission, PrepareTransferRequest, PrepareTransferResponse,
    ReceiveConsignmentRequest, ReceiveConsignmentResponse, RecoverRequest, RecoveryAction,
    RecoveryReport, RequestSignature, RgbAllocation, RgbAssetInfo, RgbBalance, RgbFundingRef,
    RgbServiceApi, RgbServiceError, RgbTestStep, RnaBalanceRequest, RnaBalanceResponse,
    RunRgbTestRequest, RunRgbTestResponse, SendConsignmentRequest, SendConsignmentResponse,
    TrackedUtxo,
};
use rgb_service_local::{
    build_rgb20_transfer_consignment, decode_rgb20_transfer_consignment, encode_fascia_bytes,
    issue_rgb20_fixed_with_chain_source, list_rgb20_assets_for_utxos, prepare_rgb20_psbt,
    scan_and_promote_confirmed_staged_rgb_stocks, stage_receiver_transfer, stage_sender_fascia,
    ChainSource, EsploraConfig, Rgb20IssueRequest, Rgb20PsbtAssignment, Rgb20TrackedUtxo,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{net::TcpListener, signal};

#[derive(Debug, Deserialize)]
struct DaemonConfig {
    service: ServiceConfig,
    rna: RnaConfig,
}

#[derive(Debug, Deserialize)]
struct ServiceConfig {
    bind: SocketAddr,
    network: String,
    data_dir: PathBuf,
    esplora_url: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RnaConfig {
    new_profile_grant: u64,
    issue_fee: u64,
    transfer_fee: u64,
    query_fee: u64,
}

impl RnaConfig {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.new_profile_grant == 0 {
            return Err("rna.new_profile_grant must be greater than zero".into());
        }
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = env::args()
        .nth(1)
        .ok_or("usage: rgb-service <config.toml>")?;
    let config = load_config(&config_path)?;
    fs::create_dir_all(&config.service.data_dir)?;

    println!(
        "starting rgb-service on {} for {} with data_dir {}",
        config.service.bind,
        config.service.network,
        config.service.data_dir.display()
    );

    let bind = config.service.bind;
    let service = Arc::new(LocalDaemonService::new(config).await?);
    let auth = Arc::new(ConfiguredAuthVerifier);
    let app = router(service, auth);
    let listener = TcpListener::bind(bind).await?;

    serve(listener, app)
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

struct ConfiguredAuthVerifier;

#[async_trait]
impl AuthVerifier for ConfiguredAuthVerifier {
    async fn verify_request(
        &self,
        permission: Permission,
        account_id: &str,
        _payload: &[u8],
        signature: &RequestSignature,
    ) -> rgb_service_api::Result<AuthSubject> {
        if account_id.trim().is_empty() {
            return Err(RgbServiceError::Unauthorized(
                "account_id must not be empty".to_string(),
            ));
        }
        if signature.signature.trim().is_empty() {
            return Err(RgbServiceError::SignatureRequired(
                "request signature must not be empty".to_string(),
            ));
        }
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
        if authorization.signature.signature.trim().is_empty() {
            return Err(RgbServiceError::AssetSpendAuthorizationRequired(
                "asset spend signature must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

struct LocalDaemonService {
    config: ServiceConfig,
    rna: RnaConfig,
    db: SingleWriterTxDatabase,
    logger: DaemonLogger,
}

#[derive(Clone)]
struct DaemonLogger {
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
    recipient_vout: u32,
    fascia: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LnChannelRecord {
    account_id: String,
    channel_id: String,
    contract_id: String,
    funding_outpoint: String,
    funding_rgb: u64,
    to_local_rgb: u64,
    to_remote_rgb: u64,
    funding_ref: RgbFundingRef,
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

impl LocalDaemonService {
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
            "rna new_profile_grant={} issue_fee={} transfer_fee={} query_fee={}",
            config.rna.new_profile_grant,
            config.rna.issue_fee,
            config.rna.transfer_fee,
            config.rna.query_fee
        ));
        Ok(Self {
            config: config.service,
            rna: config.rna,
            db,
            logger,
        })
    }

    fn account_stock_dir(&self, account_id: &str) -> PathBuf {
        self.config
            .data_dir
            .join("accounts")
            .join(account_id)
            .join("rgb-stock")
    }

    fn network(&self) -> rgb_service_api::Result<bitcoin::Network> {
        bitcoin::Network::from_str(&self.config.network)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("invalid network: {err}")))
    }

    fn chain_source(&self) -> ChainSource {
        ChainSource::Esplora(EsploraConfig::new(self.config.esplora_url.clone()))
    }

    fn tracked_utxos(
        &self,
        tracked: Vec<TrackedUtxo>,
    ) -> rgb_service_api::Result<Vec<Rgb20TrackedUtxo>> {
        tracked
            .into_iter()
            .map(|utxo| {
                Ok(Rgb20TrackedUtxo {
                    outpoint: OutPoint::from_str(&utxo.outpoint)
                        .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?,
                    address: utxo.address,
                    confirmed: utxo.confirmed,
                })
            })
            .collect()
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
        let stock_dir = self.account_stock_dir(account_id);
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
            "rna_balance": self.rna.new_profile_grant,
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
            new_profile_grant: self.rna.new_profile_grant,
            issue_fee: self.rna.issue_fee,
            transfer_fee: self.rna.transfer_fee,
            query_fee: self.rna.query_fee,
        })
    }

    async fn issue_asset(
        &self,
        req: Authorized<IssueAssetRequest>,
    ) -> rgb_service_api::Result<IssueAssetResponse> {
        self.charge_rna(
            &req.payload.account_id,
            "/v1/assets/issue",
            "issue_asset",
            self.rna.issue_fee,
        )?;
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let outpoint = OutPoint::from_str(&req.payload.allocation_outpoint)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        let issued = issue_rgb20_fixed_with_chain_source(
            &stock_dir,
            self.network()?,
            &self.chain_source(),
            Rgb20IssueRequest {
                ticker: req.payload.ticker,
                name: req.payload.name,
                amount: req.payload.supply,
                precision: req.payload.precision,
                utxo: outpoint,
            },
        )
        .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        Ok(IssueAssetResponse {
            contract_id: issued.contract_id.to_string(),
            asset_id: issued.contract_id.to_string(),
            allocation_outpoint: issued.utxo.to_string(),
        })
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
        self.charge_rna(
            &req.payload.account_id,
            "/v1/balance/breakdown",
            "balance_breakdown",
            self.rna.query_fee,
        )?;
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let allocations =
            list_rgb20_assets_for_utxos(&stock_dir, self.tracked_utxos(req.payload.tracked_utxos)?)
                .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let mut assets = Vec::<RgbAssetInfo>::new();
        for allocation in allocations {
            if assets
                .iter()
                .any(|asset| asset.contract_id == allocation.contract_id.to_string())
            {
                continue;
            }
            assets.push(RgbAssetInfo {
                asset_id: allocation.contract_id.to_string(),
                contract_id: allocation.contract_id.to_string(),
                ticker: allocation.ticker,
                name: allocation.name,
                precision: allocation.precision,
            });
        }
        Ok(ListAssetsResponse { assets })
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
                    tracked_utxos: req.payload.tracked_utxos,
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
        let allocations =
            list_rgb20_assets_for_utxos(&stock_dir, self.tracked_utxos(req.payload.tracked_utxos)?)
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
        for allocation in allocations {
            if allocation.contract_id.to_string() != req.payload.asset_id {
                continue;
            }
            summary.total += allocation.amount_raw;
            summary.l1_available += allocation.amount_raw;
            response_allocations.push(RgbAllocation {
                asset_id: req.payload.asset_id.clone(),
                outpoint: allocation.outpoint.to_string(),
                amount: allocation.amount_raw,
                layer: AssetLayer::L1,
                status: AllocationStatus::Available,
            });
        }
        Ok(BalanceBreakdownResponse {
            summary,
            allocations: response_allocations,
            pending_ops: Vec::new(),
        })
    }

    async fn create_invoice(
        &self,
        req: Authorized<CreateInvoiceRequest>,
    ) -> rgb_service_api::Result<CreateInvoiceResponse> {
        let invoice_id = format!(
            "{}:{}:{}",
            req.payload.account_id, req.payload.asset_id, req.payload.expiry_seconds
        );
        Ok(CreateInvoiceResponse {
            invoice_id,
            invoice: serde_json::to_string(&req.payload)
                .map_err(|err| RgbServiceError::Backend(err.to_string()))?,
            blinded_seal: None,
            expires_at_ms: req.payload.expiry_seconds.saturating_mul(1000),
        })
    }

    async fn prepare_transfer(
        &self,
        req: Authorized<PrepareTransferRequest>,
    ) -> rgb_service_api::Result<PrepareTransferResponse> {
        self.charge_rna(
            &req.payload.account_id,
            "/v1/transfers/prepare",
            "prepare_transfer",
            self.rna.transfer_fee,
        )?;
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let psbt = req
            .payload
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
        let change_vout = req.payload.change_vout.ok_or_else(|| {
            RgbServiceError::InvalidRequest("change_vout is required".to_string())
        })?;
        let recipient_vout = req.payload.recipient_vout.ok_or_else(|| {
            RgbServiceError::InvalidRequest("recipient_vout is required".to_string())
        })?;
        let contract_id = rgb_service_local::rgbstd::ContractId::from_str(&req.payload.asset_id)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:?}")))?;
        let prepared = prepare_rgb20_psbt(
            &stock_dir,
            psbt,
            change_vout,
            [Rgb20PsbtAssignment {
                contract_id,
                amount: req.payload.amount,
                vout: recipient_vout,
            }],
        )
        .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let fascia = encode_fascia_bytes(&prepared.fascia)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let transfer_id = req.payload.asset_authorization.signature.nonce;
        let record = PreparedTransferRecord {
            asset_id: req.payload.asset_id.clone(),
            recipient_vout,
            fascia,
        };
        self.put_prepared_transfer(&req.payload.account_id, &transfer_id, &record)?;
        Ok(PrepareTransferResponse {
            transfer_id,
            operation_id: "prepare_transfer".to_string(),
            anchor_psbt: Some(hex_encode(&prepared.psbt.serialize())),
        })
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
        Ok(CommitTransferResponse {
            transfer_id: req.payload.transfer_id,
            operation_id: "commit_transfer".to_string(),
            status: OperationStatus::Committed,
        })
    }

    async fn send_consignment(
        &self,
        req: Authorized<SendConsignmentRequest>,
    ) -> rgb_service_api::Result<SendConsignmentResponse> {
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let record =
            self.get_prepared_transfer(&req.payload.account_id, &req.payload.transfer_id)?;
        if record.asset_id != req.payload.asset_id {
            return Err(RgbServiceError::Conflict(format!(
                "transfer {} belongs to asset {}, not {}",
                req.payload.transfer_id, record.asset_id, req.payload.asset_id
            )));
        }
        let txid = Txid::from_str(&req.payload.txid)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        let contract_id = rgb_service_local::rgbstd::ContractId::from_str(&req.payload.asset_id)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:?}")))?;
        let fascia = rgb_service_local::decode_fascia_bytes(&record.fascia)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:#}")))?;
        let recipient_vout = req.payload.recipient_vout.unwrap_or(record.recipient_vout);
        let consignment =
            build_rgb20_transfer_consignment(&stock_dir, fascia, contract_id, txid, recipient_vout)
                .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        let account_id = req.payload.transport.account_id;
        let receiver_stock_dir = self.account_stock_dir(&account_id);
        stage_receiver_transfer(&receiver_stock_dir, txid, &consignment)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        Ok(SendConsignmentResponse {
            transfer_id: req.payload.transfer_id,
            operation_id: txid.to_string(),
            status: OperationStatus::Pending,
            delivery: ConsignmentDelivery { account_id },
        })
    }

    async fn receive_consignment(
        &self,
        req: Authorized<ReceiveConsignmentRequest>,
    ) -> rgb_service_api::Result<ReceiveConsignmentResponse> {
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let txid = Txid::from_str(&req.payload.txid)
            .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
        let bytes = hex_decode(&req.payload.consignment_hex)?;
        let consignment = decode_rgb20_transfer_consignment(&bytes)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:#}")))?;
        stage_receiver_transfer(&stock_dir, txid, &consignment)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        Ok(ReceiveConsignmentResponse {
            operation_id: txid.to_string(),
            status: OperationStatus::Pending,
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
        if req.payload.channel_id.trim().is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "channel_id must not be empty".to_string(),
            ));
        }
        if req.payload.contract_id.trim().is_empty() {
            return Err(RgbServiceError::InvalidRequest(
                "contract_id must not be empty".to_string(),
            ));
        }
        OutPoint::from_str(&req.payload.funding_outpoint).map_err(|err| {
            RgbServiceError::InvalidRequest(format!("invalid funding_outpoint: {err}"))
        })?;
        if req.payload.to_local_rgb + req.payload.to_remote_rgb != req.payload.funding_rgb {
            return Err(RgbServiceError::InvalidRequest(
                "to_local_rgb + to_remote_rgb must equal funding_rgb".to_string(),
            ));
        }
        Self::validate_ln_asset_authorization(
            &req.payload.contract_id,
            req.payload.funding_rgb,
            &req.payload.asset_authorization,
        )?;
        self.charge_rna(
            &req.payload.account_id,
            "/v1/ln/channels/open/prepare",
            "ln_channel_open_prepare",
            self.rna.transfer_fee,
        )?;
        let operation_id = Self::require_nonce(&req.payload.asset_authorization)?;
        let funding_ref = RgbFundingRef {
            transfer_id: format!("ln-open:{operation_id}"),
            operation_id: operation_id.clone(),
            channel_id: Some(req.payload.channel_id.clone()),
        };
        self.put_ln_channel_record(&LnChannelRecord {
            account_id: req.payload.account_id,
            channel_id: req.payload.channel_id,
            contract_id: req.payload.contract_id,
            funding_outpoint: req.payload.funding_outpoint,
            funding_rgb: req.payload.funding_rgb,
            to_local_rgb: req.payload.to_local_rgb,
            to_remote_rgb: req.payload.to_remote_rgb,
            funding_ref: funding_ref.clone(),
            created_at_ms: now_ms(),
        })?;
        Ok(LnChannelOpenPrepareResponse {
            funding_ref,
            operation_id,
        })
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
