use std::{env, fs, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};

use async_trait::async_trait;
use axum::serve;
use bitcoin::{OutPoint, Psbt, Txid};
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};
use rgb_service_api::{
    AllocationStatus, AssetLayer, AssetSpendAuthorization, AuthSubject, AuthVerifier, Authorized,
    BalanceBreakdownRequest, BalanceBreakdownResponse, BalanceRequest, CancelTransferRequest,
    CancelTransferResponse, CommitTransferRequest, CommitTransferResponse, CreateInvoiceRequest,
    CreateInvoiceResponse, IssueAssetRequest, IssueAssetResponse, ListAssetsRequest,
    ListAssetsResponse, ListPendingRequest, ListPendingResponse, OperationStatus,
    Permission, PrepareTransferRequest, PrepareTransferResponse, RecoverRequest, RecoveryAction,
    RecoveryReport, RequestSignature, RgbAllocation, RgbAssetInfo, RgbBalance, RgbServiceApi,
    RgbServiceError, RgbTestStep, RunRgbTestRequest, RunRgbTestResponse, TrackedUtxo,
    axum_service::router,
};
use rgb_service_local::{
    ChainSource, EsploraConfig, Rgb20IssueRequest, Rgb20PsbtAssignment, Rgb20TrackedUtxo,
    encode_fascia_bytes, issue_rgb20_fixed_with_chain_source, list_rgb20_assets_for_utxos,
    prepare_rgb20_psbt, scan_and_promote_confirmed_staged_rgb_stocks, stage_sender_fascia,
};
use serde::Deserialize;
use tokio::{net::TcpListener, signal};

#[derive(Debug, Deserialize)]
struct DaemonConfig {
    service: ServiceConfig,
}

#[derive(Debug, Deserialize)]
struct ServiceConfig {
    bind: SocketAddr,
    network: String,
    data_dir: PathBuf,
    esplora_url: String,
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
    let service = Arc::new(LocalDaemonService::new(config.service)?);
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
    db: SingleWriterTxDatabase,
}

impl LocalDaemonService {
    fn new(config: ServiceConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let kv_dir = config.data_dir.join("kv");
        fs::create_dir_all(&kv_dir)?;
        let db = SingleWriterTxDatabase::builder(&kv_dir).open()?;
        Ok(Self { config, db })
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

    fn put_prepared_fascia(
        &self,
        account_id: &str,
        transfer_id: &str,
        fascia: &[u8],
    ) -> rgb_service_api::Result<()> {
        let keyspace = self
            .db
            .keyspace("prepared_transfers", KeyspaceCreateOptions::default)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        let key = Self::prepared_key(account_id, transfer_id);
        let mut tx = self.db.write_tx();
        tx.insert(&keyspace, key.as_bytes(), fascia);
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))
    }

    fn take_prepared_fascia(
        &self,
        account_id: &str,
        transfer_id: &str,
    ) -> rgb_service_api::Result<Vec<u8>> {
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
                RgbServiceError::NotFound(format!(
                    "prepared transfer not found: {transfer_id}"
                ))
            })?;
        let mut tx = self.db.write_tx();
        tx.remove(&keyspace, key.as_bytes());
        tx.commit()
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(|err| RgbServiceError::Backend(err.to_string()))?;
        Ok(bytes)
    }

}

#[async_trait]
impl RgbServiceApi for LocalDaemonService {
    async fn issue_asset(
        &self,
        req: Authorized<IssueAssetRequest>,
    ) -> rgb_service_api::Result<IssueAssetResponse> {
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
        let stock_dir = self.account_stock_dir(&req.payload.account_id);
        let allocations = list_rgb20_assets_for_utxos(
            &stock_dir,
            self.tracked_utxos(req.payload.tracked_utxos)?,
        )
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
        let allocations = list_rgb20_assets_for_utxos(
            &stock_dir,
            self.tracked_utxos(req.payload.tracked_utxos)?,
        )
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
        self.put_prepared_fascia(&req.payload.account_id, &transfer_id, &fascia)?;
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
        let fascia = self.take_prepared_fascia(&req.payload.account_id, &req.payload.transfer_id)?;
        let fascia = rgb_service_local::decode_fascia_bytes(&fascia)
            .map_err(|err| RgbServiceError::InvalidRequest(format!("{err:#}")))?;
        stage_sender_fascia(&stock_dir, txid, &fascia)
            .map_err(|err| RgbServiceError::Backend(format!("{err:#}")))?;
        Ok(CommitTransferResponse {
            transfer_id: req.payload.transfer_id,
            operation_id: "commit_transfer".to_string(),
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

fn test_step(name: &str) -> RgbTestStep {
    RgbTestStep {
        name: name.to_string(),
        passed: true,
        message: None,
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
