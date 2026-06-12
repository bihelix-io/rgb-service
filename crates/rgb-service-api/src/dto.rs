use serde::{Deserialize, Serialize};

use crate::auth::{AccountScoped, AssetSpendAuthorization, AssetSpendAuthorized};

pub type AccountId = String;
pub type AssetId = String;
pub type ContractId = String;
pub type OperationId = String;
pub type TransferId = String;
pub type ConsignmentId = String;
pub type InvoiceId = String;
pub type ReservationId = String;
pub type Txid = String;
pub type Outpoint = String;
pub type BtcAddress = String;
pub type IrohNodeId = String;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegisterIrohNodeRequest {
    pub account_id: AccountId,
    pub btc_address: BtcAddress,
    pub iroh_node_id: IrohNodeId,
    pub label: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegisterIrohNodeResponse {
    pub binding: IrohNodeBinding,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LookupIrohNodeRequest {
    pub account_id: AccountId,
    pub btc_address: BtcAddress,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LookupIrohNodeResponse {
    pub binding: Option<IrohNodeBinding>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RnaBalanceRequest {
    pub account_id: AccountId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RnaBalanceResponse {
    pub account_id: AccountId,
    pub rna_balance: u64,
    pub new_profile_grant: u64,
    pub issue_fee: u64,
    pub transfer_fee: u64,
    pub query_fee: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IrohNodeBinding {
    pub account_id: AccountId,
    pub btc_address: BtcAddress,
    pub iroh_node_id: IrohNodeId,
    pub label: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IssueAssetRequest {
    pub account_id: AccountId,
    pub ticker: String,
    pub name: String,
    pub precision: u8,
    pub supply: u64,
    pub allocation_outpoint: Outpoint,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IssueAssetResponse {
    pub contract_id: ContractId,
    pub asset_id: AssetId,
    pub allocation_outpoint: Outpoint,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListAssetsRequest {
    pub account_id: AccountId,
    #[serde(default)]
    pub tracked_utxos: Vec<TrackedUtxo>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListAssetsResponse {
    pub assets: Vec<RgbAssetInfo>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbAssetInfo {
    pub asset_id: AssetId,
    pub contract_id: ContractId,
    pub ticker: String,
    pub name: String,
    pub precision: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BalanceRequest {
    pub account_id: AccountId,
    pub asset_id: AssetId,
    pub scope: BalanceScope,
    #[serde(default)]
    pub tracked_utxos: Vec<TrackedUtxo>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BalanceScope {
    All,
    L1,
    L2,
    Account,
    Channel(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbBalance {
    pub asset_id: AssetId,
    pub total: u64,
    pub l1_available: u64,
    pub l1_pending_in: u64,
    pub l1_pending_out: u64,
    pub l2_available: u64,
    pub l2_locked: u64,
    pub l2_pending_in: u64,
    pub l2_pending_out: u64,
    pub reserved: u64,
    pub settling: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BalanceBreakdownRequest {
    pub account_id: AccountId,
    pub asset_id: AssetId,
    #[serde(default)]
    pub tracked_utxos: Vec<TrackedUtxo>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BalanceBreakdownResponse {
    pub summary: RgbBalance,
    pub allocations: Vec<RgbAllocation>,
    pub pending_ops: Vec<PendingOperation>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbAllocation {
    pub asset_id: AssetId,
    pub outpoint: Outpoint,
    pub amount: u64,
    pub layer: AssetLayer,
    pub status: AllocationStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackedUtxo {
    pub outpoint: Outpoint,
    pub address: Option<String>,
    pub confirmed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetLayer {
    L1,
    L2,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationStatus {
    Available,
    Reserved,
    PendingIn,
    PendingOut,
    Settling,
    Locked,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateInvoiceRequest {
    pub account_id: AccountId,
    pub asset_id: AssetId,
    pub amount: Option<u64>,
    pub expiry_seconds: u64,
    pub transport_hints: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateInvoiceResponse {
    pub invoice_id: InvoiceId,
    pub invoice: String,
    pub blinded_seal: Option<String>,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrepareTransferRequest {
    pub account_id: AccountId,
    pub asset_id: AssetId,
    pub amount: u64,
    pub recipient: String,
    pub fee_rate_sat_vb: Option<u64>,
    pub unsigned_anchor_psbt: Option<String>,
    pub change_vout: Option<u32>,
    pub recipient_vout: Option<u32>,
    pub asset_authorization: AssetSpendAuthorization,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrepareTransferResponse {
    pub transfer_id: TransferId,
    pub operation_id: OperationId,
    pub anchor_psbt: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitTransferRequest {
    pub account_id: AccountId,
    pub transfer_id: TransferId,
    pub txid: Txid,
    pub signed_anchor_psbt: Option<String>,
    pub asset_authorization: AssetSpendAuthorization,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitTransferResponse {
    pub transfer_id: TransferId,
    pub operation_id: OperationId,
    pub status: OperationStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendConsignmentRequest {
    pub account_id: AccountId,
    pub transfer_id: TransferId,
    pub asset_id: AssetId,
    pub txid: Txid,
    pub recipient_vout: Option<u32>,
    pub transport: ConsignmentTransport,
    pub asset_authorization: AssetSpendAuthorization,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendConsignmentResponse {
    pub transfer_id: TransferId,
    pub operation_id: OperationId,
    pub status: OperationStatus,
    pub delivery: ConsignmentDelivery,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConsignmentTransport {
    Inline,
    ServiceInbox { account_id: AccountId },
    Iroh {
        node_id: String,
        topic: Option<String>,
        timeout_ms: Option<u64>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConsignmentDelivery {
    Inline { consignment_hex: String },
    ServiceInbox { account_id: AccountId },
    Iroh {
        node_id: String,
        delivery_id: ConsignmentId,
        status: ConsignmentDeliveryStatus,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsignmentDeliveryStatus {
    Pending,
    Sent,
    Delivered,
    Accepted,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReceiveConsignmentRequest {
    pub account_id: AccountId,
    pub txid: Txid,
    pub consignment_hex: String,
    pub source_transfer_id: Option<TransferId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReceiveConsignmentResponse {
    pub operation_id: OperationId,
    pub status: OperationStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CancelTransferRequest {
    pub account_id: AccountId,
    pub transfer_id: TransferId,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CancelTransferResponse {
    pub transfer_id: TransferId,
    pub status: OperationStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListPendingRequest {
    pub account_id: AccountId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListPendingResponse {
    pub pending: Vec<PendingOperation>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecoverRequest {
    pub account_id: AccountId,
    pub operation_id: Option<OperationId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunRgbTestRequest {
    pub account_id: AccountId,
    pub scenario: RgbTestScenario,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RgbTestScenario {
    FullRgb20Lifecycle,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunRgbTestResponse {
    pub scenario: RgbTestScenario,
    pub passed: bool,
    pub steps: Vec<RgbTestStep>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbTestStep {
    pub name: String,
    pub passed: bool,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecoveryReport {
    pub scanned: usize,
    pub recovered: usize,
    pub failed: usize,
    pub actions: Vec<RecoveryAction>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PendingOperation {
    pub operation_id: OperationId,
    pub asset_id: Option<AssetId>,
    pub amount: Option<u64>,
    pub status: OperationStatus,
    pub layer: Option<AssetLayer>,
    pub related_txid: Option<Txid>,
    pub related_l2_ref: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Prepared,
    Reserved,
    Pending,
    Committed,
    Settled,
    Cancelled,
    Failed,
    RecoveryRequired,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RecoveryAction {
    pub operation_id: OperationId,
    pub action: String,
    pub message: String,
}

macro_rules! account_scoped {
    ($($ty:ty),* $(,)?) => {
        $(
            impl AccountScoped for $ty {
                fn account_id(&self) -> &str {
                    &self.account_id
                }
            }
        )*
    };
}

account_scoped!(
    RegisterIrohNodeRequest,
    LookupIrohNodeRequest,
    RnaBalanceRequest,
    IssueAssetRequest,
    ListAssetsRequest,
    BalanceRequest,
    BalanceBreakdownRequest,
    CreateInvoiceRequest,
    PrepareTransferRequest,
    CommitTransferRequest,
    SendConsignmentRequest,
    ReceiveConsignmentRequest,
    CancelTransferRequest,
    ListPendingRequest,
    RecoverRequest,
    RunRgbTestRequest,
);

impl AssetSpendAuthorized for PrepareTransferRequest {
    fn asset_spend_authorization(&self) -> Option<&AssetSpendAuthorization> {
        Some(&self.asset_authorization)
    }
}

impl AssetSpendAuthorized for CommitTransferRequest {
    fn asset_spend_authorization(&self) -> Option<&AssetSpendAuthorization> {
        Some(&self.asset_authorization)
    }
}

impl AssetSpendAuthorized for SendConsignmentRequest {
    fn asset_spend_authorization(&self) -> Option<&AssetSpendAuthorization> {
        Some(&self.asset_authorization)
    }
}
