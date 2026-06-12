use async_trait::async_trait;

use crate::{
    auth::Authorized,
    dto::*,
    error::Result,
};

#[async_trait]
pub trait RgbServiceApi: Send + Sync + 'static {
    async fn register_iroh_node(
        &self,
        req: Authorized<RegisterIrohNodeRequest>,
    ) -> Result<RegisterIrohNodeResponse>;

    async fn lookup_iroh_node(
        &self,
        req: Authorized<LookupIrohNodeRequest>,
    ) -> Result<LookupIrohNodeResponse>;

    async fn rna_balance(
        &self,
        req: Authorized<RnaBalanceRequest>,
    ) -> Result<RnaBalanceResponse>;

    async fn issue_asset(
        &self,
        req: Authorized<IssueAssetRequest>,
    ) -> Result<IssueAssetResponse>;

    async fn list_assets(
        &self,
        req: Authorized<ListAssetsRequest>,
    ) -> Result<ListAssetsResponse>;

    async fn balance(&self, req: Authorized<BalanceRequest>) -> Result<RgbBalance>;

    async fn balance_breakdown(
        &self,
        req: Authorized<BalanceBreakdownRequest>,
    ) -> Result<BalanceBreakdownResponse>;

    async fn create_invoice(
        &self,
        req: Authorized<CreateInvoiceRequest>,
    ) -> Result<CreateInvoiceResponse>;

    async fn prepare_transfer(
        &self,
        req: Authorized<PrepareTransferRequest>,
    ) -> Result<PrepareTransferResponse>;

    async fn commit_transfer(
        &self,
        req: Authorized<CommitTransferRequest>,
    ) -> Result<CommitTransferResponse>;

    async fn send_consignment(
        &self,
        req: Authorized<SendConsignmentRequest>,
    ) -> Result<SendConsignmentResponse>;

    async fn receive_consignment(
        &self,
        req: Authorized<ReceiveConsignmentRequest>,
    ) -> Result<ReceiveConsignmentResponse>;

    async fn cancel_transfer(
        &self,
        req: Authorized<CancelTransferRequest>,
    ) -> Result<CancelTransferResponse>;

    async fn list_pending(
        &self,
        req: Authorized<ListPendingRequest>,
    ) -> Result<ListPendingResponse>;

    async fn recover(&self, req: Authorized<RecoverRequest>) -> Result<RecoveryReport>;

    async fn run_rgb_test(
        &self,
        req: Authorized<RunRgbTestRequest>,
    ) -> Result<RunRgbTestResponse>;
}
