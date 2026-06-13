use async_trait::async_trait;

use crate::{auth::Authorized, dto::*, error::Result};

#[async_trait]
pub trait RgbServiceApi: Send + Sync + 'static {
    async fn rna_balance(&self, req: Authorized<RnaBalanceRequest>) -> Result<RnaBalanceResponse>;

    async fn issue_asset(&self, req: Authorized<IssueAssetRequest>) -> Result<IssueAssetResponse>;

    async fn list_assets(&self, req: Authorized<ListAssetsRequest>) -> Result<ListAssetsResponse>;

    async fn token_list(&self) -> Result<TokenListResponse>;

    async fn balance(&self, req: Authorized<BalanceRequest>) -> Result<RgbBalance>;

    async fn balance_breakdown(
        &self,
        req: Authorized<BalanceBreakdownRequest>,
    ) -> Result<BalanceBreakdownResponse>;

    async fn prepare_transfer(
        &self,
        req: Authorized<PrepareTransferRequest>,
    ) -> Result<PrepareTransferResponse>;

    async fn commit_transfer(
        &self,
        req: Authorized<CommitTransferRequest>,
    ) -> Result<CommitTransferResponse>;

    async fn cancel_transfer(
        &self,
        req: Authorized<CancelTransferRequest>,
    ) -> Result<CancelTransferResponse>;

    async fn list_pending(
        &self,
        req: Authorized<ListPendingRequest>,
    ) -> Result<ListPendingResponse>;

    async fn recover(&self, req: Authorized<RecoverRequest>) -> Result<RecoveryReport>;

    async fn prepare_ln_channel_open(
        &self,
        req: Authorized<LnChannelOpenPrepareRequest>,
    ) -> Result<LnChannelOpenPrepareResponse>;

    async fn compose_ln_commitment(
        &self,
        req: Authorized<LnCommitmentComposeRequest>,
    ) -> Result<LnComposeResponse>;

    async fn compose_ln_closing(
        &self,
        req: Authorized<LnClosingComposeRequest>,
    ) -> Result<LnComposeResponse>;

    async fn compose_ln_onchain_claim(
        &self,
        req: Authorized<LnOnchainClaimComposeRequest>,
    ) -> Result<LnComposeResponse>;

    async fn recover_ln(&self, req: Authorized<LnRecoverRequest>) -> Result<LnRecoveryReport>;

    async fn run_rgb_test(&self, req: Authorized<RunRgbTestRequest>) -> Result<RunRgbTestResponse>;
}
