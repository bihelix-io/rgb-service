#![cfg(feature = "axum")]

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use rgb_service_api::{
    axum_service::router, AccountId, AllocationStatus, AssetLayer, AssetSpendAuthorization,
    AuthSubject, AuthVerifier, Authorized, BalanceBreakdownRequest, BalanceBreakdownResponse,
    BalanceRequest, CancelTransferRequest, CancelTransferResponse, CommitTransferRequest,
    CommitTransferResponse, IssueAssetRequest, IssueAssetResponse, ListAssetsRequest,
    ListAssetsResponse, ListPendingRequest, ListPendingResponse, LnChannelFundingRefRequest,
    LnChannelFundingRefResponse, LnChannelOpenPrepareRequest, LnChannelOpenPrepareResponse,
    LnClosingComposeRequest, LnCommitmentComposeRequest, LnComposeResponse,
    LnOnchainClaimComposeRequest, LnPaymentClaimRequest, LnPaymentClaimResponse, LnRecoverRequest,
    LnRecoveryReport, Permission, PrepareTransferRequest, PrepareTransferResponse, RecoverRequest,
    RecoveryReport, RequestSignature, RgbAllocation, RgbAssetInfo, RgbBalance, RgbServiceApi,
    RgbServiceError, RgbTestScenario, RgbTestStep, RnaBalanceRequest, RnaBalanceResponse,
    RunRgbTestRequest, RunRgbTestResponse, SignatureScheme, SignedRequest, TokenListResponse,
    UtxoAssetsRequest, UtxoAssetsResponse,
};
use tower::ServiceExt;

struct AllowAllAuth;

#[async_trait]
impl AuthVerifier for AllowAllAuth {
    async fn verify_request(
        &self,
        permission: Permission,
        account_id: &str,
        _payload: &[u8],
        signature: &RequestSignature,
    ) -> rgb_service_api::Result<AuthSubject> {
        Ok(AuthSubject {
            account_id: account_id.to_string(),
            signer_id: signature.signer_id.clone(),
            permissions: vec![permission],
        })
    }

    async fn verify_asset_spend(
        &self,
        _account_id: &str,
        _authorization: &AssetSpendAuthorization,
    ) -> rgb_service_api::Result<()> {
        Ok(())
    }
}

struct TestRgbService;

#[async_trait]
impl RgbServiceApi for TestRgbService {
    async fn rna_balance(
        &self,
        _req: Authorized<RnaBalanceRequest>,
    ) -> rgb_service_api::Result<RnaBalanceResponse> {
        Err(unimplemented_call("rna_balance"))
    }

    async fn issue_asset(
        &self,
        _req: Authorized<IssueAssetRequest>,
    ) -> rgb_service_api::Result<IssueAssetResponse> {
        Err(unimplemented_call("issue_asset"))
    }

    async fn list_assets(
        &self,
        _req: Authorized<ListAssetsRequest>,
    ) -> rgb_service_api::Result<ListAssetsResponse> {
        Err(unimplemented_call("list_assets"))
    }

    async fn assets_by_utxo(
        &self,
        req: UtxoAssetsRequest,
    ) -> rgb_service_api::Result<UtxoAssetsResponse> {
        let outpoint = req.outpoint;
        Ok(UtxoAssetsResponse {
            account_id: req.account_id,
            outpoint: outpoint.clone(),
            assets: vec![RgbAssetInfo {
                asset_id: "rgb:asset-1".to_string(),
                contract_id: "rgb:asset-1".to_string(),
                ticker: "TEST".to_string(),
                name: "Test Asset".to_string(),
                precision: 8,
                supply: None,
                issue_utxo: String::new(),
                contract_type: "Rgb20".to_string(),
                issuer_desc: String::new(),
                created_at: String::new(),
                ext: None,
            }],
            allocations: vec![RgbAllocation {
                asset_id: "rgb:asset-1".to_string(),
                outpoint,
                amount: 100_000_000,
                layer: AssetLayer::L1,
                status: AllocationStatus::Available,
                address: Some("bc1qtest".to_string()),
                confirmed: Some(true),
            }],
        })
    }

    async fn token_list(&self) -> rgb_service_api::Result<TokenListResponse> {
        Ok(TokenListResponse {
            contracts: Vec::new(),
            assets: Vec::new(),
        })
    }

    async fn balance(
        &self,
        _req: Authorized<BalanceRequest>,
    ) -> rgb_service_api::Result<RgbBalance> {
        Err(unimplemented_call("balance"))
    }

    async fn balance_breakdown(
        &self,
        _req: Authorized<BalanceBreakdownRequest>,
    ) -> rgb_service_api::Result<BalanceBreakdownResponse> {
        Err(unimplemented_call("balance_breakdown"))
    }

    async fn prepare_transfer(
        &self,
        _req: Authorized<PrepareTransferRequest>,
    ) -> rgb_service_api::Result<PrepareTransferResponse> {
        Err(unimplemented_call("prepare_transfer"))
    }

    async fn commit_transfer(
        &self,
        _req: Authorized<CommitTransferRequest>,
    ) -> rgb_service_api::Result<CommitTransferResponse> {
        Err(unimplemented_call("commit_transfer"))
    }

    async fn cancel_transfer(
        &self,
        _req: Authorized<CancelTransferRequest>,
    ) -> rgb_service_api::Result<CancelTransferResponse> {
        Err(unimplemented_call("cancel_transfer"))
    }

    async fn list_pending(
        &self,
        _req: Authorized<ListPendingRequest>,
    ) -> rgb_service_api::Result<ListPendingResponse> {
        Err(unimplemented_call("list_pending"))
    }

    async fn recover(
        &self,
        _req: Authorized<RecoverRequest>,
    ) -> rgb_service_api::Result<RecoveryReport> {
        Err(unimplemented_call("recover"))
    }

    async fn prepare_ln_channel_open(
        &self,
        _req: Authorized<LnChannelOpenPrepareRequest>,
    ) -> rgb_service_api::Result<LnChannelOpenPrepareResponse> {
        Err(unimplemented_call("prepare_ln_channel_open"))
    }

    async fn ln_channel_funding_ref(
        &self,
        _req: Authorized<LnChannelFundingRefRequest>,
    ) -> rgb_service_api::Result<LnChannelFundingRefResponse> {
        Err(unimplemented_call("ln_channel_funding_ref"))
    }

    async fn claim_ln_payment(
        &self,
        _req: Authorized<LnPaymentClaimRequest>,
    ) -> rgb_service_api::Result<LnPaymentClaimResponse> {
        Err(unimplemented_call("claim_ln_payment"))
    }

    async fn compose_ln_commitment(
        &self,
        _req: Authorized<LnCommitmentComposeRequest>,
    ) -> rgb_service_api::Result<LnComposeResponse> {
        Err(unimplemented_call("compose_ln_commitment"))
    }

    async fn compose_ln_closing(
        &self,
        _req: Authorized<LnClosingComposeRequest>,
    ) -> rgb_service_api::Result<LnComposeResponse> {
        Err(unimplemented_call("compose_ln_closing"))
    }

    async fn compose_ln_onchain_claim(
        &self,
        _req: Authorized<LnOnchainClaimComposeRequest>,
    ) -> rgb_service_api::Result<LnComposeResponse> {
        Err(unimplemented_call("compose_ln_onchain_claim"))
    }

    async fn recover_ln(
        &self,
        _req: Authorized<LnRecoverRequest>,
    ) -> rgb_service_api::Result<LnRecoveryReport> {
        Err(unimplemented_call("recover_ln"))
    }

    async fn run_rgb_test(
        &self,
        req: Authorized<RunRgbTestRequest>,
    ) -> rgb_service_api::Result<RunRgbTestResponse> {
        Ok(RunRgbTestResponse {
            scenario: req.payload.scenario,
            passed: true,
            steps: vec![
                step("issue_rgb20"),
                step("prepare_transfer"),
                step("commit_transfer"),
                step("direct_delivery"),
                step("recover_pending"),
            ],
        })
    }
}

#[tokio::test]
async fn token_list_route_is_public() {
    let app = router(Arc::new(TestRgbService), Arc::new(AllowAllAuth));
    let req = Request::builder()
        .method("GET")
        .uri("/v1/tokens/list")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let response: TokenListResponse = serde_json::from_slice(&body).unwrap();
    assert!(response.contracts.is_empty());
    assert!(response.assets.is_empty());
}

#[tokio::test]
async fn assets_by_utxo_route_returns_allocations_for_outpoint() {
    let app = router(Arc::new(TestRgbService), Arc::new(AllowAllAuth));
    let request = UtxoAssetsRequest {
        account_id: account("alice"),
        outpoint: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef:1".to_string(),
        address: Some("bc1qexample".to_string()),
        confirmed: true,
    };
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assets/by-utxo")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&request).unwrap()))
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let response: UtxoAssetsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(response.account_id, account("alice"));
    assert_eq!(response.assets.len(), 1);
    assert_eq!(response.assets[0].contract_id, "rgb:asset-1");
    assert_eq!(response.allocations.len(), 1);
    assert_eq!(response.allocations[0].amount, 100_000_000);
    assert_eq!(response.allocations[0].status, AllocationStatus::Available);
}

#[tokio::test]
async fn rgb_test_route_returns_full_lifecycle_report() {
    let app = router(Arc::new(TestRgbService), Arc::new(AllowAllAuth));
    let signed = SignedRequest {
        payload: RunRgbTestRequest {
            account_id: account("alice"),
            scenario: RgbTestScenario::FullRgb20Lifecycle,
        },
        signature: test_signature(),
    };
    let req = Request::builder()
        .method("POST")
        .uri("/v1/test/rgb")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&signed).unwrap()))
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let report: RunRgbTestResponse = serde_json::from_slice(&body).unwrap();
    assert!(report.passed);
    assert_eq!(report.scenario, RgbTestScenario::FullRgb20Lifecycle);
    assert_eq!(report.steps.len(), 5);
}

fn account(value: &str) -> AccountId {
    value.to_string()
}

fn test_signature() -> RequestSignature {
    RequestSignature {
        signer_id: "test-signer".to_string(),
        public_key: "test-public-key".to_string(),
        scheme: SignatureScheme::Ed25519,
        nonce: "nonce-1".to_string(),
        timestamp_ms: 1,
        signature: "test-signature".to_string(),
    }
}

fn step(name: &str) -> RgbTestStep {
    RgbTestStep {
        name: name.to_string(),
        passed: true,
        message: None,
    }
}

fn unimplemented_call(name: &str) -> RgbServiceError {
    RgbServiceError::Backend(format!("{name} is not implemented in the test service"))
}
