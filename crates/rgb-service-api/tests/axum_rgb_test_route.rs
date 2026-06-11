#![cfg(feature = "axum")]

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use rgb_service_api::{
    AccountId, AssetSpendAuthorization, AuthSubject, AuthVerifier, Authorized,
    BalanceBreakdownRequest, BalanceBreakdownResponse, BalanceRequest, CancelTransferRequest,
    CancelTransferResponse, CommitTransferRequest, CommitTransferResponse, CreateInvoiceRequest,
    CreateInvoiceResponse, IssueAssetRequest, IssueAssetResponse, ListAssetsRequest,
    ListAssetsResponse, ListPendingRequest, ListPendingResponse, Permission, PrepareTransferRequest,
    PrepareTransferResponse, RecoverRequest, RecoveryReport, RequestSignature, RgbBalance,
    RgbServiceApi, RgbServiceError, RgbTestScenario, RgbTestStep, RunRgbTestRequest,
    RunRgbTestResponse, SignatureScheme, SignedRequest, axum_service::router,
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

    async fn create_invoice(
        &self,
        _req: Authorized<CreateInvoiceRequest>,
    ) -> rgb_service_api::Result<CreateInvoiceResponse> {
        Err(unimplemented_call("create_invoice"))
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

    async fn run_rgb_test(
        &self,
        req: Authorized<RunRgbTestRequest>,
    ) -> rgb_service_api::Result<RunRgbTestResponse> {
        Ok(RunRgbTestResponse {
            scenario: req.payload.scenario,
            passed: true,
            steps: vec![
                step("issue_rgb20"),
                step("create_invoice"),
                step("prepare_transfer"),
                step("commit_transfer"),
                step("recover_pending"),
            ],
        })
    }
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
