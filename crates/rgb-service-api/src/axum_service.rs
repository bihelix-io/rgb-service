use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde::Serialize;

use crate::{
    auth::{
        AccountScoped, AssetSpendAuthorized, AuthVerifier, Authorized, Permission,
        SignedRequest,
    },
    dto::*,
    error::RgbServiceError,
    service::RgbServiceApi,
};

#[derive(Clone)]
pub struct ApiState {
    pub service: Arc<dyn RgbServiceApi>,
    pub auth: Arc<dyn AuthVerifier>,
}

pub fn router(service: Arc<dyn RgbServiceApi>, auth: Arc<dyn AuthVerifier>) -> Router {
    let state = ApiState { service, auth };
    Router::new()
        .route("/v1/iroh-nodes/register", post(register_iroh_node))
        .route("/v1/iroh-nodes/lookup", post(lookup_iroh_node))
        .route("/v1/rna/balance", post(rna_balance))
        .route("/v1/assets/issue", post(issue_asset))
        .route("/v1/assets/list", post(list_assets))
        .route("/v1/balance", post(balance))
        .route("/v1/balance/breakdown", post(balance_breakdown))
        .route("/v1/invoices/create", post(create_invoice))
        .route("/v1/transfers/prepare", post(prepare_transfer))
        .route("/v1/transfers/commit", post(commit_transfer))
        .route("/v1/consignments/send", post(send_consignment))
        .route("/v1/consignments/receive", post(receive_consignment))
        .route("/v1/transfers/cancel", post(cancel_transfer))
        .route("/v1/pending/list", post(list_pending))
        .route("/v1/recover", post(recover))
        .route("/v1/test/rgb", post(run_rgb_test))
        .with_state(state)
}

async fn authorize<T>(
    state: &ApiState,
    permission: Permission,
    signed: SignedRequest<T>,
) -> Result<Authorized<T>, HttpError>
where
    T: AccountScoped + Serialize,
{
    let payload = serde_json::to_vec(&signed.payload)
        .map_err(|err| RgbServiceError::InvalidRequest(err.to_string()))?;
    let subject = state
        .auth
        .verify_request(
            permission,
            signed.payload.account_id(),
            &payload,
            &signed.signature,
        )
        .await?;
    Ok(Authorized {
        subject,
        payload: signed.payload,
    })
}

async fn authorize_asset_spend<T>(
    state: &ApiState,
    permission: Permission,
    signed: SignedRequest<T>,
) -> Result<Authorized<T>, HttpError>
where
    T: AccountScoped + AssetSpendAuthorized + Serialize,
{
    let account_id = signed.payload.account_id().to_string();
    let authorization = signed
        .payload
        .asset_spend_authorization()
        .ok_or_else(|| {
            RgbServiceError::AssetSpendAuthorizationRequired(
                "missing asset spend authorization".to_string(),
            )
        })?
        .clone();
    let authorized = authorize(state, permission, signed).await?;
    state
        .auth
        .verify_asset_spend(&account_id, &authorization)
        .await?;
    Ok(authorized)
}

async fn register_iroh_node(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<RegisterIrohNodeRequest>>,
) -> Result<Json<RegisterIrohNodeResponse>, HttpError> {
    let req = authorize(&state, Permission::RegisterIrohNode, req).await?;
    Ok(Json(state.service.register_iroh_node(req).await?))
}

async fn lookup_iroh_node(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<LookupIrohNodeRequest>>,
) -> Result<Json<LookupIrohNodeResponse>, HttpError> {
    let req = authorize(&state, Permission::LookupIrohNode, req).await?;
    Ok(Json(state.service.lookup_iroh_node(req).await?))
}

async fn rna_balance(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<RnaBalanceRequest>>,
) -> Result<Json<RnaBalanceResponse>, HttpError> {
    let req = authorize(&state, Permission::ReadRnaBalance, req).await?;
    Ok(Json(state.service.rna_balance(req).await?))
}

async fn issue_asset(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<IssueAssetRequest>>,
) -> Result<Json<IssueAssetResponse>, HttpError> {
    let req = authorize(&state, Permission::IssueAsset, req).await?;
    Ok(Json(state.service.issue_asset(req).await?))
}

async fn list_assets(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<ListAssetsRequest>>,
) -> Result<Json<ListAssetsResponse>, HttpError> {
    let req = authorize(&state, Permission::ReadAssets, req).await?;
    Ok(Json(state.service.list_assets(req).await?))
}

async fn balance(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<BalanceRequest>>,
) -> Result<Json<RgbBalance>, HttpError> {
    let req = authorize(&state, Permission::ReadAssets, req).await?;
    Ok(Json(state.service.balance(req).await?))
}

async fn balance_breakdown(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<BalanceBreakdownRequest>>,
) -> Result<Json<BalanceBreakdownResponse>, HttpError> {
    let req = authorize(&state, Permission::ReadAssets, req).await?;
    Ok(Json(state.service.balance_breakdown(req).await?))
}

async fn create_invoice(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<CreateInvoiceRequest>>,
) -> Result<Json<CreateInvoiceResponse>, HttpError> {
    let req = authorize(&state, Permission::CreateInvoice, req).await?;
    Ok(Json(state.service.create_invoice(req).await?))
}

async fn prepare_transfer(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<PrepareTransferRequest>>,
) -> Result<Json<PrepareTransferResponse>, HttpError> {
    let req = authorize_asset_spend(&state, Permission::PrepareTransfer, req).await?;
    Ok(Json(state.service.prepare_transfer(req).await?))
}

async fn commit_transfer(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<CommitTransferRequest>>,
) -> Result<Json<CommitTransferResponse>, HttpError> {
    let req = authorize_asset_spend(&state, Permission::CommitTransfer, req).await?;
    Ok(Json(state.service.commit_transfer(req).await?))
}

async fn send_consignment(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<SendConsignmentRequest>>,
) -> Result<Json<SendConsignmentResponse>, HttpError> {
    let req = authorize_asset_spend(&state, Permission::SendConsignment, req).await?;
    Ok(Json(state.service.send_consignment(req).await?))
}

async fn receive_consignment(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<ReceiveConsignmentRequest>>,
) -> Result<Json<ReceiveConsignmentResponse>, HttpError> {
    let req = authorize(&state, Permission::ReceiveConsignment, req).await?;
    Ok(Json(state.service.receive_consignment(req).await?))
}

async fn cancel_transfer(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<CancelTransferRequest>>,
) -> Result<Json<CancelTransferResponse>, HttpError> {
    let req = authorize(&state, Permission::CancelTransfer, req).await?;
    Ok(Json(state.service.cancel_transfer(req).await?))
}

async fn list_pending(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<ListPendingRequest>>,
) -> Result<Json<ListPendingResponse>, HttpError> {
    let req = authorize(&state, Permission::ManagePending, req).await?;
    Ok(Json(state.service.list_pending(req).await?))
}

async fn recover(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<RecoverRequest>>,
) -> Result<Json<RecoveryReport>, HttpError> {
    let req = authorize(&state, Permission::Recover, req).await?;
    Ok(Json(state.service.recover(req).await?))
}

async fn run_rgb_test(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<RunRgbTestRequest>>,
) -> Result<Json<RunRgbTestResponse>, HttpError> {
    let req = authorize(&state, Permission::RunTest, req).await?;
    Ok(Json(state.service.run_rgb_test(req).await?))
}

pub struct HttpError(RgbServiceError);

impl From<RgbServiceError> for HttpError {
    fn from(err: RgbServiceError) -> Self {
        Self(err)
    }
}

impl IntoResponse for HttpError {
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
            RgbServiceError::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = ErrorBody {
            error: self.0.to_string(),
        };
        (status, Json(body)).into_response()
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}
