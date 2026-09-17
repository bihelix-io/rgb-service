use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::{
        AccountScoped, AssetSpendAuthorized, AuthVerifier, Authorized, Permission, SignedRequest,
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
        .route("/v1/external/receives/create", post(external_receive))
        .route("/v1/external/transfers/prepare", post(external_prepare))
        .route("/v1/external/transfers/finalize", post(external_finalize))
        .route("/v1/external/operations/get", post(external_get))
        .route("/v1/external/operations/list", post(external_list))
        .route("/v1/external/operations/refresh", post(external_refresh))
        .route("/v1/external/operations/cancel", post(external_cancel))
        .route("/v1/rna/balance", post(rna_balance))
        .route("/v1/daemon-rna/balance", post(rna_balance))
        .route("/v1/assets/issue", post(issue_asset))
        .route("/v1/assets/list", post(list_assets))
        .route("/v1/assets/by-utxo", post(assets_by_utxo))
        .route("/v1/tokens/list", get(token_list))
        .route("/v1/balance", post(balance))
        .route("/v1/balance/breakdown", post(balance_breakdown))
        .route("/v1/transfers/prepare", post(prepare_transfer))
        .route("/v1/transfers/commit", post(commit_transfer))
        .route(
            "/v1/ln/channels/open/prepare",
            post(prepare_ln_channel_open),
        )
        .route("/v1/ln/channels/funding-ref", post(ln_channel_funding_ref))
        .route("/v1/ln/commitments/compose", post(compose_ln_commitment))
        .route("/v1/ln/closing/compose", post(compose_ln_closing))
        .route(
            "/v1/ln/onchain-claims/compose",
            post(compose_ln_onchain_claim),
        )
        .route("/v1/ln/payments/claim", post(claim_ln_payment))
        .route("/v1/ln/recover", post(recover_ln))
        .route("/v1/transfers/cancel", post(cancel_transfer))
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

async fn assets_by_utxo(
    State(state): State<ApiState>,
    Json(req): Json<UtxoAssetsRequestBody>,
) -> Result<Json<UtxoAssetsResponseBody>, HttpError> {
    match req {
        UtxoAssetsRequestBody::Single(req) => Ok(Json(UtxoAssetsResponseBody::Single(
            state.service.assets_by_utxo(req).await?,
        ))),
        UtxoAssetsRequestBody::Batch(requests) => {
            let mut responses = Vec::with_capacity(requests.len());
            for req in requests {
                responses.push(state.service.assets_by_utxo(req).await?);
            }
            Ok(Json(UtxoAssetsResponseBody::Batch(responses)))
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum UtxoAssetsRequestBody {
    Single(UtxoAssetsRequest),
    Batch(Vec<UtxoAssetsRequest>),
}

#[derive(Serialize)]
#[serde(untagged)]
enum UtxoAssetsResponseBody {
    Single(UtxoAssetsResponse),
    Batch(Vec<UtxoAssetsResponse>),
}

async fn token_list(State(state): State<ApiState>) -> Result<Json<TokenListResponse>, HttpError> {
    Ok(Json(state.service.token_list().await?))
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

async fn prepare_ln_channel_open(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<LnChannelOpenPrepareRequest>>,
) -> Result<Json<LnChannelOpenPrepareResponse>, HttpError> {
    let req = authorize_asset_spend(&state, Permission::LnChannelOpenPrepare, req).await?;
    Ok(Json(state.service.prepare_ln_channel_open(req).await?))
}

async fn ln_channel_funding_ref(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<LnChannelFundingRefRequest>>,
) -> Result<Json<LnChannelFundingRefResponse>, HttpError> {
    let req = authorize(&state, Permission::LnChannelFundingRef, req).await?;
    Ok(Json(state.service.ln_channel_funding_ref(req).await?))
}

async fn compose_ln_commitment(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<LnCommitmentComposeRequest>>,
) -> Result<Json<LnComposeResponse>, HttpError> {
    let req = authorize_asset_spend(&state, Permission::LnCommitmentCompose, req).await?;
    Ok(Json(state.service.compose_ln_commitment(req).await?))
}

async fn compose_ln_closing(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<LnClosingComposeRequest>>,
) -> Result<Json<LnComposeResponse>, HttpError> {
    let req = authorize_asset_spend(&state, Permission::LnClosingCompose, req).await?;
    Ok(Json(state.service.compose_ln_closing(req).await?))
}

async fn compose_ln_onchain_claim(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<LnOnchainClaimComposeRequest>>,
) -> Result<Json<LnComposeResponse>, HttpError> {
    let req = authorize(&state, Permission::LnOnchainClaimCompose, req).await?;
    Ok(Json(state.service.compose_ln_onchain_claim(req).await?))
}

async fn claim_ln_payment(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<LnPaymentClaimRequest>>,
) -> Result<Json<LnPaymentClaimResponse>, HttpError> {
    let req = authorize(&state, Permission::LnPaymentClaim, req).await?;
    Ok(Json(state.service.claim_ln_payment(req).await?))
}

async fn recover_ln(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<LnRecoverRequest>>,
) -> Result<Json<LnRecoveryReport>, HttpError> {
    let req = authorize(&state, Permission::LnRecover, req).await?;
    Ok(Json(state.service.recover_ln(req).await?))
}

async fn cancel_transfer(
    State(state): State<ApiState>,
    Json(req): Json<SignedRequest<CancelTransferRequest>>,
) -> Result<Json<CancelTransferResponse>, HttpError> {
    let req = authorize(&state, Permission::CancelTransfer, req).await?;
    Ok(Json(state.service.cancel_transfer(req).await?))
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
            RgbServiceError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
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

async fn external_dispatch(
    state: ApiState,
    signed: SignedRequest<crate::ExternalRgbRequest>,
    kind: &str,
) -> Result<Json<crate::ExternalRgbResponse>, HttpError> {
    if signed.payload.action.kind() != kind {
        return Err(
            RgbServiceError::InvalidRequest("action does not match endpoint".into()).into(),
        );
    }
    state
        .auth
        .verify_account_key(&signed.payload.account_id, &signed.signature.public_key)
        .await?;
    let req = if let Some(spend) = signed.payload.asset_spend_authorization() {
        state
            .auth
            .verify_account_key(&signed.payload.account_id, &spend.signature.public_key)
            .await?;
        authorize_asset_spend(&state, Permission::ExternalRgb, signed).await?
    } else {
        authorize(&state, Permission::ExternalRgb, signed).await?
    };
    Ok(Json(state.service.external_rgb(req).await?))
}
macro_rules! external_handler {
    ($name:ident, $kind:literal) => {
        async fn $name(
            State(state): State<ApiState>,
            Json(req): Json<SignedRequest<crate::ExternalRgbRequest>>,
        ) -> Result<Json<crate::ExternalRgbResponse>, HttpError> {
            external_dispatch(state, req, $kind).await
        }
    };
}
external_handler!(external_receive, "receive");
external_handler!(external_prepare, "prepare");
external_handler!(external_finalize, "finalize");
external_handler!(external_get, "get");
external_handler!(external_list, "list");
external_handler!(external_refresh, "refresh");
external_handler!(external_cancel, "cancel");
