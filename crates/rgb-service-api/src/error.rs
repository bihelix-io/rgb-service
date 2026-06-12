use thiserror::Error;

#[derive(Debug, Error)]
pub enum RgbServiceError {
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("signature required: {0}")]
    SignatureRequired(String),

    #[error("asset spend authorization required: {0}")]
    AssetSpendAuthorizationRequired(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("backend error: {0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, RgbServiceError>;
