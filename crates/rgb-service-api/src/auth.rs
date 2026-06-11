use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    ReadAssets,
    IssueAsset,
    ImportContract,
    CreateInvoice,
    PrepareTransfer,
    CommitTransfer,
    CancelTransfer,
    ValidateConsignment,
    ImportConsignment,
    ManagePending,
    Recover,
    RunTest,
    L2Reserve,
    L2Settle,
    Admin,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RequestSignature {
    pub signer_id: String,
    pub public_key: String,
    pub scheme: SignatureScheme,
    pub nonce: String,
    pub timestamp_ms: u64,
    pub signature: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignatureScheme {
    Bip322,
    Schnorr,
    Ecdsa,
    Ed25519,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignedRequest<T> {
    pub payload: T,
    pub signature: RequestSignature,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Authorized<T> {
    pub subject: AuthSubject,
    pub payload: T,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuthSubject {
    pub account_id: String,
    pub signer_id: String,
    pub permissions: Vec<Permission>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AssetSpendAuthorization {
    pub asset_id: String,
    pub amount: u64,
    pub purpose: AssetSpendPurpose,
    pub recipient: Option<String>,
    pub anchor_psbt: Option<String>,
    pub expires_at_ms: u64,
    pub signature: RequestSignature,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetSpendPurpose {
    L1Transfer,
    L2Reserve,
    L2Settle,
    ChannelDeposit,
    ChannelWithdraw,
}

pub trait AccountScoped {
    fn account_id(&self) -> &str;
}

pub trait AssetSpendAuthorized {
    fn asset_spend_authorization(&self) -> Option<&AssetSpendAuthorization>;
}

#[async_trait]
pub trait AuthVerifier: Send + Sync + 'static {
    async fn verify_request(
        &self,
        permission: Permission,
        account_id: &str,
        payload: &[u8],
        signature: &RequestSignature,
    ) -> Result<AuthSubject>;

    async fn verify_asset_spend(
        &self,
        account_id: &str,
        authorization: &AssetSpendAuthorization,
    ) -> Result<()>;
}
