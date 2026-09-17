//! Authenticated L1 RGB operations. Amounts are raw integer units.
use crate::{AccountScoped, AssetSpendAuthorization, AssetSpendAuthorized};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalRgbRequest {
    pub account_id: String,
    pub action: ExternalRgbAction,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalRgbAction {
    Receive {
        request_id: String,
        asset_id: String,
        amount: u64,
        expires_at: u64,
        /// None creates a witness invoice to the account address.
        blind_outpoint: Option<String>,
    },
    Prepare {
        request_id: String,
        invoice: String,
        unsigned_anchor_psbt: String,
        recipient_vout: Option<u32>,
        change_vout: u32,
        asset_authorization: AssetSpendAuthorization,
    },
    Finalize {
        request_id: String,
        operation_id: String,
        signed_anchor_psbt: String,
        asset_authorization: AssetSpendAuthorization,
    },
    Get {
        operation_id: String,
    },
    List {
        after: Option<String>,
        limit: u16,
    },
    Refresh {
        operation_id: String,
    },
    Cancel {
        request_id: String,
        operation_id: String,
    },
}
impl ExternalRgbAction {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Receive { .. } => "receive",
            Self::Prepare { .. } => "prepare",
            Self::Finalize { .. } => "finalize",
            Self::Get { .. } => "get",
            Self::List { .. } => "list",
            Self::Refresh { .. } => "refresh",
            Self::Cancel { .. } => "cancel",
        }
    }
}
impl AccountScoped for ExternalRgbRequest {
    fn account_id(&self) -> &str {
        &self.account_id
    }
}
impl AssetSpendAuthorized for ExternalRgbRequest {
    fn asset_spend_authorization(&self) -> Option<&AssetSpendAuthorization> {
        match &self.action {
            ExternalRgbAction::Prepare {
                asset_authorization,
                ..
            }
            | ExternalRgbAction::Finalize {
                asset_authorization,
                ..
            } => Some(asset_authorization),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExternalRgbOperation {
    pub operation_id: String,
    pub direction: String,
    pub status: String,
    pub asset_id: String,
    pub amount: u64,
    pub invoice: String,
    pub recipient_id: String,
    pub txid: Option<String>,
    pub anchor_psbt: Option<String>,
    pub delivery_status: String,
    pub broadcast_status: String,
    pub acknowledged: Option<bool>,
    pub confirmations: u32,
    pub required_confirmations: u32,
    pub retryable: bool,
    pub last_error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExternalRgbResponse {
    pub operations: Vec<ExternalRgbOperation>,
    pub next_cursor: Option<String>,
}
