// This file is Copyright its original authors, visible in version control
// history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE
// or http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your option.

//! RGB extension types and helpers for the local `ln-rgb` fork.
//!
//! This module carries the RGB protocol boundary used by the wallet layer and
//! the Lightning state machine. The types mirror the reference `bihelix-node`
//! integration but keep the public channel/payment entry points narrow enough
//! for incremental porting onto the newer LDK base.
#![allow(missing_docs)]

use alloc::sync::Arc;
use std::collections::BTreeMap;
use std::io::Read as IoRead;
use std::net::TcpStream;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::ln::msgs::DecodeError;
use crate::util::ser::{Readable, Writeable, Writer};

/// RGB contract id tracked by the Lightning state machine.
///
/// The Lightning fork only keeps this identifier for channel accounting. Full RGB
/// contract data, stock, consignments, and validation state live in `rgb-service-daemon`.
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ContractId(pub [u8; 32]);

impl From<[u8; 32]> for ContractId {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl From<ContractId> for [u8; 32] {
    fn from(value: ContractId) -> Self {
        value.0
    }
}

impl core::fmt::Display for ContractId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&hex_encode(&self.0))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RgbClaimableOutpoint {
    pub outpoint: bitcoin::OutPoint,
    pub contract_id: ContractId,
    pub amount_rgb: u64,
    pub purpose: RgbClaimPurpose,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RgbClaimPurpose {
    ToBroadcaster,
    ToCountersignatory,
    Htlc,
}

/// Minimal blocking client for the external `rgb-service-daemon`.
///
/// The Lightning fork must not own RGB private keys or local RGB stock. The client
/// owns the LN node's RGB-service request signer and transparently wraps daemon
/// calls in `SignedRequest`, so callers pass plain request payloads.
#[derive(Clone)]
pub struct RgbServiceClient {
    daemon_url: String,
    signer: Arc<dyn RgbServiceSigner + Send + Sync>,
}

impl RgbServiceClient {
    /// Creates a daemon client. The URL must be explicit; no fallback daemon is assumed.
    pub fn new(
        daemon_url: impl Into<String>,
        signer: Arc<dyn RgbServiceSigner + Send + Sync>,
    ) -> Result<Self, RgbServiceClientError> {
        let daemon_url = daemon_url.into();
        if daemon_url.trim().is_empty() {
            return Err(RgbServiceClientError::InvalidRequest(
                "rgb-service daemon_url must not be empty".to_string(),
            ));
        }
        if !daemon_url.starts_with("http://") {
            return Err(RgbServiceClientError::InvalidRequest(
                "rgb-service daemon_url must start with http://".to_string(),
            ));
        }
        Ok(Self { daemon_url, signer })
    }

    /// Returns the configured daemon URL.
    pub fn daemon_url(&self) -> &str {
        &self.daemon_url
    }

    pub fn rna_balance(
        &self,
        req: RnaBalanceRequest,
    ) -> Result<RnaBalanceResponse, RgbServiceClientError> {
        self.post_signed("/v1/rna/balance", "rna_balance", req)
    }

    pub fn list_assets(
        &self,
        req: ListAssetsRequest,
    ) -> Result<ListAssetsResponse, RgbServiceClientError> {
        self.post_signed("/v1/assets/list", "list_assets", req)
    }

    pub fn token_list(&self) -> Result<TokenListResponse, RgbServiceClientError> {
        self.get_json("/v1/tokens/list")
    }

    pub fn balance(&self, req: BalanceRequest) -> Result<RgbBalance, RgbServiceClientError> {
        self.post_signed("/v1/balance", "balance", req)
    }

    pub fn issue_asset(
        &self,
        req: IssueAssetRequest,
    ) -> Result<IssueAssetResponse, RgbServiceClientError> {
        self.post_signed("/v1/assets/issue", "issue_asset", req)
    }

    pub fn prepare_transfer(
        &self,
        req: PrepareTransferRequest,
    ) -> Result<PrepareTransferResponse, RgbServiceClientError> {
        self.post_signed("/v1/transfers/prepare", "prepare_transfer", req)
    }

    pub fn commit_transfer(
        &self,
        req: CommitTransferRequest,
    ) -> Result<CommitTransferResponse, RgbServiceClientError> {
        self.post_signed("/v1/transfers/commit", "commit_transfer", req)
    }

    pub fn prepare_ln_channel_open(
        &self,
        req: LnChannelOpenPrepareRequest,
    ) -> Result<LnChannelOpenPrepareResponse, RgbServiceClientError> {
        self.post_signed(
            "/v1/ln/channels/open/prepare",
            "ln_channel_open_prepare",
            req,
        )
    }

    pub fn ln_channel_funding_ref(
        &self,
        req: LnChannelFundingRefRequest,
    ) -> Result<LnChannelFundingRefResponse, RgbServiceClientError> {
        self.post_signed("/v1/ln/channels/funding-ref", "ln_channel_funding_ref", req)
    }

    pub fn compose_ln_commitment(
        &self,
        req: LnCommitmentComposeRequest,
    ) -> Result<LnComposeResponse, RgbServiceClientError> {
        self.post_signed("/v1/ln/commitments/compose", "ln_commitment_compose", req)
    }

    pub fn compose_ln_closing(
        &self,
        req: LnClosingComposeRequest,
    ) -> Result<LnComposeResponse, RgbServiceClientError> {
        self.post_signed("/v1/ln/closing/compose", "ln_closing_compose", req)
    }

    pub fn compose_ln_onchain_claim(
        &self,
        req: LnOnchainClaimComposeRequest,
    ) -> Result<LnComposeResponse, RgbServiceClientError> {
        self.post_signed(
            "/v1/ln/onchain-claims/compose",
            "ln_onchain_claim_compose",
            req,
        )
    }

    pub fn claim_ln_payment(
        &self,
        req: LnPaymentClaimRequest,
    ) -> Result<LnPaymentClaimResponse, RgbServiceClientError> {
        self.post_signed("/v1/ln/payments/claim", "ln_payment_claim", req)
    }

    fn signed<T: Serialize>(
        &self,
        purpose: &str,
        payload: T,
    ) -> Result<SignedRequest<T>, RgbServiceClientError> {
        let bytes = serde_json::to_vec(&payload)?;
        let signature = self.signer.sign_rgb_service_payload(purpose, &bytes)?;
        Ok(SignedRequest { payload, signature })
    }

    fn post_signed<T, R>(
        &self,
        route: &str,
        purpose: &str,
        payload: T,
    ) -> Result<R, RgbServiceClientError>
    where
        T: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let signed = self.signed(purpose, payload)?;
        self.post_json(route, &signed)
    }

    fn post_json<T, R>(&self, route: &str, body: &T) -> Result<R, RgbServiceClientError>
    where
        T: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let url = format!("{}{}", self.daemon_url.trim_end_matches('/'), route);
        let body = serde_json::to_vec(body)?;
        let (host, port, path) = parse_http_url(&url)?;
        let mut stream = TcpStream::connect((host.as_str(), port))?;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        std::io::Write::write_all(&mut stream, request.as_bytes())?;
        std::io::Write::write_all(&mut stream, &body)?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        let response = String::from_utf8(response)?;
        let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
            RgbServiceClientError::Http("invalid daemon HTTP response".to_string())
        })?;
        let status_code = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| RgbServiceClientError::Http("missing daemon HTTP status".to_string()))?;
        if !(200..300).contains(&status_code) {
            return Err(RgbServiceClientError::Daemon {
                status_code,
                body: body.to_string(),
            });
        }
        Ok(serde_json::from_str(body)?)
    }

    fn get_json<R>(&self, route: &str) -> Result<R, RgbServiceClientError>
    where
        R: for<'de> Deserialize<'de>,
    {
        let url = format!("{}{}", self.daemon_url.trim_end_matches('/'), route);
        let (host, port, path) = parse_http_url(&url)?;
        let mut stream = TcpStream::connect((host.as_str(), port))?;
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
        std::io::Write::write_all(&mut stream, request.as_bytes())?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        let response = String::from_utf8(response)?;
        let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
            RgbServiceClientError::Http("invalid daemon HTTP response".to_string())
        })?;
        let status_code = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| RgbServiceClientError::Http("missing daemon HTTP status".to_string()))?;
        if !(200..300).contains(&status_code) {
            return Err(RgbServiceClientError::Daemon {
                status_code,
                body: body.to_string(),
            });
        }
        Ok(serde_json::from_str(body)?)
    }
}

fn parse_http_url(value: &str) -> Result<(String, u16, String), RgbServiceClientError> {
    let value = value.strip_prefix("http://").ok_or_else(|| {
        RgbServiceClientError::InvalidRequest("daemon_url must start with http://".to_string())
    })?;
    let (authority, path) = value.split_once('/').unwrap_or((value, ""));
    let (host, port) = authority.split_once(':').unwrap_or((authority, "80"));
    if host.trim().is_empty() {
        return Err(RgbServiceClientError::InvalidRequest(
            "daemon_url host must not be empty".to_string(),
        ));
    }
    let port = port.parse::<u16>().map_err(|_| {
        RgbServiceClientError::InvalidRequest("daemon_url port is invalid".to_string())
    })?;
    Ok((host.to_string(), port, format!("/{path}")))
}

/// Error returned by the blocking RGB service client.
#[derive(Debug)]
pub enum RgbServiceClientError {
    InvalidRequest(String),
    Http(String),
    Io(std::io::Error),
    Utf8(std::string::FromUtf8Error),
    Json(serde_json::Error),
    Daemon { status_code: u16, body: String },
    Compose(String),
}

impl core::fmt::Display for RgbServiceClientError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidRequest(err) => write!(f, "invalid RGB service request: {err}"),
            Self::Http(err) => write!(f, "RGB service HTTP error: {err}"),
            Self::Io(err) => write!(f, "RGB service I/O error: {err}"),
            Self::Utf8(err) => write!(f, "RGB service UTF-8 error: {err}"),
            Self::Json(err) => write!(f, "RGB service JSON error: {err}"),
            Self::Daemon { status_code, body } => {
                write!(f, "RGB service daemon returned HTTP {status_code}: {body}")
            }
            Self::Compose(err) => write!(f, "RGB LN compose error: {err}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for RgbServiceClientError {}

impl From<std::io::Error> for RgbServiceClientError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<std::string::FromUtf8Error> for RgbServiceClientError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self::Utf8(err)
    }
}

impl From<serde_json::Error> for RgbServiceClientError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<bitcoin::consensus::encode::Error> for RgbServiceClientError {
    fn from(err: bitcoin::consensus::encode::Error) -> Self {
        Self::Compose(err.to_string())
    }
}

static RGB_LN_TX_COMPOSER: OnceLock<Arc<dyn RgbLnTxComposer + Send + Sync>> = OnceLock::new();

pub fn init_rgb_ln_tx_composer(composer: Arc<dyn RgbLnTxComposer + Send + Sync>) {
    let _ = RGB_LN_TX_COMPOSER.set(composer);
}

pub fn get_rgb_ln_tx_composer() -> &'static Arc<dyn RgbLnTxComposer + Send + Sync> {
    RGB_LN_TX_COMPOSER
        .get()
        .expect("RGB LN tx composer is not initialized; configure rgb-service-daemon before using RGB channels")
}

pub trait RgbLnTxComposer {
    fn compose_commitment(
        &self,
        req: RgbLnCommitmentComposeRequest,
    ) -> Result<bitcoin::Transaction, RgbServiceClientError>;

    fn compose_closing(
        &self,
        req: RgbLnClosingComposeRequest,
    ) -> Result<bitcoin::Transaction, RgbServiceClientError>;

    fn compose_onchain_claim(
        &self,
        req: RgbLnOnchainClaimComposeRequest,
    ) -> Result<bitcoin::Transaction, RgbServiceClientError>;
}

#[derive(Clone, Debug)]
pub struct RgbLnCommitmentComposeRequest {
    pub funding_ref: RgbFundingRef,
    pub unsigned_tx: bitcoin::Transaction,
    pub funding_outpoint: bitcoin::OutPoint,
    pub contract_id: ContractId,
    pub to_broadcaster_rgb: u64,
    pub to_broadcaster_vout: Option<u32>,
    pub to_countersignatory_rgb: u64,
    pub to_countersignatory_vout: Option<u32>,
    pub htlcs: Vec<RgbLnHtlcOutput>,
    pub change_vout: u32,
}

#[derive(Clone, Debug)]
pub struct RgbLnClosingComposeRequest {
    pub funding_ref: RgbFundingRef,
    pub unsigned_tx: bitcoin::Transaction,
    pub funding_outpoint: bitcoin::OutPoint,
    pub contract_id: ContractId,
    pub to_holder_rgb: u64,
    pub to_holder_vout: Option<u32>,
    pub to_counterparty_rgb: u64,
    pub to_counterparty_vout: Option<u32>,
    pub change_vout: u32,
}

#[derive(Clone, Debug)]
pub struct RgbLnOnchainClaimComposeRequest {
    pub channel_id: String,
    pub commitment_txid: bitcoin::Txid,
    pub vout: u32,
    pub unsigned_tx: bitcoin::Transaction,
}

#[derive(Clone, Debug)]
pub struct RgbLnHtlcOutput {
    pub vout: u32,
    pub amount_rgb: u64,
}

pub trait RgbServiceSigner {
    fn sign_rgb_service_payload(
        &self,
        purpose: &str,
        payload: &[u8],
    ) -> Result<RequestSignature, RgbServiceClientError>;
}

pub struct RgbDaemonLnTxComposer {
    client: RgbServiceClient,
    account_id: AccountId,
    authorization_ttl_ms: u64,
}

impl RgbDaemonLnTxComposer {
    pub fn new(
        client: RgbServiceClient,
        account_id: impl Into<AccountId>,
        authorization_ttl_ms: u64,
    ) -> Result<Self, RgbServiceClientError> {
        let account_id = account_id.into();
        if account_id.trim().is_empty() {
            return Err(RgbServiceClientError::InvalidRequest(
                "RGB daemon LN composer account_id must not be empty".to_string(),
            ));
        }
        if authorization_ttl_ms == 0 {
            return Err(RgbServiceClientError::InvalidRequest(
                "RGB daemon LN composer authorization_ttl_ms must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            client,
            account_id,
            authorization_ttl_ms,
        })
    }

    fn channel_id(funding_ref: &RgbFundingRef) -> Result<String, RgbServiceClientError> {
        funding_ref
            .channel_id
            .clone()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                RgbServiceClientError::InvalidRequest(
                    "RGB funding_ref.channel_id is required for daemon LN compose".to_string(),
                )
            })
    }

    fn asset_authorization(
        &self,
        purpose: AssetSpendPurpose,
        asset_id: &str,
        amount: u64,
        unsigned_tx_hex: &str,
    ) -> Result<AssetSpendAuthorization, RgbServiceClientError> {
        let expires_at_ms = current_time_ms()?.saturating_add(self.authorization_ttl_ms);
        let unsigned = UnsignedAssetSpendAuthorization {
            asset_id: asset_id.to_string(),
            amount,
            purpose: purpose.clone(),
            recipient: None,
            anchor_psbt: Some(unsigned_tx_hex.to_string()),
            expires_at_ms,
        };
        let bytes = serde_json::to_vec(&unsigned)?;
        let signature = self
            .client
            .signer
            .sign_rgb_service_payload("asset_spend", &bytes)?;
        Ok(AssetSpendAuthorization {
            asset_id: unsigned.asset_id,
            amount: unsigned.amount,
            purpose,
            recipient: unsigned.recipient,
            anchor_psbt: unsigned.anchor_psbt,
            expires_at_ms,
            signature,
        })
    }

    fn tx_hex(tx: &bitcoin::Transaction) -> String {
        hex_encode(&bitcoin::consensus::serialize(tx))
    }

    fn decode_tx(
        response: LnComposeResponse,
    ) -> Result<bitcoin::Transaction, RgbServiceClientError> {
        bitcoin::consensus::deserialize(&hex_decode(&response.tx_hex)?)
            .map_err(RgbServiceClientError::from)
    }
}

impl RgbLnTxComposer for RgbDaemonLnTxComposer {
    fn compose_commitment(
        &self,
        req: RgbLnCommitmentComposeRequest,
    ) -> Result<bitcoin::Transaction, RgbServiceClientError> {
        let unsigned_tx_hex = Self::tx_hex(&req.unsigned_tx);
        let contract_id = req.contract_id.to_string();
        let htlc_total = req.htlcs.iter().map(|htlc| htlc.amount_rgb).sum::<u64>();
        let amount = req
            .to_broadcaster_rgb
            .saturating_add(req.to_countersignatory_rgb)
            .saturating_add(htlc_total);
        let asset_authorization = self.asset_authorization(
            AssetSpendPurpose::L2Settle,
            &contract_id,
            amount,
            &unsigned_tx_hex,
        )?;
        let payload = LnCommitmentComposeRequest {
            account_id: self.account_id.clone(),
            channel_id: Self::channel_id(&req.funding_ref)?,
            funding_ref: req.funding_ref,
            unsigned_tx_hex,
            funding_outpoint: req.funding_outpoint.to_string(),
            contract_id,
            to_local_rgb: req.to_broadcaster_rgb,
            to_local_vout: req.to_broadcaster_vout,
            to_remote_rgb: req.to_countersignatory_rgb,
            to_remote_vout: req.to_countersignatory_vout,
            htlcs: req
                .htlcs
                .into_iter()
                .map(|htlc| LnRgbHtlcOutput {
                    vout: htlc.vout,
                    amount_rgb: htlc.amount_rgb,
                })
                .collect(),
            change_vout: req.change_vout,
            asset_authorization,
        };
        Self::decode_tx(self.client.compose_ln_commitment(payload)?)
    }

    fn compose_closing(
        &self,
        req: RgbLnClosingComposeRequest,
    ) -> Result<bitcoin::Transaction, RgbServiceClientError> {
        let unsigned_tx_hex = Self::tx_hex(&req.unsigned_tx);
        let contract_id = req.contract_id.to_string();
        let amount = req.to_holder_rgb.saturating_add(req.to_counterparty_rgb);
        let asset_authorization = self.asset_authorization(
            AssetSpendPurpose::ChannelWithdraw,
            &contract_id,
            amount,
            &unsigned_tx_hex,
        )?;
        let payload = LnClosingComposeRequest {
            account_id: self.account_id.clone(),
            channel_id: Self::channel_id(&req.funding_ref)?,
            funding_ref: req.funding_ref,
            unsigned_tx_hex,
            funding_outpoint: req.funding_outpoint.to_string(),
            contract_id,
            to_local_rgb: req.to_holder_rgb,
            to_local_vout: req.to_holder_vout,
            to_remote_rgb: req.to_counterparty_rgb,
            to_remote_vout: req.to_counterparty_vout,
            change_vout: req.change_vout,
            asset_authorization,
        };
        Self::decode_tx(self.client.compose_ln_closing(payload)?)
    }

    fn compose_onchain_claim(
        &self,
        req: RgbLnOnchainClaimComposeRequest,
    ) -> Result<bitcoin::Transaction, RgbServiceClientError> {
        let unsigned_tx_hex = Self::tx_hex(&req.unsigned_tx);
        let payload = LnOnchainClaimComposeRequest {
            account_id: self.account_id.clone(),
            channel_id: req.channel_id,
            commitment_txid: req.commitment_txid.to_string(),
            vout: req.vout,
            unsigned_tx_hex,
        };
        Self::decode_tx(self.client.compose_ln_onchain_claim(payload)?)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct UnsignedAssetSpendAuthorization {
    asset_id: String,
    amount: u64,
    purpose: AssetSpendPurpose,
    recipient: Option<String>,
    anchor_psbt: Option<String>,
    expires_at_ms: u64,
}

fn current_time_ms() -> Result<u64, RgbServiceClientError> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| RgbServiceClientError::Compose(err.to_string()))?
        .as_millis() as u64)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(value: &str) -> Result<Vec<u8>, RgbServiceClientError> {
    let value = value.trim();
    if value.len() % 2 != 0 {
        return Err(RgbServiceClientError::InvalidRequest(
            "hex string must have an even length".to_string(),
        ));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|err| RgbServiceClientError::InvalidRequest(err.to_string()))
        })
        .collect()
}

/// Signed request envelope used by `rgb-service-daemon`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignedRequest<T> {
    pub payload: T,
    pub signature: RequestSignature,
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

pub type AccountId = String;
pub type AssetId = String;
pub type ContractIdString = String;
pub type OperationId = String;
pub type TransferId = String;
pub type InvoiceId = String;
pub type TxidString = String;
pub type OutpointString = String;
pub type BtcAddress = String;

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
pub struct IssueAssetRequest {
    pub account_id: AccountId,
    pub ticker: String,
    pub name: String,
    pub precision: u8,
    pub supply: u64,
    pub allocation_outpoint: OutpointString,
    pub utxos: Vec<TrackedUtxo>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IssueAssetResponse {
    pub contract_id: ContractIdString,
    pub asset_id: AssetId,
    pub allocation_outpoint: OutpointString,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListAssetsRequest {
    pub account_id: AccountId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListAssetsResponse {
    pub assets: Vec<RgbAssetInfo>,
    pub utxo_assets: BTreeMap<OutpointString, Vec<RgbAllocation>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TokenListResponse {
    pub contracts: Vec<RgbContractInfo>,
    pub assets: Vec<RgbAssetInfo>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbContractInfo {
    pub contract_id: ContractIdString,
    pub schema: String,
    pub asset_id: Option<AssetId>,
    pub ticker: String,
    pub name: String,
    pub precision: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbAssetInfo {
    pub asset_id: AssetId,
    pub contract_id: ContractIdString,
    pub ticker: String,
    pub name: String,
    pub precision: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BalanceRequest {
    pub account_id: AccountId,
    pub asset_id: AssetId,
    pub scope: BalanceScope,
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
pub struct TrackedUtxo {
    pub outpoint: OutpointString,
    pub address: Option<String>,
    pub confirmed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbAllocation {
    pub asset_id: AssetId,
    pub outpoint: OutpointString,
    pub amount: u64,
    pub layer: AssetLayer,
    pub status: AllocationStatus,
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
    pub txid: TxidString,
    pub signed_anchor_psbt: Option<String>,
    pub utxos: Vec<TrackedUtxo>,
    pub asset_authorization: AssetSpendAuthorization,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitTransferResponse {
    pub transfer_id: TransferId,
    pub operation_id: OperationId,
    pub status: OperationStatus,
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
pub struct LnChannelOpenPrepareRequest {
    pub account_id: AccountId,
    pub channel_id: String,
    pub contract_id: ContractIdString,
    pub unsigned_anchor_psbt: String,
    pub change_vout: u32,
    pub funding_vout: u32,
    pub funding_rgb: u64,
    pub to_local_rgb: u64,
    pub to_remote_rgb: u64,
    pub asset_authorization: AssetSpendAuthorization,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnChannelOpenPrepareResponse {
    pub funding_ref: RgbFundingRef,
    pub operation_id: OperationId,
    pub funding_outpoint: OutpointString,
    pub anchor_psbt: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnChannelFundingRefRequest {
    pub account_id: AccountId,
    pub channel_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnChannelFundingRefResponse {
    pub funding_ref: Option<RgbFundingRef>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnPaymentClaimRequest {
    pub account_id: AccountId,
    pub channel_id: Option<String>,
    pub payment_hash: String,
    pub contract_id: ContractIdString,
    pub amount_msat: u64,
    pub rgb_amount: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnPaymentClaimResponse {
    pub operation_id: OperationId,
    pub status: OperationStatus,
    pub rgb_state_ref: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnCommitmentComposeRequest {
    pub account_id: AccountId,
    pub channel_id: String,
    pub funding_ref: RgbFundingRef,
    pub unsigned_tx_hex: String,
    pub funding_outpoint: OutpointString,
    pub contract_id: ContractIdString,
    pub to_local_rgb: u64,
    pub to_local_vout: Option<u32>,
    pub to_remote_rgb: u64,
    pub to_remote_vout: Option<u32>,
    #[serde(default)]
    pub htlcs: Vec<LnRgbHtlcOutput>,
    pub change_vout: u32,
    pub asset_authorization: AssetSpendAuthorization,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnClosingComposeRequest {
    pub account_id: AccountId,
    pub channel_id: String,
    pub funding_ref: RgbFundingRef,
    pub unsigned_tx_hex: String,
    pub funding_outpoint: OutpointString,
    pub contract_id: ContractIdString,
    pub to_local_rgb: u64,
    pub to_local_vout: Option<u32>,
    pub to_remote_rgb: u64,
    pub to_remote_vout: Option<u32>,
    pub change_vout: u32,
    pub asset_authorization: AssetSpendAuthorization,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnOnchainClaimComposeRequest {
    pub account_id: AccountId,
    pub channel_id: String,
    pub commitment_txid: TxidString,
    pub vout: u32,
    pub unsigned_tx_hex: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnRgbHtlcOutput {
    pub vout: u32,
    pub amount_rgb: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LnOnchainClaimPurpose {
    CommitmentSweep,
    HtlcSuccess,
    HtlcTimeout,
    Penalty,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LnComposeResponse {
    pub operation_id: OperationId,
    pub tx_hex: String,
    pub rgb_state_ref: Option<String>,
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

impl Readable for ContractId {
    fn read<R: bitcoin::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let cid = <[u8; 32] as Readable>::read(reader)?;
        Ok(Self(cid))
    }
}

impl Writeable for ContractId {
    fn write<W: Writer>(&self, writer: &mut W) -> Result<(), crate::io::Error> {
        self.0.write(writer)
    }
}

impl Readable for RgbClaimPurpose {
    fn read<R: bitcoin::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
        match <u8 as Readable>::read(reader)? {
            0 => Ok(Self::ToBroadcaster),
            1 => Ok(Self::ToCountersignatory),
            2 => Ok(Self::Htlc),
            _ => Err(DecodeError::InvalidValue),
        }
    }
}

impl Writeable for RgbClaimPurpose {
    fn write<W: Writer>(&self, writer: &mut W) -> Result<(), crate::io::Error> {
        let value = match self {
            Self::ToBroadcaster => 0u8,
            Self::ToCountersignatory => 1u8,
            Self::Htlc => 2u8,
        };
        value.write(writer)
    }
}

impl Readable for RgbClaimableOutpoint {
    fn read<R: bitcoin::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(Self {
            outpoint: Readable::read(reader)?,
            contract_id: Readable::read(reader)?,
            amount_rgb: Readable::read(reader)?,
            purpose: Readable::read(reader)?,
        })
    }
}

impl Writeable for RgbClaimableOutpoint {
    fn write<W: Writer>(&self, writer: &mut W) -> Result<(), crate::io::Error> {
        self.outpoint.write(writer)?;
        self.contract_id.write(writer)?;
        self.amount_rgb.write(writer)?;
        self.purpose.write(writer)
    }
}

/// Daemon-owned RGB funding reference for a Lightning channel.
///
/// This is deliberately an opaque reference. The Lightning fork does not keep RGB
/// stock, validated transfers, consignments, fascia, or proof data locally.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RgbFundingRef {
    /// Daemon transfer or reservation id backing the channel funding state.
    pub transfer_id: TransferId,
    /// Daemon operation id that created or last updated this reference.
    pub operation_id: OperationId,
    /// Optional daemon-side channel namespace.
    pub channel_id: Option<String>,
}

impl RgbFundingRef {
    /// Creates a new daemon funding reference.
    pub fn new(
        transfer_id: impl Into<TransferId>,
        operation_id: impl Into<OperationId>,
        channel_id: Option<String>,
    ) -> Self {
        Self {
            transfer_id: transfer_id.into(),
            operation_id: operation_id.into(),
            channel_id,
        }
    }
}

impl Readable for RgbFundingRef {
    fn read<R: bitcoin::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(Self {
            transfer_id: Readable::read(reader)?,
            operation_id: Readable::read(reader)?,
            channel_id: Readable::read(reader)?,
        })
    }
}

impl Writeable for RgbFundingRef {
    fn write<W: Writer>(&self, writer: &mut W) -> Result<(), crate::io::Error> {
        self.transfer_id.write(writer)?;
        self.operation_id.write(writer)?;
        self.channel_id.write(writer)?;
        Ok(())
    }
}

/// RGB channel accounting tracked by the Lightning fork.
#[derive(Debug, Clone)]
pub struct RgbContext {
    /// RGB contract id committed into the channel.
    pub contract_id: ContractId,
    /// RGB amount committed into the funding output.
    pub funding_rgb: u64,
    /// Holder RGB balance for the current commitment view.
    pub to_self: u64,
    /// Opaque daemon reference for RGB funding state.
    pub funding_ref: Option<RgbFundingRef>,
}

impl Readable for RgbContext {
    fn read<R: bitcoin::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
        Ok(Self {
            contract_id: Readable::read(reader)?,
            funding_rgb: Readable::read(reader)?,
            to_self: Readable::read(reader)?,
            funding_ref: Readable::read(reader)?,
        })
    }
}

impl Writeable for RgbContext {
    fn write<W: Writer>(&self, writer: &mut W) -> Result<(), crate::io::Error> {
        self.contract_id.write(writer)?;
        self.funding_rgb.write(writer)?;
        self.to_self.write(writer)?;
        self.funding_ref.write(writer)?;
        Ok(())
    }
}

impl RgbContext {
    /// Creates RGB context for an inbound channel.
    pub fn new_for_inbound_channel(contract_id: ContractId, funding_rgb: u64) -> Self {
        Self {
            contract_id,
            funding_rgb,
            to_self: 0,
            funding_ref: None,
        }
    }

    /// Creates RGB context for an outbound channel.
    pub fn new_for_outbound_channel(contract_id: ContractId, funding_rgb: u64) -> Self {
        Self {
            contract_id,
            funding_rgb,
            to_self: funding_rgb,
            funding_ref: None,
        }
    }

    /// Attaches the daemon funding reference to this channel context.
    pub fn provide_funding_ref(&mut self, funding_ref: RgbFundingRef) {
        assert!(self.funding_ref.replace(funding_ref).is_none());
    }

    /// Backwards-compatible method name for callers that previously supplied RGB transfers.
    ///
    /// The payload is now only a daemon reference, not a validated RGB transfer body.
    pub fn provide_funding_transfer(&mut self, funding_ref: RgbFundingRef) {
        self.provide_funding_ref(funding_ref);
    }

    /// Returns true once the daemon has reserved or prepared RGB funding for this channel.
    pub fn has_funding_ref(&self) -> bool {
        self.funding_ref.is_some()
    }
}

/// RGB asset amount attached to a channel or payment intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RgbAssetAmount {
    /// RGB contract id.
    pub contract_id: ContractId,
    /// RGB amount in the asset's smallest unit.
    pub amount: u64,
}

impl RgbAssetAmount {
    /// Creates a new RGB asset amount marker.
    pub fn new(contract_id: ContractId, amount: u64) -> Self {
        Self {
            contract_id,
            amount,
        }
    }
}

/// RGB context attached to a channel-open request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RgbChannelContext {
    /// Asset committed into the funding flow.
    pub asset: RgbAssetAmount,
}

impl RgbChannelContext {
    /// Creates a new RGB channel context.
    pub fn new(asset: RgbAssetAmount) -> Self {
        Self { asset }
    }

    /// Converts this request context into persistent channel RGB state.
    pub fn into_rgb_context(self, outbound: bool) -> RgbContext {
        if outbound {
            RgbContext::new_for_outbound_channel(self.asset.contract_id, self.asset.amount)
        } else {
            RgbContext::new_for_inbound_channel(self.asset.contract_id, self.asset.amount)
        }
    }
}

/// RGB funding payload bound to an unfunded channel.
///
/// This type intentionally carries only a daemon reference. RGB entity data remains in
/// `rgb-service-daemon`.
#[derive(Clone, Debug)]
pub struct RgbFundingTransfer {
    /// Opaque daemon reference for the channel funding RGB state.
    pub funding_ref: RgbFundingRef,
}

impl RgbFundingTransfer {
    /// Creates a new RGB funding reference payload.
    pub fn new(funding_ref: RgbFundingRef) -> Self {
        Self { funding_ref }
    }
}

/// RGB metadata attached to a Lightning payment intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RgbPaymentMetadata {
    /// Asset being transferred through the Lightning payment.
    pub asset: RgbAssetAmount,
}

impl RgbPaymentMetadata {
    /// Creates a new RGB payment metadata marker.
    pub fn new(asset: RgbAssetAmount) -> Self {
        Self { asset }
    }
}
