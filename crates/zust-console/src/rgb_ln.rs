use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bitcoin::hashes::{sha256, Hash as BitcoinHash};
use bitcoin::secp256k1::PublicKey;
use bitcoin::OutPoint;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use rgbstd::ContractId as Rgb20ContractId;
use serde::{Deserialize, Serialize};

use crate::btc_ln::{BtcLnEvent, BtcLnNode};
use crate::iroh_transport::{
    bind_iroh_endpoint_from_mnemonic, probe_assignment_receiver_with_retry,
    receive_probe_with_retry,
    receive_rgb20_transfer_assignment_validation_checked_with_retry_or_reject,
    receive_rgb20_transfer_assignment_with_retry, rgb20_transfer_assignment_from_consignment,
    send_assignment_with_retry, send_assignment_with_retry_or_reject, IrohAssignmentAck,
    IrohAssignmentEnvelope, IrohAssignmentReceipt,
};
use crate::lnnode::{
    ChannelId, PaymentId, RgbAssetAmount, RgbChannelOpenRequest, RgbFundingTransfer, RgbLnNode,
    RgbPaymentRequest,
};
use crate::node_config::LightningNodeConfig;
use crate::rgb20::{accept_rgb20_transfer_bytes, export_rgb20_transfer_consignment};

#[derive(Clone, Debug)]
pub struct RgbLnRetry {
    pub attempts: u32,
    pub delay: Duration,
}

impl Default for RgbLnRetry {
    fn default() -> Self {
        Self {
            attempts: 5,
            delay: Duration::from_secs(3),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RgbLnPreflight {
    pub sender_iroh_id: EndpointId,
    pub receiver_iroh_id: EndpointId,
    pub ack: IrohAssignmentAck,
}

#[derive(Clone, Debug)]
pub struct RgbLnDirectPaymentResult {
    pub payment_id: PaymentId,
    pub contract_id: Rgb20ContractId,
    pub rgb_amount: u64,
    pub amount_msat: u64,
}

#[derive(Clone, Debug)]
pub struct RgbLnDirectTransferRequest {
    pub open_channel: Option<RgbChannelOpenRequest>,
    pub funding_transfer: Option<RgbFundingTransfer>,
    pub payment: RgbPaymentRequest,
}

#[derive(Clone, Debug)]
pub struct RgbLnDirectTransferResult {
    pub channel_id: Option<ChannelId>,
    pub payment: RgbLnDirectPaymentResult,
}

#[derive(Clone, Debug)]
pub struct RgbLnRgb20PaymentRequest {
    pub sender_stock_dir: PathBuf,
    pub receiver_stock_dir: PathBuf,
    pub receiver_network: bitcoin::Network,
    pub receiver_esplora_url: String,
    pub channel_id: ChannelId,
    pub contract_id: Rgb20ContractId,
    pub recipient_outpoint: OutPoint,
    pub payment: RgbPaymentRequest,
}

#[derive(Clone, Debug)]
pub struct RgbLnRgb20PaymentResult {
    pub assignment_ack: IrohAssignmentAck,
    pub assignment_receipt: IrohAssignmentReceipt,
    pub payment: RgbLnDirectPaymentResult,
    pub pending_assignment_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbLnPendingAssignmentRecord {
    pub transfer_id: String,
    pub contract_id: String,
    pub recipient_outpoint: String,
    pub payload_sha256: String,
    pub payload_path: PathBuf,
    pub status: String,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RgbLnPendingRecoveryReport {
    pub scanned: usize,
    pub recovered: usize,
    pub skipped: usize,
}

#[derive(Clone, Debug)]
pub struct RgbLnPaymentAcceptanceRequest {
    pub channel_id: ChannelId,
    pub funding_outpoint: OutPoint,
    pub transfer_id: String,
    pub contract_id: Rgb20ContractId,
    pub rgb_amount: u64,
    pub amount_msat: u64,
    pub payment_id: PaymentId,
    pub payload_sha256: String,
}

#[derive(Clone, Debug)]
pub struct RgbLnPaymentAcceptanceDecision {
    pub accepted: bool,
    pub message: String,
}

impl RgbLnPaymentAcceptanceDecision {
    pub fn accept() -> Self {
        Self {
            accepted: true,
            message: "accepted".to_string(),
        }
    }

    pub fn reject(message: impl Into<String>) -> Self {
        Self {
            accepted: false,
            message: message.into(),
        }
    }
}

pub struct RgbLnSession {
    sender_endpoint: Endpoint,
    _receiver_endpoint: Endpoint,
    receiver_addr: EndpointAddr,
    retry: RgbLnRetry,
    preflight: RgbLnPreflight,
}

pub trait RgbLnFundingTransferSource {
    fn generated_rgb_funding_transfer(&self, channel_id: ChannelId) -> Option<RgbFundingTransfer>;

    fn take_generated_rgb_funding_transfer(
        &self,
        channel_id: ChannelId,
    ) -> Option<RgbFundingTransfer>;

    fn rgb_channel_funding_outpoint(&self, _channel_id: ChannelId) -> Option<OutPoint> {
        None
    }
}

impl<T> RgbLnFundingTransferSource for Arc<T>
where
    T: RgbLnFundingTransferSource + ?Sized,
{
    fn generated_rgb_funding_transfer(&self, channel_id: ChannelId) -> Option<RgbFundingTransfer> {
        self.as_ref().generated_rgb_funding_transfer(channel_id)
    }

    fn take_generated_rgb_funding_transfer(
        &self,
        channel_id: ChannelId,
    ) -> Option<RgbFundingTransfer> {
        self.as_ref()
            .take_generated_rgb_funding_transfer(channel_id)
    }

    fn rgb_channel_funding_outpoint(&self, channel_id: ChannelId) -> Option<OutPoint> {
        self.as_ref().rgb_channel_funding_outpoint(channel_id)
    }
}

impl RgbLnSession {
    pub fn preflight(&self) -> &RgbLnPreflight {
        &self.preflight
    }

    /// Runs an LN/RGB backend action only after the Iroh counterparty is reachable.
    ///
    /// This is the guardrail for RGB-over-LN funding/payment operations: callers should place
    /// channel funding, HTLC/PTLC updates, or RGB-capable LN backend state transitions inside this
    /// closure so those actions cannot happen before the RGB data path is available.
    pub async fn after_preflight<F, Fut, T>(&self, action: F) -> Result<T>
    where
        F: FnOnce(RgbLnPreflight) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        action(self.preflight.clone()).await
    }

    pub async fn open_rgb_channel_after_preflight<B>(
        &self,
        backend: &B,
        request: RgbChannelOpenRequest,
    ) -> Result<ChannelId>
    where
        B: RgbLnNode,
    {
        self.after_preflight(|_| async move { backend.open_rgb_channel(request) })
            .await
    }

    pub async fn open_rgb_channel_with_funding_transfer<B>(
        &self,
        backend: &B,
        request: RgbChannelOpenRequest,
        receiver_stock_dir: &Path,
        receiver_network: bitcoin::Network,
        receiver_esplora_url: &str,
    ) -> Result<(ChannelId, IrohAssignmentAck, IrohAssignmentReceipt)>
    where
        B: RgbLnNode + RgbLnFundingTransferSource + BtcLnNode,
    {
        let channel_id = self
            .open_rgb_channel_after_preflight(backend, request)
            .await?;
        let mut funding = None;
        for _ in 0..self.retry.attempts {
            if let Some(candidate) = backend.generated_rgb_funding_transfer(channel_id) {
                funding = Some(candidate);
                break;
            }
            tokio::time::sleep(self.retry.delay).await;
        }
        let funding = funding.with_context(|| {
            format!("RGB LN funding transfer was not generated for channel {channel_id:?}")
        })?;
        let (ack, receipt) = self
            .send_accept_and_provide_funding_transfer(
                backend,
                receiver_stock_dir,
                receiver_network,
                receiver_esplora_url,
                funding,
            )
            .await?;
        backend.take_generated_rgb_funding_transfer(channel_id);
        Ok((channel_id, ack, receipt))
    }

    pub async fn open_rgb_channel_with_funding_transfer_between<B, R>(
        &self,
        backend: &B,
        receiver_node: &R,
        request: RgbChannelOpenRequest,
        receiver_stock_dir: &Path,
        receiver_network: bitcoin::Network,
        receiver_esplora_url: &str,
    ) -> Result<(ChannelId, IrohAssignmentAck, IrohAssignmentReceipt)>
    where
        B: RgbLnNode + RgbLnFundingTransferSource + BtcLnNode,
        R: RgbLnNode + BtcLnNode,
    {
        let channel_id = self
            .open_rgb_channel_after_preflight(backend, request)
            .await?;
        let mut funding = None;
        for _ in 0..self.retry.attempts {
            if let Some(candidate) = backend.generated_rgb_funding_transfer(channel_id) {
                funding = Some(candidate);
                break;
            }
            tokio::time::sleep(self.retry.delay).await;
        }
        let funding = funding.with_context(|| {
            format!("RGB LN funding transfer was not generated for channel {channel_id:?}")
        })?;
        let (ack, receipt) = self
            .send_accept_and_provide_funding_transfer_between(
                backend,
                receiver_node,
                receiver_stock_dir,
                receiver_network,
                receiver_esplora_url,
                funding,
            )
            .await?;
        backend.take_generated_rgb_funding_transfer(channel_id);
        Ok((channel_id, ack, receipt))
    }

    pub async fn provide_funding_transfer_after_preflight<B>(
        &self,
        backend: &B,
        funding: RgbFundingTransfer,
    ) -> Result<()>
    where
        B: RgbLnNode,
    {
        self.after_preflight(|_| async move { backend.provide_funding_transfer(funding) })
            .await
    }

    pub async fn send_rgb_payment_after_preflight<B>(
        &self,
        backend: &B,
        request: RgbPaymentRequest,
    ) -> Result<()>
    where
        B: RgbLnNode,
    {
        self.after_preflight(|_| async move { backend.send_rgb_payment(request) })
            .await
    }

    pub async fn send_existing_rgb20_consignment(
        &self,
        sender_stock_dir: &Path,
        contract_id: Rgb20ContractId,
        recipient_outpoint: OutPoint,
    ) -> Result<IrohAssignmentAck> {
        let consignment =
            export_rgb20_transfer_consignment(sender_stock_dir, contract_id, recipient_outpoint)?;
        let assignment = rgb20_transfer_assignment_from_consignment(
            &consignment,
            recipient_outpoint.txid,
            recipient_outpoint,
        )?;
        send_assignment_with_retry(
            &self.sender_endpoint,
            self.receiver_addr.clone(),
            &assignment,
            self.retry.attempts,
            self.retry.delay,
        )
        .await
        .context("failed to send RGB20 consignment over Iroh after RGB-LN preflight")
    }

    async fn send_existing_rgb20_consignment_or_reject(
        &self,
        sender_stock_dir: &Path,
        contract_id: Rgb20ContractId,
        recipient_outpoint: OutPoint,
    ) -> Result<IrohAssignmentAck> {
        let consignment =
            export_rgb20_transfer_consignment(sender_stock_dir, contract_id, recipient_outpoint)?;
        let assignment = rgb20_transfer_assignment_from_consignment(
            &consignment,
            recipient_outpoint.txid,
            recipient_outpoint,
        )?;
        send_assignment_with_retry_or_reject(
            &self.sender_endpoint,
            self.receiver_addr.clone(),
            &assignment,
            self.retry.attempts,
            self.retry.delay,
        )
        .await
        .context("failed to send RGB20 consignment over Iroh after RGB-LN preflight")
    }

    pub async fn send_and_accept_existing_rgb20_consignment(
        &self,
        sender_stock_dir: &Path,
        receiver_stock_dir: &Path,
        receiver_network: bitcoin::Network,
        receiver_esplora_url: &str,
        contract_id: Rgb20ContractId,
        recipient_outpoint: OutPoint,
    ) -> Result<(IrohAssignmentAck, IrohAssignmentReceipt)> {
        let receiver_endpoint = self._receiver_endpoint.clone();
        let retry = self.retry.clone();
        let receiver_stock_dir = receiver_stock_dir.to_path_buf();
        let receiver_esplora_url = receiver_esplora_url.to_string();
        let receiver = tokio::spawn(async move {
            receive_rgb20_transfer_assignment_with_retry(
                &receiver_endpoint,
                &receiver_stock_dir,
                receiver_network,
                &receiver_esplora_url,
                retry.attempts,
                retry.delay,
            )
            .await
        });

        let send = self
            .send_existing_rgb20_consignment(sender_stock_dir, contract_id, recipient_outpoint)
            .await;
        let ack = match send {
            Ok(ack) => ack,
            Err(err) => {
                receiver.abort();
                return Err(err);
            }
        };
        let receipt = receiver
            .await
            .context("RGB-LN Iroh receiver task failed")?
            .context("RGB-LN Iroh receiver rejected consignment")?;
        Ok((ack, receipt))
    }

    pub async fn send_accept_and_provide_funding_transfer<B>(
        &self,
        backend: &B,
        receiver_stock_dir: &Path,
        receiver_network: bitcoin::Network,
        receiver_esplora_url: &str,
        funding: RgbFundingTransfer,
    ) -> Result<(IrohAssignmentAck, IrohAssignmentReceipt)>
    where
        B: RgbLnNode,
    {
        let receiver_endpoint = self._receiver_endpoint.clone();
        let retry = self.retry.clone();
        let receiver_stock_dir = receiver_stock_dir.to_path_buf();
        let receiver_esplora_url = receiver_esplora_url.to_string();
        let receiver = tokio::spawn(async move {
            receive_rgb20_transfer_assignment_with_retry(
                &receiver_endpoint,
                &receiver_stock_dir,
                receiver_network,
                &receiver_esplora_url,
                retry.attempts,
                retry.delay,
            )
            .await
        });

        let assignment = rgb20_transfer_assignment_from_consignment(
            &funding.transfer,
            funding.funding_outpoint.txid,
            funding.funding_outpoint,
        )?;
        let send = send_assignment_with_retry(
            &self.sender_endpoint,
            self.receiver_addr.clone(),
            &assignment,
            self.retry.attempts,
            self.retry.delay,
        )
        .await
        .context("failed to send RGB LN funding transfer over Iroh");
        let ack = match send {
            Ok(ack) => ack,
            Err(err) => {
                receiver.abort();
                return Err(err);
            }
        };
        let receipt = receiver
            .await
            .context("RGB-LN funding Iroh receiver task failed")?
            .context("RGB-LN funding receiver rejected transfer")?;
        anyhow::ensure!(
            ack.accepted,
            "RGB-LN funding receiver did not accept assignment: {}",
            ack.message
        );
        self.after_preflight(|_| async move { backend.provide_funding_transfer(funding) })
            .await
            .context("failed to provide RGB LN funding transfer to backend")?;
        Ok((ack, receipt))
    }

    pub async fn send_accept_and_provide_funding_transfer_between<B, R>(
        &self,
        backend: &B,
        receiver_node: &R,
        receiver_stock_dir: &Path,
        receiver_network: bitcoin::Network,
        receiver_esplora_url: &str,
        funding: RgbFundingTransfer,
    ) -> Result<(IrohAssignmentAck, IrohAssignmentReceipt)>
    where
        B: RgbLnNode + BtcLnNode,
        R: RgbLnNode + BtcLnNode,
    {
        let receiver_endpoint = self._receiver_endpoint.clone();
        let retry = self.retry.clone();
        let receiver_stock_dir = receiver_stock_dir.to_path_buf();
        let receiver_esplora_url = receiver_esplora_url.to_string();
        let receiver = tokio::spawn(async move {
            receive_rgb20_transfer_assignment_with_retry(
                &receiver_endpoint,
                &receiver_stock_dir,
                receiver_network,
                &receiver_esplora_url,
                retry.attempts,
                retry.delay,
            )
            .await
        });

        let assignment = rgb20_transfer_assignment_from_consignment(
            &funding.transfer,
            funding.funding_outpoint.txid,
            funding.funding_outpoint,
        )?;
        let send = send_assignment_with_retry(
            &self.sender_endpoint,
            self.receiver_addr.clone(),
            &assignment,
            self.retry.attempts,
            self.retry.delay,
        )
        .await
        .context("failed to send RGB LN funding transfer over Iroh");
        let ack = match send {
            Ok(ack) => ack,
            Err(err) => {
                receiver.abort();
                return Err(err);
            }
        };
        let receipt = receiver
            .await
            .context("RGB-LN funding Iroh receiver task failed")?
            .context("RGB-LN funding receiver rejected transfer")?;
        anyhow::ensure!(
            ack.accepted,
            "RGB-LN funding receiver did not accept assignment: {}",
            ack.message
        );

        let receiver_funding = RgbFundingTransfer {
            temporary_channel_id: funding.temporary_channel_id,
            peer_node_id: backend.node_id(),
            funding_outpoint: funding.funding_outpoint,
            transfer: funding.transfer.clone(),
        };
        self.after_preflight(|_| async move {
            receiver_node.provide_funding_transfer(receiver_funding)
        })
        .await
        .context("failed to provide RGB LN funding transfer to receiver backend")?;
        self.after_preflight(|_| async move { backend.provide_funding_transfer(funding) })
            .await
            .context("failed to provide RGB LN funding transfer to sender backend")?;
        Ok((ack, receipt))
    }

    pub async fn send_rgb20_consignment_then_lightning_payment<B>(
        &self,
        backend: &B,
        request: RgbLnRgb20PaymentRequest,
    ) -> Result<RgbLnRgb20PaymentResult>
    where
        B: RgbLnNode + BtcLnNode + RgbLnFundingTransferSource,
    {
        self.send_rgb20_consignment_then_lightning_payment_inner(backend, None, request, |_| {
            Ok(RgbLnPaymentAcceptanceDecision::accept())
        })
        .await
    }

    pub async fn send_rgb20_consignment_then_lightning_payment_with_receiver<B, R>(
        &self,
        backend: &B,
        receiver_node: &R,
        request: RgbLnRgb20PaymentRequest,
    ) -> Result<RgbLnRgb20PaymentResult>
    where
        B: RgbLnNode + BtcLnNode + RgbLnFundingTransferSource,
        R: BtcLnNode,
    {
        self.send_rgb20_consignment_then_lightning_payment_inner(
            backend,
            Some(receiver_node),
            request,
            |_| Ok(RgbLnPaymentAcceptanceDecision::accept()),
        )
        .await
    }

    pub async fn send_rgb20_consignment_then_lightning_payment_with_receiver_decision<B, R, F>(
        &self,
        backend: &B,
        receiver_node: &R,
        request: RgbLnRgb20PaymentRequest,
        accept_rgb_payment: F,
    ) -> Result<RgbLnRgb20PaymentResult>
    where
        B: RgbLnNode + BtcLnNode + RgbLnFundingTransferSource,
        R: BtcLnNode,
        F: Fn(&RgbLnPaymentAcceptanceRequest) -> Result<RgbLnPaymentAcceptanceDecision>
            + Send
            + Sync
            + 'static,
    {
        self.send_rgb20_consignment_then_lightning_payment_inner(
            backend,
            Some(receiver_node),
            request,
            accept_rgb_payment,
        )
        .await
    }

    async fn send_rgb20_consignment_then_lightning_payment_inner<B, F>(
        &self,
        backend: &B,
        receiver_node: Option<&dyn BtcLnNode>,
        request: RgbLnRgb20PaymentRequest,
        accept_rgb_payment: F,
    ) -> Result<RgbLnRgb20PaymentResult>
    where
        B: RgbLnNode + BtcLnNode + RgbLnFundingTransferSource,
        F: Fn(&RgbLnPaymentAcceptanceRequest) -> Result<RgbLnPaymentAcceptanceDecision>
            + Send
            + Sync
            + 'static,
    {
        validate_rgb_ln_rgb20_payment_request(&request)?;
        let funding_outpoint = backend
            .rgb_channel_funding_outpoint(request.channel_id)
            .with_context(|| {
                format!(
                    "RGB-LN channel funding outpoint not found for channel {}",
                    hex32(request.channel_id.0)
                )
            })?;
        validate_rgb_ln_payment_outpoint_matches_channel(&request, funding_outpoint)?;
        let receiver_endpoint = self._receiver_endpoint.clone();
        let retry = self.retry.clone();
        let receiver_esplora_url = request.receiver_esplora_url.clone();
        let receiver_network = request.receiver_network;
        let receiver_request = request.clone();
        let receiver = tokio::spawn(async move {
            receive_rgb20_transfer_assignment_validation_checked_with_retry_or_reject(
                &receiver_endpoint,
                receiver_network,
                &receiver_esplora_url,
                retry.attempts,
                retry.delay,
                move |envelope, _payload| {
                    validate_rgb_ln_assignment_matches_channel(
                        envelope,
                        &receiver_request,
                        funding_outpoint,
                    )?;
                    let decision = accept_rgb_payment(&RgbLnPaymentAcceptanceRequest {
                        channel_id: receiver_request.channel_id,
                        funding_outpoint,
                        transfer_id: envelope.transfer_id.clone(),
                        contract_id: receiver_request.contract_id,
                        rgb_amount: receiver_request.payment.asset.amount,
                        amount_msat: receiver_request.payment.amount_msat,
                        payment_id: receiver_request.payment.payment_id,
                        payload_sha256: envelope.payload_sha256.clone(),
                    })?;
                    anyhow::ensure!(decision.accepted, "{}", decision.message);
                    Ok(())
                },
            )
            .await
        });

        let assignment_ack = self
            .send_existing_rgb20_consignment_or_reject(
                &request.sender_stock_dir,
                request.contract_id,
                request.recipient_outpoint,
            )
            .await
            .context("RGB20 consignment was not delivered for validation before RGB-LN payment")?;
        let assignment_receipt = receiver
            .await
            .context("RGB-LN validation receiver task failed")?
            .context("RGB-LN receiver rejected consignment validation")?;
        anyhow::ensure!(
            assignment_ack.accepted,
            "RGB-LN receiver did not validate consignment: {}",
            assignment_ack.message
        );
        let pending_record =
            persist_pending_rgb_ln_assignment(&request.receiver_stock_dir, &assignment_receipt)
                .context("failed to persist pending RGB-LN assignment before payment")?;

        let expected_payment = request.payment.clone();
        let payment = self
            .after_preflight(
                |_| async move { send_rgb_over_lightning(backend, request.payment).await },
            )
            .await
            .context("failed to send RGB payment through Lightning after consignment ack")?;
        wait_for_rgb_ln_payment_success_with_receiver_rgb(
            backend,
            receiver_node,
            payment.payment_id,
            Some((&expected_payment.asset, expected_payment.amount_msat)),
            &self.retry,
        )
        .await
        .context("Lightning RGB payment was not confirmed before RGB accept")?;
        mark_pending_rgb_ln_assignment_payment_succeeded(
            &request.receiver_stock_dir,
            &assignment_receipt.assignment.envelope.transfer_id,
        )
        .context("failed to mark pending RGB-LN assignment payment_succeeded")?;
        backend
            .mark_rgb_payment_receiver_accepting(payment.payment_id)
            .context("failed to mark outbound RGB payment receiver accepting")?;

        accept_rgb20_transfer_bytes(
            &request.receiver_stock_dir,
            request.receiver_network,
            &request.receiver_esplora_url,
            &assignment_receipt.assignment.payload,
        )
        .context("failed to persist RGB20 transfer after Lightning payment")?;
        mark_pending_rgb_ln_assignment_accepted(
            &request.receiver_stock_dir,
            &assignment_receipt.assignment.envelope.transfer_id,
        )
        .context("failed to mark pending RGB-LN assignment accepted")?;
        backend
            .mark_rgb_payment_receiver_accepted(payment.payment_id)
            .context("failed to mark outbound RGB payment receiver accepted")?;

        Ok(RgbLnRgb20PaymentResult {
            assignment_ack,
            assignment_receipt,
            payment,
            pending_assignment_path: pending_record.payload_path,
        })
    }
}

pub fn recover_pending_rgb_ln_assignments(
    receiver_stock_dir: &Path,
    network: bitcoin::Network,
    esplora_url: &str,
) -> Result<RgbLnPendingRecoveryReport> {
    let dir = rgb_ln_pending_assignment_dir(receiver_stock_dir);
    if !dir.exists() {
        return Ok(RgbLnPendingRecoveryReport::default());
    }

    let mut report = RgbLnPendingRecoveryReport::default();
    for entry in fs::read_dir(&dir)
        .with_context(|| format!("read RGB-LN pending assignment dir {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        report.scanned += 1;
        let record = read_pending_rgb_ln_assignment_record(&path)?;
        if record.status != "payment_succeeded" && record.status != "accepting" {
            report.skipped += 1;
            continue;
        }
        write_pending_rgb_ln_assignment_record(
            &path,
            RgbLnPendingAssignmentRecord {
                status: "accepting".to_string(),
                updated_at: now_secs(),
                ..record.clone()
            },
        )?;
        let payload = fs::read(&record.payload_path).with_context(|| {
            format!(
                "read pending RGB-LN assignment payload {}",
                record.payload_path.display()
            )
        })?;
        accept_rgb20_transfer_bytes(receiver_stock_dir, network, esplora_url, &payload)
            .with_context(|| format!("recover pending RGB-LN assignment {}", record.transfer_id))?;
        write_pending_rgb_ln_assignment_record(
            &path,
            RgbLnPendingAssignmentRecord {
                status: "accepted".to_string(),
                updated_at: now_secs(),
                ..record
            },
        )?;
        report.recovered += 1;
    }
    Ok(report)
}

pub fn persist_pending_rgb_ln_assignment(
    receiver_stock_dir: &Path,
    receipt: &IrohAssignmentReceipt,
) -> Result<RgbLnPendingAssignmentRecord> {
    let dir = rgb_ln_pending_assignment_dir(receiver_stock_dir);
    fs::create_dir_all(&dir)
        .with_context(|| format!("create RGB-LN pending assignment dir {}", dir.display()))?;
    let transfer_id = sanitize_filename(&receipt.assignment.envelope.transfer_id);
    let payload_path = dir.join(format!("{transfer_id}.rgb-transfer"));
    let record_path = dir.join(format!("{transfer_id}.json"));
    fs::write(&payload_path, &receipt.assignment.payload).with_context(|| {
        format!(
            "write pending RGB-LN assignment payload {}",
            payload_path.display()
        )
    })?;
    let now = now_secs();
    let record = RgbLnPendingAssignmentRecord {
        transfer_id: receipt.assignment.envelope.transfer_id.clone(),
        contract_id: receipt.assignment.envelope.contract_id.clone(),
        recipient_outpoint: receipt.assignment.envelope.recipient_outpoint.clone(),
        payload_sha256: receipt.assignment.envelope.payload_sha256.clone(),
        payload_path,
        status: "validated".to_string(),
        created_at: now,
        updated_at: now,
    };
    fs::write(&record_path, serde_json::to_vec_pretty(&record)?).with_context(|| {
        format!(
            "write pending RGB-LN assignment record {}",
            record_path.display()
        )
    })?;
    Ok(record)
}

pub fn mark_pending_rgb_ln_assignment_accepted(
    receiver_stock_dir: &Path,
    transfer_id: &str,
) -> Result<RgbLnPendingAssignmentRecord> {
    update_pending_rgb_ln_assignment_status(receiver_stock_dir, transfer_id, "accepted")
}

pub fn mark_pending_rgb_ln_assignment_payment_succeeded(
    receiver_stock_dir: &Path,
    transfer_id: &str,
) -> Result<RgbLnPendingAssignmentRecord> {
    update_pending_rgb_ln_assignment_status(receiver_stock_dir, transfer_id, "payment_succeeded")
}

fn update_pending_rgb_ln_assignment_status(
    receiver_stock_dir: &Path,
    transfer_id: &str,
    status: &str,
) -> Result<RgbLnPendingAssignmentRecord> {
    let record_path = pending_rgb_ln_assignment_record_path(receiver_stock_dir, transfer_id);
    let mut record = read_pending_rgb_ln_assignment_record(&record_path)?;
    record.status = status.to_string();
    record.updated_at = now_secs();
    write_pending_rgb_ln_assignment_record(&record_path, record.clone())?;
    Ok(record)
}

fn read_pending_rgb_ln_assignment_record(path: &Path) -> Result<RgbLnPendingAssignmentRecord> {
    serde_json::from_slice(
        &fs::read(path)
            .with_context(|| format!("read pending RGB-LN assignment {}", path.display()))?,
    )
    .with_context(|| format!("decode pending RGB-LN assignment {}", path.display()))
}

fn write_pending_rgb_ln_assignment_record(
    path: &Path,
    record: RgbLnPendingAssignmentRecord,
) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(&record)?)
        .with_context(|| format!("write pending RGB-LN assignment record {}", path.display()))
}

fn pending_rgb_ln_assignment_record_path(receiver_stock_dir: &Path, transfer_id: &str) -> PathBuf {
    rgb_ln_pending_assignment_dir(receiver_stock_dir)
        .join(format!("{}.json", sanitize_filename(transfer_id)))
}

pub fn rgb_ln_pending_assignment_dir(receiver_stock_dir: &Path) -> PathBuf {
    receiver_stock_dir
        .parent()
        .unwrap_or(receiver_stock_dir)
        .join("ln-pending")
}

fn sanitize_filename(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '_',
        })
        .collect()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn hex32(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn validate_rgb_ln_rgb20_payment_request(request: &RgbLnRgb20PaymentRequest) -> Result<()> {
    anyhow::ensure!(
        request.payment.asset.contract_id == request.contract_id,
        "RGB-LN payment contract mismatch: consignment={}, payment={}",
        request.contract_id,
        request.payment.asset.contract_id
    );
    anyhow::ensure!(
        request.payment.asset.amount > 0,
        "RGB-LN payment asset amount must be greater than zero"
    );
    anyhow::ensure!(
        request.payment.amount_msat > 0,
        "RGB-LN payment amount_msat must be greater than zero"
    );
    Ok(())
}

pub fn validate_rgb_ln_payment_outpoint_matches_channel(
    request: &RgbLnRgb20PaymentRequest,
    funding_outpoint: OutPoint,
) -> Result<()> {
    anyhow::ensure!(
        request.recipient_outpoint == funding_outpoint,
        "RGB-LN payment recipient outpoint {} does not match channel {} funding outpoint {}",
        request.recipient_outpoint,
        hex32(request.channel_id.0),
        funding_outpoint
    );
    Ok(())
}

pub fn validate_rgb_ln_assignment_matches_channel(
    envelope: &IrohAssignmentEnvelope,
    request: &RgbLnRgb20PaymentRequest,
    funding_outpoint: OutPoint,
) -> Result<()> {
    let recipient_outpoint: OutPoint = envelope.recipient_outpoint.parse().with_context(|| {
        format!(
            "invalid RGB-LN assignment recipient_outpoint: {}",
            envelope.recipient_outpoint
        )
    })?;
    anyhow::ensure!(
        recipient_outpoint == funding_outpoint,
        "RGB-LN assignment recipient outpoint {recipient_outpoint} does not match channel {} funding outpoint {funding_outpoint}",
        hex32(request.channel_id.0)
    );
    anyhow::ensure!(
        envelope.txid == funding_outpoint.txid.to_string(),
        "RGB-LN assignment txid {} does not match channel funding txid {}",
        envelope.txid,
        funding_outpoint.txid
    );
    anyhow::ensure!(
        envelope.contract_id == request.contract_id.to_string(),
        "RGB-LN assignment contract {} does not match payment contract {}",
        envelope.contract_id,
        request.contract_id
    );
    Ok(())
}

pub fn rgb_ln_payment_request(
    recipient_node_id: PublicKey,
    contract_id: Rgb20ContractId,
    rgb_amount: u64,
    amount_msat: u64,
    payment_id: PaymentId,
) -> RgbPaymentRequest {
    RgbPaymentRequest {
        recipient_node_id,
        amount_msat,
        payment_id,
        asset: RgbAssetAmount {
            contract_id,
            amount: rgb_amount,
        },
    }
}

pub fn derive_rgb_ln_payment_id(
    sender_node_id: &PublicKey,
    recipient_node_id: &PublicKey,
    contract_id: Rgb20ContractId,
    rgb_amount: u64,
    amount_msat: u64,
    nonce: &[u8],
) -> PaymentId {
    let mut material = Vec::new();
    material.extend_from_slice(b"bihelix-rgb-ln-payment-v1");
    material.extend_from_slice(&sender_node_id.serialize());
    material.extend_from_slice(&recipient_node_id.serialize());
    material.extend_from_slice(contract_id.to_string().as_bytes());
    material.extend_from_slice(&rgb_amount.to_be_bytes());
    material.extend_from_slice(&amount_msat.to_be_bytes());
    material.extend_from_slice(nonce);
    PaymentId(sha256::Hash::hash(&material).to_byte_array())
}

pub async fn send_rgb_over_lightning<B>(
    backend: &B,
    payment: RgbPaymentRequest,
) -> Result<RgbLnDirectPaymentResult>
where
    B: RgbLnNode,
{
    backend.send_rgb_payment(payment.clone())?;
    Ok(RgbLnDirectPaymentResult {
        payment_id: payment.payment_id,
        contract_id: payment.asset.contract_id,
        rgb_amount: payment.asset.amount,
        amount_msat: payment.amount_msat,
    })
}

pub async fn wait_for_rgb_ln_payment_success<N>(
    node: &N,
    payment_id: PaymentId,
    retry: &RgbLnRetry,
) -> Result<()>
where
    N: BtcLnNode,
{
    wait_for_rgb_ln_payment_success_with_receiver(node, None, payment_id, retry).await
}

pub async fn wait_for_rgb_ln_payment_success_with_receiver<N>(
    node: &N,
    receiver_node: Option<&dyn BtcLnNode>,
    payment_id: PaymentId,
    retry: &RgbLnRetry,
) -> Result<()>
where
    N: BtcLnNode,
{
    wait_for_rgb_ln_payment_success_with_receiver_rgb(node, receiver_node, payment_id, None, retry)
        .await
}

async fn wait_for_rgb_ln_payment_success_with_receiver_rgb<N>(
    node: &N,
    receiver_node: Option<&dyn BtcLnNode>,
    payment_id: PaymentId,
    expected_receiver_rgb: Option<(&RgbAssetAmount, u64)>,
    retry: &RgbLnRetry,
) -> Result<()>
where
    N: BtcLnNode,
{
    let expected = hex32(payment_id.0);
    let expected_rgb_contract =
        expected_receiver_rgb.map(|(asset, _)| asset.contract_id.to_string());
    let expected_rgb_amount = expected_receiver_rgb.map(|(asset, _)| asset.amount);
    let expected_rgb_msat = expected_receiver_rgb.map(|(_, amount_msat)| amount_msat);
    let mut sender_succeeded = false;
    let mut receiver_rgb_received = expected_receiver_rgb.is_none() || receiver_node.is_none();
    for _ in 0..retry.attempts.max(1) {
        if let Some(receiver_node) = receiver_node {
            while receiver_node.next_event_debug().is_some() {
                receiver_node.event_handled()?;
            }
            while let Some(event) = receiver_node.next_btc_ln_event() {
                if let BtcLnEvent::RgbPaymentReceived {
                    amount_msat,
                    contract_id,
                    rgb_amount,
                    ..
                } = &event
                {
                    if expected_rgb_contract.as_deref() == Some(contract_id.as_str())
                        && expected_rgb_amount == Some(*rgb_amount)
                        && expected_rgb_msat == Some(*amount_msat)
                    {
                        receiver_rgb_received = true;
                    }
                }
                receiver_node.event_handled()?;
            }
        }
        while let Some(event) = node.next_btc_ln_event() {
            match event {
                BtcLnEvent::PaymentSuccessful {
                    payment_id: Some(id),
                } if id == expected => {
                    sender_succeeded = true;
                    node.event_handled()?;
                }
                BtcLnEvent::PaymentFailed {
                    payment_id: Some(id),
                } if id == expected => {
                    node.event_handled()?;
                    anyhow::bail!("RGB-LN payment failed: {id}");
                }
                _ => node.event_handled()?,
            }
        }
        if sender_succeeded && receiver_rgb_received {
            return Ok(());
        }
        tokio::time::sleep(retry.delay).await;
    }
    anyhow::bail!(
        "RGB-LN payment did not fully settle: payment_id={expected} sender_succeeded={sender_succeeded} receiver_rgb_received={receiver_rgb_received}"
    )
}

pub async fn execute_rgb_ln_direct_transfer<B>(
    backend: &B,
    request: RgbLnDirectTransferRequest,
) -> Result<RgbLnDirectTransferResult>
where
    B: RgbLnNode,
{
    let channel_id = if let Some(open_channel) = request.open_channel {
        Some(backend.open_rgb_channel(open_channel)?)
    } else {
        None
    };

    if let Some(funding_transfer) = request.funding_transfer {
        backend.provide_funding_transfer(funding_transfer)?;
    }

    let payment = send_rgb_over_lightning(backend, request.payment).await?;
    Ok(RgbLnDirectTransferResult {
        channel_id,
        payment,
    })
}

pub async fn establish_rgb_ln_session(
    sender: &LightningNodeConfig,
    receiver: &LightningNodeConfig,
    retry: RgbLnRetry,
) -> Result<RgbLnSession> {
    anyhow::ensure!(
        sender.network == receiver.network,
        "RGB-LN peers must use the same Bitcoin network: sender={:?}, receiver={:?}",
        sender.network,
        receiver.network
    );

    let receiver_endpoint =
        bind_iroh_endpoint_from_mnemonic(&receiver.mnemonic, receiver.network).await?;
    let receiver_addr = wait_for_dialable_addr(&receiver_endpoint).await;
    let sender_endpoint =
        bind_iroh_endpoint_from_mnemonic(&sender.mnemonic, sender.network).await?;

    let receiver_probe_endpoint = receiver_endpoint.clone();
    let attempts = retry.attempts;
    let delay = retry.delay;
    let receiver_probe = tokio::spawn(async move {
        receive_probe_with_retry(&receiver_probe_endpoint, attempts, delay).await
    });

    let sender_ack = probe_assignment_receiver_with_retry(
        &sender_endpoint,
        receiver_addr.clone(),
        retry.attempts,
        retry.delay,
    )
    .await
    .context("RGB-LN Iroh preflight send failed")?;
    let receiver_receipt = receiver_probe
        .await
        .context("RGB-LN Iroh preflight receiver task failed")?
        .context("RGB-LN Iroh preflight receiver failed")?;

    anyhow::ensure!(
        sender_ack.accepted && receiver_receipt.ack.accepted,
        "RGB-LN Iroh preflight was not accepted"
    );

    Ok(RgbLnSession {
        _receiver_endpoint: receiver_endpoint,
        preflight: RgbLnPreflight {
            sender_iroh_id: sender_endpoint.addr().id,
            receiver_iroh_id: receiver_addr.id,
            ack: sender_ack,
        },
        sender_endpoint,
        receiver_addr,
        retry,
    })
}

async fn wait_for_dialable_addr(endpoint: &Endpoint) -> EndpointAddr {
    for _ in 0..50 {
        let addr = endpoint.addr();
        if !addr.is_empty() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    endpoint.addr()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;

    use bitcoin::hashes::Hash as BitcoinHash;
    use bitcoin::secp256k1::PublicKey;

    use super::{
        hex32, recover_pending_rgb_ln_assignments, rgb_ln_payment_request,
        validate_rgb_ln_assignment_matches_channel,
        validate_rgb_ln_payment_outpoint_matches_channel, validate_rgb_ln_rgb20_payment_request,
        ChannelId, PaymentId, Rgb20ContractId, RgbLnRgb20PaymentRequest,
    };

    const CONTRACT_A: &str = "rgb:~pzYXTtW-IpYzNwp-sXb9pxZ-sz257_K-k0GyNQK-Y6AMO60";
    const NODE_ID: &str = "027620fe1c533336106f45958979f76b6e568fb39b4ae66ad7d1bb347d9a1707fe";

    #[test]
    fn validates_rgb_ln_payment_matches_consignment_contract() {
        let contract = CONTRACT_A.parse().expect("valid RGB contract");
        let request = payment_request(contract, contract, 42, 1_000);

        validate_rgb_ln_rgb20_payment_request(&request).expect("matching request is valid");
    }

    #[test]
    fn rejects_rgb_ln_payment_with_wrong_contract_or_zero_amount() {
        let contract = CONTRACT_A.parse().expect("valid RGB contract");
        let other_contract = Rgb20ContractId::from([2; 32]);

        let wrong_contract = payment_request(contract, other_contract, 42, 1_000);
        let err = validate_rgb_ln_rgb20_payment_request(&wrong_contract).unwrap_err();
        assert!(err.to_string().contains("contract mismatch"));

        let zero_rgb = payment_request(contract, contract, 0, 1_000);
        let err = validate_rgb_ln_rgb20_payment_request(&zero_rgb).unwrap_err();
        assert!(err.to_string().contains("asset amount"));

        let zero_msat = payment_request(contract, contract, 42, 0);
        let err = validate_rgb_ln_rgb20_payment_request(&zero_msat).unwrap_err();
        assert!(err.to_string().contains("amount_msat"));
    }

    #[test]
    fn validates_rgb_ln_payment_outpoint_against_channel_funding() {
        let contract = Rgb20ContractId::from_str(CONTRACT_A).expect("valid contract");
        let funding_outpoint = bitcoin::OutPoint::new(bitcoin::Txid::from_byte_array([2; 32]), 1);
        let mut request = payment_request(contract, contract, 42, 1_000);
        request.recipient_outpoint = funding_outpoint;
        validate_rgb_ln_payment_outpoint_matches_channel(&request, funding_outpoint)
            .expect("matching funding outpoint");

        let wrong_outpoint = bitcoin::OutPoint::new(bitcoin::Txid::from_byte_array([3; 32]), 0);
        let err =
            validate_rgb_ln_payment_outpoint_matches_channel(&request, wrong_outpoint).unwrap_err();
        assert!(err.to_string().contains("does not match channel"));
    }

    #[test]
    fn validates_rgb_ln_assignment_against_channel_funding() {
        let contract = Rgb20ContractId::from_str(CONTRACT_A).expect("valid contract");
        let funding_outpoint = bitcoin::OutPoint::new(bitcoin::Txid::from_byte_array([4; 32]), 2);
        let mut request = payment_request(contract, contract, 42, 1_000);
        request.recipient_outpoint = funding_outpoint;
        let envelope = crate::iroh_transport::IrohAssignmentEnvelope {
            version: 1,
            kind: "rgb20.transfer.consignment".to_string(),
            transfer_id: funding_outpoint.txid.to_string(),
            txid: funding_outpoint.txid.to_string(),
            contract_id: contract.to_string(),
            recipient_outpoint: funding_outpoint.to_string(),
            payload_len: 0,
            payload_sha256: hex32([5; 32]),
        };
        validate_rgb_ln_assignment_matches_channel(&envelope, &request, funding_outpoint)
            .expect("matching assignment");

        let mut wrong = envelope;
        wrong.recipient_outpoint =
            bitcoin::OutPoint::new(bitcoin::Txid::from_byte_array([6; 32]), 0).to_string();
        let err = validate_rgb_ln_assignment_matches_channel(&wrong, &request, funding_outpoint)
            .unwrap_err();
        assert!(err.to_string().contains("does not match channel"));
    }

    #[test]
    fn pending_recovery_noops_when_dir_is_missing() {
        let stock_dir = PathBuf::from(format!(
            "/private/tmp/btc-local-wallet-rgb-ln-missing-{}",
            std::process::id()
        ));
        let report = recover_pending_rgb_ln_assignments(
            &stock_dir,
            bitcoin::Network::Testnet,
            "https://blockstream.info/testnet/api",
        )
        .expect("missing recovery dir is ok");
        assert_eq!(report.scanned, 0);
        assert_eq!(report.recovered, 0);
        assert_eq!(report.skipped, 0);
    }

    fn payment_request(
        consignment_contract: Rgb20ContractId,
        payment_contract: Rgb20ContractId,
        rgb_amount: u64,
        amount_msat: u64,
    ) -> RgbLnRgb20PaymentRequest {
        RgbLnRgb20PaymentRequest {
            sender_stock_dir: PathBuf::from("/tmp/a-rgb-stock"),
            receiver_stock_dir: PathBuf::from("/tmp/b-rgb-stock"),
            receiver_network: bitcoin::Network::Testnet,
            receiver_esplora_url: "https://blockstream.info/testnet/api".to_string(),
            channel_id: ChannelId([9; 32]),
            contract_id: consignment_contract,
            recipient_outpoint: bitcoin::OutPoint::null(),
            payment: rgb_ln_payment_request(
                PublicKey::from_str(NODE_ID).expect("valid node id"),
                payment_contract,
                rgb_amount,
                amount_msat,
                PaymentId([1; 32]),
            ),
        }
    }
}
