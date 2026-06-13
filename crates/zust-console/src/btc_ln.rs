use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Result};
use bitcoin::{secp256k1::PublicKey, Network};
use lightning::ln::msgs::SocketAddress;
use lightning_invoice::{Bolt11Invoice, Bolt11InvoiceDescription};

#[derive(Clone, Debug)]
pub struct BtcLnChannelOpenRequest {
    pub peer_node_id: PublicKey,
    pub address: SocketAddress,
    pub amount_sats: u64,
    pub push_msat: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct BtcLnChannelCloseRequest {
    pub channel_id: String,
    pub counterparty_node_id: PublicKey,
    pub force: bool,
    pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BtcLnChannelSpliceRequest {
    pub channel_id: String,
    pub counterparty_node_id: PublicKey,
    pub amount_sats: i64,
    pub funding_feerate_per_kw: u32,
    pub locktime: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct BtcLnBolt11InvoiceRequest {
    pub amount_msat: u64,
    pub description: Bolt11InvoiceDescription,
    pub expiry_secs: u32,
}

#[derive(Clone, Debug)]
pub struct BtcLnBolt11PaymentRequest {
    pub invoice: Bolt11Invoice,
}

#[derive(Clone, Debug)]
pub struct BtcLnKeysendRequest {
    pub recipient_node_id: PublicKey,
    pub amount_msat: u64,
}

#[derive(Clone, Debug)]
pub struct BtcLnBalanceSnapshot {
    pub total_onchain_balance_sats: u64,
    pub spendable_onchain_balance_sats: u64,
    pub total_anchor_channels_reserve_sats: u64,
    pub total_lightning_balance_sats: u64,
    pub lightning_balances: String,
    pub pending_channel_closure_sweeps: String,
}

#[derive(Clone, Debug)]
pub struct BtcLnPeerSnapshot {
    pub node_id: PublicKey,
    pub address: SocketAddress,
    pub is_persisted: bool,
    pub is_connected: bool,
}

#[derive(Clone, Debug)]
pub struct BtcLnChannelSnapshot {
    pub user_channel_id: String,
    pub counterparty_node_id: PublicKey,
    pub channel_value_sats: u64,
    pub is_outbound: bool,
    pub is_channel_ready: bool,
    pub is_usable: bool,
    pub channel_id: String,
    pub outbound_capacity_msat: u64,
    pub next_outbound_htlc_limit_msat: u64,
    pub inbound_capacity_msat: u64,
    pub funding_txo: Option<String>,
}

#[derive(Clone, Debug)]
pub enum BtcLnEvent {
    PaymentSuccessful {
        payment_id: Option<String>,
    },
    PaymentFailed {
        payment_id: Option<String>,
    },
    PaymentReceived {
        payment_hash: Option<String>,
        amount_msat: u64,
    },
    RgbPaymentReceived {
        payment_hash: Option<String>,
        amount_msat: u64,
        contract_id: String,
        rgb_amount: u64,
    },
    Other,
}

#[derive(Clone, Debug)]
pub struct BtcLnRuntimeConfig {
    pub backend: BtcLnBackendKind,
    pub network: Network,
    pub l1_data_dir: PathBuf,
    pub storage_dir: PathBuf,
    pub esplora: String,
    pub esplora_urls: Vec<String>,
    pub esplora_api_key: Option<String>,
    pub rgb_service_url: String,
    pub account_id: String,
    pub listen: Option<String>,
    pub entropy_mnemonic: Option<String>,
    pub trusted_peers_0conf: Vec<String>,
    pub accept_inbound_channels: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BtcLnBackendKind {
    LnRgb,
}

impl BtcLnBackendKind {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "ln-rgb" | "ln_rgb" | "rust-lightning" | "rust_lightning" => Ok(Self::LnRgb),
            _ => bail!("unsupported LN backend `{value}`; use ln-rgb"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::LnRgb => "ln-rgb",
        }
    }
}

pub trait BtcLnNode {
    fn start(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;
    fn node_id(&self) -> PublicKey;
    fn status_summary(&self) -> String;
    fn listening_addresses(&self) -> Option<Vec<SocketAddress>>;
    fn announcement_addresses(&self) -> Option<Vec<SocketAddress>>;
    fn next_event_debug(&self) -> Option<String>;
    fn next_btc_ln_event(&self) -> Option<BtcLnEvent>;
    fn event_handled(&self) -> Result<()>;

    fn balance_snapshot(&self) -> BtcLnBalanceSnapshot;
    fn peer_snapshots(&self) -> Vec<BtcLnPeerSnapshot>;
    fn channel_snapshots(&self) -> Vec<BtcLnChannelSnapshot>;

    fn connect(&self, node_id: PublicKey, address: SocketAddress, persist: bool) -> Result<()>;
    fn open_channel(&self, request: BtcLnChannelOpenRequest) -> Result<String>;
    fn close_channel(&self, request: BtcLnChannelCloseRequest) -> Result<()>;
    fn splice_channel(&self, request: BtcLnChannelSpliceRequest) -> Result<()>;
    fn receive_bolt11(&self, request: BtcLnBolt11InvoiceRequest) -> Result<Bolt11Invoice>;
    fn pay_bolt11(&self, request: BtcLnBolt11PaymentRequest) -> Result<String>;
    fn send_keysend(&self, request: BtcLnKeysendRequest) -> Result<String>;
}

impl<T> BtcLnNode for Box<T>
where
    T: BtcLnNode + ?Sized,
{
    fn start(&self) -> Result<()> {
        (**self).start()
    }

    fn stop(&self) -> Result<()> {
        (**self).stop()
    }

    fn node_id(&self) -> PublicKey {
        (**self).node_id()
    }

    fn status_summary(&self) -> String {
        (**self).status_summary()
    }

    fn listening_addresses(&self) -> Option<Vec<SocketAddress>> {
        (**self).listening_addresses()
    }

    fn announcement_addresses(&self) -> Option<Vec<SocketAddress>> {
        (**self).announcement_addresses()
    }

    fn next_event_debug(&self) -> Option<String> {
        (**self).next_event_debug()
    }

    fn next_btc_ln_event(&self) -> Option<BtcLnEvent> {
        (**self).next_btc_ln_event()
    }

    fn event_handled(&self) -> Result<()> {
        (**self).event_handled()
    }

    fn balance_snapshot(&self) -> BtcLnBalanceSnapshot {
        (**self).balance_snapshot()
    }

    fn peer_snapshots(&self) -> Vec<BtcLnPeerSnapshot> {
        (**self).peer_snapshots()
    }

    fn channel_snapshots(&self) -> Vec<BtcLnChannelSnapshot> {
        (**self).channel_snapshots()
    }

    fn connect(&self, node_id: PublicKey, address: SocketAddress, persist: bool) -> Result<()> {
        (**self).connect(node_id, address, persist)
    }

    fn open_channel(&self, request: BtcLnChannelOpenRequest) -> Result<String> {
        (**self).open_channel(request)
    }

    fn close_channel(&self, request: BtcLnChannelCloseRequest) -> Result<()> {
        (**self).close_channel(request)
    }

    fn splice_channel(&self, request: BtcLnChannelSpliceRequest) -> Result<()> {
        (**self).splice_channel(request)
    }

    fn receive_bolt11(&self, request: BtcLnBolt11InvoiceRequest) -> Result<Bolt11Invoice> {
        (**self).receive_bolt11(request)
    }

    fn pay_bolt11(&self, request: BtcLnBolt11PaymentRequest) -> Result<String> {
        (**self).pay_bolt11(request)
    }

    fn send_keysend(&self, request: BtcLnKeysendRequest) -> Result<String> {
        (**self).send_keysend(request)
    }
}

impl<T> BtcLnNode for Arc<T>
where
    T: BtcLnNode + ?Sized,
{
    fn start(&self) -> Result<()> {
        (**self).start()
    }

    fn stop(&self) -> Result<()> {
        (**self).stop()
    }

    fn node_id(&self) -> PublicKey {
        (**self).node_id()
    }

    fn status_summary(&self) -> String {
        (**self).status_summary()
    }

    fn listening_addresses(&self) -> Option<Vec<SocketAddress>> {
        (**self).listening_addresses()
    }

    fn announcement_addresses(&self) -> Option<Vec<SocketAddress>> {
        (**self).announcement_addresses()
    }

    fn next_event_debug(&self) -> Option<String> {
        (**self).next_event_debug()
    }

    fn next_btc_ln_event(&self) -> Option<BtcLnEvent> {
        (**self).next_btc_ln_event()
    }

    fn event_handled(&self) -> Result<()> {
        (**self).event_handled()
    }

    fn balance_snapshot(&self) -> BtcLnBalanceSnapshot {
        (**self).balance_snapshot()
    }

    fn peer_snapshots(&self) -> Vec<BtcLnPeerSnapshot> {
        (**self).peer_snapshots()
    }

    fn channel_snapshots(&self) -> Vec<BtcLnChannelSnapshot> {
        (**self).channel_snapshots()
    }

    fn connect(&self, node_id: PublicKey, address: SocketAddress, persist: bool) -> Result<()> {
        (**self).connect(node_id, address, persist)
    }

    fn open_channel(&self, request: BtcLnChannelOpenRequest) -> Result<String> {
        (**self).open_channel(request)
    }

    fn close_channel(&self, request: BtcLnChannelCloseRequest) -> Result<()> {
        (**self).close_channel(request)
    }

    fn splice_channel(&self, request: BtcLnChannelSpliceRequest) -> Result<()> {
        (**self).splice_channel(request)
    }

    fn receive_bolt11(&self, request: BtcLnBolt11InvoiceRequest) -> Result<Bolt11Invoice> {
        (**self).receive_bolt11(request)
    }

    fn pay_bolt11(&self, request: BtcLnBolt11PaymentRequest) -> Result<String> {
        (**self).pay_bolt11(request)
    }

    fn send_keysend(&self, request: BtcLnKeysendRequest) -> Result<String> {
        (**self).send_keysend(request)
    }
}
