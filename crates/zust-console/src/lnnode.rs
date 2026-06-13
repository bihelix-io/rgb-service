use std::sync::Arc;

use anyhow::Result;
use bitcoin::secp256k1::PublicKey;
use bitcoin::OutPoint;
use lightning::rgb::RgbFundingRef;
use rgbstd::ContractId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ChannelId(pub [u8; 32]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct PaymentId(pub [u8; 32]);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RgbAssetAmount {
    pub contract_id: ContractId,
    pub amount: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RgbChannelOpenRequest {
    pub peer_node_id: PublicKey,
    pub capacity_sat: u64,
    pub push_msat: u64,
    pub user_channel_id: u128,
    pub asset: RgbAssetAmount,
}

#[derive(Clone, Debug)]
pub struct RgbFundingTransfer {
    pub temporary_channel_id: ChannelId,
    pub peer_node_id: PublicKey,
    pub funding_outpoint: OutPoint,
    pub funding_ref: RgbFundingRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RgbPaymentRequest {
    pub recipient_node_id: PublicKey,
    pub amount_msat: u64,
    pub payment_id: PaymentId,
    pub asset: RgbAssetAmount,
}

pub trait RgbLnNode {
    fn open_rgb_channel(&self, request: RgbChannelOpenRequest) -> Result<ChannelId>;

    fn provide_funding_transfer(&self, funding: RgbFundingTransfer) -> Result<()>;

    fn send_rgb_payment(&self, request: RgbPaymentRequest) -> Result<()>;

    fn mark_rgb_payment_receiver_accepting(&self, _payment_id: PaymentId) -> Result<()> {
        Ok(())
    }

    fn mark_rgb_payment_receiver_accepted(&self, _payment_id: PaymentId) -> Result<()> {
        Ok(())
    }
}

impl<T> RgbLnNode for Arc<T>
where
    T: RgbLnNode + ?Sized,
{
    fn open_rgb_channel(&self, request: RgbChannelOpenRequest) -> Result<ChannelId> {
        self.as_ref().open_rgb_channel(request)
    }

    fn provide_funding_transfer(&self, funding: RgbFundingTransfer) -> Result<()> {
        self.as_ref().provide_funding_transfer(funding)
    }

    fn send_rgb_payment(&self, request: RgbPaymentRequest) -> Result<()> {
        self.as_ref().send_rgb_payment(request)
    }

    fn mark_rgb_payment_receiver_accepting(&self, payment_id: PaymentId) -> Result<()> {
        self.as_ref()
            .mark_rgb_payment_receiver_accepting(payment_id)
    }

    fn mark_rgb_payment_receiver_accepted(&self, payment_id: PaymentId) -> Result<()> {
        self.as_ref().mark_rgb_payment_receiver_accepted(payment_id)
    }
}
