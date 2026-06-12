//! Stable Lightning facade for BiHelix integration code.
//!
//! This module intentionally does not depend on `rgb-service-daemon`. It is a
//! narrow re-export layer over the local `ln-rgb-lightning` fork, so callers can
//! import the LN node surface from one place instead of depending on many
//! internal module paths.

#![allow(missing_docs)]

pub use bitcoin::{
    absolute::LockTime, block::Header, constants::ChainHash, network::Network, secp256k1,
    secp256k1::PublicKey, transaction::Transaction, Amount, BlockHash, OutPoint, ScriptBuf, Txid,
};

pub use lightning_invoice::{
    Bolt11Invoice, Bolt11InvoiceDescription, Currency, Description, InvoiceBuilder,
};

pub use crate::chain::{BestBlock, ChannelMonitorUpdateStatus, Confirm, Listen, Watch};
pub use crate::chain::chaininterface::{
    BroadcasterInterface, ConfirmationTarget, FeeEstimator,
};
pub use crate::chain::channelmonitor::{
    Balance as ChannelBalance, ChannelMonitor, ChannelMonitorUpdate,
};
pub use crate::chain::chainmonitor::{ChainMonitor, Persist};
pub use crate::events::{
    ClosureReason, Event, EventHandler, EventsProvider, PaymentFailureReason,
};
pub use crate::ln::channel_state::{ChannelDetails, ChannelShutdownState};
pub use crate::ln::channelmanager::{
    AChannelManager, Bolt11PaymentError, Bolt12PaymentError, ChainParameters, ChannelManager,
    PaymentId, RecipientOnionFields, Retry, RetryableSendFailure,
    SimpleArcChannelManager, SimpleRefChannelManager,
};
pub use crate::ln::msgs::{
    BaseMessageHandler, ChannelMessageHandler, Init, LightningError, MessageSendEvent,
    OnionMessageHandler, RoutingMessageHandler, SocketAddress,
};
pub use crate::ln::peer_handler::{
    ErroringMessageHandler, IgnoringMessageHandler, MessageHandler, PeerDetails, PeerManager,
    SocketDescriptor, APeerManager, SimpleArcPeerManager, SimpleRefPeerManager,
};
pub use crate::ln::types::ChannelId;
pub use crate::offers::{
    invoice::Bolt12Invoice,
    invoice_request::InvoiceRequest,
    offer::{Offer, OfferBuilder},
    refund::{Refund, RefundBuilder},
};
pub use crate::onion_message::messenger::{
    DefaultMessageRouter, Destination, MessageRouter, MessageSendInstructions, OnionMessenger,
};
pub use crate::routing::{
    gossip::{NetworkGraph, NodeAlias, NodeId, P2PGossipSync},
    router::{
        DefaultRouter, InFlightHtlcs, PaymentParameters, Route, RouteParameters,
        RouteParametersConfig, Router,
    },
    scoring::{
        ProbabilisticScorer, ProbabilisticScoringDecayParameters,
        ProbabilisticScoringFeeParameters,
    },
};
pub use crate::sign::{
    EntropySource, InMemorySigner, KeysManager, NodeSigner, OutputSpender, Recipient,
    SignerProvider,
};
pub use crate::types::{
    features::{
        Bolt11InvoiceFeatures, Bolt12InvoiceFeatures, ChannelFeatures, ChannelTypeFeatures,
        InitFeatures, InvoiceRequestFeatures, NodeFeatures, OfferFeatures,
    },
    payment::{PaymentHash, PaymentPreimage, PaymentSecret},
};
pub use crate::util::{
    config::{ChannelConfig, ChannelConfigOverrides, ChannelConfigUpdate, UserConfig},
    errors::APIError,
    logger::{Level, Logger, Record},
    persist::{KVStore, MonitorUpdatingPersister, MonitorUpdatingPersisterAsync},
    ser::{Readable, ReadableArgs, Writeable},
};
