use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bdk_wallet::bitcoin::Network;
use bdk_wallet::keys::bip39::{Language as BdkLanguage, Mnemonic as BdkMnemonic};
use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::SocketAddress;

use crate::btc_ln::{BtcLnBackendKind, BtcLnRuntimeConfig};
use crate::ln_rgb_btc_ln_backend::LnRgbBtcLnBackend;
use crate::local_wallet::{EsploraConfig, LocalWallet};

#[derive(Clone, Debug)]
pub struct LightningNodeConfig {
    pub network: Network,
    pub data_dir: PathBuf,
    pub ldk_data_dir: PathBuf,
    pub esplora: String,
    pub esplora_urls: Vec<String>,
    pub esplora_api_key: Option<String>,
    pub rgb_service_url: String,
    pub account_id: String,
    pub mnemonic: String,
    pub ln_backend: BtcLnBackendKind,
    pub listen: Option<String>,
    pub peers: Vec<LightningPeerConfig>,
    pub trusted_peers_0conf: Vec<String>,
    pub accept_inbound_channels: bool,
    pub announce_for_forwarding: bool,
}

#[derive(Clone, Debug)]
pub struct LightningPeerConfig {
    pub node_id: PublicKey,
    pub address: SocketAddress,
}

pub fn open_local_wallet_from_config(config: &LightningNodeConfig) -> Result<LocalWallet> {
    let mnemonic = BdkMnemonic::parse_in_normalized(BdkLanguage::English, &config.mnemonic)
        .context("invalid mnemonic from zs config")?;
    LocalWallet::open_with_mnemonic(&config.data_dir, config.network, &mnemonic)
}

pub fn build_rgb_ln_node_from_config(
    config: &LightningNodeConfig,
) -> Result<Arc<LnRgbBtcLnBackend>> {
    if config.ln_backend != BtcLnBackendKind::LnRgb {
        bail!(
            "RGB-LN flow requires ln_backend=ln-rgb; got {}",
            config.ln_backend.as_str()
        );
    }
    Ok(LnRgbBtcLnBackend::new_arc(btc_ln_runtime_config(config)))
}

fn btc_ln_runtime_config(config: &LightningNodeConfig) -> BtcLnRuntimeConfig {
    BtcLnRuntimeConfig {
        network: config.network,
        backend: config.ln_backend,
        l1_data_dir: config.data_dir.clone(),
        storage_dir: config.ldk_data_dir.clone(),
        esplora: config.esplora.clone(),
        esplora_urls: config.esplora_urls.clone(),
        esplora_api_key: config.esplora_api_key.clone(),
        rgb_service_url: config.rgb_service_url.clone(),
        account_id: config.account_id.clone(),
        listen: config.listen.clone(),
        entropy_mnemonic: Some(config.mnemonic.clone()),
        trusted_peers_0conf: config.trusted_peers_0conf.clone(),
        accept_inbound_channels: config.accept_inbound_channels,
        announce_for_forwarding: config.announce_for_forwarding,
    }
}

impl LightningNodeConfig {
    pub fn esplora_config_for_url(&self, url: String) -> EsploraConfig {
        EsploraConfig::new(url).with_api_key(self.esplora_api_key.clone())
    }
}

pub fn parse_lightning_peer(peer: &str) -> Result<LightningPeerConfig> {
    let (node_id, address) = peer
        .split_once('@')
        .with_context(|| format!("peer must be formatted as <node_id>@<host>:<port>: {peer}"))?;
    let node_id =
        PublicKey::from_str(node_id).with_context(|| format!("invalid peer node id: {node_id}"))?;
    let address = SocketAddress::from_str(address)
        .map_err(|_| anyhow::anyhow!("invalid peer address: {address}"))?;
    Ok(LightningPeerConfig { node_id, address })
}

#[cfg(test)]
mod tests {
    use super::parse_lightning_peer;

    #[test]
    fn parses_lightning_peer_from_zs_style_string() {
        let peer = parse_lightning_peer(
            "027620fe1c533336106f45958979f76b6e568fb39b4ae66ad7d1bb347d9a1707fe@127.0.0.1:9736",
        )
        .expect("valid peer");

        assert_eq!(peer.address.to_string(), "127.0.0.1:9736");
        assert_eq!(
            peer.node_id.to_string(),
            "027620fe1c533336106f45958979f76b6e568fb39b4ae66ad7d1bb347d9a1707fe"
        );
    }
}
