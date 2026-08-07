use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, SyncSender, TrySendError},
    Arc, Mutex, OnceLock,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::btc_ln::{
    BtcLnBackendKind, BtcLnBolt11InvoiceRequest, BtcLnBolt11PaymentRequest,
    BtcLnChannelCloseRequest, BtcLnChannelOpenRequest, BtcLnChannelSpliceRequest, BtcLnNode,
    BtcLnRuntimeConfig,
};
use crate::ln_rgb_btc_ln_backend::LnRgbBtcLnBackend;
use crate::lnnode::{
    PaymentId as WalletPaymentId, RgbAssetAmount as WalletRgbAssetAmount, RgbChannelOpenRequest,
    RgbLnNode, RgbPaymentRequest,
};
use crate::local_wallet::LocalWallet;
use crate::node_store::LocalNodeStore;
use anyhow::{bail, ensure, Context, Result};
use bdk_wallet::keys::bip39::{Language as BdkLanguage, Mnemonic as BdkMnemonic};
use bdk_wallet::KeychainKind;
use bip39::{Language as Bip39Language, Mnemonic as Bip39Mnemonic};
use bitcoin::{
    absolute::LockTime,
    bip32::{ChildNumber, DerivationPath, Fingerprint, Xpub},
    consensus::encode,
    hashes::{sha256, Hash},
    psbt::Psbt,
    secp256k1::PublicKey,
    transaction::Version,
    Address, Amount, CompressedPublicKey, Network, NetworkKind, OutPoint, ScriptBuf, Sequence,
    Transaction, TxIn, TxOut, Witness,
};
use dynamic::{Dynamic, FromJson, ToJson, Type};
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};
use lightning::ln::msgs::SocketAddress;
use lightning_invoice::{Bolt11Invoice, Bolt11InvoiceDescription, Description};
use serde_json::{json, Map, Value};
use std::str::FromStr;
use vm::{Vm, ZustCallback};

const LN_SCAN_DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
const LN_NODE_DEFAULT_PATH: &str = ".zust-console/ln-node.json";
const LN_DATA_DIR_DEFAULT: &str = ".zust-console/lightning";
const LN_LDK_DATA_DIR_DEFAULT: &str = ".zust-console/lightning/ldk";
const LN_LISTEN_DEFAULT: &str = "0.0.0.0:9736";
const LN_ESPLORA_DEFAULT: &str = "https://blockstream.info/api";
const BTC_ESPLORA_CONNECT_TIMEOUT_SECS: u64 = 5;
const BTC_ESPLORA_REQUEST_TIMEOUT_SECS: u64 = 15;
const BTC_ESPLORA_POOL_IDLE_TIMEOUT_SECS: u64 = 90;
const BTC_ESPLORA_POOL_MAX_IDLE_PER_HOST: usize = 4;
const LN_LOW_WATER_SATS: u64 = 100_000;
const BTC_ADDRESS_POOL_LOW_WATER: usize = 5;
const BTC_ADDRESS_POOL_TARGET: usize = 20;
const BTC_ADDRESS_XPUB_DEFAULT_PATH: &str = "0/*";
const BTC_ADDRESS_XPUB_LOOKUP_LIMIT: u32 = 20_000;
const BTC_ADDRESS_XPUB_LOOKUP_MARGIN: u32 = 1_000;
const RGB_CALLBACK_WORKER_COUNT: usize = 4;
const RGB_CALLBACK_QUEUE_CAPACITY: usize = 64;
static ESPLORA_HTTP_CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
static RGB_SERVICE_HTTP_CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
static LN_RGB_NODE: OnceLock<Mutex<Option<Arc<LnRgbBtcLnBackend>>>> = OnceLock::new();
type RgbCallbackJob = Box<dyn FnOnce() + Send + 'static>;
static RGB_CALLBACK_QUEUE: OnceLock<std::result::Result<SyncSender<RgbCallbackJob>, String>> =
    OnceLock::new();
static LN_STARTED: AtomicBool = AtomicBool::new(false);
static LN_SCANNER_STARTED: AtomicBool = AtomicBool::new(false);

pub(crate) fn daemon_url() -> Result<String> {
    let daemon_url =
        local_string("rgb-service").context("missing root value `local/rgb-service`")?;
    ensure!(
        daemon_url.starts_with("http://") || daemon_url.starts_with("https://"),
        "local/rgb-service must start with http:// or https://"
    );
    Ok(daemon_url)
}

fn local_string(name: &str) -> Option<String> {
    root::get(&format!("local/{name}"))
        .ok()
        .map(|value| value.as_str().to_string())
        .filter(|value| !value.trim().is_empty())
}

fn local_dynamic(name: &str) -> Option<Dynamic> {
    root::get(&format!("local/{name}"))
        .ok()
        .filter(|value| !matches!(value, Dynamic::Null))
}

#[derive(Clone, Debug)]
struct BtcAddressXpubConfig {
    xpub: Xpub,
    xpub_fingerprint: String,
    signing_fingerprint: Fingerprint,
    signing_fingerprint_configured: bool,
    account_path: Vec<ChildNumber>,
    account_path_text: String,
    source_format: String,
    address_type: String,
    path_template: String,
    start_index: u32,
}

fn btc_address_xpub_config() -> Result<Option<BtcAddressXpubConfig>> {
    let object_config =
        local_dynamic("btc-address-xpub-config").map(|value| dynamic_to_json(&value));
    let xpub_text = object_config
        .as_ref()
        .and_then(|config| {
            find_string_field(config, &["xpub", "public_key", "extended_public_key"])
        })
        .or_else(|| local_string("btc-address-xpub"))
        .or_else(|| local_string("btc-addr-xpub"))
        .or_else(|| local_string("btc-xpub"))
        .or_else(|| local_string("btc/address_xpub"));
    let Some(xpub_text) = xpub_text else {
        return Ok(None);
    };

    let xpub_text = xpub_text.trim();
    ensure!(!xpub_text.is_empty(), "BTC address xpub must not be empty");
    let (xpub, inferred_type, source_format) = parse_btc_address_xpub(xpub_text)?;
    let address_type = object_config
        .as_ref()
        .and_then(|config| find_string_field(config, &["address_type", "script_type", "type"]))
        .or_else(|| local_string("btc-address-xpub-type"))
        .or_else(|| local_string("btc-addr-xpub-type"))
        .unwrap_or(inferred_type)
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_");
    ensure!(
        matches!(
            address_type.as_str(),
            "p2wpkh"
                | "native_segwit"
                | "segwit"
                | "p2shwpkh"
                | "nested_segwit"
                | "legacy"
                | "p2pkh"
                | "taproot"
                | "p2tr"
        ),
        "unsupported BTC address xpub address_type `{address_type}`"
    );
    let path_template = object_config
        .as_ref()
        .and_then(|config| find_string_field(config, &["path", "derive_path", "derivation_path"]))
        .or_else(|| local_string("btc-address-xpub-path"))
        .or_else(|| local_string("btc-addr-xpub-path"))
        .unwrap_or_else(|| BTC_ADDRESS_XPUB_DEFAULT_PATH.to_string());
    let start_index = object_config
        .as_ref()
        .and_then(|config| find_u64_field(config, &["start_index", "index_start"]))
        .or_else(|| {
            local_string("btc-address-xpub-start-index").and_then(|value| value.parse().ok())
        })
        .unwrap_or(0);
    ensure!(
        start_index <= u32::MAX as u64,
        "BTC address xpub start_index exceeds u32"
    );
    let account_path_text = object_config
        .as_ref()
        .and_then(|config| find_string_field(config, &["account_path", "base_path", "origin_path"]))
        .or_else(|| local_string("btc-address-xpub-account-path"))
        .or_else(|| local_string("btc-addr-xpub-account-path"))
        .unwrap_or_default();
    let account_path = parse_bip32_path_prefix(&account_path_text)?;
    let signing_fingerprint_text = object_config
        .as_ref()
        .and_then(|config| {
            find_string_field(
                config,
                &["master_fingerprint", "root_fingerprint", "fingerprint"],
            )
        })
        .or_else(|| local_string("btc-address-xpub-master-fingerprint"))
        .or_else(|| local_string("btc-addr-xpub-master-fingerprint"));
    let signing_fingerprint_configured = signing_fingerprint_text.is_some();
    let signing_fingerprint = match signing_fingerprint_text {
        Some(text) => Fingerprint::from_str(text.trim()).with_context(|| {
            format!(
                "parse BTC address xpub master fingerprint `{}`",
                text.trim()
            )
        })?,
        None => Fingerprint::from_str(&xpub.fingerprint().to_string())
            .context("parse BTC address xpub fingerprint")?,
    };

    Ok(Some(BtcAddressXpubConfig {
        xpub,
        xpub_fingerprint: xpub.fingerprint().to_string(),
        signing_fingerprint,
        signing_fingerprint_configured,
        account_path_text: xpub_derivation_path_text(&account_path),
        account_path,
        source_format,
        address_type,
        path_template,
        start_index: start_index as u32,
    }))
}

fn parse_btc_address_xpub(value: &str) -> Result<(Xpub, String, String)> {
    let trimmed = value.trim();
    let decoded = bitcoin::base58::decode_check(trimmed)
        .with_context(|| "decode BTC address xpub base58check")?;
    ensure!(
        decoded.len() == 78,
        "BTC address xpub payload length must be 78 bytes"
    );
    let version = [decoded[0], decoded[1], decoded[2], decoded[3]];
    let (canonical_version, inferred_type, source_format) = match version {
        [0x04, 0x88, 0xb2, 0x1e] => ([0x04, 0x88, 0xb2, 0x1e], "p2wpkh", "xpub"),
        [0x04, 0x35, 0x87, 0xcf] => ([0x04, 0x35, 0x87, 0xcf], "p2wpkh", "tpub"),
        [0x04, 0x9d, 0x7c, 0xb2] => ([0x04, 0x88, 0xb2, 0x1e], "p2shwpkh", "ypub"),
        [0x04, 0x4a, 0x52, 0x62] => ([0x04, 0x35, 0x87, 0xcf], "p2shwpkh", "upub"),
        [0x04, 0xb2, 0x47, 0x46] => ([0x04, 0x88, 0xb2, 0x1e], "p2wpkh", "zpub"),
        [0x04, 0x5f, 0x1c, 0xf6] => ([0x04, 0x35, 0x87, 0xcf], "p2wpkh", "vpub"),
        [0x02, 0x95, 0xb4, 0x3f] | [0x02, 0x42, 0x89, 0xef] => {
            bail!("multisig ypub/upub BTC address xpub formats are not supported")
        }
        [0x02, 0xaa, 0x7e, 0xd3] | [0x02, 0x57, 0x54, 0x83] => {
            bail!("multisig zpub/vpub BTC address xpub formats are not supported")
        }
        _ => {
            let parsed = Xpub::from_str(trimmed).context("parse BTC address xpub")?;
            return Ok((parsed, "p2wpkh".to_string(), "xpub".to_string()));
        }
    };
    let mut canonical = decoded;
    canonical[0..4].copy_from_slice(&canonical_version);
    let xpub = Xpub::decode(&canonical).context("parse BTC address xpub payload")?;
    Ok((xpub, inferred_type.to_string(), source_format.to_string()))
}

fn derive_btc_address_from_xpub(
    config: &BtcAddressXpubConfig,
    index: u32,
) -> Result<(String, String)> {
    let path = xpub_derivation_path(&config.path_template, index)?;
    let concrete_path = xpub_derivation_path_text(&path);
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let derived = config
        .xpub
        .derive_pub(&secp, &path)
        .with_context(|| format!("derive BTC address xpub path {concrete_path}"))?;
    let network = parse_ln_network(&ln_rgb_network_name())?;
    let network_kind = NetworkKind::from(network);
    ensure!(
        derived.xpub_network_matches(network),
        "BTC address xpub network does not match configured network {network:?}"
    );
    let compressed = CompressedPublicKey(derived.public_key);
    let address = match config.address_type.as_str() {
        "p2wpkh" | "native_segwit" | "segwit" => Address::p2wpkh(&compressed, network),
        "p2shwpkh" | "nested_segwit" => Address::p2shwpkh(&compressed, network_kind),
        "legacy" | "p2pkh" => Address::p2pkh(compressed, network_kind),
        "taproot" | "p2tr" => Address::p2tr(&secp, compressed.into(), None, network),
        _ => bail!(
            "unsupported BTC address xpub address_type `{}`",
            config.address_type
        ),
    };
    Ok((address.to_string(), concrete_path))
}

trait XpubNetworkExt {
    fn xpub_network_matches(&self, network: Network) -> bool;
}

impl XpubNetworkExt for Xpub {
    fn xpub_network_matches(&self, network: Network) -> bool {
        self.network == NetworkKind::from(network)
    }
}

fn xpub_derivation_path(template: &str, index: u32) -> Result<Vec<ChildNumber>> {
    let mut path = Vec::new();
    let trimmed = template.trim().trim_start_matches("m/").trim_matches('/');
    let template = if trimmed.is_empty() { "*" } else { trimmed };
    let mut saw_wildcard = false;
    for segment in template.split('/') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        ensure!(
            !segment.ends_with('\'') && !segment.ends_with('h') && !segment.ends_with('H'),
            "BTC address xpub derivation path cannot contain hardened segment `{segment}`"
        );
        let child_index = if segment == "*" {
            saw_wildcard = true;
            index
        } else {
            segment
                .parse::<u32>()
                .with_context(|| format!("invalid BTC address xpub path segment `{segment}`"))?
        };
        path.push(ChildNumber::Normal { index: child_index });
    }
    ensure!(
        saw_wildcard,
        "BTC address xpub derivation path must contain `*` placeholder"
    );
    Ok(path)
}

fn xpub_derivation_path_text(path: &[ChildNumber]) -> String {
    path.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("/")
}

fn parse_bip32_path_prefix(path: &str) -> Result<Vec<ChildNumber>> {
    let mut parsed = Vec::new();
    let trimmed = path.trim().trim_start_matches("m/").trim_matches('/');
    if trimmed.is_empty() || trimmed == "m" {
        return Ok(parsed);
    }
    for segment in trimmed.split('/') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        parsed.push(ChildNumber::from_str(segment).with_context(|| {
            format!("invalid BTC address xpub account path segment `{segment}`")
        })?);
    }
    Ok(parsed)
}

fn xpub_signing_derivation_path(
    config: &BtcAddressXpubConfig,
    child_path: &[ChildNumber],
) -> DerivationPath {
    let mut full_path = config.account_path.clone();
    full_path.extend_from_slice(child_path);
    DerivationPath::from(full_path)
}

fn existing_btc_address_set(store: &LocalNodeStore) -> Result<std::collections::BTreeSet<String>> {
    Ok(store
        .list_btc_address_pool_records()?
        .into_iter()
        .map(|(address, _)| address)
        .chain(
            store
                .list_used_btc_address_pool_records()?
                .into_iter()
                .map(|(address, _)| address),
        )
        .chain(
            store
                .list_ident_btc_addresses()?
                .into_iter()
                .map(|(_, address)| address),
        )
        .collect())
}

fn next_btc_xpub_index(store: &LocalNodeStore, config: &BtcAddressXpubConfig) -> Result<u32> {
    let max_seen = store
        .list_btc_address_pool_records()?
        .into_iter()
        .chain(store.list_used_btc_address_pool_records()?)
        .filter_map(|(_, record)| {
            (record.get("source").and_then(Value::as_str) == Some("xpub")).then_some(record)
        })
        .filter(|record| {
            record.get("xpub_fingerprint").and_then(Value::as_str)
                == Some(config.xpub_fingerprint.as_str())
                && record.get("derivation_path").and_then(Value::as_str)
                    == Some(config.path_template.as_str())
        })
        .filter_map(|record| record.get("derivation_index").and_then(Value::as_u64))
        .max();
    Ok(max_seen
        .and_then(|value| u32::try_from(value.saturating_add(1)).ok())
        .unwrap_or(config.start_index))
}

fn refill_btc_address_pool_from_xpub(
    store: &LocalNodeStore,
    count: usize,
    purpose: &str,
) -> Result<usize> {
    if count == 0 {
        return Ok(0);
    }
    let config = btc_address_xpub_config()?
        .context("BTC address xpub is not configured; cannot refill address pool")?;
    let mut existing = existing_btc_address_set(store)?;
    let mut index = next_btc_xpub_index(store, &config)?;
    let mut added = 0usize;
    let mut attempts = 0usize;
    let max_attempts = count
        .saturating_add(existing.len())
        .saturating_add(BTC_ADDRESS_POOL_TARGET)
        .saturating_mul(2)
        .max(count);
    while added < count && attempts < max_attempts {
        attempts += 1;
        let (address, concrete_path) = derive_btc_address_from_xpub(&config, index)
            .with_context(|| format!("derive BTC address pool index {index}"))?;
        let derivation_index = index;
        index = index
            .checked_add(1)
            .context("BTC address xpub derivation index overflow")?;
        if existing.contains(&address) {
            continue;
        }
        store.put_btc_address_pool_record(
            &address,
            &json!({
                "address": address,
                "created_at_ms": now_ms(),
                "purpose": purpose,
                "source": "xpub",
                "xpub_fingerprint": config.xpub_fingerprint,
                "xpub_source_format": config.source_format,
                "address_type": config.address_type,
                "derivation_path": config.path_template,
                "concrete_derivation_path": concrete_path,
                "derivation_index": derivation_index
            }),
        )?;
        existing.insert(address);
        added += 1;
    }
    ensure!(
        added == count,
        "BTC address xpub refill added {added} addresses, requested {count}"
    );
    Ok(added)
}

fn refill_btc_address_pool_to_target(store: &LocalNodeStore, purpose: &str) -> Result<usize> {
    let available = store.list_btc_address_pool_records()?.len();
    if available >= BTC_ADDRESS_POOL_LOW_WATER {
        return Ok(0);
    }
    let to_add = BTC_ADDRESS_POOL_TARGET.saturating_sub(available);
    refill_btc_address_pool_from_xpub(store, to_add, purpose)
}

fn btc_address_pool_record_for(store: &LocalNodeStore, address: &str) -> Result<Option<Value>> {
    for (record_address, record) in store
        .list_used_btc_address_pool_records()?
        .into_iter()
        .chain(store.list_btc_address_pool_records()?)
    {
        if record_address == address {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

fn btc_xpub_lookup_limit() -> u32 {
    local_string("btc-address-xpub-lookup-limit")
        .or_else(|| local_string("btc-addr-xpub-lookup-limit"))
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(BTC_ADDRESS_XPUB_LOOKUP_LIMIT)
}

fn btc_xpub_key_source_for_index(
    config: &BtcAddressXpubConfig,
    address: &str,
    index: u32,
) -> Result<(PublicKey, Fingerprint, DerivationPath)> {
    let path = xpub_derivation_path(&config.path_template, index)?;
    let (derived_address, _) = derive_btc_address_from_xpub(config, index)?;
    ensure!(
        derived_address == address,
        "BTC address xpub derivation mismatch for {address}: derived {derived_address}"
    );
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let derived = config
        .xpub
        .derive_pub(&secp, &path)
        .with_context(|| format!("derive BTC xpub signing key for {address}"))?;
    Ok((
        derived.public_key,
        config.signing_fingerprint,
        xpub_signing_derivation_path(config, &path),
    ))
}

fn btc_xpub_find_index_for_address(
    store: &LocalNodeStore,
    config: &BtcAddressXpubConfig,
    address: &str,
) -> Result<Option<u32>> {
    let next_index = next_btc_xpub_index(store, config).unwrap_or(config.start_index);
    let configured_limit = btc_xpub_lookup_limit();
    let upper_by_limit = config.start_index.saturating_add(configured_limit);
    let upper_by_seen = next_index.saturating_add(BTC_ADDRESS_XPUB_LOOKUP_MARGIN);
    let upper = upper_by_limit.max(upper_by_seen);
    for index in config.start_index..=upper {
        let (derived_address, _) = derive_btc_address_from_xpub(config, index)
            .with_context(|| format!("derive BTC xpub lookup index {index}"))?;
        if derived_address == address {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn btc_xpub_index_map_for_addresses(
    store: &LocalNodeStore,
    config: &BtcAddressXpubConfig,
    addresses: &BTreeSet<String>,
) -> Result<BTreeMap<String, u32>> {
    if addresses.is_empty() {
        return Ok(BTreeMap::new());
    }
    let next_index = next_btc_xpub_index(store, config).unwrap_or(config.start_index);
    let configured_limit = btc_xpub_lookup_limit();
    let upper_by_limit = config.start_index.saturating_add(configured_limit);
    let upper_by_seen = next_index.saturating_add(BTC_ADDRESS_XPUB_LOOKUP_MARGIN);
    let upper = upper_by_limit.max(upper_by_seen);
    let mut matched = BTreeMap::new();
    for index in config.start_index..=upper {
        let (derived_address, _) = derive_btc_address_from_xpub(config, index)
            .with_context(|| format!("derive BTC xpub repair index {index}"))?;
        if addresses.contains(&derived_address) {
            matched.insert(derived_address, index);
            if matched.len() == addresses.len() {
                break;
            }
        }
    }
    Ok(matched)
}

fn enriched_btc_xpub_pool_record(
    record: Value,
    config: &BtcAddressXpubConfig,
    address: &str,
    index: u32,
    purpose: &str,
    ident: Option<&str>,
) -> Result<Value> {
    let (derived_address, concrete_path) = derive_btc_address_from_xpub(config, index)?;
    ensure!(
        derived_address == address,
        "BTC xpub repair mismatch for {address}: derived {derived_address}"
    );
    let mut object = match record {
        Value::Object(object) => object,
        _ => Map::new(),
    };
    let now = now_ms();
    object
        .entry("created_at_ms".to_string())
        .or_insert_with(|| json!(now));
    object
        .entry("purpose".to_string())
        .or_insert_with(|| json!(purpose));
    object.insert("address".to_string(), json!(address));
    object.insert("source".to_string(), json!("xpub"));
    object.insert(
        "xpub_fingerprint".to_string(),
        json!(config.xpub_fingerprint),
    );
    object.insert(
        "xpub_source_format".to_string(),
        json!(config.source_format),
    );
    object.insert("address_type".to_string(), json!(config.address_type));
    object.insert("derivation_path".to_string(), json!(config.path_template));
    object.insert("concrete_derivation_path".to_string(), json!(concrete_path));
    object.insert("derivation_index".to_string(), json!(index));
    object.insert("metadata_repaired_at_ms".to_string(), json!(now));
    if let Some(ident) = ident.filter(|value| !value.trim().is_empty()) {
        object.insert("ident".to_string(), json!(ident));
    }
    Ok(Value::Object(object))
}

fn btc_xpub_input_key_source(
    store: &LocalNodeStore,
    config: &BtcAddressXpubConfig,
    address: &str,
) -> Result<Option<(PublicKey, Fingerprint, DerivationPath)>> {
    let Some(record) = btc_address_pool_record_for(store, address)? else {
        let Some(index) = btc_xpub_find_index_for_address(store, config, address)? else {
            return Ok(None);
        };
        return btc_xpub_key_source_for_index(config, address, index).map(Some);
    };
    if record.get("source").and_then(Value::as_str) != Some("xpub") {
        let Some(index) = btc_xpub_find_index_for_address(store, config, address)? else {
            return Ok(None);
        };
        return btc_xpub_key_source_for_index(config, address, index).map(Some);
    }
    if record.get("xpub_fingerprint").and_then(Value::as_str)
        != Some(config.xpub_fingerprint.as_str())
    {
        let Some(index) = btc_xpub_find_index_for_address(store, config, address)? else {
            return Ok(None);
        };
        return btc_xpub_key_source_for_index(config, address, index).map(Some);
    }
    if record.get("derivation_path").and_then(Value::as_str) != Some(config.path_template.as_str())
    {
        let Some(index) = btc_xpub_find_index_for_address(store, config, address)? else {
            return Ok(None);
        };
        return btc_xpub_key_source_for_index(config, address, index).map(Some);
    }
    let Some(index) = record.get("derivation_index").and_then(Value::as_u64) else {
        let Some(index) = btc_xpub_find_index_for_address(store, config, address)? else {
            return Ok(None);
        };
        return btc_xpub_key_source_for_index(config, address, index).map(Some);
    };
    let index = u32::try_from(index).context("BTC address pool derivation_index exceeds u32")?;
    btc_xpub_key_source_for_index(config, address, index).map(Some)
}

fn validate_btc_consolidation_xpub_config(config: &BtcAddressXpubConfig) -> Result<()> {
    ensure!(
        matches!(
            config.address_type.as_str(),
            "p2wpkh" | "native_segwit" | "segwit" | "taproot" | "p2tr"
        ),
        "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: reason=unsupported_deposit_script_type; address_type={}; expected=p2wpkh_or_p2tr",
        config.address_type
    );
    ensure!(
        usize::from(config.xpub.depth) == config.account_path.len(),
        "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: reason=xpub_account_path_depth_mismatch; xpub_depth={}; account_path={}; account_path_depth={}",
        config.xpub.depth,
        config.account_path_text,
        config.account_path.len()
    );
    ensure!(
        config.xpub.depth == 0 || config.signing_fingerprint_configured,
        "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: reason=missing_master_fingerprint; xpub_depth={}; account_path={}",
        config.xpub.depth,
        config.account_path_text
    );
    Ok(())
}

fn btc_consolidation_is_p2tr(address_type: &str) -> bool {
    matches!(address_type, "taproot" | "p2tr")
}

fn btc_consolidation_address_for_public_key(
    address_type: &str,
    public_key: &PublicKey,
    network: Network,
) -> Result<Address> {
    let compressed = CompressedPublicKey(*public_key);
    match address_type {
        "p2wpkh" | "native_segwit" | "segwit" => Ok(Address::p2wpkh(&compressed, network)),
        "taproot" | "p2tr" => {
            let secp = bitcoin::secp256k1::Secp256k1::new();
            let (internal_key, _) = public_key.x_only_public_key();
            Ok(Address::p2tr(&secp, internal_key, None, network))
        }
        other => bail!(
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: reason=unsupported_deposit_script_type; address_type={other}; expected=p2wpkh_or_p2tr"
        ),
    }
}

fn validate_btc_consolidation_psbt_input(
    psbt: &Psbt,
    input_index: usize,
    deposit_address: &str,
    expected_outpoint: &OutPoint,
    expected_script: &ScriptBuf,
    public_key: &PublicKey,
    fingerprint: &Fingerprint,
    derivation_path: &DerivationPath,
    address_type: &str,
    network: Network,
) -> Result<()> {
    let tx_input = psbt.unsigned_tx.input.get(input_index).with_context(|| {
        format!(
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=missing_unsigned_tx_input"
        )
    })?;
    ensure!(
        tx_input.previous_output == *expected_outpoint,
        "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=input_order_mismatch; expected_outpoint={expected_outpoint}; actual_outpoint={}",
        tx_input.previous_output
    );

    let psbt_input = psbt.inputs.get(input_index).with_context(|| {
        format!(
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=missing_psbt_input"
        )
    })?;
    let witness_utxo = psbt_input.witness_utxo.as_ref().with_context(|| {
        format!(
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=missing_witness_utxo"
        )
    })?;
    ensure!(
        witness_utxo.script_pubkey == *expected_script,
        "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=witness_utxo_script_mismatch; expected_script={}; actual_script={}",
        bytes_to_hex(expected_script.as_bytes()),
        bytes_to_hex(witness_utxo.script_pubkey.as_bytes())
    );

    let derived_address =
        btc_consolidation_address_for_public_key(address_type, public_key, network)?;
    let derived_script = derived_address.script_pubkey();
    ensure!(
        derived_script == witness_utxo.script_pubkey,
        "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; pubkey={public_key}; reason=pubkey_script_mismatch; expected_script={}; actual_script={}",
        bytes_to_hex(derived_script.as_bytes()),
        bytes_to_hex(witness_utxo.script_pubkey.as_bytes())
    );
    ensure!(
        derived_address.to_string() == deposit_address,
        "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; pubkey={public_key}; reason=pubkey_address_mismatch; derived_address={derived_address}"
    );
    if btc_consolidation_is_p2tr(address_type) {
        let (internal_key, _) = public_key.x_only_public_key();
        ensure!(
            psbt_input.bip32_derivation.is_empty(),
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=unexpected_legacy_bip32_derivation_for_p2tr"
        );
        ensure!(
            psbt_input.tap_internal_key == Some(internal_key),
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=tap_internal_key_mismatch; expected={internal_key}; actual={:?}",
            psbt_input.tap_internal_key
        );
        ensure!(
            psbt_input.tap_key_origins.len() == 1,
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=unexpected_tap_key_origin_count; expected=1; actual={}",
            psbt_input.tap_key_origins.len()
        );
        let (leaf_hashes, (actual_fingerprint, actual_path)) = psbt_input
            .tap_key_origins
            .get(&internal_key)
            .with_context(|| {
                format!(
                    "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; internal_key={internal_key}; reason=missing_tap_key_origin"
                )
            })?;
        ensure!(
            leaf_hashes.is_empty(),
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=unexpected_tap_leaf_hashes_for_bip86; actual={}",
            leaf_hashes.len()
        );
        ensure!(
            actual_fingerprint == fingerprint && actual_path == derivation_path,
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; internal_key={internal_key}; reason=tap_key_origin_mismatch; expected_fingerprint={fingerprint}; actual_fingerprint={actual_fingerprint}; expected_path={derivation_path}; actual_path={actual_path}"
        );
    } else {
        ensure!(
            psbt_input.bip32_derivation.len() == 1,
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; reason=unexpected_bip32_derivation_count; expected=1; actual={}",
            psbt_input.bip32_derivation.len()
        );
        let (actual_fingerprint, actual_path) = psbt_input
            .bip32_derivation
            .get(public_key)
            .with_context(|| {
                format!(
                    "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; pubkey={public_key}; reason=missing_bip32_derivation"
                )
            })?;
        ensure!(
            actual_fingerprint == fingerprint && actual_path == derivation_path,
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={deposit_address}; pubkey={public_key}; reason=bip32_origin_mismatch; expected_fingerprint={fingerprint}; actual_fingerprint={actual_fingerprint}; expected_path={derivation_path}; actual_path={actual_path}"
        );
    }
    Ok(())
}

fn find_u64_field(value: &Value, names: &[&str]) -> Option<u64> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|value| {
            value.as_u64().or_else(|| {
                value
                    .as_str()
                    .and_then(|text| text.trim().parse::<u64>().ok())
            })
        })
    })
}

pub fn register_console_modules(vm: &Vm) -> Result<()> {
    register_btc_module(vm)?;
    register_rgb_module(vm)?;
    register_ln_rgb_module(vm)?;
    Ok(())
}

fn register_btc_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write();
    jit.add_native_module_ptr(
        "btc",
        "get_wallet_address",
        &[Type::Str],
        Type::Any,
        btc_get_wallet_address as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "balance",
        &[Type::Str],
        Type::Any,
        btc_balance as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "status",
        &[Type::Str],
        Type::Any,
        btc_status as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "utxos",
        &[Type::Str],
        Type::Any,
        btc_utxos as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "assets",
        &[Type::Str],
        Type::Any,
        btc_assets as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "get_deposit_address",
        &[Type::Str],
        Type::Any,
        btc_get_deposit_address as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "lookup_address_ident",
        &[Type::Str],
        Type::Any,
        btc_lookup_address_ident as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "verify_message",
        &[Type::Str, Type::Str, Type::Str],
        Type::Any,
        btc_verify_message as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "scan_deposits",
        &[Type::Str],
        Type::Any,
        btc_scan_deposits as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "scan_deposits_batch",
        &[Type::Str],
        Type::Any,
        btc_scan_deposits_batch as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "scan_ident_deposits",
        &[Type::Str],
        Type::Any,
        btc_scan_ident_deposits as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "address_pool_status",
        &[],
        Type::Any,
        btc_address_pool_status as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "derive_xpub_address",
        &[Type::U64],
        Type::Any,
        btc_derive_xpub_address as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "refill_address_pool",
        &[Type::U64],
        Type::Any,
        btc_refill_address_pool as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "repair_address_pool_metadata",
        &[],
        Type::Any,
        btc_repair_address_pool_metadata as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "broadcast",
        &[Type::Str],
        Type::Any,
        btc_broadcast as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "broadcast_psbt",
        &[Type::Str],
        Type::Any,
        btc_broadcast_psbt as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "broadcast_psbt_checked",
        &[Type::Str, Type::Str, Type::U64],
        Type::Any,
        btc_broadcast_psbt_checked as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "broadcast_psbt_outputs_checked",
        &[Type::Str, Type::Str],
        Type::Any,
        btc_broadcast_psbt_outputs_checked as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "prepare_transfer_with_inputs",
        &[Type::Str, Type::Str, Type::U64, Type::U64, Type::Str],
        Type::Any,
        btc_prepare_transfer_with_inputs as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "prepare_sweep_with_inputs",
        &[Type::Str, Type::Str, Type::U64, Type::Str],
        Type::Any,
        btc_prepare_sweep_with_inputs as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "prepare_consolidation_psbt",
        &[Type::Str, Type::U64, Type::Any],
        Type::Any,
        btc_prepare_consolidation_psbt as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "prepare_batch_transfer_with_inputs",
        &[Type::Str, Type::U64, Type::Str],
        Type::Any,
        btc_prepare_batch_transfer_with_inputs as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "transfer",
        &[Type::Str, Type::Str, Type::U64, Type::U64],
        Type::Any,
        btc_transfer as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "transfer_with_inputs",
        &[Type::Str, Type::Str, Type::U64, Type::U64, Type::Str],
        Type::Any,
        btc_transfer_with_inputs as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "tx_status",
        &[Type::Str],
        Type::Any,
        btc_tx_status as *const u8,
    )?;
    Ok(())
}

fn register_rgb_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write();
    jit.add_native_module_ptr(
        "rgb",
        "signed",
        &[Type::Any, Type::Any],
        Type::Any,
        rgb_signed as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "rna_balance",
        &[Type::Any],
        Type::Any,
        rgb_rna_balance as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "request",
        &[Type::Str, Type::Any, Type::Any],
        Type::Any,
        rgb_request as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "issue",
        &[
            Type::Str,
            Type::Str,
            Type::U8,
            Type::U64,
            Type::Str,
            Type::Any,
        ],
        Type::Any,
        rgb_issue as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "assets",
        &[Type::Any],
        Type::Any,
        rgb_assets as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "assets_by_utxo",
        &[Type::Str, Type::Str, Type::Bool, Type::Any],
        Type::Any,
        rgb_assets_by_utxo as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "assets_by_utxo_sync",
        &[Type::Str, Type::Str, Type::Bool],
        Type::Any,
        rgb_assets_by_utxo_sync as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "assert_psbt_no_assets",
        &[Type::Str],
        Type::Any,
        rgb_assert_psbt_no_assets as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "scan_utxos",
        &[Type::Str],
        Type::Any,
        rgb_scan_utxos as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "token_list",
        &[],
        Type::Any,
        rgb_token_list as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "balance",
        &[Type::Str, Type::Str, Type::Any],
        Type::Any,
        rgb_balance as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "balance_breakdown",
        &[Type::Str, Type::Any],
        Type::Any,
        rgb_balance_breakdown as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "prepare_transfer",
        &[
            Type::Str,
            Type::U64,
            Type::Str,
            Type::Str,
            Type::U32,
            Type::U32,
            Type::U64,
            Type::Any,
        ],
        Type::Any,
        rgb_prepare_transfer as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "transfer",
        &[
            Type::Str,
            Type::U64,
            Type::Str,
            Type::Str,
            Type::U32,
            Type::U32,
            Type::U64,
            Type::Any,
        ],
        Type::Any,
        rgb_transfer as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "commit_transfer",
        &[
            Type::Str,
            Type::U64,
            Type::Str,
            Type::Str,
            Type::Str,
            Type::Any,
        ],
        Type::Any,
        rgb_commit_transfer as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "test",
        &[Type::Str, Type::Any],
        Type::Any,
        rgb_test as *const u8,
    )?;
    Ok(())
}

fn register_ln_rgb_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write();
    jit.add_native_module_ptr(
        "ln_rgb",
        "node_address",
        &[Type::Any],
        Type::Any,
        ln_rgb_node_address as *const u8,
    )?;
    jit.add_native_module_ptr("ln_rgb", "start", &[], Type::Any, ln_rgb_start as *const u8)?;
    jit.add_native_module_ptr("ln_rgb", "stop", &[], Type::Any, ln_rgb_stop as *const u8)?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "spawn_scanner",
        &[Type::U64],
        Type::Any,
        ln_rgb_spawn_scanner as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "scanner_status",
        &[],
        Type::Any,
        ln_rgb_scanner_status as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "retry_sweeps",
        &[],
        Type::Any,
        ln_rgb_retry_sweeps as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "status",
        &[],
        Type::Any,
        ln_rgb_status as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "get_node_id",
        &[],
        Type::Any,
        ln_rgb_get_node_id as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "sign_message",
        &[Type::Str],
        Type::Any,
        ln_rgb_sign_message as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "get_addr",
        &[],
        Type::Any,
        ln_rgb_get_addr as *const u8,
    )?;
    jit.add_native_module_ptr("ln_rgb", "utxos", &[], Type::Any, ln_rgb_utxos as *const u8)?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "sync_utxos",
        &[],
        Type::Any,
        ln_rgb_sync_utxos as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "transfer_with_inputs",
        &[Type::Str, Type::U64, Type::U64, Type::Str],
        Type::Any,
        ln_rgb_transfer_with_inputs as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "transfer_batch_with_inputs",
        &[Type::Str, Type::U64, Type::Str],
        Type::Any,
        ln_rgb_transfer_batch_with_inputs as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "transfer_rgb_l1",
        &[Type::Str, Type::U64, Type::Str, Type::U64],
        Type::Any,
        ln_rgb_transfer_rgb_l1 as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "prepare_external_rgb_l1_sweep",
        &[Type::Str, Type::U64, Type::Str, Type::Str, Type::U64],
        Type::Any,
        ln_rgb_prepare_external_rgb_l1_sweep as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "commit_rgb_l1_transfer",
        &[Type::Str, Type::U64, Type::Str, Type::Str],
        Type::Any,
        ln_rgb_commit_rgb_l1_transfer as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "commit_external_rgb_l1_sweep",
        &[Type::Str, Type::Str, Type::U64, Type::Str, Type::Str],
        Type::Any,
        ln_rgb_commit_external_rgb_l1_sweep as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "amount",
        &[],
        Type::Any,
        ln_rgb_amount as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "btc_amount",
        &[],
        Type::Any,
        ln_rgb_btc_amount as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "ln_amount",
        &[],
        Type::Any,
        ln_rgb_ln_amount as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "get_peers",
        &[],
        Type::Any,
        ln_rgb_get_peers as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "get_channels",
        &[],
        Type::Any,
        ln_rgb_get_channels as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "connect",
        &[Type::Str, Type::Str, Type::Bool],
        Type::Any,
        ln_rgb_connect as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "open_channel",
        &[Type::Str, Type::Str, Type::U64, Type::U64],
        Type::Any,
        ln_rgb_open_channel as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "close_channel",
        &[Type::Str, Type::Str, Type::Bool, Type::Str],
        Type::Any,
        ln_rgb_close_channel as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "splice_btc",
        &[Type::Str, Type::Str, Type::I64, Type::U64, Type::U64],
        Type::Any,
        ln_rgb_splice_btc as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "invoice",
        &[Type::U64, Type::Str, Type::U64],
        Type::Any,
        ln_rgb_invoice as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "invoice_for_ident",
        &[Type::U64, Type::Str, Type::U64, Type::Str],
        Type::Any,
        ln_rgb_invoice_for_ident as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "pay",
        &[Type::Str],
        Type::Any,
        ln_rgb_pay as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "assets",
        &[],
        Type::Any,
        ln_rgb_assets as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "balance",
        &[Type::Str],
        Type::Any,
        ln_rgb_balance as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "open_rgb_channel",
        &[
            Type::Str,
            Type::Str,
            Type::U64,
            Type::U64,
            Type::U64,
            Type::Str,
            Type::U64,
        ],
        Type::Any,
        ln_rgb_open_rgb_channel as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "send_rgb_payment",
        &[Type::Str, Type::U64, Type::Str, Type::Str, Type::U64],
        Type::Any,
        ln_rgb_send_rgb_payment as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln_rgb",
        "get_info",
        &[],
        Type::Any,
        ln_rgb_get_info as *const u8,
    )?;
    Ok(())
}

extern "C" fn btc_get_wallet_address(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |ident| {
        let account = btc_account_for_ident(ident)?;
        Ok(ok(json!({
            "module": "btc",
            "ident": ident,
            "address": account
                .get("address")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "account": account
        })))
    })
}

extern "C" fn btc_derive_xpub_address(index: u64) -> *const Dynamic {
    native_result(|| {
        let index = u32::try_from(index).context("BTC xpub derivation index exceeds u32")?;
        let config = btc_address_xpub_config()?
            .context("BTC address xpub is not configured; cannot derive address")?;
        let (address, derivation_path) = derive_btc_address_from_xpub(&config, index)?;
        Ok(ok(json!({
            "module": "btc",
            "ok": true,
            "operation": "derive_xpub_address",
            "address": address,
            "derivation_index": index,
            "derivation_path": derivation_path,
            "xpub_fingerprint": config.xpub_fingerprint,
            "address_type": config.address_type
        })))
    })
}

extern "C" fn btc_get_deposit_address(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |ident| {
        let ident = ident.to_string();
        if ident.trim().is_empty() {
            return Ok(ok(json!({
                "module": "btc",
                "ident": "",
                "address": default_account_id()?,
                "created": false,
                "persisted": false,
                "source": "default_account"
            })));
        }
        ensure!(!ident.trim().is_empty(), "ident must not be empty");
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        if let Some(address) = store.get_ident_btc_address(&ident)? {
            return Ok(ok(json!({
                "module": "btc",
                "ident": ident,
                "address": address,
                "created": false,
                "persisted": true
            })));
        }
        let mut available = store.list_btc_address_pool_records()?;
        available.sort_by_key(|(_, record)| {
            record
                .get("created_at_ms")
                .and_then(Value::as_u64)
                .unwrap_or_default()
        });
        let (address, mut pool_record) = available
            .into_iter()
            .next()
            .context("BTC address pool is empty; refill required")?;
        if let Value::Object(object) = &mut pool_record {
            object.insert("used_at_ms".to_string(), json!(now_ms()));
            object.insert("ident".to_string(), json!(ident));
        }
        store.remove_btc_address_pool_record(&address)?;
        store.put_used_btc_address_pool_record(&address, &pool_record)?;
        store.put_ident_btc_address(&ident, &address)?;
        match refill_btc_address_pool_to_target(&store, "low_water_refill") {
            Ok(added) if added > 0 => eprintln!(
                "[zust-console] BTC address pool auto-refilled after assignment: added={added}, low_water={BTC_ADDRESS_POOL_LOW_WATER}, target={BTC_ADDRESS_POOL_TARGET}"
            ),
            Ok(_) => {}
            Err(err) => eprintln!(
                "[zust-console] BTC address pool auto-refill after assignment failed: {err:#}"
            ),
        }
        Ok(ok(json!({
            "module": "btc",
            "ident": ident,
            "address": address,
            "created": true,
            "persisted": true,
            "source": "local_address_pool",
            "pool_record": pool_record
        })))
    })
}

extern "C" fn btc_lookup_address_ident(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |address| {
        let address = address.to_string();
        ensure!(!address.trim().is_empty(), "address must not be empty");
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        Ok(ok(json!({
            "module": "btc",
            "address": address,
            "ident": store.lookup_ident_by_btc_address(&address)?,
        })))
    })
}

extern "C" fn btc_verify_message(
    address: *const Dynamic,
    message: *const Dynamic,
    signature: *const Dynamic,
) -> *const Dynamic {
    native_three_string_dynamic_result(
        address,
        message,
        signature,
        |address, message, signature| {
            let address_text = address.trim();
            ensure!(!address_text.is_empty(), "address must not be empty");
            ensure!(!message.is_empty(), "message must not be empty");
            ensure!(!signature.trim().is_empty(), "signature must not be empty");

            let network = parse_ln_network(&ln_rgb_network_name())?;
            let address = Address::from_str(address_text)
                .with_context(|| format!("invalid BTC address: {address_text}"))?
                .require_network(network)
                .with_context(|| format!("address is not for {network:?}: {address_text}"))?;
            let signature = bitcoin::sign_message::MessageSignature::from_base64(signature.trim())
                .context("invalid Bitcoin message signature")?;
            let hash = bitcoin::sign_message::signed_msg_hash(message);
            let secp = bitcoin::secp256k1::Secp256k1::new();
            let pubkey = signature
                .recover_pubkey(&secp, hash)
                .context("recover Bitcoin message public key")?;
            ensure!(
                address.is_related_to_pubkey(&pubkey),
                "signature does not match address"
            );

            Ok(ok(json!({
                "module": "btc",
                "address": address_text,
                "verified": true
            })))
        },
    )
}

extern "C" fn btc_scan_deposits(input: *const Dynamic) -> *const Dynamic {
    const MAX_RETURNED_DEPOSITS: usize = 100;

    native_string_dynamic_result(input, |ident_filter| {
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        store.put_wallet_btc_address(&default_account_id()?)?;
        let (esplora, tip_height) = btc_tip_height_with_fallback();
        let mut scan_esplora = esplora.clone();
        let mut deposits = Vec::new();
        let mut persisted = 0usize;
        let mut address_mappings = Vec::new();
        let (requested_ident, explicit_address) = ident_filter
            .split_once('\t')
            .map(|(ident, address)| (ident.trim(), address.trim()))
            .unwrap_or((ident_filter.trim(), ""));
        if requested_ident.is_empty() && explicit_address.is_empty() {
            let address = store
                .get_wallet_btc_address()?
                .unwrap_or(default_account_id()?);
            address_mappings.push((
                "wallet".to_string(),
                "default".to_string(),
                String::new(),
                address,
            ));
        } else if !explicit_address.is_empty() {
            ensure!(!requested_ident.is_empty(), "BTC ident must not be empty");
            let network = parse_ln_network(&ln_rgb_network_name())?;
            let address = Address::from_str(explicit_address)
                .with_context(|| format!("invalid BTC deposit address: {explicit_address}"))?
                .require_network(network)
                .with_context(|| {
                    format!("deposit address is not for {network:?}: {explicit_address}")
                })?;
            address_mappings.push((
                "ident".to_string(),
                String::new(),
                requested_ident.to_string(),
                address.to_string(),
            ));
        } else if let Some(address) = store.get_ident_btc_address(requested_ident)? {
            address_mappings.push((
                "ident".to_string(),
                String::new(),
                requested_ident.to_string(),
                address,
            ));
        } else {
            bail!("unknown BTC ident `{requested_ident}`; call btc::get_deposit_address first");
        }
        for (owner_type, owner_label, ident, address) in address_mappings {
            let mut seen = std::collections::BTreeSet::new();
            let (tx_esplora, txs) = btc_address_txs_json_with_fallback(
                &address,
                &format!("fetch BTC deposit transactions for {address}"),
            )?;
            scan_esplora = tx_esplora;
            for tx in txs.as_array().cloned().unwrap_or_default() {
                let txid = tx
                    .get("txid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                for (fallback_vout, output) in tx
                    .get("vout")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                {
                    let output_address = output
                        .get("scriptpubkey_address")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if output_address != address {
                        continue;
                    }
                    let vout = output
                        .get("n")
                        .and_then(Value::as_u64)
                        .unwrap_or(fallback_vout as u64);
                    let outpoint = format!("{txid}:{vout}");
                    seen.insert(outpoint.clone());
                    let amount_sat = output
                        .get("value")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    let status = tx.get("status").unwrap_or(&Value::Null);
                    let confirmed = status
                        .get("confirmed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let block_height = status.get("block_height").and_then(Value::as_u64);
                    let confirmations = block_height
                        .filter(|_| confirmed)
                        .map(|height| tip_height.saturating_sub(height).saturating_add(1))
                        .unwrap_or_default();
                    let record = json!({
                        "owner_type": owner_type,
                        "owner_label": owner_label,
                        "ident": ident,
                        "wallet_owner": owner_type == "wallet",
                        "address": address,
                        "txid": txid,
                        "vout": vout,
                        "outpoint": outpoint,
                        "amount_sat": amount_sat,
                        "confirmed": confirmed,
                        "confirmations": confirmations,
                        "block_height": block_height,
                        "status": if confirmed { "confirmed" } else { "unconfirmed" },
                        "updated_at_ms": now_ms()
                    });
                    store.put_btc_deposit_record(
                        record
                            .get("outpoint")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                        &record,
                    )?;
                    persisted += 1;
                    if deposits.len() < MAX_RETURNED_DEPOSITS {
                        deposits.push(record);
                    }
                }
            }
            let utxos = btc_address_utxos_json(&address, &scan_esplora)?;
            for utxo in utxos.as_array().cloned().unwrap_or_default() {
                let txid = utxo
                    .get("txid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let vout = utxo.get("vout").and_then(Value::as_u64).unwrap_or_default();
                let outpoint = format!("{txid}:{vout}");
                if seen.contains(&outpoint) {
                    continue;
                }
                let amount_sat = utxo
                    .get("value")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let status = utxo.get("status").unwrap_or(&Value::Null);
                let confirmed = status
                    .get("confirmed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let block_height = status.get("block_height").and_then(Value::as_u64);
                let confirmations = block_height
                    .filter(|_| confirmed)
                    .map(|height| tip_height.saturating_sub(height).saturating_add(1))
                    .unwrap_or_default();
                let record = json!({
                    "owner_type": owner_type,
                    "owner_label": owner_label,
                    "ident": ident,
                    "wallet_owner": owner_type == "wallet",
                    "address": address,
                    "txid": txid,
                    "vout": vout,
                    "outpoint": outpoint,
                    "amount_sat": amount_sat,
                    "confirmed": confirmed,
                    "confirmations": confirmations,
                    "block_height": block_height,
                    "status": if confirmed { "confirmed" } else { "unconfirmed" },
                    "updated_at_ms": now_ms()
                });
                store.put_btc_deposit_record(
                    record
                        .get("outpoint")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    &record,
                )?;
                persisted += 1;
                if deposits.len() < MAX_RETURNED_DEPOSITS {
                    deposits.push(record);
                }
            }
        }
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
            "network": "bitcoin",
            "esplora": scan_esplora,
            "tip_height": tip_height,
            "scanner_started": LN_SCANNER_STARTED.load(Ordering::SeqCst),
            "persisted": persisted,
            "deposits": deposits,
            "stored_deposits_count": store.count_btc_deposit_records()?
        })))
    })
}

extern "C" fn btc_scan_deposits_batch(input: *const Dynamic) -> *const Dynamic {
    const MAX_CONCURRENCY: usize = 5;
    const ADDRESS_TIMEOUT_SECS: u64 = 5;

    native_string_dynamic_result(input, |batch| {
        let mut requests = Vec::new();
        for (index, line) in batch.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let (ident, address) = line
                .split_once('\t')
                .with_context(|| format!("invalid BTC scan batch row {index}: expected ident\\taddress"))?;
            let ident = ident.trim();
            let address = address.trim();
            ensure!(!ident.is_empty(), "BTC ident must not be empty at batch row {index}");
            ensure!(!address.is_empty(), "BTC address must not be empty at batch row {index}");
            requests.push((index, ident.to_string(), address.to_string()));
        }
        ensure!(!requests.is_empty(), "BTC scan batch must not be empty");

        let request_count = requests.len();
        let queue = std::sync::Arc::new(std::sync::Mutex::new(
            requests.into_iter().collect::<std::collections::VecDeque<_>>(),
        ));
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker_count = MAX_CONCURRENCY.min(request_count);
        for _ in 0..worker_count {
            let queue = std::sync::Arc::clone(&queue);
            let sender = sender.clone();
            std::thread::spawn(move || loop {
                let request = match queue.lock() {
                    Ok(mut queue) => queue.pop_front(),
                    Err(_) => None,
                };
                let Some((index, ident, address)) = request else {
                    break;
                };
                let started = std::time::Instant::now();
                let result = btc_scan_deposit_address_with_deadline(
                    &ident,
                    &address,
                    started + std::time::Duration::from_secs(ADDRESS_TIMEOUT_SECS),
                );
                let elapsed_ms = started.elapsed().as_millis() as u64;
                let value = match result {
                    Ok(scan) => json!({
                        "ok": true,
                        "ident": ident,
                        "address": address,
                        "elapsed_ms": elapsed_ms,
                        "scan": scan
                    }),
                    Err(error) => json!({
                        "ok": false,
                        "ident": ident,
                        "address": address,
                        "elapsed_ms": elapsed_ms,
                        "error": format!("{error:#}")
                    }),
                };
                if sender.send((index, value)).is_err() {
                    break;
                }
            });
        }
        drop(sender);

        let mut indexed_results = receiver.into_iter().collect::<Vec<_>>();
        indexed_results.sort_by_key(|(index, _)| *index);
        let mut deposits = Vec::new();
        let mut failed_addresses = 0usize;
        let results = indexed_results
            .into_iter()
            .map(|(_, result)| {
                if result.get("ok").and_then(Value::as_bool) == Some(true) {
                    if let Some(items) = result
                        .get("scan")
                        .and_then(|scan| scan.get("deposits"))
                        .and_then(Value::as_array)
                    {
                        deposits.extend(items.iter().cloned());
                    }
                } else {
                    failed_addresses += 1;
                }
                result
            })
            .collect::<Vec<_>>();

        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
            "concurrency": worker_count,
            "address_timeout_secs": ADDRESS_TIMEOUT_SECS,
            "scanned_addresses": request_count,
            "successful_addresses": request_count.saturating_sub(failed_addresses),
            "failed_addresses": failed_addresses,
            "deposits": deposits,
            "results": results
        })))
    })
}

extern "C" fn btc_scan_ident_deposits(input: *const Dynamic) -> *const Dynamic {
    const MAX_RETURNED_DEPOSITS: usize = 100;

    native_string_dynamic_result(input, |ident_filter| {
        let ident_filter = ident_filter.to_string();
        ensure!(!ident_filter.trim().is_empty(), "ident must not be empty");
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        store.put_wallet_btc_address(&default_account_id()?)?;
        let (esplora, tip_height) = btc_tip_height_with_fallback();
        let mut scan_esplora = esplora.clone();
        let mut deposits = Vec::new();
        let mut persisted = 0usize;
        for (ident, address) in store.list_ident_btc_addresses()? {
            if ident != ident_filter {
                continue;
            }
            let mut seen = std::collections::BTreeSet::new();
            let (tx_esplora, txs) = btc_address_txs_json_with_fallback(
                &address,
                &format!("fetch BTC deposit transactions for {address}"),
            )?;
            scan_esplora = tx_esplora;
            for tx in txs.as_array().cloned().unwrap_or_default() {
                let txid = tx
                    .get("txid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                for (fallback_vout, output) in tx
                    .get("vout")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                {
                    let output_address = output
                        .get("scriptpubkey_address")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if output_address != address {
                        continue;
                    }
                    let vout = output
                        .get("n")
                        .and_then(Value::as_u64)
                        .unwrap_or(fallback_vout as u64);
                    let outpoint = format!("{txid}:{vout}");
                    seen.insert(outpoint.clone());
                    let amount_sat = output
                        .get("value")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    let status = tx.get("status").unwrap_or(&Value::Null);
                    let confirmed = status
                        .get("confirmed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let block_height = status.get("block_height").and_then(Value::as_u64);
                    let confirmations = block_height
                        .filter(|_| confirmed)
                        .map(|height| tip_height.saturating_sub(height).saturating_add(1))
                        .unwrap_or_default();
                    let record = json!({
                        "owner_type": "ident",
                        "owner_label": "",
                        "ident": ident,
                        "wallet_owner": false,
                        "address": address,
                        "txid": txid,
                        "vout": vout,
                        "outpoint": outpoint,
                        "amount_sat": amount_sat,
                        "confirmed": confirmed,
                        "confirmations": confirmations,
                        "block_height": block_height,
                        "status": if confirmed { "confirmed" } else { "unconfirmed" },
                        "updated_at_ms": now_ms()
                    });
                    store.put_btc_deposit_record(
                        record
                            .get("outpoint")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                        &record,
                    )?;
                    persisted += 1;
                    if deposits.len() < MAX_RETURNED_DEPOSITS {
                        deposits.push(record);
                    }
                }
            }
            let utxos = btc_address_utxos_json(&address, &scan_esplora)?;
            for utxo in utxos.as_array().cloned().unwrap_or_default() {
                let txid = utxo
                    .get("txid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let vout = utxo.get("vout").and_then(Value::as_u64).unwrap_or_default();
                let outpoint = format!("{txid}:{vout}");
                if seen.contains(&outpoint) {
                    continue;
                }
                let amount_sat = utxo
                    .get("value")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                let status = utxo.get("status").unwrap_or(&Value::Null);
                let confirmed = status
                    .get("confirmed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let block_height = status.get("block_height").and_then(Value::as_u64);
                let confirmations = block_height
                    .filter(|_| confirmed)
                    .map(|height| tip_height.saturating_sub(height).saturating_add(1))
                    .unwrap_or_default();
                let record = json!({
                    "owner_type": "ident",
                    "owner_label": "",
                    "ident": ident,
                    "wallet_owner": false,
                    "address": address,
                    "txid": txid,
                    "vout": vout,
                    "outpoint": outpoint,
                    "amount_sat": amount_sat,
                    "confirmed": confirmed,
                    "confirmations": confirmations,
                    "block_height": block_height,
                    "status": if confirmed { "confirmed" } else { "unconfirmed" },
                    "updated_at_ms": now_ms()
                });
                store.put_btc_deposit_record(
                    record
                        .get("outpoint")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    &record,
                )?;
                persisted += 1;
                if deposits.len() < MAX_RETURNED_DEPOSITS {
                    deposits.push(record);
                }
            }
        }
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
            "network": "bitcoin",
            "esplora": scan_esplora,
            "tip_height": tip_height,
            "scanner_started": LN_SCANNER_STARTED.load(Ordering::SeqCst),
            "persisted": persisted,
            "deposits": deposits,
            "stored_deposits_count": store.count_btc_deposit_records()?
        })))
    })
}

extern "C" fn btc_address_pool_status() -> *const Dynamic {
    native_result(|| {
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        let available = store.list_btc_address_pool_records()?;
        let used = store.list_used_btc_address_pool_records()?;
        let xpub_config = btc_address_xpub_config()?;
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
            "xpub": xpub_config.as_ref().map(|config| json!({
                "configured": true,
                "fingerprint": config.xpub_fingerprint,
                "signing_fingerprint": config.signing_fingerprint.to_string(),
                "source_format": config.source_format,
                "address_type": config.address_type,
                "account_path": config.account_path_text,
                "path": config.path_template,
                "start_index": config.start_index,
                "next_index": next_btc_xpub_index(&store, config).unwrap_or(config.start_index)
            })).unwrap_or_else(|| json!({
                "configured": false
            })),
            "address_pool": {
                "available": available.len(),
                "used": used.len(),
                "low_water": BTC_ADDRESS_POOL_LOW_WATER,
                "target": BTC_ADDRESS_POOL_TARGET,
                "added": 0,
                "addresses": available
                    .into_iter()
                    .map(|(_, record)| record)
                    .collect::<Vec<_>>()
            }
        })))
    })
}

extern "C" fn btc_refill_address_pool(count: u64) -> *const Dynamic {
    native_result(|| {
        let count = count as usize;
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        let mut source = "none".to_string();
        let added = if count > 0 {
            source = "xpub".to_string();
            refill_btc_address_pool_from_xpub(&store, count, "manual_refill")?
        } else {
            0
        };
        let available = store.list_btc_address_pool_records()?;
        let used = store.list_used_btc_address_pool_records()?;
        let xpub_config = btc_address_xpub_config()?;
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
            "source": source,
            "xpub": xpub_config.as_ref().map(|config| json!({
                "configured": true,
                "fingerprint": config.xpub_fingerprint,
                "signing_fingerprint": config.signing_fingerprint.to_string(),
                "source_format": config.source_format,
                "address_type": config.address_type,
                "account_path": config.account_path_text,
                "path": config.path_template,
                "start_index": config.start_index,
                "next_index": next_btc_xpub_index(&store, config).unwrap_or(config.start_index)
            })).unwrap_or_else(|| json!({
                "configured": false
            })),
            "address_pool": {
                "available": available.len(),
                "used": used.len(),
                "low_water": BTC_ADDRESS_POOL_LOW_WATER,
                "target": BTC_ADDRESS_POOL_TARGET,
                "added": added,
                "addresses": available
                    .into_iter()
                    .map(|(_, record)| record)
                    .collect::<Vec<_>>()
            }
        })))
    })
}

extern "C" fn btc_repair_address_pool_metadata() -> *const Dynamic {
    native_result(|| {
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        let config = btc_address_xpub_config()?
            .context("BTC address xpub is not configured; cannot repair address pool metadata")?;
        let available = store.list_btc_address_pool_records()?;
        let used = store.list_used_btc_address_pool_records()?;
        let ident_addresses = store.list_ident_btc_addresses()?;

        let mut addresses = BTreeSet::new();
        for (address, _) in available.iter().chain(used.iter()) {
            addresses.insert(address.clone());
        }
        for (_, address) in &ident_addresses {
            addresses.insert(address.clone());
        }
        let address_indexes = btc_xpub_index_map_for_addresses(&store, &config, &addresses)?;

        let ident_by_address = ident_addresses
            .iter()
            .map(|(ident, address)| (address.clone(), ident.clone()))
            .collect::<BTreeMap<_, _>>();
        let available_by_address = available
            .iter()
            .map(|(address, record)| (address.clone(), record.clone()))
            .collect::<BTreeMap<_, _>>();
        let used_by_address = used
            .iter()
            .map(|(address, record)| (address.clone(), record.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut available_updated = 0usize;
        for (address, record) in available {
            let Some(index) = address_indexes.get(&address).copied() else {
                continue;
            };
            let repaired = enriched_btc_xpub_pool_record(
                record,
                &config,
                &address,
                index,
                "metadata_repair_available",
                ident_by_address.get(&address).map(String::as_str),
            )?;
            store.put_btc_address_pool_record(&address, &repaired)?;
            available_updated += 1;
        }

        let mut used_updated = 0usize;
        for (address, record) in used {
            let Some(index) = address_indexes.get(&address).copied() else {
                continue;
            };
            let repaired = enriched_btc_xpub_pool_record(
                record,
                &config,
                &address,
                index,
                "metadata_repair_used",
                ident_by_address.get(&address).map(String::as_str),
            )?;
            store.put_used_btc_address_pool_record(&address, &repaired)?;
            used_updated += 1;
        }

        let mut used_created = 0usize;
        let mut removed_from_available = 0usize;
        for (ident, address) in ident_addresses {
            if used_by_address.contains_key(&address) {
                continue;
            }
            let Some(index) = address_indexes.get(&address).copied() else {
                continue;
            };
            if available_by_address.contains_key(&address) {
                store.remove_btc_address_pool_record(&address)?;
                removed_from_available += 1;
            }
            let repaired = enriched_btc_xpub_pool_record(
                Value::Null,
                &config,
                &address,
                index,
                "metadata_repair_ident",
                Some(&ident),
            )?;
            store.put_used_btc_address_pool_record(&address, &repaired)?;
            used_created += 1;
        }

        let unresolved = addresses.len().saturating_sub(address_indexes.len());
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
            "operation": "repair_address_pool_metadata",
            "address_count": addresses.len(),
            "matched_xpub": address_indexes.len(),
            "unresolved": unresolved,
            "available_updated": available_updated,
            "used_updated": used_updated,
            "used_created": used_created,
            "removed_from_available": removed_from_available,
            "xpub": {
                "fingerprint": config.xpub_fingerprint,
                "signing_fingerprint": config.signing_fingerprint.to_string(),
                "source_format": config.source_format,
                "address_type": config.address_type,
                "account_path": config.account_path_text,
                "path": config.path_template,
                "start_index": config.start_index,
                "next_index": next_btc_xpub_index(&store, &config).unwrap_or(config.start_index)
            }
        })))
    })
}

extern "C" fn btc_status(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |ident| {
        let account = btc_account_for_ident(ident)?;
        let address = account
            .get("address")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let balance = btc_balance_json(ident)?;
        let assets = btc_assets_json(ident).unwrap_or_else(|err| {
            json!({
                "error": format!("{err:#}")
            })
        });
        Ok(ok(json!({
            "module": "btc",
            "ident": ident,
            "address": address,
            "account": account,
            "network": "bitcoin",
            "balance": balance,
            "assets": assets
        })))
    })
}

extern "C" fn btc_balance(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |ident| Ok(ok(btc_balance_json(ident)?)))
}

extern "C" fn btc_utxos(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |ident| {
        let account = btc_account_for_ident(ident)?;
        let address = account
            .get("address")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let esplora = btc_esplora_url();
        let utxos = btc_address_utxos_json(&address, &esplora)?;
        Ok(ok(json!({
            "module": "btc",
            "ident": ident,
            "address": address,
            "account": account,
            "network": "bitcoin",
            "esplora": esplora,
            "utxos": utxos
        })))
    })
}

extern "C" fn btc_assets(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |ident| Ok(json_to_dynamic(&btc_assets_json(ident)?)))
}

extern "C" fn btc_broadcast(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |tx_hex| {
        Ok(json_to_dynamic(&broadcast_raw_transaction_json(tx_hex)?))
    })
}

extern "C" fn btc_broadcast_psbt(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |signed_psbt| {
        ensure!(
            !signed_psbt.trim().is_empty(),
            "signed_psbt must not be empty"
        );
        let psbt = Psbt::from_str(signed_psbt).context("decode signed PSBT base64")?;
        let (psbt, finalize_report) = finalize_psbt_for_broadcast(psbt)?;
        let tx = psbt
            .extract_tx()
            .context("extract signed transaction from PSBT")?;
        let raw_tx = bytes_to_hex(&encode::serialize(&tx));
        let extracted_txid = tx.compute_txid().to_string();
        let broadcast = broadcast_raw_transaction_json(&raw_tx)?;
        let broadcast_txid = broadcast
            .get("txid")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(extracted_txid.as_str())
            .to_string();
        ensure!(
            broadcast_txid == extracted_txid,
            "broadcast txid mismatch: extracted={extracted_txid}, broadcast={broadcast_txid}"
        );
        Ok(ok(json!({
            "module": "btc",
            "operation": "broadcast_psbt",
            "txid": broadcast_txid,
            "raw_tx": raw_tx,
            "signed_psbt": signed_psbt,
            "finalize": finalize_report,
            "outputs": tx.output
                .iter()
                .enumerate()
                .map(|(vout, output)| {
                    let address = Address::from_script(&output.script_pubkey, Network::Bitcoin)
                        .map(|address| address.to_string())
                        .unwrap_or_default();
                    json!({
                        "vout": vout,
                        "value": output.value.to_sat(),
                        "address": address,
                        "script_pubkey": bytes_to_hex(output.script_pubkey.as_bytes())
                    })
                })
                .collect::<Vec<_>>(),
            "broadcast": broadcast
        })))
    })
}

extern "C" fn btc_broadcast_psbt_checked(
    input: *const Dynamic,
    recipient: *const Dynamic,
    amount_sats: u64,
) -> *const Dynamic {
    let input = unsafe { &*input };
    let recipient = unsafe { &*recipient };
    native_result(|| {
        ensure!(input.is_str(), "signed_psbt must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(amount_sats > 0, "amount_sats must be greater than zero");
        let signed_psbt = input.as_str().trim();
        ensure!(!signed_psbt.is_empty(), "signed_psbt must not be empty");
        let recipient_address = recipient.as_str().trim().to_string();
        ensure!(
            !recipient_address.is_empty(),
            "recipient address must not be empty"
        );
        let network = Network::Bitcoin;
        let recipient_address = Address::from_str(&recipient_address)
            .with_context(|| format!("invalid recipient BTC address: {recipient_address}"))?
            .require_network(network)
            .with_context(|| format!("recipient address is not for {network:?}"))?;
        let recipient_script = recipient_address.script_pubkey();
        let psbt = Psbt::from_str(signed_psbt).context("decode signed PSBT base64")?;
        let (psbt, finalize_report) = finalize_psbt_for_broadcast(psbt)?;
        let tx = psbt
            .extract_tx()
            .context("extract signed transaction from PSBT")?;
        ensure!(
            tx.output.iter().any(|output| {
                output.script_pubkey == recipient_script && output.value.to_sat() == amount_sats
            }),
            "signed PSBT does not pay expected withdrawal output"
        );
        let raw_tx = bytes_to_hex(&encode::serialize(&tx));
        let extracted_txid = tx.compute_txid().to_string();
        let broadcast = broadcast_raw_transaction_json(&raw_tx)?;
        let broadcast_txid = broadcast
            .get("txid")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(extracted_txid.as_str())
            .to_string();
        ensure!(
            broadcast_txid == extracted_txid,
            "broadcast txid mismatch: extracted={extracted_txid}, broadcast={broadcast_txid}"
        );
        Ok(ok(json!({
            "module": "btc",
            "operation": "broadcast_psbt_checked",
            "txid": broadcast_txid,
            "raw_tx": raw_tx,
            "signed_psbt": signed_psbt,
            "expected_output": {
                "address": recipient_address.to_string(),
                "amount_sats": amount_sats
            },
            "finalize": finalize_report,
            "outputs": tx.output
                .iter()
                .enumerate()
                .map(|(vout, output)| {
                    let address = Address::from_script(&output.script_pubkey, Network::Bitcoin)
                        .map(|address| address.to_string())
                        .unwrap_or_default();
                    json!({
                        "vout": vout,
                        "value": output.value.to_sat(),
                        "address": address,
                        "script_pubkey": bytes_to_hex(output.script_pubkey.as_bytes())
                    })
                })
                .collect::<Vec<_>>(),
            "broadcast": broadcast
        })))
    })
}

fn parse_btc_outputs(outputs: &str, network: Network) -> Result<Vec<(String, ScriptBuf, u64)>> {
    let mut parsed = Vec::new();
    for entry in outputs
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (address, amount) = entry
            .split_once('=')
            .with_context(|| format!("invalid BTC output entry: {entry}"))?;
        let address = address.trim().to_string();
        ensure!(!address.is_empty(), "BTC output address must not be empty");
        let amount_sats = amount
            .trim()
            .parse::<u64>()
            .with_context(|| format!("invalid BTC output amount for {address}"))?;
        ensure!(
            amount_sats > 0,
            "BTC output amount must be greater than zero"
        );
        let script = Address::from_str(&address)
            .with_context(|| format!("invalid recipient BTC address: {address}"))?
            .require_network(network)
            .with_context(|| format!("recipient address is not for {network:?}"))?
            .script_pubkey();
        parsed.push((address, script, amount_sats));
    }
    ensure!(
        !parsed.is_empty(),
        "outputs must include at least one BTC output"
    );
    Ok(parsed)
}

fn btc_output_counts(
    outputs: &[(String, ScriptBuf, u64)],
) -> std::collections::BTreeMap<String, u64> {
    let mut counts = std::collections::BTreeMap::new();
    for (_, script, amount) in outputs {
        let key = format!("{}:{amount}", bytes_to_hex(script.as_bytes()));
        *counts.entry(key).or_insert(0) += 1;
    }
    counts
}

fn validate_p2wpkh_signing_pubkey(
    vin: usize,
    input: &bitcoin::psbt::Input,
    public_key: &bitcoin::PublicKey,
) -> Result<()> {
    let witness_utxo = input.witness_utxo.as_ref().with_context(|| {
        format!(
            "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=missing_witness_utxo"
        )
    })?;
    ensure!(
        witness_utxo.script_pubkey.is_p2wpkh(),
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=expected_p2wpkh_input; actual_script={}",
        bytes_to_hex(witness_utxo.script_pubkey.as_bytes())
    );
    let derived_script = Address::p2wpkh(
        &CompressedPublicKey(public_key.inner),
        Network::Bitcoin,
    )
    .script_pubkey();
    ensure!(
        derived_script == witness_utxo.script_pubkey,
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; pubkey={public_key}; reason=pubkey_script_mismatch; expected_script={}; actual_script={}",
        bytes_to_hex(witness_utxo.script_pubkey.as_bytes()),
        bytes_to_hex(derived_script.as_bytes())
    );
    Ok(())
}

fn validate_finalized_p2wpkh_witness(vin: usize, input: &bitcoin::psbt::Input) -> Result<()> {
    let witness = input.final_script_witness.as_ref().with_context(|| {
        format!(
            "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=missing_final_script_witness"
        )
    })?;
    ensure!(
        witness.len() == 2,
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=invalid_p2wpkh_witness_item_count; expected=2; actual={}",
        witness.len()
    );
    ensure!(
        input
            .final_script_sig
            .as_ref()
            .map(|script| script.is_empty())
            .unwrap_or(true),
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=non_empty_p2wpkh_final_script_sig"
    );
    let signature = witness.iter().next().with_context(|| {
        format!("BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=missing_witness_signature")
    })?;
    ensure!(
        !signature.is_empty(),
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=empty_witness_signature"
    );
    let public_key_bytes = witness.iter().nth(1).with_context(|| {
        format!("BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=missing_witness_pubkey")
    })?;
    let public_key = bitcoin::PublicKey::from_slice(public_key_bytes).with_context(|| {
        format!(
            "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=invalid_witness_pubkey; pubkey={}",
            bytes_to_hex(public_key_bytes)
        )
    })?;
    validate_p2wpkh_signing_pubkey(vin, input, &public_key)
}

fn validate_p2tr_signing_key(vin: usize, input: &bitcoin::psbt::Input) -> Result<()> {
    let witness_utxo = input.witness_utxo.as_ref().with_context(|| {
        format!("BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=missing_witness_utxo")
    })?;
    ensure!(
        witness_utxo.script_pubkey.is_p2tr(),
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=expected_p2tr_input; actual_script={}",
        bytes_to_hex(witness_utxo.script_pubkey.as_bytes())
    );
    ensure!(
        input.tap_merkle_root.is_none(),
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=unexpected_tap_merkle_root_for_bip86"
    );
    let internal_key = input.tap_internal_key.with_context(|| {
        format!("BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=missing_tap_internal_key")
    })?;
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let derived_script = Address::p2tr(&secp, internal_key, None, Network::Bitcoin).script_pubkey();
    ensure!(
        derived_script == witness_utxo.script_pubkey,
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; internal_key={internal_key}; reason=tap_internal_key_script_mismatch; expected_script={}; actual_script={}",
        bytes_to_hex(witness_utxo.script_pubkey.as_bytes()),
        bytes_to_hex(derived_script.as_bytes())
    );
    Ok(())
}

fn validate_finalized_p2tr_witness(vin: usize, input: &bitcoin::psbt::Input) -> Result<()> {
    let witness = input.final_script_witness.as_ref().with_context(|| {
        format!("BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=missing_final_script_witness")
    })?;
    ensure!(
        witness.len() == 1,
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=invalid_p2tr_key_path_witness_item_count; expected=1; actual={}",
        witness.len()
    );
    ensure!(
        input.final_script_sig.as_ref().map(|script| script.is_empty()).unwrap_or(true),
        "BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=non_empty_p2tr_final_script_sig"
    );
    let signature = witness.iter().next().with_context(|| {
        format!("BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=missing_taproot_key_signature")
    })?;
    bitcoin::taproot::Signature::from_slice(signature).with_context(|| {
        format!("BTC_SIGNED_PSBT_INPUT_MISMATCH: vin={vin}; reason=invalid_taproot_key_signature")
    })?;
    Ok(())
}

fn finalize_psbt_for_broadcast(mut psbt: Psbt) -> Result<(Psbt, Value)> {
    let mut inputs = Vec::new();
    let mut auto_finalized = 0usize;
    for (vin, input) in psbt.inputs.iter_mut().enumerate() {
        let has_final = input.final_script_witness.is_some() || input.final_script_sig.is_some();
        let script_kind = input
            .witness_utxo
            .as_ref()
            .map(|utxo| {
                if utxo.script_pubkey.is_p2wpkh() {
                    "p2wpkh"
                } else if utxo.script_pubkey.is_p2tr() {
                    "p2tr"
                } else if utxo.script_pubkey.is_p2wsh() {
                    "p2wsh"
                } else if utxo.script_pubkey.is_p2pkh() {
                    "p2pkh"
                } else {
                    "unknown"
                }
            })
            .unwrap_or("missing_witness_utxo");
        let partial_sig_count = input.partial_sigs.len();
        if has_final {
            if script_kind == "p2wpkh" {
                validate_finalized_p2wpkh_witness(vin, input)?;
            } else if script_kind == "p2tr" {
                validate_finalized_p2tr_witness(vin, input)?;
            }
            inputs.push(json!({
                "vin": vin,
                "status": "already_finalized",
                "script_kind": script_kind,
                "partial_sig_count": partial_sig_count,
                "has_final_script_sig": input.final_script_sig.is_some(),
                "has_final_script_witness": input.final_script_witness.is_some()
            }));
            continue;
        }
        if script_kind == "p2wpkh" && partial_sig_count == 1 {
            let (pubkey, sig) = input
                .partial_sigs
                .iter()
                .next()
                .map(|(pubkey, sig)| (*pubkey, *sig))
                .context("P2WPKH partial signature disappeared before finalization")?;
            validate_p2wpkh_signing_pubkey(vin, input, &pubkey)?;
            let mut witness = Witness::new();
            witness.push(sig.to_vec());
            witness.push(pubkey.to_bytes());
            input.final_script_witness = Some(witness);
            input.final_script_sig = Some(ScriptBuf::new());
            input.partial_sigs = BTreeMap::new();
            input.sighash_type = None;
            input.redeem_script = None;
            input.witness_script = None;
            input.bip32_derivation = BTreeMap::new();
            auto_finalized += 1;
            inputs.push(json!({
                "vin": vin,
                "status": "auto_finalized_p2wpkh",
                "script_kind": script_kind,
                "partial_sig_count": partial_sig_count
            }));
        } else if script_kind == "p2tr" && input.tap_key_sig.is_some() {
            validate_p2tr_signing_key(vin, input)?;
            let signature = input
                .tap_key_sig
                .take()
                .context("P2TR key signature disappeared before finalization")?;
            let mut witness = Witness::new();
            witness.push(signature.to_vec());
            input.final_script_witness = Some(witness);
            input.final_script_sig = Some(ScriptBuf::new());
            input.sighash_type = None;
            input.tap_scripts = BTreeMap::new();
            input.tap_key_origins = BTreeMap::new();
            input.tap_internal_key = None;
            input.tap_merkle_root = None;
            auto_finalized += 1;
            inputs.push(json!({
                "vin": vin,
                "status": "auto_finalized_p2tr_key_path",
                "script_kind": script_kind,
                "partial_sig_count": 0
            }));
        } else {
            let status = if partial_sig_count <= 0 {
                "missing_partial_sigs"
            } else {
                "unsupported_partial_finalization"
            };
            inputs.push(json!({
                "vin": vin,
                "status": status,
                "script_kind": script_kind,
                "partial_sig_count": partial_sig_count,
                "has_witness_utxo": input.witness_utxo.is_some(),
                "has_non_witness_utxo": input.non_witness_utxo.is_some()
            }));
        }
    }
    let report = json!({
        "auto_finalized_inputs": auto_finalized,
        "inputs": inputs
    });
    if auto_finalized > 0
        || report
            .get("inputs")
            .and_then(Value::as_array)
            .map(|items| {
                items.iter().any(|item| {
                    item.get("status").and_then(Value::as_str) == Some("missing_partial_sigs")
                        || item.get("status").and_then(Value::as_str)
                            == Some("unsupported_partial_finalization")
                })
            })
            .unwrap_or(false)
    {
        log_btc_broadcast_event("psbt_finalize", report.clone());
    }
    Ok((psbt, report))
}

extern "C" fn btc_broadcast_psbt_outputs_checked(
    input: *const Dynamic,
    outputs: *const Dynamic,
) -> *const Dynamic {
    let input = unsafe { &*input };
    let outputs = unsafe { &*outputs };
    native_result(|| {
        ensure!(input.is_str(), "signed_psbt must be string");
        ensure!(outputs.is_str(), "outputs must be string");
        let signed_psbt = input.as_str().trim();
        ensure!(!signed_psbt.is_empty(), "signed_psbt must not be empty");
        let expected_outputs = parse_btc_outputs(outputs.as_str(), Network::Bitcoin)?;
        let expected_counts = btc_output_counts(&expected_outputs);
        let psbt = Psbt::from_str(signed_psbt).context("decode signed PSBT base64")?;
        let (psbt, finalize_report) = finalize_psbt_for_broadcast(psbt)?;
        let tx = psbt
            .extract_tx()
            .context("extract signed transaction from PSBT")?;
        let actual_tuples = tx
            .output
            .iter()
            .map(|output| {
                (
                    String::new(),
                    output.script_pubkey.clone(),
                    output.value.to_sat(),
                )
            })
            .collect::<Vec<_>>();
        let actual_counts = btc_output_counts(&actual_tuples);
        for (key, expected_count) in expected_counts {
            let actual_count = actual_counts.get(&key).copied().unwrap_or_default();
            ensure!(
                actual_count >= expected_count,
                "signed PSBT does not pay all expected withdrawal outputs"
            );
        }
        let raw_tx = bytes_to_hex(&encode::serialize(&tx));
        let extracted_txid = tx.compute_txid().to_string();
        let broadcast = broadcast_raw_transaction_json(&raw_tx)?;
        let broadcast_txid = broadcast
            .get("txid")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(extracted_txid.as_str())
            .to_string();
        ensure!(
            broadcast_txid == extracted_txid,
            "broadcast txid mismatch: extracted={extracted_txid}, broadcast={broadcast_txid}"
        );
        Ok(ok(json!({
            "module": "btc",
            "operation": "broadcast_psbt_outputs_checked",
            "txid": broadcast_txid,
            "raw_tx": raw_tx,
            "signed_psbt": signed_psbt,
            "expected_outputs": expected_outputs
                .iter()
                .map(|(address, _, amount)| json!({
                    "address": address,
                    "amount_sats": amount
                }))
                .collect::<Vec<_>>(),
            "finalize": finalize_report,
            "outputs": tx.output
                .iter()
                .enumerate()
                .map(|(vout, output)| {
                    let address = Address::from_script(&output.script_pubkey, Network::Bitcoin)
                        .map(|address| address.to_string())
                        .unwrap_or_default();
                    json!({
                        "vout": vout,
                        "value": output.value.to_sat(),
                        "address": address,
                        "script_pubkey": bytes_to_hex(output.script_pubkey.as_bytes())
                    })
                })
                .collect::<Vec<_>>(),
            "broadcast": broadcast
        })))
    })
}

extern "C" fn btc_transfer(
    ident: *const Dynamic,
    recipient: *const Dynamic,
    amount_sats: u64,
    fee_rate_sat_vb: u64,
) -> *const Dynamic {
    let ident = unsafe { &*ident };
    let recipient = unsafe { &*recipient };
    native_result(|| {
        ensure!(ident.is_str(), "ident must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(amount_sats > 0, "amount_sats must be greater than zero");
        let ident = ident.as_str().trim().to_string();
        let account = btc_account_for_ident(&ident)?;
        let sender_address = account
            .get("address")
            .and_then(Value::as_str)
            .filter(|address| !address.trim().is_empty())
            .context("BTC sender account missing address")?
            .to_string();
        let recipient_address = recipient.as_str().trim().to_string();
        ensure!(
            !recipient_address.is_empty(),
            "recipient address must not be empty"
        );
        let network = Network::Bitcoin;
        let recipient_address = Address::from_str(&recipient_address)
            .with_context(|| format!("invalid recipient BTC address: {recipient_address}"))?
            .require_network(network)
            .with_context(|| format!("recipient address is not for {network:?}"))?;
        let sender_address_checked = Address::from_str(&sender_address)
            .with_context(|| format!("invalid sender BTC address: {sender_address}"))?
            .require_network(network)
            .with_context(|| format!("sender address is not for {network:?}"))?;
        let recipient_script = recipient_address.script_pubkey();
        let change_script = sender_address_checked.script_pubkey();
        let esplora = btc_esplora_url();
        let utxos = btc_address_utxos_json(&sender_address, &esplora)?;
        let mut candidates = utxos
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|utxo| {
                utxo.get("status")
                    .and_then(|status| status.get("confirmed"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .map(|utxo| {
                let txid = utxo
                    .get("txid")
                    .and_then(Value::as_str)
                    .context("utxo missing txid")?;
                let vout = utxo
                    .get("vout")
                    .and_then(Value::as_u64)
                    .context("utxo missing vout")?;
                let value = utxo
                    .get("value")
                    .and_then(Value::as_u64)
                    .context("utxo missing value")?;
                Ok((
                    OutPoint {
                        txid: txid
                            .parse()
                            .with_context(|| format!("invalid utxo txid {txid}"))?,
                        vout: u32::try_from(vout).context("utxo vout exceeds u32")?,
                    },
                    value,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        candidates.sort_by_key(|(_, value)| *value);
        let fee_rate_sat_vb = if fee_rate_sat_vb > 0 {
            fee_rate_sat_vb
        } else {
            2
        };
        let mut selected = Vec::new();
        let mut selected_sats = 0u64;
        for candidate in candidates {
            selected_sats = selected_sats.saturating_add(candidate.1);
            selected.push(candidate);
            let output_count = 2u64;
            let estimated_vbytes =
                10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
            let fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
            if selected_sats >= amount_sats.saturating_add(fee_sats) {
                break;
            }
        }
        ensure!(!selected.is_empty(), "no confirmed BTC UTXO available");
        let mut output_count = 2u64;
        let mut estimated_vbytes =
            10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
        let mut fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
        ensure!(
            selected_sats >= amount_sats.saturating_add(fee_sats),
            "insufficient confirmed BTC funds: need {} sats, selected {} sats",
            amount_sats.saturating_add(fee_sats),
            selected_sats
        );
        let mut change_sats = selected_sats
            .saturating_sub(amount_sats)
            .saturating_sub(fee_sats);
        if change_sats > 0 && change_sats < 546 {
            output_count = 1;
            estimated_vbytes =
                10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
            fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
            ensure!(
                selected_sats >= amount_sats.saturating_add(fee_sats),
                "insufficient confirmed BTC funds after dust change removal"
            );
            change_sats = 0;
        }
        let mut tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: selected
                .iter()
                .map(|(outpoint, _)| TxIn {
                    previous_output: *outpoint,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                })
                .collect(),
            output: vec![TxOut {
                value: Amount::from_sat(amount_sats),
                script_pubkey: recipient_script,
            }],
        };
        if change_sats >= 546 {
            tx.output.push(TxOut {
                value: Amount::from_sat(change_sats),
                script_pubkey: change_script.clone(),
            });
        }
        let mut psbt = Psbt::from_unsigned_tx(tx).context("build BTC transfer PSBT")?;
        for (index, (_, value)) in selected.iter().enumerate() {
            psbt.inputs[index].witness_utxo = Some(TxOut {
                value: Amount::from_sat(*value),
                script_pubkey: change_script.clone(),
            });
        }
        let unsigned_psbt = psbt.to_string();
        let sign = external_signature_unavailable::<Value>()?;
        let signed_psbt = ["psbt", "signed_psbt", "signed_anchor_psbt"]
            .iter()
            .find_map(|key| sign.get(*key).and_then(Value::as_str))
            .filter(|value| !value.trim().is_empty())
            .with_context(|| format!("signer PSBT response missing signed psbt: {sign}"))?
            .to_string();
        let (raw_tx, extracted_txid) = extract_transaction_hex_from_signed_psbt(&signed_psbt)?;
        let broadcast = broadcast_raw_transaction_json(&raw_tx)?;
        let broadcast_txid = broadcast
            .get("txid")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(extracted_txid.as_str())
            .to_string();
        ensure!(
            broadcast_txid == extracted_txid,
            "broadcast txid mismatch: extracted={extracted_txid}, broadcast={broadcast_txid}"
        );
        Ok(ok(json!({
            "module": "btc",
            "operation": "transfer",
            "ident": ident,
            "from": sender_address,
            "to": recipient_address.to_string(),
            "amount_sats": amount_sats,
            "fee_rate_sat_vb": fee_rate_sat_vb,
            "fee_sats": selected_sats.saturating_sub(amount_sats).saturating_sub(change_sats),
            "change_sats": change_sats,
            "selected_sats": selected_sats,
            "inputs": selected
                .iter()
                .map(|(outpoint, value)| json!({
                    "outpoint": outpoint.to_string(),
                    "value": value
                }))
                .collect::<Vec<_>>(),
            "txid": broadcast_txid,
            "raw_tx": raw_tx,
            "unsigned_psbt": unsigned_psbt,
            "signed_psbt": signed_psbt,
            "sign": sign,
            "broadcast": broadcast
        })))
    })
}

extern "C" fn btc_transfer_with_inputs(
    ident: *const Dynamic,
    recipient: *const Dynamic,
    amount_sats: u64,
    fee_rate_sat_vb: u64,
    input_outpoints: *const Dynamic,
) -> *const Dynamic {
    let ident = unsafe { &*ident };
    let recipient = unsafe { &*recipient };
    let input_outpoints = unsafe { &*input_outpoints };
    native_result(|| {
        ensure!(ident.is_str(), "ident must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(input_outpoints.is_str(), "input_outpoints must be string");
        ensure!(amount_sats > 0, "amount_sats must be greater than zero");
        let ident = ident.as_str().trim().to_string();
        let account = btc_account_for_ident(&ident)?;
        let sender_address = account
            .get("address")
            .and_then(Value::as_str)
            .filter(|address| !address.trim().is_empty())
            .context("BTC sender account missing address")?
            .to_string();
        let recipient_address = recipient.as_str().trim().to_string();
        ensure!(
            !recipient_address.is_empty(),
            "recipient address must not be empty"
        );
        let requested = input_outpoints.as_str().trim().to_string();
        ensure!(
            !requested.is_empty(),
            "input_outpoints must include at least one outpoint"
        );
        let network = Network::Bitcoin;
        let recipient_address = Address::from_str(&recipient_address)
            .with_context(|| format!("invalid recipient BTC address: {recipient_address}"))?
            .require_network(network)
            .with_context(|| format!("recipient address is not for {network:?}"))?;
        let sender_address_checked = Address::from_str(&sender_address)
            .with_context(|| format!("invalid sender BTC address: {sender_address}"))?
            .require_network(network)
            .with_context(|| format!("sender address is not for {network:?}"))?;
        let recipient_script = recipient_address.script_pubkey();
        let change_script = sender_address_checked.script_pubkey();
        let esplora = btc_esplora_url();
        let utxos = btc_address_utxos_json(&sender_address, &esplora)?;
        let candidates = utxos
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|utxo| {
                utxo.get("status")
                    .and_then(|status| status.get("confirmed"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .map(|utxo| {
                let txid = utxo
                    .get("txid")
                    .and_then(Value::as_str)
                    .context("utxo missing txid")?;
                let vout = utxo
                    .get("vout")
                    .and_then(Value::as_u64)
                    .context("utxo missing vout")?;
                let value = utxo
                    .get("value")
                    .and_then(Value::as_u64)
                    .context("utxo missing value")?;
                Ok((
                    OutPoint {
                        txid: txid
                            .parse()
                            .with_context(|| format!("invalid utxo txid {txid}"))?,
                        vout: u32::try_from(vout).context("utxo vout exceeds u32")?,
                    },
                    value,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let available = candidates
            .iter()
            .map(|(outpoint, value)| (*outpoint, *value))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut seen = std::collections::BTreeSet::new();
        let mut selected = Vec::new();
        let mut selected_sats = 0u64;
        for token in requested
            .split(|ch: char| ch == ',' || ch == '\n' || ch == '\r' || ch == '\t' || ch == ' ')
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            let outpoint = OutPoint::from_str(token)
                .with_context(|| format!("invalid selected BTC outpoint: {token}"))?;
            ensure!(
                seen.insert(outpoint),
                "duplicate selected BTC outpoint: {outpoint}"
            );
            let value = *available.get(&outpoint).with_context(|| {
                format!("selected BTC outpoint is not confirmed/available: {outpoint}")
            })?;
            selected_sats = selected_sats.saturating_add(value);
            selected.push((outpoint, value));
        }
        ensure!(!selected.is_empty(), "no selected BTC UTXO provided");
        let fee_rate_sat_vb = if fee_rate_sat_vb > 0 {
            fee_rate_sat_vb
        } else {
            2
        };
        let mut output_count = 2u64;
        let mut estimated_vbytes =
            10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
        let mut fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
        ensure!(
            selected_sats >= amount_sats.saturating_add(fee_sats),
            "insufficient selected BTC funds: need {} sats, selected {} sats",
            amount_sats.saturating_add(fee_sats),
            selected_sats
        );
        let mut change_sats = selected_sats
            .saturating_sub(amount_sats)
            .saturating_sub(fee_sats);
        if change_sats > 0 && change_sats < 546 {
            output_count = 1;
            estimated_vbytes =
                10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
            fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
            ensure!(
                selected_sats >= amount_sats.saturating_add(fee_sats),
                "insufficient selected BTC funds after dust change removal"
            );
            change_sats = 0;
        }
        let mut tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: selected
                .iter()
                .map(|(outpoint, _)| TxIn {
                    previous_output: *outpoint,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                })
                .collect(),
            output: vec![TxOut {
                value: Amount::from_sat(amount_sats),
                script_pubkey: recipient_script,
            }],
        };
        if change_sats >= 546 {
            tx.output.push(TxOut {
                value: Amount::from_sat(change_sats),
                script_pubkey: change_script.clone(),
            });
        }
        let mut psbt = Psbt::from_unsigned_tx(tx).context("build BTC transfer PSBT")?;
        for (index, (_, value)) in selected.iter().enumerate() {
            psbt.inputs[index].witness_utxo = Some(TxOut {
                value: Amount::from_sat(*value),
                script_pubkey: change_script.clone(),
            });
        }
        let unsigned_psbt = psbt.to_string();
        let sign = external_signature_unavailable::<Value>()?;
        let signed_psbt = ["psbt", "signed_psbt", "signed_anchor_psbt"]
            .iter()
            .find_map(|key| sign.get(*key).and_then(Value::as_str))
            .filter(|value| !value.trim().is_empty())
            .with_context(|| format!("signer PSBT response missing signed psbt: {sign}"))?
            .to_string();
        let (raw_tx, extracted_txid) = extract_transaction_hex_from_signed_psbt(&signed_psbt)?;
        let broadcast = broadcast_raw_transaction_json(&raw_tx)?;
        let broadcast_txid = broadcast
            .get("txid")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(extracted_txid.as_str())
            .to_string();
        ensure!(
            broadcast_txid == extracted_txid,
            "broadcast txid mismatch: extracted={extracted_txid}, broadcast={broadcast_txid}"
        );
        Ok(ok(json!({
            "module": "btc",
            "operation": "transfer_with_inputs",
            "ident": ident,
            "from": sender_address,
            "to": recipient_address.to_string(),
            "amount_sats": amount_sats,
            "fee_rate_sat_vb": fee_rate_sat_vb,
            "fee_sats": selected_sats.saturating_sub(amount_sats).saturating_sub(change_sats),
            "change_sats": change_sats,
            "selected_sats": selected_sats,
            "inputs": selected
                .iter()
                .map(|(outpoint, value)| json!({
                    "outpoint": outpoint.to_string(),
                    "value": value
                }))
                .collect::<Vec<_>>(),
            "txid": broadcast_txid,
            "raw_tx": raw_tx,
            "unsigned_psbt": unsigned_psbt,
            "signed_psbt": signed_psbt,
            "sign": sign,
            "broadcast": broadcast
        })))
    })
}

extern "C" fn btc_prepare_transfer_with_inputs(
    ident: *const Dynamic,
    recipient: *const Dynamic,
    amount_sats: u64,
    fee_rate_sat_vb: u64,
    input_outpoints: *const Dynamic,
) -> *const Dynamic {
    let ident = unsafe { &*ident };
    let recipient = unsafe { &*recipient };
    let input_outpoints = unsafe { &*input_outpoints };
    native_result(|| {
        ensure!(ident.is_str(), "ident must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(input_outpoints.is_str(), "input_outpoints must be string");
        ensure!(amount_sats > 0, "amount_sats must be greater than zero");
        let ident = ident.as_str().trim().to_string();
        let account = btc_account_for_ident(&ident)?;
        let sender_address = account
            .get("address")
            .and_then(Value::as_str)
            .filter(|address| !address.trim().is_empty())
            .context("BTC sender account missing address")?
            .to_string();
        let recipient_address = recipient.as_str().trim().to_string();
        ensure!(
            !recipient_address.is_empty(),
            "recipient address must not be empty"
        );
        let requested = input_outpoints.as_str().trim().to_string();
        ensure!(
            !requested.is_empty(),
            "input_outpoints must include at least one outpoint"
        );
        let network = Network::Bitcoin;
        let recipient_address = Address::from_str(&recipient_address)
            .with_context(|| format!("invalid recipient BTC address: {recipient_address}"))?
            .require_network(network)
            .with_context(|| format!("recipient address is not for {network:?}"))?;
        let sender_address_checked = Address::from_str(&sender_address)
            .with_context(|| format!("invalid sender BTC address: {sender_address}"))?
            .require_network(network)
            .with_context(|| format!("sender address is not for {network:?}"))?;
        let recipient_script = recipient_address.script_pubkey();
        let change_script = sender_address_checked.script_pubkey();
        let esplora = btc_esplora_url();
        let utxos = btc_address_utxos_json(&sender_address, &esplora)?;
        let candidates = utxos
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|utxo| {
                utxo.get("status")
                    .and_then(|status| status.get("confirmed"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .map(|utxo| {
                let txid = utxo
                    .get("txid")
                    .and_then(Value::as_str)
                    .context("utxo missing txid")?;
                let vout = utxo
                    .get("vout")
                    .and_then(Value::as_u64)
                    .context("utxo missing vout")?;
                let value = utxo
                    .get("value")
                    .and_then(Value::as_u64)
                    .context("utxo missing value")?;
                Ok((
                    OutPoint {
                        txid: txid
                            .parse()
                            .with_context(|| format!("invalid utxo txid {txid}"))?,
                        vout: u32::try_from(vout).context("utxo vout exceeds u32")?,
                    },
                    value,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let available = candidates
            .iter()
            .map(|(outpoint, value)| (*outpoint, *value))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut seen = std::collections::BTreeSet::new();
        let mut selected = Vec::new();
        let mut selected_sats = 0u64;
        for token in requested
            .split(|ch: char| ch == ',' || ch == '\n' || ch == '\r' || ch == '\t' || ch == ' ')
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            let outpoint = OutPoint::from_str(token)
                .with_context(|| format!("invalid selected BTC outpoint: {token}"))?;
            ensure!(
                seen.insert(outpoint),
                "duplicate selected BTC outpoint: {outpoint}"
            );
            let value = *available.get(&outpoint).with_context(|| {
                format!("selected BTC outpoint is not confirmed/available: {outpoint}")
            })?;
            selected_sats = selected_sats.saturating_add(value);
            selected.push((outpoint, value));
        }
        ensure!(!selected.is_empty(), "no selected BTC UTXO provided");
        let fee_rate_sat_vb = if fee_rate_sat_vb > 0 {
            fee_rate_sat_vb
        } else {
            2
        };
        let mut output_count = 2u64;
        let mut estimated_vbytes =
            10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
        let mut fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
        ensure!(
            selected_sats >= amount_sats.saturating_add(fee_sats),
            "insufficient selected BTC funds: need {} sats, selected {} sats",
            amount_sats.saturating_add(fee_sats),
            selected_sats
        );
        let mut change_sats = selected_sats
            .saturating_sub(amount_sats)
            .saturating_sub(fee_sats);
        if change_sats > 0 && change_sats < 546 {
            output_count = 1;
            estimated_vbytes =
                10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
            fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
            ensure!(
                selected_sats >= amount_sats.saturating_add(fee_sats),
                "insufficient selected BTC funds after dust change removal"
            );
            change_sats = 0;
        }
        let mut tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: selected
                .iter()
                .map(|(outpoint, _)| TxIn {
                    previous_output: *outpoint,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                })
                .collect(),
            output: vec![TxOut {
                value: Amount::from_sat(amount_sats),
                script_pubkey: recipient_script,
            }],
        };
        if change_sats >= 546 {
            tx.output.push(TxOut {
                value: Amount::from_sat(change_sats),
                script_pubkey: change_script.clone(),
            });
        }
        let mut psbt = Psbt::from_unsigned_tx(tx).context("build BTC transfer PSBT")?;
        for (index, (_, value)) in selected.iter().enumerate() {
            psbt.inputs[index].witness_utxo = Some(TxOut {
                value: Amount::from_sat(*value),
                script_pubkey: change_script.clone(),
            });
        }
        let unsigned_psbt = psbt.to_string();
        Ok(ok(json!({
            "module": "btc",
            "operation": "prepare_transfer_with_inputs",
            "ident": ident,
            "from": sender_address,
            "to": recipient_address.to_string(),
            "amount_sats": amount_sats,
            "fee_rate_sat_vb": fee_rate_sat_vb,
            "fee_sats": selected_sats.saturating_sub(amount_sats).saturating_sub(change_sats),
            "change_sats": change_sats,
            "selected_sats": selected_sats,
            "inputs": selected
                .iter()
                .map(|(outpoint, value)| json!({
                    "outpoint": outpoint.to_string(),
                    "value": value
                }))
                .collect::<Vec<_>>(),
            "unsigned_psbt": unsigned_psbt,
            "psbt": unsigned_psbt,
            "signer": "admin_btc_wallet"
        })))
    })
}

extern "C" fn btc_prepare_sweep_with_inputs(
    ident: *const Dynamic,
    recipient: *const Dynamic,
    fee_rate_sat_vb: u64,
    input_outpoints: *const Dynamic,
) -> *const Dynamic {
    let ident = unsafe { &*ident };
    let recipient = unsafe { &*recipient };
    let input_outpoints = unsafe { &*input_outpoints };
    native_result(|| {
        ensure!(ident.is_str(), "ident must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(input_outpoints.is_str(), "input_outpoints must be string");
        let ident = ident.as_str().trim().to_string();
        ensure!(
            !ident.is_empty(),
            "ident must not be empty for deposit sweep"
        );
        let account = btc_account_for_ident(&ident)?;
        let sender_address = account
            .get("address")
            .and_then(Value::as_str)
            .filter(|address| !address.trim().is_empty())
            .context("BTC sender account missing address")?
            .to_string();
        let recipient_address = recipient.as_str().trim().to_string();
        ensure!(
            !recipient_address.is_empty(),
            "recipient address must not be empty"
        );
        let requested = input_outpoints.as_str().trim().to_string();
        ensure!(
            !requested.is_empty(),
            "input_outpoints must include at least one outpoint"
        );
        let network = Network::Bitcoin;
        let recipient_address = Address::from_str(&recipient_address)
            .with_context(|| format!("invalid recipient BTC address: {recipient_address}"))?
            .require_network(network)
            .with_context(|| format!("recipient address is not for {network:?}"))?;
        let sender_address_checked = Address::from_str(&sender_address)
            .with_context(|| format!("invalid sender BTC address: {sender_address}"))?
            .require_network(network)
            .with_context(|| format!("sender address is not for {network:?}"))?;
        let recipient_script = recipient_address.script_pubkey();
        let sender_script = sender_address_checked.script_pubkey();
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        let xpub_config = btc_address_xpub_config()?;
        let key_source = match xpub_config.as_ref() {
            Some(config) => btc_xpub_input_key_source(&store, config, &sender_address)?,
            None => None,
        };
        let esplora = btc_esplora_url();
        let utxos = btc_address_utxos_json(&sender_address, &esplora)?;
        let candidates = utxos
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|utxo| {
                utxo.get("status")
                    .and_then(|status| status.get("confirmed"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .map(|utxo| {
                let txid = utxo
                    .get("txid")
                    .and_then(Value::as_str)
                    .context("utxo missing txid")?;
                let vout = utxo
                    .get("vout")
                    .and_then(Value::as_u64)
                    .context("utxo missing vout")?;
                let value = utxo
                    .get("value")
                    .and_then(Value::as_u64)
                    .context("utxo missing value")?;
                Ok((
                    OutPoint {
                        txid: txid
                            .parse()
                            .with_context(|| format!("invalid utxo txid {txid}"))?,
                        vout: u32::try_from(vout).context("utxo vout exceeds u32")?,
                    },
                    value,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let available = candidates
            .iter()
            .map(|(outpoint, value)| (*outpoint, *value))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut seen = std::collections::BTreeSet::new();
        let mut selected = Vec::new();
        let mut selected_sats = 0u64;
        for token in requested
            .split(|ch: char| ch == ',' || ch == '\n' || ch == '\r' || ch == '\t' || ch == ' ')
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            let outpoint = OutPoint::from_str(token)
                .with_context(|| format!("invalid selected BTC outpoint: {token}"))?;
            ensure!(
                seen.insert(outpoint),
                "duplicate selected BTC outpoint: {outpoint}"
            );
            let value = *available.get(&outpoint).with_context(|| {
                format!("selected BTC outpoint is not confirmed/available: {outpoint}")
            })?;
            selected_sats = selected_sats.saturating_add(value);
            selected.push((outpoint, value));
        }
        ensure!(!selected.is_empty(), "no selected BTC UTXO provided");
        let fee_rate_sat_vb = if fee_rate_sat_vb > 0 {
            fee_rate_sat_vb
        } else {
            2
        };
        let estimated_vbytes = 10u64 + (selected.len() as u64).saturating_mul(68) + 31;
        let fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
        ensure!(
            selected_sats > fee_sats,
            "selected BTC funds do not cover sweep fee: selected {} sats, fee {} sats",
            selected_sats,
            fee_sats
        );
        let output_sats = selected_sats.saturating_sub(fee_sats);
        ensure!(
            output_sats >= 546,
            "sweep output would be dust: {output_sats} sats"
        );
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: selected
                .iter()
                .map(|(outpoint, _)| TxIn {
                    previous_output: *outpoint,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                })
                .collect(),
            output: vec![TxOut {
                value: Amount::from_sat(output_sats),
                script_pubkey: recipient_script,
            }],
        };
        let mut psbt = Psbt::from_unsigned_tx(tx).context("build BTC sweep PSBT")?;
        let mut derivation_count = 0usize;
        for (index, (_, value)) in selected.iter().enumerate() {
            psbt.inputs[index].witness_utxo = Some(TxOut {
                value: Amount::from_sat(*value),
                script_pubkey: sender_script.clone(),
            });
            if let Some((public_key, fingerprint, derivation_path)) = key_source.as_ref() {
                let mut derivations = BTreeMap::new();
                derivations.insert(*public_key, (*fingerprint, derivation_path.clone()));
                psbt.inputs[index].bip32_derivation = derivations;
                derivation_count += 1;
            }
        }
        let unsigned_psbt = psbt.to_string();
        Ok(ok(json!({
            "module": "btc",
            "operation": "prepare_sweep_with_inputs",
            "ident": ident,
            "from": sender_address,
            "to": recipient_address.to_string(),
            "amount_sats": output_sats,
            "input_sats": selected_sats,
            "fee_rate_sat_vb": fee_rate_sat_vb,
            "fee_sats": fee_sats,
            "estimated_vbytes": estimated_vbytes,
            "inputs": selected
                .iter()
                .map(|(outpoint, value)| json!({
                    "outpoint": outpoint.to_string(),
                    "value": value
                }))
                .collect::<Vec<_>>(),
            "unsigned_psbt": unsigned_psbt,
            "psbt": unsigned_psbt,
            "signer": "xpub_owner_wallet",
            "bip32_derivation_count": derivation_count
        })))
    })
}

extern "C" fn btc_prepare_consolidation_psbt(
    recipient: *const Dynamic,
    fee_rate_sat_vb: u64,
    inputs: *const Dynamic,
) -> *const Dynamic {
    let recipient = unsafe { &*recipient };
    let inputs = unsafe { &*inputs };
    native_result(|| {
        ensure!(recipient.is_str(), "recipient must be string");
        let recipient_address = recipient.as_str().trim().to_string();
        ensure!(
            !recipient_address.is_empty(),
            "recipient address must not be empty"
        );
        let network = Network::Bitcoin;
        let recipient_address = Address::from_str(&recipient_address)
            .with_context(|| format!("invalid recipient BTC address: {recipient_address}"))?
            .require_network(network)
            .with_context(|| format!("recipient address is not for {network:?}"))?;
        let recipient_script = recipient_address.script_pubkey();
        let input_doc = dynamic_to_json(inputs);
        let input_items = input_doc
            .as_array()
            .cloned()
            .or_else(|| input_doc.get("inputs").and_then(Value::as_array).cloned())
            .or_else(|| input_doc.get("utxos").and_then(Value::as_array).cloned())
            .context("inputs must be an array or object with inputs/utxos array")?;

        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        let xpub_config = btc_address_xpub_config()?.context(
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: reason=deposit_xpub_not_configured",
        )?;
        validate_btc_consolidation_xpub_config(&xpub_config)?;
        let mut selected = Vec::new();
        let mut xpub_rejected_inputs = Vec::new();
        let mut chain_rejected_inputs = Vec::new();
        let mut live_utxos_by_address = BTreeMap::<String, BTreeMap<String, u64>>::new();
        let mut selected_sats = 0u64;
        let mut seen = std::collections::BTreeSet::new();
        for (input_index, item) in input_items.into_iter().enumerate() {
            let address = item
                .get("address")
                .or_else(|| item.get("deposit_address"))
                .and_then(Value::as_str)
                .context("consolidation input missing address")?
                .trim()
                .to_string();
            ensure!(!address.is_empty(), "consolidation input address is empty");
            let source_address = Address::from_str(&address)
                .with_context(|| format!("invalid input BTC address: {address}"))?
                .require_network(network)
                .with_context(|| format!("input address is not for {network:?}"))?;
            let outpoint = if let Some(outpoint) = item.get("outpoint").and_then(Value::as_str) {
                OutPoint::from_str(outpoint.trim())
                    .with_context(|| format!("invalid consolidation outpoint: {outpoint}"))?
            } else {
                let txid = item
                    .get("txid")
                    .and_then(Value::as_str)
                    .context("consolidation input missing txid/outpoint")?;
                let vout = item
                    .get("vout")
                    .and_then(Value::as_u64)
                    .context("consolidation input missing vout")?;
                OutPoint {
                    txid: txid
                        .parse()
                        .with_context(|| format!("invalid consolidation txid {txid}"))?,
                    vout: u32::try_from(vout).context("consolidation vout exceeds u32")?,
                }
            };
            ensure!(
                seen.insert(outpoint),
                "duplicate consolidation input outpoint: {outpoint}"
            );
            let value = item
                .get("value")
                .or_else(|| item.get("amount_sat"))
                .or_else(|| item.get("value_sats"))
                .and_then(Value::as_u64)
                .context("consolidation input missing value")?;
            ensure!(value > 0, "consolidation input value must be positive");
            let confirmed = item
                .get("status")
                .and_then(|status| status.get("confirmed"))
                .and_then(Value::as_bool)
                .or_else(|| item.get("confirmed").and_then(Value::as_bool))
                .unwrap_or(true);
            ensure!(
                confirmed,
                "consolidation input must be confirmed: {outpoint}"
            );
            if !live_utxos_by_address.contains_key(&address) {
                let chain_source = btc_esplora_url();
                let live_utxos = btc_address_utxos_json(&address, &chain_source)
                    .with_context(|| {
                        format!(
                            "fetch live consolidation UTXOs for input_index={input_index}; deposit_address={address}"
                        )
                    })?
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|utxo| {
                        utxo.get("status")
                            .and_then(|status| status.get("confirmed"))
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                    })
                    .filter_map(|utxo| {
                        let txid = utxo.get("txid")?.as_str()?;
                        let vout = utxo.get("vout")?.as_u64()?;
                        let value = utxo.get("value")?.as_u64()?;
                        Some((format!("{txid}:{vout}"), value))
                    })
                    .collect::<BTreeMap<_, _>>();
                live_utxos_by_address.insert(address.clone(), live_utxos);
            }
            let live_value = live_utxos_by_address
                .get(&address)
                .and_then(|utxos| utxos.get(&outpoint.to_string()))
                .copied();
            let Some(live_value) = live_value else {
                chain_rejected_inputs.push(json!({
                    "input_index": input_index,
                    "address": address,
                    "outpoint": outpoint.to_string(),
                    "value": value,
                    "reason": "spent_or_missing_utxo"
                }));
                continue;
            };
            if live_value != value {
                chain_rejected_inputs.push(json!({
                    "input_index": input_index,
                    "address": address,
                    "outpoint": outpoint.to_string(),
                    "value": value,
                    "live_value": live_value,
                    "reason": "utxo_value_mismatch"
                }));
                continue;
            }
            let source_script = source_address.script_pubkey();
            let Some(key_source) =
                btc_xpub_input_key_source(&store, &xpub_config, &address)?
            else {
                xpub_rejected_inputs.push(json!({
                    "input_index": input_index,
                    "address": address,
                    "outpoint": outpoint.to_string(),
                    "value": value,
                    "reason": "address_not_in_configured_deposit_xpub"
                }));
                continue;
            };
            let public_key_script = btc_consolidation_address_for_public_key(
                &xpub_config.address_type,
                &key_source.0,
                network,
            )?
            .script_pubkey();
            ensure!(
                public_key_script == source_script,
                "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: input_index={input_index}; deposit_address={address}; pubkey={}; reason=pubkey_script_mismatch; expected_script={}; actual_script={}",
                key_source.0,
                bytes_to_hex(public_key_script.as_bytes()),
                bytes_to_hex(source_script.as_bytes())
            );
            selected_sats = selected_sats.saturating_add(value);
            selected.push((
                address,
                source_script,
                outpoint,
                value,
                key_source,
            ));
        }
        ensure!(
            !selected.is_empty(),
            "BTC_CONSOLIDATION_PSBT_BIP32_MISMATCH: reason=no_signable_inputs; rejected_input_count={}",
            xpub_rejected_inputs.len()
        );

        let fee_rate_sat_vb = if fee_rate_sat_vb > 0 {
            fee_rate_sat_vb
        } else {
            2
        };
        let input_vbytes = if btc_consolidation_is_p2tr(&xpub_config.address_type) {
            58
        } else {
            68
        };
        let estimated_vbytes =
            10u64 + (selected.len() as u64).saturating_mul(input_vbytes) + 31;
        let fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
        ensure!(
            selected_sats > fee_sats,
            "selected BTC funds do not cover consolidation fee: selected {} sats, fee {} sats",
            selected_sats,
            fee_sats
        );
        let output_sats = selected_sats.saturating_sub(fee_sats);
        ensure!(
            output_sats >= 546,
            "consolidation output would be dust: {output_sats} sats"
        );

        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: selected
                .iter()
                .map(|(_, _, outpoint, _, _)| TxIn {
                    previous_output: *outpoint,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                })
                .collect(),
            output: vec![TxOut {
                value: Amount::from_sat(output_sats),
                script_pubkey: recipient_script,
            }],
        };
        let mut psbt = Psbt::from_unsigned_tx(tx).context("build BTC consolidation PSBT")?;
        let mut derivation_count = 0usize;
        for (index, (_, script_pubkey, _, value, key_source)) in selected.iter().enumerate() {
            psbt.inputs[index].witness_utxo = Some(TxOut {
                value: Amount::from_sat(*value),
                script_pubkey: script_pubkey.clone(),
            });
            let (public_key, fingerprint, derivation_path) = key_source;
            if btc_consolidation_is_p2tr(&xpub_config.address_type) {
                let (internal_key, _) = public_key.x_only_public_key();
                psbt.inputs[index].tap_internal_key = Some(internal_key);
                psbt.inputs[index].tap_key_origins.insert(
                    internal_key,
                    (Vec::new(), (*fingerprint, derivation_path.clone())),
                );
            } else {
                psbt.inputs[index]
                    .bip32_derivation
                    .insert(*public_key, (*fingerprint, derivation_path.clone()));
            }
            derivation_count += 1;
        }
        for (index, (address, script_pubkey, outpoint, _, key_source))
            in selected.iter().enumerate()
        {
            let (public_key, fingerprint, derivation_path) = key_source;
            validate_btc_consolidation_psbt_input(
                &psbt,
                index,
                address,
                outpoint,
                script_pubkey,
                public_key,
                fingerprint,
                derivation_path,
                &xpub_config.address_type,
                network,
            )?;
        }
        let unsigned_tx_hex = encode::serialize_hex(&psbt.unsigned_tx);
        let unsigned_psbt = psbt.to_string();
        Ok(ok(json!({
            "module": "btc",
            "operation": "prepare_consolidation_psbt",
            "to": recipient_address.to_string(),
            "amount_sats": output_sats,
            "input_sats": selected_sats,
            "fee_rate_sat_vb": fee_rate_sat_vb,
            "fee_sats": fee_sats,
            "estimated_vbytes": estimated_vbytes,
            "input_count": selected.len(),
            "xpub_rejected_input_count": xpub_rejected_inputs.len(),
            "xpub_rejected_inputs": xpub_rejected_inputs,
            "chain_rejected_input_count": chain_rejected_inputs.len(),
            "chain_rejected_inputs": chain_rejected_inputs,
            "inputs": selected
                .iter()
                .map(|(address, _, outpoint, value, key_source)| json!({
                    "address": address,
                    "outpoint": outpoint.to_string(),
                    "value": value,
                    "has_bip32_derivation": true,
                    "public_key": key_source.0.to_string(),
                    "master_fingerprint": key_source.1.to_string(),
                    "derivation_path": key_source.2.to_string()
                }))
                .collect::<Vec<_>>(),
            "outputs": [{
                "address": recipient_address.to_string(),
                "value": output_sats
            }],
            "unsigned_tx_hex": unsigned_tx_hex,
            "unsigned_psbt": unsigned_psbt,
            "psbt": unsigned_psbt,
            "signer": "xpub_owner_wallet",
            "bip32_derivation_count": derivation_count
        })))
    })
}

extern "C" fn btc_prepare_batch_transfer_with_inputs(
    outputs: *const Dynamic,
    fee_rate_sat_vb: u64,
    input_outpoints: *const Dynamic,
) -> *const Dynamic {
    let outputs = unsafe { &*outputs };
    let input_outpoints = unsafe { &*input_outpoints };
    native_result(|| {
        ensure!(outputs.is_str(), "outputs must be string");
        ensure!(input_outpoints.is_str(), "input_outpoints must be string");
        let ident = "";
        let account = btc_account_for_ident(ident)?;
        let sender_address = account
            .get("address")
            .and_then(Value::as_str)
            .filter(|address| !address.trim().is_empty())
            .context("BTC sender account missing address")?
            .to_string();
        let requested = input_outpoints.as_str().trim().to_string();
        ensure!(
            !requested.is_empty(),
            "input_outpoints must include at least one outpoint"
        );
        let network = Network::Bitcoin;
        let sender_address_checked = Address::from_str(&sender_address)
            .with_context(|| format!("invalid sender address: {sender_address}"))?
            .require_network(network)
            .with_context(|| format!("sender address is not for {network:?}"))?;
        let recipients = parse_btc_outputs(outputs.as_str(), network)?;
        let amount_sats = recipients
            .iter()
            .fold(0u64, |sum, (_, _, amount)| sum.saturating_add(*amount));
        ensure!(
            amount_sats > 0,
            "total output amount must be greater than zero"
        );
        let change_script = sender_address_checked.script_pubkey();
        let esplora = btc_esplora_url();
        let utxos = btc_address_utxos_json(&sender_address, &esplora)?;
        let candidates = utxos
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|utxo| {
                utxo.get("status")
                    .and_then(|status| status.get("confirmed"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .map(|utxo| {
                let txid = utxo
                    .get("txid")
                    .and_then(Value::as_str)
                    .context("utxo missing txid")?;
                let vout = utxo
                    .get("vout")
                    .and_then(Value::as_u64)
                    .context("utxo missing vout")?;
                let value = utxo
                    .get("value")
                    .and_then(Value::as_u64)
                    .context("utxo missing value")?;
                Ok((
                    OutPoint {
                        txid: txid
                            .parse()
                            .with_context(|| format!("invalid utxo txid {txid}"))?,
                        vout: u32::try_from(vout).context("utxo vout exceeds u32")?,
                    },
                    value,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let available = candidates
            .iter()
            .map(|(outpoint, value)| (*outpoint, *value))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut seen = std::collections::BTreeSet::new();
        let mut selected = Vec::new();
        let mut selected_sats = 0u64;
        for token in requested
            .split(|ch: char| ch == ',' || ch == '\n' || ch == '\r' || ch == '\t' || ch == ' ')
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            let outpoint = OutPoint::from_str(token)
                .with_context(|| format!("invalid selected BTC outpoint: {token}"))?;
            ensure!(
                seen.insert(outpoint),
                "duplicate selected BTC outpoint: {outpoint}"
            );
            let value = *available.get(&outpoint).with_context(|| {
                format!("selected BTC outpoint is not confirmed/available: {outpoint}")
            })?;
            selected_sats = selected_sats.saturating_add(value);
            selected.push((outpoint, value));
        }
        ensure!(!selected.is_empty(), "no selected BTC UTXO provided");
        let fee_rate_sat_vb = if fee_rate_sat_vb > 0 {
            fee_rate_sat_vb
        } else {
            2
        };
        let recipient_output_count =
            u64::try_from(recipients.len()).context("too many BTC outputs")?;
        let mut output_count = recipient_output_count.saturating_add(1);
        let mut estimated_vbytes =
            10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
        let mut fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
        ensure!(
            selected_sats >= amount_sats.saturating_add(fee_sats),
            "insufficient selected BTC funds: need {} sats, selected {} sats",
            amount_sats.saturating_add(fee_sats),
            selected_sats
        );
        let mut change_sats = selected_sats
            .saturating_sub(amount_sats)
            .saturating_sub(fee_sats);
        if change_sats > 0 && change_sats < 546 {
            output_count = recipient_output_count;
            estimated_vbytes =
                10u64 + (selected.len() as u64).saturating_mul(68) + output_count * 31;
            fee_sats = fee_rate_sat_vb.saturating_mul(estimated_vbytes);
            ensure!(
                selected_sats >= amount_sats.saturating_add(fee_sats),
                "insufficient selected BTC funds after dust change removal"
            );
            change_sats = 0;
        }
        let mut tx_outputs = recipients
            .iter()
            .map(|(_, script, amount)| TxOut {
                value: Amount::from_sat(*amount),
                script_pubkey: script.clone(),
            })
            .collect::<Vec<_>>();
        if change_sats >= 546 {
            tx_outputs.push(TxOut {
                value: Amount::from_sat(change_sats),
                script_pubkey: change_script.clone(),
            });
        }
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: selected
                .iter()
                .map(|(outpoint, _)| TxIn {
                    previous_output: *outpoint,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                })
                .collect(),
            output: tx_outputs,
        };
        let mut psbt = Psbt::from_unsigned_tx(tx).context("build BTC batch transfer PSBT")?;
        for (index, (_, value)) in selected.iter().enumerate() {
            psbt.inputs[index].witness_utxo = Some(TxOut {
                value: Amount::from_sat(*value),
                script_pubkey: change_script.clone(),
            });
        }
        let unsigned_psbt = psbt.to_string();
        Ok(ok(json!({
            "module": "btc",
            "operation": "prepare_batch_transfer_with_inputs",
            "ident": ident,
            "from": sender_address,
            "amount_sats": amount_sats,
            "fee_rate_sat_vb": fee_rate_sat_vb,
            "fee_sats": selected_sats.saturating_sub(amount_sats).saturating_sub(change_sats),
            "change_sats": change_sats,
            "selected_sats": selected_sats,
            "outputs": recipients
                .iter()
                .map(|(address, _, amount)| json!({
                    "address": address,
                    "amount_sats": amount
                }))
                .collect::<Vec<_>>(),
            "inputs": selected
                .iter()
                .map(|(outpoint, value)| json!({
                    "outpoint": outpoint.to_string(),
                    "value": value
                }))
                .collect::<Vec<_>>(),
            "unsigned_psbt": unsigned_psbt,
            "psbt": unsigned_psbt,
            "signer": "admin_btc_wallet"
        })))
    })
}

extern "C" fn btc_tx_status(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |txid| {
        let txid = txid.to_string();
        ensure!(!txid.trim().is_empty(), "txid must not be empty");
        let esplora = btc_esplora_url();
        let status = esplora_get_json(&format!(
            "{}/tx/{txid}/status",
            esplora.trim_end_matches('/')
        ))
        .with_context(|| format!("fetch BTC tx status {txid}"))?;
        Ok(ok(json!({
            "module": "btc",
            "txid": txid,
            "network": "bitcoin",
            "esplora": esplora,
            "status": status
        })))
    })
}

fn broadcast_raw_transaction_json(tx_hex: &str) -> Result<Value> {
    ensure!(!tx_hex.trim().is_empty(), "tx_hex must not be empty");
    ensure_btc_transaction_finalized(tx_hex)?;
    let (esplora, txid) = match esplora_post_tx_with_fallback(tx_hex) {
        Ok(result) => result,
        Err(err) => {
            log_btc_broadcast_failure(tx_hex, &err);
            return Err(err);
        }
    };
    Ok(dynamic_to_json(&ok(json!({
        "module": "btc",
        "broadcast": true,
        "network": "bitcoin",
        "esplora": esplora,
        "txid": txid.trim()
    }))))
}

fn esplora_post_tx_with_fallback(tx_hex: &str) -> Result<(String, String)> {
    let mut errors = Vec::new();
    for esplora in btc_esplora_urls() {
        let base = esplora.trim_end_matches('/');
        if is_electrum_chain_source(base) {
            match electrum_rpc(base, "blockchain.transaction.broadcast", json!([tx_hex])) {
                Ok(result) => {
                    let txid = result
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| result.to_string());
                    return Ok((base.to_string(), txid));
                }
                Err(err) => {
                    errors.push(format!("{base}: Electrum broadcast failed: {err:#}"));
                    continue;
                }
            }
        }
        let url = format!("{base}/tx");
        let response = match attohttpc::post(&url)
            .header("content-type", "text/plain")
            .text(tx_hex.to_string())
            .send()
        {
            Ok(response) => response,
            Err(err) => {
                errors.push(format!("{base}: POST failed: {err:#}"));
                continue;
            }
        };
        let status = response.status();
        let body = response
            .text()
            .with_context(|| format!("read Esplora response body from {url}"))?;
        if (200..300).contains(&status.as_u16()) {
            return Ok((base.to_string(), body));
        }
        errors.push(format!(
            "{base}: Esplora POST {url} failed with HTTP {status}: {body}"
        ));
    }
    bail!(
        "broadcast BTC transaction: all Esplora endpoints failed: {}",
        errors.join(" | ")
    )
}

fn ensure_btc_transaction_finalized(tx_hex: &str) -> Result<()> {
    let bytes = hex_to_bytes(tx_hex)?;
    let tx: Transaction = encode::deserialize(&bytes).context("decode raw tx before broadcast")?;
    let unsigned_inputs = tx
        .input
        .iter()
        .enumerate()
        .filter(|(_, input)| input.witness.is_empty() && input.script_sig.is_empty())
        .map(|(vin, input)| {
            json!({
                "vin": vin,
                "outpoint": input.previous_output.to_string()
            })
        })
        .collect::<Vec<_>>();
    if unsigned_inputs.is_empty() {
        return Ok(());
    }
    let diagnostics = btc_transaction_diagnostics_from_tx_hex(tx_hex, &tx);
    log_btc_broadcast_event(
        "not_signed_or_finalized",
        json!({
            "error": "BTC_PSBT_NOT_SIGNED_OR_FINALIZED",
            "reason": "raw transaction has inputs with empty script_sig and empty witness",
            "unsigned_inputs": unsigned_inputs,
            "diagnostics": diagnostics
        }),
    );
    bail!("BTC_PSBT_NOT_SIGNED_OR_FINALIZED: raw transaction has unsigned/non-finalized inputs")
}

fn log_btc_broadcast_failure(tx_hex: &str, err: &anyhow::Error) {
    log_btc_broadcast_event(
        "broadcast_failed",
        json!({
            "error": format!("{err:#}"),
            "diagnostics": btc_transaction_diagnostics_from_hex(tx_hex)
        }),
    );
}

fn log_btc_broadcast_event(event: &str, payload: Value) {
    let line = json!({
        "ts_ms": now_ms(),
        "event": event,
        "payload": payload
    })
    .to_string();
    let path = std::env::var("BTC_BROADCAST_LOG_FILE").unwrap_or_else(|_| {
        let dir = std::env::var("BTC_BROADCAST_LOG_DIR")
            .unwrap_or_else(|_| "/data/logs/super_bazaar".to_string());
        format!("{}/btc-broadcast.log", dir.trim_end_matches('/'))
    });
    let path = PathBuf::from(path);
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

fn btc_transaction_diagnostics_from_hex(tx_hex: &str) -> Value {
    let mut out = Map::new();
    out.insert("raw_tx".to_string(), json!(tx_hex));
    match hex_to_bytes(tx_hex)
        .and_then(|bytes| encode::deserialize::<Transaction>(&bytes).context("decode raw tx"))
    {
        Ok(tx) => return btc_transaction_diagnostics_from_tx_hex(tx_hex, &tx),
        Err(err) => {
            out.insert("decode_error".to_string(), json!(format!("{err:#}")));
        }
    }
    Value::Object(out)
}

fn btc_transaction_diagnostics_from_tx_hex(tx_hex: &str, tx: &Transaction) -> Value {
    json!({
        "raw_tx": tx_hex,
        "txid": tx.compute_txid().to_string(),
        "version": tx.version.0,
        "lock_time": tx.lock_time.to_consensus_u32(),
        "inputs": tx.input
            .iter()
            .enumerate()
            .map(|(vin, input)| {
                json!({
                    "vin": vin,
                    "outpoint": input.previous_output.to_string(),
                    "txid": input.previous_output.txid.to_string(),
                    "vout": input.previous_output.vout,
                    "sequence": input.sequence.to_consensus_u32(),
                    "script_sig": bytes_to_hex(input.script_sig.as_bytes()),
                    "witness_items": input.witness.len()
                })
            })
            .collect::<Vec<_>>(),
        "input_statuses": tx.input
            .iter()
            .take(100)
            .map(|input| match btc_outpoint_status_json(&input.previous_output) {
                Ok(status) => status,
                Err(err) => json!({
                    "outpoint": input.previous_output.to_string(),
                    "error": format!("{err:#}")
                }),
            })
            .collect::<Vec<_>>(),
        "input_status_truncated": tx.input.len() > 100,
        "outputs": tx.output
            .iter()
            .enumerate()
            .map(|(vout, output)| {
                let address = Address::from_script(&output.script_pubkey, Network::Bitcoin)
                    .map(|address| address.to_string())
                    .unwrap_or_default();
                json!({
                    "vout": vout,
                    "value": output.value.to_sat(),
                    "address": address,
                    "script_pubkey": bytes_to_hex(output.script_pubkey.as_bytes())
                })
            })
            .collect::<Vec<_>>()
    })
}

fn btc_outpoint_status_json(outpoint: &OutPoint) -> Result<Value> {
    let mut errors = Vec::new();
    for source in btc_esplora_urls() {
        let base = source.trim_end_matches('/').to_string();
        if is_electrum_chain_source(&base) {
            match btc_outpoint_status_electrum_json(&base, outpoint) {
                Ok(status) => return Ok(status),
                Err(err) => errors.push(format!("{base}: {err:#}")),
            }
        } else {
            match btc_outpoint_status_esplora_json(&base, outpoint) {
                Ok(status) => return Ok(status),
                Err(err) => errors.push(format!("{base}: {err:#}")),
            }
        }
    }
    bail!(
        "fetch BTC outpoint status {} failed: {}",
        outpoint,
        errors.join(" | ")
    )
}

fn btc_outpoint_status_electrum_json(source: &str, outpoint: &OutPoint) -> Result<Value> {
    let txid = outpoint.txid.to_string();
    let vout = outpoint.vout as usize;
    let raw = electrum_rpc(source, "blockchain.transaction.get", json!([txid]))?;
    let raw_hex = raw
        .as_str()
        .with_context(|| format!("Electrum transaction.get returned non-string for {outpoint}"))?;
    let raw_tx = hex_to_bytes(raw_hex)?;
    let tx: Transaction = encode::deserialize(&raw_tx)
        .with_context(|| format!("decode Electrum prev transaction {outpoint}"))?;
    let output = tx
        .output
        .get(vout)
        .with_context(|| format!("prev transaction missing vout for {outpoint}"))?;
    let script_hash = electrum_script_hash_hex(&output.script_pubkey);
    let unspent = electrum_rpc(
        source,
        "blockchain.scripthash.listunspent",
        json!([script_hash]),
    )?;
    let still_unspent = unspent
        .as_array()
        .map(|items| {
            items.iter().any(|item| {
                item.get("tx_hash").and_then(Value::as_str)
                    == Some(outpoint.txid.to_string().as_str())
                    && item.get("tx_pos").and_then(Value::as_u64) == Some(outpoint.vout as u64)
            })
        })
        .unwrap_or(false);
    let address = Address::from_script(&output.script_pubkey, Network::Bitcoin)
        .map(|address| address.to_string())
        .unwrap_or_default();
    Ok(json!({
        "source": source,
        "outpoint": outpoint.to_string(),
        "value": output.value.to_sat(),
        "address": address,
        "script_pubkey": bytes_to_hex(output.script_pubkey.as_bytes()),
        "still_unspent": still_unspent
    }))
}

fn btc_outpoint_status_esplora_json(source: &str, outpoint: &OutPoint) -> Result<Value> {
    let txid = outpoint.txid.to_string();
    let vout = outpoint.vout as usize;
    let tx = esplora_get_json(&format!("{source}/tx/{txid}"))?;
    let output = tx
        .get("vout")
        .and_then(Value::as_array)
        .and_then(|items| items.get(vout))
        .with_context(|| format!("Esplora transaction missing vout for {outpoint}"))?;
    let outspend = esplora_get_json(&format!("{source}/tx/{txid}/outspend/{vout}"))
        .unwrap_or_else(|err| json!({"error": format!("{err:#}")}));
    Ok(json!({
        "source": source,
        "outpoint": outpoint.to_string(),
        "value": output.get("value").and_then(Value::as_u64).unwrap_or_default(),
        "address": output.get("scriptpubkey_address").and_then(Value::as_str).unwrap_or_default(),
        "script_pubkey": output.get("scriptpubkey").and_then(Value::as_str).unwrap_or_default(),
        "still_unspent": !outspend.get("spent").and_then(Value::as_bool).unwrap_or(false),
        "outspend": outspend
    }))
}

fn extract_transaction_hex_from_signed_psbt(signed_psbt: &str) -> Result<(String, String)> {
    let psbt = Psbt::from_str(signed_psbt).context("decode signed PSBT base64")?;
    let tx = psbt
        .extract_tx()
        .context("extract signed transaction from PSBT")?;
    let txid = tx.compute_txid().to_string();
    let tx_hex = bytes_to_hex(&encode::serialize(&tx));
    Ok((tx_hex, txid))
}

fn rgb_callback_queue() -> Result<&'static SyncSender<RgbCallbackJob>> {
    match RGB_CALLBACK_QUEUE.get_or_init(|| {
        let (sender, receiver) = mpsc::sync_channel::<RgbCallbackJob>(RGB_CALLBACK_QUEUE_CAPACITY);
        let receiver = Arc::new(Mutex::new(receiver));
        for worker_index in 0..RGB_CALLBACK_WORKER_COUNT {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("zust-rgb-worker-{worker_index}"))
                .spawn(move || loop {
                    let job = {
                        let receiver = match receiver.lock() {
                            Ok(receiver) => receiver,
                            Err(_) => break,
                        };
                        match receiver.recv() {
                            Ok(job) => job,
                            Err(_) => break,
                        }
                    };
                    job();
                })
                .map_err(|err| format!("spawn RGB callback worker {worker_index}: {err}"))?;
        }
        Ok(sender)
    }) {
        Ok(sender) => Ok(sender),
        Err(err) => bail!("{err}"),
    }
}

fn spawn_rgb_callback_worker<F>(
    operation: &'static str,
    callback: &Dynamic,
    work: F,
) -> Result<Dynamic>
where
    F: FnOnce() -> Result<Dynamic> + Send + 'static,
{
    let Some(callback) = callback.as_custom::<ZustCallback>().cloned() else {
        bail!("callback must be a Zust callback, for example `|result| {{ ... }}`");
    };
    let job: RgbCallbackJob = Box::new(move || {
        let result = work().unwrap_or_else(|err| {
            json_to_dynamic(&json!({
                "ok": false,
                "module": "rgb",
                "operation": operation,
                "error": format!("{err:#}")
            }))
        });
        if let Err(err) = callback.call1(result) {
            eprintln!("[zust-console] rgb::{operation} callback failed: {err:#}");
        }
    });
    let queue = rgb_callback_queue()?;
    match queue.try_send(job) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            bail!("RGB callback worker queue is full; retry later")
        }
        Err(TrySendError::Disconnected(_)) => {
            bail!("RGB callback worker queue is disconnected")
        }
    }
    Ok(ok(json!({
        "module": "rgb",
        "operation": operation,
        "status": "queued"
    })))
}

extern "C" fn rgb_signed(input: *const Dynamic, callback: *const Dynamic) -> *const Dynamic {
    let input = unsafe { &*input };
    let callback = unsafe { &*callback };
    native_result(|| {
        let input = input.deep_clone();
        spawn_rgb_callback_worker("signed", callback, move || signed_request(&input, ""))
    })
}

extern "C" fn rgb_request(
    route: *const Dynamic,
    payload: *const Dynamic,
    callback: *const Dynamic,
) -> *const Dynamic {
    let route = unsafe { &*route };
    let payload = unsafe { &*payload };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(route.is_str(), "route must be string");
        let route = route.as_str().to_string();
        let payload = payload.deep_clone();
        spawn_rgb_callback_worker("request", callback, move || {
            rgb_post_dynamic(&payload, &route)
        })
    })
}

extern "C" fn rgb_rna_balance(callback: *const Dynamic) -> *const Dynamic {
    let callback = unsafe { &*callback };
    native_result(|| {
        spawn_rgb_callback_worker("rna_balance", callback, move || {
            rgb_post_dynamic(&Dynamic::Null, "/v1/rna/balance")
        })
    })
}
extern "C" fn rgb_issue(
    ticker: *const Dynamic,
    name: *const Dynamic,
    precision: u8,
    supply: u64,
    allocation_outpoint: *const Dynamic,
    callback: *const Dynamic,
) -> *const Dynamic {
    let ticker = unsafe { &*ticker };
    let name = unsafe { &*name };
    let allocation_outpoint = unsafe { &*allocation_outpoint };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(ticker.is_str(), "ticker must be string");
        ensure!(name.is_str(), "name must be string");
        ensure!(
            allocation_outpoint.is_str(),
            "allocation_outpoint must be string"
        );
        let ticker = ticker.as_str().to_string();
        let name = name.as_str().to_string();
        let allocation_outpoint = allocation_outpoint.as_str().trim().to_string();
        spawn_rgb_callback_worker("issue", callback, move || {
            let node = current_ln_node()
                .context("LN RGB node is not running; rgb::issue uses the LN hot wallet")?;
            let account_id = node.account_id().to_string();
            let utxos = scan_utxos_json(&account_id, &btc_esplora_url())?;
            let utxo_items = utxos.as_array().cloned().unwrap_or_default();
            let selected_outpoint = if allocation_outpoint.is_empty() {
                utxo_items
                    .iter()
                    .find(|utxo| {
                        utxo.get("confirmed")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                    })
                    .and_then(|utxo| utxo.get("outpoint").and_then(Value::as_str))
                    .map(str::to_string)
                    .with_context(|| {
                        format!("no confirmed BTC UTXO found for RGB issue account {account_id}")
                    })?
            } else {
                allocation_outpoint.clone()
            };
            ensure!(
                utxo_items.iter().any(|utxo| {
                    utxo.get("outpoint")
                        .and_then(Value::as_str)
                        .map(|outpoint| outpoint == selected_outpoint)
                    .unwrap_or(false)
                }),
                "allocation_outpoint {selected_outpoint} is not in RGB issue account {account_id} UTXOs"
            );
            let tracked_utxos =
                serde_json::from_value(utxos).context("decode RGB issue account UTXOs")?;
            let response = node.issue_rgb_asset(
                ticker,
                name,
                precision,
                supply,
                selected_outpoint,
                tracked_utxos,
            )?;
            Ok(json_to_dynamic(&serde_json::to_value(response)?))
        })
    })
}
extern "C" fn rgb_assets(callback: *const Dynamic) -> *const Dynamic {
    let callback = unsafe { &*callback };
    native_result(|| {
        spawn_rgb_callback_worker("assets", callback, move || {
            rgb_post_dynamic(&Dynamic::Null, "/v1/assets/list")
        })
    })
}
extern "C" fn rgb_assets_by_utxo(
    outpoint: *const Dynamic,
    address: *const Dynamic,
    confirmed: bool,
    callback: *const Dynamic,
) -> *const Dynamic {
    let outpoint = unsafe { &*outpoint };
    let address = unsafe { &*address };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(outpoint.is_str(), "outpoint must be string");
        ensure!(address.is_str(), "address must be string");
        let outpoint = outpoint.as_str().trim().to_string();
        let address = address.as_str().trim().to_string();
        ensure!(!outpoint.is_empty(), "outpoint must not be empty");
        spawn_rgb_callback_worker("assets_by_utxo", callback, move || {
            let payload = json_to_dynamic(&json!({
                "account_id": address.clone(),
                "outpoint": outpoint,
                "address": if address.is_empty() { Value::Null } else { json!(address) },
                "confirmed": confirmed
            }));
            rgb_public_post_dynamic(&payload, "/v1/assets/by-utxo")
        })
    })
}
fn rgb_assets_by_utxo_json(outpoint: &str, address: &str, confirmed: bool) -> Result<Value> {
    let payload = json_to_dynamic(&json!({
        "account_id": address,
        "outpoint": outpoint,
        "address": if address.is_empty() { Value::Null } else { json!(address) },
        "confirmed": confirmed
    }));
    let response = dynamic_to_json(&rgb_public_post_dynamic(&payload, "/v1/assets/by-utxo")?);
    if response.get("ok").and_then(Value::as_bool) == Some(false) {
        let error = response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("RGB daemon UTXO query failed");
        bail!("{error}");
    }
    if let Some(error) = response
        .get("error")
        .and_then(Value::as_str)
        .filter(|error| !error.trim().is_empty())
    {
        bail!("{error}");
    }
    let returned_outpoint = response
        .get("outpoint")
        .and_then(Value::as_str)
        .context("RGB daemon UTXO query response missing outpoint")?;
    ensure!(
        returned_outpoint == outpoint,
        "RGB daemon UTXO query returned mismatched outpoint: requested={outpoint} returned={returned_outpoint}"
    );
    Ok(response)
}

extern "C" fn rgb_assets_by_utxo_sync(
    outpoint: *const Dynamic,
    address: *const Dynamic,
    confirmed: bool,
) -> *const Dynamic {
    let outpoint = unsafe { &*outpoint };
    let address = unsafe { &*address };
    native_result(|| {
        ensure!(outpoint.is_str(), "outpoint must be string");
        ensure!(address.is_str(), "address must be string");
        let outpoint = outpoint.as_str().trim();
        let address = address.as_str().trim();
        ensure!(!outpoint.is_empty(), "outpoint must not be empty");
        Ok(json_to_dynamic(&rgb_assets_by_utxo_json(
            outpoint, address, confirmed,
        )?))
    })
}

extern "C" fn rgb_assert_psbt_no_assets(input: *const Dynamic) -> *const Dynamic {
    let input = unsafe { &*input };
    native_result(|| {
        ensure!(input.is_str(), "signed_psbt must be string");
        let signed_psbt = input.as_str().trim();
        ensure!(!signed_psbt.is_empty(), "signed_psbt must not be empty");
        let psbt = Psbt::from_str(signed_psbt).context("decode signed PSBT base64")?;
        let mut checked = Vec::with_capacity(psbt.unsigned_tx.input.len());
        for (index, txin) in psbt.unsigned_tx.input.iter().enumerate() {
            let outpoint = txin.previous_output.to_string();
            let psbt_input = psbt
                .inputs
                .get(index)
                .with_context(|| format!("signed PSBT input metadata missing at index {index}"))?;
            let previous_output = psbt_input
                .witness_utxo
                .as_ref()
                .or_else(|| {
                    psbt_input
                        .non_witness_utxo
                        .as_ref()
                        .and_then(|transaction| {
                            transaction.output.get(txin.previous_output.vout as usize)
                        })
                })
                .with_context(|| {
                    format!("signed PSBT input {outpoint} is missing previous output metadata")
                })?;
            let address = Address::from_script(&previous_output.script_pubkey, Network::Bitcoin)
                .with_context(|| format!("derive source address for signed PSBT input {outpoint}"))?
                .to_string();
            let response = rgb_assets_by_utxo_json(&outpoint, &address, true)
                .with_context(|| format!("check RGB allocations for PSBT input {outpoint}"))?;
            let allocations = response
                .get("allocations")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            ensure!(
                allocations.is_empty(),
                "BTC sweep PSBT contains RGB-bearing outpoint {outpoint}"
            );
            checked.push(json!({
                "outpoint": outpoint,
                "address": address,
                "allocation_count": 0
            }));
        }
        Ok(ok(json!({
            "module": "rgb",
            "operation": "assert_psbt_no_assets",
            "safe": true,
            "input_count": checked.len(),
            "inputs": checked
        })))
    })
}

extern "C" fn rgb_scan_utxos(address: *const Dynamic) -> *const Dynamic {
    let address = unsafe { &*address };
    native_result(|| {
        ensure!(address.is_str(), "address must be string");
        let address = address.as_str().trim().to_string();
        ensure!(!address.is_empty(), "address must not be empty");
        let esplora = btc_esplora_url();
        let utxos = scan_utxos_json(&address, &esplora)?;
        let recorded = record_daemon_account_utxos(&address, &utxos)?;
        let count = utxos.as_array().map(Vec::len).unwrap_or_default();
        Ok(ok(json!({
            "module": "rgb",
            "operation": "scan_utxos",
            "account_id": address,
            "address": address,
            "count": count,
            "recorded": recorded,
            "utxos": utxos
        })))
    })
}

extern "C" fn rgb_token_list() -> *const Dynamic {
    native_result(|| {
        let response = http_get_json(&daemon_route_url("/v1/tokens/list")?)?;
        Ok(json_to_dynamic(&response))
    })
}
extern "C" fn rgb_balance(
    asset_id: *const Dynamic,
    scope: *const Dynamic,
    callback: *const Dynamic,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let scope = unsafe { &*scope };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(scope.is_str(), "scope must be string");
        let asset_id = asset_id.as_str().to_string();
        let scope = if scope.as_str().trim().is_empty() {
            "all".to_string()
        } else {
            scope.as_str().to_string()
        };
        spawn_rgb_callback_worker("balance", callback, move || {
            rgb_post_dynamic(
                &json_to_dynamic(&json!({
                    "asset_id": asset_id,
                    "scope": scope
                })),
                "/v1/balance",
            )
        })
    })
}
extern "C" fn rgb_balance_breakdown(
    asset_id: *const Dynamic,
    callback: *const Dynamic,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        let asset_id = asset_id.as_str().to_string();
        spawn_rgb_callback_worker("balance_breakdown", callback, move || {
            rgb_post_dynamic(
                &json_to_dynamic(&json!({
                    "asset_id": asset_id
                })),
                "/v1/balance/breakdown",
            )
        })
    })
}
extern "C" fn rgb_prepare_transfer(
    asset_id: *const Dynamic,
    amount: u64,
    recipient: *const Dynamic,
    unsigned_anchor_psbt: *const Dynamic,
    change_vout: u32,
    recipient_vout: u32,
    fee_rate_sat_vb: u64,
    callback: *const Dynamic,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let recipient = unsafe { &*recipient };
    let unsigned_anchor_psbt = unsafe { &*unsigned_anchor_psbt };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(
            unsigned_anchor_psbt.is_str(),
            "unsigned_anchor_psbt must be string"
        );
        ensure!(amount > 0, "amount must be greater than zero");
        let asset_id = asset_id.as_str().trim().to_string();
        let recipient = recipient.as_str().trim().to_string();
        let unsigned_anchor_psbt = unsigned_anchor_psbt.as_str().trim().to_string();
        ensure!(!asset_id.is_empty(), "asset_id must not be empty");
        ensure!(!recipient.is_empty(), "recipient must not be empty");
        ensure!(
            !unsigned_anchor_psbt.is_empty(),
            "unsigned_anchor_psbt must not be empty"
        );
        let fee_rate_sat_vb = (fee_rate_sat_vb > 0).then_some(fee_rate_sat_vb);
        spawn_rgb_callback_worker("prepare_transfer", callback, move || {
            let expires_at_ms = now_ms() + 300000;
            let auth_body = json!({
                "account_id": default_account_id()?,
                "permission": "prepare_transfer",
                "payload": {
                    "account_id": default_account_id()?,
                    "asset_id": asset_id.clone(),
                    "amount": amount,
                    "purpose": "l1_transfer",
                    "recipient": recipient.clone(),
                    "anchor_psbt": unsigned_anchor_psbt.clone(),
                    "expires_at_ms": expires_at_ms
                },
                "domain": "bihelix-rgb-service",
                "expires_at_ms": expires_at_ms,
                "timestamp_ms": now_ms()
            });
            let asset_authorization = json!({
                "asset_id": asset_id.clone(),
                "amount": amount,
                "purpose": "l1_transfer",
                "recipient": recipient.clone(),
                "anchor_psbt": unsigned_anchor_psbt.clone(),
                "expires_at_ms": expires_at_ms,
                "signature": external_signature_unavailable::<Value>()?
            });
            let payload = json!({
                "asset_id": asset_id,
                "amount": amount,
                "recipient": recipient,
                "fee_rate_sat_vb": fee_rate_sat_vb,
                "unsigned_anchor_psbt": unsigned_anchor_psbt,
                "change_vout": change_vout,
                "recipient_vout": recipient_vout,
                "asset_authorization": asset_authorization
            });
            rgb_post_dynamic(&json_to_dynamic(&payload), "/v1/transfers/prepare")
        })
    })
}

extern "C" fn rgb_transfer(
    asset_id: *const Dynamic,
    amount: u64,
    recipient: *const Dynamic,
    unsigned_anchor_psbt: *const Dynamic,
    change_vout: u32,
    recipient_vout: u32,
    fee_rate_sat_vb: u64,
    callback: *const Dynamic,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let recipient = unsafe { &*recipient };
    let unsigned_anchor_psbt = unsafe { &*unsigned_anchor_psbt };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(
            unsigned_anchor_psbt.is_str(),
            "unsigned_anchor_psbt must be string"
        );
        ensure!(amount > 0, "amount must be greater than zero");
        let asset_id = asset_id.as_str().trim().to_string();
        let recipient = recipient.as_str().trim().to_string();
        let unsigned_anchor_psbt = unsigned_anchor_psbt.as_str().trim().to_string();
        ensure!(!asset_id.is_empty(), "asset_id must not be empty");
        ensure!(!recipient.is_empty(), "recipient must not be empty");
        ensure!(
            !unsigned_anchor_psbt.is_empty(),
            "unsigned_anchor_psbt must not be empty"
        );
        let fee_rate_sat_vb = (fee_rate_sat_vb > 0).then_some(fee_rate_sat_vb);
        spawn_rgb_callback_worker("transfer", callback, move || {
            let expires_at_ms = now_ms() + 300000;
            let prepare_auth_body = json!({
                "account_id": default_account_id()?,
                "permission": "prepare_transfer",
                "payload": {
                    "account_id": default_account_id()?,
                    "asset_id": asset_id.clone(),
                    "amount": amount,
                    "purpose": "l1_transfer",
                    "recipient": recipient.clone(),
                    "anchor_psbt": unsigned_anchor_psbt.clone(),
                    "expires_at_ms": expires_at_ms
                },
                "domain": "bihelix-rgb-service",
                "expires_at_ms": expires_at_ms,
                "timestamp_ms": now_ms()
            });
            let prepare_asset_authorization = json!({
                "asset_id": asset_id.clone(),
                "amount": amount,
                "purpose": "l1_transfer",
                "recipient": recipient.clone(),
                "anchor_psbt": unsigned_anchor_psbt.clone(),
                "expires_at_ms": expires_at_ms,
                "signature": external_signature_unavailable::<Value>()?
            });
            let prepare_payload = Dynamic::map(Default::default());
            prepare_payload.insert("asset_id", asset_id.clone());
            prepare_payload.insert("amount", amount);
            prepare_payload.insert("recipient", recipient.clone());
            if let Some(fee_rate_sat_vb) = fee_rate_sat_vb {
                prepare_payload.insert("fee_rate_sat_vb", fee_rate_sat_vb);
            }
            prepare_payload.insert("unsigned_anchor_psbt", unsigned_anchor_psbt.clone());
            prepare_payload.insert("change_vout", change_vout);
            prepare_payload.insert("recipient_vout", recipient_vout);
            prepare_payload.insert(
                "asset_authorization",
                json_to_dynamic(&prepare_asset_authorization),
            );
            let prepare = dynamic_to_json(&rgb_post_dynamic(
                &prepare_payload,
                "/v1/transfers/prepare",
            )?);
            let transfer_id = match prepare.get("transfer_id") {
                Some(Value::String(value)) if !value.trim().is_empty() => value.clone(),
                Some(Value::Number(value)) => value.to_string(),
                _ => bail!("RGB prepare response missing transfer_id: {prepare}"),
            };
            let prepared_anchor_psbt_hex = prepare
                .get("anchor_psbt")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .with_context(|| format!("RGB prepare response missing anchor_psbt: {prepare}"))?
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>();
            ensure!(
                prepared_anchor_psbt_hex.len() % 2 == 0,
                "prepared anchor PSBT hex must have even length"
            );
            let mut prepared_anchor_psbt_bytes =
                Vec::with_capacity(prepared_anchor_psbt_hex.len() / 2);
            for (index, chunk) in prepared_anchor_psbt_hex
                .as_bytes()
                .chunks_exact(2)
                .enumerate()
            {
                let hex =
                    std::str::from_utf8(chunk).context("prepared anchor PSBT hex is not UTF-8")?;
                prepared_anchor_psbt_bytes.push(u8::from_str_radix(hex, 16).with_context(
                    || format!("invalid prepared anchor PSBT hex at byte {index}"),
                )?);
            }
            let prepared_anchor_psbt_base64 = Psbt::deserialize(&prepared_anchor_psbt_bytes)
                .context("decode prepared anchor PSBT hex")?
                .to_string();
            let sign = external_signature_unavailable::<Value>()?;
            let signed_anchor_psbt = ["psbt", "signed_psbt", "signed_anchor_psbt"]
                .iter()
                .find_map(|key| sign.get(*key).and_then(Value::as_str))
                .filter(|value| !value.trim().is_empty())
                .with_context(|| format!("signer PSBT response missing signed psbt: {sign}"))?
                .to_string();
            let (raw_tx, extracted_txid) =
                extract_transaction_hex_from_signed_psbt(&signed_anchor_psbt)?;
            let broadcast = broadcast_raw_transaction_json(&raw_tx)?;
            let broadcast_txid = broadcast
                .get("txid")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(extracted_txid.as_str())
                .to_string();
            ensure!(
                broadcast_txid == extracted_txid,
                "broadcast txid mismatch: extracted={extracted_txid}, broadcast={broadcast_txid}"
            );

            let commit_expires_at_ms = now_ms() + 300000;
            let commit_auth_body = json!({
                "account_id": default_account_id()?,
                "permission": "commit_transfer",
                "payload": {
                    "account_id": default_account_id()?,
                    "asset_id": asset_id.clone(),
                    "amount": amount,
                    "purpose": "l1_transfer",
                    "recipient": Value::Null,
                    "anchor_psbt": signed_anchor_psbt.clone(),
                    "expires_at_ms": commit_expires_at_ms
                },
                "domain": "bihelix-rgb-service",
                "expires_at_ms": commit_expires_at_ms,
                "timestamp_ms": now_ms()
            });
            let commit_asset_authorization = json!({
                "asset_id": asset_id.clone(),
                "amount": amount,
                "purpose": "l1_transfer",
                "recipient": Value::Null,
                "anchor_psbt": signed_anchor_psbt.clone(),
                "expires_at_ms": commit_expires_at_ms,
                "signature": external_signature_unavailable::<Value>()?
            });
            let commit_payload = Dynamic::map(Default::default());
            commit_payload.insert("transfer_id", transfer_id.clone());
            commit_payload.insert("txid", broadcast_txid.clone());
            commit_payload.insert("signed_anchor_psbt", signed_anchor_psbt.clone());
            commit_payload.insert(
                "asset_authorization",
                json_to_dynamic(&commit_asset_authorization),
            );
            let commit =
                dynamic_to_json(&rgb_post_dynamic(&commit_payload, "/v1/transfers/commit")?);
            Ok(ok(json!({
                "module": "rgb",
                "operation": "transfer",
                "status": "committed",
                "asset_id": asset_id,
                "amount": amount,
                "recipient": recipient,
                "transfer_id": transfer_id,
                "txid": broadcast_txid,
                "raw_tx": raw_tx,
                "prepared_anchor_psbt": prepared_anchor_psbt_hex,
                "prepared_anchor_psbt_base64": prepared_anchor_psbt_base64,
                "signed_anchor_psbt": signed_anchor_psbt,
                "prepare": prepare,
                "sign": sign,
                "broadcast": broadcast,
                "commit": commit
            })))
        })
    })
}

extern "C" fn rgb_commit_transfer(
    asset_id: *const Dynamic,
    amount: u64,
    transfer_id: *const Dynamic,
    txid: *const Dynamic,
    signed_anchor_psbt: *const Dynamic,
    callback: *const Dynamic,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let transfer_id = unsafe { &*transfer_id };
    let txid = unsafe { &*txid };
    let signed_anchor_psbt = unsafe { &*signed_anchor_psbt };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(transfer_id.is_str(), "transfer_id must be string");
        ensure!(txid.is_str(), "txid must be string");
        ensure!(
            signed_anchor_psbt.is_str(),
            "signed_anchor_psbt must be string"
        );
        let asset_id = asset_id.as_str().to_string();
        let transfer_id = transfer_id.as_str().to_string();
        let txid = txid.as_str().to_string();
        let signed_anchor_psbt = (!signed_anchor_psbt.as_str().trim().is_empty())
            .then(|| signed_anchor_psbt.as_str().to_string());
        spawn_rgb_callback_worker("commit_transfer", callback, move || {
            let expires_at_ms = now_ms() + 300000;
            let auth_body = json!({
                "account_id": default_account_id()?,
                "permission": "commit_transfer",
                "payload": {
                    "account_id": default_account_id()?,
                    "asset_id": asset_id.clone(),
                    "amount": amount,
                    "purpose": "l1_transfer",
                    "recipient": Value::Null,
                    "anchor_psbt": signed_anchor_psbt.clone(),
                    "expires_at_ms": expires_at_ms
                },
                "domain": "bihelix-rgb-service",
                "expires_at_ms": expires_at_ms,
                "timestamp_ms": now_ms()
            });
            let asset_authorization = json!({
                "asset_id": asset_id.clone(),
                "amount": amount,
                "purpose": "l1_transfer",
                "recipient": Value::Null,
                "anchor_psbt": signed_anchor_psbt.clone(),
                "expires_at_ms": expires_at_ms,
                "signature": external_signature_unavailable::<Value>()?
            });
            let payload = json!({
                "transfer_id": transfer_id,
                "txid": txid,
                "signed_anchor_psbt": signed_anchor_psbt,
                "asset_authorization": asset_authorization
            });
            rgb_post_dynamic(&json_to_dynamic(&payload), "/v1/transfers/commit")
        })
    })
}
extern "C" fn rgb_test(scenario: *const Dynamic, callback: *const Dynamic) -> *const Dynamic {
    let scenario = unsafe { &*scenario };
    let callback = unsafe { &*callback };
    native_result(|| {
        ensure!(scenario.is_str(), "scenario must be string");
        let scenario = if scenario.as_str().trim().is_empty() {
            "full_rgb20_lifecycle".to_string()
        } else {
            scenario.as_str().to_string()
        };
        spawn_rgb_callback_worker("test", callback, move || {
            rgb_post_dynamic(
                &json_to_dynamic(&json!({
                    "scenario": scenario
                })),
                "/v1/test/rgb",
            )
        })
    })
}

extern "C" fn ln_rgb_status() -> *const Dynamic {
    native_result(|| {
        let node = current_ln_node();
        let (node_id, status, peers, channels, balances, storage_dir, network) =
            if let Some(node) = node.as_ref() {
                let balance = node.cached_balance_snapshot();
                (
                    node.node_id().to_string(),
                    node.status_summary(),
                    node.peer_snapshots().len(),
                    node.channel_snapshots().len(),
                    Some(balance),
                    ln_rgb_storage_dir().to_string_lossy().to_string(),
                    ln_rgb_network_name(),
                )
            } else {
                (
                    String::new(),
                    "stopped".to_string(),
                    0,
                    0,
                    None,
                    String::new(),
                    String::new(),
                )
            };
        Ok(ok(json!({
            "module": "ln_rgb",
            "enabled": true,
            "ln_rgb_lightning_linked": true,
            "ln_rgb_composer_bound": node.is_some(),
            "started": LN_STARTED.load(Ordering::SeqCst),
            "scanner_started": LN_SCANNER_STARTED.load(Ordering::SeqCst),
            "layers": ["l1", "l2"],
            "mode": "hot_wallet",
            "backend": "ln-rgb",
            "rgb_backend": "ln-rgb-lightning",
            "node_id": node_id,
            "status": status,
            "network": network,
            "storage_dir": storage_dir,
            "peer_count": peers,
            "channel_count": channels,
            "balances": balances.map(|balance| json!({
                "total_onchain_balance_sats": balance.total_onchain_balance_sats,
                "spendable_onchain_balance_sats": balance.spendable_onchain_balance_sats,
                "total_anchor_channels_reserve_sats": balance.total_anchor_channels_reserve_sats,
                "total_lightning_balance_sats": balance.total_lightning_balance_sats,
                "lightning_balances": balance.lightning_balances,
                "pending_channel_closure_sweeps": balance.pending_channel_closure_sweeps,
                "balance_source": "cached"
            }))
        })))
    })
}

extern "C" fn ln_rgb_start() -> *const Dynamic {
    native_result(|| {
        let lightning = local_dynamic("lightning").context(
            "missing root value `local/lightning`; run ln_rgb::node_address and root::add first",
        )?;
        let requested = dynamic_to_json(&lightning);
        let path = find_string_field(&requested, &["path"])
            .map(PathBuf::from)
            .unwrap_or_else(|| ln_node_path(&lightning));
        let stored = read_json_file(&path)
            .with_context(|| format!("read LN node state {}", path.display()))?;
        let address = find_string_field(&stored, &["address", "btc_address"]).unwrap_or_default();
        ensure!(
            !address.trim().is_empty(),
            "LN hot wallet address is missing"
        );
        let low_water_sats = value_u64(&stored, "low_water_sats").unwrap_or(LN_LOW_WATER_SATS);
        let config = normalized_ln_config(ln_config_from_value(&stored), low_water_sats);
        log_ln_initialization_paths("ln_rgb::start", &path, &stored, &config);
        let mnemonic = required_ln_entropy_mnemonic(&stored)?;
        let interval_ms = optional_u64(&lightning, "interval_ms")
            .or_else(|| value_u64(&stored, "interval_ms"))
            .unwrap_or_else(|| LN_SCAN_DEFAULT_INTERVAL.as_millis() as u64);
        let interval = Duration::from_millis(interval_ms.max(1000));
        let ln_config = console_ln_rgb_config(&config, mnemonic, address.clone())?;

        if LN_STARTED.swap(true, Ordering::SeqCst) {
            let node = current_ln_node();
            return Ok(ok(json!({
                "module": "ln_rgb",
                "started": true,
                "already_running": true,
                "address": address,
                "config": config,
                "source": "local/lightning",
                "backend": "ln-rgb",
                "rgb_backend": "ln-rgb-lightning",
                "ln_rgb_composer_bound": true,
                "node_id": node.as_ref().map(|node| node.node_id().to_string()).unwrap_or_default(),
                "listeners": ["l1_onchain_deposit", "l2_ln_deposit"],
                "scan_enabled": false
            })));
        }

        let node_handle = LnRgbBtcLnBackend::new_arc(ln_config);
        if let Err(err) = node_handle.start() {
            LN_STARTED.store(false, Ordering::SeqCst);
            return Err(err);
        }
        let node_id = node_handle.node_id().to_string();
        *ln_node_slot().lock().expect("LN node slot lock poisoned") =
            Some(Arc::clone(&node_handle));

        let thread_node = stored.clone();
        if let Err(err) = thread::Builder::new()
            .name("zust-ln-inbound-listener".to_string())
            .spawn(move || ln_inbound_loop(thread_node, interval))
        {
            let stop_result = node_handle.stop();
            *ln_node_slot().lock().expect("LN node slot lock poisoned") = None;
            LN_STARTED.store(false, Ordering::SeqCst);
            if let Err(stop_err) = stop_result {
                return Err(err)
                    .context("spawn LN inbound listener thread")
                    .context(stop_err);
            }
            return Err(err).context("spawn LN inbound listener thread");
        }

        Ok(ok(json!({
            "module": "ln_rgb",
            "started": true,
            "already_running": false,
            "address": address,
            "config": config,
            "source": "local/lightning",
            "backend": "ln-rgb",
            "rgb_backend": "ln-rgb-lightning",
            "ln_rgb_composer_bound": true,
            "node_id": node_id,
            "listeners": ["l1_onchain_deposit", "l2_ln_deposit"],
            "scan_enabled": false,
            "note": "LnRgbBtcLnBackend is running with patched ln-rgb-lightning ChannelManager"
        })))
    })
}

extern "C" fn ln_rgb_stop() -> *const Dynamic {
    native_result(|| {
        let node = ln_node_slot()
            .lock()
            .expect("LN node slot lock poisoned")
            .take();
        if let Some(node) = node {
            node.stop()?;
        }
        LN_STARTED.store(false, Ordering::SeqCst);
        Ok(ok(json!({
            "module": "ln_rgb",
            "stopped": true,
            "backend": "ln-rgb"
        })))
    })
}

extern "C" fn ln_rgb_retry_sweeps() -> *const Dynamic {
    native_result(|| {
        let node =
            current_ln_node().context("LN RGB node is not running; call ln_rgb::start() first")?;
        node.retry_pending_sweeps();
        Ok(ok(json!({
            "module": "ln_rgb",
            "operation": "retry_sweeps",
            "ok": true,
            "note": "processed pending LDK events and asked output sweeper to rebroadcast pending spends"
        })))
    })
}

extern "C" fn ln_rgb_assets() -> *const Dynamic {
    native_result(|| {
        let node = running_ln_node()?;
        let account_id = node.account_id().to_string();
        let response = node.list_rgb_assets()?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "account_id": account_id,
            "assets": response.assets,
            "utxo_assets": response.utxo_assets
        })))
    })
}

extern "C" fn ln_rgb_balance(asset_id: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(asset_id, |asset_id| {
        ensure!(!asset_id.trim().is_empty(), "asset_id must not be empty");
        let node = running_ln_node()?;
        let account_id = node.account_id().to_string();
        let balance = node.rgb_balance(asset_id.to_string())?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "account_id": account_id,
            "balance": balance
        })))
    })
}

fn ln_rgb_amount_snapshot(kind: &str) -> Result<Dynamic> {
    let node = running_ln_node()?;
    let balances = node.balance_snapshot();
    let amount = match kind {
        "btc" => balances.spendable_onchain_balance_sats,
        "ln" => balances.total_lightning_balance_sats,
        _ => balances
            .spendable_onchain_balance_sats
            .saturating_add(balances.total_lightning_balance_sats),
    };
    Ok(ok(json!({
        "module": "ln_rgb",
        "kind": kind,
        "amount_sats": amount
    })))
}

extern "C" fn ln_rgb_get_node_id() -> *const Dynamic {
    native_result(|| {
        let node = running_ln_node()?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "node_id": node.node_id().to_string()
        })))
    })
}

extern "C" fn ln_rgb_sign_message(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |message| {
        ensure!(!message.is_empty(), "message must not be empty");
        ensure!(
            message.len() <= 65_536,
            "message must not exceed 65536 bytes"
        );
        let node = running_ln_node()?;
        let signature = node.sign_node_message(message.as_bytes())?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "node_id": node.node_id().to_string(),
            "message": message,
            "signature": signature,
            "scheme": "ldk_lightning_signed_message",
            "prefix": "Lightning Signed Message:"
        })))
    })
}

extern "C" fn ln_rgb_get_addr() -> *const Dynamic {
    native_result(|| {
        Ok(ok(json!({
            "module": "ln_rgb",
            "address": current_ln_hot_address()?
        })))
    })
}

extern "C" fn ln_rgb_utxos() -> *const Dynamic {
    native_result(|| {
        let node = running_ln_node()?;
        Ok(ok(node.l1_utxos_json()?))
    })
}

extern "C" fn ln_rgb_sync_utxos() -> *const Dynamic {
    native_result(|| {
        let node = running_ln_node()?;
        Ok(ok(node.sync_l1_utxos_json()?))
    })
}

extern "C" fn ln_rgb_transfer_with_inputs(
    recipient: *const Dynamic,
    amount_sats: u64,
    fee_rate_sat_vb: u64,
    input_outpoints: *const Dynamic,
) -> *const Dynamic {
    let recipient = unsafe { &*recipient };
    let input_outpoints = unsafe { &*input_outpoints };
    native_result(|| {
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(input_outpoints.is_str(), "input_outpoints must be string");
        let node = running_ln_node()?;
        Ok(ok(node.transfer_l1_with_inputs_json(
            recipient.as_str(),
            amount_sats,
            fee_rate_sat_vb,
            input_outpoints.as_str(),
        )?))
    })
}

extern "C" fn ln_rgb_transfer_batch_with_inputs(
    outputs: *const Dynamic,
    fee_rate_sat_vb: u64,
    input_outpoints: *const Dynamic,
) -> *const Dynamic {
    let outputs = unsafe { &*outputs };
    let input_outpoints = unsafe { &*input_outpoints };
    native_result(|| {
        ensure!(outputs.is_str(), "outputs must be string");
        ensure!(input_outpoints.is_str(), "input_outpoints must be string");
        let node = running_ln_node()?;
        Ok(ok(node.transfer_l1_batch_with_inputs_json(
            outputs.as_str(),
            fee_rate_sat_vb,
            input_outpoints.as_str(),
        )?))
    })
}

extern "C" fn ln_rgb_transfer_rgb_l1(
    asset_id: *const Dynamic,
    amount: u64,
    recipient: *const Dynamic,
    fee_rate_sat_vb: u64,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let recipient = unsafe { &*recipient };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        let node = running_ln_node()?;
        let result = node.transfer_rgb_l1_json(
            asset_id.as_str(),
            amount,
            recipient.as_str(),
            fee_rate_sat_vb,
        )?;
        if result.get("ok").and_then(Value::as_bool) == Some(false) {
            Ok(json_to_dynamic(&result))
        } else {
            Ok(ok(result))
        }
    })
}

extern "C" fn ln_rgb_prepare_external_rgb_l1_sweep(
    asset_id: *const Dynamic,
    amount: u64,
    source_outpoint: *const Dynamic,
    source_address: *const Dynamic,
    fee_rate_sat_vb: u64,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let source_outpoint = unsafe { &*source_outpoint };
    let source_address = unsafe { &*source_address };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(source_outpoint.is_str(), "source_outpoint must be string");
        ensure!(source_address.is_str(), "source_address must be string");
        let source_outpoint_text = source_outpoint.as_str().trim();
        let source_address_text = source_address.as_str().trim();
        let source_outpoint = OutPoint::from_str(source_outpoint_text).with_context(|| {
            format!("invalid external RGB source outpoint: {source_outpoint_text}")
        })?;
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        let xpub_config = btc_address_xpub_config()?
            .context("BTC address xpub is required to describe the external RGB source input")?;
        let (public_key, fingerprint, derivation_path) =
            btc_xpub_input_key_source(&store, &xpub_config, source_address_text)?.with_context(
                || {
                    format!(
                "external RGB source address is not in the configured xpub: {source_address_text}"
            )
                },
            )?;
        let node = running_ln_node()?;
        let mut result = node.prepare_external_rgb_l1_sweep_json(
            asset_id.as_str(),
            amount,
            source_outpoint_text,
            source_address_text,
            fee_rate_sat_vb,
        )?;
        let psbt_text = result
            .get("partially_signed_psbt")
            .and_then(Value::as_str)
            .context("external RGB sweep prepare result missing PSBT")?;
        let mut psbt =
            Psbt::from_str(psbt_text).context("decode partially signed external RGB sweep PSBT")?;
        let source_input_index = psbt
            .unsigned_tx
            .input
            .iter()
            .position(|input| input.previous_output == source_outpoint)
            .context("external RGB source input is missing from partially signed PSBT")?;
        psbt.inputs[source_input_index]
            .bip32_derivation
            .insert(public_key, (fingerprint, derivation_path.clone()));
        let psbt_bytes = psbt.serialize();
        let psbt_text = psbt.to_string();
        let transfer_id = result
            .get("transfer_id")
            .and_then(Value::as_str)
            .context("external RGB sweep prepare result missing transfer_id")?;
        let safe_transfer_id = transfer_id
            .chars()
            .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
            .collect::<String>();
        ensure!(
            !safe_transfer_id.is_empty(),
            "external RGB sweep transfer_id cannot be used as an artifact name"
        );
        let artifact_dir = PathBuf::from("artifacts");
        fs::create_dir_all(&artifact_dir)?;
        let psbt_path = artifact_dir.join(format!(
            "rgb-custody-sweep-{safe_transfer_id}-partially-signed.psbt"
        ));
        fs::write(&psbt_path, &psbt_bytes)
            .with_context(|| format!("write Sparrow PSBT artifact {}", psbt_path.display()))?;
        restrict_private_file(&psbt_path)?;
        let psbt_sha256 = sha256::Hash::hash(&psbt_bytes).to_string();
        let object = result
            .as_object_mut()
            .context("external RGB sweep prepare result must be an object")?;
        object.insert("partially_signed_psbt".to_string(), json!(psbt_text));
        object.insert(
            "prepared_anchor_psbt".to_string(),
            json!(bytes_to_hex(&psbt_bytes)),
        );
        object.insert(
            "source_public_key".to_string(),
            json!(public_key.to_string()),
        );
        object.insert(
            "source_fingerprint".to_string(),
            json!(fingerprint.to_string()),
        );
        object.insert(
            "source_derivation_path".to_string(),
            json!(derivation_path.to_string()),
        );
        object.insert(
            "sparrow_action".to_string(),
            json!("sign_external_rgb_input_and_return_psbt_without_broadcasting"),
        );
        object.insert(
            "artifact_path".to_string(),
            json!(psbt_path.display().to_string()),
        );
        object.insert("artifact_sha256".to_string(), json!(psbt_sha256));
        let metadata_path = artifact_dir.join(format!(
            "rgb-custody-sweep-{safe_transfer_id}-metadata.json"
        ));
        object.insert(
            "metadata_path".to_string(),
            json!(metadata_path.display().to_string()),
        );
        write_private_json_file(&metadata_path, &result)?;
        Ok(ok(result))
    })
}

extern "C" fn ln_rgb_commit_rgb_l1_transfer(
    asset_id: *const Dynamic,
    amount: u64,
    transfer_id: *const Dynamic,
    txid: *const Dynamic,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let transfer_id = unsafe { &*transfer_id };
    let txid = unsafe { &*txid };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(transfer_id.is_str(), "transfer_id must be string");
        ensure!(txid.is_str(), "txid must be string");
        let node = running_ln_node()?;
        Ok(ok(node.commit_rgb_l1_transfer_json(
            asset_id.as_str(),
            amount,
            transfer_id.as_str(),
            txid.as_str(),
        )?))
    })
}

extern "C" fn ln_rgb_commit_external_rgb_l1_sweep(
    source_account_id: *const Dynamic,
    asset_id: *const Dynamic,
    amount: u64,
    transfer_id: *const Dynamic,
    txid: *const Dynamic,
) -> *const Dynamic {
    let source_account_id = unsafe { &*source_account_id };
    let asset_id = unsafe { &*asset_id };
    let transfer_id = unsafe { &*transfer_id };
    let txid = unsafe { &*txid };
    native_result(|| {
        ensure!(
            source_account_id.is_str(),
            "source_account_id must be string"
        );
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(transfer_id.is_str(), "transfer_id must be string");
        ensure!(txid.is_str(), "txid must be string");
        let node = running_ln_node()?;
        Ok(ok(node.commit_external_rgb_l1_sweep_json(
            source_account_id.as_str(),
            asset_id.as_str(),
            amount,
            transfer_id.as_str(),
            txid.as_str(),
        )?))
    })
}

extern "C" fn ln_rgb_get_peers() -> *const Dynamic {
    native_result(|| {
        let node = running_ln_node()?;
        let peers = node
            .peer_snapshots()
            .into_iter()
            .map(|peer| {
                json!({
                    "node_id": peer.node_id.to_string(),
                    "address": peer.address.to_string(),
                    "persisted": peer.is_persisted,
                    "connected": peer.is_connected
                })
            })
            .collect::<Vec<_>>();
        Ok(ok(json!({
            "module": "ln_rgb",
            "peers": peers
        })))
    })
}

extern "C" fn ln_rgb_get_channels() -> *const Dynamic {
    native_result(|| {
        let node = running_ln_node()?;
        let channels = node
            .channel_snapshots()
            .into_iter()
            .map(|channel| {
                json!({
                    "user_channel_id": channel.user_channel_id,
                    "counterparty_node_id": channel.counterparty_node_id.to_string(),
                    "channel_value_sats": channel.channel_value_sats,
                    "is_outbound": channel.is_outbound,
                    "is_channel_ready": channel.is_channel_ready,
                    "is_usable": channel.is_usable,
                    "channel_id": channel.channel_id,
                    "outbound_capacity_msat": channel.outbound_capacity_msat,
                    "next_outbound_htlc_limit_msat": channel.next_outbound_htlc_limit_msat,
                    "inbound_capacity_msat": channel.inbound_capacity_msat,
                    "funding_txo": channel.funding_txo
                })
            })
            .collect::<Vec<_>>();
        Ok(ok(json!({
            "module": "ln_rgb",
            "channels": channels
        })))
    })
}

extern "C" fn ln_rgb_connect(
    node_id: *const Dynamic,
    address: *const Dynamic,
    persist: bool,
) -> *const Dynamic {
    let node_id = unsafe { &*node_id };
    let address = unsafe { &*address };
    native_result(|| {
        let node = running_ln_node()?;
        ensure!(node_id.is_str(), "node_id must be string");
        ensure!(address.is_str(), "address must be string");
        let peer_node_id = ldk_public_key(node_id.as_str())?;
        let address = SocketAddress::from_str(address.as_str())
            .map_err(|_| anyhow::anyhow!("invalid LN peer address"))?;
        node.connect(peer_node_id, address.clone(), persist)
            .context("connect LN peer")?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "connected": true,
            "node_id": peer_node_id.to_string(),
            "address": address.to_string(),
            "persist": persist
        })))
    })
}

extern "C" fn ln_rgb_open_channel(
    node_id: *const Dynamic,
    address: *const Dynamic,
    amount_sats: u64,
    push_msat: u64,
) -> *const Dynamic {
    let node_id = unsafe { &*node_id };
    let address = unsafe { &*address };
    native_result(|| {
        let node = running_ln_node()?;
        ensure!(node_id.is_str(), "node_id must be string");
        ensure!(address.is_str(), "address must be string");
        let peer_node_id = ldk_public_key(node_id.as_str())?;
        let address = SocketAddress::from_str(address.as_str())
            .map_err(|_| anyhow::anyhow!("invalid LN peer address"))?;
        let push_msat = (push_msat > 0).then_some(push_msat);
        let channel_id = node
            .open_channel(BtcLnChannelOpenRequest {
                peer_node_id,
                address: address.clone(),
                amount_sats,
                push_msat,
            })
            .context("open LN channel")?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "channel_open_submitted": true,
            "channel_id": channel_id,
            "node_id": peer_node_id.to_string(),
            "address": address.to_string()
        })))
    })
}

extern "C" fn ln_rgb_close_channel(
    channel_id: *const Dynamic,
    counterparty_node_id: *const Dynamic,
    force: bool,
    reason: *const Dynamic,
) -> *const Dynamic {
    let channel_id = unsafe { &*channel_id };
    let counterparty_node_id = unsafe { &*counterparty_node_id };
    let reason = unsafe { &*reason };
    native_result(|| {
        let node = running_ln_node()?;
        ensure!(channel_id.is_str(), "channel_id must be string");
        ensure!(
            counterparty_node_id.is_str(),
            "counterparty_node_id must be string"
        );
        ensure!(reason.is_str(), "reason must be string");
        let channel_id = channel_id.as_str().to_string();
        let counterparty_node_id = ldk_public_key(counterparty_node_id.as_str())?;
        let reason = (!reason.as_str().trim().is_empty()).then(|| reason.as_str().to_string());
        node.close_channel(BtcLnChannelCloseRequest {
            channel_id: channel_id.clone(),
            counterparty_node_id,
            force,
            reason,
        })
        .context("close LN channel")?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "channel_close_submitted": true,
            "channel_id": channel_id,
            "counterparty_node_id": counterparty_node_id.to_string(),
            "force": force
        })))
    })
}

extern "C" fn ln_rgb_splice_btc(
    channel_id: *const Dynamic,
    counterparty_node_id: *const Dynamic,
    amount_sats: i64,
    funding_feerate_per_kw: u64,
    locktime: u64,
) -> *const Dynamic {
    let channel_id = unsafe { &*channel_id };
    let counterparty_node_id = unsafe { &*counterparty_node_id };
    native_result(|| {
        let node = running_ln_node()?;
        ensure!(channel_id.is_str(), "channel_id must be string");
        ensure!(
            counterparty_node_id.is_str(),
            "counterparty_node_id must be string"
        );
        ensure!(amount_sats != 0, "amount_sats must not be zero");
        let channel_id = channel_id.as_str().trim().to_string();
        let counterparty_node_id = ldk_public_key(counterparty_node_id.as_str())?;
        let funding_feerate_per_kw = if funding_feerate_per_kw > 0 {
            u32::try_from(funding_feerate_per_kw).context("funding_feerate_per_kw exceeds u32")?
        } else {
            2000
        };
        let locktime = (locktime > 0)
            .then(|| u32::try_from(locktime).context("locktime exceeds u32"))
            .transpose()?;
        node.splice_channel(BtcLnChannelSpliceRequest {
            channel_id: channel_id.clone(),
            counterparty_node_id,
            amount_sats,
            funding_feerate_per_kw,
            locktime,
        })
        .context("splice BTC into LN channel")?;
        let direction = if amount_sats > 0 { "in" } else { "out" };
        Ok(ok(json!({
            "module": "ln_rgb",
            "btc_splice_submitted": true,
            "direction": direction,
            "channel_id": channel_id,
            "counterparty_node_id": counterparty_node_id.to_string(),
            "amount_sats": amount_sats,
            "funding_feerate_per_kw": funding_feerate_per_kw,
            "locktime": locktime
        })))
    })
}

extern "C" fn ln_rgb_invoice(
    amount_msat: u64,
    description: *const Dynamic,
    expiry_secs: u64,
) -> *const Dynamic {
    let description = unsafe { &*description };
    native_result(|| {
        let node = running_ln_node()?;
        ensure!(description.is_str(), "description must be string");
        let description = if description.as_str().trim().is_empty() {
            "BiHelix LN invoice".to_string()
        } else {
            description.as_str().to_string()
        };
        let expiry_secs = (expiry_secs > 0).then_some(expiry_secs).unwrap_or(3600) as u32;
        let description = Bolt11InvoiceDescription::Direct(
            Description::new(description).map_err(|err| anyhow::anyhow!("{err:?}"))?,
        );
        let invoice = node
            .receive_bolt11(BtcLnBolt11InvoiceRequest {
                amount_msat,
                description,
                expiry_secs,
            })
            .context("create BOLT11 invoice")?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "invoice": invoice.to_string(),
            "payment_hash": invoice.payment_hash().to_string()
        })))
    })
}

extern "C" fn ln_rgb_invoice_for_ident(
    amount_msat: u64,
    description: *const Dynamic,
    expiry_secs: u64,
    ident: *const Dynamic,
) -> *const Dynamic {
    let description = unsafe { &*description };
    let ident = unsafe { &*ident };
    native_result(|| {
        let node = running_ln_node()?;
        ensure!(description.is_str(), "description must be string");
        ensure!(ident.is_str(), "ident must be string");
        let ident = ident.as_str().trim().to_string();
        ensure!(!ident.is_empty(), "ident must not be empty");
        let description = if description.as_str().trim().is_empty() {
            "BiHelix LN invoice".to_string()
        } else {
            description.as_str().to_string()
        };
        let expiry_secs = (expiry_secs > 0).then_some(expiry_secs).unwrap_or(3600) as u32;
        let description = Bolt11InvoiceDescription::Direct(
            Description::new(description).map_err(|err| anyhow::anyhow!("{err:?}"))?,
        );
        let invoice = node
            .receive_bolt11(BtcLnBolt11InvoiceRequest {
                amount_msat,
                description,
                expiry_secs,
            })
            .context("create BOLT11 invoice")?;
        let invoice_text = invoice.to_string();
        let payment_hash = invoice.payment_hash().to_string();
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        store.put_ident_ln_invoice(&ident, &invoice_text)?;
        store.put_ln_payment_hash_ident(&payment_hash, &ident)?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "invoice": invoice_text,
            "payment_hash": payment_hash,
            "ident": ident
        })))
    })
}

extern "C" fn ln_rgb_pay(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |invoice| {
        let node = running_ln_node()?;
        let invoice = Bolt11Invoice::from_str(invoice)
            .map_err(|err| anyhow::anyhow!("parse BOLT11 invoice: {err:?}"))?;
        let payment_hash = node
            .pay_bolt11(BtcLnBolt11PaymentRequest { invoice })
            .context("send BOLT11 payment")?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "payment_hash": payment_hash
        })))
    })
}

extern "C" fn ln_rgb_amount() -> *const Dynamic {
    native_result(|| ln_rgb_amount_snapshot("total"))
}

extern "C" fn ln_rgb_btc_amount() -> *const Dynamic {
    native_result(|| ln_rgb_amount_snapshot("btc"))
}

extern "C" fn ln_rgb_ln_amount() -> *const Dynamic {
    native_result(|| ln_rgb_amount_snapshot("ln"))
}

extern "C" fn ln_rgb_open_rgb_channel(
    node_id: *const Dynamic,
    address: *const Dynamic,
    capacity_sat: u64,
    push_msat: u64,
    user_channel_id: u64,
    contract_id: *const Dynamic,
    amount: u64,
) -> *const Dynamic {
    let node_id = unsafe { &*node_id };
    let address = unsafe { &*address };
    let contract_id = unsafe { &*contract_id };
    native_result(|| {
        let node = running_ln_node()?;
        ensure!(node_id.is_str(), "node_id must be string");
        ensure!(address.is_str(), "address must be string");
        ensure!(contract_id.is_str(), "contract_id must be string");
        let peer_node_id = ldk_public_key(node_id.as_str())?;
        if !address.as_str().trim().is_empty() {
            let address = SocketAddress::from_str(address.as_str())
                .map_err(|_| anyhow::anyhow!("invalid LN peer address"))?;
            node.connect(peer_node_id, address, true)?;
        }
        let user_channel_id = (user_channel_id > 0)
            .then_some(user_channel_id)
            .map(u128::from)
            .unwrap_or_else(now_ms_u128);
        let asset = WalletRgbAssetAmount {
            contract_id: rgbstd::ContractId::from_str(contract_id.as_str())
                .with_context(|| format!("invalid RGB contract_id: {}", contract_id.as_str()))?,
            amount,
        };
        let channel_id = node.open_rgb_channel(RgbChannelOpenRequest {
            peer_node_id,
            capacity_sat,
            push_msat,
            user_channel_id,
            asset,
        })?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "channel_id": bytes_to_hex(&channel_id.0),
            "peer_node_id": peer_node_id.to_string(),
            "capacity_sat": capacity_sat,
            "push_msat": push_msat,
            "user_channel_id": user_channel_id
        })))
    })
}

extern "C" fn ln_rgb_send_rgb_payment(
    recipient_node_id: *const Dynamic,
    amount_msat: u64,
    payment_id: *const Dynamic,
    contract_id: *const Dynamic,
    amount: u64,
) -> *const Dynamic {
    let recipient_node_id = unsafe { &*recipient_node_id };
    let payment_id = unsafe { &*payment_id };
    let contract_id = unsafe { &*contract_id };
    native_result(|| {
        let node = running_ln_node()?;
        ensure!(
            recipient_node_id.is_str(),
            "recipient_node_id must be string"
        );
        ensure!(payment_id.is_str(), "payment_id must be string");
        ensure!(contract_id.is_str(), "contract_id must be string");
        let recipient_node_id = ldk_public_key(recipient_node_id.as_str())?;
        let payment_id = if payment_id.as_str().trim().is_empty() {
            let mut bytes = [0u8; 32];
            getrandom::fill(&mut bytes).context("generate RGB-LN payment_id")?;
            bytes
        } else {
            hex32_to_bytes(payment_id.as_str())?
        };
        let asset = WalletRgbAssetAmount {
            contract_id: rgbstd::ContractId::from_str(contract_id.as_str())
                .with_context(|| format!("invalid RGB contract_id: {}", contract_id.as_str()))?,
            amount,
        };
        node.send_rgb_payment(RgbPaymentRequest {
            recipient_node_id,
            amount_msat,
            payment_id: WalletPaymentId(payment_id),
            asset,
        })?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "payment_id": bytes_to_hex(&payment_id),
            "recipient_node_id": recipient_node_id.to_string(),
            "amount_msat": amount_msat
        })))
    })
}

extern "C" fn ln_rgb_get_info() -> *const Dynamic {
    native_result(|| {
        let node = running_ln_node()?;
        let balances = node.balance_snapshot();
        let peers = node.peer_snapshots();
        let channels = node.channel_snapshots();
        Ok(ok(json!({
            "module": "ln_rgb",
            "backend": "ln-rgb",
            "rgb_backend": "ln-rgb-lightning",
            "runtime": "LnRgbBtcLnBackend",
            "node_id": node.node_id().to_string(),
            "account_id": node.account_id(),
            "status": node.status_summary(),
            "network": ln_rgb_network_name(),
            "storage_dir": ln_rgb_storage_dir().to_string_lossy().to_string(),
            "peer_count": peers.len(),
            "channel_count": channels.len(),
            "total_onchain_balance_sats": balances.total_onchain_balance_sats,
            "spendable_onchain_balance_sats": balances.spendable_onchain_balance_sats,
            "total_lightning_balance_sats": balances.total_lightning_balance_sats,
            "rgb_channel_methods_available": true
        })))
    })
}

extern "C" fn ln_rgb_spawn_scanner(interval_ms: u64) -> *const Dynamic {
    native_result(|| {
        let btc_addr = default_account_id()?;
        let rgb_service = local_string("rgb-service").unwrap_or_default();
        let interval_ms = (interval_ms > 0)
            .then_some(interval_ms)
            .unwrap_or_else(|| LN_SCAN_DEFAULT_INTERVAL.as_millis() as u64);
        let interval = Duration::from_millis(interval_ms.max(1000));
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        let available = store.list_btc_address_pool_records()?;
        let used = store.list_used_btc_address_pool_records()?;
        let address_pool = json!({
            "available": available.len(),
            "used": used.len(),
            "low_water": BTC_ADDRESS_POOL_LOW_WATER,
            "target": BTC_ADDRESS_POOL_TARGET,
            "added": 0,
            "addresses": available
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>()
        });

        if LN_SCANNER_STARTED.swap(true, Ordering::SeqCst) {
            return Ok(ok(json!({
                "module": "ln_rgb",
                "scanner_started": true,
                "already_running": true,
                "btc_addr": btc_addr,
                "rgb_service": rgb_service,
                "layers": ["l1", "l2"],
                "scan_enabled": true,
                "address_pool": address_pool
            })));
        }

        let thread_btc_addr = btc_addr.clone();
        let thread_rgb_service = rgb_service.clone();
        if let Err(err) = thread::Builder::new()
            .name("zust-ln-chain-scanner".to_string())
            .spawn(move || ln_scanner_loop(thread_btc_addr, thread_rgb_service, interval))
        {
            LN_SCANNER_STARTED.store(false, Ordering::SeqCst);
            return Err(err).context("spawn LN chain scanner thread");
        }

        Ok(ok(json!({
            "module": "ln_rgb",
            "scanner_started": true,
            "already_running": false,
            "btc_addr": btc_addr,
            "rgb_service": rgb_service,
            "layers": ["l1", "l2"],
            "accepts": ["l1_onchain_deposit", "l2_ln_deposit"],
            "scan_enabled": true,
            "address_pool": address_pool
        })))
    })
}

extern "C" fn ln_rgb_node_address(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let path = ln_node_path(input);
        let low_water_sats = optional_u64(input, "low_water_sats").unwrap_or(LN_LOW_WATER_SATS);
        let config = normalized_ln_config(dynamic_to_json(input), low_water_sats);
        if path.exists() {
            let stored = read_json_file(&path)
                .with_context(|| format!("read LN node state {}", path.display()))?;
            required_ln_entropy_mnemonic(&stored)?;
            let config = normalized_ln_config(ln_config_from_value(&stored), low_water_sats);
            log_ln_initialization_paths("ln_rgb::node_address", &path, &stored, &config);
            let address_source = stored
                .get("address_source")
                .and_then(Value::as_str)
                .unwrap_or_default();
            ensure!(
                address_source == "ln_hot_wallet",
                "LN node state {} has invalid address_source `{}`; existing state is read-only",
                path.display(),
                address_source
            );
            ensure!(
                find_string_field(&stored, &["address", "btc_address"])
                    .is_some_and(|address| !address.trim().is_empty()),
                "LN node state {} has no hot wallet address; existing state is read-only",
                path.display()
            );
            return Ok(ok(redacted_ln_node_response(stored, &path, false)));
        }

        let stored = json!({
            "version": 1,
            "kind": "ln_node_hot_wallet",
            "created_at_ms": now_ms(),
            "low_water_sats": low_water_sats,
            "config": config
        });
        let mut stored = stored;
        let mnemonic = ensure_ln_entropy_mnemonic(&mut stored)?;
        let config = stored
            .get("config")
            .cloned()
            .unwrap_or_else(|| normalized_ln_config(Value::Object(Map::new()), low_water_sats));
        log_ln_initialization_paths("ln_rgb::node_address", &path, &stored, &config);
        let network_name = config
            .get("network")
            .and_then(Value::as_str)
            .unwrap_or("bitcoin");
        let network = parse_ln_network(network_name)?;
        let data_dir = PathBuf::from(
            config
                .get("data_dir")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(LN_DATA_DIR_DEFAULT),
        );
        let mnemonic = BdkMnemonic::parse_in_normalized(BdkLanguage::English, &mnemonic)
            .context("invalid LN hot wallet mnemonic")?;
        let mut wallet = LocalWallet::open_with_mnemonic(&data_dir, network, &mnemonic)?;
        let address = wallet.wallet.reveal_next_address(KeychainKind::External);
        wallet.persist()?;
        if let Value::Object(object) = &mut stored {
            object.insert("address".to_string(), json!(address.address.to_string()));
            object.insert("address_source".to_string(), json!("ln_hot_wallet"));
            object.insert("wallet_can_sign".to_string(), json!(true));
        }
        write_private_json_file(&path, &stored)
            .with_context(|| format!("write LN node state {}", path.display()))?;
        Ok(ok(redacted_ln_node_response(stored, &path, true)))
    })
}

fn ln_node_slot() -> &'static Mutex<Option<Arc<LnRgbBtcLnBackend>>> {
    LN_RGB_NODE.get_or_init(|| Mutex::new(None))
}

pub fn current_ln_node() -> Option<Arc<LnRgbBtcLnBackend>> {
    ln_node_slot()
        .lock()
        .expect("LN node slot lock poisoned")
        .as_ref()
        .cloned()
}

fn running_ln_node() -> Result<Arc<LnRgbBtcLnBackend>> {
    current_ln_node().context("LN RGB node is not running; call ln_rgb::start() first")
}

fn console_ln_rgb_config(
    config: &Value,
    mnemonic: String,
    account_id: String,
) -> Result<BtcLnRuntimeConfig> {
    let network_name = config
        .get("network")
        .and_then(Value::as_str)
        .unwrap_or("bitcoin")
        .to_string();
    let network = parse_ln_network(&network_name)?;
    let l1_data_dir = PathBuf::from(
        config
            .get("data_dir")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(LN_DATA_DIR_DEFAULT),
    );
    let storage_dir = PathBuf::from(
        config
            .get("ldk_data_dir")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(LN_LDK_DATA_DIR_DEFAULT),
    );
    let listen = config
        .get("listen")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty());
    let trusted_peers_0conf = value_string_list(config, "trusted_peers_0conf")?;
    let esplora = config
        .get("chain_source")
        .and_then(|chain_source| {
            chain_source
                .get("url")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    chain_source
                        .get("urls")
                        .and_then(Value::as_array)
                        .and_then(|values| values.first())
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| LN_ESPLORA_DEFAULT.to_string());
    let esplora_urls = config
        .get("chain_source")
        .and_then(|chain_source| chain_source.get("urls"))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .filter(|value| !value.trim().is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| vec![esplora.clone()]);
    Ok(BtcLnRuntimeConfig {
        backend: BtcLnBackendKind::LnRgb,
        network,
        l1_data_dir,
        storage_dir,
        esplora,
        esplora_urls,
        esplora_api_key: config
            .get("chain_source")
            .and_then(|chain_source| chain_source.get("api_key"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|value| !value.trim().is_empty()),
        rgb_service_url: daemon_url()?,
        account_id,
        listen,
        entropy_mnemonic: Some(mnemonic),
        trusted_peers_0conf,
        accept_inbound_channels: config
            .get("accept_inbound_channels")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        announce_for_forwarding: config
            .get("announce_for_forwarding")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn parse_ln_network(value: &str) -> Result<Network> {
    match value {
        "bitcoin" | "mainnet" => Ok(Network::Bitcoin),
        "testnet" => Ok(Network::Testnet),
        "testnet4" => Ok(Network::Testnet4),
        "signet" => Ok(Network::Signet),
        "regtest" => Ok(Network::Regtest),
        _ => bail!("unsupported LN network `{value}`"),
    }
}

fn ldk_public_key(value: &str) -> Result<PublicKey> {
    PublicKey::from_str(value).with_context(|| format!("invalid LN node id: {value}"))
}

fn ensure_ln_entropy_mnemonic(stored: &mut Value) -> Result<String> {
    if let Some(mnemonic) = find_string_field(stored, &["entropy_mnemonic", "mnemonic"])
        .filter(|value| value != "<persisted>")
    {
        return Ok(mnemonic);
    }
    let mut entropy = [0u8; 16];
    getrandom::fill(&mut entropy).context("generate LN node entropy")?;
    let mnemonic = Bip39Mnemonic::from_entropy_in(Bip39Language::English, &entropy)
        .context("create LN node mnemonic")?
        .to_string();
    if let Value::Object(object) = stored {
        object.insert("entropy_mnemonic".to_string(), json!(mnemonic.clone()));
    }
    Ok(mnemonic)
}

fn required_ln_entropy_mnemonic(stored: &Value) -> Result<String> {
    find_string_field(stored, &["entropy_mnemonic", "mnemonic"])
        .filter(|value| value != "<persisted>" && !value.trim().is_empty())
        .context("LN node state has no persisted entropy mnemonic; existing state is read-only")
}

fn value_string_list(value: &Value, key: &str) -> Result<Vec<String>> {
    match value.get(key) {
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .with_context(|| format!("{key} entries must be strings"))
            })
            .collect(),
        Some(Value::String(item)) if !item.trim().is_empty() => Ok(vec![item.clone()]),
        Some(Value::Null) | None => Ok(Vec::new()),
        _ => bail!("{key} must be a string list"),
    }
}

extern "C" fn ln_rgb_scanner_status() -> *const Dynamic {
    native_result(|| {
        let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
        let available = store.list_btc_address_pool_records()?;
        let used = store.list_used_btc_address_pool_records()?;
        Ok(ok(json!({
            "module": "ln_rgb",
            "scanner_started": LN_SCANNER_STARTED.load(Ordering::SeqCst),
            "layers": ["l1", "l2"],
            "scan_enabled": true,
            "address_pool": {
                "available": available.len(),
                "used": used.len(),
                "low_water": BTC_ADDRESS_POOL_LOW_WATER,
                "target": BTC_ADDRESS_POOL_TARGET,
                "added": 0,
                "addresses": available
                    .into_iter()
                    .map(|(_, record)| record)
                    .collect::<Vec<_>>()
            }
        })))
    })
}

fn ln_scanner_loop(_btc_addr: String, _rgb_service: String, interval: Duration) {
    while LN_SCANNER_STARTED.load(Ordering::SeqCst) {
        let _ = (|| -> Result<()> {
            let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
            store.put_wallet_btc_address(&default_account_id()?)?;
            let esplora = btc_esplora_url();
            let mut addresses = Vec::new();
            if let Some(address) = store.get_wallet_btc_address()? {
                addresses.push((
                    "wallet".to_string(),
                    "default".to_string(),
                    String::new(),
                    address,
                ));
            }
            for (ident, address) in store.list_ident_btc_addresses()? {
                addresses.push(("ident".to_string(), String::new(), ident, address));
            }
            for (owner_type, owner_label, ident, address) in addresses {
                let utxos = btc_address_utxos_json(&address, &esplora)?;
                for utxo in utxos.as_array().cloned().unwrap_or_default() {
                    let txid = utxo
                        .get("txid")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let vout = utxo.get("vout").and_then(Value::as_u64).unwrap_or_default();
                    let status = utxo.get("status").unwrap_or(&Value::Null);
                    let confirmed = status
                        .get("confirmed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let record = json!({
                        "owner_type": owner_type,
                        "owner_label": owner_label,
                        "ident": ident,
                        "wallet_owner": owner_type == "wallet",
                        "address": address,
                        "txid": txid,
                        "vout": vout,
                        "outpoint": format!("{txid}:{vout}"),
                        "amount_sat": utxo.get("value").and_then(Value::as_u64).unwrap_or_default(),
                        "confirmed": confirmed,
                        "confirmations": 0,
                        "block_height": status.get("block_height").and_then(Value::as_u64),
                        "status": if confirmed { "confirmed" } else { "unconfirmed" },
                        "updated_at_ms": now_ms()
                    });
                    store.put_btc_deposit_record(
                        record
                            .get("outpoint")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                        &record,
                    )?;
                }
            }
            Ok(())
        })();
        thread::sleep(interval);
    }
}

fn ln_inbound_loop(_node: Value, interval: Duration) {
    while LN_STARTED.load(Ordering::SeqCst) {
        let _ = (|| -> Result<()> {
            let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
            let available = store.list_btc_address_pool_records()?.len();
            if available < BTC_ADDRESS_POOL_LOW_WATER {
                match refill_btc_address_pool_to_target(&store, "low_water_refill") {
                    Ok(added) => eprintln!(
                        "[zust-console] BTC address pool auto-refilled: available_before={available}, added={added}, low_water={BTC_ADDRESS_POOL_LOW_WATER}, target={BTC_ADDRESS_POOL_TARGET}"
                    ),
                    Err(err) => eprintln!(
                        "[zust-console] BTC address pool auto-refill failed: available={available}, low_water={BTC_ADDRESS_POOL_LOW_WATER}, target={BTC_ADDRESS_POOL_TARGET}; error={err:#}"
                    ),
                }
            }
            Ok(())
        })();
        thread::sleep(interval);
    }
}

fn rgb_post_dynamic(input: &Dynamic, route: &str) -> Result<Dynamic> {
    let options = request_options(input, route)?;
    let response = http_request_options(&options)?;
    Ok(json_to_dynamic(&response))
}

fn rgb_public_post_dynamic(input: &Dynamic, route: &str) -> Result<Dynamic> {
    let url = daemon_route_url(route)?;
    let response = http_post_json(&url, &dynamic_to_json(input))?;
    Ok(json_to_dynamic(&response))
}

fn request_options(input: &Dynamic, route: &str) -> Result<Dynamic> {
    let url = daemon_route_url(route)?;
    let request = Dynamic::map(Default::default());
    request.insert("method", "POST");
    request.insert("url", url);
    request.insert("json", signed_request(input, route)?);
    request.insert(
        "timeout_ms",
        optional_u64(input, "timeout_ms").unwrap_or(30000),
    );
    Ok(request)
}

fn daemon_route_url(route: &str) -> Result<String> {
    let daemon_url = daemon_url()?;
    Ok(format!("{}{}", daemon_url.trim_end_matches('/'), route))
}

fn http_request_options(options: &Dynamic) -> Result<Value> {
    let method = optional_string(options, "method").unwrap_or_else(|| "POST".to_string());
    ensure!(
        method.eq_ignore_ascii_case("POST"),
        "rgb daemon requests must use POST"
    );
    let url = required_string(options, "url")?;
    let body = options
        .get_dynamic("json")
        .map(|value| dynamic_to_json(&value))
        .context("missing request json body")?;
    http_post_json(&url, &body)
}

fn http_post_json(url: &str, body: &Value) -> Result<Value> {
    let body = serde_json::to_vec(body)?;
    let response = rgb_service_http_client()?
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .with_context(|| format!("POST {url}"))?;
    let status_code = response.status().as_u16();
    let body = response
        .text()
        .context("RGB service response is not UTF-8")?;
    let json = if body.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&body)
            .with_context(|| format!("decode RGB service JSON body: {body}"))?
    };
    if !(200..300).contains(&status_code) {
        bail!("RGB service {url} failed with HTTP {status_code}: {json}");
    }
    Ok(json)
}

fn http_get_json(url: &str) -> Result<Value> {
    let response = rgb_service_http_client()?
        .get(url)
        .send()
        .with_context(|| format!("GET {url}"))?;
    let status_code = response.status().as_u16();
    let body = response.bytes().context("read RGB service response body")?;
    let json = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body)
            .with_context(|| format!("decode RGB service JSON body from {url}"))?
    };
    if !(200..300).contains(&status_code) {
        bail!("RGB service {url} failed with HTTP {status_code}: {json}");
    }
    Ok(json)
}

fn btc_account_for_ident(ident: &str) -> Result<Value> {
    let ident = ident.trim();
    let root_account_id = default_account_id()?;
    if ident.is_empty() {
        return Ok(json!({
            "kind": "default",
            "ident": "",
            "account_id": root_account_id.clone(),
            "address": root_account_id,
            "source": "local/btc-addr",
            "signer_response": Value::Null
        }));
    }
    let store = LocalNodeStore::open(&btc_wallet_data_dir())?;
    let address = store.get_ident_btc_address(ident)?.with_context(|| {
        format!("unknown BTC ident `{ident}`; call btc::get_deposit_address first")
    })?;
    let pool_record = store
        .list_used_btc_address_pool_records()?
        .into_iter()
        .find_map(|(candidate, record)| (candidate == address).then_some(record))
        .unwrap_or(Value::Null);
    let signer_response = pool_record
        .get("signer_response")
        .cloned()
        .unwrap_or(Value::Null);
    Ok(json!({
        "kind": "derived",
        "ident": ident,
        "account_id": root_account_id,
        "address": address,
        "source": "local_address_pool",
        "derivation_path": signer_response
            .get("derivation_path")
            .cloned()
            .unwrap_or(Value::Null),
        "index": signer_response
            .get("index")
            .cloned()
            .unwrap_or(Value::Null),
        "script_pubkey": signer_response
            .get("script_pubkey")
            .cloned()
            .unwrap_or(Value::Null),
        "signer_response": signer_response
    }))
}

fn btc_balance_json(ident: &str) -> Result<Value> {
    let account = btc_account_for_ident(ident)?;
    let address = account
        .get("address")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let (esplora, stats) = btc_esplora_get_json_with_fallback(
        &format!("address/{address}"),
        &format!("fetch BTC L1 balance for {address}"),
    )?;
    let utxos = btc_address_utxos_json(&address, &esplora)?;
    let chain_funded = value_path_u64(&stats, &["chain_stats", "funded_txo_sum"]);
    let chain_spent = value_path_u64(&stats, &["chain_stats", "spent_txo_sum"]);
    let mempool_funded = value_path_u64(&stats, &["mempool_stats", "funded_txo_sum"]);
    let mempool_spent = value_path_u64(&stats, &["mempool_stats", "spent_txo_sum"]);
    let confirmed_sats = chain_funded.saturating_sub(chain_spent);
    let mempool_sats = mempool_funded.saturating_sub(mempool_spent);
    Ok(json!({
        "module": "btc",
        "ident": ident,
        "address": address,
        "account": account,
        "network": "bitcoin",
        "esplora": esplora,
        "confirmed_sats": confirmed_sats,
        "mempool_sats": mempool_sats,
        "total_sats": confirmed_sats + mempool_sats,
        "chain_stats": stats.get("chain_stats").cloned().unwrap_or(Value::Null),
        "mempool_stats": stats.get("mempool_stats").cloned().unwrap_or(Value::Null),
        "utxos": utxos
    }))
}

fn btc_assets_json(ident: &str) -> Result<Value> {
    let account = btc_account_for_ident(ident)?;
    let address = account
        .get("address")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if let Some(node) = current_ln_node() {
        if ident.trim().is_empty() || address == node.account_id() {
            return Ok(serde_json::to_value(node.list_rgb_assets()?)?);
        }
    }
    Ok(json!({
        "assets": [],
        "utxo_assets": {},
        "account_id": address,
        "ident": ident,
        "account": account,
        "source": "rgb_assets_unavailable_without_ln_hot_wallet"
    }))
}

fn btc_scan_deposit_address_with_deadline(
    ident: &str,
    address: &str,
    deadline: std::time::Instant,
) -> Result<Value> {
    const MAX_RETURNED_DEPOSITS: usize = 100;

    let network = parse_ln_network(&ln_rgb_network_name())?;
    Address::from_str(address)
        .with_context(|| format!("invalid BTC deposit address: {address}"))?
        .require_network(network)
        .with_context(|| format!("deposit address is not for {network:?}: {address}"))?;

    let mut errors = Vec::new();
    for source in btc_esplora_urls() {
        let base = source.trim_end_matches('/');
        if is_electrum_chain_source(base) {
            let attempt = (|| -> Result<Value> {
                let txs = electrum_address_txs_json_with_deadline(address, base, deadline)?;
                let utxos = electrum_address_utxos_json_with_deadline(address, base, deadline)?;
                let header = electrum_rpc_with_deadline(
                    base,
                    "blockchain.headers.subscribe",
                    json!([]),
                    deadline,
                )?;
                let tip_height = header
                    .get("height")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                btc_scan_deposit_records_from_chain_data(
                    ident,
                    address,
                    base,
                    tip_height,
                    txs,
                    utxos,
                )
            })();
            match attempt {
                Ok(result) => return Ok(result),
                Err(error) => errors.push(format!("{base}: {error:#}")),
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            continue;
        }
        let attempt = (|| -> Result<Value> {
            let txs = btc_timed_esplora_json(
                &format!("{base}/address/{address}/txs"),
                deadline,
            )?;
            let utxos = btc_timed_esplora_json(
                &format!("{base}/address/{address}/utxo"),
                deadline,
            )?;
            let tip_height = btc_timed_esplora_text(
                &format!("{base}/blocks/tip/height"),
                deadline,
            )?
            .trim()
            .parse::<u64>()
            .with_context(|| format!("invalid BTC tip height from {base}"))?;

            let mut seen = std::collections::BTreeSet::new();
            let mut deposits = Vec::new();
            for tx in txs.as_array().cloned().unwrap_or_default() {
                let txid = tx
                    .get("txid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                for (fallback_vout, output) in tx
                    .get("vout")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                {
                    if output
                        .get("scriptpubkey_address")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        != address
                    {
                        continue;
                    }
                    let vout = output
                        .get("n")
                        .and_then(Value::as_u64)
                        .unwrap_or(fallback_vout as u64);
                    let outpoint = format!("{txid}:{vout}");
                    seen.insert(outpoint.clone());
                    let status = tx.get("status").unwrap_or(&Value::Null);
                    let confirmed = status
                        .get("confirmed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let block_height = status.get("block_height").and_then(Value::as_u64);
                    let confirmations = block_height
                        .filter(|_| confirmed)
                        .map(|height| tip_height.saturating_sub(height).saturating_add(1))
                        .unwrap_or_default();
                    if deposits.len() < MAX_RETURNED_DEPOSITS {
                        deposits.push(json!({
                            "owner_type": "ident",
                            "owner_label": "",
                            "ident": ident,
                            "wallet_owner": false,
                            "address": address,
                            "txid": txid,
                            "vout": vout,
                            "outpoint": outpoint,
                            "amount_sat": output.get("value").and_then(Value::as_u64).unwrap_or_default(),
                            "confirmed": confirmed,
                            "confirmations": confirmations,
                            "block_height": block_height,
                            "status": if confirmed { "confirmed" } else { "unconfirmed" },
                            "updated_at_ms": now_ms()
                        }));
                    }
                }
            }

            for utxo in utxos.as_array().cloned().unwrap_or_default() {
                let txid = utxo
                    .get("txid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let vout = utxo.get("vout").and_then(Value::as_u64).unwrap_or_default();
                let outpoint = format!("{txid}:{vout}");
                if seen.contains(&outpoint) {
                    continue;
                }
                let status = utxo.get("status").unwrap_or(&Value::Null);
                let confirmed = status
                    .get("confirmed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let block_height = status.get("block_height").and_then(Value::as_u64);
                let confirmations = block_height
                    .filter(|_| confirmed)
                    .map(|height| tip_height.saturating_sub(height).saturating_add(1))
                    .unwrap_or_default();
                if deposits.len() < MAX_RETURNED_DEPOSITS {
                    deposits.push(json!({
                        "owner_type": "ident",
                        "owner_label": "",
                        "ident": ident,
                        "wallet_owner": false,
                        "address": address,
                        "txid": txid,
                        "vout": vout,
                        "outpoint": outpoint,
                        "amount_sat": utxo.get("value").and_then(Value::as_u64).unwrap_or_default(),
                        "confirmed": confirmed,
                        "confirmations": confirmations,
                        "block_height": block_height,
                        "status": if confirmed { "confirmed" } else { "unconfirmed" },
                        "updated_at_ms": now_ms()
                    }));
                }
            }

            Ok(json!({
                "deposits": deposits,
                "persisted": 0,
                "tip_height": tip_height,
                "esplora": base,
                "address": address,
                "ident": ident
            }))
        })();
        match attempt {
            Ok(result) => return Ok(result),
            Err(error) => errors.push(format!("{base}: {error:#}")),
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
    }

    if std::time::Instant::now() >= deadline {
        bail!("BTC address scan timed out after 5 seconds for {address}");
    }
    bail!(
        "BTC address scan failed for {address}: {}",
        if errors.is_empty() {
            "no HTTP Esplora endpoint configured".to_string()
        } else {
            errors.join(" | ")
        }
    )
}

fn btc_scan_deposit_records_from_chain_data(
    ident: &str,
    address: &str,
    source: &str,
    tip_height: u64,
    txs: Value,
    utxos: Value,
) -> Result<Value> {
    const MAX_RETURNED_DEPOSITS: usize = 100;

    let mut seen = std::collections::BTreeSet::new();
    let mut deposits = Vec::new();
    for tx in txs.as_array().cloned().unwrap_or_default() {
        let txid = tx
            .get("txid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        for (fallback_vout, output) in tx
            .get("vout")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .enumerate()
        {
            if output
                .get("scriptpubkey_address")
                .and_then(Value::as_str)
                .unwrap_or_default()
                != address
            {
                continue;
            }
            let vout = output
                .get("n")
                .and_then(Value::as_u64)
                .unwrap_or(fallback_vout as u64);
            let outpoint = format!("{txid}:{vout}");
            seen.insert(outpoint.clone());
            let status = tx.get("status").unwrap_or(&Value::Null);
            let confirmed = status
                .get("confirmed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let block_height = status.get("block_height").and_then(Value::as_u64);
            let confirmations = block_height
                .filter(|_| confirmed)
                .map(|height| tip_height.saturating_sub(height).saturating_add(1))
                .unwrap_or_default();
            if deposits.len() < MAX_RETURNED_DEPOSITS {
                deposits.push(json!({
                    "owner_type": "ident",
                    "owner_label": "",
                    "ident": ident,
                    "wallet_owner": false,
                    "address": address,
                    "txid": txid,
                    "vout": vout,
                    "outpoint": outpoint,
                    "amount_sat": output.get("value").and_then(Value::as_u64).unwrap_or_default(),
                    "confirmed": confirmed,
                    "confirmations": confirmations,
                    "block_height": block_height,
                    "status": if confirmed { "confirmed" } else { "unconfirmed" },
                    "updated_at_ms": now_ms()
                }));
            }
        }
    }

    for utxo in utxos.as_array().cloned().unwrap_or_default() {
        let txid = utxo
            .get("txid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let vout = utxo.get("vout").and_then(Value::as_u64).unwrap_or_default();
        let outpoint = format!("{txid}:{vout}");
        if seen.contains(&outpoint) {
            continue;
        }
        let status = utxo.get("status").unwrap_or(&Value::Null);
        let confirmed = status
            .get("confirmed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let block_height = status.get("block_height").and_then(Value::as_u64);
        let confirmations = block_height
            .filter(|_| confirmed)
            .map(|height| tip_height.saturating_sub(height).saturating_add(1))
            .unwrap_or_default();
        if deposits.len() < MAX_RETURNED_DEPOSITS {
            deposits.push(json!({
                "owner_type": "ident",
                "owner_label": "",
                "ident": ident,
                "wallet_owner": false,
                "address": address,
                "txid": txid,
                "vout": vout,
                "outpoint": outpoint,
                "amount_sat": utxo.get("value").and_then(Value::as_u64).unwrap_or_default(),
                "confirmed": confirmed,
                "confirmations": confirmations,
                "block_height": block_height,
                "status": if confirmed { "confirmed" } else { "unconfirmed" },
                "updated_at_ms": now_ms()
            }));
        }
    }

    Ok(json!({
        "deposits": deposits,
        "persisted": 0,
        "tip_height": tip_height,
        "esplora": source,
        "address": address,
        "ident": ident
    }))
}

fn electrum_address_txs_json_with_deadline(
    address: &str,
    source: &str,
    deadline: std::time::Instant,
) -> Result<Value> {
    let network = Network::Bitcoin;
    let address = Address::from_str(address)
        .with_context(|| format!("invalid BTC address: {address}"))?
        .require_network(network)
        .with_context(|| format!("address is not for {network:?}"))?;
    let script = address.script_pubkey();
    let script_hash = electrum_script_hash_hex(&script);
    // Scheduled address scans and withdrawal preflight only need transactions
    // containing currently spendable outputs. Fetching the complete history for
    // a heavily reused custody address can exceed the Electrum server's history
    // lookup limit and block every withdrawal from that address.
    let unspents = electrum_rpc_with_deadline(
        source,
        "blockchain.scripthash.listunspent",
        json!([script_hash]),
        deadline,
    )?;
    let mut txs = Vec::new();
    let mut seen_txids = std::collections::HashSet::new();
    for item in unspents.as_array().cloned().unwrap_or_default() {
        let txid = item
            .get("tx_hash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if txid.is_empty() || !seen_txids.insert(txid.clone()) {
            continue;
        }
        let raw = electrum_rpc_with_deadline(
            source,
            "blockchain.transaction.get",
            json!([txid]),
            deadline,
        )?;
        let raw_hex = raw
            .as_str()
            .with_context(|| format!("Electrum transaction.get returned non-string for {txid}"))?;
        let raw_tx = hex_to_bytes(raw_hex)?;
        let tx: Transaction = encode::deserialize(&raw_tx)
            .with_context(|| format!("decode Electrum raw transaction {txid}"))?;
        let block_height = item
            .get("height")
            .and_then(Value::as_i64)
            .filter(|height| *height > 0)
            .map(|height| height as u64);
        let confirmed = block_height.is_some();
        let vout = tx
            .output
            .iter()
            .enumerate()
            .map(|(n, output)| {
                let output_address = if output.script_pubkey == script {
                    address.to_string()
                } else {
                    Address::from_script(&output.script_pubkey, network)
                        .map(|address| address.to_string())
                        .unwrap_or_default()
                };
                json!({
                    "n": n,
                    "scriptpubkey": bytes_to_hex(output.script_pubkey.as_bytes()),
                    "scriptpubkey_address": output_address,
                    "value": output.value.to_sat()
                })
            })
            .collect::<Vec<_>>();
        txs.push(json!({
            "txid": txid,
            "vout": vout,
            "status": {
                "confirmed": confirmed,
                "block_height": block_height
            }
        }));
    }
    Ok(Value::Array(txs))
}

fn electrum_address_utxos_json_with_deadline(
    address: &str,
    source: &str,
    deadline: std::time::Instant,
) -> Result<Value> {
    let network = Network::Bitcoin;
    let address = Address::from_str(address)
        .with_context(|| format!("invalid BTC address: {address}"))?
        .require_network(network)
        .with_context(|| format!("address is not for {network:?}"))?;
    let script_hash = electrum_script_hash_hex(&address.script_pubkey());
    let unspent = electrum_rpc_with_deadline(
        source,
        "blockchain.scripthash.listunspent",
        json!([script_hash]),
        deadline,
    )?;
    let utxos = unspent
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|utxo| {
            let txid = utxo.get("tx_hash")?.as_str()?.to_string();
            let vout = utxo
                .get("tx_pos")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let amount = utxo
                .get("value")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let block_height = utxo
                .get("height")
                .and_then(Value::as_i64)
                .filter(|height| *height > 0)
                .map(|height| height as u64);
            Some(json!({
                "txid": txid,
                "vout": vout,
                "value": amount,
                "status": {
                    "confirmed": block_height.is_some(),
                    "block_height": block_height
                }
            }))
        })
        .collect::<Vec<_>>();
    Ok(Value::Array(utxos))
}

fn electrum_rpc_with_deadline(
    source: &str,
    method: &str,
    params: Value,
    deadline: std::time::Instant,
) -> Result<Value> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    ensure!(!remaining.is_zero(), "BTC address scan deadline exceeded");
    let endpoint = electrum_endpoint(source)?;
    let address = endpoint
        .to_socket_addrs()
        .with_context(|| format!("resolve Electrum endpoint {endpoint}"))?
        .next()
        .with_context(|| format!("Electrum endpoint {endpoint} resolved no addresses"))?;
    let connect_timeout = remaining.min(Duration::from_secs(BTC_ESPLORA_CONNECT_TIMEOUT_SECS));
    let mut stream = TcpStream::connect_timeout(&address, connect_timeout)
        .with_context(|| format!("connect Electrum endpoint {endpoint}"))?;
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    ensure!(!remaining.is_zero(), "BTC address scan deadline exceeded");
    stream
        .set_read_timeout(Some(remaining))
        .context("set Electrum read timeout")?;
    stream
        .set_write_timeout(Some(remaining))
        .context("set Electrum write timeout")?;
    let request = json!({
        "id": now_ms(),
        "method": method,
        "params": params
    });
    let request_line = format!("{}\n", serde_json::to_string(&request)?);
    stream
        .write_all(request_line.as_bytes())
        .with_context(|| format!("write Electrum request {method} to {endpoint}"))?;
    stream
        .flush()
        .with_context(|| format!("flush Electrum request {method} to {endpoint}"))?;
    let mut reader = BufReader::new(stream);
    let mut response_line = String::new();
    reader
        .read_line(&mut response_line)
        .with_context(|| format!("read Electrum response {method} from {endpoint}"))?;
    ensure!(
        !response_line.trim().is_empty(),
        "empty Electrum response for {method} from {endpoint}"
    );
    let response: Value = serde_json::from_str(&response_line)
        .with_context(|| format!("decode Electrum response for {method}: {response_line}"))?;
    if let Some(error) = response.get("error").filter(|error| !error.is_null()) {
        bail!("Electrum {method} failed: {error}");
    }
    response
        .get("result")
        .cloned()
        .context("Electrum response missing result")
}

fn btc_timed_esplora_json(url: &str, deadline: std::time::Instant) -> Result<Value> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    ensure!(!remaining.is_zero(), "BTC address scan deadline exceeded");
    let response = attohttpc::get(url)
        .timeout(remaining)
        .send()
        .with_context(|| format!("GET {url}"))?;
    ensure!(response.is_success(), "GET {url} returned HTTP {}", response.status());
    let body = response
        .text()
        .with_context(|| format!("decode text from {url}"))?;
    serde_json::from_str(&body).with_context(|| format!("decode JSON from {url}"))
}

fn btc_timed_esplora_text(url: &str, deadline: std::time::Instant) -> Result<String> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    ensure!(!remaining.is_zero(), "BTC address scan deadline exceeded");
    let response = attohttpc::get(url)
        .timeout(remaining)
        .send()
        .with_context(|| format!("GET {url}"))?;
    ensure!(response.is_success(), "GET {url} returned HTTP {}", response.status());
    response
        .text()
        .with_context(|| format!("decode text from {url}"))
}

fn btc_address_utxos_json(address: &str, esplora: &str) -> Result<Value> {
    if is_electrum_chain_source(esplora) {
        match electrum_address_utxos_json(address, esplora) {
            Ok(utxos) => return Ok(utxos),
            Err(primary_error) => {
                let mut errors = vec![format!("{esplora}: {primary_error:#}")];
                for fallback in btc_esplora_urls() {
                    if fallback.trim_end_matches('/') == esplora.trim_end_matches('/') {
                        continue;
                    }
                    let fallback = fallback.trim_end_matches('/').to_string();
                    let result = if is_electrum_chain_source(&fallback) {
                        electrum_address_utxos_json(address, &fallback)
                    } else {
                        esplora_get_json(&format!("{fallback}/address/{address}/utxo"))
                    };
                    match result {
                        Ok(utxos) => return Ok(utxos),
                        Err(err) => errors.push(format!("{fallback}: {err:#}")),
                    }
                }
                bail!(
                    "fetch BTC L1 UTXOs for {address}: all chain sources failed: {}",
                    errors.join(" | ")
                )
            }
        }
    }
    let path = format!("address/{address}/utxo");
    match esplora_get_json(&format!("{}/{}", esplora.trim_end_matches('/'), path)) {
        Ok(utxos) => Ok(utxos),
        Err(primary_error) => {
            let mut errors = vec![format!(
                "{}: {primary_error:#}",
                esplora.trim_end_matches('/')
            )];
            for fallback in btc_esplora_urls() {
                if fallback.trim_end_matches('/') == esplora.trim_end_matches('/') {
                    continue;
                }
                match esplora_get_json(&format!("{}/{}", fallback.trim_end_matches('/'), path)) {
                    Ok(utxos) => return Ok(utxos),
                    Err(err) => errors.push(format!("{}: {err:#}", fallback.trim_end_matches('/'))),
                }
            }
            bail!(
                "fetch BTC L1 UTXOs for {address}: all Esplora endpoints failed: {}",
                errors.join(" | ")
            )
        }
    }
}

fn btc_address_txs_json_with_fallback(address: &str, context: &str) -> Result<(String, Value)> {
    let mut errors = Vec::new();
    let path = format!("address/{address}/txs");
    for source in btc_esplora_urls() {
        let base = source.trim_end_matches('/').to_string();
        let result = if is_electrum_chain_source(&base) {
            electrum_address_txs_json(address, &base)
        } else {
            esplora_get_json(&format!("{base}/{path}"))
        };
        match result {
            Ok(json) => return Ok((base, json)),
            Err(err) => errors.push(format!("{base}: {err:#}")),
        }
    }
    bail!(
        "{context}: all BTC chain sources failed: {}",
        errors.join(" | ")
    )
}

fn electrum_address_txs_json(address: &str, source: &str) -> Result<Value> {
    let network = Network::Bitcoin;
    let address = Address::from_str(address)
        .with_context(|| format!("invalid BTC address: {address}"))?
        .require_network(network)
        .with_context(|| format!("address is not for {network:?}"))?;
    let script = address.script_pubkey();
    let script_hash = electrum_script_hash_hex(&script);
    let history = electrum_rpc(
        source,
        "blockchain.scripthash.get_history",
        json!([script_hash]),
    )?;
    let mut txs = Vec::new();
    for item in history.as_array().cloned().unwrap_or_default() {
        let txid = item
            .get("tx_hash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if txid.is_empty() {
            continue;
        }
        let raw = electrum_rpc(source, "blockchain.transaction.get", json!([txid]))?;
        let raw_hex = raw
            .as_str()
            .with_context(|| format!("Electrum transaction.get returned non-string for {txid}"))?;
        let raw_tx = hex_to_bytes(raw_hex)?;
        let tx: Transaction = encode::deserialize(&raw_tx)
            .with_context(|| format!("decode Electrum raw transaction {txid}"))?;
        let block_height = item
            .get("height")
            .and_then(Value::as_i64)
            .filter(|height| *height > 0)
            .map(|height| height as u64);
        let confirmed = block_height.is_some();
        let vout = tx
            .output
            .iter()
            .enumerate()
            .map(|(n, output)| {
                let output_address = if output.script_pubkey == script {
                    address.to_string()
                } else {
                    Address::from_script(&output.script_pubkey, network)
                        .map(|address| address.to_string())
                        .unwrap_or_default()
                };
                json!({
                    "n": n,
                    "scriptpubkey": bytes_to_hex(output.script_pubkey.as_bytes()),
                    "scriptpubkey_address": output_address,
                    "value": output.value.to_sat()
                })
            })
            .collect::<Vec<_>>();
        txs.push(json!({
            "txid": txid,
            "vout": vout,
            "status": {
                "confirmed": confirmed,
                "block_height": block_height
            }
        }));
    }
    Ok(Value::Array(txs))
}

fn electrum_address_utxos_json(address: &str, source: &str) -> Result<Value> {
    let network = Network::Bitcoin;
    let address = Address::from_str(address)
        .with_context(|| format!("invalid BTC address: {address}"))?
        .require_network(network)
        .with_context(|| format!("address is not for {network:?}"))?;
    let script_hash = electrum_script_hash_hex(&address.script_pubkey());
    let unspent = electrum_rpc(
        source,
        "blockchain.scripthash.listunspent",
        json!([script_hash]),
    )?;
    let utxos = unspent
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|utxo| {
            let txid = utxo.get("tx_hash")?.as_str()?.to_string();
            let vout = utxo
                .get("tx_pos")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let amount = utxo
                .get("value")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let block_height = utxo
                .get("height")
                .and_then(Value::as_i64)
                .filter(|height| *height > 0)
                .map(|height| height as u64);
            Some(json!({
                "txid": txid,
                "vout": vout,
                "value": amount,
                "status": {
                    "confirmed": block_height.is_some(),
                    "block_height": block_height
                }
            }))
        })
        .collect::<Vec<_>>();
    Ok(Value::Array(utxos))
}

fn scan_utxos_json(address: &str, esplora: &str) -> Result<Value> {
    let utxos = btc_address_utxos_json(address, esplora)?;
    let tracked = utxos
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|utxo| {
            let txid = utxo.get("txid")?.as_str()?;
            let vout = utxo.get("vout")?.as_u64()?;
            let confirmed = utxo
                .get("status")
                .and_then(|status| status.get("confirmed"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Some(json!({
                "outpoint": format!("{txid}:{vout}"),
                "address": address,
                "confirmed": confirmed
            }))
        })
        .collect::<Vec<_>>();
    Ok(Value::Array(tracked))
}

fn record_daemon_account_utxos(account_id: &str, utxos: &Value) -> Result<usize> {
    let data_dir = rgb_service_data_dir()?;
    let db_dir = data_dir.join("kv");
    ensure!(
        db_dir.is_dir(),
        "RGB service database not found at {}; set `local/rgb-service-data` to daemon service.data_dir on the SSH host",
        db_dir.display()
    );
    let db = SingleWriterTxDatabase::builder(&db_dir)
        .open()
        .with_context(|| format!("open RGB service database {}", db_dir.display()))?;
    let keyspace = db
        .keyspace("account_utxos", KeyspaceCreateOptions::default)
        .context("open RGB service account_utxos keyspace")?;
    let mut tx = db.write_tx();
    let mut recorded = 0usize;
    for utxo in utxos.as_array().cloned().unwrap_or_default() {
        let outpoint = utxo
            .get("outpoint")
            .and_then(Value::as_str)
            .context("scanned UTXO missing outpoint")?;
        OutPoint::from_str(outpoint).with_context(|| format!("invalid outpoint {outpoint}"))?;
        let key = format!("{account_id}:{outpoint}");
        let bytes = serde_json::to_vec(&utxo).context("encode account UTXO")?;
        tx.insert(&keyspace, key.as_bytes(), bytes);
        recorded += 1;
    }
    tx.commit().context("record RGB account UTXOs")?;
    db.persist(PersistMode::SyncAll)
        .context("persist RGB account UTXOs")?;
    Ok(recorded)
}

fn rgb_service_data_dir() -> Result<PathBuf> {
    local_string("rgb-service-data")
        .or_else(|| {
            local_dynamic("rgb-service-config")
                .map(|value| dynamic_to_json(&value))
                .and_then(|value| {
                    value
                        .get("service")
                        .and_then(|service| service.get("data_dir"))
                        .and_then(Value::as_str)
                        .or_else(|| value.get("data_dir").and_then(Value::as_str))
                        .map(str::to_string)
                })
        })
        .map(PathBuf::from)
        .context(
            "missing root value `local/rgb-service-data`; set it to daemon service.data_dir on the SSH host",
        )
}

fn esplora_http_client() -> Result<&'static reqwest::blocking::Client> {
    if let Some(client) = ESPLORA_HTTP_CLIENT.get() {
        return Ok(client);
    }
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(BTC_ESPLORA_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(BTC_ESPLORA_REQUEST_TIMEOUT_SECS))
        .pool_idle_timeout(Duration::from_secs(BTC_ESPLORA_POOL_IDLE_TIMEOUT_SECS))
        .pool_max_idle_per_host(BTC_ESPLORA_POOL_MAX_IDLE_PER_HOST)
        .user_agent("bihelix-super-bazaar/1.0")
        .build()
        .context("build Esplora HTTP client")?;
    let _ = ESPLORA_HTTP_CLIENT.set(client);
    ESPLORA_HTTP_CLIENT
        .get()
        .context("Esplora HTTP client was not initialized")
}

fn rgb_service_http_client() -> Result<&'static reqwest::blocking::Client> {
    if let Some(client) = RGB_SERVICE_HTTP_CLIENT.get() {
        return Ok(client);
    }
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .user_agent("bihelix-zust-console/1.0")
        .build()
        .context("build RGB service HTTP client")?;
    let _ = RGB_SERVICE_HTTP_CLIENT.set(client);
    RGB_SERVICE_HTTP_CLIENT
        .get()
        .context("RGB service HTTP client was not initialized")
}

fn esplora_get_text(url: &str, accept: &str) -> Result<String> {
    let response = esplora_http_client()?
        .get(url)
        .header("accept", accept)
        .send()
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let body = response
        .text()
        .with_context(|| format!("read Esplora response body from {url}"))?;
    if !status.is_success() {
        bail!("Esplora GET {url} failed with HTTP {status}: {body}");
    }
    Ok(body)
}

fn esplora_get_json(url: &str) -> Result<Value> {
    let body = esplora_get_text(url, "application/json")?;
    let json = if body.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&body)
            .with_context(|| format!("decode Esplora JSON body from {url}: {body}"))?
    };
    Ok(json)
}

fn is_electrum_chain_source(source: &str) -> bool {
    let source = source.trim();
    !source.starts_with("http://") && !source.starts_with("https://")
}

fn electrum_endpoint(source: &str) -> Result<String> {
    let source = source.trim().trim_end_matches('/');
    let source = source
        .strip_prefix("electrum://")
        .or_else(|| source.strip_prefix("tcp://"))
        .unwrap_or(source);
    ensure!(
        !source.starts_with("ssl://") && !source.starts_with("tls://"),
        "Electrum TLS endpoints are not supported here; use plaintext tcp/electrum"
    );
    let authority = source.split('/').next().unwrap_or_default();
    ensure!(
        authority.contains(':'),
        "Electrum source must include host:port"
    );
    Ok(authority.to_string())
}

fn electrum_rpc(source: &str, method: &str, params: Value) -> Result<Value> {
    let endpoint = electrum_endpoint(source)?;
    let address = endpoint
        .to_socket_addrs()
        .with_context(|| format!("resolve Electrum endpoint {endpoint}"))?
        .next()
        .with_context(|| format!("Electrum endpoint {endpoint} resolved no addresses"))?;
    let mut stream = TcpStream::connect_timeout(
        &address,
        Duration::from_secs(BTC_ESPLORA_CONNECT_TIMEOUT_SECS),
    )
    .with_context(|| format!("connect Electrum endpoint {endpoint}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(BTC_ESPLORA_REQUEST_TIMEOUT_SECS)))
        .context("set Electrum read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(BTC_ESPLORA_REQUEST_TIMEOUT_SECS)))
        .context("set Electrum write timeout")?;
    let request = json!({
        "id": now_ms(),
        "method": method,
        "params": params.as_array().cloned().unwrap_or_default()
    });
    let request_line = serde_json::to_string(&request).context("encode Electrum request")? + "\n";
    stream
        .write_all(request_line.as_bytes())
        .with_context(|| format!("write Electrum request {method} to {endpoint}"))?;
    stream
        .flush()
        .with_context(|| format!("flush Electrum request {method} to {endpoint}"))?;
    let mut reader = BufReader::new(stream);
    let mut response_line = String::new();
    reader
        .read_line(&mut response_line)
        .with_context(|| format!("read Electrum response {method} from {endpoint}"))?;
    ensure!(
        !response_line.trim().is_empty(),
        "empty Electrum response for {method} from {endpoint}"
    );
    let response: Value = serde_json::from_str(&response_line)
        .with_context(|| format!("decode Electrum response for {method}: {response_line}"))?;
    if let Some(error) = response.get("error").filter(|error| !error.is_null()) {
        bail!("Electrum {method} failed: {error}");
    }
    response
        .get("result")
        .cloned()
        .context("Electrum response missing result")
}

fn electrum_script_hash_hex(script: &ScriptBuf) -> String {
    let digest = sha256::Hash::hash(script.as_bytes());
    let mut bytes = digest.to_byte_array();
    bytes.reverse();
    bytes_to_hex(&bytes)
}

fn btc_esplora_get_json_with_fallback(path: &str, context: &str) -> Result<(String, Value)> {
    let mut errors = Vec::new();
    let path = path.trim_start_matches('/');
    for esplora in btc_esplora_urls() {
        let base = esplora.trim_end_matches('/');
        if is_electrum_chain_source(base) {
            errors.push(format!(
                "{base}: Electrum source cannot serve Esplora path {path}"
            ));
            continue;
        }
        let url = format!("{base}/{path}");
        match esplora_get_json(&url) {
            Ok(json) => return Ok((base.to_string(), json)),
            Err(err) => errors.push(format!("{base}: {err:#}")),
        }
    }
    bail!(
        "{context}: all Esplora endpoints failed: {}",
        errors.join(" | ")
    )
}

fn btc_tip_height_with_fallback() -> (String, u64) {
    let mut errors = Vec::new();
    for source in btc_esplora_urls() {
        let base = source.trim_end_matches('/').to_string();
        if is_electrum_chain_source(&base) {
            match electrum_rpc(&base, "blockchain.headers.subscribe", json!([])) {
                Ok(header) => {
                    let tip_height = header
                        .get("height")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    return (base, tip_height);
                }
                Err(err) => errors.push(format!("{base}: {err:#}")),
            }
        } else {
            let url = format!("{base}/blocks/tip/height");
            match esplora_get_text(&url, "text/plain") {
                Ok(text) => {
                    let tip_height = text.trim().parse::<u64>().unwrap_or_default();
                    return (base, tip_height);
                }
                Err(err) => errors.push(format!("{base}: {err:#}")),
            }
        }
    }
    eprintln!(
        "[zust-console] BTC chain tip height fetch failed: {}",
        errors.join(" | ")
    );
    (btc_esplora_url(), 0)
}

fn parse_http_json_response(path: &str, response: Vec<u8>) -> Result<Value> {
    let response = String::from_utf8(response).context("RGB service response is not UTF-8")?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .context("invalid HTTP response from RGB service")?;
    let status = head.lines().next().unwrap_or_default();
    let status_code = status
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .context("missing HTTP status code from RGB service")?;
    let json = if body.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(body)
            .with_context(|| format!("decode RGB service JSON body: {body}"))?
    };
    if !(200..300).contains(&status_code) {
        bail!("RGB service {path} failed with HTTP {status_code}: {json}");
    }
    Ok(json)
}

fn parse_http_url(value: &str) -> Result<(String, u16, String)> {
    let value = value
        .strip_prefix("http://")
        .context("daemon_url must start with http://")?;
    let (authority, path) = value.split_once('/').unwrap_or((value, ""));
    let (host, port) = authority.split_once(':').unwrap_or((authority, "8787"));
    ensure!(!host.is_empty(), "daemon_url host must not be empty");
    let port = port.parse::<u16>().context("invalid daemon_url port")?;
    Ok((host.to_string(), port, format!("/{path}")))
}

fn signed_request(input: &Dynamic, route: &str) -> Result<Dynamic> {
    let payload = signed_payload(input, route)?;
    let signature = input
        .get_dynamic("signature")
        .map(|signature| dynamic_to_json(&signature))
        .context("missing caller-provided signature")?;
    Ok(json_to_dynamic(&json!({
        "payload": payload,
        "signature": signature
    })))
}

fn signed_payload(input: &Dynamic, route: &str) -> Result<Value> {
    let payload = if matches!(input, Dynamic::Null) {
        Value::Object(Map::new())
    } else {
        input
            .get_dynamic("payload")
            .map(|payload| dynamic_to_json(&payload))
            .unwrap_or_else(|| dynamic_to_json(input))
    };
    let mut object = match payload {
        Value::Object(object) => object,
        other => {
            let mut object = Map::new();
            object.insert("value".to_string(), other);
            object
        }
    };
    object.remove("daemon_url");
    object.remove("timeout_ms");
    object.remove("signer_id");
    object.remove("signature");
    object.remove("route");
    object.remove("btc_addr");
    object.remove("btc_address");
    object
        .entry("account_id".to_string())
        .or_insert(json!(default_account_id()?));
    if route == "/v1/balance" {
        object.entry("scope".to_string()).or_insert(json!("all"));
    }
    Ok(Value::Object(object))
}

pub(crate) fn default_account_id() -> Result<String> {
    local_string("btc-addr").context("missing root value `local/btc-addr`")
}

fn btc_esplora_url() -> String {
    local_string("btc-chain-source")
        .or_else(|| local_string("btc-deposit-chain-source"))
        .or_else(|| {
            local_dynamic("lightning")
                .map(|lightning| dynamic_to_json(&lightning))
                .and_then(|lightning| {
                    lightning
                        .get("config")
                        .and_then(|config| config.get("chain_source"))
                        .and_then(|chain_source| chain_source.get("url"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
        })
        .filter(|url| !url.trim().is_empty())
        .unwrap_or_else(|| LN_ESPLORA_DEFAULT.to_string())
}

fn btc_esplora_urls() -> Vec<String> {
    let mut urls = Vec::new();
    let primary = btc_esplora_url().trim().trim_end_matches('/').to_string();
    if !primary.is_empty() && !urls.iter().any(|existing| existing == &primary) {
        urls.push(primary);
    }
    for fallback in local_string("btc-esplora-fallback")
        .unwrap_or_default()
        .split(',')
        .map(|value| value.trim().trim_end_matches('/'))
        .filter(|value| !value.is_empty())
    {
        let fallback = fallback.to_string();
        if !urls.iter().any(|existing| existing == &fallback) {
            urls.push(fallback);
        }
    }
    urls
}

fn external_signature_unavailable<T>() -> Result<T> {
    bail!(
        "external signing is unavailable; provide a caller-signed PSBT or request signature"
    )
}

fn ok(value: Value) -> Dynamic {
    let out = json_to_dynamic(&value);
    out.insert("ok", true);
    out
}

fn native_dynamic_result(
    input: *const Dynamic,
    f: impl FnOnce(&Dynamic) -> Result<Dynamic>,
) -> *const Dynamic {
    let input = unsafe { &*input };
    match f(input) {
        Ok(value) => Box::into_raw(Box::new(value)),
        Err(err) => Box::into_raw(Box::new(json_to_dynamic(&json!({
            "ok": false,
            "error": format!("{err:#}")
        })))),
    }
}

fn native_result(f: impl FnOnce() -> Result<Dynamic>) -> *const Dynamic {
    match f() {
        Ok(value) => Box::into_raw(Box::new(value)),
        Err(err) => Box::into_raw(Box::new(json_to_dynamic(&json!({
            "ok": false,
            "error": format!("{err:#}")
        })))),
    }
}

fn native_string_dynamic_result(
    input: *const Dynamic,
    f: impl FnOnce(&str) -> Result<Dynamic>,
) -> *const Dynamic {
    let input = unsafe { &*input };
    native_result(|| {
        ensure!(input.is_str(), "expected string argument");
        f(input.as_str())
    })
}

fn native_three_string_dynamic_result(
    first: *const Dynamic,
    second: *const Dynamic,
    third: *const Dynamic,
    f: impl FnOnce(&str, &str, &str) -> Result<Dynamic>,
) -> *const Dynamic {
    let first = unsafe { &*first };
    let second = unsafe { &*second };
    let third = unsafe { &*third };
    native_result(|| {
        ensure!(first.is_str(), "first argument must be string");
        ensure!(second.is_str(), "second argument must be string");
        ensure!(third.is_str(), "third argument must be string");
        f(first.as_str(), second.as_str(), third.as_str())
    })
}

fn required_string(input: &Dynamic, key: &str) -> Result<String> {
    optional_string(input, key)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing `{key}`"))
}

fn optional_string(input: &Dynamic, key: &str) -> Option<String> {
    input
        .get_dynamic(key)
        .map(|value| value.as_str().to_string())
}

fn optional_u64(input: &Dynamic, key: &str) -> Option<u64> {
    input.get_dynamic(key).and_then(|value| match value {
        Dynamic::U8(value) => Some(value as u64),
        Dynamic::I8(value) => u64::try_from(value).ok(),
        Dynamic::U16(value) => Some(value as u64),
        Dynamic::I16(value) => u64::try_from(value).ok(),
        Dynamic::U32(value) => Some(value as u64),
        Dynamic::I32(value) => u64::try_from(value).ok(),
        Dynamic::U64(value) => Some(value),
        Dynamic::I64(value) => u64::try_from(value).ok(),
        value if value.is_str() => value.as_str().parse::<u64>().ok(),
        _ => None,
    })
}

fn hex32_to_bytes(value: &str) -> Result<[u8; 32]> {
    let value = value.trim();
    ensure!(value.len() == 64, "expected 32-byte hex string");
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
            .with_context(|| format!("invalid hex at byte {index}"))?;
    }
    Ok(bytes)
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.trim();
    ensure!(value.len() % 2 == 0, "hex string must have even length");
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let mut index = 0usize;
    while index < value.len() {
        let byte = u8::from_str_radix(&value[index..index + 2], 16)
            .with_context(|| format!("invalid hex at byte {}", index / 2))?;
        bytes.push(byte);
        index += 2;
    }
    Ok(bytes)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms_u128() -> u128 {
    now_ms() as u128
}

fn current_ln_hot_address() -> Result<String> {
    let lightning = local_dynamic("lightning").context("missing root value `local/lightning`")?;
    let requested = dynamic_to_json(&lightning);
    let path = find_string_field(&requested, &["path"])
        .map(PathBuf::from)
        .unwrap_or_else(|| ln_node_path(&lightning));
    let stored =
        read_json_file(&path).with_context(|| format!("read LN node state {}", path.display()))?;
    find_string_field(&stored, &["address", "btc_address"]).with_context(|| {
        format!(
            "LN node state {} has no signer address; run ln_rgb::node_address first",
            path.display()
        )
    })
}

fn ln_rgb_storage_dir() -> PathBuf {
    local_dynamic("lightning")
        .map(|value| dynamic_to_json(&value))
        .and_then(|value| {
            value
                .get("config")
                .and_then(|config| config.get("ldk_data_dir"))
                .and_then(Value::as_str)
                .or_else(|| value.get("ldk_data_dir").and_then(Value::as_str))
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| PathBuf::from(LN_LDK_DATA_DIR_DEFAULT))
}

fn btc_wallet_data_dir() -> PathBuf {
    std::env::var("SUPER_BAZAAR_WALLET_DATA_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .or_else(|| local_dynamic("btc_wallet/node")
        .map(|value| dynamic_to_json(&value))
        .and_then(|value| {
            value
                .get("data_dir")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from)
        }))
        .unwrap_or_else(|| {
            let network = std::env::var("SUPER_BAZAAR_BTC_NETWORK")
                .or_else(|_| std::env::var("SUPER_BAZAAR_NETWORK"))
                .unwrap_or_else(|_| "mainnet".to_string());
            PathBuf::from("./wallet-data").join(network).join("market")
        })
}

fn ln_rgb_network_name() -> String {
    local_dynamic("lightning")
        .map(|value| dynamic_to_json(&value))
        .and_then(|value| {
            value
                .get("config")
                .and_then(|config| config.get("network"))
                .and_then(Value::as_str)
                .or_else(|| value.get("network").and_then(Value::as_str))
                .map(str::to_string)
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "bitcoin".to_string())
}

fn json_to_dynamic(value: &Value) -> Dynamic {
    let json = value.to_string();
    let (dynamic, consumed) = Dynamic::from_json(json.as_bytes())
        .expect("serde_json emitted invalid JSON for Zust Dynamic");
    assert!(
        json.as_bytes()[consumed..]
            .iter()
            .all(|byte| byte.is_ascii_whitespace()),
        "Zust Dynamic FromJson did not consume full JSON value"
    );
    dynamic
}

fn dynamic_to_json(value: &Dynamic) -> Value {
    let mut json = String::new();
    value.to_json(&mut json);
    serde_json::from_str(&json).expect("zust Dynamic ToJson emitted invalid JSON")
}

fn ln_node_path(input: &Dynamic) -> PathBuf {
    optional_string(input, "path")
        .or_else(|| local_string("ln-node-file"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(LN_NODE_DEFAULT_PATH))
}

fn absolute_runtime_path(path: &PathBuf) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| {
        if path.is_absolute() {
            path.clone()
        } else {
            std::env::current_dir()
                .map(|cwd| cwd.join(path))
                .unwrap_or_else(|_| path.clone())
        }
    })
}

fn log_ln_initialization_paths(
    stage: &str,
    config_path: &PathBuf,
    stored: &Value,
    config: &Value,
) {
    let data_dir = PathBuf::from(
        config
            .get("data_dir")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(LN_DATA_DIR_DEFAULT),
    );
    let ldk_data_dir = PathBuf::from(
        config
            .get("ldk_data_dir")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(LN_LDK_DATA_DIR_DEFAULT),
    );
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let config_path = absolute_runtime_path(config_path);
    let data_dir = absolute_runtime_path(&data_dir);
    let ldk_data_dir = absolute_runtime_path(&ldk_data_dir);
    let btc_owner_address = local_string("btc-addr").unwrap_or_default();
    let ln_hot_wallet_address = find_string_field(stored, &["address", "btc_address"])
        .unwrap_or_default();
    let address_source = stored
        .get("address_source")
        .and_then(Value::as_str)
        .unwrap_or_default();

    eprintln!(
        "{stage} initialization: cwd={} config_file={} data_dir={} bdk_wallet={} local_store={} ldk_data_dir={} ldk_store={} btc_owner_address={} ln_hot_wallet_address={} address_source={}",
        cwd.display(),
        config_path.display(),
        data_dir.display(),
        data_dir.join("bdk_wallet").display(),
        data_dir.join("local-store").display(),
        ldk_data_dir.display(),
        ldk_data_dir.join("ln-rgb").join("ldk-store").display(),
        btc_owner_address,
        ln_hot_wallet_address,
        address_source,
    );
}

fn normalized_ln_config(input: Value, low_water_sats: u64) -> Value {
    let mut config = match input {
        Value::Object(mut object) => {
            object.remove("path");
            object.remove("signer_response");
            object.remove("node");
            object.remove("created");
            object.remove("ok");
            object
        }
        _ => Map::new(),
    };

    let network = config
        .get("network")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("bitcoin")
        .to_string();

    config
        .entry("network".to_string())
        .or_insert_with(|| json!(network));
    config
        .entry("data_dir".to_string())
        .or_insert_with(|| json!(LN_DATA_DIR_DEFAULT));
    config
        .entry("ldk_data_dir".to_string())
        .or_insert_with(|| json!(LN_LDK_DATA_DIR_DEFAULT));
    config
        .entry("ln_backend".to_string())
        .or_insert_with(|| json!("ln-rgb"));
    config
        .entry("listen".to_string())
        .or_insert_with(|| json!(LN_LISTEN_DEFAULT));
    config
        .entry("peers".to_string())
        .or_insert_with(|| json!([]));
    config
        .entry("trusted_peers_0conf".to_string())
        .or_insert_with(|| json!([]));
    config
        .entry("accept_inbound_channels".to_string())
        .or_insert_with(|| json!(true));
    config
        .entry("low_water_sats".to_string())
        .or_insert_with(|| json!(low_water_sats));
    config.entry("chain_source".to_string()).or_insert_with(|| {
        json!({
            "kind": "esplora",
            "url": LN_ESPLORA_DEFAULT
        })
    });

    Value::Object(config)
}

fn ln_config_from_value(value: &Value) -> Value {
    value
        .get("config")
        .cloned()
        .or_else(|| {
            value
                .get("node")
                .and_then(|node| node.get("config"))
                .cloned()
        })
        .unwrap_or_else(|| Value::Object(Map::new()))
}

fn read_json_file(path: &PathBuf) -> Result<Value> {
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn write_private_json_file(path: &PathBuf, value: &Value) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes)?;
    restrict_private_file(path)?;
    Ok(())
}

#[cfg(unix)]
fn restrict_private_file(path: &PathBuf) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_private_file(_path: &PathBuf) -> Result<()> {
    Ok(())
}

fn redacted_ln_node_response(mut stored: Value, path: &PathBuf, created: bool) -> Value {
    let address = find_string_field(&stored, &["address", "btc_address"]).unwrap_or_default();
    redact_secret_fields(&mut stored);
    json!({
        "module": "ln_rgb",
        "node_state": "created_or_loaded",
        "created": created,
        "address": address,
        "path": path.display().to_string(),
        "low_water_sats": stored.get("low_water_sats").cloned().unwrap_or(json!(LN_LOW_WATER_SATS)),
        "config": stored.get("config").cloned().unwrap_or_else(|| normalized_ln_config(Value::Object(Map::new()), LN_LOW_WATER_SATS)),
        "private_key_persisted": true,
        "node": stored
    })
}

fn find_string_field(value: &Value, keys: &[&str]) -> Option<String> {
    match value {
        Value::Object(object) => {
            for key in keys {
                if let Some(Value::String(value)) = object.get(*key) {
                    return Some(value.clone());
                }
            }
            object
                .values()
                .find_map(|value| find_string_field(value, keys))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|value| find_string_field(value, keys)),
        _ => None,
    }
}

fn value_u64(value: &Value, key: &str) -> Option<u64> {
    match value {
        Value::Object(object) => object.get(key).and_then(|value| match value {
            Value::Number(number) => number.as_u64(),
            Value::String(value) => value.parse().ok(),
            _ => None,
        }),
        _ => None,
    }
}

fn value_path_u64(value: &Value, path: &[&str]) -> u64 {
    let mut current = value;
    for key in path {
        let Some(next) = current.get(*key) else {
            return 0;
        };
        current = next;
    }
    match current {
        Value::Number(number) => number.as_u64().unwrap_or(0),
        Value::String(value) => value.parse().unwrap_or(0),
        _ => 0,
    }
}

fn redact_secret_fields(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object.iter_mut() {
                let key = key.to_ascii_lowercase();
                if key.contains("private")
                    || key.contains("secret")
                    || key.contains("mnemonic")
                    || key.contains("seed")
                    || key == "wif"
                    || key.contains("xprv")
                {
                    *value = Value::String("<persisted>".to_string());
                } else {
                    redact_secret_fields(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_secret_fields(value);
            }
        }
        _ => {}
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
