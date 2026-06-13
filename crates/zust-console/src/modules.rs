use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::btc_ln::{
    BtcLnBackendKind, BtcLnBolt11InvoiceRequest, BtcLnBolt11PaymentRequest,
    BtcLnChannelCloseRequest, BtcLnChannelOpenRequest, BtcLnNode, BtcLnRuntimeConfig,
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
use bitcoin::{secp256k1::PublicKey, Network};
use dynamic::{Dynamic, FromJson, MsgPack, MsgUnpack, ToJson, Type};
use iroh::{endpoint::presets, Endpoint, EndpointAddr, EndpointId, SecretKey};
use lightning::ln::msgs::SocketAddress;
use lightning::rgb::{
    init_rgb_ln_tx_composer, ContractId as LnContractId, RequestSignature as LnRequestSignature,
    RgbAssetAmount as LdkRgbAssetAmount, RgbChannelContext, RgbDaemonLnTxComposer, RgbLnTxComposer,
    RgbServiceClient, RgbServiceClientError, RgbServiceSigner,
};
use lightning_invoice::{Bolt11Invoice, Bolt11InvoiceDescription, Description};
use serde_json::{json, Map, Value};
use std::str::FromStr;
use vm::Vm;

const SIGNER_ALPN: &[u8] = b"bihelix/signer/1";
const SIGNER_REQUEST_SIGNATURE_PATH: &str = "/v1/signer/request-signature";
const SIGNER_ASSET_AUTHORIZATION_PATH: &str = "/v1/signer/asset-authorization";
const SIGNER_ADDRESS_BATCH_PATH: &str = "/v1/signer/address/batch";
const SIGNER_PSBT_SIGN_PATH: &str = "/v1/signer/psbt/sign";
const SIGNER_TIMEOUT: Duration = Duration::from_secs(10);
const SIGNER_ATTEMPTS: usize = 3;
const SIGNER_RETRY_DELAY: Duration = Duration::from_millis(500);
const LN_SCAN_DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
const LN_NODE_DEFAULT_PATH: &str = ".zust-console/ln-node.json";
const LN_DATA_DIR_DEFAULT: &str = ".zust-console/lightning";
const LN_LDK_DATA_DIR_DEFAULT: &str = ".zust-console/lightning/ldk";
const LN_LISTEN_DEFAULT: &str = "0.0.0.0:9736";
const LN_ESPLORA_DEFAULT: &str = "https://blockstream.info/api";
const LN_LOW_WATER_SATS: u64 = 100_000;
const BTC_ADDRESS_POOL_LOW_WATER: usize = 5;
const BTC_ADDRESS_POOL_TARGET: usize = 20;
static CONSOLE_IROH_SECRET: OnceLock<SecretKey> = OnceLock::new();
static CONSOLE_IROH_ENDPOINT: OnceLock<Endpoint> = OnceLock::new();
static CONSOLE_ASYNC_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static LN_RGB_COMPOSER: OnceLock<Arc<RgbDaemonLnTxComposer>> = OnceLock::new();
static LN_RGB_NODE: OnceLock<Mutex<Option<Arc<LnRgbBtcLnBackend>>>> = OnceLock::new();
static LN_STARTED: AtomicBool = AtomicBool::new(false);
static LN_SCANNER_STARTED: AtomicBool = AtomicBool::new(false);

struct ConsoleRgbServiceSigner;

impl RgbServiceSigner for ConsoleRgbServiceSigner {
    fn sign_rgb_service_payload(
        &self,
        purpose: &str,
        payload: &[u8],
    ) -> std::result::Result<LnRequestSignature, RgbServiceClientError> {
        let mut body: Value = serde_json::from_slice(payload).map_err(|err| {
            RgbServiceClientError::Compose(format!("decode LN RGB signer payload: {err}"))
        })?;
        if let Value::Object(object) = &mut body {
            object.insert("purpose".to_string(), json!(purpose));
        }
        let signature = request_signature(SIGNER_REQUEST_SIGNATURE_PATH, &body)
            .map_err(|err| RgbServiceClientError::Compose(err.to_string()))?;
        serde_json::from_value(signature).map_err(|err| {
            RgbServiceClientError::Compose(format!("decode LN RGB signer response: {err}"))
        })
    }
}

pub(crate) fn daemon_url() -> Result<String> {
    let daemon_url =
        local_string("rgb-service").context("missing root value `local/rgb-service`")?;
    ensure!(
        daemon_url.starts_with("http://"),
        "local/rgb-service must start with http://"
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

pub fn register_console_modules(vm: &Vm) -> Result<()> {
    register_btc_module(vm)?;
    register_rgb_module(vm)?;
    register_ln_module(vm)?;
    register_ln_rgb_module(vm)?;
    Ok(())
}

fn register_btc_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write().unwrap();
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
        "scan_deposits",
        &[Type::Str],
        Type::Any,
        btc_scan_deposits as *const u8,
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
        "refill_address_pool",
        &[Type::U64],
        Type::Any,
        btc_refill_address_pool as *const u8,
    )?;
    jit.add_native_module_ptr(
        "btc",
        "sign_psbt",
        &[Type::Str, Type::Str],
        Type::Any,
        btc_sign_psbt as *const u8,
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
        "tx_status",
        &[Type::Str],
        Type::Any,
        btc_tx_status as *const u8,
    )?;
    Ok(())
}

fn register_rgb_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write().unwrap();
    jit.add_native_module_ptr(
        "rgb",
        "signed",
        &[Type::Any],
        Type::Any,
        rgb_signed as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "rna_balance",
        &[],
        Type::Any,
        rgb_rna_balance as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "request_signature",
        &[Type::Any],
        Type::Any,
        rgb_request_signature as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "asset_authorization",
        &[
            Type::Str,
            Type::U64,
            Type::Str,
            Type::Str,
            Type::Str,
            Type::U64,
        ],
        Type::Any,
        rgb_asset_authorization as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "request",
        &[Type::Str, Type::Any],
        Type::Any,
        rgb_request as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "issue",
        &[Type::Str, Type::Str, Type::U8, Type::U64, Type::Str],
        Type::Any,
        rgb_issue as *const u8,
    )?;
    jit.add_native_module_ptr("rgb", "assets", &[], Type::Any, rgb_assets as *const u8)?;
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
        &[Type::Str, Type::Str],
        Type::Any,
        rgb_balance as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "balance_breakdown",
        &[Type::Str],
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
        ],
        Type::Any,
        rgb_prepare_transfer as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "commit_transfer",
        &[Type::Str, Type::U64, Type::Str, Type::Str, Type::Str],
        Type::Any,
        rgb_commit_transfer as *const u8,
    )?;
    jit.add_native_module_ptr("rgb", "pending", &[], Type::Any, rgb_pending as *const u8)?;
    jit.add_native_module_ptr(
        "rgb",
        "recover",
        &[Type::Str],
        Type::Any,
        rgb_recover as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "test",
        &[Type::Str],
        Type::Any,
        rgb_test as *const u8,
    )?;
    Ok(())
}

fn register_ln_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write().unwrap();
    jit.add_native_module_ptr(
        "ln",
        "spawn_scanner",
        &[Type::U64],
        Type::Any,
        ln_spawn_scanner as *const u8,
    )?;
    jit.add_native_module_ptr("ln", "start", &[], Type::Any, ln_start as *const u8)?;
    jit.add_native_module_ptr("ln", "stop", &[], Type::Any, ln_stop as *const u8)?;
    jit.add_native_module_ptr(
        "ln",
        "scanner_status",
        &[],
        Type::Any,
        ln_scanner_status as *const u8,
    )?;
    jit.add_native_module_ptr("ln", "status", &[], Type::Any, ln_status as *const u8)?;
    jit.add_native_module_ptr("ln", "events", &[], Type::Any, ln_events as *const u8)?;
    jit.add_native_module_ptr(
        "ln",
        "get_node_id",
        &[],
        Type::Any,
        ln_get_node_id as *const u8,
    )?;
    jit.add_native_module_ptr("ln", "get_addr", &[], Type::Any, ln_get_addr as *const u8)?;
    jit.add_native_module_ptr("ln", "get_peers", &[], Type::Any, ln_get_peers as *const u8)?;
    jit.add_native_module_ptr(
        "ln",
        "get_channels",
        &[],
        Type::Any,
        ln_get_channels as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "connect",
        &[Type::Str, Type::Str, Type::Bool],
        Type::Any,
        ln_connect as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "open_channel",
        &[Type::Str, Type::Str, Type::U64, Type::U64],
        Type::Any,
        ln_open_channel as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "close_channel",
        &[Type::Str, Type::Str, Type::Bool, Type::Str],
        Type::Any,
        ln_close_channel as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "invoice",
        &[Type::U64, Type::Str, Type::U64],
        Type::Any,
        ln_invoice as *const u8,
    )?;
    jit.add_native_module_ptr("ln", "pay", &[Type::Str], Type::Any, ln_pay as *const u8)?;
    jit.add_native_module_ptr(
        "ln",
        "token_list",
        &[],
        Type::Any,
        ln_token_list as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "rgb_channel_context",
        &[Type::Str, Type::U64, Type::Bool],
        Type::Any,
        ln_rgb_channel_context as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "node_address",
        &[Type::Any],
        Type::Any,
        ln_node_address as *const u8,
    )?;
    Ok(())
}

fn register_ln_rgb_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write().unwrap();
    jit.add_native_module_ptr("ln_rgb", "start", &[], Type::Any, ln_rgb_start as *const u8)?;
    jit.add_native_module_ptr("ln_rgb", "stop", &[], Type::Any, ln_rgb_stop as *const u8)?;
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
        "get_addr",
        &[],
        Type::Any,
        ln_rgb_get_addr as *const u8,
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
        "invoice",
        &[Type::U64, Type::Str, Type::U64],
        Type::Any,
        ln_rgb_invoice as *const u8,
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
        "events",
        &[],
        Type::Any,
        ln_rgb_events as *const u8,
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
    jit.add_native_module_ptr(
        "ln_rgb",
        "rgb_channel_context",
        &[Type::Str, Type::U64, Type::Bool],
        Type::Any,
        ln_rgb_rgb_channel_context as *const u8,
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
        let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
        if let Some(address) = store.get_ident_btc_address(&ident)? {
            return Ok(ok(json!({
                "module": "btc",
                "ident": ident,
                "address": address,
                "created": false,
                "persisted": true
            })));
        }
        let available = store.list_btc_address_pool_records()?.len();
        if available < BTC_ADDRESS_POOL_LOW_WATER {
            let count = BTC_ADDRESS_POOL_TARGET.saturating_sub(available);
            if count > 0 {
                let body = json!({
                    "account_id": default_account_id()?,
                    "network": "bitcoin",
                    "purpose": "low_water_refill",
                    "count": count,
                    "timestamp_ms": now_ms()
                });
                let response = signer_request(SIGNER_ADDRESS_BATCH_PATH, &body)?;
                let addresses = response
                    .get("addresses")
                    .and_then(Value::as_array)
                    .with_context(|| {
                        format!("signer batch address response missing `addresses`: {response}")
                    })?;
                ensure!(
                    addresses.len() == count,
                    "signer batch address response count mismatch: requested={count}, returned={}",
                    addresses.len()
                );
                let existing = store
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
                    .collect::<std::collections::BTreeSet<_>>();
                for response in addresses {
                    let address = response
                        .get("address")
                        .and_then(Value::as_str)
                        .filter(|address| !address.trim().is_empty())
                        .with_context(|| {
                            format!(
                                "signer batch address item missing non-empty `address`: {response}"
                            )
                        })?;
                    if existing.contains(address) {
                        continue;
                    }
                    store.put_btc_address_pool_record(
                        address,
                        &json!({
                            "address": address,
                            "created_at_ms": now_ms(),
                            "purpose": "low_water_refill",
                            "signer_response": response
                        }),
                    )?;
                }
            }
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
            .context("BTC address pool is empty after refill")?;
        if let Value::Object(object) = &mut pool_record {
            object.insert("used_at_ms".to_string(), json!(now_ms()));
            object.insert("ident".to_string(), json!(ident));
        }
        store.remove_btc_address_pool_record(&address)?;
        store.put_used_btc_address_pool_record(&address, &pool_record)?;
        store.put_ident_btc_address(&ident, &address)?;
        let available = store.list_btc_address_pool_records()?.len();
        if available < BTC_ADDRESS_POOL_LOW_WATER {
            let count = BTC_ADDRESS_POOL_TARGET.saturating_sub(available);
            if count > 0 {
                let body = json!({
                    "account_id": default_account_id()?,
                    "network": "bitcoin",
                    "purpose": "low_water_refill",
                    "count": count,
                    "timestamp_ms": now_ms()
                });
                let response = signer_request(SIGNER_ADDRESS_BATCH_PATH, &body)?;
                let addresses = response
                    .get("addresses")
                    .and_then(Value::as_array)
                    .with_context(|| {
                        format!("signer batch address response missing `addresses`: {response}")
                    })?;
                ensure!(
                    addresses.len() == count,
                    "signer batch address response count mismatch: requested={count}, returned={}",
                    addresses.len()
                );
                let existing = store
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
                    .collect::<std::collections::BTreeSet<_>>();
                for response in addresses {
                    let address = response
                        .get("address")
                        .and_then(Value::as_str)
                        .filter(|address| !address.trim().is_empty())
                        .with_context(|| {
                            format!(
                                "signer batch address item missing non-empty `address`: {response}"
                            )
                        })?;
                    if existing.contains(address) {
                        continue;
                    }
                    store.put_btc_address_pool_record(
                        address,
                        &json!({
                            "address": address,
                            "created_at_ms": now_ms(),
                            "purpose": "low_water_refill",
                            "signer_response": response
                        }),
                    )?;
                }
            }
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
        let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
        Ok(ok(json!({
            "module": "btc",
            "address": address,
            "ident": store.lookup_ident_by_btc_address(&address)?,
        })))
    })
}

extern "C" fn btc_scan_deposits(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |ident_filter| {
        let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
        store.put_wallet_btc_address(&default_account_id()?)?;
        let esplora = btc_esplora_url();
        let tip_url = format!("{}/blocks/tip/height", esplora.trim_end_matches('/'));
        let tip_height = attohttpc::get(&tip_url)
            .send()
            .ok()
            .and_then(|response| response.text().ok())
            .and_then(|text| text.trim().parse::<u64>().ok())
            .unwrap_or_default();
        let mut deposits = Vec::new();
        let mut persisted = 0usize;
        let mut address_mappings = Vec::new();
        if ident_filter.trim().is_empty() {
            let address = store
                .get_wallet_btc_address()?
                .unwrap_or(default_account_id()?);
            address_mappings.push((
                "wallet".to_string(),
                "default".to_string(),
                String::new(),
                address,
            ));
        } else if let Some(address) = store.get_ident_btc_address(ident_filter)? {
            address_mappings.push((
                "ident".to_string(),
                String::new(),
                ident_filter.to_string(),
                address,
            ));
        } else {
            bail!("unknown BTC ident `{ident_filter}`; call btc::get_deposit_address first");
        }
        for (owner_type, owner_label, ident, address) in address_mappings {
            let mut seen = std::collections::BTreeSet::new();
            let txs = esplora_get_json(&format!(
                "{}/address/{address}/txs",
                esplora.trim_end_matches('/')
            ))
            .with_context(|| format!("fetch BTC deposit transactions for {address}"))?;
            for tx in txs.as_array().cloned().unwrap_or_default() {
                let txid = tx
                    .get("txid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                for output in tx
                    .get("vout")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                {
                    let output_address = output
                        .get("scriptpubkey_address")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if output_address != address {
                        continue;
                    }
                    let vout = output.get("n").and_then(Value::as_u64).unwrap_or_default();
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
                    deposits.push(record);
                }
            }
            let utxos = btc_address_utxos_json(&address, &esplora)?;
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
                deposits.push(record);
            }
        }
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
            "network": "bitcoin",
            "esplora": esplora,
            "tip_height": tip_height,
            "scanner_started": LN_SCANNER_STARTED.load(Ordering::SeqCst),
            "persisted": persisted,
            "deposits": deposits,
            "stored_deposits": store
                .list_btc_deposit_records()?
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>()
        })))
    })
}

extern "C" fn btc_scan_ident_deposits(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |ident_filter| {
        let ident_filter = ident_filter.to_string();
        ensure!(!ident_filter.trim().is_empty(), "ident must not be empty");
        let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
        store.put_wallet_btc_address(&default_account_id()?)?;
        let esplora = btc_esplora_url();
        let tip_url = format!("{}/blocks/tip/height", esplora.trim_end_matches('/'));
        let tip_height = attohttpc::get(&tip_url)
            .send()
            .ok()
            .and_then(|response| response.text().ok())
            .and_then(|text| text.trim().parse::<u64>().ok())
            .unwrap_or_default();
        let mut deposits = Vec::new();
        let mut persisted = 0usize;
        for (ident, address) in store.list_ident_btc_addresses()? {
            if ident != ident_filter {
                continue;
            }
            let mut seen = std::collections::BTreeSet::new();
            let txs = esplora_get_json(&format!(
                "{}/address/{address}/txs",
                esplora.trim_end_matches('/')
            ))
            .with_context(|| format!("fetch BTC deposit transactions for {address}"))?;
            for tx in txs.as_array().cloned().unwrap_or_default() {
                let txid = tx
                    .get("txid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                for output in tx
                    .get("vout")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                {
                    let output_address = output
                        .get("scriptpubkey_address")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if output_address != address {
                        continue;
                    }
                    let vout = output.get("n").and_then(Value::as_u64).unwrap_or_default();
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
                    deposits.push(record);
                }
            }
            let utxos = btc_address_utxos_json(&address, &esplora)?;
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
                deposits.push(record);
            }
        }
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
            "network": "bitcoin",
            "esplora": esplora,
            "tip_height": tip_height,
            "scanner_started": LN_SCANNER_STARTED.load(Ordering::SeqCst),
            "persisted": persisted,
            "deposits": deposits,
            "stored_deposits": store
                .list_btc_deposit_records()?
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>()
        })))
    })
}

extern "C" fn btc_address_pool_status() -> *const Dynamic {
    native_result(|| {
        let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
        let available = store.list_btc_address_pool_records()?;
        let used = store.list_used_btc_address_pool_records()?;
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
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
        let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
        let mut added = 0usize;
        if count > 0 {
            let body = json!({
                "account_id": default_account_id()?,
                "network": "bitcoin",
                "purpose": "manual_refill",
                "count": count,
                "timestamp_ms": now_ms()
            });
            let response = signer_request(SIGNER_ADDRESS_BATCH_PATH, &body)?;
            let addresses = response
                .get("addresses")
                .and_then(Value::as_array)
                .with_context(|| {
                    format!("signer batch address response missing `addresses`: {response}")
                })?;
            ensure!(
                addresses.len() == count,
                "signer batch address response count mismatch: requested={count}, returned={}",
                addresses.len()
            );
            let existing = store
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
                .collect::<std::collections::BTreeSet<_>>();
            for response in addresses {
                let address = response
                    .get("address")
                    .and_then(Value::as_str)
                    .filter(|address| !address.trim().is_empty())
                    .with_context(|| {
                        format!("signer batch address item missing non-empty `address`: {response}")
                    })?;
                if existing.contains(address) {
                    continue;
                }
                store.put_btc_address_pool_record(
                    address,
                    &json!({
                        "address": address,
                        "created_at_ms": now_ms(),
                        "purpose": "manual_refill",
                        "signer_response": response
                    }),
                )?;
                added += 1;
            }
        }
        let available = store.list_btc_address_pool_records()?;
        let used = store.list_used_btc_address_pool_records()?;
        Ok(json_to_dynamic(&json!({
            "module": "btc",
            "ok": true,
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

extern "C" fn btc_sign_psbt(psbt: *const Dynamic, ident: *const Dynamic) -> *const Dynamic {
    native_two_string_dynamic_result(psbt, ident, |psbt, ident| {
        ensure!(!psbt.trim().is_empty(), "psbt must not be empty");
        let signing_account = btc_account_for_ident(ident)?;
        let body = json!({
            "account_id": default_account_id()?,
            "ident": ident,
            "domain": "bihelix-btc-wallet",
            "psbt": psbt,
            "signing_accounts": [signing_account],
            "policy": {},
            "expires_at_ms": now_ms() + 300000,
            "timestamp_ms": now_ms()
        });
        let response = signer_request(SIGNER_PSBT_SIGN_PATH, &body)?;
        Ok(json_to_dynamic(&response))
    })
}

extern "C" fn btc_broadcast(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |tx_hex| {
        let tx_hex = tx_hex.to_string();
        ensure!(!tx_hex.trim().is_empty(), "tx_hex must not be empty");
        let esplora = btc_esplora_url();
        let url = format!("{}/tx", esplora.trim_end_matches('/'));
        let response = attohttpc::post(&url)
            .header("content-type", "text/plain")
            .text(tx_hex)
            .send()
            .with_context(|| format!("POST {url}"))?;
        let status = response.status();
        let txid = response
            .text()
            .with_context(|| format!("read Esplora response body from {url}"))?;
        ensure!(
            (200..300).contains(&status.as_u16()),
            "Esplora POST {url} failed with HTTP {status}: {txid}"
        );
        Ok(ok(json!({
            "module": "btc",
            "broadcast": true,
            "network": "bitcoin",
            "esplora": esplora,
            "txid": txid.trim()
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

extern "C" fn rgb_signed(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| signed_request(input, ""))
}

extern "C" fn rgb_request_signature(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let body = signed_body(input)?;
        let signature = request_signature(SIGNER_REQUEST_SIGNATURE_PATH, &body)?;
        Ok(ok(json!({
            "status": "signed",
            "account_id": default_account_id()?,
            "signer_node": signer_node_id()?,
            "transport": "iroh",
            "signature": signature
        })))
    })
}

extern "C" fn rgb_asset_authorization(
    asset_id: *const Dynamic,
    amount: u64,
    purpose: *const Dynamic,
    recipient: *const Dynamic,
    anchor_psbt: *const Dynamic,
    expires_at_ms: u64,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let purpose = unsafe { &*purpose };
    let recipient = unsafe { &*recipient };
    let anchor_psbt = unsafe { &*anchor_psbt };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(purpose.is_str(), "purpose must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(anchor_psbt.is_str(), "anchor_psbt must be string");
        let asset_id = asset_id.as_str().to_string();
        let purpose = if purpose.as_str().trim().is_empty() {
            "l1_transfer".to_string()
        } else {
            purpose.as_str().to_string()
        };
        let recipient =
            (!recipient.as_str().trim().is_empty()).then(|| recipient.as_str().to_string());
        let anchor_psbt =
            (!anchor_psbt.as_str().trim().is_empty()).then(|| anchor_psbt.as_str().to_string());
        let expires_at_ms = (expires_at_ms > 0)
            .then_some(expires_at_ms)
            .unwrap_or_else(|| now_ms() + 300000);
        let body = json!({
            "account_id": default_account_id()?,
            "permission": "asset_authorization",
            "payload": {
                "account_id": default_account_id()?,
                "asset_id": asset_id,
                "amount": amount,
                "purpose": purpose,
                "recipient": recipient,
                "anchor_psbt": anchor_psbt,
                "expires_at_ms": expires_at_ms
            },
            "domain": "bihelix-rgb-service",
            "expires_at_ms": expires_at_ms,
            "timestamp_ms": now_ms()
        });
        let signature = request_signature(SIGNER_ASSET_AUTHORIZATION_PATH, &body)?;
        let payload = body.get("payload").cloned().unwrap_or(Value::Null);
        Ok(ok(json!({
            "status": "signed",
            "account_id": default_account_id()?,
            "signer_node": signer_node_id()?,
            "transport": "iroh",
            "asset_authorization_request": {
                "asset_id": payload.get("asset_id").cloned().unwrap_or(Value::Null),
                "amount": payload.get("amount").cloned().unwrap_or(Value::Null),
                "purpose": payload.get("purpose").cloned().unwrap_or(Value::Null),
                "recipient": payload.get("recipient").cloned().unwrap_or(Value::Null),
                "anchor_psbt": payload.get("anchor_psbt").cloned().unwrap_or(Value::Null),
                "expires_at_ms": payload.get("expires_at_ms").cloned().unwrap_or(Value::Null),
            },
            "signature": signature
        })))
    })
}

extern "C" fn rgb_request(route: *const Dynamic, payload: *const Dynamic) -> *const Dynamic {
    let route = unsafe { &*route };
    let payload = unsafe { &*payload };
    native_result(|| {
        ensure!(route.is_str(), "route must be string");
        rgb_post_dynamic(payload, route.as_str())
    })
}

extern "C" fn rgb_rna_balance() -> *const Dynamic {
    native_result(|| rgb_post_dynamic(&Dynamic::Null, "/v1/rna/balance"))
}
extern "C" fn rgb_issue(
    ticker: *const Dynamic,
    name: *const Dynamic,
    precision: u8,
    supply: u64,
    allocation_outpoint: *const Dynamic,
) -> *const Dynamic {
    let ticker = unsafe { &*ticker };
    let name = unsafe { &*name };
    let allocation_outpoint = unsafe { &*allocation_outpoint };
    native_result(|| {
        ensure!(ticker.is_str(), "ticker must be string");
        ensure!(name.is_str(), "name must be string");
        ensure!(
            allocation_outpoint.is_str(),
            "allocation_outpoint must be string"
        );
        let payload = json!({
            "ticker": ticker.as_str(),
            "name": name.as_str(),
            "precision": precision,
            "supply": supply,
            "allocation_outpoint": allocation_outpoint.as_str()
        });
        rgb_post_dynamic(&json_to_dynamic(&payload), "/v1/assets/issue")
    })
}
extern "C" fn rgb_assets() -> *const Dynamic {
    native_result(|| rgb_post_dynamic(&Dynamic::Null, "/v1/assets/list"))
}
extern "C" fn rgb_token_list() -> *const Dynamic {
    native_result(|| {
        let response = http_get_json(&daemon_route_url("/v1/tokens/list")?)?;
        Ok(json_to_dynamic(&response))
    })
}
extern "C" fn rgb_balance(asset_id: *const Dynamic, scope: *const Dynamic) -> *const Dynamic {
    native_two_string_dynamic_result(asset_id, scope, |asset_id, scope| {
        let scope = if scope.trim().is_empty() {
            "all"
        } else {
            scope
        };
        rgb_post_dynamic(
            &json_to_dynamic(&json!({
                "asset_id": asset_id,
                "scope": scope
            })),
            "/v1/balance",
        )
    })
}
extern "C" fn rgb_balance_breakdown(asset_id: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(asset_id, |asset_id| {
        rgb_post_dynamic(
            &json_to_dynamic(&json!({
                "asset_id": asset_id
            })),
            "/v1/balance/breakdown",
        )
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
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let recipient = unsafe { &*recipient };
    let unsigned_anchor_psbt = unsafe { &*unsigned_anchor_psbt };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(recipient.is_str(), "recipient must be string");
        ensure!(
            unsigned_anchor_psbt.is_str(),
            "unsigned_anchor_psbt must be string"
        );
        let asset_id = asset_id.as_str().to_string();
        let recipient = recipient.as_str().to_string();
        let unsigned_anchor_psbt = unsigned_anchor_psbt.as_str().to_string();
        let fee_rate_sat_vb = (fee_rate_sat_vb > 0).then_some(fee_rate_sat_vb);
        let expires_at_ms = now_ms() + 300000;
        let auth_body = json!({
            "account_id": default_account_id()?,
            "permission": "prepare_transfer",
            "payload": {
                "account_id": default_account_id()?,
                "asset_id": asset_id,
                "amount": amount,
                "purpose": "l1_transfer",
                "recipient": recipient,
                "anchor_psbt": unsigned_anchor_psbt,
                "expires_at_ms": expires_at_ms
            },
            "domain": "bihelix-rgb-service",
            "expires_at_ms": expires_at_ms,
            "timestamp_ms": now_ms()
        });
        let asset_authorization = json!({
            "asset_id": asset_id,
            "amount": amount,
            "purpose": "l1_transfer",
            "recipient": recipient,
            "anchor_psbt": unsigned_anchor_psbt,
            "expires_at_ms": expires_at_ms,
            "signature": request_signature(SIGNER_ASSET_AUTHORIZATION_PATH, &auth_body)?
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
}
extern "C" fn rgb_commit_transfer(
    asset_id: *const Dynamic,
    amount: u64,
    transfer_id: *const Dynamic,
    txid: *const Dynamic,
    signed_anchor_psbt: *const Dynamic,
) -> *const Dynamic {
    let asset_id = unsafe { &*asset_id };
    let transfer_id = unsafe { &*transfer_id };
    let txid = unsafe { &*txid };
    let signed_anchor_psbt = unsafe { &*signed_anchor_psbt };
    native_result(|| {
        ensure!(asset_id.is_str(), "asset_id must be string");
        ensure!(transfer_id.is_str(), "transfer_id must be string");
        ensure!(txid.is_str(), "txid must be string");
        ensure!(
            signed_anchor_psbt.is_str(),
            "signed_anchor_psbt must be string"
        );
        let asset_id = asset_id.as_str().to_string();
        let signed_anchor_psbt = (!signed_anchor_psbt.as_str().trim().is_empty())
            .then(|| signed_anchor_psbt.as_str().to_string());
        let expires_at_ms = now_ms() + 300000;
        let auth_body = json!({
            "account_id": default_account_id()?,
            "permission": "commit_transfer",
            "payload": {
                "account_id": default_account_id()?,
                "asset_id": asset_id,
                "amount": amount,
                "purpose": "l1_transfer",
                "recipient": Value::Null,
                "anchor_psbt": signed_anchor_psbt,
                "expires_at_ms": expires_at_ms
            },
            "domain": "bihelix-rgb-service",
            "expires_at_ms": expires_at_ms,
            "timestamp_ms": now_ms()
        });
        let asset_authorization = json!({
            "asset_id": asset_id,
            "amount": amount,
            "purpose": "l1_transfer",
            "recipient": Value::Null,
            "anchor_psbt": signed_anchor_psbt,
            "expires_at_ms": expires_at_ms,
            "signature": request_signature(SIGNER_ASSET_AUTHORIZATION_PATH, &auth_body)?
        });
        let payload = json!({
            "transfer_id": transfer_id.as_str(),
            "txid": txid.as_str(),
            "signed_anchor_psbt": signed_anchor_psbt,
            "asset_authorization": asset_authorization
        });
        rgb_post_dynamic(&json_to_dynamic(&payload), "/v1/transfers/commit")
    })
}
extern "C" fn rgb_pending() -> *const Dynamic {
    native_result(|| rgb_post_dynamic(&Dynamic::Null, "/v1/pending/list"))
}
extern "C" fn rgb_recover(operation_id: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(operation_id, |operation_id| {
        let payload = json!({
            "operation_id": (!operation_id.trim().is_empty()).then(|| operation_id.to_string())
        });
        rgb_post_dynamic(&json_to_dynamic(&payload), "/v1/recover")
    })
}
extern "C" fn rgb_test(scenario: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(scenario, |scenario| {
        let scenario = if scenario.trim().is_empty() {
            "full_rgb20_lifecycle"
        } else {
            scenario
        };
        rgb_post_dynamic(
            &json_to_dynamic(&json!({
                "scenario": scenario
            })),
            "/v1/test/rgb",
        )
    })
}

extern "C" fn ln_status() -> *const Dynamic {
    native_result(|| {
        let node = current_ln_node();
        let (node_id, status, peers, channels, balances, storage_dir, network) =
            if let Some(node) = node.as_ref() {
                let balance = node.balance_snapshot();
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
            "module": "ln",
            "enabled": true,
            "ln_rgb_lightning_linked": true,
            "ln_rgb_composer_bound": LN_RGB_COMPOSER.get().is_some(),
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
                "pending_channel_closure_sweeps": balance.pending_channel_closure_sweeps
            }))
        })))
    })
}

extern "C" fn ln_token_list() -> *const Dynamic {
    native_result(|| {
        let signer: Arc<dyn RgbServiceSigner + Send + Sync> = Arc::new(ConsoleRgbServiceSigner);
        let client =
            RgbServiceClient::new(daemon_url()?, signer).map_err(|err| anyhow::anyhow!("{err}"))?;
        let response = client
            .token_list()
            .map_err(|err| anyhow::anyhow!("{err}"))?;
        Ok(json_to_dynamic(&serde_json::to_value(response)?))
    })
}

extern "C" fn ln_rgb_channel_context(
    contract_id: *const Dynamic,
    amount: u64,
    outbound: bool,
) -> *const Dynamic {
    let contract_id = unsafe { &*contract_id };
    native_result(|| {
        ensure!(contract_id.is_str(), "contract_id must be string");
        let contract_id = parse_ln_contract_id(contract_id.as_str())?;
        let asset = LdkRgbAssetAmount::new(contract_id, amount);
        let context = RgbChannelContext::new(asset).into_rgb_context(outbound);
        Ok(ok(json!({
            "module": "ln",
            "backend": "ln-rgb-lightning",
            "contract_id": context.contract_id.to_string(),
            "funding_rgb": context.funding_rgb,
            "to_self": context.to_self,
            "outbound": outbound,
            "has_funding_ref": context.has_funding_ref()
        })))
    })
}

extern "C" fn ln_start() -> *const Dynamic {
    native_result(|| {
        let lightning = local_dynamic("lightning").context(
            "missing root value `local/lightning`; run ln::node_address and root::add first",
        )?;
        let requested = dynamic_to_json(&lightning);
        let path = find_string_field(&requested, &["path"])
            .map(PathBuf::from)
            .unwrap_or_else(|| ln_node_path(&lightning));
        let mut stored = read_json_file(&path)
            .with_context(|| format!("read LN node state {}", path.display()))?;
        let address = find_string_field(&stored, &["address", "btc_address"]).unwrap_or_default();
        let low_water_sats = value_u64(&stored, "low_water_sats").unwrap_or(LN_LOW_WATER_SATS);
        let config = normalized_ln_config(ln_config_from_value(&stored), low_water_sats);
        let mnemonic = ensure_ln_entropy_mnemonic(&mut stored)?;
        write_private_json_file(&path, &stored)
            .with_context(|| format!("write LN node state {}", path.display()))?;
        let composer_bound = bind_ln_rgb_composer()?;
        let interval_ms = optional_u64(&lightning, "interval_ms")
            .or_else(|| value_u64(&stored, "interval_ms"))
            .unwrap_or_else(|| LN_SCAN_DEFAULT_INTERVAL.as_millis() as u64);
        let interval = Duration::from_millis(interval_ms.max(1000));
        let ln_config = console_ln_rgb_config(&config, mnemonic)?;

        if LN_STARTED.swap(true, Ordering::SeqCst) {
            let node = current_ln_node();
            return Ok(ok(json!({
                "module": "ln",
                "started": true,
                "already_running": true,
                "address": address,
                "config": config,
                "source": "local/lightning",
                "backend": "ln-rgb",
                "rgb_backend": "ln-rgb-lightning",
                "ln_rgb_composer_bound": composer_bound,
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
            "module": "ln",
            "started": true,
            "already_running": false,
            "address": address,
            "config": config,
            "source": "local/lightning",
            "backend": "ln-rgb",
            "rgb_backend": "ln-rgb-lightning",
            "ln_rgb_composer_bound": composer_bound,
            "node_id": node_id,
            "listeners": ["l1_onchain_deposit", "l2_ln_deposit"],
            "scan_enabled": false,
            "note": "LnRgbBtcLnBackend is running with patched ln-rgb-lightning ChannelManager"
        })))
    })
}

extern "C" fn ln_stop() -> *const Dynamic {
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
            "module": "ln",
            "stopped": true,
            "backend": "ln-rgb"
        })))
    })
}

extern "C" fn ln_events() -> *const Dynamic {
    native_result(|| ln_events_with_limit(100))
}

fn ln_events_with_limit(limit: usize) -> Result<Dynamic> {
    let node = running_ln_node()?;
    let mut out = Vec::new();
    for _ in 0..limit {
        let Some(event) = node.next_event_debug() else {
            break;
        };
        out.push(json!(event));
        node.event_handled()?;
    }
    Ok(ok(json!({
        "module": "ln",
        "events": out
    })))
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

extern "C" fn ln_get_node_id() -> *const Dynamic {
    native_result(|| {
        let node = running_ln_node()?;
        Ok(ok(json!({
            "module": "ln",
            "node_id": node.node_id().to_string()
        })))
    })
}

extern "C" fn ln_get_addr() -> *const Dynamic {
    native_result(|| {
        Ok(ok(json!({
            "module": "ln",
            "address": current_ln_hot_address()?
        })))
    })
}

extern "C" fn ln_get_peers() -> *const Dynamic {
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
            "module": "ln",
            "peers": peers
        })))
    })
}

extern "C" fn ln_get_channels() -> *const Dynamic {
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
            "module": "ln",
            "channels": channels
        })))
    })
}

extern "C" fn ln_connect(
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
            "module": "ln",
            "connected": true,
            "node_id": peer_node_id.to_string(),
            "address": address.to_string(),
            "persist": persist
        })))
    })
}

extern "C" fn ln_open_channel(
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
            "module": "ln",
            "channel_open_submitted": true,
            "channel_id": channel_id,
            "node_id": peer_node_id.to_string(),
            "address": address.to_string()
        })))
    })
}

extern "C" fn ln_close_channel(
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
            "module": "ln",
            "channel_close_submitted": true,
            "channel_id": channel_id,
            "counterparty_node_id": counterparty_node_id.to_string(),
            "force": force
        })))
    })
}

extern "C" fn ln_invoice(
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
            "module": "ln",
            "invoice": invoice.to_string()
        })))
    })
}

extern "C" fn ln_pay(input: *const Dynamic) -> *const Dynamic {
    native_string_dynamic_result(input, |invoice| {
        let node = running_ln_node()?;
        let invoice = Bolt11Invoice::from_str(invoice)
            .map_err(|err| anyhow::anyhow!("parse BOLT11 invoice: {err:?}"))?;
        let payment_hash = node
            .pay_bolt11(BtcLnBolt11PaymentRequest { invoice })
            .context("send BOLT11 payment")?;
        Ok(ok(json!({
            "module": "ln",
            "payment_hash": payment_hash
        })))
    })
}

extern "C" fn ln_rgb_start() -> *const Dynamic {
    ln_start()
}

extern "C" fn ln_rgb_stop() -> *const Dynamic {
    ln_stop()
}

extern "C" fn ln_rgb_status() -> *const Dynamic {
    ln_status()
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

extern "C" fn ln_rgb_get_addr() -> *const Dynamic {
    native_result(|| {
        Ok(ok(json!({
            "module": "ln_rgb",
            "address": current_ln_hot_address()?
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

extern "C" fn ln_rgb_get_peers() -> *const Dynamic {
    ln_get_peers()
}

extern "C" fn ln_rgb_get_channels() -> *const Dynamic {
    ln_get_channels()
}

extern "C" fn ln_rgb_connect(
    node_id: *const Dynamic,
    address: *const Dynamic,
    persist: bool,
) -> *const Dynamic {
    ln_connect(node_id, address, persist)
}

extern "C" fn ln_rgb_open_channel(
    node_id: *const Dynamic,
    address: *const Dynamic,
    amount_sats: u64,
    push_msat: u64,
) -> *const Dynamic {
    ln_open_channel(node_id, address, amount_sats, push_msat)
}

extern "C" fn ln_rgb_close_channel(
    channel_id: *const Dynamic,
    counterparty_node_id: *const Dynamic,
    force: bool,
    reason: *const Dynamic,
) -> *const Dynamic {
    ln_close_channel(channel_id, counterparty_node_id, force, reason)
}

extern "C" fn ln_rgb_invoice(
    amount_msat: u64,
    description: *const Dynamic,
    expiry_secs: u64,
) -> *const Dynamic {
    ln_invoice(amount_msat, description, expiry_secs)
}

extern "C" fn ln_rgb_pay(input: *const Dynamic) -> *const Dynamic {
    ln_pay(input)
}

extern "C" fn ln_rgb_events() -> *const Dynamic {
    ln_events()
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

extern "C" fn ln_rgb_rgb_channel_context(
    contract_id: *const Dynamic,
    amount: u64,
    outbound: bool,
) -> *const Dynamic {
    ln_rgb_channel_context(contract_id, amount, outbound)
}

extern "C" fn ln_spawn_scanner(interval_ms: u64) -> *const Dynamic {
    native_result(|| {
        let btc_addr = default_account_id()?;
        let rgb_service = local_string("rgb-service").unwrap_or_default();
        let interval_ms = (interval_ms > 0)
            .then_some(interval_ms)
            .unwrap_or_else(|| LN_SCAN_DEFAULT_INTERVAL.as_millis() as u64);
        let interval = Duration::from_millis(interval_ms.max(1000));
        let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
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
                "module": "ln",
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
            "module": "ln",
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

extern "C" fn ln_node_address(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let path = ln_node_path(input);
        let low_water_sats = optional_u64(input, "low_water_sats").unwrap_or(LN_LOW_WATER_SATS);
        let config = normalized_ln_config(dynamic_to_json(input), low_water_sats);
        if path.exists() {
            let mut stored = read_json_file(&path)
                .with_context(|| format!("read LN node state {}", path.display()))?;
            update_ln_node_config(&mut stored, config, low_water_sats);
            let mnemonic = ensure_ln_entropy_mnemonic(&mut stored)?;
            let config = stored
                .get("config")
                .cloned()
                .unwrap_or_else(|| normalized_ln_config(Value::Object(Map::new()), low_water_sats));
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
            let address_source = stored
                .get("address_source")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let address = find_string_field(&stored, &["address", "btc_address"]);
            if address_source != "ln_hot_wallet" || address.is_none() {
                let mnemonic = BdkMnemonic::parse_in_normalized(BdkLanguage::English, &mnemonic)
                    .context("invalid LN hot wallet mnemonic")?;
                let mut wallet = LocalWallet::open_with_mnemonic(&data_dir, network, &mnemonic)?;
                let address = wallet.wallet.reveal_next_address(KeychainKind::External);
                wallet.persist()?;
                if let Value::Object(object) = &mut stored {
                    object.insert("address".to_string(), json!(address.address.to_string()));
                    object.remove("btc_address");
                    object.remove("signer_response");
                    object.remove("signer_node");
                    object.insert("address_source".to_string(), json!("ln_hot_wallet"));
                    object.insert("wallet_can_sign".to_string(), json!(true));
                    object.insert("updated_at_ms".to_string(), json!(now_ms()));
                }
            }
            write_private_json_file(&path, &stored)
                .with_context(|| format!("write LN node state {}", path.display()))?;
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

fn bind_ln_rgb_composer() -> Result<bool> {
    if LN_RGB_COMPOSER.get().is_some() {
        return Ok(true);
    }
    let signer: Arc<dyn RgbServiceSigner + Send + Sync> = Arc::new(ConsoleRgbServiceSigner);
    let client =
        RgbServiceClient::new(daemon_url()?, signer).map_err(|err| anyhow::anyhow!("{err}"))?;
    let composer = Arc::new(
        RgbDaemonLnTxComposer::new(client, default_account_id()?, 300000)
            .map_err(|err| anyhow::anyhow!("{err}"))?,
    );
    let global_composer: Arc<dyn RgbLnTxComposer + Send + Sync> = composer.clone();
    init_rgb_ln_tx_composer(global_composer);
    let _ = LN_RGB_COMPOSER.set(composer);
    Ok(LN_RGB_COMPOSER.get().is_some())
}

fn ln_node_slot() -> &'static Mutex<Option<Arc<LnRgbBtcLnBackend>>> {
    LN_RGB_NODE.get_or_init(|| Mutex::new(None))
}

fn current_ln_node() -> Option<Arc<LnRgbBtcLnBackend>> {
    ln_node_slot()
        .lock()
        .expect("LN node slot lock poisoned")
        .as_ref()
        .cloned()
}

fn running_ln_node() -> Result<Arc<LnRgbBtcLnBackend>> {
    current_ln_node().context("LN RGB node is not running; call ln::start() first")
}

fn console_ln_rgb_config(config: &Value, mnemonic: String) -> Result<BtcLnRuntimeConfig> {
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
        account_id: default_account_id()?,
        listen,
        entropy_mnemonic: Some(mnemonic),
        trusted_peers_0conf,
        accept_inbound_channels: config
            .get("accept_inbound_channels")
            .and_then(Value::as_bool)
            .unwrap_or(true),
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

extern "C" fn ln_scanner_status() -> *const Dynamic {
    native_result(|| {
        let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
        let available = store.list_btc_address_pool_records()?;
        let used = store.list_used_btc_address_pool_records()?;
        Ok(ok(json!({
            "module": "ln",
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
            let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
            let available = store.list_btc_address_pool_records()?.len();
            if available < BTC_ADDRESS_POOL_LOW_WATER {
                let count = BTC_ADDRESS_POOL_TARGET.saturating_sub(available);
                if count > 0 {
                    let response = signer_request(
                        SIGNER_ADDRESS_BATCH_PATH,
                        &json!({
                            "account_id": default_account_id()?,
                            "network": "bitcoin",
                            "purpose": "low_water_refill",
                            "count": count,
                            "timestamp_ms": now_ms()
                        }),
                    )?;
                    let addresses = response
                        .get("addresses")
                        .and_then(Value::as_array)
                        .with_context(|| {
                            format!("signer batch address response missing `addresses`: {response}")
                        })?;
                    ensure!(
                        addresses.len() == count,
                        "signer batch address response count mismatch: requested={count}, returned={}",
                        addresses.len()
                    );
                    let existing = store
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
                        .collect::<std::collections::BTreeSet<_>>();
                    for response in addresses {
                        let address = response
                            .get("address")
                            .and_then(Value::as_str)
                            .filter(|address| !address.trim().is_empty())
                            .with_context(|| {
                                format!(
                                    "signer batch address item missing non-empty `address`: {response}"
                                )
                            })?;
                        if existing.contains(address) {
                            continue;
                        }
                        store.put_btc_address_pool_record(
                            address,
                            &json!({
                                "address": address,
                                "created_at_ms": now_ms(),
                                "purpose": "low_water_refill",
                                "signer_response": response
                            }),
                        )?;
                    }
                }
            }

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
            let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
            let available = store.list_btc_address_pool_records()?.len();
            if available >= BTC_ADDRESS_POOL_LOW_WATER {
                return Ok(());
            }
            let count = BTC_ADDRESS_POOL_TARGET.saturating_sub(available);
            if count == 0 {
                return Ok(());
            }
            let response = signer_request(
                SIGNER_ADDRESS_BATCH_PATH,
                &json!({
                    "account_id": default_account_id()?,
                    "network": "bitcoin",
                    "purpose": "low_water_refill",
                    "count": count,
                    "timestamp_ms": now_ms()
                }),
            )?;
            let addresses = response
                .get("addresses")
                .and_then(Value::as_array)
                .with_context(|| {
                    format!("signer batch address response missing `addresses`: {response}")
                })?;
            ensure!(
                addresses.len() == count,
                "signer batch address response count mismatch: requested={count}, returned={}",
                addresses.len()
            );
            let existing = store
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
                .collect::<std::collections::BTreeSet<_>>();
            for response in addresses {
                let address = response
                    .get("address")
                    .and_then(Value::as_str)
                    .filter(|address| !address.trim().is_empty())
                    .with_context(|| {
                        format!("signer batch address item missing non-empty `address`: {response}")
                    })?;
                if existing.contains(address) {
                    continue;
                }
                store.put_btc_address_pool_record(
                    address,
                    &json!({
                        "address": address,
                        "created_at_ms": now_ms(),
                        "purpose": "low_water_refill",
                        "signer_response": response
                    }),
                )?;
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

fn request_options(input: &Dynamic, route: &str) -> Result<Dynamic> {
    let url = daemon_route_url(route)?;
    let request = json!({
        "method": "POST",
        "url": url,
        "json": dynamic_to_json(&signed_request(input, route)?),
        "timeout_ms": optional_u64(input, "timeout_ms").unwrap_or(30000)
    });
    Ok(json_to_dynamic(&request))
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
    let (host, port, path) = parse_http_url(url)?;
    let body = serde_json::to_vec(body)?;
    let mut stream = TcpStream::connect((host.as_str(), port))
        .with_context(|| format!("connect RGB service daemon {host}:{port}"))?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes())?;
    stream.write_all(&body)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
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

fn http_get_json(url: &str) -> Result<Value> {
    let (host, port, path) = parse_http_url(url)?;
    let mut stream = TcpStream::connect((host.as_str(), port))
        .with_context(|| format!("connect RGB service daemon {host}:{port}"))?;
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    parse_http_json_response(&path, response)
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
    let store = LocalNodeStore::open(&PathBuf::from(".zust-console"))?;
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
    let esplora = btc_esplora_url();
    let stats = esplora_get_json(&format!(
        "{}/address/{address}",
        esplora.trim_end_matches('/')
    ))
    .with_context(|| format!("fetch BTC L1 balance for {address}"))?;
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
    let payload = json!({
        "account_id": address,
        "ident": ident,
        "account": account,
        "tracked_utxos": tracked_utxos_json(&address)?
    });
    Ok(dynamic_to_json(&rgb_post_dynamic(
        &json_to_dynamic(&payload),
        "/v1/assets/list",
    )?))
}

fn btc_address_utxos_json(address: &str, esplora: &str) -> Result<Value> {
    esplora_get_json(&format!(
        "{}/address/{address}/utxo",
        esplora.trim_end_matches('/')
    ))
    .with_context(|| format!("fetch BTC L1 UTXOs for {address}"))
}

fn tracked_utxos_json(address: &str) -> Result<Value> {
    let esplora = btc_esplora_url();
    let utxos = btc_address_utxos_json(address, &esplora)?;
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

fn esplora_get_json(url: &str) -> Result<Value> {
    let response = attohttpc::get(url)
        .header("accept", "application/json")
        .send()
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let body = response
        .text()
        .with_context(|| format!("read Esplora response body from {url}"))?;
    let json = if body.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&body)
            .with_context(|| format!("decode Esplora JSON body from {url}: {body}"))?
    };
    if !(200..300).contains(&status.as_u16()) {
        bail!("Esplora GET {url} failed with HTTP {status}: {json}");
    }
    Ok(json)
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
        .map(Ok)
        .unwrap_or_else(|| request_signature(SIGNER_REQUEST_SIGNATURE_PATH, &payload))?;
    Ok(json_to_dynamic(&json!({
        "payload": payload,
        "signature": signature
    })))
}

fn signed_body(input: &Dynamic) -> Result<Value> {
    let permission = optional_string(input, "permission")
        .or_else(|| optional_string(input, "operation"))
        .unwrap_or_else(|| "request_signature".to_string());
    Ok(json!({
        "account_id": default_account_id()?,
        "permission": permission,
        "payload": signed_payload(input, "")?,
        "domain": optional_string(input, "domain").unwrap_or_else(|| "bihelix-rgb-service".to_string()),
        "expires_at_ms": optional_u64(input, "expires_at_ms").unwrap_or_else(|| now_ms() + 300000),
        "timestamp_ms": now_ms()
    }))
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
    if matches!(
        route,
        "/v1/assets/list" | "/v1/balance" | "/v1/balance/breakdown"
    ) {
        object
            .entry("tracked_utxos".to_string())
            .or_insert(tracked_utxos_json(&default_account_id()?)?);
    }
    if route == "/v1/balance" {
        object.entry("scope".to_string()).or_insert(json!("all"));
    }
    Ok(Value::Object(object))
}

pub(crate) fn default_account_id() -> Result<String> {
    local_string("btc-addr").context("missing root value `local/btc-addr`")
}

fn signer_node_id() -> Result<String> {
    local_string("signer-node").context("missing root value `local/signer-node`")
}

fn btc_esplora_url() -> String {
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
        .filter(|url| !url.trim().is_empty())
        .unwrap_or_else(|| LN_ESPLORA_DEFAULT.to_string())
}

pub(crate) fn request_signature(path: &str, body: &Value) -> Result<Value> {
    let response = signer_request(path, body)?;
    response
        .get("signature")
        .cloned()
        .with_context(|| format!("signer response missing `signature`: {response}"))
}

fn signer_request(path: &str, body: &Value) -> Result<Value> {
    let signer_node = signer_node_id()?;
    let path = path.to_string();
    let body = json_to_dynamic(body);
    eprintln!("[zust-console] signer request start: path={path}, signer={signer_node}");
    let mut request = Dynamic::list(Vec::<Dynamic>::new());
    request.push(path);
    request.push_dynamic(body);
    let bytes = dynamic_to_msgpack(&request);
    let response = console_async_runtime()?.block_on(iroh_call_with_retries(signer_node, bytes))?;
    eprintln!("[zust-console] signer request returned");
    Ok(dynamic_to_json(&response))
}

fn console_async_runtime() -> Result<&'static tokio::runtime::Runtime> {
    if let Some(runtime) = CONSOLE_ASYNC_RUNTIME.get() {
        return Ok(runtime);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build zust-console async runtime")?;
    let _ = CONSOLE_ASYNC_RUNTIME.set(runtime);
    CONSOLE_ASYNC_RUNTIME
        .get()
        .context("zust-console async runtime was not initialized")
}

async fn iroh_call_with_retries(signer_node: String, bytes: Vec<u8>) -> Result<Dynamic> {
    let remote_id = EndpointId::from_str(&signer_node)
        .with_context(|| format!("invalid local/signer-node iroh id: {signer_node}"))?;
    let remote_addr = EndpointAddr::new(remote_id);
    let endpoint = console_iroh_endpoint().await?;

    let mut last_error = None;
    for attempt in 1..=SIGNER_ATTEMPTS {
        eprintln!("[zust-console] signer attempt {attempt}/{SIGNER_ATTEMPTS}");
        let result = tokio::time::timeout(
            SIGNER_TIMEOUT,
            iroh_call(&endpoint, remote_addr.clone(), bytes.clone()),
        )
        .await;
        match result {
            Ok(Ok(response)) => return Ok(response),
            Ok(Err(err)) => {
                eprintln!("[zust-console] signer attempt {attempt} failed: {err:#}");
                last_error = Some(err);
            }
            Err(err) => {
                let err = anyhow::anyhow!("signer iroh request timed out after 10s: {err}");
                eprintln!("[zust-console] signer attempt {attempt} failed: {err:#}");
                last_error = Some(err);
            }
        }
        if attempt < SIGNER_ATTEMPTS {
            tokio::time::sleep(SIGNER_RETRY_DELAY).await;
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("signer iroh request failed")))
        .with_context(|| format!("signer iroh request failed after {SIGNER_ATTEMPTS} attempts"))
}

async fn iroh_call(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
    bytes: Vec<u8>,
) -> Result<Dynamic> {
    eprintln!("[zust-console] iroh connect signer");
    let conn = tokio::time::timeout(SIGNER_TIMEOUT, endpoint.connect(remote_addr, SIGNER_ALPN))
        .await
        .context("connect signer iroh endpoint timed out")?
        .context("connect signer iroh endpoint")?;
    eprintln!("[zust-console] iroh open bi stream");
    let (mut send, mut recv) = tokio::time::timeout(SIGNER_TIMEOUT, conn.open_bi())
        .await
        .context("open signer iroh stream timed out")?
        .context("open signer iroh stream")?;
    eprintln!("[zust-console] iroh write request: bytes={}", bytes.len());
    tokio::time::timeout(SIGNER_TIMEOUT, send.write_all(&bytes))
        .await
        .context("write signer msgpack request timed out")?
        .context("write signer msgpack request")?;
    eprintln!("[zust-console] iroh finish request stream");
    send.finish().context("finish signer request stream")?;
    eprintln!("[zust-console] iroh read response");
    let response = tokio::time::timeout(SIGNER_TIMEOUT, recv.read_to_end(1024 * 1024))
        .await
        .context("read signer msgpack response timed out")?
        .context("read signer msgpack response")?;
    eprintln!(
        "[zust-console] iroh response received: bytes={}",
        response.len()
    );
    eprintln!("[zust-console] signer response decode msgpack");
    msgpack_to_dynamic(&response)
}

fn console_iroh_secret() -> SecretKey {
    CONSOLE_IROH_SECRET.get_or_init(SecretKey::generate).clone()
}

async fn console_iroh_endpoint() -> Result<Endpoint> {
    if let Some(endpoint) = CONSOLE_IROH_ENDPOINT.get() {
        return Ok(endpoint.clone());
    }

    eprintln!("[zust-console] iroh bind endpoint");
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(console_iroh_secret())
        .alpns(vec![SIGNER_ALPN.to_vec()])
        .bind()
        .await
        .context("bind signer iroh endpoint")?;
    eprintln!("[zust-console] iroh wait online");
    endpoint.online().await;
    eprintln!("[zust-console] iroh online");

    let _ = CONSOLE_IROH_ENDPOINT.set(endpoint.clone());
    Ok(endpoint)
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

fn native_two_string_dynamic_result(
    first: *const Dynamic,
    second: *const Dynamic,
    f: impl FnOnce(&str, &str) -> Result<Dynamic>,
) -> *const Dynamic {
    let first = unsafe { &*first };
    let second = unsafe { &*second };
    native_result(|| {
        ensure!(first.is_str(), "first argument must be string");
        ensure!(second.is_str(), "second argument must be string");
        f(first.as_str(), second.as_str())
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

fn parse_ln_contract_id(value: &str) -> Result<LnContractId> {
    let value = value.trim();
    ensure!(value.len() == 64, "contract_id must be 32-byte hex");
    let mut bytes = [0u8; 32];
    for index in 0..32 {
        let offset = index * 2;
        bytes[index] = u8::from_str_radix(&value[offset..offset + 2], 16)
            .with_context(|| format!("invalid contract_id hex at byte {index}"))?;
    }
    Ok(LnContractId::from(bytes))
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

fn bytes_to_hex(bytes: &[u8; 32]) -> String {
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
            "LN node state {} has no signer address; run ln::node_address first",
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

fn dynamic_to_msgpack(value: &Dynamic) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.encode(&mut bytes);
    bytes
}

fn msgpack_to_dynamic(bytes: &[u8]) -> Result<Dynamic> {
    let (dynamic, consumed) = Dynamic::decode(bytes)?;
    ensure!(
        consumed == bytes.len(),
        "trailing data after Zust Dynamic msgpack payload"
    );
    Ok(dynamic)
}

fn ln_node_path(input: &Dynamic) -> PathBuf {
    optional_string(input, "path")
        .or_else(|| local_string("ln-node-file"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(LN_NODE_DEFAULT_PATH))
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

fn update_ln_node_config(stored: &mut Value, config: Value, low_water_sats: u64) {
    let Value::Object(object) = stored else {
        return;
    };
    object.insert("low_water_sats".to_string(), json!(low_water_sats));
    object.insert("config".to_string(), config);
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
        "module": "ln",
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
