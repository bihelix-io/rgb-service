use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    OnceLock,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use dynamic::{Dynamic, FromJson, MsgPack, MsgUnpack, ToJson, Type};
use iroh::{endpoint::presets, Endpoint, EndpointAddr, EndpointId, SecretKey};
use serde_json::{json, Map, Value};
use std::str::FromStr;
use vm::Vm;

const SIGNER_ALPN: &[u8] = b"bihelix/signer/1";
const SIGNER_REQUEST_SIGNATURE_PATH: &str = "/v1/signer/request-signature";
const SIGNER_ASSET_AUTHORIZATION_PATH: &str = "/v1/signer/asset-authorization";
const SIGNER_ADDRESS_NEW_PATH: &str = "/v1/signer/address/new";
const SIGNER_TIMEOUT: Duration = Duration::from_secs(10);
const SIGNER_ATTEMPTS: usize = 3;
const SIGNER_RETRY_DELAY: Duration = Duration::from_millis(500);
const LN_SCAN_DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
const LN_NODE_DEFAULT_PATH: &str = ".zust-console/ln-node.json";
const LN_DATA_DIR_DEFAULT: &str = ".zust-console/lightning";
const LN_LDK_DATA_DIR_DEFAULT: &str = ".zust-console/lightning/ldk";
const LN_LISTEN_DEFAULT: &str = "0.0.0.0:9736";
const LN_ESPLORA_DEFAULT: &str = "https://mempool.space/api";
const LN_LOW_WATER_SATS: u64 = 100_000;
static CONSOLE_IROH_SECRET: OnceLock<SecretKey> = OnceLock::new();
static CONSOLE_IROH_ENDPOINT: OnceLock<Endpoint> = OnceLock::new();
static CONSOLE_ASYNC_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static LN_STARTED: AtomicBool = AtomicBool::new(false);
static LN_SCANNER_STARTED: AtomicBool = AtomicBool::new(false);

fn daemon_url() -> Result<String> {
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
    register_env_module(vm)?;
    register_bdk_module(vm)?;
    register_rgb_module(vm)?;
    register_ln_module(vm)?;
    Ok(())
}

fn register_env_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write().unwrap();
    jit.add_native_module_ptr("env", "get", &[Type::Str], Type::Str, env_get as *const u8)?;
    Ok(())
}

fn register_bdk_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write().unwrap();
    jit.add_native_module_ptr(
        "bdk",
        "wallet",
        &[Type::Any],
        Type::Any,
        bdk_wallet as *const u8,
    )?;
    jit.add_native_module_ptr(
        "bdk",
        "sign_request",
        &[Type::Any],
        Type::Any,
        bdk_sign_request as *const u8,
    )?;
    jit.add_native_module_ptr(
        "bdk",
        "asset_authorization",
        &[Type::Any],
        Type::Any,
        bdk_asset_authorization as *const u8,
    )?;
    jit.add_native_module_ptr(
        "bdk",
        "external_anchor",
        &[Type::Any],
        Type::Any,
        bdk_external_anchor as *const u8,
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
        "register_iroh_node",
        &[Type::Any],
        Type::Any,
        rgb_register_iroh_node as *const u8,
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
        &[Type::Any],
        Type::Any,
        rgb_asset_authorization as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "request",
        &[Type::Any],
        Type::Any,
        rgb_request as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "issue",
        &[Type::Any],
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
        "balance",
        &[Type::Any],
        Type::Any,
        rgb_balance as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "balance_breakdown",
        &[Type::Any],
        Type::Any,
        rgb_balance_breakdown as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "invoice",
        &[Type::Any],
        Type::Any,
        rgb_invoice as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "prepare_transfer",
        &[Type::Any],
        Type::Any,
        rgb_prepare_transfer as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "commit_transfer",
        &[Type::Any],
        Type::Any,
        rgb_commit_transfer as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "send_consignment",
        &[Type::Any],
        Type::Any,
        rgb_send_consignment as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "receive_consignment",
        &[Type::Any],
        Type::Any,
        rgb_receive_consignment as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "pending",
        &[Type::Any],
        Type::Any,
        rgb_pending as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "recover",
        &[Type::Any],
        Type::Any,
        rgb_recover as *const u8,
    )?;
    jit.add_native_module_ptr(
        "rgb",
        "test",
        &[Type::Any],
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
        &[Type::Any],
        Type::Any,
        ln_spawn_scanner as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "start",
        &[Type::Any],
        Type::Any,
        ln_start as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "scanner_status",
        &[Type::Any],
        Type::Any,
        ln_scanner_status as *const u8,
    )?;
    jit.add_native_module_ptr(
        "ln",
        "status",
        &[Type::Any],
        Type::Any,
        ln_status as *const u8,
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

extern "C" fn env_get(name: *const Dynamic) -> *const Dynamic {
    native_string_result(name, |name| {
        Ok(std::env::var(name.as_str()).unwrap_or_default())
    })
}

extern "C" fn bdk_wallet(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        Ok(ok(json!({
            "module": "bdk",
            "role": "external_btc_wallet",
            "network": optional_string(input, "network").unwrap_or_else(|| "regtest".to_string()),
            "data_dir": optional_string(input, "data_dir").unwrap_or_default(),
            "note": "BTC UTXO selection, PSBT signing, and broadcast stay outside RGB Service"
        })))
    })
}

extern "C" fn bdk_sign_request(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let body = signed_body(input)?;
        let signature = request_signature(SIGNER_REQUEST_SIGNATURE_PATH, &body)?;
        Ok(json_to_dynamic(&signature))
    })
}

extern "C" fn bdk_asset_authorization(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let asset_id = required_string(input, "asset_id")?;
        let amount = required_u64(input, "amount")?;
        let purpose =
            optional_string(input, "purpose").unwrap_or_else(|| "l1_transfer".to_string());
        let signature = request_signature(SIGNER_ASSET_AUTHORIZATION_PATH, &signed_body(input)?)?;
        let authorization = json!({
            "asset_id": asset_id,
            "amount": amount,
            "purpose": purpose,
            "recipient": optional_string(input, "recipient"),
            "anchor_psbt": optional_string(input, "anchor_psbt"),
            "expires_at_ms": optional_u64(input, "expires_at_ms").unwrap_or_else(|| now_ms() + 300000),
            "signature": signature
        });
        Ok(json_to_dynamic(&authorization))
    })
}

extern "C" fn bdk_external_anchor(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        Ok(ok(json!({
            "unsigned_anchor_psbt": optional_string(input, "unsigned_anchor_psbt"),
            "signed_anchor_psbt": optional_string(input, "signed_anchor_psbt"),
            "txid": optional_string(input, "txid"),
            "change_vout": optional_u64(input, "change_vout"),
            "recipient_vout": optional_u64(input, "recipient_vout"),
            "note": "Populate these fields from the real external BTC wallet before prepare/commit"
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

extern "C" fn rgb_asset_authorization(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let asset_id = required_string(input, "asset_id")?;
        let amount = required_u64(input, "amount")?;
        let purpose =
            optional_string(input, "purpose").unwrap_or_else(|| "l1_transfer".to_string());
        let mut body = signed_body(input)?;
        if let Value::Object(object) = &mut body {
            object.insert("asset_id".to_string(), json!(asset_id));
            object.insert("amount".to_string(), json!(amount));
            object.insert("purpose".to_string(), json!(purpose));
            object.insert(
                "recipient".to_string(),
                optional_string(input, "recipient")
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            object.insert(
                "anchor_psbt".to_string(),
                optional_string(input, "anchor_psbt")
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
            object.insert(
                "expires_at_ms".to_string(),
                json!(optional_u64(input, "expires_at_ms").unwrap_or_else(|| now_ms() + 300000)),
            );
        }
        let signature = request_signature(SIGNER_ASSET_AUTHORIZATION_PATH, &body)?;
        Ok(ok(json!({
            "status": "signed",
            "account_id": default_account_id()?,
            "signer_node": signer_node_id()?,
            "transport": "iroh",
            "asset_authorization_request": {
                "asset_id": body.get("asset_id").cloned().unwrap_or(Value::Null),
                "amount": body.get("amount").cloned().unwrap_or(Value::Null),
                "purpose": body.get("purpose").cloned().unwrap_or(Value::Null),
                "recipient": body.get("recipient").cloned().unwrap_or(Value::Null),
                "anchor_psbt": body.get("anchor_psbt").cloned().unwrap_or(Value::Null),
                "expires_at_ms": body.get("expires_at_ms").cloned().unwrap_or(Value::Null),
            },
            "signature": signature
        })))
    })
}

extern "C" fn rgb_request(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let route = required_string(input, "route")?;
        rgb_post_dynamic(input, &route)
    })
}

extern "C" fn rgb_register_iroh_node(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/iroh-nodes/register")
}
extern "C" fn rgb_rna_balance() -> *const Dynamic {
    native_result(|| rgb_post_dynamic(&Dynamic::Null, "/v1/rna/balance"))
}
extern "C" fn rgb_issue(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/assets/issue")
}
extern "C" fn rgb_assets(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/assets/list")
}
extern "C" fn rgb_balance(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/balance")
}
extern "C" fn rgb_balance_breakdown(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/balance/breakdown")
}
extern "C" fn rgb_invoice(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/invoices/create")
}
extern "C" fn rgb_prepare_transfer(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/transfers/prepare")
}
extern "C" fn rgb_commit_transfer(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/transfers/commit")
}
extern "C" fn rgb_send_consignment(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/consignments/send")
}
extern "C" fn rgb_receive_consignment(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/consignments/receive")
}
extern "C" fn rgb_pending(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/pending/list")
}
extern "C" fn rgb_recover(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/recover")
}
extern "C" fn rgb_test(input: *const Dynamic) -> *const Dynamic {
    rgb_route(input, "/v1/test/rgb")
}

extern "C" fn ln_status(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |_input| {
        Ok(ok(json!({
            "module": "ln",
            "enabled": true,
            "started": LN_STARTED.load(Ordering::SeqCst),
            "scanner_started": LN_SCANNER_STARTED.load(Ordering::SeqCst),
            "layers": ["l1", "l2"],
            "mode": "hot_wallet",
            "note": "LN node service is deferred; only the Zust API and scanner placeholder are wired"
        })))
    })
}

extern "C" fn ln_start(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let lightning = if is_null_or_empty_object(input) {
            local_dynamic("lightning").context(
                "missing root value `local/lightning`; run ln::node_address and root::add first",
            )?
        } else {
            input.clone()
        };
        let node = dynamic_to_json(&lightning);
        let address = find_string_field(&node, &["address", "btc_address"]).unwrap_or_default();
        let low_water_sats = value_u64(&node, "low_water_sats").unwrap_or(LN_LOW_WATER_SATS);
        let config = normalized_ln_config(ln_config_from_value(&node), low_water_sats);
        let interval_ms = optional_u64(&lightning, "interval_ms")
            .or_else(|| value_u64(&node, "interval_ms"))
            .unwrap_or_else(|| LN_SCAN_DEFAULT_INTERVAL.as_millis() as u64);
        let interval = Duration::from_millis(interval_ms.max(1000));

        if LN_STARTED.swap(true, Ordering::SeqCst) {
            return Ok(ok(json!({
                "module": "ln",
                "started": true,
                "already_running": true,
                "address": address,
                "config": config,
                "source": "local/lightning",
                "listeners": ["l1_onchain_deposit", "l2_ln_deposit"],
                "scan_enabled": false
            })));
        }

        let thread_node = node.clone();
        if let Err(err) = thread::Builder::new()
            .name("zust-ln-inbound-listener".to_string())
            .spawn(move || ln_inbound_loop(thread_node, interval))
        {
            LN_STARTED.store(false, Ordering::SeqCst);
            return Err(err).context("spawn LN inbound listener thread");
        }

        Ok(ok(json!({
            "module": "ln",
            "started": true,
            "already_running": false,
            "address": address,
            "config": config,
            "source": "local/lightning",
            "listeners": ["l1_onchain_deposit", "l2_ln_deposit"],
            "scan_enabled": false,
            "note": "LN inbound listener is running; real L1/L2 scanning is intentionally disabled"
        })))
    })
}

extern "C" fn ln_spawn_scanner(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let btc_addr = default_account_id()?;
        let rgb_service = local_string("rgb-service").unwrap_or_default();
        let interval_ms = optional_u64(input, "interval_ms")
            .unwrap_or_else(|| LN_SCAN_DEFAULT_INTERVAL.as_millis() as u64);
        let interval = Duration::from_millis(interval_ms.max(1000));

        if LN_SCANNER_STARTED.swap(true, Ordering::SeqCst) {
            return Ok(ok(json!({
                "module": "ln",
                "scanner_started": true,
                "already_running": true,
                "btc_addr": btc_addr,
                "rgb_service": rgb_service,
                "layers": ["l1", "l2"],
                "scan_enabled": false
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
            "scan_enabled": false,
            "note": "scanner thread is started, but real chain scanning is intentionally disabled"
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
            write_private_json_file(&path, &stored)
                .with_context(|| format!("write LN node state {}", path.display()))?;
            return Ok(ok(redacted_ln_node_response(stored, &path, false)));
        }

        let body = json!({
            "account_id": default_account_id()?,
            "network": optional_string(input, "network").unwrap_or_else(|| "bitcoin".to_string()),
            "purpose": "ln_node_hot_wallet",
            "low_water_sats": low_water_sats,
            "timestamp_ms": now_ms()
        });
        let signer_response = signer_request(SIGNER_ADDRESS_NEW_PATH, &body)?;
        let stored = json!({
            "version": 1,
            "kind": "ln_node_hot_wallet",
            "created_at_ms": now_ms(),
            "account_id": default_account_id()?,
            "signer_node": signer_node_id()?,
            "low_water_sats": low_water_sats,
            "config": config,
            "signer_response": signer_response
        });
        write_private_json_file(&path, &stored)
            .with_context(|| format!("write LN node state {}", path.display()))?;
        Ok(ok(redacted_ln_node_response(stored, &path, true)))
    })
}

extern "C" fn ln_scanner_status(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |_input| {
        Ok(ok(json!({
            "module": "ln",
            "scanner_started": LN_SCANNER_STARTED.load(Ordering::SeqCst),
            "layers": ["l1", "l2"],
            "scan_enabled": false
        })))
    })
}

fn ln_scanner_loop(_btc_addr: String, _rgb_service: String, interval: Duration) {
    loop {
        thread::sleep(interval);
    }
}

fn ln_inbound_loop(_node: Value, interval: Duration) {
    loop {
        thread::sleep(interval);
    }
}

fn rgb_route(input: *const Dynamic, route: &str) -> *const Dynamic {
    native_dynamic_result(input, |input| rgb_post_dynamic(input, route))
}

fn rgb_post_dynamic(input: &Dynamic, route: &str) -> Result<Dynamic> {
    let options = request_options(input, route)?;
    let response = http_request_options(&options)?;
    Ok(json_to_dynamic(&response))
}

fn request_options(input: &Dynamic, route: &str) -> Result<Dynamic> {
    let daemon_url = daemon_url()?;
    let url = format!("{}{}", daemon_url.trim_end_matches('/'), route);
    let request = json!({
        "method": "POST",
        "url": url,
        "json": dynamic_to_json(&signed_request(input, route)?),
        "timeout_ms": optional_u64(input, "timeout_ms").unwrap_or(30000)
    });
    Ok(json_to_dynamic(&request))
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
    if route == "/v1/iroh-nodes/register" {
        object
            .entry("btc_address".to_string())
            .or_insert(json!(default_account_id()?));
        object
            .entry("iroh_node_id".to_string())
            .or_insert(json!(signer_node_id()?));
    } else if route == "/v1/iroh-nodes/lookup" {
        object
            .entry("btc_address".to_string())
            .or_insert(json!(default_account_id()?));
    }
    Ok(Value::Object(object))
}

fn default_account_id() -> Result<String> {
    local_string("btc-addr").context("missing root value `local/btc-addr`")
}

fn signer_node_id() -> Result<String> {
    local_string("signer-node").context("missing root value `local/signer-node`")
}

fn request_signature(path: &str, body: &Value) -> Result<Value> {
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
    let conn = endpoint
        .connect(remote_addr, SIGNER_ALPN)
        .await
        .context("connect signer iroh endpoint")?;
    eprintln!("[zust-console] iroh open bi stream");
    let (mut send, mut recv) = conn.open_bi().await.context("open signer iroh stream")?;
    eprintln!("[zust-console] iroh write request: bytes={}", bytes.len());
    send.write_all(&bytes)
        .await
        .context("write signer msgpack request")?;
    eprintln!("[zust-console] iroh finish request stream");
    send.finish().context("finish signer request stream")?;
    eprintln!("[zust-console] iroh read response");
    let response = recv
        .read_to_end(1024 * 1024)
        .await
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

fn native_string_result(
    input: *const Dynamic,
    f: impl FnOnce(&Dynamic) -> Result<String>,
) -> *const Dynamic {
    let input = unsafe { &*input };
    match f(input) {
        Ok(value) => Box::into_raw(Box::new(Dynamic::from(value))),
        Err(_) => Box::into_raw(Box::new(Dynamic::from(""))),
    }
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

fn required_u64(input: &Dynamic, key: &str) -> Result<u64> {
    optional_u64(input, key).with_context(|| format!("missing `{key}`"))
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

fn is_null_or_empty_object(value: &Dynamic) -> bool {
    if matches!(value, Dynamic::Null) {
        return true;
    }
    matches!(dynamic_to_json(value), Value::Object(object) if object.is_empty())
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
        .entry("accept_inbound_rgb_transfers".to_string())
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
