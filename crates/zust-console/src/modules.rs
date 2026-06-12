use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use dynamic::{Dynamic, FromJson, MsgPack, MsgUnpack, ToJson, Type};
use serde_json::{Map, Value, json};
use vm::Vm;


#[derive(Clone, Debug)]
struct ConsoleConfig {
    daemon_url: String,
    timeout_ms: u64,
}

static CONSOLE_CONFIG: OnceLock<RwLock<ConsoleConfig>> = OnceLock::new();
static SIGNER_NODE_CACHE: OnceLock<RwLock<HashMap<String, Value>>> = OnceLock::new();

pub fn configure_console(input: &Dynamic) -> Result<()> {
    let daemon_url = required_string(input, "daemon_url")?;
    ensure!(daemon_url.starts_with("http://"), "daemon_url must start with http://");
    let timeout_ms = optional_u64(input, "timeout_ms").unwrap_or(30000);
    let config = ConsoleConfig { daemon_url, timeout_ms };
    let lock = CONSOLE_CONFIG.get_or_init(|| RwLock::new(config.clone()));
    let mut guard = lock.write().map_err(|_| anyhow::anyhow!("Zust console config lock poisoned"))?;
    *guard = config;
    Ok(())
}

fn console_config() -> Result<ConsoleConfig> {
    let lock = CONSOLE_CONFIG
        .get()
        .context("zust-console requires daemon config at startup: pass {\"daemon_url\":\"http://host:port\"}")?;
    let guard = lock.read().map_err(|_| anyhow::anyhow!("Zust console config lock poisoned"))?;
    Ok(guard.clone())
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
    jit.add_native_module_ptr("bdk", "wallet", &[Type::Any], Type::Any, bdk_wallet as *const u8)?;
    jit.add_native_module_ptr("bdk", "sign_request", &[Type::Any], Type::Any, bdk_sign_request as *const u8)?;
    jit.add_native_module_ptr("bdk", "asset_authorization", &[Type::Any], Type::Any, bdk_asset_authorization as *const u8)?;
    jit.add_native_module_ptr("bdk", "external_anchor", &[Type::Any], Type::Any, bdk_external_anchor as *const u8)?;
    Ok(())
}

fn register_rgb_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write().unwrap();
    jit.add_native_module_ptr("rgb", "signed", &[Type::Any], Type::Any, rgb_signed as *const u8)?;
    jit.add_native_module_ptr("rgb", "register_iroh_node", &[Type::Any], Type::Any, rgb_register_iroh_node as *const u8)?;
    jit.add_native_module_ptr("rgb", "rna_balance", &[Type::Any], Type::Any, rgb_rna_balance as *const u8)?;
    jit.add_native_module_ptr("rgb", "request_signature", &[Type::Any], Type::Any, rgb_request_signature as *const u8)?;
    jit.add_native_module_ptr("rgb", "asset_authorization", &[Type::Any], Type::Any, rgb_asset_authorization as *const u8)?;
    jit.add_native_module_ptr("rgb", "request", &[Type::Any], Type::Any, rgb_request as *const u8)?;
    jit.add_native_module_ptr("rgb", "issue", &[Type::Any], Type::Any, rgb_issue as *const u8)?;
    jit.add_native_module_ptr("rgb", "assets", &[Type::Any], Type::Any, rgb_assets as *const u8)?;
    jit.add_native_module_ptr("rgb", "balance", &[Type::Any], Type::Any, rgb_balance as *const u8)?;
    jit.add_native_module_ptr("rgb", "balance_breakdown", &[Type::Any], Type::Any, rgb_balance_breakdown as *const u8)?;
    jit.add_native_module_ptr("rgb", "invoice", &[Type::Any], Type::Any, rgb_invoice as *const u8)?;
    jit.add_native_module_ptr("rgb", "prepare_transfer", &[Type::Any], Type::Any, rgb_prepare_transfer as *const u8)?;
    jit.add_native_module_ptr("rgb", "commit_transfer", &[Type::Any], Type::Any, rgb_commit_transfer as *const u8)?;
    jit.add_native_module_ptr("rgb", "send_consignment", &[Type::Any], Type::Any, rgb_send_consignment as *const u8)?;
    jit.add_native_module_ptr("rgb", "receive_consignment", &[Type::Any], Type::Any, rgb_receive_consignment as *const u8)?;
    jit.add_native_module_ptr("rgb", "pending", &[Type::Any], Type::Any, rgb_pending as *const u8)?;
    jit.add_native_module_ptr("rgb", "recover", &[Type::Any], Type::Any, rgb_recover as *const u8)?;
    jit.add_native_module_ptr("rgb", "test", &[Type::Any], Type::Any, rgb_test as *const u8)?;
    Ok(())
}

fn register_ln_module(vm: &Vm) -> Result<()> {
    let mut jit = vm.jit.write().unwrap();
    jit.add_native_module_ptr("ln", "status", &[Type::Any], Type::Any, ln_status as *const u8)?;
    Ok(())
}

extern "C" fn env_get(name: *const Dynamic) -> *const Dynamic {
    native_string_result(name, |name| Ok(std::env::var(name.as_str()).unwrap_or_default()))
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
        let signer_id = optional_string(input, "signer_id").unwrap_or_else(|| "zust-console".to_string());
        Ok(json_to_dynamic(&request_signature(&signer_id)))
    })
}

extern "C" fn bdk_asset_authorization(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let asset_id = required_string(input, "asset_id")?;
        let amount = required_u64(input, "amount")?;
        let purpose = optional_string(input, "purpose").unwrap_or_else(|| "l1_transfer".to_string());
        let signer_id = optional_string(input, "signer_id").unwrap_or_else(|| "zust-console".to_string());
        let authorization = json!({
            "asset_id": asset_id,
            "amount": amount,
            "purpose": purpose,
            "recipient": optional_string(input, "recipient"),
            "anchor_psbt": optional_string(input, "anchor_psbt"),
            "expires_at_ms": optional_u64(input, "expires_at_ms").unwrap_or_else(|| now_ms() + 300000),
            "signature": request_signature(&signer_id)
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
    native_dynamic_result(input, |input| Ok(signed_request(input)))
}

extern "C" fn rgb_request_signature(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let btc_address = optional_btc_address(input).context("missing `btc_addr`")?;
        let signer_target = resolve_signer_target(input, &btc_address)?;
        Ok(ok(json!({
            "status": "waiting_for_signer_app",
            "btc_addr": btc_address,
            "signer_target": signer_target,
            "transport": "iroh",
            "note": "rgb Zust module resolved btc_addr to signer iroh node; console is only the script runtime"
        })))
    })
}

extern "C" fn rgb_asset_authorization(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let btc_address = optional_btc_address(input).context("missing `btc_addr`")?;
        let asset_id = required_string(input, "asset_id")?;
        let amount = required_u64(input, "amount")?;
        let purpose = optional_string(input, "purpose").unwrap_or_else(|| "l1_transfer".to_string());
        let signer_target = resolve_signer_target(input, &btc_address)?;
        Ok(ok(json!({
            "status": "waiting_for_signer_app",
            "btc_addr": btc_address,
            "signer_target": signer_target,
            "transport": "iroh",
            "asset_authorization_request": {
                "asset_id": asset_id,
                "amount": amount,
                "purpose": purpose,
                "recipient": optional_string(input, "recipient"),
                "anchor_psbt": optional_string(input, "anchor_psbt"),
                "expires_at_ms": optional_u64(input, "expires_at_ms").unwrap_or_else(|| now_ms() + 300000)
            },
            "note": "send this request to the signer App over iroh and continue with the returned authorization"
        })))
    })
}

extern "C" fn rgb_request(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |input| {
        let route = required_string(input, "route")?;
        rgb_post_dynamic(input, &route)
    })
}

extern "C" fn rgb_register_iroh_node(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/iroh-nodes/register") }
extern "C" fn rgb_lookup_iroh_node(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/iroh-nodes/lookup") }
extern "C" fn rgb_rna_balance(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/rna/balance") }
extern "C" fn rgb_issue(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/assets/issue") }
extern "C" fn rgb_assets(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/assets/list") }
extern "C" fn rgb_balance(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/balance") }
extern "C" fn rgb_balance_breakdown(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/balance/breakdown") }
extern "C" fn rgb_invoice(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/invoices/create") }
extern "C" fn rgb_prepare_transfer(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/transfers/prepare") }
extern "C" fn rgb_commit_transfer(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/transfers/commit") }
extern "C" fn rgb_send_consignment(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/consignments/send") }
extern "C" fn rgb_receive_consignment(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/consignments/receive") }
extern "C" fn rgb_pending(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/pending/list") }
extern "C" fn rgb_recover(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/recover") }
extern "C" fn rgb_test(input: *const Dynamic) -> *const Dynamic { rgb_route(input, "/v1/test/rgb") }

fn optional_btc_address(input: &Dynamic) -> Option<String> {
    optional_string(input, "btc_addr")
        .or_else(|| optional_string(input, "btc_address"))
        .filter(|value| !value.trim().is_empty())
}

fn resolve_signer_target(input: &Dynamic, btc_address: &str) -> Result<Value> {
    let cache = SIGNER_NODE_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
    if let Some(binding) = cache
        .read()
        .map_err(|_| anyhow::anyhow!("signer node cache lock poisoned"))?
        .get(btc_address)
        .cloned()
    {
        return Ok(json!({
            "btc_address": btc_address,
            "iroh_node_id": binding.get("iroh_node_id").cloned().unwrap_or(Value::Null),
            "binding": binding,
            "cached": true
        }));
    }

    let account_id = required_string(input, "account_id")?;
    let signer_id = optional_string(input, "signer_id").unwrap_or_else(|| "zust-console".to_string());
    let lookup = json_to_dynamic(&json!({
        "payload": {
            "account_id": account_id,
            "btc_address": btc_address
        },
        "signature": dynamic_to_json(&input.get_dynamic("signature").unwrap_or_else(|| json_to_dynamic(&request_signature(&signer_id))))
    }));
    let response = rgb_post_dynamic(&lookup, "/v1/iroh-nodes/lookup")?;
    let response = dynamic_to_json(&response);
    let binding = response
        .get("binding")
        .filter(|value| !value.is_null())
        .cloned()
        .with_context(|| format!("no iroh signer node registered for btc address {btc_address}"))?;
    let iroh_node_id = binding
        .get("iroh_node_id")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("iroh node binding for {btc_address} is missing iroh_node_id"))?
        .to_string();
    cache
        .write()
        .map_err(|_| anyhow::anyhow!("signer node cache lock poisoned"))?
        .insert(btc_address.to_string(), binding.clone());
    Ok(json!({
        "btc_address": btc_address,
        "iroh_node_id": iroh_node_id,
        "binding": binding,
        "cached": false
    }))
}

extern "C" fn ln_status(input: *const Dynamic) -> *const Dynamic {
    native_dynamic_result(input, |_input| {
        Ok(ok(json!({
            "module": "ln",
            "enabled": false,
            "status": "disabled",
            "message": "LN module is registered for Zust API compatibility; implementation is intentionally deferred"
        })))
    })
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
    let config = console_config()?;
    let url = format!("{}{}", config.daemon_url.trim_end_matches('/'), route);
    let request = json!({
        "method": "POST",
        "url": url,
        "json": dynamic_to_json(&signed_request(input)),
        "timeout_ms": optional_u64(input, "timeout_ms").unwrap_or(config.timeout_ms)
    });
    Ok(json_to_dynamic(&request))
}

fn http_request_options(options: &Dynamic) -> Result<Value> {
    let method = optional_string(options, "method").unwrap_or_else(|| "POST".to_string());
    ensure!(method.eq_ignore_ascii_case("POST"), "rgb daemon requests must use POST");
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
        serde_json::from_str(body).with_context(|| format!("decode RGB service JSON body: {body}"))?
    };
    if !(200..300).contains(&status_code) {
        bail!("RGB service {path} failed with HTTP {status_code}: {json}");
    }
    Ok(json)
}

fn parse_http_url(value: &str) -> Result<(String, u16, String)> {
    let value = value.strip_prefix("http://").context("daemon_url must start with http://")?;
    let (authority, path) = value.split_once('/').unwrap_or((value, ""));
    let (host, port) = authority.split_once(':').unwrap_or((authority, "8787"));
    ensure!(!host.is_empty(), "daemon_url host must not be empty");
    let port = port.parse::<u16>().context("invalid daemon_url port")?;
    Ok((host.to_string(), port, format!("/{path}")))
}

fn signed_request(input: &Dynamic) -> Dynamic {
    let payload = input.get_dynamic("payload").unwrap_or_else(|| {
        let json = dynamic_to_json(input);
        let mut object = match json {
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
        json_to_dynamic(&Value::Object(object))
    });
    let signer_id = optional_string(input, "signer_id").unwrap_or_else(|| "zust-console".to_string());
    let signature = input
        .get_dynamic("signature")
        .map(|signature| dynamic_to_json(&signature))
        .unwrap_or_else(|| request_signature(&signer_id));
    json_to_dynamic(&json!({
        "payload": dynamic_to_json(&payload),
        "signature": signature
    }))
}

fn request_signature(signer_id: &str) -> Value {
    json!({
        "signer_id": signer_id,
        "public_key": format!("{signer_id}-public-key"),
        "scheme": "ed25519",
        "nonce": format!("zust-{}", now_ms()),
        "timestamp_ms": now_ms(),
        "signature": format!("zust-console-signature-{signer_id}")
    })
}

fn ok(value: Value) -> Dynamic {
    let out = json_to_dynamic(&value);
    out.insert("ok", true);
    out
}

fn native_dynamic_result(input: *const Dynamic, f: impl FnOnce(&Dynamic) -> Result<Dynamic>) -> *const Dynamic {
    let input = unsafe { &*input };
    match f(input) {
        Ok(value) => Box::into_raw(Box::new(value)),
        Err(err) => Box::into_raw(Box::new(json_to_dynamic(&json!({
            "ok": false,
            "error": err.to_string()
        }))))
    }
}

fn native_string_result(input: *const Dynamic, f: impl FnOnce(&Dynamic) -> Result<String>) -> *const Dynamic {
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
    input.get_dynamic(key).map(|value| value.as_str().to_string())
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
    let (dynamic, consumed) = Dynamic::from_json(json.as_bytes()).expect("serde_json emitted invalid JSON for Zust Dynamic");
    assert!(json.as_bytes()[consumed..].iter().all(|byte| byte.is_ascii_whitespace()), "Zust Dynamic FromJson did not consume full JSON value");
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
    ensure!(consumed == bytes.len(), "trailing data after Zust Dynamic msgpack payload");
    Ok(dynamic)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
