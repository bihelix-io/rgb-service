use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::OnceLock;
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
const SIGNER_TIMEOUT: Duration = Duration::from_secs(10);
const SIGNER_ATTEMPTS: usize = 3;
const SIGNER_RETRY_DELAY: Duration = Duration::from_millis(500);
static CONSOLE_IROH_SECRET: OnceLock<SecretKey> = OnceLock::new();
static CONSOLE_IROH_ENDPOINT: OnceLock<Endpoint> = OnceLock::new();

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
        "status",
        &[Type::Any],
        Type::Any,
        ln_status as *const u8,
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
    Ok(json!({
        "account_id": default_account_id()?,
        "payload": signed_payload(input, "")?,
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
    let signer_node = signer_node_id()?;
    let path = path.to_string();
    let body = json_to_dynamic(body);
    eprintln!("[zust-console] signer request start: path={path}, signer={signer_node}");
    let mut request = Dynamic::list(Vec::<Dynamic>::new());
    request.push(path);
    request.push_dynamic(body);
    let bytes = dynamic_to_msgpack(&request);
    let response =
        root::block_on_async(move || Box::pin(iroh_call_with_retries(signer_node, bytes)))?;
    eprintln!("[zust-console] signer request returned");
    let response = dynamic_to_json(&response);
    response
        .get("signature")
        .cloned()
        .with_context(|| format!("signer response missing `signature`: {response}"))
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
