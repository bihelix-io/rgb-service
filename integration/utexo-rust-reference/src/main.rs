//! Isolated signet reference wallet. Never run against the only wallet copy.
use rgb_lib::{
    AssetSchema, Assignment,
    wallet::{Invoice, Recipient, WitnessData},
};
use rgb_lib::{
    BitcoinNetwork,
    wallet::{
        DatabaseType, OnlineOptions, RgbWalletOpsOffline, RgbWalletOpsOnline, SinglesigKeys,
        Wallet, WalletData,
    },
};
use serde_json::{Value, json};
use std::collections::HashMap;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{fs, path::PathBuf};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 3 || args.len() > 4 {
        return Err(
            "usage: utexo-rust-reference <isolated-wallet-dir> <keys-json> [operation-json]".into(),
        );
    }
    let dir = PathBuf::from(&args[1]);
    fs::create_dir_all(&dir)?;
    #[cfg(unix)]
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    let lock_path = dir.join("reference.lock");
    let _lock = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)?;
    let result = run(&dir, &args[2], args.get(3));
    fs::remove_file(lock_path)?;
    result
}
fn run(
    dir: &std::path::Path,
    key_path: &str,
    operation_path: Option<&String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let keys: Value = serde_json::from_slice(&fs::read(key_path)?)?;
    let field = |k: &str| {
        keys[k]
            .as_str()
            .map(String::from)
            .ok_or("missing key field")
    };
    let data = WalletData {
        data_dir: dir.to_string_lossy().into(),
        bitcoin_network: BitcoinNetwork::Signet,
        database_type: DatabaseType::Sqlite,
        max_allocations_per_utxo: 1,
        supported_schemas: vec![
            AssetSchema::Nia,
            AssetSchema::Ifa,
            AssetSchema::Cfa,
            AssetSchema::Uda,
        ],
        reuse_addresses: false,
    };
    let keys = SinglesigKeys {
        account_xpub_vanilla: field("accountXpubVanilla")?,
        account_xpub_colored: field("accountXpubColored")?,
        master_fingerprint: field("masterFingerprint")?,
        mnemonic: None,
        vanilla_keychain: Some(0),
        witness_version: Default::default(),
    };
    let mut wallet = Wallet::new(data.clone(), keys.clone())?;
    let online = wallet.go_online(OnlineOptions {
        indexer_url: "https://esplora-api.utexo.com".into(),
        skip_consistency_check: false,
        vanilla_sync_lookback: 20,
    })?;
    if let Some(spec_path) = operation_path {
        let spec: Value = serde_json::from_slice(&fs::read(spec_path)?)?;
        let op = spec["operation"].as_str().ok_or("operation required")?;
        let output = PathBuf::from(
            spec["output"]
                .as_str()
                .ok_or("new output directory required")?,
        );
        fs::create_dir(&output)?; // One-shot marker: inspect before retrying any ambiguous failure.
        #[cfg(unix)]
        fs::set_permissions(&output, fs::Permissions::from_mode(0o700))?;
        fs::write(
            output.join("intent.json"),
            serde_json::to_vec_pretty(&spec)?,
        )?;
        let result = match op {
            "witness" => {
                let asset = spec["asset_id"]
                    .as_str()
                    .ok_or("asset_id required")?
                    .to_string();
                let amount = spec["amount"]
                    .as_u64()
                    .filter(|n| *n > 0)
                    .ok_or("positive amount required")?;
                let expiry = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs()
                    + 86400;
                serde_json::to_value(wallet.witness_receive(
                    Some(asset),
                    Assignment::Fungible(amount),
                    Some(expiry),
                    vec!["rpcs://rgb-proxy.utexo.com/json-rpc".into()],
                    1,
                )?)?
            }
            "prepare-send" => {
                let invoice =
                    Invoice::new(spec["invoice"].as_str().ok_or("invoice required")?.into())?
                        .invoice_data();
                if !matches!(
                    invoice.network,
                    BitcoinNetwork::Signet | BitcoinNetwork::SignetCustom
                ) {
                    return Err("wrong network".into());
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs();
                if invoice.expiration_timestamp.is_some_and(|t| t <= now) {
                    return Err("expired invoice".into());
                }
                if !matches!(invoice.assignment, Assignment::Fungible(n) if n > 0) {
                    return Err("fungible invoice required".into());
                }
                if invoice.transport_endpoints != ["rpcs://rgb-proxy.utexo.com/json-rpc"] {
                    return Err("unexpected transport".into());
                }
                let asset = invoice.asset_id.ok_or("contract-bound invoice required")?;
                let recipient = Recipient {
                    recipient_id: invoice.recipient_id,
                    assignment: invoice.assignment,
                    transport_endpoints: invoice.transport_endpoints,
                    witness_data: Some(WitnessData {
                        amount_sat: 5000,
                        blinding: None,
                    }),
                };
                let prepared = wallet.send_begin(
                    online,
                    HashMap::from([(asset, vec![recipient])]),
                    true,
                    2,
                    1,
                    invoice.expiration_timestamp,
                    false,
                )?;
                fs::write(output.join("unsigned.psbt.txt"), &prepared.psbt)?;
                let signing_dir = output.join("offline-signer");
                fs::create_dir(&signing_dir)?;
                let mut signing_data = data;
                signing_data.data_dir = signing_dir.to_string_lossy().into();
                let mut signing_keys = keys;
                signing_keys.mnemonic = Some(field("mnemonic")?);
                let signer = Wallet::new(signing_data, signing_keys)?;
                let signed = signer.sign_psbt(prepared.psbt, None)?;
                fs::write(output.join("signed.psbt.txt"), signed)?;
                json!({"prepared":true,"batch_transfer_idx":prepared.batch_transfer_idx,"details":prepared.details})
            }
            "complete-send" => {
                let signed = fs::read_to_string(
                    spec["signed_psbt_path"]
                        .as_str()
                        .ok_or("signed_psbt_path required")?,
                )?;
                serde_json::to_value(wallet.send_end(online, signed)?)?
            }
            _ => return Err("unknown operation".into()),
        };
        fs::write(
            output.join("result.json"),
            serde_json::to_vec_pretty(&result)?,
        )?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }
    let refreshed = wallet.refresh(online, None, vec![], false)?;
    let assets = serde_json::to_value(wallet.list_assets(vec![])?)?;
    let mut transfers = wallet.list_transfers(None)?;
    for list in assets.as_object().ok_or("invalid assets result")?.values() {
        if let Some(list) = list.as_array() {
            for asset in list {
                if let Some(id) = asset["asset_id"].as_str() {
                    transfers.extend(wallet.list_transfers(Some(id.into()))?);
                }
            }
        }
    }
    let btc = wallet.get_btc_balance(Some(online), true)?;
    let utxos = wallet.list_unspents(Some(online), false, true)?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"refresh":refreshed,"assets":assets,"transfers":transfers,"btc":btc,"utxos":utxos})
        )?
    );
    Ok(())
}
