//! Dedicated single-key integration signer, never use for mainnet funds.
//! Stores a test WIF privately; stdout contains public data or a signed request only.
use anyhow::{bail, Context, Result};
use bitcoin::{
    hashes::{sha256, Hash, HashEngine},
    secp256k1::{Message, Secp256k1, SecretKey},
    Address, CompressedPublicKey, Network, PrivateKey,
};
use serde_json::json;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .context("usage: signet_test_signer init|rna-request <private-wif-path>")?;
    let path = PathBuf::from(args.next().context("private WIF path required")?);
    let key = match mode.as_str() {
        "init" => {
            let mut entropy = [0u8; 32];
            std::fs::File::open("/dev/urandom")?.read_exact(&mut entropy)?;
            let key = PrivateKey::new(SecretKey::from_slice(&entropy)?, Network::Signet);
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            writeln!(file, "{}", key.to_wif())?;
            file.sync_all()?;
            key
        }
        "rna-request" => PrivateKey::from_wif(std::fs::read_to_string(&path)?.trim())?,
        _ => bail!("unsupported mode"),
    };
    anyhow::ensure!(
        key.network == bitcoin::NetworkKind::Test,
        "test key required"
    );
    let secp = Secp256k1::new();
    let public_key = key.public_key(&secp);
    let address =
        Address::p2wpkh(&CompressedPublicKey(public_key.inner), Network::Signet).to_string();
    if mode == "init" {
        println!(
            "{}",
            json!({"address": address, "public_key": public_key.to_string(), "network": "utexo-signet", "purpose": "integration-only"})
        );
        return Ok(());
    }
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    let nonce = format!("signet-smoke-{}-{timestamp}", std::process::id());
    let payload = json!({"account_id": address});
    let mut engine = sha256::Hash::engine();
    engine.input(b"bihelix-ln-rgb-auth-v1");
    engine.input(b"read_rna_balance");
    engine.input(nonce.as_bytes());
    engine.input(&timestamp.to_be_bytes());
    engine.input(&serde_json::to_vec(&payload)?);
    let signature = secp.sign_ecdsa(
        &Message::from_digest(sha256::Hash::from_engine(engine).to_byte_array()),
        &key.inner,
    );
    println!(
        "{}",
        json!({"payload": payload, "signature": {
            "signer_id": address, "public_key": public_key.to_string(), "scheme": "ecdsa",
            "nonce": nonce, "timestamp_ms": timestamp, "signature": signature.to_string()
        }})
    );
    Ok(())
}
