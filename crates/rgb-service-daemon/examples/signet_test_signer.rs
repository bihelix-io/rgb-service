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
        "rna-request" | "test-payment" | "sign-test-psbt" | "external-request"
        | "assets-request" => PrivateKey::from_wif(std::fs::read_to_string(&path)?.trim())?,
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
    if mode == "assets-request" {
        let payload = rgb_service_api::ListAssetsRequest {
            account_id: address.clone(),
        };
        let signature = sign_payload(&key, "read_assets", &payload)?;
        println!(
            "{}",
            serde_json::to_string(&rgb_service_api::SignedRequest { payload, signature })?
        );
        return Ok(());
    }
    if mode == "external-request" {
        use rgb_service_api::{
            AssetSpendAuthorization, AssetSpendPurpose, ExternalRgbAction, ExternalRgbRequest,
        };
        #[derive(serde::Serialize)]
        struct Unsigned<'a> {
            asset_id: &'a str,
            amount: u64,
            purpose: &'a AssetSpendPurpose,
            recipient: &'a Option<String>,
            anchor_psbt: &'a Option<String>,
            expires_at_ms: u64,
        }
        let file = args.next().context("request payload file required")?;
        let mut payload: ExternalRgbRequest = serde_json::from_slice(&std::fs::read(file)?)?;
        anyhow::ensure!(
            payload.account_id == address,
            "request account differs from test key"
        );
        let sign_spend = |a: &mut AssetSpendAuthorization| -> Result<()> {
            a.signature = sign_payload(
                &key,
                "asset_spend",
                &Unsigned {
                    asset_id: &a.asset_id,
                    amount: a.amount,
                    purpose: &a.purpose,
                    recipient: &a.recipient,
                    anchor_psbt: &a.anchor_psbt,
                    expires_at_ms: a.expires_at_ms,
                },
            )?;
            Ok(())
        };
        match &mut payload.action {
            ExternalRgbAction::Prepare {
                asset_authorization,
                ..
            }
            | ExternalRgbAction::Finalize {
                asset_authorization,
                ..
            } => sign_spend(asset_authorization)?,
            _ => {}
        }
        let signature = sign_payload(&key, "external_rgb", &payload)?;
        println!(
            "{}",
            serde_json::to_string(&rgb_service_api::SignedRequest { payload, signature })?
        );
        return Ok(());
    }
    if mode == "sign-test-psbt" {
        use bitcoin::sighash::{EcdsaSighashType, SighashCache};
        let file = args.next().context("PSBT file required")?;
        let psbt = bitcoin::Psbt::deserialize(&std::fs::read(file)?)?;
        let own_script = Address::p2wpkh(&CompressedPublicKey(public_key.inner), Network::Signet)
            .script_pubkey();
        let mut tx = psbt.unsigned_tx.clone();
        let mut total = 0u64;
        for (i, input) in psbt.inputs.iter().enumerate() {
            let prev = input
                .witness_utxo
                .as_ref()
                .context("witness UTXO required")?;
            anyhow::ensure!(
                prev.script_pubkey == own_script,
                "input not owned by test key"
            );
            total = total.checked_add(prev.value.to_sat()).context("overflow")?;
            let hash = SighashCache::new(&psbt.unsigned_tx).p2wpkh_signature_hash(
                i,
                &own_script,
                prev.value,
                EcdsaSighashType::All,
            )?;
            let signature = bitcoin::ecdsa::Signature::sighash_all(
                secp.sign_ecdsa(&Message::from_digest(hash.to_byte_array()), &key.inner),
            );
            tx.input[i].witness = bitcoin::Witness::p2wpkh(&signature, &public_key.inner);
        }
        let spent = tx
            .output
            .iter()
            .try_fold(0u64, |sum, o| sum.checked_add(o.value.to_sat()))
            .context("output overflow")?;
        let fee = total.checked_sub(spent).context("negative fee")?;
        anyhow::ensure!(fee > 0 && fee <= 2000, "test fee exceeds limit");
        let mut finalized = psbt.clone();
        for (input, signed) in finalized.inputs.iter_mut().zip(&tx.input) {
            input.final_script_witness = Some(signed.witness.clone());
        }
        let signed_psbt = finalized
            .serialize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        println!(
            "{}",
            json!({"txid":tx.compute_txid().to_string(),"hex":bitcoin::consensus::encode::serialize_hex(&tx),"fee":fee,"signed_psbt":signed_psbt})
        );
        return Ok(());
    }
    if mode == "test-payment" {
        use bitcoin::sighash::{EcdsaSighashType, SighashCache};
        use bitcoin::{
            absolute, transaction, Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut,
            Witness,
        };
        use std::str::FromStr;
        let outpoint = OutPoint::from_str(&args.next().context("outpoint required")?)?;
        let input_value: u64 = args.next().context("input sats required")?.parse()?;
        let destination = Address::from_str(&args.next().context("destination required")?)?
            .require_network(Network::Signet)?;
        let amount: u64 = args.next().context("amount sats required")?.parse()?;
        let fee: u64 = args.next().context("fee sats required")?.parse()?;
        anyhow::ensure!(
            amount >= 546 && fee > 0 && fee <= 2000,
            "invalid test payment amount/fee"
        );
        let change = input_value
            .checked_sub(amount)
            .and_then(|v| v.checked_sub(fee))
            .context("insufficient input")?;
        anyhow::ensure!(change >= 546, "change below test dust limit");
        let own = Address::p2wpkh(&CompressedPublicKey(public_key.inner), Network::Signet);
        let mut tx = Transaction {
            version: transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: outpoint,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![
                TxOut {
                    value: Amount::from_sat(amount),
                    script_pubkey: destination.script_pubkey(),
                },
                TxOut {
                    value: Amount::from_sat(change),
                    script_pubkey: own.script_pubkey(),
                },
            ],
        };
        let hash = SighashCache::new(&tx).p2wpkh_signature_hash(
            0,
            &own.script_pubkey(),
            Amount::from_sat(input_value),
            EcdsaSighashType::All,
        )?;
        let signature = bitcoin::ecdsa::Signature::sighash_all(
            secp.sign_ecdsa(&Message::from_digest(hash.to_byte_array()), &key.inner),
        );
        tx.input[0].witness = Witness::p2wpkh(&signature, &public_key.inner);
        println!(
            "{}",
            json!({"txid":tx.compute_txid().to_string(),"hex":bitcoin::consensus::encode::serialize_hex(&tx),"destination":destination.to_string(),"amount":amount,"fee":fee,"change":change})
        );
        return Ok(());
    }
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

fn sign_payload<T: serde::Serialize>(
    key: &PrivateKey,
    purpose: &str,
    payload: &T,
) -> Result<rgb_service_api::RequestSignature> {
    let secp = Secp256k1::new();
    let public = key.public_key(&secp);
    let time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    let timestamp_ms = time.as_millis() as u64;
    let nonce = format!("external-test-{}-{}", std::process::id(), time.as_nanos());
    let mut engine = sha256::Hash::engine();
    engine.input(b"bihelix-ln-rgb-auth-v1");
    engine.input(purpose.as_bytes());
    engine.input(nonce.as_bytes());
    engine.input(&timestamp_ms.to_be_bytes());
    engine.input(&serde_json::to_vec(payload)?);
    let signature = secp.sign_ecdsa(
        &Message::from_digest(sha256::Hash::from_engine(engine).to_byte_array()),
        &key.inner,
    );
    Ok(rgb_service_api::RequestSignature {
        signer_id: Address::p2wpkh(&CompressedPublicKey(public.inner), Network::Signet).to_string(),
        public_key: public.to_string(),
        scheme: rgb_service_api::SignatureScheme::Ecdsa,
        nonce,
        timestamp_ms,
        signature: signature.to_string(),
    })
}
