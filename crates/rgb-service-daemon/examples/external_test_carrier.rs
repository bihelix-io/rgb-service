//! Build an unsigned test-only BTC carrier; RGB commitment is added by daemon HTTP prepare.
use anyhow::{Context, Result};
use bitcoin::{
    Address, Amount, Network, OutPoint, Psbt, ScriptBuf, Sequence, Transaction, TxIn, TxOut,
    Witness,
};
use rgb_service_local::rgbstd::{
    invoice::{Beneficiary, InvoiceState, RgbInvoice},
    ChainNet,
};
use serde_json::{json, Value};
use std::str::FromStr;
fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("carrier specification JSON required")?;
    let spec: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let text = |key: &str| {
        spec[key]
            .as_str()
            .with_context(|| format!("{key} required"))
    };
    let address = Address::from_str(text("account_id")?)?.require_network(Network::Signet)?;
    let invoice = RgbInvoice::from_str(text("invoice")?)?;
    anyhow::ensure!(
        invoice.chain_network() == ChainNet::BitcoinSignet,
        "test signet invoice required"
    );
    let contract = invoice.contract.context("contract required")?;
    let amount = match invoice.assignment_state {
        Some(InvoiceState::Amount(a)) => a.value(),
        _ => anyhow::bail!("fungible amount required"),
    };
    let fee = spec["fee"].as_u64().unwrap_or(500);
    anyhow::ensure!(fee > 0 && fee <= 2000, "test fee limit");
    let mut total = 0u64;
    let mut inputs = vec![];
    let mut prevouts = vec![];
    for input in spec["inputs"].as_array().context("inputs required")? {
        let outpoint =
            OutPoint::from_str(input["outpoint"].as_str().context("outpoint required")?)?;
        let sats = input["sats"].as_u64().context("sats required")?;
        total = total.checked_add(sats).context("input overflow")?;
        inputs.push(TxIn {
            previous_output: outpoint,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        });
        prevouts.push(TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: address.script_pubkey(),
        });
    }
    let mut outputs = vec![TxOut {
        value: Amount::ZERO,
        script_pubkey: ScriptBuf::new_op_return([]),
    }];
    let recipient_vout = match invoice.beneficiary.clone().into_inner() {
        Beneficiary::WitnessVout(v, None) => {
            let sats = spec["recipient_sats"].as_u64().unwrap_or(2000);
            anyhow::ensure!(sats >= 546, "recipient dust");
            outputs.push(TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: v.to_script(),
            });
            Some(1)
        }
        Beneficiary::BlindedSeal(_) => None,
        _ => anyhow::bail!("unsupported test beneficiary"),
    };
    let change_vout = outputs.len() as u32;
    let outgoing = outputs.iter().map(|o| o.value.to_sat()).sum::<u64>();
    let change = total
        .checked_sub(outgoing)
        .and_then(|n| n.checked_sub(fee))
        .context("insufficient BTC")?;
    anyhow::ensure!(change >= 546, "change dust");
    outputs.push(TxOut {
        value: Amount::from_sat(change),
        script_pubkey: address.script_pubkey(),
    });
    let mut psbt = Psbt::from_unsigned_tx(Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: inputs,
        output: outputs,
    })?;
    for (input, prev) in psbt.inputs.iter_mut().zip(prevouts) {
        input.witness_utxo = Some(prev);
    }
    let encoded = psbt
        .serialize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    println!(
        "{}",
        json!({"invoice":invoice.to_string(),"asset_id":contract.to_string(),"amount":amount,"unsigned_anchor_psbt":encoded,"recipient_vout":recipient_vout,"change_vout":change_vout,"fee":fee})
    );
    Ok(())
}
