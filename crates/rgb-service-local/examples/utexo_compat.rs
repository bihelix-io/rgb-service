//! Isolated compatibility harness against the exact BiHelix RGB core.
//! Never point the stock argument at a running daemon's database.
use anyhow::{Context, Result};
use rgb_service_local::{
    decode_rgb20_transfer_consignment, validate_rgb20_transfer_with_chain_source, ChainSource,
    EsploraConfig,
};
use rgbstd::containers::{ConsignmentExt, Contract, FileContent};
use rgbstd::invoice::{
    AddressPayload, Beneficiary, Pay2Vout, RgbInvoice, RgbInvoiceBuilder, XChainNet,
};
use serde_json::json;
use std::{path::PathBuf, str::FromStr};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .context("invoice|consignment <file> [esplora URL]")?;
    let path = PathBuf::from(args.next().context("input file required")?);
    if mode == "make-invoice" {
        let address = bitcoin::Address::from_str(&args.next().context("address required")?)?
            .require_network(bitcoin::Network::Signet)?;
        let contract = rgbstd::ContractId::from_str(&args.next().context("contract required")?)?;
        let amount: u64 = args.next().context("amount required")?.parse()?;
        let beneficiary = Beneficiary::WitnessVout(
            Pay2Vout::new(AddressPayload::from_script(&address.script_pubkey())?),
            None,
        );
        let invoice = RgbInvoiceBuilder::with(contract, XChainNet::BitcoinSignet(beneficiary))
            .set_amount_raw(amount)
            .set_assignment_name("assetOwner")
            .set_expiry_timestamp(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs() as i64
                    + 86400,
            )
            .add_transport("rpcs://rgb-proxy.utexo.com/json-rpc")
            .map_err(|(_, e)| anyhow::anyhow!("{e}"))?
            .finish();
        std::fs::write(path, invoice.to_string())?;
        println!(
            "{}",
            json!({"invoice":invoice.to_string(),"beneficiary":invoice.beneficiary.to_string()})
        );
    } else if mode == "invoice" {
        let text = std::fs::read_to_string(path)?;
        let invoice = RgbInvoice::from_str(text.trim()).context("invoice decode failed")?;
        println!(
            "{}",
            json!({"invoice":invoice.to_string(),"decoded":format!("{invoice:?}")})
        );
    } else if mode == "accept" {
        let stock = PathBuf::from(args.next().context("isolated stock directory required")?);
        let outpoint =
            bitcoin::OutPoint::from_str(&args.next().context("recipient outpoint required")?)?;
        let transfer = decode_rgb20_transfer_consignment(&std::fs::read(path)?)?;
        let contract_id = transfer.contract_id();
        let source =
            ChainSource::Esplora(EsploraConfig::new("https://esplora-api.utexo.com".into()));
        rgb_service_local::accept_rgb20_transfer_with_chain_source(
            &stock,
            bitcoin::Network::Signet,
            &source,
            transfer,
        )?;
        let allocations = rgb_service_local::list_rgb20_assets_for_utxos(
            &stock,
            [rgb_service_local::Rgb20TrackedUtxo {
                outpoint,
                address: None,
                confirmed: true,
            }],
        )?;
        let amount: u64 = allocations
            .iter()
            .filter(|a| a.contract_id == contract_id)
            .map(|a| a.amount_raw)
            .sum();
        println!(
            "{}",
            json!({"contract_id":contract_id.to_string(),"outpoint":outpoint.to_string(),"amount":amount})
        );
    } else if mode == "prepare-return" {
        let spec: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let text = |key: &str| spec[key].as_str().with_context(|| format!("missing {key}"));
        let stock = PathBuf::from(text("stock")?);
        let invoice = RgbInvoice::from_str(text("invoice")?)?;
        anyhow::ensure!(
            invoice.chain_network() == rgbstd::ChainNet::BitcoinSignet,
            "wrong invoice network"
        );
        let contract = invoice.contract.context("contract required")?;
        let amount = match invoice.assignment_state {
            Some(rgbstd::invoice::InvoiceState::Amount(a)) => a.value(),
            _ => anyhow::bail!("fungible amount required"),
        };
        let script = match invoice.beneficiary.into_inner() {
            Beneficiary::WitnessVout(v, None) => v.to_script(),
            _ => anyhow::bail!("test return requires plain witness invoice"),
        };
        let change = bitcoin::Address::from_str(text("change_address")?)?
            .require_network(bitcoin::Network::Signet)?;
        let input = spec["inputs"].as_array().context("inputs required")?;
        let mut total = 0u64;
        let mut inputs = Vec::new();
        let mut prevouts = Vec::new();
        for i in input {
            let outpoint =
                bitcoin::OutPoint::from_str(i["outpoint"].as_str().context("outpoint required")?)?;
            let value = i["sats"].as_u64().context("sats required")?;
            total = total.checked_add(value).context("input overflow")?;
            inputs.push(bitcoin::TxIn {
                previous_output: outpoint,
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            });
            prevouts.push(bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(value),
                script_pubkey: change.script_pubkey(),
            });
        }
        let change_value = total.checked_sub(2500).context("insufficient input")?;
        anyhow::ensure!(change_value >= 546, "dust change");
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: inputs,
            output: vec![
                bitcoin::TxOut {
                    value: bitcoin::Amount::ZERO,
                    script_pubkey: bitcoin::ScriptBuf::new_op_return([]),
                },
                bitcoin::TxOut {
                    value: bitcoin::Amount::from_sat(2000),
                    script_pubkey: script,
                },
                bitcoin::TxOut {
                    value: bitcoin::Amount::from_sat(change_value),
                    script_pubkey: change.script_pubkey(),
                },
            ],
        };
        let mut psbt = bitcoin::Psbt::from_unsigned_tx(tx)?;
        for (i, prevout) in prevouts.into_iter().enumerate() {
            psbt.inputs[i].witness_utxo = Some(prevout);
        }
        let prepared = rgb_service_local::prepare_rgb20_external_psbt(
            &stock,
            psbt,
            2,
            [rgb_service_local::Rgb20PsbtAssignment {
                contract_id: contract,
                amount,
                vout: 1,
            }],
        )?;
        let txid = prepared.psbt.unsigned_tx.compute_txid();
        let transfer = rgb_service_local::build_rgb20_transfer_consignment(
            &stock,
            prepared.fascia.clone(),
            contract,
            txid,
            1,
        )?;
        let output = PathBuf::from(text("output_dir")?);
        std::fs::create_dir(&output).context("output directory must be new")?;
        std::fs::write(output.join("unsigned.psbt"), prepared.psbt.serialize())?;
        std::fs::write(
            output.join("fascia.bin"),
            rgb_service_local::encode_fascia_bytes(&prepared.fascia)?,
        )?;
        std::fs::write(
            output.join("return.consignment"),
            rgb_service_local::encode_rgb20_transfer_consignment(&transfer)?,
        )?;
        println!(
            "{}",
            json!({"txid":txid.to_string(),"amount":amount,"recipient_vout":1,"change_vout":2,"fee":500})
        );
    } else if mode == "settle-return" {
        let stock = PathBuf::from(args.next().context("stock required")?);
        let txid = bitcoin::Txid::from_str(&args.next().context("txid required")?)?;
        let fascia = rgb_service_local::decode_fascia_bytes(&std::fs::read(path)?)?;
        rgb_service_local::stage_sender_fascia(&stock, txid, &fascia)?;
        let report = rgb_service_local::scan_and_promote_confirmed_staged_rgb_stocks(
            &stock,
            bitcoin::Network::Signet,
            &["https://esplora-api.utexo.com".into()],
        )?;
        let assets = rgb_service_local::list_rgb20_assets_for_utxos(
            &stock,
            [rgb_service_local::Rgb20TrackedUtxo {
                outpoint: bitcoin::OutPoint { txid, vout: 2 },
                address: None,
                confirmed: true,
            }],
        )?;
        println!(
            "{}",
            json!({"promoted":report.promoted,"amounts":assets.iter().map(|a|a.amount_raw).collect::<Vec<_>>()})
        );
    } else if mode == "contract" {
        let bytes = std::fs::read(path)?;
        let contract = Contract::load(bytes.as_slice()).context("contract decode failed")?;
        println!(
            "{}",
            json!({"decoded":true,"contract_id":contract.contract_id().to_string(),"schema_id":contract.schema_id().to_string()})
        );
    } else if mode == "consignment" {
        let bytes = std::fs::read(path)?;
        let transfer = decode_rgb20_transfer_consignment(&bytes)?;
        println!(
            "{}",
            json!({"decoded":true,"schema_id":transfer.schema_id().to_string(),"consignment_id":transfer.consignment_id().to_string(),"bundles":transfer.bundles.len()})
        );
        if let Some(url) = args.next() {
            let source = ChainSource::Esplora(EsploraConfig::new(url));
            validate_rgb20_transfer_with_chain_source(bitcoin::Network::Signet, &source, transfer)?;
            println!("{}", json!({"validated":true}));
        }
    } else {
        anyhow::bail!("unknown mode");
    }
    Ok(())
}
