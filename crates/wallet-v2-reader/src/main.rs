// Reads a legacy wallet-service-v2 BDK wallet file (written by bdk_wallet 1.1.0
// / bdk_file_store 0.18.1) and emits the recovered descriptor plus unspent
// UTXOs as JSON. bdk_wallet 2.x cannot read these files due to an incompatible
// file format, so this binary pins bdk_wallet 1.1.0 to decode them.
//
// Usage: wallet-v2-reader <bdk_wallet-file> <network> [--reveal-count N]
//   network: bitcoin | testnet | testnet4 | signet | regtest
// Prints one JSON object to stdout.

use std::env;
use std::process::ExitCode;

use bdk_wallet::file_store;
use bdk_wallet::{KeychainKind, Wallet};
use bitcoin::{Address, Network};
use serde::Serialize;

const MAGIC: &[u8] = b"RgbWallet";

#[derive(Serialize)]
struct Output {
    descriptor: String,
    network: String,
    utxos: Vec<Utxo>,
    addresses: Vec<String>,
}

#[derive(Serialize)]
struct Utxo {
    outpoint: String,
    address: Option<String>,
    value_sats: u64,
    is_confirmed: bool,
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "usage: wallet-v2-reader <bdk_wallet-file> <network> [--reveal-count N]"
        );
        return ExitCode::from(2);
    }
    let wallet_path = &args[1];
    let network = match args[2].as_str() {
        "bitcoin" | "mainnet" => Network::Bitcoin,
        "testnet" | "testnet3" => Network::Testnet,
        "testnet4" => Network::Testnet4,
        "signet" => Network::Signet,
        "regtest" => Network::Regtest,
        other => {
            eprintln!("unknown network: {other}");
            return ExitCode::from(2);
        }
    };

    let mut db = match file_store::Store::open_or_create_new(MAGIC, wallet_path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("open store failed: {err}");
            return ExitCode::FAILURE;
        }
    };
    // Recover the wallet straight from the persisted ChangeSet. The descriptor
    // is stored in the changeset, so no explicit `.descriptor()` is needed.
    let wallet = match Wallet::load()
        .check_network(network)
        .load_wallet(&mut db)
    {
        Ok(Some(w)) => w,
        Ok(None) => {
            eprintln!("wallet not found in file");
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("load wallet failed: {err}");
            return ExitCode::FAILURE;
        }
    };

    let descriptor = wallet
        .public_descriptor(KeychainKind::External)
        .to_string();

    // Derive the requested count of addresses read-only (`peek_address` does
    // not advance the wallet's reveal index) so callers can build the
    // address -> account index without depending on the wallet's persisted
    // reveal state.
    let mut addresses = Vec::new();
    if let Some(count) = reveal_count(&args[3..]) {
        for index in 0..count {
            addresses.push(wallet.peek_address(KeychainKind::External, index).address.to_string());
        }
    }

    let utxos = wallet
        .list_unspent()
        .map(|utxo| Utxo {
            outpoint: utxo.outpoint.to_string(),
            address: Address::from_script(&utxo.txout.script_pubkey, network)
                .ok()
                .map(|a| a.to_string()),
            value_sats: utxo.txout.value.to_sat(),
            is_confirmed: utxo.chain_position.is_confirmed(),
        })
        .collect::<Vec<_>>();

    let out = Output {
        descriptor,
        network: network.to_string(),
        utxos,
        addresses,
    };
    match serde_json::to_string(&out) {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("serialize failed: {err}");
            ExitCode::FAILURE
        }
    }
}

fn reveal_count(args: &[String]) -> Option<u32> {
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        if a == "--reveal-count" {
            if let Some(v) = iter.next() {
                return v.parse().ok();
            }
        }
    }
    None
}
