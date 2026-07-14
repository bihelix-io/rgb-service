// Reproduce electrum sync errors.
use bdk_electrum::electrum_client::{self, ElectrumApi};
use bdk_electrum::BdkElectrumClient;
use bdk_wallet::file_store;
use bdk_wallet::Wallet;

fn main() {
    let url = "ssl://electrs-mainnet.bihelix.inner:50001";
    println!("connecting to {}", url);
    let client = match electrum_client::Client::new(url) {
        Ok(client) => client,
        Err(err) => {
            println!("client error: {}", err);
            return;
        }
    };
    println!(
        "connected, tip: {:?}",
        client.block_headers_subscribe().map(|header| header.height)
    );

    // Use an account with transaction history to test sync.
    let wallet_path = "/wallet-v2-from-eks/5b74ffbe_hd027wws/bdk_wallet";
    let (mut db, _) =
        match file_store::Store::<bdk_wallet::ChangeSet>::load_or_create(b"RgbWallet", wallet_path)
        {
            Ok(store) => store,
            Err(err) => {
                println!("store error: {}", err);
                return;
            }
        };
    let wallet = match Wallet::load()
        .check_network(bitcoin::Network::Bitcoin)
        .load_wallet(&mut db)
    {
        Ok(Some(wallet)) => wallet,
        Ok(None) => {
            println!("no wallet");
            return;
        }
        Err(err) => {
            println!("load error: {}", err);
            return;
        }
    };
    println!(
        "wallet loaded, spks: {:?}",
        wallet.derivation_index(bdk_wallet::KeychainKind::External)
    );

    let client = BdkElectrumClient::new(client);
    let request = wallet.start_sync_with_revealed_spks().build();
    println!("syncing with fetch_prev_txouts=true...");
    match client.sync(request, 10, true) {
        Ok(_) => println!("sync OK!"),
        Err(err) => println!("sync FAILED: {} (type: {:?})", err, err),
    }
}
