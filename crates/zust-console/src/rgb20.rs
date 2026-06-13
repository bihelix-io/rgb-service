use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    hash::{Hash, Hasher},
    num::NonZeroU32,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use amplify::confinement::{Confined, U32 as U32MAX};
use anyhow::{Context, Result};
use bdk_bitcoind_rpc::bitcoincore_rpc::RpcApi;
use bitcoin::{
    absolute::LockTime, constants::ChainHash, Address, Amount, FeeRate, Network, OutPoint, Psbt,
    ScriptBuf, Transaction, Txid,
};
use nonasync::persistence::CloneNoPersistence;
use psrgbt::{RgbOutExt, RgbPsbtExt};
use rgb_schemata::NonInflatableAsset;
use rgbstd::txout::CloseMethod;
use rgbstd::{
    containers::{
        BuilderSeal, Consignment, ConsignmentExt, Fascia, FileContent, Transfer, ValidTransfer,
    },
    contract::{AllocatedState, ContractBuilder, IssuerWrapper},
    indexers::{esplora_blocking::esplora_client, AnyResolver},
    persistence::{StashReadProvider, Stock},
    stl::{AssetSpec, ContractTerms, Name, Ticker},
    validation::{
        ResolveWitness, ValidationConfig, WitnessOrdProvider, WitnessResolverError, WitnessStatus,
    },
    vm::{WitnessOrd, WitnessPos},
    ContractId, GenesisSeal, Identity, Operation, Opout, OutputSeal, Transition, Txid as RgbTxid,
};
use serde::{Deserialize, Serialize};
use strict_types::{StrictDeserialize, StrictSerialize};

use crate::local_wallet::{
    bitcoin_core_tx_status, broadcast_transaction, esplora_client, esplora_client_with_config,
    sync_wallet_with_chain_source, BitcoinCoreConfig, ChainSource, EsploraConfig, LocalWallet,
};
use crate::node_store::{default_store_dir, LocalNodeStore};

pub struct Rgb20IssueRequest {
    pub ticker: String,
    pub name: String,
    pub amount: u64,
    pub precision: u8,
    pub utxo: OutPoint,
}

pub struct Rgb20IssueResult {
    pub contract_id: rgbstd::ContractId,
    pub utxo: OutPoint,
    pub stock_dir: String,
}

#[derive(Clone, Debug)]
pub struct Rgb20TrackedUtxo {
    pub outpoint: OutPoint,
    pub address: Option<String>,
    pub confirmed: bool,
}

#[derive(Clone, Debug)]
pub struct Rgb20AssetAllocation {
    pub contract_id: rgbstd::ContractId,
    pub outpoint: OutPoint,
    pub address: Option<String>,
    pub amount: rgbstd::Amount,
    pub amount_raw: u64,
    pub amount_display: String,
    pub amount_encoding: String,
    pub ticker: String,
    pub name: String,
    pub precision: u8,
    pub witness: Option<rgbstd::Txid>,
    pub confirmed: bool,
}

#[derive(Clone, Debug)]
pub struct Rgb20TransferRequest {
    pub contract_id: ContractId,
    pub recipient_address: String,
    pub amount: u64,
    pub recipient_sats: u64,
    pub fee_rate_sat_vb: u64,
}

pub struct Rgb20TransferResult {
    pub txid: Txid,
    pub transaction: Transaction,
    pub fascia: Fascia,
    pub consignment: Transfer,
    pub recipient_outpoint: OutPoint,
    pub staged_stock_dir: Option<PathBuf>,
}

pub fn default_rgb_stock_dir(data_dir: &Path) -> std::path::PathBuf {
    default_store_dir(data_dir)
}

#[derive(Clone, Debug)]
pub struct Rgb20ChannelFundingRequest {
    pub contract_id: ContractId,
    pub rgb_amount: u64,
    pub channel_value_satoshis: u64,
    pub funding_script_pubkey: ScriptBuf,
    pub fee_rate_sat_vb: u64,
}

pub struct Rgb20ChannelFundingResult {
    pub txid: Txid,
    pub transaction: Transaction,
    pub fascia: Fascia,
    pub transfer: ValidTransfer,
    pub funding_outpoint: OutPoint,
    pub staged_stock_dir: PathBuf,
}

#[derive(Clone, Debug, Default)]
pub struct RgbPendingStockScanReport {
    pub scanned: usize,
    pub promoted: usize,
    pub revoked: usize,
    pub pending: usize,
    pub skipped: usize,
    pub promoted_txids: Vec<Txid>,
    pub revoked_txids: Vec<Txid>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RgbPendingStockStatus {
    pub txid: String,
    pub status: String,
    pub main_stock_dir: String,
    pub staged_stock_dir: String,
    pub updated_at: u64,
    #[serde(default)]
    pub confirmed_at: Option<u64>,
    #[serde(default)]
    pub promoted_at: Option<u64>,
    #[serde(default)]
    pub revoked_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RgbPendingOperation {
    SenderFascia {
        txid: String,
        fascia: Vec<u8>,
    },
    ReceiverTransfer {
        txid: String,
        consignment: Vec<u8>,
    },
    RecolorTxs {
        txids: Vec<String>,
        retrospective: bool,
    },
}

#[derive(Clone, Copy, Debug)]
struct TentativeWitnessOrd;

impl WitnessOrdProvider for TentativeWitnessOrd {
    fn witness_ord(&self, _witness_id: rgbstd::Txid) -> Result<WitnessOrd, WitnessResolverError> {
        Ok(WitnessOrd::Tentative)
    }
}

struct LocalWitnessResolver {
    inner: Box<dyn ResolveWitness + Send>,
    local_txs: HashMap<RgbTxid, Transaction>,
}

impl LocalWitnessResolver {
    fn new(
        inner: impl ResolveWitness + Send + 'static,
        local_txs: impl IntoIterator<Item = Transaction>,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            local_txs: local_txs
                .into_iter()
                .map(|tx| (txid_to_rgb(tx.compute_txid()), tx))
                .collect(),
        }
    }
}

impl ResolveWitness for LocalWitnessResolver {
    fn resolve_witness(&self, witness_id: RgbTxid) -> Result<WitnessStatus, WitnessResolverError> {
        if let Some(tx) = self.local_txs.get(&witness_id) {
            return Ok(WitnessStatus::Resolved(tx.clone(), WitnessOrd::Tentative));
        }
        self.inner.resolve_witness(witness_id)
    }

    fn check_chain_net(&self, chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        self.inner.check_chain_net(chain_net)
    }
}

struct BitcoinCoreWitnessResolver {
    config: BitcoinCoreConfig,
    known_txs: HashMap<RgbTxid, Transaction>,
}

impl BitcoinCoreWitnessResolver {
    fn new(config: BitcoinCoreConfig) -> Self {
        Self {
            config,
            known_txs: HashMap::new(),
        }
    }

    fn add_consignment_txes<const TYPE: bool>(&mut self, consignment: &Consignment<TYPE>) {
        self.known_txs
            .extend(consignment_txs(consignment).map(|tx| (txid_to_rgb(tx.compute_txid()), tx)));
    }
}

impl ResolveWitness for BitcoinCoreWitnessResolver {
    fn resolve_witness(&self, witness_id: RgbTxid) -> Result<WitnessStatus, WitnessResolverError> {
        let txid = Txid::from_str(&witness_id.to_string()).map_err(|err| {
            WitnessResolverError::ResolverIssue(Some(witness_id), err.to_string())
        })?;
        match bitcoin_core_tx_status(&self.config, txid) {
            Ok(Some(status)) => {
                let ord = if status.confirmed {
                    let height = status
                        .height
                        .and_then(NonZeroU32::new)
                        .ok_or(WitnessResolverError::InvalidResolverData)?;
                    let block_time = status
                        .block_time
                        .ok_or(WitnessResolverError::InvalidResolverData)?;
                    WitnessOrd::Mined(
                        WitnessPos::bitcoin(height, block_time as i64)
                            .ok_or(WitnessResolverError::InvalidResolverData)?,
                    )
                } else {
                    WitnessOrd::Tentative
                };
                Ok(WitnessStatus::Resolved(status.tx, ord))
            }
            Ok(None) => Ok(self
                .known_txs
                .get(&witness_id)
                .cloned()
                .map(|tx| WitnessStatus::Resolved(tx, WitnessOrd::Tentative))
                .unwrap_or(WitnessStatus::Unresolved)),
            Err(err) => self
                .known_txs
                .get(&witness_id)
                .cloned()
                .map(|tx| WitnessStatus::Resolved(tx, WitnessOrd::Tentative))
                .ok_or(WitnessResolverError::ResolverIssue(
                    Some(witness_id),
                    err.to_string(),
                )),
        }
    }

    fn check_chain_net(&self, chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        let client = self
            .config
            .client()
            .map_err(|err| WitnessResolverError::ResolverIssue(None, err.to_string()))?;
        let block_hash = client
            .get_block_hash(0)
            .map_err(|err| WitnessResolverError::ResolverIssue(None, err.to_string()))?;
        if chain_net.chain_hash() != ChainHash::from_genesis_block_hash(block_hash) {
            return Err(WitnessResolverError::WrongChainNet);
        }
        Ok(())
    }
}

pub fn issue_rgb20_fixed(
    stock_dir: &Path,
    network: Network,
    esplora_url: &str,
    request: Rgb20IssueRequest,
) -> Result<Rgb20IssueResult> {
    let chain_source = chain_source_from_esplora_arg(network, esplora_url)?;
    issue_rgb20_fixed_with_chain_source(stock_dir, network, &chain_source, request)
}

pub fn issue_rgb20_fixed_with_chain_source(
    stock_dir: &Path,
    network: Network,
    chain_source: &ChainSource,
    request: Rgb20IssueRequest,
) -> Result<Rgb20IssueResult> {
    let ticker: Ticker = request
        .ticker
        .try_into()
        .map_err(|err| anyhow::anyhow!("invalid RGB ticker: {err:?}"))?;
    let name: Name = request
        .name
        .try_into()
        .map_err(|err| anyhow::anyhow!("invalid RGB asset name: {err:?}"))?;
    let precision = rgbstd::Precision::try_from(request.precision)
        .map_err(|err| anyhow::anyhow!("invalid RGB precision: {err:?}"))?;

    let spec = AssetSpec {
        ticker,
        name,
        details: None,
        precision,
    };
    let genesis_outpoint = outpoint_to_rgb(request.utxo);
    let genesis_seal = GenesisSeal::new_random(genesis_outpoint.txid, genesis_outpoint.vout);

    let contract = ContractBuilder::with(
        Identity::default(),
        NonInflatableAsset::schema(),
        NonInflatableAsset::types(),
        NonInflatableAsset::scripts(),
        network_to_rgb(network),
    )
    .add_global_state("spec", spec)
    .context("failed to add RGB asset spec")?
    .add_global_state(
        "terms",
        ContractTerms {
            text: Default::default(),
            media: None,
        },
    )
    .context("failed to add RGB contract terms")?
    .add_global_state("issuedSupply", rgbstd::Amount::from(request.amount))
    .context("failed to add RGB issued supply")?
    .add_fungible_state(
        "assetOwner",
        BuilderSeal::Revealed(genesis_seal),
        request.amount,
    )
    .context("failed to add RGB owner state")?
    .issue_contract()
    .map_err(|err| anyhow::anyhow!("failed to issue RGB contract: {err:?}"))?;

    let contract_id = contract.contract_id();
    let resolver = rgb_resolver(network, chain_source, [])?;
    with_rgb_stock_write_lock(stock_dir, || {
        let mut stock = open_or_create_stock(stock_dir)?;
        stock
            .import_contract(contract, resolver)
            .map_err(|err| anyhow::anyhow!("failed to import RGB contract into stock: {err:?}"))?;
        stock
            .store()
            .map_err(|err| anyhow::anyhow!("failed to persist RGB stock: {err:?}"))?;
        Ok(())
    })?;

    Ok(Rgb20IssueResult {
        contract_id,
        utxo: request.utxo,
        stock_dir: stock_dir.display().to_string(),
    })
}

pub fn transfer_rgb20_fixed(
    sender_stock_dir: &Path,
    sender_wallet: &mut LocalWallet,
    network: Network,
    esplora_url: &str,
    request: Rgb20TransferRequest,
    broadcast: bool,
) -> Result<Rgb20TransferResult> {
    let chain_source = chain_source_from_esplora_arg(network, esplora_url)?;
    sync_wallet_with_chain_source(sender_wallet, &chain_source)?;
    sender_wallet.persist()?;

    let mut stock = open_or_create_stock(sender_stock_dir)?;
    let recipient = Address::from_str(&request.recipient_address)
        .with_context(|| {
            format!(
                "invalid RGB recipient address: {}",
                request.recipient_address
            )
        })?
        .require_network(network)
        .with_context(|| {
            format!(
                "RGB recipient address is not for {network:?}: {}",
                request.recipient_address
            )
        })?;
    let fee_rate = FeeRate::from_sat_per_vb(request.fee_rate_sat_vb).context("invalid fee rate")?;
    let rgb_amount = rgbstd::Amount::from(request.amount);
    let rgb_inputs = select_rgb20_inputs(&stock, sender_wallet, request.contract_id, rgb_amount)?;
    let change_script = sender_wallet
        .wallet
        .peek_address(bdk_wallet::KeychainKind::Internal, 0)
        .script_pubkey();

    let mut builder = sender_wallet.wallet.build_tx();
    builder
        .add_utxos(&rgb_inputs)
        .context("failed to add RGB-bearing inputs to BTC transaction")?
        .add_recipient(
            recipient.script_pubkey(),
            Amount::from_sat(request.recipient_sats),
        )
        .add_recipient(ScriptBuf::new_op_return([0; 32]), Amount::ZERO)
        .drain_to(change_script.clone())
        .fee_rate(fee_rate)
        .nlocktime(LockTime::ZERO)
        .ordering(bdk_wallet::TxOrdering::Untouched);

    let psbt = builder
        .finish()
        .context("failed to build RGB carrier BTC transaction")?;
    let change_vout = find_output_vout(&psbt, &change_script)
        .context("RGB partial transfer requires a BTC change output for sender RGB change")?;
    let recipient_vout = find_output_vout(&psbt, &recipient.script_pubkey())
        .context("failed to find recipient output in RGB carrier transaction")?;

    let (fascia, mut psbt) = prepare_rgb20_psbt(
        &mut stock,
        psbt,
        change_vout,
        [(
            request.contract_id,
            rgb_amount,
            RgbSeal::Vout(recipient_vout),
        )],
    )?;

    let finalized = sender_wallet
        .wallet
        .sign(&mut psbt, bdk_wallet::SignOptions::default())
        .context("failed to sign RGB carrier BTC transaction")?;
    anyhow::ensure!(finalized, "RGB carrier BTC transaction was not finalized");
    let transaction = psbt
        .extract_tx()
        .context("failed to extract signed RGB carrier transaction")?;
    let txid = transaction.compute_txid();
    let recipient_outpoint = OutPoint::new(txid, recipient_vout);
    let transfer_opids = transfer_opids(&fascia, request.contract_id);

    let staged_stock_dir = staged_rgb_stock_dir(sender_stock_dir, txid);
    let mut staged_stock = stock.clone_no_persistence();
    staged_stock
        .consume_fascia(fascia.clone(), TentativeWitnessOrd)
        .map_err(|err| anyhow::anyhow!("failed to consume staged sender RGB fascia: {err:?}"))?;
    let consignment = staged_stock
        .transfer(
            request.contract_id,
            [OutputSeal::with(txid_to_rgb(txid), recipient_vout)],
            [],
            transfer_opids,
            Some(txid_to_rgb(txid)),
        )
        .map_err(|err| anyhow::anyhow!("failed to build RGB transfer consignment: {err:?}"))?;

    if broadcast {
        broadcast_transaction(network, Some(&chain_source), &transaction)
            .context("failed to broadcast RGB carrier BTC transaction")?;
        store_pending_rgb_operation(
            sender_stock_dir,
            txid,
            "pending_sender_transfer",
            RgbPendingOperation::SenderFascia {
                txid: txid.to_string(),
                fascia: encode_fascia(&fascia)?,
            },
        )?;
        sender_wallet
            .wallet
            .apply_unconfirmed_txs([(transaction.clone(), now())]);
        sender_wallet.persist()?;
    }

    Ok(Rgb20TransferResult {
        txid,
        transaction,
        fascia,
        consignment,
        recipient_outpoint,
        staged_stock_dir: broadcast.then_some(staged_stock_dir),
    })
}

pub fn build_rgb20_channel_funding(
    sender_stock_dir: &Path,
    sender_wallet: &mut LocalWallet,
    network: Network,
    esplora_url: &str,
    request: Rgb20ChannelFundingRequest,
) -> Result<Rgb20ChannelFundingResult> {
    build_rgb20_channel_funding_inner(
        sender_stock_dir,
        sender_wallet,
        network,
        esplora_url,
        request,
        true,
    )
}

pub fn build_rgb20_channel_funding_cached_first(
    sender_stock_dir: &Path,
    sender_wallet: &mut LocalWallet,
    network: Network,
    esplora_url: &str,
    request: Rgb20ChannelFundingRequest,
) -> Result<Rgb20ChannelFundingResult> {
    build_rgb20_channel_funding_inner(
        sender_stock_dir,
        sender_wallet,
        network,
        esplora_url,
        request,
        false,
    )
}

fn build_rgb20_channel_funding_inner(
    sender_stock_dir: &Path,
    sender_wallet: &mut LocalWallet,
    network: Network,
    esplora_url: &str,
    request: Rgb20ChannelFundingRequest,
    sync_before_build: bool,
) -> Result<Rgb20ChannelFundingResult> {
    if sync_before_build || sender_wallet.wallet.balance().total().to_sat() == 0 {
        let chain_source = chain_source_from_esplora_arg(network, esplora_url)?;
        match sync_wallet_with_chain_source(sender_wallet, &chain_source) {
            Ok(()) => sender_wallet.persist()?,
            Err(err)
                if is_transient_esplora_error_message(&format!("{err:#}"))
                    && sender_wallet.wallet.balance().total().to_sat() > 0 =>
            {
                // Public Esplora instances can rate-limit or temporarily fail. If the wallet has a
                // persisted confirmed view, keep the RGB-LN funding path moving instead of dropping
                // the in-flight channel negotiation.
            }
            Err(err) => return Err(err),
        }
    }

    let mut stock = open_or_create_stock(sender_stock_dir)?;
    let fee_rate = FeeRate::from_sat_per_vb(request.fee_rate_sat_vb).context("invalid fee rate")?;
    let rgb_amount = rgbstd::Amount::from(request.rgb_amount);
    let rgb_inputs = select_rgb20_inputs(&stock, sender_wallet, request.contract_id, rgb_amount)?;
    let change_script = sender_wallet
        .wallet
        .peek_address(bdk_wallet::KeychainKind::Internal, 0)
        .script_pubkey();

    let mut builder = sender_wallet.wallet.build_tx();
    builder
        .add_utxos(&rgb_inputs)
        .context("failed to add RGB-bearing inputs to LN funding transaction")?
        .add_recipient(
            request.funding_script_pubkey.clone(),
            Amount::from_sat(request.channel_value_satoshis),
        )
        .add_recipient(ScriptBuf::new_op_return([0; 32]), Amount::ZERO)
        .drain_to(change_script.clone())
        .fee_rate(fee_rate)
        .nlocktime(LockTime::ZERO)
        .ordering(bdk_wallet::TxOrdering::Untouched);

    let psbt = builder
        .finish()
        .context("failed to build RGB LN funding transaction")?;
    let change_vout = find_output_vout(&psbt, &change_script)
        .context("RGB LN funding requires a BTC change output for sender RGB change")?;
    let funding_vout = find_output_vout(&psbt, &request.funding_script_pubkey)
        .context("failed to find LN funding output in RGB carrier transaction")?;

    let (fascia, mut psbt) = prepare_rgb20_psbt(
        &mut stock,
        psbt,
        change_vout,
        [(request.contract_id, rgb_amount, RgbSeal::Vout(funding_vout))],
    )?;

    let finalized = sender_wallet
        .wallet
        .sign(&mut psbt, bdk_wallet::SignOptions::default())
        .context("failed to sign RGB LN funding transaction")?;
    anyhow::ensure!(finalized, "RGB LN funding transaction was not finalized");
    let transaction = psbt
        .extract_tx()
        .context("failed to extract signed RGB LN funding transaction")?;
    let txid = transaction.compute_txid();
    let funding_outpoint = OutPoint::new(txid, funding_vout);
    let transfer_opids = transfer_opids(&fascia, request.contract_id);
    let staged_stock_dir = staged_rgb_stock_dir(sender_stock_dir, txid);
    let mut staged_stock = stock.clone_no_persistence();
    staged_stock
        .consume_fascia(fascia.clone(), TentativeWitnessOrd)
        .map_err(|err| {
            anyhow::anyhow!("failed to consume staged sender RGB LN funding fascia: {err:?}")
        })?;
    let consignment = staged_stock
        .transfer(
            request.contract_id,
            [OutputSeal::with(txid_to_rgb(txid), funding_vout)],
            [],
            transfer_opids,
            Some(txid_to_rgb(txid)),
        )
        .map_err(|err| anyhow::anyhow!("failed to build RGB LN funding transfer: {err:?}"))?;

    let chain_source = chain_source_from_esplora_arg(network, esplora_url)?;
    let resolver =
        rgb_resolver_with_consignment(network, &chain_source, &consignment, [transaction.clone()])?;
    let validation = ValidationConfig {
        chain_net: network_to_rgb(network),
        trusted_typesystem: consignment.types.clone(),
        ..Default::default()
    };
    let transfer = consignment
        .validate(&resolver, &validation)
        .map_err(|status| anyhow::anyhow!("invalid RGB LN funding transfer: {status:?}"))?;

    store_pending_rgb_operation(
        sender_stock_dir,
        txid,
        "pending_sender_ln_funding",
        RgbPendingOperation::SenderFascia {
            txid: txid.to_string(),
            fascia: encode_fascia(&fascia)?,
        },
    )?;

    Ok(Rgb20ChannelFundingResult {
        txid,
        transaction,
        fascia,
        transfer,
        funding_outpoint,
        staged_stock_dir,
    })
}

pub fn accept_rgb20_transfer(
    receiver_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
    consignment: Transfer,
) -> Result<()> {
    let chain_source = chain_source_from_esplora_arg(network, esplora_url)?;
    accept_rgb20_transfer_with_chain_source(receiver_stock_dir, network, &chain_source, consignment)
}

pub fn accept_rgb20_transfer_with_chain_source(
    receiver_stock_dir: &Path,
    network: Network,
    chain_source: &ChainSource,
    consignment: Transfer,
) -> Result<()> {
    let valid = validate_rgb20_transfer_with_chain_source(network, chain_source, consignment)?;
    let resolver = rgb_resolver_with_consignment(network, chain_source, &valid, [])?;
    with_rgb_stock_write_lock(receiver_stock_dir, || {
        let mut stock = open_or_create_stock(receiver_stock_dir)?;
        stock
            .accept_transfer(valid, resolver)
            .map_err(|err| anyhow::anyhow!("failed to accept RGB transfer: {err:?}"))?;
        stock
            .store()
            .map_err(|err| anyhow::anyhow!("failed to persist receiver RGB stock: {err:?}"))?;
        Ok(())
    })?;
    Ok(())
}

pub fn accept_rgb20_transfer_staged(
    receiver_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
    txid: Txid,
    consignment: Transfer,
) -> Result<PathBuf> {
    let staged_stock_dir = staged_rgb_stock_dir(receiver_stock_dir, txid);
    validate_rgb20_transfer(network, esplora_url, consignment.clone())?;
    let mut consignment_bytes = Vec::new();
    consignment
        .save(&mut consignment_bytes)
        .context("failed to encode pending receiver RGB transfer")?;
    store_pending_rgb_operation(
        receiver_stock_dir,
        txid,
        "pending_receiver_transfer",
        RgbPendingOperation::ReceiverTransfer {
            txid: txid.to_string(),
            consignment: consignment_bytes,
        },
    )?;
    Ok(staged_stock_dir)
}

pub fn validate_rgb20_transfer(
    network: Network,
    esplora_url: &str,
    consignment: Transfer,
) -> Result<ValidTransfer> {
    let chain_source = chain_source_from_esplora_arg(network, esplora_url)?;
    validate_rgb20_transfer_with_chain_source(network, &chain_source, consignment)
}

pub fn validate_rgb20_transfer_with_chain_source(
    network: Network,
    chain_source: &ChainSource,
    consignment: Transfer,
) -> Result<ValidTransfer> {
    let resolver = rgb_resolver_with_consignment(network, &chain_source, &consignment, [])?;
    let validation = ValidationConfig {
        chain_net: network_to_rgb(network),
        trusted_typesystem: consignment.types.clone(),
        ..Default::default()
    };
    consignment
        .validate(&resolver, &validation)
        .map_err(|status| anyhow::anyhow!("invalid RGB transfer consignment: {status:?}"))
}

pub fn export_rgb20_transfer_consignment(
    sender_stock_dir: &Path,
    contract_id: ContractId,
    recipient_outpoint: OutPoint,
) -> Result<Transfer> {
    let stock = open_or_create_stock(sender_stock_dir)?;
    stock
        .transfer(
            contract_id,
            [OutputSeal::with(
                txid_to_rgb(recipient_outpoint.txid),
                recipient_outpoint.vout,
            )],
            [],
            [],
            Some(txid_to_rgb(recipient_outpoint.txid)),
        )
        .map_err(|err| anyhow::anyhow!("failed to export RGB transfer consignment: {err:?}"))
}

pub fn encode_rgb20_transfer_consignment(consignment: &Transfer) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    consignment
        .save(&mut bytes)
        .context("failed to encode RGB transfer consignment")?;
    Ok(bytes)
}

pub fn decode_rgb20_transfer_consignment(bytes: &[u8]) -> Result<Transfer> {
    Transfer::load(bytes).context("failed to decode RGB transfer consignment")
}

pub fn accept_rgb20_transfer_bytes(
    receiver_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
    consignment_bytes: &[u8],
) -> Result<()> {
    accept_rgb20_transfer(
        receiver_stock_dir,
        network,
        esplora_url,
        decode_rgb20_transfer_consignment(consignment_bytes)?,
    )
}

pub fn accept_rgb20_transfer_bytes_staged(
    receiver_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
    txid: Txid,
    consignment_bytes: &[u8],
) -> Result<PathBuf> {
    accept_rgb20_transfer_staged(
        receiver_stock_dir,
        network,
        esplora_url,
        txid,
        decode_rgb20_transfer_consignment(consignment_bytes)?,
    )
}

pub fn validate_rgb20_transfer_bytes(
    network: Network,
    esplora_url: &str,
    consignment_bytes: &[u8],
) -> Result<ValidTransfer> {
    validate_rgb20_transfer(
        network,
        esplora_url,
        decode_rgb20_transfer_consignment(consignment_bytes)?,
    )
}

pub fn list_rgb20_assets_for_address(
    stock_dir: &Path,
    address: &str,
    utxos: impl IntoIterator<Item = Rgb20TrackedUtxo>,
) -> Result<Vec<Rgb20AssetAllocation>> {
    list_rgb20_assets_for_utxos(
        stock_dir,
        utxos
            .into_iter()
            .filter(|utxo| utxo.address.as_deref() == Some(address)),
    )
}

pub fn list_rgb20_assets_for_utxos(
    stock_dir: &Path,
    utxos: impl IntoIterator<Item = Rgb20TrackedUtxo>,
) -> Result<Vec<Rgb20AssetAllocation>> {
    let stock = open_or_create_stock(stock_dir)?;
    let utxos = utxos.into_iter().collect::<Vec<_>>();
    let rgb_to_wallet = utxos
        .iter()
        .map(|utxo| (outpoint_to_rgb(utxo.outpoint), utxo))
        .collect::<HashMap<_, _>>();
    let filter = rgb_to_wallet.keys().copied().collect::<HashSet<_>>();

    let mut assets = Vec::new();
    for contract in stock
        .contracts()
        .map_err(|err| anyhow::anyhow!("failed to list RGB contracts: {err:?}"))?
    {
        let contract_data = stock
            .contract_data(contract.id)
            .map_err(|err| anyhow::anyhow!("failed to load RGB contract data: {err:?}"))?;
        let Ok(allocations) = contract_data.fungible("assetOwner", &filter) else {
            continue;
        };
        let spec = contract_data
            .global("spec")
            .next()
            .map(|strict_val| AssetSpec::from_strict_val_unchecked(&strict_val));

        for allocation in allocations {
            let rgb_outpoint = allocation.seal.to_outpoint();
            let Some(utxo) = rgb_to_wallet.get(&rgb_outpoint) else {
                continue;
            };
            let amount_raw = allocation.state.value();
            let (ticker, name, precision) = spec
                .as_ref()
                .map(|spec| {
                    (
                        spec.ticker.to_string(),
                        spec.name.to_string(),
                        spec.precision,
                    )
                })
                .unwrap_or_else(|| {
                    (
                        "".to_string(),
                        "".to_string(),
                        rgbstd::Precision::Indivisible,
                    )
                });
            assets.push(Rgb20AssetAllocation {
                contract_id: contract.id,
                outpoint: utxo.outpoint,
                address: utxo.address.clone(),
                amount: allocation.state,
                amount_raw,
                amount_display: rgbstd::CoinAmount::new(allocation.state, precision).to_string(),
                amount_encoding: allocation.state.to_string(),
                ticker,
                name,
                precision: precision.decimals(),
                witness: allocation.witness,
                confirmed: utxo.confirmed,
            });
        }
    }

    Ok(assets)
}

fn select_rgb20_inputs(
    stock: &Stock,
    wallet: &LocalWallet,
    contract_id: ContractId,
    amount: rgbstd::Amount,
) -> Result<Vec<OutPoint>> {
    let wallet_utxos = wallet.wallet.list_unspent().collect::<Vec<_>>();
    let available = wallet_utxos
        .iter()
        .filter(|utxo| !utxo.is_spent)
        .map(|utxo| outpoint_to_rgb(utxo.outpoint))
        .collect::<HashSet<_>>();
    let wallet_outpoints = wallet_utxos
        .iter()
        .map(|utxo| (outpoint_to_rgb(utxo.outpoint), utxo.outpoint))
        .collect::<HashMap<_, _>>();

    let contract = stock
        .contract_data(contract_id)
        .map_err(|err| anyhow::anyhow!("failed to load RGB contract data: {err:?}"))?;
    let mut state = contract
        .fungible("assetOwner", &available)
        .map_err(|err| anyhow::anyhow!("failed to list RGB assetOwner state: {err:?}"))?
        .fold(
            BTreeMap::<_, Vec<rgbstd::Amount>>::new(),
            |mut map, allocation| {
                map.entry(allocation.seal)
                    .or_default()
                    .push(allocation.state);
                map
            },
        )
        .into_iter()
        .map(|(seal, amounts)| {
            (
                amounts.into_iter().sum::<rgbstd::Amount>(),
                seal.to_outpoint(),
            )
        })
        .collect::<Vec<_>>();
    state.sort_by_key(|(sum, _)| std::cmp::Reverse(*sum));

    let mut selected = BTreeSet::new();
    let mut collected = rgbstd::Amount::ZERO;
    for (value, seal) in state {
        if collected >= amount {
            break;
        }
        collected += value;
        selected.insert(seal);
    }
    anyhow::ensure!(
        collected >= amount,
        "not enough RGB20 balance for contract {contract_id}: need {}, have {}",
        amount,
        collected
    );

    selected
        .into_iter()
        .map(|rgb_outpoint| {
            wallet_outpoints
                .get(&rgb_outpoint)
                .copied()
                .with_context(|| format!("RGB UTXO is not in wallet: {rgb_outpoint}"))
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RgbSeal {
    Vout(u32),
}

fn prepare_rgb20_psbt(
    stock: &mut Stock,
    mut psbt: Psbt,
    change_vout: u32,
    rgb_assign: impl IntoIterator<Item = (ContractId, rgbstd::Amount, RgbSeal)>,
) -> Result<(Fascia, Psbt)> {
    anyhow::ensure!(
        psbt.unsigned_tx
            .output
            .get(change_vout as usize)
            .is_some_and(|output| output.value > Amount::ZERO),
        "RGB sender change output is missing or zero"
    );

    let rgb_assign = rgb_assign.into_iter().fold(
        HashMap::<ContractId, HashMap<RgbSeal, rgbstd::Amount>>::new(),
        |mut map, (contract_id, amount, seal)| {
            map.entry(contract_id)
                .or_default()
                .entry(seal)
                .and_modify(|existing| existing.saturating_add_assign(amount))
                .or_insert(amount);
            map
        },
    );

    let (opret_vout, _) = psbt
        .unsigned_tx
        .output
        .iter()
        .enumerate()
        .find(|(_, output)| output.script_pubkey.is_op_return())
        .context("RGB carrier transaction has no OP_RETURN output")?;
    psbt.unsigned_tx.output[opret_vout].script_pubkey = ScriptBuf::new_op_return([]);

    let mut rgb_psbt = psbt;
    let prev_outputs = rgb_psbt
        .unsigned_tx()
        .input
        .iter()
        .map(|input| input.previous_output)
        .collect::<Vec<_>>();
    let assignment_name = "assetOwner";
    let transfer_name = "transfer";
    let transfer_contract_ids = rgb_assign.keys().copied().collect::<HashSet<_>>();
    let mut all_transitions = HashMap::<ContractId, Vec<Transition>>::new();
    let mut blank_state = HashMap::<ContractId, Vec<(Opout, AllocatedState)>>::new();

    for (contract_id, assign) in rgb_assign {
        let mut total_input = rgbstd::Amount::ZERO;
        let mut builder = stock
            .transition_builder(contract_id, transfer_name)
            .map_err(|err| anyhow::anyhow!("failed to create RGB transition builder: {err:?}"))?;

        for (_seal, opout_state_map) in stock
            .contract_assignments_for(contract_id, prev_outputs.clone())
            .map_err(|err| anyhow::anyhow!("failed to load RGB input assignments: {err:?}"))?
        {
            for (opout, state) in opout_state_map {
                match state {
                    AllocatedState::Amount(value) => total_input += value.as_u64().into(),
                    _ => {
                        blank_state
                            .entry(contract_id)
                            .or_default()
                            .push((opout, state.clone()));
                        continue;
                    }
                }
                builder = builder.add_input(opout, state).map_err(|err| {
                    anyhow::anyhow!("failed to add RGB transition input: {err:?}")
                })?;
            }
        }

        let mut change_amount = total_input;
        for (seal, amount) in assign {
            change_amount = change_amount.saturating_sub(amount);
            builder = builder
                .add_fungible_state(assignment_name, builder_seal(seal, contract_id), amount)
                .map_err(|err| anyhow::anyhow!("failed to add RGB recipient state: {err:?}"))?;
        }
        if change_amount > rgbstd::Amount::ZERO {
            builder = builder
                .add_fungible_state(
                    assignment_name,
                    BuilderSeal::Revealed(rgbstd::GraphSeal::new_random_vout(change_vout)),
                    change_amount,
                )
                .map_err(|err| anyhow::anyhow!("failed to add RGB change state: {err:?}"))?;
        }

        let transition = builder
            .complete_transition()
            .map_err(|err| anyhow::anyhow!("failed to complete RGB transition: {err:?}"))?;
        all_transitions
            .entry(contract_id)
            .or_default()
            .push(transition.clone());
        rgb_psbt
            .push_rgb_transition(transition)
            .map_err(|err| anyhow::anyhow!("failed to push RGB transition into PSBT: {err:?}"))?;
    }

    for utxo in prev_outputs {
        for contract_id in stock
            .contracts_assigning([utxo])
            .map_err(|err| anyhow::anyhow!("failed to find RGB blank-state contracts: {err:?}"))?
        {
            if transfer_contract_ids.contains(&contract_id) {
                continue;
            }
            for (_seal, assignments) in stock
                .contract_assignments_for(contract_id, [utxo])
                .map_err(|err| anyhow::anyhow!("failed to load RGB blank-state inputs: {err:?}"))?
            {
                blank_state
                    .entry(contract_id)
                    .or_default()
                    .extend(assignments);
            }
        }
    }

    for (contract_id, opouts) in blank_state {
        let schema = stock
            .as_stash_provider()
            .contract_schema(contract_id)
            .map_err(|err| anyhow::anyhow!("failed to load RGB schema: {err:?}"))?;
        for (opout, state) in opouts {
            let transition_type = schema.default_transition_for_assignment(&opout.ty);
            let transition = stock
                .transition_builder_raw(contract_id, transition_type)
                .map_err(|err| anyhow::anyhow!("failed to build RGB blank transition: {err:?}"))?
                .add_input(opout, state.clone())
                .map_err(|err| anyhow::anyhow!("failed to add RGB blank input: {err:?}"))?
                .add_owned_state_raw(
                    opout.ty,
                    rgbstd::GraphSeal::new_random_vout(change_vout),
                    state,
                )
                .map_err(|err| anyhow::anyhow!("failed to add RGB blank change: {err:?}"))?
                .complete_transition()
                .map_err(|err| {
                    anyhow::anyhow!("failed to complete RGB blank transition: {err:?}")
                })?;
            all_transitions
                .entry(contract_id)
                .or_default()
                .push(transition.clone());
            rgb_psbt
                .push_rgb_transition(transition)
                .map_err(|err| anyhow::anyhow!("failed to push RGB blank transition: {err:?}"))?;
        }
    }

    let (opret_vout, _) = rgb_psbt
        .unsigned_tx()
        .output
        .iter()
        .enumerate()
        .find(|(_, output)| output.script_pubkey.is_op_return())
        .context("RGB PSBT has no OP_RETURN output")?;
    rgb_psbt.outputs[opret_vout].set_opret_host();

    for (contract_id, transitions) in &all_transitions {
        for transition in transitions {
            for opout in transition.inputs() {
                rgb_psbt
                    .set_rgb_contract_consumer(*contract_id, opout, transition.id())
                    .map_err(|err| {
                        anyhow::anyhow!("failed to set RGB contract consumer: {err:?}")
                    })?;
            }
        }
    }

    rgb_psbt.set_rgb_close_method(CloseMethod::OpretFirst);
    let fascia = rgb_psbt
        .rgb_commit()
        .map_err(|err| anyhow::anyhow!("failed to commit RGB PSBT: {err:?}"))?;
    Ok((fascia, rgb_psbt))
}

fn is_transient_esplora_error_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("esplora sync failed")
        && (message.contains("unexpected eof")
            || message.contains("connection reset")
            || message.contains("ssl_error")
            || message.contains("timeout")
            || message.contains("timed out")
            || message.contains("network")
            || message.contains("minreq")
            || message.contains("429")
            || message.contains("too many requests")
            || message.contains("502")
            || message.contains("503")
            || message.contains("504")
            || message.contains("bad gateway")
            || message.contains("service unavailable")
            || message.contains("gateway timeout"))
}

fn builder_seal(seal: RgbSeal, contract_id: ContractId) -> BuilderSeal<rgbstd::GraphSeal> {
    match seal {
        RgbSeal::Vout(vout) => {
            let mut hasher = std::hash::DefaultHasher::new();
            contract_id.hash(&mut hasher);
            BuilderSeal::Revealed(rgbstd::GraphSeal::with_blinded_vout(vout, hasher.finish()))
        }
    }
}

fn find_output_vout(psbt: &Psbt, script_pubkey: &ScriptBuf) -> Option<u32> {
    psbt.unsigned_tx
        .output
        .iter()
        .position(|output| &output.script_pubkey == script_pubkey)
        .and_then(|vout| vout.try_into().ok())
}

fn esplora_builder(config: &EsploraConfig) -> esplora_client::Builder {
    let mut builder = esplora_client::Builder::new(&config.url).timeout(10);
    if let Some(api_key) = config.api_key.as_deref().filter(|value| !value.is_empty()) {
        builder = builder.header("api-key", api_key);
    }
    builder
}

fn chain_source_from_esplora_arg(network: Network, esplora_url: &str) -> Result<ChainSource> {
    let trimmed = esplora_url.trim();
    ChainSource::from_esplora_or_default(network, (!trimmed.is_empty()).then_some(trimmed))
}

fn rgb_resolver(
    network: Network,
    chain_source: &ChainSource,
    local_txs: impl IntoIterator<Item = Transaction>,
) -> Result<LocalWitnessResolver> {
    match chain_source {
        ChainSource::Esplora(config) => {
            let resolver = AnyResolver::esplora_blocking(esplora_builder(config))
                .map_err(|err| anyhow::anyhow!("failed to create RGB Esplora resolver: {err}"))?;
            Ok(LocalWitnessResolver::new(resolver, local_txs))
        }
        ChainSource::BitcoinCore(config) => Ok(LocalWitnessResolver::new(
            BitcoinCoreWitnessResolver::new(config.clone()),
            local_txs,
        )),
    }
    .and_then(|resolver| {
        resolver
            .check_chain_net(network_to_rgb(network))
            .map_err(|err| anyhow::anyhow!("RGB witness resolver chain check failed: {err:?}"))?;
        Ok(resolver)
    })
}

fn rgb_resolver_with_consignment<const TYPE: bool>(
    network: Network,
    chain_source: &ChainSource,
    consignment: &Consignment<TYPE>,
    local_txs: impl IntoIterator<Item = Transaction>,
) -> Result<LocalWitnessResolver> {
    match chain_source {
        ChainSource::Esplora(config) => {
            let mut resolver = AnyResolver::esplora_blocking(esplora_builder(config))
                .map_err(|err| anyhow::anyhow!("failed to create RGB Esplora resolver: {err}"))?;
            resolver.add_consignment_txes(consignment);
            Ok(LocalWitnessResolver::new(resolver, local_txs))
        }
        ChainSource::BitcoinCore(config) => {
            let mut resolver = BitcoinCoreWitnessResolver::new(config.clone());
            resolver.add_consignment_txes(consignment);
            Ok(LocalWitnessResolver::new(resolver, local_txs))
        }
    }
    .and_then(|resolver| {
        resolver
            .check_chain_net(network_to_rgb(network))
            .map_err(|err| anyhow::anyhow!("RGB witness resolver chain check failed: {err:?}"))?;
        Ok(resolver)
    })
}

fn consignment_txs<const TYPE: bool>(
    consignment: &Consignment<TYPE>,
) -> impl Iterator<Item = Transaction> + '_ {
    consignment
        .bundles
        .iter()
        .filter_map(|bundle| bundle.pub_witness.tx().cloned())
}

fn transfer_opids(fascia: &Fascia, contract_id: ContractId) -> Vec<rgbstd::OpId> {
    fascia
        .clone()
        .into_bundles()
        .into_iter()
        .find_map(|(id, bundle)| (id == contract_id).then(|| bundle.known_transitions_opids()))
        .map(|set| set.into_iter().collect())
        .unwrap_or_default()
}

fn open_or_create_stock(stock_dir: &Path) -> Result<Stock> {
    let store = LocalNodeStore::open_store_dir(stock_dir)?;
    let provider = store.rgb_stock_store()?;
    let stock_is_empty = !store.rgb_stock_has_data()?;

    match Stock::load(provider.clone(), true) {
        Ok(stock) => Ok(stock),
        Err(_err) if stock_is_empty => {
            let mut stock = Stock::in_memory();
            stock
                .make_persistent(provider, true)
                .map_err(|err| anyhow::anyhow!("failed to initialize RGB stock store: {err:?}"))?;
            Ok(stock)
        }
        Err(err) => Err(anyhow::anyhow!("failed to load RGB stock store: {err:?}")),
    }
}

fn with_rgb_stock_write_lock<T>(stock_dir: &Path, f: impl FnOnce() -> Result<T>) -> Result<T> {
    LocalNodeStore::open_store_dir(stock_dir)?.with_rgb_stock_write_lock(f)
}

fn ensure_staged_stock_can_promote(main_stock_dir: &Path, staged_stock_dir: &Path) -> Result<()> {
    let txid = pending_txid_from_staged_stock_dir(staged_stock_dir)?;
    let store = LocalNodeStore::open_store_dir(main_stock_dir)?;
    anyhow::ensure!(
        store.get_rgb_pending_op(txid)?.is_some(),
        "staged RGB operation is missing: {}",
        staged_stock_dir.display()
    );
    Ok(())
}

pub fn promote_staged_rgb_stock(main_stock_dir: &Path, staged_stock_dir: &Path) -> Result<()> {
    let txid = pending_txid_from_staged_stock_dir(staged_stock_dir)?;
    let store = LocalNodeStore::open_store_dir(main_stock_dir)?;
    anyhow::ensure!(
        store.get_rgb_pending_op(txid)?.is_some(),
        "staged RGB operation is missing: {}",
        staged_stock_dir.display()
    );
    anyhow::bail!(
        "promoting RGB pending operations requires chain context; use promote_staged_rgb_stock_if_tx_confirmed_with_esploras"
    )
}

pub fn promote_staged_rgb_stock_if_tx_confirmed(
    main_stock_dir: &Path,
    staged_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
    txid: Txid,
) -> Result<bool> {
    promote_staged_rgb_stock_if_tx_confirmed_with_esploras(
        main_stock_dir,
        staged_stock_dir,
        network,
        &[esplora_url.to_string()],
        txid,
    )
}

pub fn promote_staged_rgb_stock_if_tx_confirmed_with_esploras(
    main_stock_dir: &Path,
    _staged_stock_dir: &Path,
    network: Network,
    esplora_urls: &[String],
    txid: Txid,
) -> Result<bool> {
    match fetch_tx_confirmation_consensus(network, esplora_urls, txid, 0)? {
        Some(true) => {
            let chain_source = chain_source_from_esplora_urls(network, esplora_urls)?;
            replay_pending_rgb_operation(main_stock_dir, network, &chain_source, txid)?;
            Ok(true)
        }
        Some(false) | None => Ok(false),
    }
}

fn normalized_esplora_urls(esplora_urls: &[String]) -> Vec<String> {
    esplora_urls
        .iter()
        .map(|url| url.trim())
        .filter(|url| !url.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn rotate_esplora_urls(esplora_urls: &[String], start: usize) -> Vec<String> {
    let urls = normalized_esplora_urls(esplora_urls);
    if urls.is_empty() {
        return urls;
    }
    (0..urls.len())
        .map(|offset| urls[(start + offset) % urls.len()].clone())
        .collect()
}

const RGB_TX_STATUS_CONFIRMED_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const RGB_TX_STATUS_UNCONFIRMED_CACHE_TTL: Duration = Duration::from_secs(60);
const RGB_TX_STATUS_ERROR_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct TxConfirmationCacheEntry {
    fetched_at: Instant,
    result: std::result::Result<Option<bool>, String>,
}

static TX_CONFIRMATION_CACHE: OnceLock<Mutex<HashMap<(Network, Txid), TxConfirmationCacheEntry>>> =
    OnceLock::new();

fn fetch_tx_confirmation_consensus(
    network: Network,
    esplora_urls: &[String],
    txid: Txid,
    start: usize,
) -> Result<Option<bool>> {
    let cache_key = (network, txid);
    if let Some(cached) = cached_tx_confirmation(&cache_key) {
        return cached.map_err(anyhow::Error::msg);
    }
    let fetched = fetch_tx_confirmation_consensus_uncached(network, esplora_urls, txid, start)
        .map_err(|err| format!("{err:#}"));
    cache_tx_confirmation(cache_key, fetched.clone());
    fetched.map_err(anyhow::Error::msg)
}

fn cached_tx_confirmation(
    cache_key: &(Network, Txid),
) -> Option<std::result::Result<Option<bool>, String>> {
    let cache = TX_CONFIRMATION_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().expect("RGB tx confirmation cache poisoned");
    let Some(entry) = cache.get(cache_key) else {
        return None;
    };
    let ttl = match entry.result {
        Ok(Some(true)) => RGB_TX_STATUS_CONFIRMED_CACHE_TTL,
        Ok(Some(false)) | Ok(None) => RGB_TX_STATUS_UNCONFIRMED_CACHE_TTL,
        Err(_) => RGB_TX_STATUS_ERROR_CACHE_TTL,
    };
    if entry.fetched_at.elapsed() <= ttl {
        return Some(entry.result.clone());
    }
    cache.remove(cache_key);
    None
}

fn cache_tx_confirmation(
    cache_key: (Network, Txid),
    result: std::result::Result<Option<bool>, String>,
) {
    let cache = TX_CONFIRMATION_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    cache
        .lock()
        .expect("RGB tx confirmation cache poisoned")
        .insert(
            cache_key,
            TxConfirmationCacheEntry {
                fetched_at: Instant::now(),
                result,
            },
        );
}

fn fetch_tx_confirmation_consensus_uncached(
    network: Network,
    esplora_urls: &[String],
    txid: Txid,
    start: usize,
) -> Result<Option<bool>> {
    let rotated_urls = rotate_esplora_urls(esplora_urls, start);
    if rotated_urls.is_empty() {
        let source = ChainSource::from_esplora_or_default(network, None)?;
        return fetch_tx_confirmation_from_chain_source(network, &source, txid);
    }

    let mut saw_confirmed = false;
    let mut last_error = None;
    for url in rotated_urls {
        let client = match esplora_client(network, Some(&url)) {
            Ok(client) => client,
            Err(err) => {
                last_error = Some(err);
                continue;
            }
        };
        match client.get_tx_status(&txid) {
            Ok(status) if status.confirmed => saw_confirmed = true,
            Ok(_) => return Ok(Some(false)),
            Err(err) => last_error = Some(anyhow::anyhow!("{err:?}")),
        }
    }
    if saw_confirmed {
        return Ok(Some(true));
    }
    if let Some(err) = last_error {
        return Err(err).with_context(|| format!("failed to fetch RGB carrier tx status: {txid}"));
    }
    Ok(None)
}

fn fetch_tx_confirmation_from_chain_source(
    _network: Network,
    source: &ChainSource,
    txid: Txid,
) -> Result<Option<bool>> {
    match source {
        ChainSource::Esplora(config) => {
            let client = esplora_client_with_config(config);
            Ok(client
                .get_tx_status(&txid)
                .ok()
                .map(|status| status.confirmed))
        }
        ChainSource::BitcoinCore(config) => {
            Ok(bitcoin_core_tx_status(config, txid)?.map(|status| status.confirmed))
        }
    }
}

pub fn scan_and_promote_confirmed_staged_rgb_stocks(
    main_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
) -> Result<RgbPendingStockScanReport> {
    scan_and_promote_confirmed_staged_rgb_stocks_with_esploras(
        main_stock_dir,
        network,
        &[esplora_url.to_string()],
    )
}

pub fn scan_and_promote_confirmed_staged_rgb_stocks_with_esploras(
    main_stock_dir: &Path,
    network: Network,
    esplora_urls: &[String],
) -> Result<RgbPendingStockScanReport> {
    scan_and_promote_or_revoke_staged_rgb_stocks_inner(main_stock_dir, network, esplora_urls, None)
}

pub fn scan_and_promote_or_revoke_staged_rgb_stocks_with_esploras(
    main_stock_dir: &Path,
    network: Network,
    esplora_urls: &[String],
    recover_revoked_tx: &mut dyn FnMut(Txid) -> Result<bool>,
) -> Result<RgbPendingStockScanReport> {
    scan_and_promote_or_revoke_staged_rgb_stocks_inner(
        main_stock_dir,
        network,
        esplora_urls,
        Some(recover_revoked_tx),
    )
}

fn scan_and_promote_or_revoke_staged_rgb_stocks_inner(
    main_stock_dir: &Path,
    network: Network,
    esplora_urls: &[String],
    mut recover_revoked_tx: Option<&mut dyn FnMut(Txid) -> Result<bool>>,
) -> Result<RgbPendingStockScanReport> {
    let pending_root = pending_rgb_stock_root(main_stock_dir);
    let mut report = RgbPendingStockScanReport::default();
    let Ok(entries) = fs::read_dir(&pending_root) else {
        return Ok(report);
    };
    let mut staged_dirs = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            entry
                .file_type()
                .ok()
                .filter(|file_type| file_type.is_dir())
                .map(|_| entry.path())
        })
        .collect::<Vec<_>>();
    staged_dirs.sort();

    let mut waiting_stocks = Vec::new();
    for staged_stock_dir in staged_dirs {
        let Some(txid) = staged_stock_dir
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<Txid>().ok())
        else {
            report.skipped += 1;
            continue;
        };
        let status_path = pending_stock_status_path(&staged_stock_dir);
        let existing_status = read_pending_stock_status(&status_path).ok();
        if existing_status
            .as_ref()
            .is_some_and(|status| is_terminal_pending_stock_status(&status.status))
        {
            report.skipped += 1;
            continue;
        }
        let staged_has_data = LocalNodeStore::open_store_dir(main_stock_dir)
            .and_then(|store| Ok(store.get_rgb_pending_op(txid)?.is_some()))
            .unwrap_or(false);
        if !staged_has_data {
            if recover_revoked_tx.is_some() {
                report.scanned += 1;
                if revoke_pending_stock_tx(
                    txid,
                    main_stock_dir,
                    &staged_stock_dir,
                    &status_path,
                    &mut recover_revoked_tx,
                )? {
                    report.revoked += 1;
                    report.revoked_txids.push(txid);
                } else {
                    report.skipped += 1;
                }
                continue;
            }
            write_pending_stock_status(
                &status_path,
                pending_stock_status(
                    txid,
                    "invalid",
                    main_stock_dir,
                    &staged_stock_dir,
                    None,
                    None,
                ),
            )?;
            report.skipped += 1;
            continue;
        }
        waiting_stocks.push((staged_stock_dir, txid, status_path, existing_status));
    }

    if waiting_stocks.is_empty() {
        return Ok(report);
    }

    for (index, (staged_stock_dir, txid, status_path, existing_status)) in
        waiting_stocks.into_iter().enumerate()
    {
        report.scanned += 1;
        let Some(tx_confirmed) =
            fetch_tx_confirmation_consensus(network, esplora_urls, txid, index)?
        else {
            report.skipped += 1;
            continue;
        };
        if !tx_confirmed {
            if existing_status
                .as_ref()
                .is_some_and(|status| matches!(status.status.as_str(), "stale" | "revoke_pending"))
                && recover_revoked_tx.is_some()
            {
                if revoke_pending_stock_tx(
                    txid,
                    main_stock_dir,
                    &staged_stock_dir,
                    &status_path,
                    &mut recover_revoked_tx,
                )? {
                    report.revoked += 1;
                    report.revoked_txids.push(txid);
                } else {
                    report.skipped += 1;
                }
                continue;
            }
            write_pending_stock_status(
                &status_path,
                pending_stock_status(
                    txid,
                    "pending",
                    main_stock_dir,
                    &staged_stock_dir,
                    None,
                    None,
                ),
            )?;
            report.pending += 1;
            continue;
        }
        ensure_staged_stock_can_promote(main_stock_dir, &staged_stock_dir)?;
        let chain_source = chain_source_from_esplora_urls(network, esplora_urls)?;
        replay_pending_rgb_operation(main_stock_dir, network, &chain_source, txid)?;
        let now = now();
        write_pending_stock_status(
            &status_path,
            pending_stock_status(
                txid,
                "confirmed",
                main_stock_dir,
                &staged_stock_dir,
                Some(now),
                Some(now),
            ),
        )?;
        report.promoted += 1;
        report.promoted_txids.push(txid);
    }
    Ok(report)
}

fn revoke_pending_stock_tx(
    txid: Txid,
    main_stock_dir: &Path,
    staged_stock_dir: &Path,
    status_path: &Path,
    recover_revoked_tx: &mut Option<&mut dyn FnMut(Txid) -> Result<bool>>,
) -> Result<bool> {
    let confirmed_at = None;
    write_pending_stock_status(
        status_path,
        pending_stock_status(
            txid,
            "revoke_pending",
            main_stock_dir,
            staged_stock_dir,
            confirmed_at,
            None,
        ),
    )?;
    let recovered = match recover_revoked_tx.as_mut() {
        Some(recover) => recover(txid)?,
        None => false,
    };
    let mut status = pending_stock_status(
        txid,
        if recovered {
            "revoked"
        } else {
            "revoke_failed"
        },
        main_stock_dir,
        staged_stock_dir,
        confirmed_at,
        None,
    );
    if recovered {
        let _ = LocalNodeStore::open_store_dir(main_stock_dir)
            .and_then(|store| store.remove_rgb_pending_op(txid));
        status.revoked_at = Some(now());
    }
    write_pending_stock_status(status_path, status)?;
    Ok(recovered)
}

pub fn stage_rgb_stock_for_tx<F>(
    main_stock_dir: &Path,
    txid: Txid,
    status: &str,
    update_stock: F,
) -> Result<PathBuf>
where
    F: FnOnce(&mut Stock) -> Result<()>,
{
    let staged_stock_dir = staged_rgb_stock_dir(main_stock_dir, txid);
    let mut stock = open_or_create_stock(main_stock_dir)?.clone_no_persistence();
    update_stock(&mut stock)?;
    store_pending_rgb_operation(
        main_stock_dir,
        txid,
        status,
        RgbPendingOperation::RecolorTxs {
            txids: vec![txid.to_string()],
            retrospective: true,
        },
    )?;
    Ok(staged_stock_dir)
}

fn store_pending_rgb_operation(
    main_stock_dir: &Path,
    txid: Txid,
    status: &str,
    op: RgbPendingOperation,
) -> Result<PathBuf> {
    let staged_stock_dir = staged_rgb_stock_dir(main_stock_dir, txid);
    fs::create_dir_all(&staged_stock_dir).with_context(|| {
        format!(
            "create staged RGB operation marker {}",
            staged_stock_dir.display()
        )
    })?;
    let bytes = serde_json::to_vec(&op).context("encode RGB pending operation")?;
    LocalNodeStore::open_store_dir(main_stock_dir)?.put_rgb_pending_op(txid, &bytes)?;
    write_pending_stock_status(
        &pending_stock_status_path(&staged_stock_dir),
        pending_stock_status(txid, status, main_stock_dir, &staged_stock_dir, None, None),
    )?;
    Ok(staged_stock_dir)
}

fn replay_pending_rgb_operation(
    main_stock_dir: &Path,
    network: Network,
    chain_source: &ChainSource,
    txid: Txid,
) -> Result<()> {
    let store = LocalNodeStore::open_store_dir(main_stock_dir)?;
    let bytes = store
        .get_rgb_pending_op(txid)?
        .with_context(|| format!("RGB pending operation not found for {txid}"))?;
    let op: RgbPendingOperation =
        serde_json::from_slice(&bytes).context("decode RGB pending operation")?;
    match op {
        RgbPendingOperation::SenderFascia { fascia, .. } => {
            let fascia = decode_fascia(&fascia)?;
            with_rgb_stock_write_lock(main_stock_dir, || {
                let mut stock = open_or_create_stock(main_stock_dir)?;
                stock
                    .consume_fascia(fascia, TentativeWitnessOrd)
                    .map_err(|err| {
                        anyhow::anyhow!("failed to replay sender RGB fascia: {err:?}")
                    })?;
                stock.store().map_err(|err| {
                    anyhow::anyhow!("failed to persist replayed sender RGB stock: {err:?}")
                })?;
                Ok(())
            })?;
        }
        RgbPendingOperation::ReceiverTransfer { consignment, .. } => {
            accept_rgb20_transfer_with_chain_source(
                main_stock_dir,
                network,
                chain_source,
                decode_rgb20_transfer_consignment(&consignment)?,
            )?;
        }
        RgbPendingOperation::RecolorTxs {
            txids,
            retrospective,
        } => {
            let txids = txids
                .iter()
                .map(|txid| {
                    Txid::from_str(txid)
                        .with_context(|| format!("invalid RGB pending recolor txid {txid}"))
                })
                .collect::<Result<Vec<_>>>()?;
            with_rgb_stock_write_lock(main_stock_dir, || {
                let mut stock = open_or_create_stock(main_stock_dir)?;
                let _ = (&mut stock, &txids, retrospective);
                stock.store().map_err(|err| {
                    anyhow::anyhow!("failed to persist replayed RGB recolor operation: {err:?}")
                })?;
                Ok(())
            })?;
        }
    }
    store.remove_rgb_pending_op(txid)?;
    Ok(())
}

fn chain_source_from_esplora_urls(
    network: Network,
    esplora_urls: &[String],
) -> Result<ChainSource> {
    let url = normalized_esplora_urls(esplora_urls)
        .into_iter()
        .next()
        .with_context(|| format!("no Esplora URL configured for {network:?}"))?;
    chain_source_from_esplora_arg(network, &url)
}

fn encode_fascia(fascia: &Fascia) -> Result<Vec<u8>> {
    Ok(fascia
        .to_strict_serialized::<U32MAX>()
        .context("encode RGB fascia")?
        .release())
}

fn decode_fascia(bytes: &[u8]) -> Result<Fascia> {
    let confined = Confined::try_from(bytes.to_vec()).context("confine RGB fascia bytes")?;
    Fascia::from_strict_serialized::<U32MAX>(confined).context("decode RGB fascia")
}

fn staged_rgb_stock_dir(main_stock_dir: &Path, txid: Txid) -> PathBuf {
    pending_rgb_stock_root(main_stock_dir).join(txid.to_string())
}

fn pending_rgb_stock_root(main_stock_dir: &Path) -> PathBuf {
    let stock_name = main_stock_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("stock");
    main_stock_dir
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{stock_name}_pending"))
}

fn pending_stock_status_path(staged_stock_dir: &Path) -> PathBuf {
    staged_stock_dir.join("pending-status.json")
}

fn pending_txid_from_staged_stock_dir(staged_stock_dir: &Path) -> Result<Txid> {
    staged_stock_dir
        .file_name()
        .and_then(|name| name.to_str())
        .context("staged RGB stock path has no txid component")?
        .parse::<Txid>()
        .with_context(|| {
            format!(
                "invalid staged RGB stock txid: {}",
                staged_stock_dir.display()
            )
        })
}

fn is_terminal_pending_stock_status(status: &str) -> bool {
    matches!(
        status,
        "confirmed" | "invalid" | "revoked" | "stale_confirmed_stock_conflict"
    )
}

fn pending_stock_status(
    txid: Txid,
    status: &str,
    main_stock_dir: &Path,
    staged_stock_dir: &Path,
    confirmed_at: Option<u64>,
    promoted_at: Option<u64>,
) -> RgbPendingStockStatus {
    RgbPendingStockStatus {
        txid: txid.to_string(),
        status: status.to_string(),
        main_stock_dir: main_stock_dir.display().to_string(),
        staged_stock_dir: staged_stock_dir.display().to_string(),
        updated_at: now(),
        confirmed_at,
        promoted_at,
        revoked_at: None,
    }
}

fn read_pending_stock_status(path: &Path) -> Result<RgbPendingStockStatus> {
    serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("read RGB pending stock status {}", path.display()))
}

fn write_pending_stock_status(path: &Path, status: RgbPendingStockStatus) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(&status)?)
        .with_context(|| format!("write RGB pending stock status {}", path.display()))
}

fn outpoint_to_rgb(outpoint: OutPoint) -> rgbstd::Outpoint {
    rgbstd::Outpoint::from_str(&outpoint.to_string())
        .expect("bitcoin outpoint is valid RGB outpoint")
}

fn txid_to_rgb(txid: Txid) -> RgbTxid {
    RgbTxid::from_str(&txid.to_string()).expect("bitcoin txid is valid RGB txid")
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn network_to_rgb(network: Network) -> rgbstd::ChainNet {
    match network {
        Network::Bitcoin => rgbstd::ChainNet::BitcoinMainnet,
        Network::Testnet => rgbstd::ChainNet::BitcoinTestnet3,
        Network::Testnet4 => rgbstd::ChainNet::BitcoinTestnet4,
        Network::Signet => rgbstd::ChainNet::BitcoinSignet,
        Network::Regtest => rgbstd::ChainNet::BitcoinRegtest,
    }
}
