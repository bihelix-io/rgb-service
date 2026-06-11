// RGB smart contracts for Bitcoin & Lightning
//
// SPDX-License-Identifier: Apache-2.0
//
// Written in 2019-2023 by
//     Dr Maxim Orlovsky <orlovsky@lnp-bp.org>
//
// Copyright (C) 2019-2023 LNP/BP Standards Association. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;

use amplify::confinement::{SmallOrdMap, U16 as MAX16};
use baid64::DisplayBaid64;
use bpwallet::cli::{BpCommand, Config, Exec};
use bpwallet::psbt::{Output, PropKey, Psbt, PsbtConstructor, PsbtVer};
use bpwallet::{Derive, Sats, Wallet, XpubDerivable};
use psrgbt::bp_conversion_utils::{
    address_payload_bitcoin_from_script_pubkey, network_bp_to_bitcoin, outpoint_bitcoin_to_bp,
    outpoint_bp_to_bitcoin,
};
use rgb::containers::{
    BuilderSeal, ConsignmentExt, ContainerVer, Contract, FileContent, SecretSeals, Transfer,
    UniversalFile,
};
use rgb::invoice::{Beneficiary, Pay2Vout, RgbInvoice, RgbInvoiceBuilder, XChainNet};
use rgb::persistence::{MemContract, StashReadProvider, Stock};
use rgb::resolvers::ContractIssueResolver;
use rgb::schema::SchemaId;
use rgb::validation::{ValidationConfig, Validity};
use rgb::vm::{RgbIsa, WitnessOrd};
use rgb::{
    Allocation, BundleId, ContractId, GenesisSeal,
    GraphSeal, Identity, OpId, Outpoint, OutputSeal, OwnedFraction, RgbDescr, RgbWallet, StateType,
    TokenIndex, TransferParams, Txid, WalletError, WalletProvider,
};
use rgbstd::contract::{AllocatedState, AssignmentsFilter, ContractData, ContractOp};
use rgbstd::persistence::MemContractState;
use rgbstd::{KnownState, OutputAssignment};
use serde_crate::{Deserialize, Serialize};
use strict_types::{FieldName, StrictVal};

use crate::RgbArgs;

fn parse_key_val<T, U>(
    s: &str,
) -> Result<(T, U), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: std::error::Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[derive(Subcommand, Clone, PartialEq, Eq, Debug, Display)]
#[display(lowercase)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    #[clap(flatten)]
    #[display(inner)]
    General(bpwallet::cli::Command),

    #[clap(flatten)]
    #[display(inner)]
    Debug(DebugCommand),

    /// Prints out list of known RGB schemata
    Schemata,

    /// Prints out list of known RGB contracts
    #[display("contracts")]
    Contracts,

    /// Imports RGB data into the stash: contracts, schema, etc
    #[display("import")]
    Import {
        /// Use BASE64 ASCII armoring for binary data
        #[arg(short)]
        armored: bool,

        /// File with RGB data
        ///
        /// If not provided, assumes `-a` and prints out data to STDOUT
        file: PathBuf,
    },

    /// Exports existing RGB contract
    #[display("export")]
    Export {
        /// Use BASE64 ASCII armoring for binary data
        #[arg(short)]
        armored: bool,

        /// Contract to export
        contract_id: ContractId,

        /// File with RGB data
        ///
        /// If not provided, assumes `-a` and reads the data from STDIN
        file: Option<PathBuf>,
    },

    /// Convert binary RGB file into a text armored version
    #[display("convert")]
    Armor {
        /// File with RGB data
        ///
        /// If not provided, assumes `-a` and reads the data from STDIN
        file: PathBuf,
    },

    /// Reports information about state of a contract
    #[display("state")]
    State {
        /// Show all state, including already spent and not owned by the wallet
        #[arg(short, long)]
        all: bool,

        /// Contract identifier
        contract_id: ContractId,
    },

    /// Print operation history for a contract
    #[display("history")]
    History {
        /// Print detailed information
        #[arg(long)]
        details: bool,

        /// Contract identifier
        contract_id: ContractId,
    },

    /// Display all known UTXOs belonging to this wallet
    Utxos,

    /// Issues new contract
    #[display("issue")]
    Issue {
        /// Issuer identity string
        issuer: Identity,

        /// File containing contract genesis description in YAML format
        contract_path: PathBuf,
    },

    /// Create new invoice
    #[display("invoice")]
    Invoice {
        /// Force address-based invoice
        #[arg(short('a'), long)]
        address_based: bool,

        /// Assignment state name to use for the invoice
        ///
        /// If no state name is provided, it will be detected.
        #[arg(short('s'), long)]
        assignment_name: Option<String>,

        /// Contract identifier
        contract_id: ContractId,

        /// Amount of tokens (in the smallest unit) to transfer
        #[arg(short('m'), long)]
        amount: Option<u64>,

        /// Token index for NFT transfer
        #[arg(long)]
        token_index: Option<TokenIndex>,

        /// Fraction of an NFT token to transfer
        #[arg(long, requires = "token_index")]
        token_fraction: Option<OwnedFraction>,
    },

    /// Prepare PSBT file for transferring RGB assets
    ///
    /// In the most of cases you need to use `transfer` command instead of `prepare` and `consign`.
    #[display("prepare")]
    Prepare {
        /// Encode PSBT as V2
        #[clap(short = '2')]
        v2: bool,

        /// Amount of satoshis which should be paid to the address-based
        /// beneficiary
        #[arg(long, default_value = "2000")]
        sats: u64,

        /// Invoice data
        invoice: RgbInvoice,

        /// Fee
        fee: u64,

        /// Name of PSBT file to save. If not given, prints PSBT to STDOUT
        psbt: Option<PathBuf>,
    },

    /// Prepare consignment for transferring RGB assets
    ///
    /// In the most of the cases you need to use `transfer` command instead of `prepare` and
    /// `consign`.
    #[display("prepare")]
    Consign {
        /// Invoice data
        invoice: RgbInvoice,

        /// Name of PSBT file containing prepared transfer data
        psbt: PathBuf,

        /// File for generated transfer consignment
        consignment: PathBuf,
    },

    /// Transfer RGB assets
    #[display("transfer")]
    Transfer {
        /// Encode PSBT as V2
        #[arg(short = '2')]
        v2: bool,

        /// Amount of satoshis which should be paid to the address-based
        /// beneficiary
        #[arg(long, default_value = "2000")]
        sats: u64,

        /// Invoice data
        invoice: RgbInvoice,

        /// Fee for bitcoin transaction, in satoshis
        #[arg(short, long, default_value = "400")]
        fee: u64,

        /// File for generated transfer consignment
        consignment: PathBuf,

        /// Name of PSBT file to save. If not given, prints PSBT to STDOUT
        psbt: Option<PathBuf>,
    },

    /// Execute a named RGB contract transition
    #[display("transit")]
    Transit {
        /// Encode PSBT as V2
        #[arg(short = '2')]
        v2: bool,

        /// Amount of satoshis which should be paid to the address-based
        /// beneficiary
        #[arg(long, default_value = "2000")]
        sats: u64,

        /// Contract identifier
        contract_id: ContractId,

        /// Transition name as defined in the contract schema
        transition_name: String,

        /// Arguments as key=value pairs, interpreted per the contract interface
        #[arg(short, long = "arg", value_parser = parse_key_val::<String, String>)]
        args: Vec<(String, String)>,

        /// Beneficiary bitcoin address. If provided, an output is created for
        /// this address in the PSBT and the script's "benifery" return is
        /// assigned to it.
        #[arg(short, long)]
        beneficiary: Option<String>,

        /// Fee for bitcoin transaction, in satoshis
        #[arg(short, long, default_value = "400")]
        fee: u64,

        /// File for generated transfer consignment
        consignment: PathBuf,

        /// Name of PSBT file to save. If not given, prints PSBT to STDOUT
        psbt: Option<PathBuf>,
    },

    /// Inspects any RGB data file
    #[display("inspect")]
    Inspect {
        /// RGB file to inspect
        file: PathBuf,

        /// Path to save the dumped data. If not given, prints PSBT to STDOUT.
        path: Option<PathBuf>,

        /// Export using directory format for the compound bundles
        #[clap(long, requires("path"))]
        dir: bool,
    },

    /// Reconstructs consignment from a YAML file
    #[display("reconstruct")]
    #[clap(hide = true)]
    Reconstruct {
        #[clap(long)]
        contract: bool,

        /// RGB file with the consignment YAML data
        src: PathBuf,

        /// Path for the resulting consignment file. If not given, prints the
        /// consignment to STDOUT.
        dst: Option<PathBuf>,
    },

    /// Debug-dump all stash and inventory data
    #[display("dump")]
    Dump {
        /// Directory to put the dump into
        #[arg(default_value = "./rgb-dump")]
        root_dir: String,
    },

    /// Validate transfer consignment
    #[display("validate")]
    Validate {
        /// File with the transfer consignment
        file: PathBuf,
    },

    /// Validate transfer consignment & accept to the stash
    #[display("accept")]
    Accept {
        /// Force accepting consignments with non-mined terminal witness
        #[arg(short, long)]
        force: bool,

        /// File with the transfer consignment
        file: PathBuf,
    },
}

#[derive(Subcommand, Clone, PartialEq, Eq, Debug, Display)]
#[display(lowercase)]
#[clap(hide = true)]
pub enum DebugCommand {
    /// List known tapret tweaks for a wallet
    Taprets,
}

impl Exec for RgbArgs {
    type Error = WalletError;
    const CONF_FILE_NAME: &'static str = "rgb.toml";

    fn exec(self, config: Config, _name: &'static str) -> Result<(), WalletError> {
        match &self.command {
            Command::General(cmd) => {
                self.inner.translate(cmd).exec(config, "rgb")?;
            }
            Command::Utxos => {
                self.inner
                    .translate(&BpCommand::Balance {
                        addr: true,
                        utxo: true,
                    })
                    .exec(config, "rgb")?;
            }

            Command::Debug(DebugCommand::Taprets) => {
                let stock = self.rgb_stock()?;
                for (witness_id, tapret) in stock.as_stash_provider().taprets()? {
                    println!("{witness_id}\t{tapret}");
                }
            }
            Command::Schemata => {
                let stock = self.rgb_stock()?;
                for info in stock.schemata()? {
                    print!("{info}");
                }
            }
            Command::Contracts => {
                let stock = self.rgb_stock()?;
                for info in stock.contracts()? {
                    print!("{info}");
                }
            }

            Command::History {
                contract_id,
                details,
            } => {
                let wallet = self.rgb_wallet(&config)?;
                let mut history = wallet.history(*contract_id)?;
                history.sort_by_key(|op| op.witness.map(|w| w.ord).unwrap_or(WitnessOrd::Archived));
                if *details {
                    println!("Operation\tValue    \tState\t{:78}\tWitness", "Seal");
                } else {
                    println!("Operation\tValue    \t{:78}\tWitness", "Seal");
                }
                for ContractOp {
                    direction,
                    ty,
                    opids,
                    state,
                    to,
                    witness,
                } in history
                {
                    print!("{:9}\t", direction.to_string());
                    if let AllocatedState::Amount(amount) = state {
                        print!("{: >9}", amount.as_u64());
                    } else {
                        print!("{state:>9}");
                    }
                    if *details {
                        print!("\t{ty}");
                    }
                    println!(
                        "\t{}\t{}",
                        to.first().expect("at least one receiver is always present"),
                        witness
                            .map(|info| format!("{} ({})", info.id, info.ord))
                            .unwrap_or_else(|| s!("~"))
                    );
                    if *details {
                        println!(
                            "\topid={}",
                            opids
                                .iter()
                                .map(OpId::to_string)
                                .collect::<Vec<_>>()
                                .join("\n\topid=")
                        )
                    }
                }
            }

            Command::Import { armored, file } => {
                let mut stock = self.rgb_stock()?;
                assert!(!armored, "importing armored files is not yet supported");
                // TODO: Support armored files
                let content = UniversalFile::load_file(file)?;
                match content {
                    UniversalFile::Kit(kit) => {
                        let id = kit.kit_id();
                        eprintln!("Importing kit {id}:");
                        let mut schema_names = map![];
                        for schema in &kit.schemata {
                            let schema_id = schema.schema_id();
                            schema_names.insert(schema_id, &schema.name);
                            eprintln!("- schema {} {:-}", schema.name, schema_id);
                        }
                        for lib in &kit.scripts {
                            eprintln!("- script library {}", lib.id());
                        }
                        eprintln!("- strict types: {} definitions", kit.types.len());
                        let kit = kit.validate().map_err(|err| format!("{err:?}"))?;
                        stock.import_kit(kit)?;
                        eprintln!("Kit is imported");
                    }
                    UniversalFile::Contract(contract) => {
                        let id = contract.consignment_id();
                        eprintln!("Importing consignment {id}:");
                        let resolver = self.resolver()?;
                        eprint!("- validating the contract {} ... ", contract.contract_id());
                        let validation_config = ValidationConfig {
                            chain_net: self.chain_net(),
                            trusted_typesystem: stock.as_stash_provider().type_system()?.clone(),
                            ..Default::default()
                        };
                        let contract =
                            contract
                                .validate(&resolver, &validation_config)
                                .map_err(|status| {
                                    eprintln!("failure");
                                    status.to_string()
                                })?;
                        eprintln!("success");
                        stock.import_contract(contract, &resolver)?;
                        eprintln!("Consignment is imported");
                    }
                    UniversalFile::Transfer(_) => {
                        return Err(s!("use `validate` and `accept` commands to work with \
                                       transfer consignments")
                        .into());
                    }
                }
            }
            Command::Export {
                armored: _,
                contract_id,
                file,
            } => {
                let stock = self.rgb_stock()?;
                let contract = stock
                    .export_contract(*contract_id)
                    .map_err(|err| err.to_string())?;
                if let Some(file) = file {
                    // TODO: handle armored flag
                    contract.save_file(file)?;
                    eprintln!("Contract {contract_id} exported to '{}'", file.display());
                } else {
                    println!("{contract}");
                }
            }

            Command::Armor { file } => {
                let content = UniversalFile::load_file(file)?;
                println!("{content}");
            }

            Command::State { contract_id, all } => {
                let stock_path = self.general.base_dir();
                let stock = self.load_stock(stock_path.clone())?;

                enum StockOrWallet {
                    Stock(Box<Stock>),
                    Wallet(Box<RgbWallet<Wallet<XpubDerivable, RgbDescr<XpubDerivable>>>>),
                }
                impl StockOrWallet {
                    fn stock(&self) -> &Stock {
                        match self {
                            StockOrWallet::Stock(stock) => stock,
                            StockOrWallet::Wallet(wallet) => wallet.stock(),
                        }
                    }
                }

                let stock_wallet = match self.rgb_wallet_from_stock(&config, stock) {
                    Ok(wallet) => StockOrWallet::Wallet(Box::new(wallet)),
                    Err(_) => StockOrWallet::Stock(Box::new(self.load_stock(stock_path)?)),
                };

                let filter = match stock_wallet {
                    StockOrWallet::Wallet(ref wallet) if *all => Filter::WalletAll(wallet),
                    StockOrWallet::Wallet(ref wallet) => Filter::Wallet(wallet),
                    StockOrWallet::Stock(_) => {
                        println!("no wallets found");
                        Filter::NoWallet
                    }
                };

                let contract = stock_wallet.stock().contract_data(*contract_id)?;

                println!("\nGlobal:");
                for global_details in contract.schema.global_types.values() {
                    let values = contract.global(global_details.name.clone());
                    for val in values {
                        println!("  {} := {}", global_details.name, val);
                    }
                }

                enum Filter<'w> {
                    Wallet(&'w RgbWallet<Wallet<XpubDerivable, RgbDescr<XpubDerivable>>>),
                    WalletAll(&'w RgbWallet<Wallet<XpubDerivable, RgbDescr<XpubDerivable>>>),
                    NoWallet,
                }
                impl AssignmentsFilter for Filter<'_> {
                    fn should_include(
                        &self,
                        outpoint: impl Into<Outpoint>,
                        id: Option<Txid>,
                    ) -> bool {
                        match self {
                            Filter::Wallet(wallet) => wallet
                                .wallet()
                                .filter_unspent()
                                .should_include(outpoint, id),
                            _ => true,
                        }
                    }
                }
                impl Filter<'_> {
                    fn comment(&self, outpoint: Outpoint) -> &'static str {
                        let outpoint = outpoint_bitcoin_to_bp(outpoint);
                        match self {
                            Filter::Wallet(rgb) if rgb.wallet().is_unspent(outpoint) => "",
                            Filter::WalletAll(rgb) if rgb.wallet().is_unspent(outpoint) => {
                                "-- unspent"
                            }
                            Filter::WalletAll(rgb) if rgb.wallet().has_outpoint(outpoint) => {
                                "-- spent"
                            }
                            _ => "-- third-party",
                        }
                    }
                }

                println!("\nOwned:");
                fn witness<S: KnownState>(
                    allocation: &OutputAssignment<S>,
                    contract: &ContractData<MemContract<&MemContractState>>,
                ) -> String {
                    allocation
                        .witness
                        .and_then(|w| contract.witness_info(w))
                        .map(|info| format!("{} ({})", info.id, info.ord))
                        .unwrap_or_else(|| s!("~"))
                }
                for details in contract.schema.owned_types.values() {
                    println!("  State      \t{:78}\tWitness", "Seal");
                    println!("  {}:", details.name);
                    if let Ok(allocations) = contract.fungible(details.name.clone(), &filter) {
                        for allocation in allocations {
                            println!(
                                "    {: >9}\t{}\t{} {}",
                                allocation.state.value(),
                                allocation.seal,
                                witness(&allocation, &contract),
                                filter.comment(allocation.seal.to_outpoint())
                            );
                        }
                    }
                    if let Ok(allocations) = contract.data(details.name.clone(), &filter) {
                        for allocation in allocations {
                            println!(
                                "    {: >9}\t{}\t{} {}",
                                allocation.state,
                                allocation.seal,
                                witness(&allocation, &contract),
                                filter.comment(allocation.seal.to_outpoint())
                            );
                        }
                    }
                    if let Ok(allocations) = contract.rights(details.name.clone(), &filter) {
                        for allocation in allocations {
                            println!(
                                "    {: >9}\t{}\t{} {}",
                                "right",
                                allocation.seal,
                                witness(&allocation, &contract),
                                filter.comment(allocation.seal.to_outpoint())
                            );
                        }
                    }
                }
            }
            Command::Issue {
                issuer,
                contract_path,
            } => {
                let mut stock = self.rgb_stock()?;

                let file = fs::File::open(contract_path)?;

                let code = serde_yaml::from_reader::<_, serde_yaml::Value>(file)?;

                let code = code
                    .as_mapping()
                    .expect("invalid YAML root-level structure");

                let schema_id_str = code
                    .get("schema")
                    .expect("must specify a schema")
                    .as_str()
                    .expect("schema must be a string");

                let schema_id = SchemaId::from_str(schema_id_str)?;
                let schema = stock.schema(schema_id)?;

                let mut builder =
                    stock.contract_builder(issuer.clone(), schema_id, self.chain_net())?;
                let types = builder.type_system().clone();
                if let Some(globals) = code.get("globals") {
                    for (name, val) in globals
                        .as_mapping()
                        .expect("invalid YAML: globals must be an mapping")
                    {
                        let name = name
                            .as_str()
                            .expect("invalid YAML: global name must be a string");
                        // Workaround for borrow checker:
                        let name = FieldName::try_from(name.to_owned()).expect("invalid type name");
                        let (type_id, global_details) = schema.global(name);
                        let sem_id = global_details.global_state_schema.sem_id;
                        let val = StrictVal::from(val.clone());
                        let typed_val = types
                            .typify(val, sem_id)
                            .expect("global type doesn't match type definition");

                        #[allow(deprecated)]
                        let serialized = types
                            .strict_serialize_type::<MAX16>(&typed_val)
                            .expect("internal error");
                        builder = builder
                            .add_global_state_raw(*type_id, serialized)
                            .expect("invalid global state data");
                    }
                }

                if let Some(assignments) = code.get("assignments") {
                    for (name, val) in assignments
                        .as_mapping()
                        .expect("invalid YAML: assignments must be an mapping")
                    {
                        let name = name
                            .as_str()
                            .expect("invalid YAML: assignments name must be a string");
                        // Workaround for borrow checker:
                        let name = FieldName::try_from(name.to_owned()).expect("invalid type name");
                        let (type_id, assignment_details) = schema.assignment(name);
                        let state_schema = assignment_details.owned_state_schema;

                        let assign = val.as_mapping().expect("an assignment must be a mapping");
                        let seal = assign
                            .get("seal")
                            .expect("assignment doesn't provide seal information")
                            .as_str()
                            .expect("seal must be a string");
                        let seal = OutputSeal::from_str(seal).expect("invalid seal definition");
                        let seal = GenesisSeal::new_random(seal.txid, seal.vout);

                        match state_schema.state_type() {
                            StateType::Void => todo!(),
                            StateType::Fungible => {
                                let amount = assign
                                    .get("amount")
                                    .expect("owned state must be a fungible amount")
                                    .as_u64()
                                    .expect("fungible state must be an integer");
                                let seal = BuilderSeal::Revealed(seal);
                                builder = builder
                                    .add_fungible_state_raw(*type_id, seal, amount)
                                    .expect("invalid global state data");
                            }
                            StateType::Structured => todo!(),
                        }
                    }
                }

                let contract = builder.issue_contract()?;
                let id = contract.contract_id();
                stock.import_contract(contract, &ContractIssueResolver)?;
                eprintln!(
                    "A new contract {id} is issued and added to the stash.\nUse `export` command \
                     to export the contract."
                );
            }
            Command::Invoice {
                address_based,
                assignment_name,
                contract_id,
                amount,
                token_index,
                token_fraction,
            } => {
                let mut wallet = self.rgb_wallet(&config)?;

                let outpoint = wallet.wallet().coinselect(Sats::ZERO, |_| true).next();
                let network = wallet.wallet().network();
                let beneficiary = match (address_based, outpoint) {
                    (false, None) => {
                        return Err(WalletError::Custom(s!(
                            "blinded invoice requested but no suitable outpoint is available"
                        )));
                    }
                    (true, _) => {
                        let addr = wallet
                            .wallet()
                            .addresses(wallet.wallet().default_keychain())
                            .next()
                            .expect("no addresses left")
                            .addr;
                        let address_payload = address_payload_bitcoin_from_script_pubkey(
                            &addr.payload.script_pubkey(),
                        );
                        Beneficiary::WitnessVout(Pay2Vout::new(address_payload), None)
                    }
                    (_, Some(outpoint)) => {
                        let outpoint = outpoint_bp_to_bitcoin(outpoint);
                        let seal = GraphSeal::new_random(outpoint.txid, outpoint.vout);
                        wallet.stock_mut().store_secret_seal(seal)?;
                        Beneficiary::BlindedSeal(seal.to_secret_seal())
                    }
                };

                let network = network_bp_to_bitcoin(network);
                let mut builder = RgbInvoiceBuilder::new(XChainNet::bitcoin(network, beneficiary))
                    .set_contract(*contract_id);

                let state_type = match (amount, token_index.map(|i| (i, token_fraction))) {
                    (Some(amount), None) => {
                        builder = builder.set_amount_raw(*amount);
                        StateType::Fungible
                    }
                    (None, Some((index, fraction))) => {
                        builder = builder.set_allocation_raw(Allocation::with(
                            index,
                            fraction.unwrap_or(OwnedFraction::from(0)),
                        ));
                        StateType::Structured
                    }
                    _ => {
                        return Err(WalletError::Invoicing(s!(
                            "only amount or token data should be provided"
                        )))
                    }
                };

                let mut ass_name = assignment_name
                    .clone()
                    .map(FieldName::try_from)
                    .transpose()
                    .map_err(|e| {
                        WalletError::Invoicing(format!("invalid assignment name - {e}"))
                    })?;

                if let Ok(contract) = wallet.stock().contract_data(*contract_id) {
                    if let Some(ref assignment_name) = ass_name {
                        let (_, details) = contract.schema.assignment(assignment_name.clone());
                        if details.owned_state_schema.state_type() != state_type {
                            return Err(WalletError::Invoicing(s!(
                                "invalid assignment name for state type"
                            )));
                        }
                    } else {
                        let assignment_types =
                            contract.schema.assignment_types_for_state(state_type);
                        if assignment_types.len() == 1 {
                            ass_name = Some(
                                contract
                                    .schema
                                    .assignment_name(*assignment_types[0])
                                    .clone(),
                            );
                        } else {
                            return Err(WalletError::Invoicing(s!(
                                "cannot detect a default assignment type"
                            )));
                        }
                    }
                }

                if let Some(name) = ass_name {
                    builder = builder.set_assignment_name(name);
                }

                let invoice = builder.finish();
                println!("{invoice}");
            }
            Command::Prepare {
                v2,
                invoice,
                fee,
                sats,
                psbt: psbt_file,
            } => {
                let mut wallet = self.rgb_wallet(&config)?;
                // TODO: Support lock time and RBFs
                let params = TransferParams::with(*fee, *sats);

                let (psbt, _) = wallet
                    .construct_psbt::<PropKey, Output>(invoice, params)
                    .map_err(|err| err.to_string())?;

                let ver = if *v2 { PsbtVer::V2 } else { PsbtVer::V0 };
                match psbt_file {
                    Some(file_name) => {
                        let mut psbt_file = File::create(file_name)?;
                        psbt.encode(ver, &mut psbt_file)?;
                    }
                    None => match ver {
                        PsbtVer::V0 => println!("{psbt}"),
                        PsbtVer::V2 => println!("{psbt:#}"),
                    },
                }
            }
            Command::Consign {
                invoice,
                psbt: psbt_name,
                consignment: out_file,
            } => {
                let mut wallet = self.rgb_wallet(&config)?;
                let mut psbt_file = File::open(psbt_name)?;
                let mut psbt = Psbt::decode(&mut psbt_file)?;
                let transfer = wallet
                    .transfer(invoice, &mut psbt, None)
                    .map_err(|err| err.to_string())?;
                let mut psbt_file = File::create(psbt_name)?;
                psbt.encode(psbt.version, &mut psbt_file)?;
                transfer.save_file(out_file)?;
            }
            Command::Transfer {
                v2,
                invoice,
                fee,
                sats,
                psbt: psbt_file,
                consignment: out_file,
            } => {
                let mut wallet = self.rgb_wallet(&config)?;
                // TODO: Support lock time and RBFs
                let params = TransferParams::with(*fee, *sats);

                let (mut psbt, _, transfer) = wallet
                    .pay::<PropKey, Output>(invoice, params)
                    .map_err(|err| err.to_string())?;

                transfer.save_file(out_file)?;

                psbt.version = if *v2 { PsbtVer::V2 } else { PsbtVer::V0 };
                match psbt_file {
                    Some(file_name) => {
                        let mut psbt_file = File::create(file_name)?;
                        psbt.encode(psbt.version, &mut psbt_file)?;
                    }
                    None => println!("{psbt}"),
                }
            }
            Command::Transit {
                v2,
                contract_id,
                transition_name,
                args,
                beneficiary,
                fee,
                sats,
                psbt: psbt_file,
                consignment: out_file,
            } => {
                use std::collections::HashMap;
                use std::env;

                use psrgbt::{RgbOutExt, RgbPsbtExt};
                use rgb::containers::Batch;
                use rgb::contract::AllocatedState;
                use rgb::invoice::Amount;
                use rgb::pay::{build_extra_transitions, create_change_output_seal};
                use rgb::scripts::{
                    add_transition_states, generate_transition_parameters_from_args, get_interface,
                    main_assignment_type_from_returns_abi, run_script_with_contract_state,
                };
                use rgb::validation::WitnessOrdProvider;
                use rgb::vm::WitnessOrd;
                use rgb::WalletProvider as _;

                let mut wallet = self.rgb_wallet(&config)?;
                let params = TransferParams::with(*fee, *sats);
                let transit_debug = env::var("RGB_TRANSIT_DEBUG")
                    .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                    .unwrap_or(false);

                let export = wallet
                    .stock()
                    .export_contract(*contract_id)
                    .map_err(|e| e.to_string())?;
                let (interface, interface_libid) = get_interface(&export).map_err(|e| e.to_string())?;

                let (&transition_type, transition_details) = export
                    .schema
                    .transitions
                    .iter()
                    .find(|(_, d)| d.name.to_string() == *transition_name)
                    .ok_or_else(|| {
                        WalletError::Custom(format!(
                            "transition '{}' not found in schema",
                            transition_name
                        ))
                    })?;

                let transition_interface =
                    interface.get(transition_name.as_str()).ok_or_else(|| {
                        WalletError::Custom(format!(
                            "interface JSON does not contain '{}'",
                            transition_name
                        ))
                    })?;

                // let bz_transition_type =
                //     TransitionType::with(u16::from(transition_type) + 0x8000u16);
                // let bl_transition_details = export
                //     .schema
                //     .transitions
                //     .get(&bz_transition_type)
                //     .ok_or_else(|| {
                //         WalletError::Custom(format!(
                //             "bizlogic transition 0x{:04x} not found in schema",
                //             u16::from(bz_transition_type)
                //         ))
                //     })?;

                // let bl_validator = bl_transition_details
                //     .transition_schema
                //     .validator
                //     .ok_or_else(|| {
                //         WalletError::Custom(
                //             "bizlogic transition has no validator script".to_string(),
                //         )
                //     })?;
                let transition_script = transition_interface.get("script").unwrap();
                let script_pos = transition_script.get("position").unwrap().as_u64().unwrap() as u16;

                let close_method = wallet.wallet().close_method();

                let (prev_outputs, default_assignment_type) = {
                    let filter = wallet.wallet().filter_unspent();
                    let contract = wallet
                        .stock()
                        .contract_data(*contract_id)
                        .map_err(|e| e.to_string())?;

                    let default_at = *contract
                        .schema
                        .default_assignment
                        .as_ref()
                        .unwrap_or(contract.schema.owned_types.keys().next().unwrap());

                    let mut prev_outputs = std::collections::BTreeSet::new();
                    for assignment_type in contract.schema.owned_types.keys().copied() {
                        for a in contract
                            .fungible_raw(assignment_type, &filter)
                            .map_err(|e| e.to_string())?
                        {
                            prev_outputs.insert(a.seal);
                        }
                        for a in contract
                            .data_raw(assignment_type, &filter)
                            .map_err(|e| e.to_string())?
                        {
                            prev_outputs.insert(a.seal);
                        }
                        for a in contract
                            .rights_raw(assignment_type, &filter)
                            .map_err(|e| e.to_string())?
                        {
                            prev_outputs.insert(a.seal);
                        }
                    }
                    (prev_outputs, default_at)
                };

                if prev_outputs.is_empty() {
                    return Err(WalletError::Custom(
                        "no unspent state found for this contract".to_string(),
                    ));
                }
                if transit_debug {
                    eprintln!(
                        "[transit-debug] selected prev outputs: {}",
                        prev_outputs.len()
                    );
                }

                let prev_outpoints = prev_outputs
                    .iter()
                    .map(|o| Outpoint::new(o.txid, o.vout.to_u32()));

                let (mut psbt, meta) = if let Some(addr) = beneficiary {
                    wallet
                        .wallet_mut()
                        .create_psbt_with_address(addr, close_method, prev_outpoints, params)
                        .map_err(|e| e.to_string())?
                } else {
                    wallet
                        .wallet_mut()
                        .create_psbt_no_beneficiary(close_method, prev_outpoints, params)
                        .map_err(|e| e.to_string())?
                };

                let beneficiary_seal = meta.beneficiary_vout.map(|vout| {
                    use rgb::rgbcore::secp256k1::rand;
                    rgb::containers::BuilderSeal::Revealed(GraphSeal::with_blinded_vout(
                        vout,
                        rand::random(),
                    ))
                });

                let abi = transition_interface
                    .get("returns")
                    .ok_or_else(|| {
                        WalletError::Custom(
                            "interface transition must contain 'returns'".to_string(),
                        )
                    })?
                    .as_array()
                    .ok_or_else(|| {
                        WalletError::Custom("'returns' must be a JSON array".to_string())
                    })?;
                let main_assignment_type = main_assignment_type_from_returns_abi(
                    abi.as_slice(),
                    default_assignment_type,
                )
                .map_err(|e| WalletError::Custom(e.to_string()))?;

                let mut main_builder = wallet
                    .stock()
                    .transition_builder_raw(*contract_id, transition_type)
                    .map_err(|e| e.to_string())?;

                let mut sum_inputs = Amount::ZERO;
                let mut input_type_counts: std::collections::BTreeMap<rgb::AssignmentType, u16> =
                    std::collections::BTreeMap::new();
                for (_output, list) in wallet
                    .stock()
                    .contract_assignments_for(*contract_id, prev_outputs.iter().copied())
                    .map_err(|e| e.to_string())?
                {
                    for (opout, state) in list {
                        main_builder = main_builder.add_input(opout, state.clone())?;
                        *input_type_counts.entry(opout.ty).or_insert(0) += 1;
                        if opout.ty != main_assignment_type {
                            let seal = create_change_output_seal(opout.ty, &meta)
                                .map_err(|e| e.to_string())?;
                            main_builder =
                                main_builder.add_owned_state_raw(opout.ty, seal, state)?;
                        } else if let AllocatedState::Amount(value) = state {
                            sum_inputs += Amount::from(value);
                        } else {
                            let seal = create_change_output_seal(opout.ty, &meta)
                                .map_err(|e| e.to_string())?;
                            main_builder =
                                main_builder.add_owned_state_raw(opout.ty, seal, state)?;
                        }
                    }
                }
                if transit_debug {
                    let mut pairs: Vec<String> = input_type_counts
                        .iter()
                        .map(|(ty, c)| format!("{}={}", u16::from(*ty), c))
                        .collect();
                    pairs.sort();
                    eprintln!(
                        "[transit-debug] input assignment counts: {}",
                        if pairs.is_empty() {
                            "(empty)".to_string()
                        } else {
                            pairs.join(", ")
                        }
                    );
                }
                for (type_id, occ) in &transition_details.transition_schema.inputs {
                    let found = input_type_counts.get(type_id).copied().unwrap_or(0);
                    if let Err(mismatch) = occ.check(found) {
                        return Err(WalletError::Custom(format!(
                            "insufficient transition inputs for type {}: required min={}, max={}, found={}",
                            u16::from(*type_id),
                            mismatch.min,
                            mismatch.max,
                            mismatch.found
                        )));
                    }
                }

                let args_map: HashMap<String, String> = args.iter().cloned().collect();

                let parameters = transition_interface.get("parameters").ok_or_else(|| {
                    WalletError::Custom(
                        "interface transition must contain 'parameters'".to_string(),
                    )
                })?;
                let script_params =
                    generate_transition_parameters_from_args(parameters, &args_map, sum_inputs, &prev_outputs)
                        .map_err(|e| e.to_string())?;

                let script_contract = wallet
                    .stock()
                    .contract_data(*contract_id)
                    .map_err(|e| e.to_string())?;
                let outputs =
                    run_script_with_contract_state(
                        &export,
                        interface_libid,
                        script_pos,
                        script_params,
                        script_contract.state,
                    )
                        .map_err(|e| e.to_string())?;

                let change_seal =
                    create_change_output_seal(main_assignment_type, &meta)
                        .map_err(|e| e.to_string())?;
                let ben_seal = beneficiary_seal.as_ref().unwrap_or(&change_seal);
                main_builder = add_transition_states(
                    &export,
                    abi,
                    &outputs,
                    main_builder,
                    ben_seal,
                    &change_seal,
                )
                .map_err(|e| e.to_string())?;

                let contract = wallet
                    .stock()
                    .contract_data(*contract_id)
                    .map_err(|e| e.to_string())?;
                main_builder = rgb::pay::apply_transition_schema_globals_from_contract_state(
                    main_builder,
                    &contract,
                    &transition_details.transition_schema,
                )
                .map_err(|e| e.to_string())?;

                let transition = main_builder.complete_transition()?;

                let extras =
                    build_extra_transitions(wallet.stock(), *contract_id, &prev_outputs, &meta)
                        .map_err(|e| e.to_string())?;

                let mut batch = Batch {
                    main: transition,
                    extras,
                };
                batch.set_priority(u64::MAX);

                psbt.set_rgb_close_method(close_method);
                psbt.set_as_unmodifiable();
                psbt.rgb_embed(batch).map_err(|e| e.to_string())?;

                let fascia = psbt.rgb_commit().map_err(|e| e.to_string())?;
                {
                    use rgb::rgbcore::dbc::tapret::TapretProof;
                    use rgb::rgbcore::dbc::Proof as _;
                    if matches!(
                        fascia.seal_witness().dbc_proof.method(),
                        rgb::rgbcore::seals::txout::CloseMethod::TapretFirst
                    ) {
                        if psbt.rgb_tapret_host_on_change() {
                            let output = psbt
                                .dbc_output::<TapretProof>()
                                .ok_or_else(|| {
                                    WalletError::Custom(
                                        "no taproot output for tapret".to_string(),
                                    )
                                })?;
                            let terminal: psrgbt::Terminal = output
                                .terminal_derivation()
                                .ok_or_else(|| {
                                    WalletError::Custom("inconclusive derivation".to_string())
                                })?
                                .into();
                            let tapret_commitment = output
                                .tapret_commitment()
                                .map_err(|e| WalletError::Custom(e.to_string()))?;
                            wallet
                                .wallet_mut()
                                .add_tapret_tweak(terminal, tapret_commitment)
                                .map_err(|e| WalletError::Custom(e.to_string()))?;
                        }
                    }
                }

                let witness_id = psbt.get_txid();

                struct FasciaResolver {
                    witness_id: rgb::Txid,
                }
                impl WitnessOrdProvider for FasciaResolver {
                    fn witness_ord(
                        &self,
                        witness_id: rgb::Txid,
                    ) -> Result<WitnessOrd, rgb::validation::WitnessResolverError> {
                        assert_eq!(witness_id, self.witness_id);
                        Ok(WitnessOrd::Tentative)
                    }
                }

                wallet
                    .stock_mut()
                    .consume_fascia(fascia, FasciaResolver { witness_id })
                    .map_err(|e| e.to_string())?;

                // `Stock::transfer` seeds opids via `opouts_by_outputs` (and secret seal terminals).
                // The mem index does not fill `public_opouts`, so seeds must list every witness
                // output that may own the new transition's state (beneficiary and/or change).
                let mut transfer_output_seals: Vec<OutputSeal> = Vec::new();
                let mut push_seal = |vout: u32| {
                    let seal = OutputSeal::new(Outpoint::new(witness_id, vout));
                    if !transfer_output_seals.contains(&seal) {
                        transfer_output_seals.push(seal);
                    }
                };
                if let Some(vout) = meta.beneficiary_vout {
                    push_seal(vout);
                }
                if let Some(vout) = meta.change_vout {
                    push_seal(vout);
                }
                if transfer_output_seals.is_empty() {
                    return Err(WalletError::Custom(
                        "cannot build transfer consignment: missing beneficiary_vout and change_vout for witness RGB outputs".into(),
                    ));
                }
                let transfer = wallet
                    .stock()
                    .transfer(
                        *contract_id,
                        transfer_output_seals,
                        vec![],
                        [],
                        Some(witness_id),
                    )
                    .map_err(|e| e.to_string())?;

                transfer.save_file(out_file)?;

                psbt.version = if *v2 { PsbtVer::V2 } else { PsbtVer::V0 };
                match psbt_file {
                    Some(file_name) => {
                        let mut psbt_file = File::create(file_name)?;
                        psbt.encode(psbt.version, &mut psbt_file)?;
                    }
                    None => println!("{psbt}"),
                }
            }
            Command::Inspect { file, dir, path } => {
                #[derive(Clone, Debug)]
                #[derive(Serialize, Deserialize)]
                #[serde(crate = "serde_crate", rename_all = "camelCase")]
                pub struct ConsignmentInspection {
                    version: ContainerVer,
                    transfer: bool,
                    terminals: SmallOrdMap<BundleId, SecretSeals>,
                }

                let content = UniversalFile::load_file(file)?;
                let consignment = match content {
                    UniversalFile::Contract(contract) if *dir => Some(contract),
                    UniversalFile::Transfer(transfer) if *dir => Some(transfer.into_contract()),
                    content => {
                        let s = serde_yaml::to_string(&content).expect("unable to present as YAML");
                        match path {
                            None => println!("{s}"),
                            Some(path) => fs::write(path, s)?,
                        }
                        None
                    }
                };
                if let Some(consignment) = consignment {
                    let mut map = map![
                        s!("genesis.yaml") => serde_yaml::to_string(&consignment.genesis)?,
                        s!("schema.yaml") => serde_yaml::to_string(&consignment.schema)?,
                        s!("bundles.yaml") => serde_yaml::to_string(&consignment.bundles)?,
                        s!("types.sty") => consignment.types.to_string(),
                    ];
                    for lib in consignment.scripts {
                        let mut buf = Vec::new();
                        lib.print_disassemble::<RgbIsa<MemContract>>(&mut buf)?;
                        map.insert(format!("{}.aluasm", lib.id().to_baid64_mnemonic()), unsafe {
                            String::from_utf8_unchecked(buf)
                        });
                    }
                    let contract = ConsignmentInspection {
                        version: consignment.version,
                        transfer: consignment.transfer,
                        terminals: consignment.terminals,
                    };
                    map.insert(s!("consignment-meta.yaml"), serde_yaml::to_string(&contract)?);
                    let path = path.as_ref().expect("required by clap");
                    fs::create_dir_all(path)?;
                    for (file, value) in map {
                        fs::write(format!("{}/{file}", path.display()), value)?;
                    }
                }
            }
            Command::Reconstruct {
                contract: false,
                src,
                dst,
            } => {
                let file = File::open(src)?;
                let transfer: Transfer = serde_yaml::from_reader(&file)?;
                match dst {
                    None => println!("{transfer}"),
                    Some(dst) => {
                        transfer.save_file(dst)?;
                    }
                }
            }
            Command::Reconstruct {
                contract: true,
                src,
                dst,
            } => {
                let file = File::open(src)?;
                let contract: Contract = serde_yaml::from_reader(&file)?;
                match dst {
                    None => println!("{contract}"),
                    Some(dst) => {
                        contract.save_file(dst)?;
                    }
                }
            }
            Command::Dump { root_dir } => {
                let stock = self.rgb_stock()?;

                fs::remove_dir_all(root_dir).ok();
                fs::create_dir_all(format!("{root_dir}/stash/schemata"))?;
                fs::create_dir_all(format!("{root_dir}/stash/geneses"))?;
                fs::create_dir_all(format!("{root_dir}/stash/bundles"))?;
                fs::create_dir_all(format!("{root_dir}/stash/witnesses"))?;
                fs::create_dir_all(format!("{root_dir}/state"))?;
                fs::create_dir_all(format!("{root_dir}/index"))?;

                // Stash
                for (id, schema) in stock.as_stash_provider().debug_schemata() {
                    fs::write(
                        format!("{root_dir}/stash/schemata/{}.{id:-#}.yaml", schema.name),
                        serde_yaml::to_string(&schema)?,
                    )?;
                }
                for (id, genesis) in stock.as_stash_provider().debug_geneses() {
                    fs::write(
                        format!("{root_dir}/stash/geneses/{id:-}.yaml"),
                        serde_yaml::to_string(genesis)?,
                    )?;
                }
                for (id, bundle) in stock.as_stash_provider().debug_bundles() {
                    fs::write(
                        format!("{root_dir}/stash/bundles/{id}.yaml"),
                        serde_yaml::to_string(bundle)?,
                    )?;
                }
                for (id, witness) in stock.as_stash_provider().debug_witnesses() {
                    fs::write(
                        format!("{root_dir}/stash/witnesses/{id}.yaml"),
                        serde_yaml::to_string(witness)?,
                    )?;
                }
                fs::write(
                    format!("{root_dir}/stash/seal-secret.yaml"),
                    serde_yaml::to_string(stock.as_stash_provider().debug_secret_seals())?,
                )?;

                // State
                fs::write(
                    format!("{root_dir}/state/witnesses.yaml"),
                    serde_yaml::to_string(stock.as_state_provider().debug_witnesses())?,
                )?;
                for (id, state) in stock.as_state_provider().debug_contracts() {
                    fs::write(
                        format!("{root_dir}/state/{id:-}.yaml"),
                        serde_yaml::to_string(state)?,
                    )?;
                }

                // Index
                fs::write(
                    format!("{root_dir}/index/op-to-bundle.yaml"),
                    serde_yaml::to_string(stock.as_index_provider().debug_op_bundle_index())?,
                )?;
                fs::write(
                    format!("{root_dir}/index/bundle-to-contract.yaml"),
                    serde_yaml::to_string(stock.as_index_provider().debug_bundle_contract_index())?,
                )?;
                fs::write(
                    format!("{root_dir}/index/bundle-to-witness.yaml"),
                    serde_yaml::to_string(stock.as_index_provider().debug_bundle_witness_index())?,
                )?;
                fs::write(
                    format!("{root_dir}/index/contracts.yaml"),
                    serde_yaml::to_string(stock.as_index_provider().debug_contract_index())?,
                )?;
                fs::write(
                    format!("{root_dir}/index/terminals.yaml"),
                    serde_yaml::to_string(stock.as_index_provider().debug_terminal_index())?,
                )?;
                eprintln!("Dump is successfully generated and saved to '{root_dir}'");
            }
            Command::Validate { file } => {
                let stock = self.rgb_stock()?;
                let mut resolver = self.resolver()?;
                let consignment = Transfer::load_file(file)?;
                resolver.add_consignment_txes(&consignment);
                let validation_config = ValidationConfig {
                    chain_net: self.chain_net(),
                    trusted_typesystem: consignment.types.clone(),
                    ..Default::default()
                };
                let state = stock.as_state_provider();
                let extra_states = Some(vec![std::slice::from_ref(state)]);
                let validated_consignment = consignment.validate_extra_states(
                    &resolver,
                    &validation_config,
                    extra_states,
                )?;
                let status = validated_consignment.validation_status();
                if status.validity() == Validity::Valid {
                    eprintln!("The provided consignment is valid")
                } else {
                    eprintln!("{status}");
                }
            }
            Command::Accept { force: _, file } => {
                // TODO: Ensure we properly handle unmined terminal transactions
                let mut stock = self.rgb_stock()?;
                let mut resolver = self.resolver()?;
                let transfer = Transfer::load_file(file)?;
                resolver.add_consignment_txes(&transfer);
                let validation_config = ValidationConfig {
                    chain_net: self.chain_net(),
                    trusted_typesystem: transfer.types.clone(),
                    ..Default::default()
                };
                let state = stock.as_state_provider();
                let extra_states = Some(vec![std::slice::from_ref(state)]);
                let valid = transfer.validate_extra_states(
                    &resolver,
                    &validation_config,
                    extra_states,
                )?;
                stock.accept_transfer(valid, &resolver)?;
                eprintln!("Transfer accepted into the stash");
            }
        }
        Ok(())
    }
}
