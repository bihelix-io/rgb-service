// RGB schemas
//
// SPDX-License-Identifier: Apache-2.0
//
// Written in 2023-2024 by
//     Dr Maxim Orlovsky <orlovsky@lnp-bp.org>
//
// Copyright (C) 2023-2024 LNP/BP Standards Association. All rights reserved.
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

//! Non-Inflatable Assets (NIA) schema.

use aluvm::isa::opcodes::INSTR_PUTA;
use aluvm::isa::Instr;
use aluvm::library::{Lib, LibSite};
use amplify::confinement::Confined;
use amplify::Wrapper;
use rgbstd::contract::{
    AssignmentsFilter, ContractData, FungibleAllocation, IssuerWrapper, SchemaWrapper,
};
use rgbstd::persistence::{ContractStateRead, MemContract};
use rgbstd::schema::{
    AssignmentDetails, FungibleType, GenesisSchema, GlobalDetails, GlobalStateSchema, Occurrences,
    OwnedStateSchema, Schema, TransitionSchema,
};
use rgbstd::stl::{rgb_contract_stl, AssetSpec, ContractTerms, StandardTypes};
use rgbstd::validation::Scripts;
use rgbstd::vm::opcodes::INSTR_SVS;
use rgbstd::vm::RgbIsa;
use rgbstd::{rgbasm, Amount, SchemaId, TransitionDetails};
use strict_types::TypeSystem;

use crate::{
    ERRNO_ISSUED_MISMATCH, ERRNO_NON_EQUAL_IN_OUT, GS_ISSUED_SUPPLY, GS_NOMINAL, GS_TERMS,
    OS_ASSET, TS_INTERFACE, TS_TRANSFER,
};

pub const NIA2_SCHEMA_ID: SchemaId = SchemaId::from_array([
    0x22, 0x1a, 0x09, 0xdd, 0x03, 0xc1, 0x42, 0xde, 0xf6, 0x81, 0x37, 0xf3, 0x7a, 0x93, 0x50, 0xaf,
    0x61, 0x9c, 0x31, 0x84, 0xf7, 0x66, 0x13, 0xbe, 0x66, 0x39, 0x27, 0xe3, 0x47, 0x9d, 0x08, 0x74,
]);

pub(crate) const FN_NIA_BL_TRANSFER_OFFSET: u16 = 9;
pub(crate) const FN_NIA_BL_TRANSFER_DOBURN_OFFSET: u16 = 9 + 0x11;

use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

fn hash256_to_base62(hash: [u8; 32]) -> String {
    let mut n = BigUint::from_bytes_be(&hash);
    if n.is_zero() {
        return "0".to_string();
    }

    let base = BigUint::from(62u32);
    let mut out = Vec::new();

    while !n.is_zero() {
        let rem = (&n % &base).to_u8().unwrap() as usize;
        out.push(ALPHABET[rem] as char);
        n /= &base;
    }

    out.iter().rev().collect()
}

pub(crate) fn nia2_lib_interface() -> Lib {
    let interface_json = serde_json::json!({
        "transfer": {
            "script": {
                "position": FN_NIA_BL_TRANSFER_OFFSET
            },
            "parameters": [
                {
                    "name": "inputs",
                    "reg": "a64"
                },
                {
                    "name": "amount",
                    "reg": "a64"
                }
            ],
            "returns": [
                {
                    "name": "benifery",
                    "reg": "a64",
                    "type": OS_ASSET.into_inner()
                },
                {
                    "name": "change",
                    "reg": "a64",
                    "type": OS_ASSET.into_inner()
                },
                {
                    "name": "owner_state",
                    "reg": "a64",
                    "type": OS_ASSET.into_inner()
                }
            ]
        }
    });
    let interface_abi = serde_json::to_string(&interface_json).unwrap();

    let code = rgbasm! {
        // return data ABI, hjson string
        put s16[0],interface_abi;
        outr s16[0];
        ret;
        // A temporary implementation for BL_Transfer validation
        // Assume the application has already pushed the following values to the stack:
        //     total_from_payment_utxos: a64[0]
        //     transfer_amount: a64[1]
        // 销毁额 a3 = 转账额 / 100
        put     a64[3],100;
        div.uc  a64[1],a64[3];
        ifz a64[3];
        inv st0;
        jif FN_NIA_BL_TRANSFER_DOBURN_OFFSET;
        put a64[3],1;
        // FN_NIA_BL_TRANSFER_DOBURN_OFFSET:
        // 保留销毁额 a4 = 销毁额
        dup     a64[3],a64[4];
        // 转账额 a2 = a1 - 销毁额
        sub.uc    a64[1],a64[3];
        dup       a64[3],a64[2];
        // （找零）a1 = total - transfer - burn
        sub.uc    a64[0],a64[1];
        // sub.uc    a64[1],a64[3];

        put s16[1],"c5c3f8d1d75c39c1ff537f3f96286ab15fcd58ffdf2d66e9d869c52f55ddb35d:1"; // 销毁UTXO

        outr    a64[2];     // 收款总额
        outr    a64[1];     // 找零额
        outr    s16[1];     // 销毁UTXO
        outr    a64[4];     // 销毁额
        ret;

        // SUBROUTINE BL_Transfer validation
        // total_from_payment_utxos: a64[0]
        put     a16[0],0;
        ldp     OS_ASSET,a16[0],s16[0];
        put     a16[1],0;
        extr    s16[0],a64[0],a16[1];
        // transfer_amount: a64[1]
        put     a16[0],1;
        ldp     OS_ASSET,a16[0],s16[1];
        put     a16[1],0;
        extr    s16[1],a64[1],a16[1];
        // 保留转账额：a2 = a1
        dup     a64[1],a64[2];
        // a1 = total - transfer（找零）
        sub.uw    a64[0],a64[1];

        // 收款总额
        outr    a64[1];
        // 找零额
        outr    a64[0];
        ret;
    };
    Lib::assemble::<Instr<RgbIsa<MemContract>>>(&code).expect("wrong interface script")
}

pub(crate) fn nia_lib() -> Lib {
    let code = rgbasm! {
        // SUBROUTINE Transfer validation
        // Set errno
        put     a8[0],ERRNO_NON_EQUAL_IN_OUT;
        // Checking that the sum of inputs is equal to the sum of outputs.
        svs     OS_ASSET;
        test;
        ret;

        // SUBROUTINE Genesis validation
        // Checking genesis assignments amount against reported amount of issued assets present in
        // the global state.
        put     a8[0],ERRNO_ISSUED_MISMATCH;
        put     a8[1],0;
        put     a16[0],0;
        // Read global state into s16[0]
        ldg     GS_ISSUED_SUPPLY,a8[1],s16[0];
        // Extract 64 bits from the beginning of s16[0] into a64[0]
        // NB: if the global state is invalid, we will fail here and fail the validation
        extr    s16[0],a64[0],a16[0];
        // verify sum of outputs against a64[0] value
        sas     OS_ASSET;
        test;
        ret;
    };
    Lib::assemble::<Instr<RgbIsa<MemContract>>>(&code).expect("wrong non-inflatable asset script")
}

pub(crate) const FN_NIA_GENESIS_OFFSET: u16 = 4 + 3 + 2;
pub(crate) const FN_NIA_TRANSFER_OFFSET: u16 = 0;

fn nia_standard_types() -> StandardTypes {
    StandardTypes::with(rgb_contract_stl())
}

fn nia_schema() -> Schema {
    let types = nia_standard_types();

    let alu_lib = nia_lib();
    let alu_id = alu_lib.id();
    assert_eq!(alu_lib.code.as_ref()[FN_NIA_TRANSFER_OFFSET as usize + 4], INSTR_SVS);
    assert_eq!(alu_lib.code.as_ref()[FN_NIA_GENESIS_OFFSET as usize], INSTR_PUTA);
    assert_eq!(alu_lib.code.as_ref()[FN_NIA_GENESIS_OFFSET as usize + 4], INSTR_PUTA);
    assert_eq!(alu_lib.code.as_ref()[FN_NIA_GENESIS_OFFSET as usize + 8], INSTR_PUTA);

    Schema {
        ffv: zero!(),
        name: tn!("AutoBurnNonInflatableAsset"),
        meta_types: none!(),
        global_types: tiny_bmap! {
            GS_NOMINAL => GlobalDetails {
                global_state_schema: GlobalStateSchema::once(types.get("RGBContract.AssetSpec")),
                name: fname!("spec"),
            },
            GS_TERMS => GlobalDetails {
                global_state_schema: GlobalStateSchema::once(types.get("RGBContract.ContractTerms")),
                name: fname!("terms"),
            },
            GS_ISSUED_SUPPLY => GlobalDetails {
                global_state_schema: GlobalStateSchema::once(types.get("RGBContract.Amount")),
                name: fname!("issuedSupply"),
            },
        },
        owned_types: tiny_bmap! {
            OS_ASSET => AssignmentDetails {
                owned_state_schema: OwnedStateSchema::Fungible(FungibleType::Unsigned64Bit),
                name: fname!("assetOwner"),
                default_transition: TS_TRANSFER,
            }
        },
        genesis: GenesisSchema {
            metadata: none!(),
            globals: tiny_bmap! {
                GS_NOMINAL => Occurrences::Once,
                GS_TERMS => Occurrences::Once,
                GS_ISSUED_SUPPLY => Occurrences::Once,
            },
            assignments: tiny_bmap! {
                OS_ASSET => Occurrences::OnceOrMore,
            },
            validator: Some(LibSite::with(FN_NIA_GENESIS_OFFSET, alu_id)),
        },
        transitions: tiny_bmap! {
            TS_INTERFACE => TransitionDetails {
                transition_schema: TransitionSchema {
                    metadata: none!(),
                    globals: none!(),
                    inputs: tiny_bmap! {
                        // useless, just for building the schema
                        OS_ASSET => Occurrences::OnceOrMore,
                    },
                    assignments: none!(),
                    // validator: Some(LibSite::with(0, LibId::from([0; 32]))),
                    validator: Some(LibSite::with(0, nia2_lib_interface().id())),
                },
                // name: fname!("interface"),
                name: fname!("interface".to_owned()+&hash256_to_base62(nia2_lib_interface().id().to_byte_array())),
            },
            TS_TRANSFER => TransitionDetails {
                transition_schema: TransitionSchema {
                    metadata: none!(),
                    globals: none!(),
                    inputs: tiny_bmap! {
                        OS_ASSET => Occurrences::OnceOrMore
                    },
                    assignments: tiny_bmap! {
                        OS_ASSET => Occurrences::OnceOrMore
                    },
                    validator: Some(LibSite::with(FN_NIA_TRANSFER_OFFSET, alu_id))
                },
                name: fname!("transfer"),
            }
        },
        default_assignment: Some(OS_ASSET),
    }
}

#[derive(Default)]
pub struct AutoBurnNonInflatableAsset;

impl IssuerWrapper for AutoBurnNonInflatableAsset {
    type Wrapper<S: ContractStateRead> = Nia2Wrapper<S>;

    fn schema() -> Schema {
        nia_schema()
    }

    fn types() -> TypeSystem {
        nia_standard_types().type_system(nia_schema())
    }

    fn scripts() -> Scripts {
        let interface_lib = nia2_lib_interface();
        let transfer_lib = nia_lib();
        Confined::from_checked(bmap! {
            interface_lib.id() => interface_lib,
            transfer_lib.id() => transfer_lib,
        })
    }
}

#[derive(Clone, Eq, PartialEq, Debug, From)]
pub struct Nia2Wrapper<S: ContractStateRead>(ContractData<S>);

impl<S: ContractStateRead> SchemaWrapper<S> for Nia2Wrapper<S> {
    fn with(data: ContractData<S>) -> Self {
        if data.schema.schema_id() != NIA2_SCHEMA_ID {
            panic!("the provided schema is not NIA");
        }
        Self(data)
    }
}

impl<S: ContractStateRead> Nia2Wrapper<S> {
    pub fn spec(&self) -> AssetSpec {
        let strict_val = &self
            .0
            .global("spec")
            .next()
            .expect("NIA requires global state `spec` to have at least one item");
        AssetSpec::from_strict_val_unchecked(strict_val)
    }

    pub fn contract_terms(&self) -> ContractTerms {
        let strict_val = &self
            .0
            .global("terms")
            .next()
            .expect("NIA requires global state `terms` to have at least one item");
        ContractTerms::from_strict_val_unchecked(strict_val)
    }

    pub fn total_issued_supply(&self) -> Amount {
        self.0
            .global("issuedSupply")
            .map(|amount| Amount::from_strict_val_unchecked(&amount))
            .sum()
    }

    pub fn allocations<'c>(
        &'c self,
        filter: impl AssignmentsFilter + 'c,
    ) -> impl Iterator<Item = FungibleAllocation> + 'c {
        self.0.fungible_raw(OS_ASSET, filter).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use rgbstd::containers::{BuilderSeal, ConsignmentExt};
    use rgbstd::contract::*;
    use rgbstd::invoice::Precision;
    use rgbstd::stl::*;
    use rgbstd::txout::BlindSeal;
    use rgbstd::*;

    use super::*;

    #[test]
    fn schema_id() {
        let schema_id = nia_schema().schema_id();
        eprintln!("{:#04x?}", schema_id.to_byte_array());
        assert_eq!(NIA2_SCHEMA_ID, schema_id);
    }

    #[test]
    fn deterministic_contract_id() {
        let created_at = 1713261744;
        let terms = ContractTerms {
            text: RicardianContract::default(),
            media: None,
        };
        let spec = AssetSpec {
            ticker: Ticker::from("TICKER"),
            name: Name::from("NAME"),
            details: None,
            precision: Precision::try_from(2).unwrap(),
        };
        let issued_supply = 999u64;
        let seal: BlindSeal<Txid> = GenesisSeal::from(BlindSeal::with_blinding(
            Txid::from_str("8d54c98d4c29a1ec4fd90635f543f0f7a871a78eb6a6e706342f831d92e3ba19")
                .unwrap(),
            0,
            654321,
        ));

        let builder = ContractBuilder::with(
            Identity::default(),
            AutoBurnNonInflatableAsset::schema(),
            AutoBurnNonInflatableAsset::types(),
            AutoBurnNonInflatableAsset::scripts(),
            ChainNet::BitcoinTestnet4,
        )
        .add_global_state("spec", spec)
        .unwrap()
        .add_global_state("terms", terms)
        .unwrap()
        .add_global_state("issuedSupply", Amount::from(issued_supply))
        .unwrap()
        .add_fungible_state("assetOwner", BuilderSeal::from(seal), issued_supply)
        .unwrap();

        let contract = builder.issue_contract_raw(created_at).unwrap();

        assert_eq!(
            contract.contract_id().to_string(),
            s!("rgb:Obr5FCpg-_HRFHfM-H~gG822-3jhDOsi-CIxcO2S-Avz0Ea4")
        );
    }
}
