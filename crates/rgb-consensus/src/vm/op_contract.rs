// RGB Consensus Library: consensus layer for RGB smart contracts.
//
// SPDX-License-Identifier: Apache-2.0
//
// Written in 2019-2024 by
//     Dr Maxim Orlovsky <orlovsky@lnp-bp.org>
//
// Copyright (C) 2019-2024 LNP/BP Standards Association. All rights reserved.
// Copyright (C) 2019-2024 Dr Maxim Orlovsky. All rights reserved.
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

#![allow(clippy::unusual_byte_groupings)]

use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::ops::RangeInclusive;

use aluvm::isa::{Bytecode, BytecodeError, ExecStep, InstructionSet};
use aluvm::library::{CodeEofError, IsaSeg, LibSite, Read, Write};
use aluvm::reg::{CoreRegs, Reg, Reg16, Reg32, RegA, RegR, RegS};
use amplify::num::{u24, u3, u4};
use amplify::Wrapper;
use bitcoin::hashes::Hash as _;
use bitcoin::{OutPoint as Outpoint, Txid};
use secp256k1::{ecdsa, Message, PublicKey};

use super::opcodes::*;
use super::{ContractStateAccess, VmContext};
use crate::vm::{GlobalsIter, OrdOpRef};
use crate::{Assign, AssignmentType, GlobalStateType, MetaType, RevealedState, TypedAssigns};

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Display)]
pub enum ContractOp<S: ContractStateAccess> {
    /// Counts number of inputs (previous state entries) of the provided type
    /// and puts the number to the destination `a16` register.
    ///
    /// If the operation doesn't contain inputs with a given assignment type,
    /// sets destination index to zero. Does not change `st0` register.
    #[display("cnp     {0},a16{1}")]
    CnP(AssignmentType, Reg32),

    /// Counts number of outputs (owned state entries) of the provided type
    /// and puts the number to the destination `a16` register.
    ///
    /// If the operation doesn't contain inputs with a given assignment type,
    /// sets destination index to zero. Does not change `st0` register.
    #[display("cns     {0},a16{1}")]
    CnS(AssignmentType, Reg32),

    /// Counts number of global state items of the provided type affected by the
    /// current operation and puts the number to the destination `a8` register.
    ///
    /// If the operation doesn't contain inputs with a given assignment type,
    /// sets destination index to zero. Does not change `st0` register.
    #[display("cng     {0},a8{1}")]
    CnG(GlobalStateType, Reg32),

    /// Counts number of global state items of the provided type in the contract
    /// state and puts the number to the destination `a32` register.
    ///
    /// If the operation doesn't contain inputs with a given assignment type,
    /// sets destination index to zero. Does not change `st0` register.
    #[display("cnc     {0},a32{1}")]
    CnC(GlobalStateType, Reg32),

    /// Loads input (previous) structured state with type id from the first
    /// argument and index from the second argument `a16` register into a
    /// register provided in the third argument.
    ///
    /// If the state is absent or is not a structured state sets `st0` to
    /// `false` and terminates the program.
    ///
    /// If the state at the index is concealed, sets destination to `None`.
    #[display("ldp     {0},a16{1},{2}")]
    LdP(AssignmentType, Reg16, RegS),

    /// Loads owned structured state with type id from the first argument and
    /// index from the second argument `a16` register into a register provided
    /// in the third argument.
    ///
    /// If the state is absent or is not a structured state sets `st0` to
    /// `false` and terminates the program.
    ///
    /// If the state at the index is concealed, sets destination to `None`.
    #[display("lds     {0},a16{1},{2}")]
    LdS(AssignmentType, Reg16, RegS),

    /// Loads owned fungible state with type id from the first argument and
    /// index from the second argument `a16` register into `a64` register
    /// provided in the third argument.
    ///
    /// If the state is absent or is not a fungible state sets `st0` to
    /// `false` and terminates the program.
    ///
    /// If the state at the index is concealed, sets destination to `None`.
    #[display("ldf     {0},a16{1},a64{2}")]
    LdF(AssignmentType, Reg16, Reg16),

    /// Checks owned declarative state with type id from the first argument and
    /// index from the second argument `a16` register, writing result as
    /// boolean value into destination `a8` register from the third argument.
    ///
    /// Writes `1` if assignment exists and has declarative class; otherwise
    /// writes `0`. Does not modify `st0`.
    #[display("ldr     {0},a16{1},a8{2}")]
    LdR(AssignmentType, Reg16, Reg16),

    /// Loads global state from the current operation with type id from the
    /// first argument and index from the second argument `a8` register into a
    /// register provided in the third argument.
    ///
    /// If the state is absent sets `st0` to `false` and terminates the program.
    #[display("ldg     {0},a8{1},{2}")]
    LdG(GlobalStateType, Reg16, RegS),

    /// Loads part of the contract global state with type id from the first
    /// argument at the depth from the second argument `a32` register into a
    /// register provided in the third argument.
    ///
    /// If the contract doesn't have the provided global state type, or it
    /// doesn't contain a value at the requested index, sets `st0`
    /// to fail state and terminates the program. The value of the
    /// destination register is not changed.
    #[display("ldc     {0},a32{1},{2}")]
    LdC(GlobalStateType, Reg16, RegS),

    /// Loads operation metadata with a type id from the first argument into a
    /// register provided in the second argument.
    ///
    /// If the operation doesn't have metadata, sets `st0` to fail state and
    /// terminates the program. The value of the destination register is not
    /// changed.
    #[display("ldm     {0},{1}")]
    LdM(MetaType, RegS),

    /// Loads owned fungible state from the contract state at a specified
    /// outpoint. The outpoint is composed from `r256` register (txid) and
    /// `a32` register (vout). Item index is taken from `a16` register.
    /// Result is written to destination `a64` register.
    ///
    /// If the outpoint has no fungible state of the given type, or the
    /// index is out of range, sets `st0` to fail state and stops execution.
    #[display("ldof    {0},r256{1},a32{2},a16{3},a64{4}")]
    LdOF(AssignmentType, Reg16, Reg16, Reg16, Reg16),

    /// Loads owned structured data state from the contract state at a
    /// specified outpoint. The outpoint is composed from `r256` register
    /// (txid) and `a32` register (vout). Item index is taken from `a16`
    /// register. Result is written to destination `s16` register.
    ///
    /// If the outpoint has no data state of the given type, or the index
    /// is out of range, sets `st0` to fail state and stops execution.
    #[display("ldod    {0},r256{1},a32{2},a16{3},{4}")]
    LdOD(AssignmentType, Reg16, Reg16, Reg16, RegS),

    /// Loads owned rights count from the contract state at a specified
    /// outpoint. The outpoint is composed from `r256` register (txid) and
    /// `a32` register (vout). Result (u32 count) is written to destination
    /// `a32` register.
    ///
    /// Does not fail if no rights exist; writes zero instead.
    #[display("ldor    {0},r256{1},a32{2},a32{3}")]
    LdOR(AssignmentType, Reg16, Reg16, Reg16),

    /// Verify sum of inputs and outputs are equal.
    ///
    /// The only argument specifies owned state type for the sum operation. If
    /// this state does not exist, or either inputs or outputs does not have
    /// any data for the state, the verification fails.
    ///
    /// If verification succeeds, doesn't change `st0` value; otherwise sets it
    /// to `false` and stops execution.
    #[display("svs     {0}")]
    Svs(AssignmentType),

    /// Verify sum of outputs and value in `a64[0]` register are equal.
    ///
    /// The first argument specifies owned state type for the sum operation. If
    /// this state does not exist, or either inputs or outputs does not have
    /// any data for the state, the verification fails.
    ///
    /// If `a64[0]` register does not contain value, the verification fails.
    ///
    /// If verification succeeds, doesn't change `st0` value; otherwise sets it
    /// to `false` and stops execution.
    #[display("sas     {0}")]
    Sas(/** owned state type */ AssignmentType),

    /// Verify sum of inputs and value in `a64[0]` register are equal.
    ///
    /// The first argument specifies owned state type for the sum operation. If
    /// this state does not exist, or either inputs or outputs does not have
    /// any data for the state, the verification fails.
    ///
    /// If `a64[0]` register does not contain value, the verification fails.
    ///
    /// If verification succeeds, doesn't change `st0` value; otherwise sets it
    /// to `false` and stops execution.
    #[display("sps     {0}")]
    Sps(/** owned state type */ AssignmentType),

    /// Verifies the signature of a transition against the pubkey in the first argument.
    ///
    /// If the register doesn't contain a valid public key or the operation
    /// signature is missing or invalid, sets `st0` to fail state and terminates
    /// the program.
    #[display("vts     {0}")]
    Vts(RegS),

    /// All other future unsupported operations, which must set `st0` to
    /// `false` and stop the execution.
    #[display("fail    {0}")]
    Fail(u8, PhantomData<S>),
}

impl<S: ContractStateAccess> InstructionSet for ContractOp<S> {
    type Context<'ctx> = VmContext<'ctx, S>;

    fn isa_ids() -> IsaSeg { IsaSeg::with("RGB") }

    fn src_regs(&self) -> BTreeSet<Reg> {
        match self {
            ContractOp::LdP(_, reg, _)
            | ContractOp::LdF(_, reg, _)
            | ContractOp::LdR(_, reg, _)
            | ContractOp::LdS(_, reg, _) => bset![Reg::A(RegA::A16, (*reg).into())],
            ContractOp::LdG(_, reg, _) => bset![Reg::A(RegA::A8, (*reg).into())],
            ContractOp::LdC(_, reg, _) => bset![Reg::A(RegA::A32, (*reg).into())],
            ContractOp::LdOF(_, txid_reg, vout_reg, idx_reg, _)
            | ContractOp::LdOD(_, txid_reg, vout_reg, idx_reg, _) => bset![
                Reg::R(RegR::R256, (*txid_reg).into()),
                Reg::A(RegA::A32, (*vout_reg).into()),
                Reg::A(RegA::A16, (*idx_reg).into())
            ],
            ContractOp::LdOR(_, txid_reg, vout_reg, _) => bset![
                Reg::R(RegR::R256, (*txid_reg).into()),
                Reg::A(RegA::A32, (*vout_reg).into())
            ],

            ContractOp::CnP(_, _)
            | ContractOp::CnS(_, _)
            | ContractOp::CnG(_, _)
            | ContractOp::CnC(_, _)
            | ContractOp::LdM(_, _) => bset![],
            ContractOp::Svs(_) => bset![],
            ContractOp::Sas(_) | ContractOp::Sps(_) => bset![Reg::A(RegA::A64, Reg32::Reg0)],

            ContractOp::Vts(_) => bset![],

            ContractOp::Fail(_, _) => bset![],
        }
    }

    fn dst_regs(&self) -> BTreeSet<Reg> {
        match self {
            ContractOp::CnG(_, reg) => {
                bset![Reg::A(RegA::A8, *reg)]
            }
            ContractOp::LdR(_, _, reg) => {
                bset![Reg::A(RegA::A8, (*reg).into())]
            }
            ContractOp::CnP(_, reg) | ContractOp::CnS(_, reg) | ContractOp::CnC(_, reg) => {
                bset![Reg::A(RegA::A16, *reg)]
            }
            ContractOp::LdF(_, _, reg) | ContractOp::LdOF(_, _, _, _, reg) => {
                bset![Reg::A(RegA::A64, (*reg).into())]
            }
            ContractOp::LdOR(_, _, _, reg) => {
                bset![Reg::A(RegA::A32, (*reg).into())]
            }
            ContractOp::LdG(_, _, reg)
            | ContractOp::LdS(_, _, reg)
            | ContractOp::LdP(_, _, reg)
            | ContractOp::LdC(_, _, reg)
            | ContractOp::LdM(_, reg)
            | ContractOp::LdOD(_, _, _, _, reg) => {
                bset![Reg::S(*reg)]
            }
            ContractOp::Svs(_) | ContractOp::Sas(_) | ContractOp::Sps(_) => {
                bset![]
            }
            ContractOp::Vts(reg) => bset![Reg::S(*reg)],
            ContractOp::Fail(_, _) => bset![],
        }
    }

    fn complexity(&self) -> u64 {
        match self {
            ContractOp::CnP(_, _)
            | ContractOp::CnS(_, _)
            | ContractOp::CnG(_, _)
            | ContractOp::CnC(_, _) => 2,
            ContractOp::LdP(_, _, _)
            | ContractOp::LdS(_, _, _)
            | ContractOp::LdF(_, _, _)
            | ContractOp::LdR(_, _, _)
            | ContractOp::LdG(_, _, _)
            | ContractOp::LdC(_, _, _)
            | ContractOp::LdOF(_, _, _, _, _)
            | ContractOp::LdOD(_, _, _, _, _)
            | ContractOp::LdOR(_, _, _, _) => 8,
            ContractOp::LdM(_, _) => 6,
            ContractOp::Svs(_) | ContractOp::Sas(_) | ContractOp::Sps(_) => 20,
            ContractOp::Vts(_) => 512,
            ContractOp::Fail(_, _) => u64::MAX,
        }
    }

    fn exec(&self, regs: &mut CoreRegs, _site: LibSite, context: &Self::Context<'_>) -> ExecStep {
        macro_rules! fail {
            () => {{
                regs.set_failure();
                return ExecStep::Stop;
            }};
        }
        macro_rules! load_revealed_inputs {
            ($state_type:ident) => {{
                match context.op_info.prev_state.get($state_type) {
                    Some(assignments) => {
                        let mut values = vec![];
                        for assign in assignments.iter() {
                            if let RevealedState::Fungible(assign) = assign {
                                values.push(assign.as_inner().as_u64())
                            } else {
                                fail!()
                            }
                        }
                        values
                    }
                    None => vec![],
                }
            }};
        }
        macro_rules! load_revealed_outputs {
            ($state_type:ident) => {{
                match context.op_info.owned_state().get(*$state_type) {
                    Some(TypedAssigns::Fungible(state)) => {
                        let mut values = vec![];
                        for assign in state.iter().map(Assign::as_revealed_state) {
                            values.push(assign.as_inner().as_u64())
                        }
                        values
                    }
                    None => vec![],
                    _ => fail!(),
                }
            }};
        }

        match self {
            ContractOp::CnP(state_type, reg) => {
                regs.set_n(
                    RegA::A16,
                    *reg,
                    context
                        .op_info
                        .prev_state
                        .get(state_type)
                        .map(|a| a.len() as u16)
                        .unwrap_or(0),
                );
            }
            ContractOp::CnS(state_type, reg) => {
                regs.set_n(
                    RegA::A16,
                    *reg,
                    context
                        .op_info
                        .owned_state()
                        .get(*state_type)
                        .map(|a| a.len_u16())
                        .unwrap_or(0),
                );
            }
            ContractOp::CnG(state_type, reg) => {
                regs.set_n(
                    RegA::A8,
                    *reg,
                    context
                        .op_info
                        .global()
                        .get(state_type)
                        .map(|a| a.len_u16())
                        .unwrap_or(0),
                );
            }
            ContractOp::CnC(state_type, reg) => {
                if let Ok(global) = RefCell::borrow(&context.contract_state).global(*state_type) {
                    regs.set_n(RegA::A32, *reg, global.count() as u32);
                } else {
                    regs.set_n(RegA::A32, *reg, 0u32);
                }
            }
            ContractOp::LdP(state_type, reg_32, reg) => {
                let Some(reg_32) = *regs.get_n(RegA::A16, *reg_32) else {
                    fail!()
                };
                let index: u16 = reg_32.into();

                let Some(state) = context.op_info.prev_state.get(state_type).and_then(|a| {
                    a.get(index as usize).and_then(|s| match s {
                        RevealedState::Structured(state) => Some(state),
                        _ => None,
                    })
                }) else {
                    fail!()
                };
                let state = state.as_inner();
                regs.set_s16(*reg, state);
            }
            ContractOp::LdS(state_type, reg_32, reg) => {
                let Some(reg_32) = *regs.get_n(RegA::A16, *reg_32) else {
                    fail!()
                };
                let index: u16 = reg_32.into();

                let Some(Ok(state)) = context
                    .op_info
                    .owned_state()
                    .get(*state_type)
                    .map(|a| a.into_structured_state_at(index))
                else {
                    fail!()
                };
                let state = state.into_inner();
                regs.set_s16(*reg, state);
            }
            ContractOp::LdF(state_type, reg_32, reg) => {
                let Some(reg_32) = *regs.get_n(RegA::A16, *reg_32) else {
                    fail!()
                };
                let index: u16 = reg_32.into();

                let Some(Ok(state)) = context
                    .op_info
                    .owned_state()
                    .get(*state_type)
                    .map(|a| a.into_fungible_state_at(index))
                else {
                    fail!()
                };
                regs.set_n(RegA::A64, *reg, state.as_inner().as_u64());
            }
            ContractOp::LdR(state_type, reg_32, reg) => {
                let ok = regs
                    .get_n(RegA::A16, *reg_32)
                    .map(u16::from)
                    .and_then(|index| {
                        context
                            .op_info
                            .owned_state()
                            .get(*state_type)
                            .map(|typed| typed.is_declarative() && index < typed.len_u16())
                    })
                    .unwrap_or(false);
                regs.set_n(RegA::A8, *reg, if ok { 1u8 } else { 0u8 });
            }
            ContractOp::LdG(state_type, reg_8, reg_s) => {
                let Some(reg_32) = *regs.get_n(RegA::A8, *reg_8) else {
                    fail!()
                };
                let index: u8 = reg_32.into();

                let Some(state) = context
                    .op_info
                    .global()
                    .get(state_type)
                    .and_then(|a| a.get(index as usize))
                else {
                    fail!()
                };
                regs.set_s16(*reg_s, state.as_inner());
            }

            ContractOp::LdC(state_type, reg_32, reg_s) => {
                let state = RefCell::borrow(&context.contract_state);
                let Ok(global) = state.global(*state_type) else {
                    fail!()
                };
                let Some(reg_32) = *regs.get_n(RegA::A32, *reg_32) else {
                    fail!()
                };
                let index: u32 = reg_32.into();
                let Ok(index) = u24::try_from(index) else {
                    fail!()
                };
                let Some(state) = global.at_depth(index.to_usize()) else {
                    fail!()
                };
                regs.set_s16(*reg_s, state.borrow().data().as_inner());
            }
            ContractOp::LdM(type_id, reg) => {
                let Some(meta) = context.op_info.metadata().get(type_id) else {
                    fail!()
                };
                regs.set_s16(*reg, meta.to_inner());
            }
            ContractOp::LdOF(assign_type, txid_reg, vout_reg, idx_reg, dst_reg) => {
                let Some(txid_num) = *regs.get_n(RegR::R256, *txid_reg) else {
                    fail!()
                };
                let mut txid_arr = [0u8; 32];
                txid_arr.copy_from_slice(txid_num.as_ref());
                let txid = Txid::from_byte_array(txid_arr);

                let Some(vout_num) = *regs.get_n(RegA::A32, *vout_reg) else {
                    fail!()
                };
                let vout: u32 = vout_num.into();
                let outpoint = Outpoint::new(txid, vout);

                let Some(idx_num) = *regs.get_n(RegA::A16, *idx_reg) else {
                    fail!()
                };
                let index: u16 = idx_num.into();

                let state = RefCell::borrow(&context.contract_state);
                let Some(fungible) = state.fungible(outpoint, *assign_type).nth(index as usize)
                else {
                    fail!()
                };
                regs.set_n(RegA::A64, *dst_reg, fungible.as_u64());
            }
            ContractOp::LdOD(assign_type, txid_reg, vout_reg, idx_reg, dst_reg) => {
                let Some(txid_num) = *regs.get_n(RegR::R256, *txid_reg) else {
                    fail!()
                };
                let mut txid_arr = [0u8; 32];
                txid_arr.copy_from_slice(txid_num.as_ref());
                let txid = Txid::from_byte_array(txid_arr);

                let Some(vout_num) = *regs.get_n(RegA::A32, *vout_reg) else {
                    fail!()
                };
                let vout: u32 = vout_num.into();
                let outpoint = Outpoint::new(txid, vout);

                let Some(idx_num) = *regs.get_n(RegA::A16, *idx_reg) else {
                    fail!()
                };
                let index: u16 = idx_num.into();

                let state = RefCell::borrow(&context.contract_state);
                let Some(data) = state.data(outpoint, *assign_type).nth(index as usize) else {
                    fail!()
                };
                regs.set_s16(*dst_reg, data.borrow().as_inner());
            }
            ContractOp::LdOR(assign_type, txid_reg, vout_reg, dst_reg) => {
                let Some(txid_num) = *regs.get_n(RegR::R256, *txid_reg) else {
                    fail!()
                };
                let mut txid_arr = [0u8; 32];
                txid_arr.copy_from_slice(txid_num.as_ref());
                let txid = Txid::from_byte_array(txid_arr);

                let Some(vout_num) = *regs.get_n(RegA::A32, *vout_reg) else {
                    fail!()
                };
                let vout: u32 = vout_num.into();
                let outpoint = Outpoint::new(txid, vout);

                let state = RefCell::borrow(&context.contract_state);
                let rights = state.rights(outpoint, *assign_type);
                regs.set_n(RegA::A32, *dst_reg, rights);
            }
            ContractOp::Svs(state_type) => {
                let Some(input_amt) = load_revealed_inputs!(state_type)
                    .iter()
                    .try_fold(0u64, |acc, &x| acc.checked_add(x))
                else {
                    fail!()
                };
                let outputs = load_revealed_outputs!(state_type);
                if outputs.contains(&0) {
                    fail!()
                }
                let Some(output_amt) = outputs.iter().try_fold(0u64, |acc, &x| acc.checked_add(x))
                else {
                    fail!()
                };
                if input_amt != output_amt {
                    fail!()
                }
            }
            ContractOp::Sas(owned_state) => {
                let Some(sum) = *regs.get_n(RegA::A64, Reg32::Reg0) else {
                    fail!()
                };
                let sum = u64::from(sum);

                let outputs = load_revealed_outputs!(owned_state);
                if outputs.contains(&0) {
                    fail!()
                }
                let Some(output_amt) = outputs.iter().try_fold(0u64, |acc, &x| acc.checked_add(x))
                else {
                    fail!()
                };

                if sum != output_amt {
                    fail!()
                }
            }
            ContractOp::Sps(owned_state) => {
                let Some(sum) = *regs.get_n(RegA::A64, Reg32::Reg0) else {
                    fail!()
                };
                let sum = u64::from(sum);

                let Some(input_amt) = load_revealed_inputs!(owned_state)
                    .iter()
                    .try_fold(0u64, |acc, &x| acc.checked_add(x))
                else {
                    fail!()
                };

                if sum != input_amt {
                    fail!()
                }
            }
            ContractOp::Vts(reg_s) => match context.op_info.op {
                OrdOpRef::Genesis(_) => fail!(),
                OrdOpRef::Transition(transition, _, _, _) => {
                    let Some(pubkey) = regs.s16(*reg_s) else {
                        fail!()
                    };
                    let Ok(pubkey) = PublicKey::from_slice(&pubkey.to_vec()) else {
                        fail!()
                    };
                    let Some(ref witness) = transition.signature else {
                        fail!()
                    };
                    let sig_bytes = witness.clone().into_inner().into_inner();
                    let Ok(sig) = ecdsa::Signature::from_compact(&sig_bytes) else {
                        fail!()
                    };

                    let transition_id = context.op_info.id.into_inner().into_inner();
                    let msg = Message::from_digest(transition_id);

                    if sig.verify(&msg, &pubkey).is_err() {
                        fail!()
                    }
                }
            },
            // All other future unsupported operations, which must set `st0` to `false`.
            _ => fail!(),
        }
        ExecStep::Next
    }
}

impl<S: ContractStateAccess> Bytecode for ContractOp<S> {
    fn instr_range() -> RangeInclusive<u8> { INSTR_CONTRACT_FROM..=INSTR_CONTRACT_TO }

    fn instr_byte(&self) -> u8 {
        match self {
            ContractOp::CnP(_, _) => INSTR_CNP,
            ContractOp::CnS(_, _) => INSTR_CNS,
            ContractOp::CnG(_, _) => INSTR_CNG,
            ContractOp::CnC(_, _) => INSTR_CNC,

            ContractOp::LdG(_, _, _) => INSTR_LDG,
            ContractOp::LdS(_, _, _) => INSTR_LDS,
            ContractOp::LdP(_, _, _) => INSTR_LDP,
            ContractOp::LdF(_, _, _) => INSTR_LDF,
            ContractOp::LdR(_, _, _) => INSTR_LDR,
            ContractOp::LdC(_, _, _) => INSTR_LDC,
            ContractOp::LdM(_, _) => INSTR_LDM,
            ContractOp::LdOF(_, _, _, _, _) => INSTR_LDOF,
            ContractOp::LdOD(_, _, _, _, _) => INSTR_LDOD,
            ContractOp::LdOR(_, _, _, _) => INSTR_LDOR,

            ContractOp::Svs(_) => INSTR_SVS,
            ContractOp::Sas(_) => INSTR_SAS,
            ContractOp::Sps(_) => INSTR_SPS,

            ContractOp::Vts(_) => INSTR_VTS,

            ContractOp::Fail(other, _) => *other,
        }
    }

    fn encode_args<W>(&self, writer: &mut W) -> Result<(), BytecodeError>
    where W: Write {
        match self {
            ContractOp::CnP(state_type, reg) => {
                writer.write_u16(*state_type)?;
                writer.write_u5(reg)?;
                writer.write_u3(u3::ZERO)?;
            }
            ContractOp::CnS(state_type, reg) => {
                writer.write_u16(*state_type)?;
                writer.write_u5(reg)?;
                writer.write_u3(u3::ZERO)?;
            }
            ContractOp::CnG(state_type, reg) => {
                writer.write_u16(*state_type)?;
                writer.write_u5(reg)?;
                writer.write_u3(u3::ZERO)?;
            }
            ContractOp::CnC(state_type, reg) => {
                writer.write_u16(*state_type)?;
                writer.write_u5(reg)?;
                writer.write_u3(u3::ZERO)?;
            }
            ContractOp::LdP(state_type, reg_a, reg_s) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(reg_a)?;
                writer.write_u4(reg_s)?;
            }
            ContractOp::LdS(state_type, reg_a, reg_s) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(reg_a)?;
                writer.write_u4(reg_s)?;
            }
            ContractOp::LdF(state_type, reg_a, reg_dst) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(reg_a)?;
                writer.write_u4(reg_dst)?;
            }
            ContractOp::LdR(state_type, reg_a, reg_dst) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(reg_a)?;
                writer.write_u4(reg_dst)?;
            }
            ContractOp::LdG(state_type, reg_a, reg_s) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(reg_a)?;
                writer.write_u4(reg_s)?;
            }
            ContractOp::LdC(state_type, reg_a, reg_s) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(reg_a)?;
                writer.write_u4(reg_s)?;
            }
            ContractOp::LdM(state_type, reg) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(reg)?;
                writer.write_u4(u4::ZERO)?;
            }
            ContractOp::LdOF(state_type, txid_reg, vout_reg, idx_reg, dst_reg) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(txid_reg)?;
                writer.write_u4(vout_reg)?;
                writer.write_u4(idx_reg)?;
                writer.write_u4(dst_reg)?;
            }
            ContractOp::LdOD(state_type, txid_reg, vout_reg, idx_reg, dst_reg) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(txid_reg)?;
                writer.write_u4(vout_reg)?;
                writer.write_u4(idx_reg)?;
                writer.write_u4(dst_reg)?;
            }
            ContractOp::LdOR(state_type, txid_reg, vout_reg, dst_reg) => {
                writer.write_u16(*state_type)?;
                writer.write_u4(txid_reg)?;
                writer.write_u4(vout_reg)?;
                writer.write_u4(dst_reg)?;
                writer.write_u4(u4::ZERO)?;
            }

            ContractOp::Svs(state_type) => writer.write_u16(*state_type)?,
            ContractOp::Sas(owned_type) => writer.write_u16(*owned_type)?,
            ContractOp::Sps(owned_type) => writer.write_u16(*owned_type)?,

            ContractOp::Vts(reg_s) => writer.write_u4(*reg_s)?,

            ContractOp::Fail(_, _) => {}
        }
        Ok(())
    }

    fn decode<R>(reader: &mut R) -> Result<Self, CodeEofError>
    where
        Self: Sized,
        R: Read,
    {
        Ok(match reader.read_u8()? {
            INSTR_CNP => {
                let i = Self::CnP(reader.read_u16()?.into(), reader.read_u5()?.into());
                reader.read_u3()?; // Discard garbage bits
                i
            }
            INSTR_CNS => {
                let i = Self::CnS(reader.read_u16()?.into(), reader.read_u5()?.into());
                reader.read_u3()?; // Discard garbage bits
                i
            }
            INSTR_CNG => {
                let i = Self::CnG(reader.read_u16()?.into(), reader.read_u5()?.into());
                reader.read_u3()?; // Discard garbage bits
                i
            }
            INSTR_CNC => {
                let i = Self::CnC(reader.read_u16()?.into(), reader.read_u5()?.into());
                reader.read_u3()?; // Discard garbage bits
                i
            }

            INSTR_LDP => Self::LdP(
                reader.read_u16()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
            ),
            INSTR_LDF => Self::LdF(
                reader.read_u16()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
            ),
            INSTR_LDR => Self::LdR(
                reader.read_u16()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
            ),
            INSTR_LDG => Self::LdG(
                reader.read_u16()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
            ),
            INSTR_LDS => Self::LdS(
                reader.read_u16()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
            ),
            INSTR_LDC => Self::LdC(
                reader.read_u16()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
            ),
            INSTR_LDM => {
                let i = Self::LdM(reader.read_u16()?.into(), reader.read_u4()?.into());
                reader.read_u4()?; // Discard garbage bits
                i
            }
            INSTR_LDOF => Self::LdOF(
                reader.read_u16()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
            ),
            INSTR_LDOD => Self::LdOD(
                reader.read_u16()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
                reader.read_u4()?.into(),
            ),
            INSTR_LDOR => {
                let i = Self::LdOR(
                    reader.read_u16()?.into(),
                    reader.read_u4()?.into(),
                    reader.read_u4()?.into(),
                    reader.read_u4()?.into(),
                );
                reader.read_u4()?; // Discard padding bits
                i
            }

            INSTR_SVS => Self::Svs(reader.read_u16()?.into()),
            INSTR_SAS => Self::Sas(reader.read_u16()?.into()),
            INSTR_SPS => Self::Sps(reader.read_u16()?.into()),

            INSTR_VTS => Self::Vts(reader.read_u4()?.into()),

            x => Self::Fail(x, PhantomData),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use aluvm::data::ByteStr;
    use aluvm::isa::Bytecode;
    use aluvm::library::{Cursor, LibSeg};
    use amplify::num::u4;
    use bitcoin::OutPoint as Outpoint;

    use super::*;
    use crate::vm::{GlobalsIter, UnknownGlobalStateType};
    use crate::vm::GlobalStateEntry;
    use crate::{FungibleState, RevealedData};

    #[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
    struct DummyState;

    impl GlobalsIter for std::iter::Empty<GlobalStateEntry> {
        fn at_depth(&self, _depth: usize) -> Option<Self::Item> { None }
    }

    impl ContractStateAccess for DummyState {
        fn global(
            &self,
            ty: GlobalStateType,
        ) -> Result<
            impl GlobalsIter<Item = impl Borrow<GlobalStateEntry>>,
            UnknownGlobalStateType,
        > {
            Err::<std::iter::Empty<GlobalStateEntry>, _>(UnknownGlobalStateType(ty))
        }
        fn rights(&self, _outpoint: Outpoint, _ty: AssignmentType) -> u32 { 0 }
        fn fungible(
            &self,
            _outpoint: Outpoint,
            _ty: AssignmentType,
        ) -> impl DoubleEndedIterator<Item = FungibleState> {
            std::iter::empty()
        }
        fn data(
            &self,
            _outpoint: Outpoint,
            _ty: AssignmentType,
        ) -> impl DoubleEndedIterator<Item = impl Borrow<RevealedData>> {
            std::iter::empty::<RevealedData>()
        }
    }

    type TestOp = ContractOp<DummyState>;

    fn roundtrip(op: TestOp) -> TestOp {
        let libseg = LibSeg::default();
        let mut code = [0u8; 16];
        {
            let mut cursor = Cursor::<_, ByteStr>::new(&mut code[..], &libseg);
            cursor.write_u8(op.instr_byte()).unwrap();
            op.encode_args(&mut cursor).unwrap();
        }
        let mut cursor = Cursor::<_, ByteStr>::new(&code[..], &libseg);
        TestOp::decode(&mut cursor).unwrap()
    }

    #[test]
    fn ldof_roundtrip() {
        let op = TestOp::LdOF(
            AssignmentType::from(42u16),
            Reg16::from(u4::with(1)),
            Reg16::from(u4::with(2)),
            Reg16::from(u4::with(3)),
            Reg16::from(u4::with(4)),
        );
        assert_eq!(roundtrip(op), op);
    }

    #[test]
    fn ldod_roundtrip() {
        let op = TestOp::LdOD(
            AssignmentType::from(100u16),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(5)),
            Reg16::from(u4::with(7)),
            RegS::from(u4::with(3)),
        );
        assert_eq!(roundtrip(op), op);
    }

    #[test]
    fn ldor_roundtrip() {
        let op = TestOp::LdOR(
            AssignmentType::from(7u16),
            Reg16::from(u4::with(2)),
            Reg16::from(u4::with(3)),
            Reg16::from(u4::with(8)),
        );
        assert_eq!(roundtrip(op), op);
    }

    #[test]
    fn ldr_roundtrip() {
        let op = TestOp::LdR(
            AssignmentType::from(11u16),
            Reg16::from(u4::with(6)),
            Reg16::from(u4::with(1)),
        );
        assert_eq!(roundtrip(op), op);
    }

    #[test]
    fn ldc_roundtrip_unchanged() {
        let op = TestOp::LdC(
            GlobalStateType::from(10u16),
            Reg16::from(u4::with(5)),
            RegS::from(u4::with(2)),
        );
        assert_eq!(roundtrip(op), op);
    }

    #[test]
    fn ldof_instr_byte() {
        let op = TestOp::LdOF(
            AssignmentType::from(0u16),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(0)),
        );
        assert_eq!(op.instr_byte(), INSTR_LDOF);
    }

    #[test]
    fn ldod_instr_byte() {
        let op = TestOp::LdOD(
            AssignmentType::from(0u16),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(0)),
            RegS::from(u4::with(0)),
        );
        assert_eq!(op.instr_byte(), INSTR_LDOD);
    }

    #[test]
    fn ldor_instr_byte() {
        let op = TestOp::LdOR(
            AssignmentType::from(0u16),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(0)),
        );
        assert_eq!(op.instr_byte(), INSTR_LDOR);
    }

    #[test]
    fn ldr_instr_byte() {
        let op = TestOp::LdR(
            AssignmentType::from(0u16),
            Reg16::from(u4::with(0)),
            Reg16::from(u4::with(0)),
        );
        assert_eq!(op.instr_byte(), INSTR_LDR);
    }
}
