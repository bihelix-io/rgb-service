
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]



use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::Infallible;
use std::rc::Rc;

use aluvm::data::{ByteStr, Number};
use aluvm::library::{LibId, LibSite};
use aluvm::reg::{Reg8, Reg16, Reg32, RegAFR, RegR, RegS};
// use aluvm::reg::CoreRegs;
use amplify::confinement::{Confined, TinyBlob, U16 as MAX16, U24, U32};
use amplify::hex::FromHex;
use amplify::num::u5;
use chrono::Utc;
use rgbstd::bitcoin::hashes::sha256d;
use rgbstd::containers::{Batch, BuilderSeal, Transfer};
use rgbstd::contract::TransitionBuilder;
use rgbstd::invoice::{Amount, Beneficiary, InvoiceState, RgbInvoice};
use rgbstd::persistence::{IndexProvider, StashInconsistency, StashProvider, StateProvider, Stock};
use rgbstd::rgbcore::dbc::tapret::{TapretCommitment, TapretProof};
use rgbstd::rgbcore::dbc::Proof;
use rgbstd::rgbcore::secp256k1::rand;
use rgbstd::validation::{ResolveWitness, ValidationConfig, ValidationError, WitnessOrdProvider};
use rgbstd::containers::{Consignment, ConsignmentExt, ValidConsignment};
use rgbstd::vm::{ContractStateAccess, ContractStateEvolve, OrdOpRef, RgbIsa};
use rgbstd::{
    AssignmentType, ContractId, GraphSeal, OpId, Operation, Opout, Outpoint, OutputSeal, RevealedData,
    RevealedState, Transition, TransitionType, Txid,
};
use {
    aluvm::Vm,
    aluvm::isa::{Instr, InstructionSet, OutrValue},
};
use strict_types::{StrictSerialize, StrictVal, Ty, TypeRef};
use strict_types::value::Blob;
use rgbstd::Vout; // 或你本地的 vout 类型

use crate::filters::{Filter, WalletFilter};
use crate::validation::WitnessResolverError;
use crate::vm::WitnessOrd;
use crate::{CompletionError, CompositionError, PayError, WalletError};

#[derive(Debug, Clone)]
pub struct ScriptParam {
    pub reg_name: String,
    pub idx: u8,
    pub value: String,
}

use rgbstd::schema::{OwnedStateSchema, Schema};
use rgbstd::persistence::{MemContract, MemContractState};

/// Matches `rgbcore::vm::contract::OpInfo` layout (keep in sync with rgb-consensus).
struct OpInfoWire<'op> {
    id: OpId,
    prev_state: &'op BTreeMap<AssignmentType, Vec<RevealedState>>,
    op: &'op OrdOpRef<'op>,
}

/// Matches `rgbcore::vm::contract::VmContext` layout (keep in sync with rgb-consensus).
struct VmContextWire<'op, S: ContractStateAccess> {
    contract_id: ContractId,
    op_info: OpInfoWire<'op>,
    contract_state: Rc<RefCell<S>>,
}

/// `VmContext` is not public outside rgb-consensus; `InstructionSet::Context` is the same type.
#[inline]
fn wire_as_exec_ctx<'op, S: ContractStateAccess>(
    wire: &'op VmContextWire<'op, S>,
) -> &'op <Instr<RgbIsa<S>> as InstructionSet>::Context<'op> {
    // SAFETY: `VmContextWire` / `OpInfoWire` mirror the private `VmContext` / `OpInfo` structs.
    unsafe { std::mem::transmute(wire) }
}

pub fn run_script<const TRANSFER: bool>(
    consignment: &Consignment<TRANSFER>,
    lib_id: LibId,
    pos: u16,
    params: Vec<ScriptParam>,
) -> Result<Vec<OutrValue>, CompositionError> {
    type M = MemContract<MemContractState>;

    let init_ctx = (&consignment.schema, consignment.contract_id());
    let contract_state = M::init(init_ctx);
    run_script_with_contract_state(consignment, lib_id, pos, params, contract_state)
}

pub fn run_script_with_contract_state<const TRANSFER: bool, M: ContractStateAccess>(
    consignment: &Consignment<TRANSFER>,
    lib_id: LibId,
    pos: u16,
    params: Vec<ScriptParam>,
    contract_state: M,
) -> Result<Vec<OutrValue>, CompositionError> {
    let contract_state = Rc::new(RefCell::new(contract_state));
    let mut vm = Vm::<Instr<RgbIsa<M>>>::new();
    vm.registers.set_outstack_limit(1024);
    for param in params {
        match param.reg_name.as_str() {
            "a64" => {
                let _ = vm.registers.set_a64(
                    Reg32::from(u5::try_from(param.idx).unwrap()),
                    param.value.parse::<u64>().unwrap(),
                );
            }
            "a32" => {
                let _ = vm.registers.set_a32(
                    Reg32::from(u5::try_from(param.idx).unwrap()),
                    param.value.parse::<u32>().unwrap(),
                );
            }
            "r256" => {
                let n = parse_r256_number_forward(&param.value)?;
                let _ = vm.registers.set_n(
                    RegAFR::R(RegR::R256),
                    Reg32::from(u5::try_from(param.idx).unwrap()),
                    n,
                );
            }
            "s16" => {
                let _ = vm.registers.set_s16(
                    RegS::from(u5::try_from(param.idx).unwrap()),
                    ByteStr::from(ByteStr::with(param.value.as_bytes())),
                );
            }
            "outpoint" => {
                let _ = vm.registers.set_s16(
                    RegS::from(u5::try_from(param.idx).unwrap()),
                    ByteStr::from(ByteStr::with(param.value.as_bytes())),
                );
                // 将param.value解析为outpoint，设置到r256和a32中
                let outpoint = parse_outpoint(&OutrValue::Bytes(param.value.as_bytes().to_vec()))?;
                let _ = vm.registers.set_n(
                    RegAFR::R(RegR::R256),
                    Reg32::from(u5::try_from(param.idx).unwrap()),
                    parse_r256_number_forward(&outpoint.txid.to_string())?,
                );
                let _ = vm.registers.set_a32(
                    Reg32::from(u5::try_from(param.idx).unwrap()),
                    outpoint.vout,
                );
            }
            _ => {
                return Err(CompositionError::Unexpected(format!(
                    "Invalid register name: {}",
                    param.reg_name
                )));
            }
        }
    }

    let scripts: BTreeMap<_, _> =
        consignment.scripts.iter().map(|s| (s.id(), s.clone())).collect();
    let prev_state = BTreeMap::new();
    let ord_op = OrdOpRef::Genesis(consignment.genesis());
    let wire = VmContextWire {
        contract_id: consignment.contract_id(),
        op_info: OpInfoWire {
            id: ord_op.id(),
            prev_state: &prev_state,
            op: &ord_op,
        },
        contract_state: Rc::clone(&contract_state),
    };
    let exec_ctx = wire_as_exec_ctx(&wire);

    let ok = vm.exec(LibSite::with(pos, lib_id), |id| scripts.get(&id), exec_ctx);
    if !ok {
        return Err(CompositionError::Unexpected(format!(
            "script {}@{} failed to execute",
            pos, lib_id
        )));
    }
    Ok(vm.registers.outstack().to_vec())
}

pub fn outr_value_to_str(outr_value: &OutrValue) -> Result<&str, CompositionError> {
    let s = match &outr_value {
        OutrValue::Bytes(v) => {
            match std::str::from_utf8(v.as_slice()) {
                Ok(json_str) => json_str,
                Err(_) => return Err(CompositionError::Unexpected(
                    "outputs[0] is not valid UTF-8 string".to_string(),
                )),
            }
        },
        _ => {
            return Err(CompositionError::Unexpected(
                "outputs[0] must be a bytes value containing JSON string".to_string(),
            ))
        }
    };
    Ok(s)
}

use num_bigint::BigUint;
use num_traits::Zero;

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub fn base62_to_hash256(s: &str) -> Result<[u8; 32], String> {
    if s.is_empty() {
        return Err("empty base62 string".to_string());
    }

    // 与编码函数保持一致：特殊值 "0" 对应全零 hash
    if s == "0" {
        return Ok([0u8; 32]);
    }

    let base = BigUint::from(62u32);
    let mut n = BigUint::zero();

    for ch in s.bytes() {
        let digit = ALPHABET
            .iter()
            .position(|&c| c == ch)
            .ok_or_else(|| format!("invalid base62 character: {}", ch as char))?;

        n = n * &base + BigUint::from(digit as u32);
    }

    let bytes = n.to_bytes_be();
    if bytes.len() > 32 {
        return Err("decoded value does not fit into 32 bytes".to_string());
    }

    let mut out = [0u8; 32];
    out[32 - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}

/// Owned-state semantic type ids (aligned with `rgb-schemas` `lib.rs`).
const SEM_TYPE_OS_ASSET: u64 = 4000;
const SEM_TYPE_OS_HASH: u64 = 4202;
const SEM_TYPE_OS_OUTPOINT: u64 = 4014;

fn json_number_to_semantic_id(n: &serde_json::Number) -> Option<u64> {
    n.as_u64()
        .or_else(|| n.as_i64().and_then(|i| u64::try_from(i).ok()))
        .or_else(|| {
            let f = n.as_f64()?;
            (f >= 0.0 && f.is_finite() && f.fract() == 0.0).then_some(f as u64)
        })
}

fn json_string_to_semantic_id(s: &str) -> Option<u64> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    t.parse::<u64>().ok()
}

/// Interface `"type"`: JSON number (incl. whole float) or decimal string.
fn json_semantic_type_id(v: &serde_json::Value) -> Option<u64> {
    match v {
        serde_json::Value::Number(n) => json_number_to_semantic_id(n),
        serde_json::Value::String(s) => json_string_to_semantic_id(s),
        _ => None,
    }
}

/// Principal owned assignment type for this transition (same role as `PaymentContext::assignment_type` in `pay`).
/// Uses the interface `returns` entry named `benifery` if present; otherwise `fallback` (e.g. schema default).
pub fn main_assignment_type_from_returns_abi(
    returns_abi: &[serde_json::Value],
    fallback: AssignmentType,
) -> Result<AssignmentType, CompositionError> {
    for value in returns_abi {
        if value.get("name").and_then(|v| v.as_str()) != Some("benifery") {
            continue;
        }
        let type_raw = value.get("type").and_then(json_semantic_type_id).ok_or_else(|| {
            CompositionError::Unexpected(
                "ABI returns entry 'benifery': missing or invalid 'type'".to_string(),
            )
        })?;
        return Ok(AssignmentType::from(type_raw as u16));
    }
    Ok(fallback)
}

/// `inputs` 为 interface JSON 里的 `inputs` 数组；`sum_inputs` / `amt` 为本次支付侧已知金额。
pub fn generate_transition_parameters(
    parameters: &serde_json::Value,
    sum_inputs: Amount,
    amt: Amount,
) -> Result<Vec<ScriptParam>, CompositionError> {
    let Some(rows) = parameters.as_array() else {
        return Err(CompositionError::Unexpected(
            "interface parameters must be a JSON array".to_string(),
        ));
    };
    let mut script_params = Vec::new();
    for input in rows {
        let param_reg = input.get("reg").and_then(|v| v.as_str()).unwrap_or_default();
        let param_name = input.get("name").and_then(|v| v.as_str()).unwrap_or_default();
        match param_name {
            "inputs" | "sum_inputs" => {
                match param_reg {
                    "a64" => {
                        script_params.push(ScriptParam {
                            reg_name: "a64".to_string(),
                            idx: 0,
                            value: u64::from(sum_inputs).to_string(),
                        });
                    }
                    // "s16" => {
                    //     script_params.push(ScriptParam {
                    //         reg_name: "s16".to_string(),
                    //         idx: 0,
                    //         value: parse_outpoint(prev_outputs)?.to_string(),
                    //     });
                    // }
                    _ => {
                        return Err(CompositionError::Unexpected(format!(
                            "Invalid parameter name: {} for type: {}",
                            param_name,
                            param_reg,
                        )));
                    }
                }
            }
            "amount" | "amt" if param_reg == "a64" => {
                script_params.push(ScriptParam {
                    reg_name: "a64".to_string(),
                    idx: 1,
                    value: u64::from(amt).to_string(),
                });
            }
            _ => {
                return Err(CompositionError::Unexpected(format!(
                    "Invalid parameter name: {} for type: {}",
                    param_name,
                    param_reg,
                )));
            }
        }
    }

    Ok(script_params)
}

pub fn generate_transition_parameters_from_args(
    parameters: &serde_json::Value,
    args: &std::collections::HashMap<String, String>,
    sum_inputs: Amount,
    prev_outputs: &BTreeSet<OutputSeal>,
) -> Result<Vec<ScriptParam>, CompositionError> {
    let Some(rows) = parameters.as_array() else {
        return Err(CompositionError::Unexpected(
            "interface parameters must be a JSON array".to_string(),
        ));
    };
    let mut script_params = Vec::new();
    for (idx, param) in rows.iter().enumerate() {
        let param_name = param
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let param_reg = param
            .get("reg")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        match param_name {
            "inputs" | "sum_inputs" => {
                match param_reg {
                    "a64" => {
                        script_params.push(ScriptParam {
                            reg_name: param_reg.to_string(),
                            idx: idx as u8,
                            value: u64::from(sum_inputs).to_string(),
                        });
                    }
                    "outpoint" => {
                        let output = prev_outputs.iter().next().ok_or_else(|| {
                            CompositionError::Unexpected(
                                "missing previous outputs for s16 input".to_string(),
                            )
                        })?;
                        script_params.push(ScriptParam {
                            reg_name: "r256".to_string(),
                            idx: idx as u8,
                            value: "0x".to_string() + &output.txid.to_string(),
                        });
                        script_params.push(ScriptParam {
                            reg_name: "a32".to_string(),
                            idx: idx as u8,
                            value: output.vout.to_u32().to_string(),
                        });
                    }
                    _ => {
                        return Err(CompositionError::Unexpected(format!(
                            "Invalid parameter name: {} for type: {}",
                            param_name,
                            param_reg,
                        )));
                    }
                }
            },
            other => {
                let value = args
                    .get(other)
                    .ok_or_else(|| {
                        CompositionError::Unexpected(format!(
                            "missing required arg '{}' for transition parameter",
                            other
                        ))
                    })?
                    .clone();
                script_params.push(ScriptParam {
                    reg_name: param_reg.to_string(),
                    idx: idx as u8,
                    value: value,
                });
            }
        };
    }
    Ok(script_params)
}

pub fn parse_amount(value: &OutrValue) -> Result<Amount, CompositionError> {
    match value {
        OutrValue::Int(v) if *v >= 0 => Ok(Amount::from(*v as u64)),
        OutrValue::Int(v) => Err(CompositionError::Unexpected(format!(
            "amount must be non-negative int (a64 / OUTR), got {v}"
        ))),
        other => Err(CompositionError::Unexpected(format!(
            "amount must be a64 integer OUTR value, got {other:?} (use ABI type OS_HASH for sha256 Bytes)"
        ))),
    }
}

pub fn parse_string(value: &OutrValue) -> Result<Vec<u8>, CompositionError> {
    match value {
        OutrValue::Bytes(v) => Ok(v.as_slice().to_vec()),
        other => Err(CompositionError::Unexpected(format!(
            "string/data must be bytes OUTR value, got {other:?}"
        ))),
    }
}

fn parse_s16_payload<const TRANSFER: bool>(
    consignment: &Consignment<TRANSFER>,
    assignment_type: AssignmentType,
    value: &OutrValue,
) -> Result<Vec<u8>, CompositionError> {
    let raw = parse_string(value)?;
    let assignment = consignment
        .schema
        .owned_types
        .get(&assignment_type)
        .ok_or_else(|| {
            CompositionError::Unexpected(format!(
                "assignment type {} is not listed in schema.owned_types",
                u16::from(assignment_type)
            ))
        })?;

    let OwnedStateSchema::Structured(sem_id) = assignment.owned_state_schema else {
        return Err(CompositionError::Unexpected(format!(
            "s16 return requires structured assignment type {}, got {:?}",
            u16::from(assignment_type),
            assignment.owned_state_schema
        )));
    };

    if consignment
        .types
        .strict_deserialize_type(sem_id, raw.as_slice())
        .is_ok()
    {
        return Ok(raw);
    }

    let ty = consignment.types.find(sem_id).ok_or_else(|| {
        CompositionError::Unexpected(format!(
            "schema type system does not contain sem id {sem_id} for assignment type {}",
            u16::from(assignment_type)
        ))
    })?;
    if let Some(len) = target_byte_array_len(&consignment.types, ty) {
        let bytes = parse_fixed_bytes(&raw, len, assignment_type)?;
        let typed_val = consignment
            .types
            .typify(StrictVal::Bytes(Blob(bytes)), sem_id)
            .map_err(|e| {
                CompositionError::Unexpected(format!(
                    "s16 bytes do not match assignment type {} ({sem_id}): {e}",
                    u16::from(assignment_type)
                ))
            })?;
        let encoded = consignment
            .types
            .strict_serialize_value::<MAX16>(&typed_val)
            .map_err(|e| CompositionError::Unexpected(format!("s16 strict serialize: {e}")))?;
        return Ok(encoded.release());
    }

    let text = std::str::from_utf8(raw.as_slice()).map_err(|e| {
        CompositionError::Unexpected(format!("s16 structured value must be utf-8 text: {e}"))
    })?;
    let typed_val = consignment
        .types
        .typify(StrictVal::from(text), sem_id)
        .map_err(|e| {
            CompositionError::Unexpected(format!(
                "s16 value does not match assignment type {} ({sem_id}): {e}",
                u16::from(assignment_type)
            ))
        })?;
    let encoded = consignment
        .types
        .strict_serialize_value::<MAX16>(&typed_val)
        .map_err(|e| CompositionError::Unexpected(format!("s16 strict serialize: {e}")))?;
    Ok(encoded.release())
}

fn target_byte_array_len(
    types: &strict_types::TypeSystem,
    ty: &Ty<strict_types::SemId>,
) -> Option<usize> {
    match ty {
        Ty::Array(id, len) if id.is_byte() => Some(*len as usize),
        Ty::Tuple(fields) if fields.len() == 1 => fields
            .first()
            .and_then(|id| types.find(*id))
            .and_then(|inner| target_byte_array_len(types, inner)),
        _ => None,
    }
}

fn parse_fixed_bytes(
    raw: &[u8],
    len: usize,
    assignment_type: AssignmentType,
) -> Result<Vec<u8>, CompositionError> {
    if raw.len() == len {
        return Ok(raw.to_vec());
    }
    let text = std::str::from_utf8(raw).map_err(|e| {
        CompositionError::Unexpected(format!(
            "s16 bytes for assignment type {} must be {len} raw bytes or hex text: {e}",
            u16::from(assignment_type)
        ))
    })?;
    let hex = text
        .trim()
        .strip_prefix("0x")
        .or_else(|| text.trim().strip_prefix("0X"))
        .unwrap_or_else(|| text.trim());
    let bytes = Vec::<u8>::from_hex(hex).map_err(|e| {
        CompositionError::Unexpected(format!(
            "s16 fixed bytes for assignment type {} expects {len} raw bytes or {} hex chars: {e}",
            u16::from(assignment_type),
            len * 2
        ))
    })?;
    if bytes.len() != len {
        return Err(CompositionError::Unexpected(format!(
            "s16 fixed bytes for assignment type {} expects {len} bytes, got {} bytes",
            u16::from(assignment_type),
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn parse_r256_number_forward(value: &str) -> Result<Number, CompositionError> {
    let hex = value.trim().trim_start_matches("0x").trim_start_matches("0X");
    let mut bytes = Vec::<u8>::from_hex(hex).map_err(|e| {
        CompositionError::Unexpected(format!("r256 expects valid hex string: {e}"))
    })?;
    if bytes.len() != 32 {
        return Err(CompositionError::Unexpected(format!(
            "r256 expects exactly 32 bytes (64 hex chars), got {} bytes",
            bytes.len()
        )));
    }
    // AluVM Number stores integer bytes in little-endian order.
    // Reverse user-provided hex (big-endian textual order) to keep script-side semantics forward.
    bytes.reverse();
    Ok(Number::from_slice(bytes))
}

fn parse_outpoint(value: &OutrValue) -> Result<Outpoint, CompositionError> {    
    let s = match value {
        OutrValue::Bytes(v) => std::str::from_utf8(v.as_slice()).map_err(|e| {
            CompositionError::Unexpected(format!("OS_OUTPOINT must be utf-8 bytes 'txid:vout': {e}"))
        })?,
        other => {
            return Err(CompositionError::Unexpected(format!(
                "OS_OUTPOINT must be bytes encoded as 'txid:vout', got {other:?}"
            )))
        }
    };
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(CompositionError::Unexpected(
            "OS_OUTPOINT must be in the format txid:vout".to_string(),
        ));
    }
    let txid = parts[0].parse::<Txid>().map_err(|e| {
        CompositionError::Unexpected(format!("invalid txid in OS_OUTPOINT '{}': {}", parts[0], e))
    })?;
    let vout = parts[1].parse::<u32>().map_err(|e| {
        CompositionError::Unexpected(format!("invalid vout in OS_OUTPOINT '{}': {}", parts[1], e))
    })?;
    Ok(Outpoint::new(txid, vout))
}

fn parse_outpoint_payload(value: &OutrValue) -> Result<Vec<u8>, CompositionError> {
    let outpoint = parse_outpoint(value)?;
    let bytes = outpoint
        .to_strict_serialized::<U32>()
        .map_err(|e| CompositionError::Unexpected(format!("OS_OUTPOINT strict encode failed: {e}")))?;
    Ok(bytes.to_vec())
}

/// Parses sha256 from AluVM outstack: **32 raw bytes**, or **64 hex digits** (optional `0x`), UTF-8.
pub fn parse_hash256(value: &OutrValue) -> Result<[u8; 32], CompositionError> {
    match value {
        OutrValue::Bytes(b) => {
            if b.len() == 32 {
                let mut a = [0u8; 32];
                a.copy_from_slice(b.as_slice());
                return Ok(a);
            }
            let s = std::str::from_utf8(b.as_slice()).map_err(|e| {
                CompositionError::Unexpected(format!("OS_HASH: invalid utf-8 in outstack bytes: {e}"))
            })?;
            let t = s.trim();
            let hex_body = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")).unwrap_or(t);
            if hex_body.len() != 64 {
                return Err(CompositionError::Unexpected(format!(
                    "OS_HASH: expected 32 raw bytes or 64 hex chars, got {} bytes / {} hex chars",
                    b.len(),
                    hex_body.len()
                )));
            }
            if !hex_body.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(CompositionError::Unexpected(
                    "OS_HASH: hex digest must be 64 hex digits".to_string(),
                ));
            }
            let mut out = [0u8; 32];
            for i in 0..32 {
                out[i] = u8::from_str_radix(&hex_body[2 * i..2 * i + 2], 16).map_err(|e| {
                    CompositionError::Unexpected(format!("OS_HASH: invalid hex: {e}"))
                })?;
            }
            Ok(out)
        }
        other => Err(CompositionError::Unexpected(format!(
            "OS_HASH: expected Bytes (32 raw or 64-char hex), got {other:?}"
        ))),
    }
}

pub fn get_interface<const TRANSFER: bool>(
    consignment: &Consignment<TRANSFER>,
) -> Result<(serde_json::Value, LibId), CompositionError> {
    let interface_transition = consignment.schema.transitions.get(&TransitionType::with(65535u16))
        .ok_or(CompositionError::Unexpected("interface transition not found".to_string()))?;
    let interface_name = interface_transition.name.to_string();
    let interface_libid = LibId::from(base62_to_hash256(&interface_name[9..])?);
    let interface_outr_values =
        run_script(
            &consignment, 
            interface_libid, 
            0, 
            Vec::<ScriptParam>::new(),
        ).map_err(|e| e.to_string())?;
    if interface_outr_values.len() != 1 {
        return Err(CompositionError::Unexpected(
            "interface outr values must provide only one value".to_string(),
        ));
    }
    let interface_str = outr_value_to_str(&interface_outr_values[0])?;
    let interface: serde_json::Value = serde_json::from_str(interface_str)
        .map_err(|e| CompositionError::Unexpected(format!("Failed to parse interface as JSON: {}", e)))?;
    Ok((interface, interface_libid))
}

pub fn add_transition_states<const TRANSFER: bool>(
    consignment: &Consignment<TRANSFER>,
    abi: &Vec<serde_json::Value>,
    outputs: &Vec<OutrValue>,
    mut main_builder: TransitionBuilder,
    beneficiary_seal: &BuilderSeal<GraphSeal>,
    change_seal: &BuilderSeal<GraphSeal>,
) -> Result<TransitionBuilder, CompositionError> {
    let mut j  = 0;
    for (i, value) in abi.iter().enumerate() {
        let outr_value = &outputs[j];
        j = j + 1;
        let abi_name = value
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                CompositionError::Unexpected(format!(
                    "ABI entry at index {i}: missing or non-string 'name'"
                ))
            })?;
        let abi_reg = value.get("reg").and_then(|v| v.as_str()).ok_or_else(|| {
            CompositionError::Unexpected(format!(
                "ABI entry at index {i} (name={abi_name:?}): missing or invalid 'reg' (expect string)"
            ))
        })?;
        let type_raw = value.get("type").and_then(json_semantic_type_id).ok_or_else(|| {
            CompositionError::Unexpected(format!(
                "ABI entry at index {i} (name={abi_name:?}): missing or invalid 'type' (expect integer, whole float, or decimal string)"
            ))
        })?;
        let abi_type = AssignmentType::from(type_raw as u16);

        match abi_name {
            "benifery" => {
                if abi_reg != "a64" {
                    return Err(CompositionError::Unexpected(format!(
                        "ABI 'benifery' at index {i}: expected reg {abi_reg} (expected a64)",
                    )));
                }
                let received = parse_amount(outr_value)?;
                if received > Amount::ZERO {
                    main_builder = main_builder.add_fungible_state_raw(
                        abi_type,
                        beneficiary_seal.clone(),
                        received,
                    )?;
                }
            }
            "change" => match abi_reg {
                "a64" => {
                    let change = parse_amount(outr_value)?;
                    if change > Amount::ZERO {
                        main_builder = main_builder.add_fungible_state_raw(
                            abi_type,
                            change_seal.clone(),
                            change,
                        )?;
                    }
                }
                "r256" => {
                    let hash = parse_hash256(outr_value)?;
                    let payload = Confined::try_from_iter(hash.iter().copied()).map_err(|e| {
                        CompositionError::Unexpected(format!("OS_HASH RevealedData: {e}"))
                    })?;
                    main_builder = main_builder.add_data_raw(
                        abi_type,
                        change_seal.clone(),
                        RevealedData::new(payload),
                    )?;
                }
                "outpoint" => {
                    let raw = parse_outpoint_payload(outr_value)?;
                    let payload = Confined::try_from_iter(raw.into_iter()).map_err(|e| {
                        CompositionError::Unexpected(format!("OS_OUTPOINT RevealedData: {e}"))
                    })?;
                    main_builder = main_builder.add_data_raw(
                        abi_type,
                        change_seal.clone(),
                        RevealedData::new(payload),
                    )?;
                }
                "s16" => {
                    let data = parse_s16_payload(consignment, abi_type, outr_value)?;
                    main_builder = main_builder.add_data_raw(
                        abi_type,
                        change_seal.clone(),
                        RevealedData::new(Confined::try_from(data).map_err(|e| {
                            CompositionError::Unexpected(format!("String RevealedData: {e}"))
                        })?),
                    )?;
                }
                other => {
                    return Err(CompositionError::Unexpected(format!(
                        "ABI 'change' at index {i}: unsupported reg {other} (expected a64, r256 or s16)"
                    )));
                }
            },
            "owner_state" => {
                // 解析state的格式为"txid:vout"，其后必须跟一个state值，然后一同添加到main_builder中
                let outpoint = parse_outpoint(outr_value)?;
                let owner_seal = BuilderSeal::Revealed(GraphSeal::rand_from(outpoint));
                let outr_value = &outputs[j];
                j = j + 1;
                match abi_reg {
                    "a64" => {
                        let amount = parse_amount(outr_value)?;
                        main_builder = main_builder.add_fungible_state_raw(
                            abi_type,
                            owner_seal,
                            amount,
                        )?;
                    }
                    "a8" => {
                        // if *outr_value == OutrValue::Int(0) {
                        //     return Err(CompositionError::Unexpected(format!(
                        //         "ABI 'owner_state' at index {i}: expected a8 to be non-zero",
                        //     )));
                        // }
                        main_builder = main_builder.add_rights_raw(abi_type, owner_seal)?;
                    }
                    "r256" => {
                        let hash = parse_hash256(outr_value)?;
                        let payload = Confined::try_from_iter(hash.iter().copied()).map_err(|e| {
                            CompositionError::Unexpected(format!("OS_HASH RevealedData: {e}"))
                        })?;
                        main_builder = main_builder.add_data_raw(
                            abi_type,
                            owner_seal,
                            RevealedData::new(payload),
                        )?;
                    }
                    "outpoint" => {
                        let raw = parse_outpoint_payload(outr_value)?;
                        let payload = Confined::try_from_iter(raw.into_iter()).map_err(|e| {
                            CompositionError::Unexpected(format!("OS_OUTPOINT RevealedData: {e}"))
                        })?;
                        main_builder = main_builder.add_data_raw(
                            abi_type,
                            owner_seal,
                            RevealedData::new(payload),
                        )?;
                    }
                    "s16" => {
                        let data = parse_s16_payload(consignment, abi_type, outr_value)?;
                        main_builder = main_builder.add_data_raw(
                            abi_type,
                            owner_seal,
                            RevealedData::new(Confined::try_from(data).map_err(|e| {
                                CompositionError::Unexpected(format!("String RevealedData: {e}"))
                            })?),
                        )?;
                    }
                    other => {
                        return Err(CompositionError::Unexpected(format!(
                            "ABI 'owner_state' at index {i}: unsupported reg {other} (expected a64, a8, r256, outpoint or s16)",
                        )));
                    }
                }
            }
            // "amount" => {
            //     if abi_reg != "a64" {
            //         return Err(CompositionError::Unexpected(format!(
            //             "ABI 'amount' at index {i}: expected reg {abi_reg} (expected a64)",
            //         )));
            //     }
            //     let local_stashed_seal = stashed_seal.ok_or_else(|| {
            //         CompositionError::Unexpected(
            //             "state 'amount' encountered before 'owner'".to_string(),
            //         )
            //     })?;
            //     let amount = parse_amount(outr_value)?;
            //     main_builder = main_builder.add_fungible_state_raw(
            //         abi_type,
            //         local_stashed_seal,
            //         amount,
            //     )?;
            //     stashed_seal = None;
            // }
            _ => {
                return Err(CompositionError::Unexpected(format!(
                    "unknown ABI name {:?} at index {i}",
                    abi_name
                )));
            }
        }
    }
    Ok(main_builder)
}

/// Extension trait: call `validate_ext` after `use ...::ConsignmentValidateExt`.
// pub trait ConsignmentValidateExt<const TRANSFER: bool> {
//     fn validate_ext(
//         self,
//         resolver: &impl ResolveWitness,
//         validation_config: &ValidationConfig,
//         extra_states: Option<Vec<MemContractState>>,
//     ) -> Result<ValidConsignment<TRANSFER>, ValidationError>;
// }

// impl<const TRANSFER: bool> ConsignmentValidateExt<TRANSFER> for Consignment<TRANSFER> {
//     fn validate_ext(
//         self,
//         resolver: &impl ResolveWitness,
//         validation_config: &ValidationConfig,
//         extra_states: Option<Vec<MemContractState>>,
//     ) -> Result<ValidConsignment<TRANSFER>, ValidationError> {
//         let _ = extra_states;
//         self.validate_ext(resolver, validation_config, extra_states)
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use strict_types::StrictDeserialize;

    #[test]
    fn parse_outpoint_payload_roundtrip() {
        let txid =
            "c5c3f8d1d75c39c1ff537f3f96286ab15fcd58ffdf2d66e9d869c52f55ddb35d";
        let vout = 1u32;
        let raw = format!("{txid}:{vout}").into_bytes();
        let outr = OutrValue::Bytes(raw);

        let payload = parse_outpoint_payload(&outr).expect("must parse valid outpoint");
        let payload = Confined::try_from(payload).expect("payload must fit confinement");
        let decoded = Outpoint::from_strict_serialized::<U32>(payload)
            .expect("must decode strict-serialized outpoint");

        assert_eq!(decoded.txid.to_string(), txid);
        assert_eq!(decoded.vout, vout);
    }

    #[test]
    fn parse_outpoint_payload_rejects_invalid_format() {
        let outr = OutrValue::Bytes(b"not-an-outpoint".to_vec());
        assert!(parse_outpoint_payload(&outr).is_err());
    }
}
