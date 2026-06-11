use aluvm::{aluasm, aluasm_rgb};
use aluvm::isa::{ControlFlowOp, Instr, OutstackOp};

#[test]
fn aluasm_rgb_outr_mnemonics() {
    let code = aluasm! {
        outr    a64[7]                        ;
        outr    s16[2]                        ;
        outr    40                            ;
        ret                                     ;
    };
    assert_eq!(code.len(), 4);
    assert!(matches!(code[0], Instr::Outstack(OutstackOp::Outr(7))));
    assert!(matches!(code[1], Instr::Outstack(OutstackOp::Outr(34))));
    assert!(matches!(code[2], Instr::Outstack(OutstackOp::Outr(40))));
    assert!(matches!(code[3], Instr::ControlFlow(ControlFlowOp::Ret)));
}

#[test]
fn aluasm_rgb_outr_mixed_with_base_instr() {
    let code = aluasm_rgb! {
        outr    a64[3]                        ;
        mov     r256[1],r256[2]               ;
        outr    s16[2]                        ;
        ret                                  ;
    };
    assert_eq!(code.len(), 4);
    assert!(matches!(code[0], Instr::Outstack(OutstackOp::Outr(3))));
    assert!(matches!(code[1], Instr::Move(_)));
    assert!(matches!(code[2], Instr::Outstack(OutstackOp::Outr(34))));
    assert!(matches!(code[3], Instr::ControlFlow(ControlFlowOp::Ret)));
}

#[test]
fn aluasm_rgb_outr_mixed_with_control_flow() {
    let code = aluasm_rgb! {
        outr    s16[2]                        ;
        jif     2                             ;
        outr    40                            ;
        ret                                  ;
    };
    assert_eq!(code.len(), 4);
    assert!(matches!(code[0], Instr::Outstack(OutstackOp::Outr(34))));
    assert!(matches!(code[1], Instr::ControlFlow(ControlFlowOp::Jif(2))));
    assert!(matches!(code[2], Instr::Outstack(OutstackOp::Outr(40))));
    assert!(matches!(code[3], Instr::ControlFlow(ControlFlowOp::Ret)));
}
