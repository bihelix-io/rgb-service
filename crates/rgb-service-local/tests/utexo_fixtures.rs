//! Cross-implementation fixtures from UTEXO Node SDK and Rust reference wallets.
//! These offline tests cover wire compatibility; live validation is separate.
use rgb_service_local::{decode_rgb20_transfer_consignment, encode_rgb20_transfer_consignment};
use rgbstd::{
    containers::ConsignmentExt,
    invoice::{Beneficiary, InvoiceState, RgbInvoice},
    ChainNet,
};
use std::str::FromStr;

const PROOF: &[u8] =
    include_bytes!("../../../tests/fixtures/utexo/reference-to-bihelix.consignment");

#[test]
fn daemon_http_roundtrip_preserves_history_and_targets_the_blind_invoice() {
    let proofs: [(&[u8], usize); 3] = [
        (
            include_bytes!("../../../tests/fixtures/utexo-daemon/reference-to-a.consignment"),
            107,
        ),
        (
            include_bytes!("../../../tests/fixtures/utexo-daemon/a-to-b.consignment"),
            108,
        ),
        (
            include_bytes!("../../../tests/fixtures/utexo-daemon/b-to-reference.consignment"),
            109,
        ),
    ];
    for (bytes, bundles) in proofs {
        let transfer = decode_rgb20_transfer_consignment(bytes).unwrap();
        assert_eq!(
            transfer.contract_id().to_string(),
            "rgb:f~9F4X0C-TiLOTvy-pALF29V-2xJ2p0m-hP3_vpW-Alj4G5Y"
        );
        assert_eq!(
            transfer.schema_id().to_string(),
            "rgb:sch:IpjJhFLz3oywYKQxO3KmFgR0Aa415nlTNrNyEFqMZCE#shoe-colombo-mango"
        );
        assert_eq!(transfer.bundles.len(), bundles);
        assert_eq!(encode_rgb20_transfer_consignment(&transfer).unwrap(), bytes);
    }
    let invoice = RgbInvoice::from_str(
        include_str!("../../../tests/fixtures/utexo-daemon/b-blind.invoice").trim(),
    )
    .unwrap();
    assert_eq!(invoice.chain_network(), ChainNet::BitcoinSignet);
    assert_eq!(
        invoice.assignment_state,
        Some(InvoiceState::Amount(200000u64.into()))
    );
    let Beneficiary::BlindedSeal(seal) = invoice.beneficiary.into_inner() else {
        panic!("expected the actual daemon blind invoice");
    };
    let transfer = decode_rgb20_transfer_consignment(proofs[1].0).unwrap();
    assert!(transfer
        .terminals
        .values()
        .any(|seals| seals.contains(&seal)));
}

#[test]
fn official_test_usdt_ifa_roundtrip_preserves_contract_and_wire_format() {
    let fixtures: [(&[u8], usize); 3] = [
        (
            include_bytes!("../../../tests/fixtures/utexo-usdt/official-faucet-usdt.consignment"),
            104,
        ),
        (
            include_bytes!(
                "../../../tests/fixtures/utexo-usdt/usdt-reference-to-carol.consignment"
            ),
            105,
        ),
        (
            include_bytes!("../../../tests/fixtures/utexo-usdt/return.consignment"),
            106,
        ),
    ];
    for (bytes, bundles) in fixtures {
        let transfer = decode_rgb20_transfer_consignment(bytes).unwrap();
        assert_eq!(
            transfer.contract_id().to_string(),
            "rgb:f~9F4X0C-TiLOTvy-pALF29V-2xJ2p0m-hP3_vpW-Alj4G5Y"
        );
        assert_eq!(
            transfer.schema_id().to_string(),
            "rgb:sch:IpjJhFLz3oywYKQxO3KmFgR0Aa415nlTNrNyEFqMZCE#shoe-colombo-mango"
        );
        assert_eq!(transfer.bundles.len(), bundles);
        assert_eq!(encode_rgb20_transfer_consignment(&transfer).unwrap(), bytes);
    }
}

#[test]
fn utexo_transfer_preserves_contract_schema_and_wire_format() {
    let transfer = decode_rgb20_transfer_consignment(PROOF).unwrap();
    assert_eq!(
        transfer.contract_id().to_string(),
        "rgb:b_dR~5l9-L9C80KW-7DXKfZg-fdnamKh-MZs88LZ-aRSDBRs"
    );
    assert_eq!(
        transfer.schema_id().to_string(),
        "rgb:sch:RWhwUfTMpuP2Zfx1~j4nswCANGeJrYOqDcKelaMV4zU#remote-digital-pegasus"
    );
    assert_eq!(transfer.bundles.len(), 1);
    assert_eq!(encode_rgb20_transfer_consignment(&transfer).unwrap(), PROOF);
    let mut invalid = PROOF.to_vec();
    invalid[0] ^= 1;
    assert!(decode_rgb20_transfer_consignment(&invalid).is_err());
    assert!(decode_rgb20_transfer_consignment(&PROOF[..PROOF.len() / 2]).is_err());
}

#[test]
fn utexo_blind_and_witness_invoices_preserve_semantics() {
    for (text, blind) in [
        (
            include_str!("../../../tests/fixtures/utexo/reference-blind.invoice"),
            true,
        ),
        (
            include_str!("../../../tests/fixtures/utexo/reference-witness.invoice"),
            false,
        ),
    ] {
        let invoice = RgbInvoice::from_str(text.trim()).unwrap();
        assert_eq!(invoice.chain_network(), ChainNet::BitcoinSignet);
        assert_eq!(
            invoice.assignment_state,
            Some(InvoiceState::Amount(42u64.into()))
        );
        assert_eq!(
            matches!(
                invoice.beneficiary.into_inner(),
                Beneficiary::BlindedSeal(_)
            ),
            blind
        );
        assert_eq!(invoice.transports.len(), 1);
        assert_eq!(invoice.to_string(), text.trim());
    }
}

#[test]
fn rejects_the_actual_carrier_rejected_by_utexo_before_building_a_transfer() {
    let bytes = include_bytes!("../../../tests/fixtures/utexo/rejected-taproot-first.psbt");
    let mut psbt = bitcoin::Psbt::deserialize(bytes).unwrap();
    let error = rgb_service_local::validate_rgb20_external_carrier(&psbt).unwrap_err();
    assert!(error.to_string().contains("precede every Taproot"));
    // This ordering change is only a preflight test, not a re-signable RGB proof.
    let opret = psbt.unsigned_tx.output.pop().unwrap();
    assert!(opret.script_pubkey.is_op_return());
    psbt.unsigned_tx.output.insert(0, opret);
    rgb_service_local::validate_rgb20_external_carrier(&psbt).unwrap();
    psbt.unsigned_tx.output.remove(0);
    assert!(rgb_service_local::validate_rgb20_external_carrier(&psbt).is_err());
    let accepted = bitcoin::Psbt::deserialize(include_bytes!(
        "../../../tests/fixtures/utexo/accepted-opret-first.psbt"
    ))
    .unwrap();
    rgb_service_local::validate_rgb20_external_carrier(&accepted).unwrap();
}

#[test]
fn roundtrip_fixture_preserves_the_same_contract_and_two_witness_bundles() {
    let incoming = decode_rgb20_transfer_consignment(include_bytes!(
        "../../../tests/fixtures/utexo/reference-to-bob.consignment"
    ))
    .unwrap();
    let outgoing = decode_rgb20_transfer_consignment(include_bytes!(
        "../../../tests/fixtures/utexo/bihelix-to-reference.consignment"
    ))
    .unwrap();
    assert_eq!(incoming.contract_id(), outgoing.contract_id());
    assert_eq!(
        outgoing.contract_id().to_string(),
        "rgb:LD5JyIwl-UJQCAdg-YRoXiaa-2Q8yV8V-Ry01N26-bNcbx40"
    );
    assert_eq!(incoming.bundles.len(), 1);
    assert_eq!(outgoing.bundles.len(), 2);
}
