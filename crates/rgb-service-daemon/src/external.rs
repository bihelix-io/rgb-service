//! Durable external L1 RGB operations. No wallet private keys are held here.
use super::*;
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bitcoin::{CompressedPublicKey, TxOut};
use fjall::Readable;
use rgb_service_api::{
    ExternalRgbAction as Action, ExternalRgbOperation as View, ExternalRgbRequest,
    ExternalRgbResponse,
};
use rgb_service_local::{
    decode_rgb20_transfer_consignment, encode_rgb20_transfer_consignment,
    prepare_external_rgb_transfer, preview_external_rgb_receive,
    rgbstd::{
        containers::ConsignmentExt,
        invoice::{
            AddressPayload, Beneficiary, InvoiceState, Pay2Vout, RgbInvoice, RgbInvoiceBuilder,
            XChainNet,
        },
        rgbcore::commit_verify::Conceal,
        GraphSeal,
    },
    store_external_rgb_receive_secret, ExternalRgbSeal,
};
use std::{
    io::Read,
    net::{IpAddr, ToSocketAddrs},
};

const OPS: &str = "external_rgb_operations_v1";
const ARTIFACTS: &str = "external_rgb_artifacts_v1";
const IDEM: &str = "external_rgb_idempotency_v1";
const RESERVED: &str = "external_rgb_reservations_v1";
const ACCOUNTS: &str = "external_rgb_accounts_v1";
const MAX_PSBT: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct ExternalConfig {
    pub enabled: bool,
    pub proxy_allowlist: Vec<String>,
    pub max_consignment_bytes: usize,
    pub request_timeout_secs: u64,
    pub required_confirmations: u32,
    pub max_operations: usize,
}
impl Default for ExternalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            proxy_allowlist: vec![],
            max_consignment_bytes: 2 * 1024 * 1024,
            request_timeout_secs: 30,
            required_confirmations: 1,
            max_operations: 10000,
        }
    }
}
impl ExternalConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        anyhow::ensure!(
            !self.proxy_allowlist.is_empty(),
            "external RGB proxy allowlist is required"
        );
        anyhow::ensure!(
            (1..=100).contains(&self.required_confirmations),
            "invalid confirmation policy"
        );
        anyhow::ensure!(
            (1..=120).contains(&self.request_timeout_secs),
            "invalid external timeout"
        );
        anyhow::ensure!(
            (1024..=16 * 1024 * 1024).contains(&self.max_consignment_bytes),
            "invalid proof size limit"
        );
        anyhow::ensure!(
            (1..=100000).contains(&self.max_operations),
            "invalid operation limit"
        );
        for url in &self.proxy_allowlist {
            proxy_url(url)?;
        }
        Ok(())
    }
}

fn invalid(s: impl ToString) -> anyhow::Error {
    anyhow!(RgbServiceError::InvalidRequest(s.to_string()))
}
fn conflict(s: impl ToString) -> anyhow::Error {
    anyhow!(RgbServiceError::Conflict(s.to_string()))
}
fn classify(e: anyhow::Error) -> RgbServiceError {
    match e.downcast::<RgbServiceError>() {
        Ok(e) => e,
        Err(e) => RgbServiceError::Backend(format!("{e:#}")),
    }
}
fn digest(bytes: &[u8]) -> String {
    sha256::Hash::hash(bytes).to_string()
}
fn timestamp() -> u64 {
    now_ms() / 1000
}
fn scoped(account: &str, id: &str) -> String {
    format!("{account}/{id}")
}
fn identifier(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(invalid(
            "identifier must contain 1..128 letters, digits, '-' or '_'",
        ));
    }
    Ok(())
}

/// Prove ownership from the verified request key, never from signer_id text.
pub(super) fn verify_account_key(account: &str, key: &str) -> rgb_service_api::Result<()> {
    let address = Address::from_str(account)
        .map_err(|e| RgbServiceError::Forbidden(e.to_string()))?
        .assume_checked();
    let pk = PublicKey::from_str(key).map_err(|e| RgbServiceError::Forbidden(e.to_string()))?;
    let p2wpkh = Address::p2wpkh(&CompressedPublicKey(pk), Network::Bitcoin).script_pubkey();
    let p2tr = Address::p2tr(
        &Secp256k1::verification_only(),
        pk.x_only_public_key().0,
        None,
        Network::Bitcoin,
    )
    .script_pubkey();
    if address.script_pubkey() != p2wpkh && address.script_pubkey() != p2tr {
        return Err(RgbServiceError::Forbidden(
            "request key does not own P2WPKH/BIP86 account address".into(),
        ));
    }
    Ok(())
}

pub(super) fn guard_legacy_account(
    db: &SingleWriterTxDatabase,
    account: &str,
) -> rgb_service_api::Result<()> {
    let keys = db
        .keyspace(ACCOUNTS, KeyspaceCreateOptions::default)
        .map_err(|e| RgbServiceError::Backend(e.to_string()))?;
    if keys
        .contains_key(account)
        .map_err(|e| RgbServiceError::Backend(e.to_string()))?
    {
        return Err(RgbServiceError::Conflict(
            "account uses external RGB operations; use /v1/external for spends".into(),
        ));
    }
    Ok(())
}

pub(super) struct AccountProjection {
    pub quarantined: bool,
    pub reserved: BTreeSet<String>,
    pub pending: Vec<rgb_service_api::PendingOperation>,
}
impl AccountProjection {
    pub fn allocation_status(&self, outpoint: &str, confirmed: bool) -> AllocationStatus {
        if self.quarantined {
            AllocationStatus::Locked
        } else if self.reserved.contains(outpoint) {
            AllocationStatus::Reserved
        } else if !confirmed {
            AllocationStatus::PendingIn
        } else {
            AllocationStatus::Available
        }
    }
}
pub(super) fn account_projection(
    db: &SingleWriterTxDatabase,
    account: &str,
) -> rgb_service_api::Result<AccountProjection> {
    (|| -> Result<AccountProjection> {
        let keys = db.keyspace(OPS, KeyspaceCreateOptions::default)?;
        let mut result = AccountProjection {
            quarantined: false,
            reserved: BTreeSet::new(),
            pending: vec![],
        };
        for entry in keys.as_ref().prefix(format!("{account}/")) {
            let (_, bytes) = entry.into_inner()?;
            let r: Record = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                r.schema_version == 1,
                "unsupported external operation schema"
            );
            result.quarantined |= r.quarantine_account;
            if r.view.status == "settled"
                || (r.view.status == "cancelled" && r.view.direction == "send")
            {
                continue;
            }
            result.reserved.extend(r.inputs);
            if r.view.status == "cancelled" {
                continue;
            }
            let status = match r.view.status.as_str() {
                "preparing" => OperationStatus::Reserved,
                "awaiting_signature" => OperationStatus::Prepared,
                "needs_review" => OperationStatus::RecoveryRequired,
                _ => OperationStatus::Pending,
            };
            result.pending.push(rgb_service_api::PendingOperation {
                operation_id: r.view.operation_id,
                asset_id: Some(r.view.asset_id),
                amount: Some(r.view.amount),
                status,
                layer: Some(AssetLayer::L1),
                related_txid: r.view.txid,
                related_l2_ref: None,
            });
        }
        Ok(result)
    })()
    .map_err(classify)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Record {
    schema_version: u8,
    account: String,
    view: View,
    proxy: String,
    expires_at: u64,
    blind_secret: Option<String>,
    receive_outpoint: Option<String>,
    recipient_vout: Option<u32>,
    change_vout: Option<u32>,
    inputs: Vec<String>,
    #[serde(skip)]
    fascia: Vec<u8>,
    #[serde(skip)]
    proof: Vec<u8>,
    candidate_hex: Option<String>,
    signed_hex: Option<String>,
    next_retry_at: u64,
    attempts: u32,
    #[serde(default)]
    quarantine_account: bool,
}

#[derive(Clone)]
pub(super) struct Engine {
    db: SingleWriterTxDatabase,
    config: ExternalConfig,
    network: Network,
    chain: String,
    kv: PathBuf,
    mutation: Arc<Mutex<()>>,
}
impl Engine {
    pub fn new(service: &LocalDaemonService) -> rgb_service_api::Result<Self> {
        if !service.config.external_rgb.enabled {
            return Err(RgbServiceError::NotImplemented(
                "external RGB is disabled".into(),
            ));
        }
        if rgb_service_local::is_electrum_url(&service.config.esplora_url) {
            return Err(RgbServiceError::InvalidRequest(
                "external RGB requires an Esplora HTTP backend".into(),
            ));
        }
        Ok(Self {
            db: service.db.clone(),
            config: service.config.external_rgb.clone(),
            network: service.network()?,
            chain: service.config.esplora_url.trim_end_matches('/').into(),
            kv: service.config.data_dir.join("kv"),
            mutation: service.external_mutation.clone(),
        })
    }
    fn stock(&self, account: &str) -> PathBuf {
        shared_rgb_store_locator(&self.kv, account)
    }
    fn account_address(&self, account: &str) -> Result<Address> {
        let address = Address::from_str(account)
            .map_err(invalid)?
            .require_network(self.network)
            .map_err(invalid)?;
        if address.to_string() != account {
            return Err(invalid("canonical lowercase account address required"));
        }
        Ok(address)
    }
    fn keys(&self, name: &str) -> Result<fjall::SingleWriterTxKeyspace> {
        Ok(self.db.keyspace(name, KeyspaceCreateOptions::default)?)
    }
    fn load(&self, account: &str, id: &str) -> Result<Record> {
        identifier(id)?;
        let bytes = self.keys(OPS)?.get(scoped(account, id))?.ok_or_else(|| {
            anyhow!(RgbServiceError::NotFound(
                "external operation not found".into()
            ))
        })?;
        let mut record: Record = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            record.schema_version == 1 && record.account == account,
            "unsupported or corrupt external record"
        );
        if let Some(bytes) = self.keys(ARTIFACTS)?.get(scoped(account, id))? {
            (record.fascia, record.proof) = serde_json::from_slice(&bytes)?;
        }
        Ok(record)
    }
    fn save(&self, r: &Record, idem: Option<(&str, &str)>, reserve: bool) -> Result<()> {
        let operations = self.keys(OPS)?;
        let artifacts = self.keys(ARTIFACTS)?;
        let keys = self.keys(IDEM)?;
        let reservations = self.keys(RESERVED)?;
        let accounts = self.keys(ACCOUNTS)?;
        let mut tx = self.db.write_tx();
        if let Some((key, hash)) = idem {
            if let Some(bytes) = tx.get(&keys, key)? {
                let old: (String, String) = serde_json::from_slice(&bytes)?;
                if old != (hash.into(), r.view.operation_id.clone()) {
                    return Err(conflict("request_id already used with another request"));
                }
            }
            tx.insert(
                &keys,
                key,
                serde_json::to_vec(&(hash, &r.view.operation_id))?,
            );
        }
        if reserve {
            for input in &r.inputs {
                // Daemon config has one chain; key is global across accounts.
                if let Some(owner) = tx.get(&reservations, input)? {
                    if owner.as_ref() != r.view.operation_id.as_bytes() {
                        return Err(conflict(format!("input reserved: {input}")));
                    }
                }
                tx.insert(&reservations, input, r.view.operation_id.as_bytes());
            }
        }
        if r.view.direction == "send" && r.view.status == "cancelled" {
            for input in &r.inputs {
                if tx
                    .get(&reservations, input)?
                    .is_some_and(|v| v.as_ref() == r.view.operation_id.as_bytes())
                {
                    tx.remove(&reservations, input);
                }
            }
        }
        if !r.fascia.is_empty() || !r.proof.is_empty() {
            let key = scoped(&r.account, &r.view.operation_id);
            let bytes = serde_json::to_vec(&(&r.fascia, &r.proof))?;
            if tx
                .get(&artifacts, &key)?
                .is_none_or(|v| v.as_ref() != bytes.as_slice())
            {
                tx.insert(&artifacts, key, bytes);
            }
        }
        tx.insert(&accounts, &r.account, b"external-v1");
        tx.insert(
            &operations,
            scoped(&r.account, &r.view.operation_id),
            serde_json::to_vec(r)?,
        );
        tx.commit()?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }
    fn replay(
        &self,
        account: &str,
        kind: &str,
        request_id: &str,
        hash: &str,
    ) -> Result<Option<Record>> {
        identifier(request_id)?;
        let key = scoped(account, &format!("{kind}/{request_id}"));
        let Some(bytes) = self.keys(IDEM)?.get(&key)? else {
            return Ok(None);
        };
        let (old, id): (String, String) = serde_json::from_slice(&bytes)?;
        if old != hash {
            return Err(conflict(
                "request_id already used with different parameters",
            ));
        }
        Ok(Some(self.load(account, &id)?))
    }
    fn new_record(
        &self,
        account: &str,
        id: String,
        invoice: &RgbInvoice,
        amount: u64,
        direction: &str,
    ) -> Result<Record> {
        if self.keys(OPS)?.as_ref().len()? >= self.config.max_operations {
            return Err(conflict("external operation capacity reached"));
        }
        // Existing prepared operations cannot be safely inferred or migrated.
        if self
            .keys("prepared_transfers")?
            .as_ref()
            .prefix(format!("{account}:"))
            .next()
            .is_some()
            || LocalRgbStore::open(&self.stock(account))?.has_active_pending()?
        {
            return Err(conflict(
                "account has legacy prepared/pending operations; reconcile before external use",
            ));
        }
        let proxy = self.invoice_proxy(invoice)?;
        Ok(Record {
            schema_version: 1,
            account: account.into(),
            view: View {
                operation_id: id,
                direction: direction.into(),
                status: if direction == "receive" {
                    "awaiting_transfer"
                } else {
                    "preparing"
                }
                .into(),
                asset_id: invoice
                    .contract
                    .ok_or_else(|| invalid("contract required"))?
                    .to_string(),
                amount,
                invoice: invoice.to_string(),
                recipient_id: invoice.beneficiary.to_string(),
                txid: None,
                anchor_psbt: None,
                delivery_status: "not_started".into(),
                broadcast_status: "not_started".into(),
                acknowledged: None,
                confirmations: 0,
                required_confirmations: self.config.required_confirmations,
                retryable: true,
                last_error: None,
                created_at: timestamp(),
                updated_at: timestamp(),
            },
            proxy,
            expires_at: invoice
                .expiry
                .ok_or_else(|| invalid("invoice expiry required"))? as u64,
            blind_secret: None,
            receive_outpoint: None,
            recipient_vout: None,
            change_vout: None,
            inputs: vec![],
            fascia: vec![],
            proof: vec![],
            candidate_hex: None,
            signed_hex: None,
            next_retry_at: 0,
            attempts: 0,
            quarantine_account: false,
        })
    }
    fn invoice_proxy(&self, invoice: &RgbInvoice) -> Result<String> {
        if invoice.transports.len() != 1 {
            return Err(invalid("exactly one configured proxy transport required"));
        }
        let encoded = invoice.transports[0].to_string();
        let url = encoded
            .strip_prefix("rpcs://")
            .map(|s| format!("https://{s}"))
            .ok_or_else(|| invalid("HTTPS RGB proxy required"))?;
        if !self.config.proxy_allowlist.contains(&url) {
            return Err(invalid("invoice proxy is not allowlisted"));
        }
        Ok(url)
    }
    pub fn execute(
        &self,
        request: ExternalRgbRequest,
    ) -> rgb_service_api::Result<ExternalRgbResponse> {
        if matches!(&request.action, Action::Get { .. } | Action::List { .. }) {
            return self.execute_locked(request).map_err(classify);
        }
        let _lock = self
            .mutation
            .lock()
            .map_err(|e| RgbServiceError::Backend(e.to_string()))?;
        self.execute_locked(request).map_err(classify)
    }
    fn execute_locked(&self, request: ExternalRgbRequest) -> Result<ExternalRgbResponse> {
        let account = request.account_id;
        let address = self.account_address(&account)?;
        let mut semantic = serde_json::to_value(&request.action)?;
        // Re-signing an identical business action is not a new payment.
        if let Some(m) = semantic.as_object_mut() {
            m.remove("asset_authorization");
        }
        let hash = digest(&serde_json::to_vec(&semantic)?);
        let view = match request.action {
            Action::Receive {
                request_id,
                asset_id,
                amount,
                expires_at,
                blind_outpoint,
            } => {
                if let Some(r) = self.replay(&account, "receive", &request_id, &hash)? {
                    return Ok(one(r.view));
                }
                if amount == 0 || expires_at <= timestamp() || expires_at > timestamp() + 7 * 86400
                {
                    return Err(invalid(
                        "positive amount and expiry within seven days required",
                    ));
                }
                let contract = RgbContractId::from_str(&asset_id).map_err(invalid)?;
                let secret = if let Some(outpoint) = &blind_outpoint {
                    let outpoint = OutPoint::from_str(outpoint).map_err(invalid)?;
                    self.owned_unspent(outpoint, &address)?;
                    if list_rgb20_assets_for_utxos(
                        &self.stock(&account),
                        [Rgb20TrackedUtxo {
                            outpoint,
                            address: None,
                            confirmed: true,
                        }],
                    )?
                    .iter()
                    .any(|a| a.amount_raw > 0)
                    {
                        return Err(conflict("blind receive requires an unallocated UTXO"));
                    }
                    Some(GraphSeal::new_random(outpoint.txid, outpoint.vout))
                } else {
                    None
                };
                let beneficiary = match secret {
                    Some(s) => Beneficiary::BlindedSeal(s.conceal()),
                    None => Beneficiary::WitnessVout(
                        Pay2Vout::new(
                            AddressPayload::from_script(&address.script_pubkey())
                                .map_err(invalid)?,
                        ),
                        None,
                    ),
                };
                let transport = self.config.proxy_allowlist[0].replacen("https://", "rpcs://", 1);
                let invoice = RgbInvoiceBuilder::with(
                    contract,
                    XChainNet::bitcoin(self.network, beneficiary),
                )
                .set_amount_raw(amount)
                .set_assignment_name("assetOwner")
                .set_expiry_timestamp(expires_at as i64)
                .add_transport(transport.as_str())
                .map_err(|(_, e)| invalid(e))?
                .finish();
                // Witness recipient IDs derive from the address; never reuse a proxy slot.
                for entry in self.keys(OPS)?.as_ref().prefix(format!("{account}/")) {
                    let (_, bytes) = entry.into_inner()?;
                    let existing: Record = serde_json::from_slice(&bytes)?;
                    if existing.view.direction == "receive"
                        && existing.view.recipient_id == invoice.beneficiary.to_string()
                    {
                        return Err(conflict("witness recipient already used; use a new account address or a blind invoice"));
                    }
                }
                let id = digest(scoped(&account, &format!("receive/{request_id}")).as_bytes());
                let mut r = self.new_record(&account, id, &invoice, amount, "receive")?;
                r.blind_secret = secret.map(|s| s.to_string());
                r.receive_outpoint = blind_outpoint;
                if let Some(outpoint) = &r.receive_outpoint {
                    r.inputs.push(outpoint.clone());
                }
                self.save(
                    &r,
                    Some((&scoped(&account, &format!("receive/{request_id}")), &hash)),
                    true,
                )?;
                r.view
            }
            Action::Prepare {
                request_id,
                invoice,
                unsigned_anchor_psbt,
                recipient_vout,
                change_vout,
                asset_authorization,
            } => {
                if let Some(r) = self.replay(&account, "prepare", &request_id, &hash)? {
                    return Ok(one(r.view));
                }
                for entry in self.keys(OPS)?.as_ref().prefix(format!("{account}/")) {
                    let (_, bytes) = entry.into_inner()?;
                    let prior: Record = serde_json::from_slice(&bytes)?;
                    if prior.quarantine_account {
                        return Err(conflict("account has an operation requiring review"));
                    }
                }
                let invoice = RgbInvoice::from_str(&invoice).map_err(invalid)?;
                let amount = invoice_amount(&invoice, self.network)?;
                if let Some(expected) = invoice.schema {
                    let actual = rgb_service_local::external_rgb_schema(
                        &self.stock(&account),
                        invoice.contract.context("contract missing")?,
                    )?;
                    if expected != actual {
                        return Err(invalid("invoice schema differs from stored contract"));
                    }
                }
                check_authorization(
                    &asset_authorization,
                    &invoice,
                    amount,
                    &unsigned_anchor_psbt,
                )?;
                let psbt = parse_psbt(&unsigned_anchor_psbt)?;
                validate_carrier(&psbt, &address, &invoice, recipient_vout, change_vout)?;
                for input in &psbt.unsigned_tx.input {
                    let actual = self.owned_unspent(input.previous_output, &address)?;
                    let n = psbt
                        .unsigned_tx
                        .input
                        .iter()
                        .position(|i| i.previous_output == input.previous_output)
                        .unwrap();
                    if psbt.inputs[n].witness_utxo.as_ref() != Some(&actual) {
                        return Err(invalid("PSBT witness_utxo differs from chain"));
                    }
                }
                let allocations = list_rgb20_assets_for_utxos(
                    &self.stock(&account),
                    psbt.unsigned_tx.input.iter().map(|i| Rgb20TrackedUtxo {
                        outpoint: i.previous_output,
                        address: None,
                        confirmed: true,
                    }),
                )?;
                for allocation in allocations {
                    let key = LocalDaemonService::account_utxo_key(
                        &account,
                        &allocation.outpoint.to_string(),
                    );
                    let bytes = self
                        .keys("account_utxos")?
                        .get(key)?
                        .ok_or_else(|| conflict("RGB input has not completed daemon settlement"))?;
                    let tracked: TrackedUtxo = serde_json::from_slice(&bytes)?;
                    if !tracked.confirmed {
                        return Err(conflict("RGB input requires confirmation recovery"));
                    }
                }
                let seal = match invoice.beneficiary.clone().into_inner() {
                    Beneficiary::BlindedSeal(s) => ExternalRgbSeal::Blind(s),
                    Beneficiary::WitnessVout(_, None) => ExternalRgbSeal::Witness(
                        recipient_vout.ok_or_else(|| invalid("recipient_vout required"))?,
                    ),
                    _ => return Err(invalid("tapret recipient not supported")),
                };
                let id = digest(scoped(&account, &format!("prepare/{request_id}")).as_bytes());
                let mut r = self.new_record(&account, id, &invoice, amount, "send")?;
                r.inputs = psbt
                    .unsigned_tx
                    .input
                    .iter()
                    .map(|i| i.previous_output.to_string())
                    .collect();
                r.change_vout = Some(change_vout);
                r.recipient_vout = recipient_vout;
                r.candidate_hex = Some(unsigned_anchor_psbt.clone()); // Durable preparation input for crash recovery.
                let idem = scoped(&account, &format!("prepare/{request_id}"));
                self.save(&r, Some((&idem, &hash)), true)?;
                match self.finish_preparation(&mut r, psbt, seal) {
                    Ok(()) => {}
                    Err(e) => {
                        r.view.last_error = Some(format!("{e:#}"));
                        self.save(&r, None, false)?;
                        return Err(e);
                    }
                }
                r.view
            }
            Action::Finalize {
                request_id,
                operation_id,
                signed_anchor_psbt,
                asset_authorization,
            } => {
                if let Some(r) = self.replay(&account, "finalize", &request_id, &hash)? {
                    return Ok(one(r.view));
                }
                let mut r = self.load(&account, &operation_id)?;
                if r.view.direction != "send" {
                    return Err(invalid("not a send operation"));
                }
                let invoice = RgbInvoice::from_str(&r.view.invoice).map_err(invalid)?;
                let prepared = r
                    .view
                    .anchor_psbt
                    .as_ref()
                    .ok_or_else(|| conflict("preparation is not complete"))?;
                check_authorization(&asset_authorization, &invoice, r.view.amount, prepared)?;
                let signed = parse_psbt(&signed_anchor_psbt)?;
                let prepared = parse_psbt(prepared)?;
                let tx = finalized_transaction(&prepared, &signed)?;
                let raw = hex_encode(&serialize(&tx));
                if let Some(old) = &r.signed_hex {
                    if old != &raw {
                        return Err(conflict(
                            "operation already finalized with a different signed transaction",
                        ));
                    }
                } else {
                    if r.view.status != "awaiting_signature" || timestamp() >= r.expires_at {
                        return Err(conflict("operation is not awaiting a valid signature"));
                    }
                    r.signed_hex = Some(raw);
                    r.view.status = "delivering".into();
                    r.view.retryable = true;
                }
                self.save(
                    &r,
                    Some((&scoped(&account, &format!("finalize/{request_id}")), &hash)),
                    false,
                )?;
                // Network side effects are performed by the recovery worker, after durable authorization.
                r.view
            }
            Action::Get { operation_id } => self.load(&account, &operation_id)?.view,
            Action::List { after, limit } => {
                if limit == 0 || limit > 100 {
                    return Err(invalid("limit must be 1..100"));
                }
                if let Some(a) = &after {
                    identifier(a)?;
                }
                let mut views = vec![];
                for entry in self.keys(OPS)?.as_ref().prefix(format!("{account}/")) {
                    let (_, bytes) = entry.into_inner()?;
                    let r: Record = serde_json::from_slice(&bytes)?;
                    if after.as_ref().is_some_and(|id| r.view.operation_id <= *id) {
                        continue;
                    }
                    views.push(r.view);
                    if views.len() > limit as usize {
                        break;
                    }
                }
                let next = if views.len() > limit as usize {
                    views.pop();
                    views.last().map(|v| v.operation_id.clone())
                } else {
                    None
                };
                return Ok(ExternalRgbResponse {
                    operations: views,
                    next_cursor: next,
                });
            }
            Action::Refresh { operation_id } => {
                let mut r = self.load(&account, &operation_id)?;
                r.next_retry_at = 0;
                self.save(&r, None, false)?;
                r.view
            }
            Action::Cancel {
                request_id,
                operation_id,
            } => {
                if let Some(r) = self.replay(&account, "cancel", &request_id, &hash)? {
                    return Ok(one(r.view));
                }
                let mut r = self.load(&account, &operation_id)?;
                if r.view.direction == "send" {
                    return Err(conflict("a returned PSBT may already be signed; cancellation cannot release its inputs"));
                }
                if r.view.direction == "receive" && (!r.proof.is_empty() || r.view.txid.is_some()) {
                    return Err(conflict("payment already detected; continue recovery"));
                }
                r.view.status = "cancelled".into();
                r.view.retryable = false;
                self.save(
                    &r,
                    Some((&scoped(&account, &format!("cancel/{request_id}")), &hash)),
                    false,
                )?;
                r.view
            }
        };
        Ok(one(view))
    }
    fn finish_preparation(&self, r: &mut Record, psbt: Psbt, seal: ExternalRgbSeal) -> Result<()> {
        let contract = RgbContractId::from_str(&r.view.asset_id).map_err(invalid)?;
        let (prepared, transfer) = prepare_external_rgb_transfer(
            &self.stock(&r.account),
            psbt,
            r.change_vout.context("missing change")?,
            contract,
            r.view.amount,
            seal,
        )?;
        r.fascia = encode_fascia_bytes(&prepared.fascia)?;
        r.proof = encode_rgb20_transfer_consignment(&transfer)?;
        if r.proof.len() > self.config.max_consignment_bytes {
            return Err(invalid("outgoing proof exceeds configured limit"));
        }
        r.view.txid = Some(prepared.psbt.unsigned_tx.compute_txid().to_string());
        r.candidate_hex = Some(hex_encode(&serialize(&prepared.psbt.unsigned_tx)));
        r.view.anchor_psbt = Some(hex_encode(&prepared.psbt.serialize()));
        r.view.status = "awaiting_signature".into();
        r.view.retryable = false;
        r.view.last_error = None;
        self.save(r, None, false)
    }
    fn owned_unspent(&self, outpoint: OutPoint, address: &Address) -> Result<TxOut> {
        let tx = self
            .chain_tx(outpoint.txid)?
            .ok_or_else(|| invalid("input transaction not found"))?;
        let output = tx
            .output
            .get(outpoint.vout as usize)
            .ok_or_else(|| invalid("input vout missing"))?;
        if output.script_pubkey != address.script_pubkey() {
            return Err(invalid("input not owned by account"));
        }
        let spent =
            self.chain_json(&format!("/tx/{}/outspend/{}", outpoint.txid, outpoint.vout))?;
        if spent["spent"].as_bool() != Some(false) {
            return Err(conflict("input spent or chain status unavailable"));
        }
        if self.confirmations(outpoint.txid)? < self.config.required_confirmations {
            return Err(conflict("input not sufficiently confirmed"));
        }
        Ok(output.clone())
    }
}
fn one(view: View) -> ExternalRgbResponse {
    ExternalRgbResponse {
        operations: vec![view],
        next_cursor: None,
    }
}
fn parse_psbt(text: &str) -> Result<Psbt> {
    if text.len() > MAX_PSBT {
        return Err(invalid("PSBT exceeds limit"));
    }
    Psbt::deserialize(&hex_decode(text).map_err(|e| invalid(e))?).map_err(invalid)
}
fn invoice_amount(invoice: &RgbInvoice, network: Network) -> Result<u64> {
    if invoice.chain_network() != XChainNet::bitcoin(network, ()).chain_network() {
        return Err(invalid("invoice network mismatch"));
    }
    if invoice.contract.is_none() || invoice.expiry.is_none_or(|t| t <= timestamp() as i64) {
        return Err(invalid("contract-bound unexpired invoice required"));
    }
    if invoice
        .assignment_name
        .as_ref()
        .is_some_and(|n| n.to_string() != "assetOwner")
        || !invoice.unknown_query.is_empty()
    {
        return Err(invalid("unsupported invoice assignment or extension"));
    }
    match invoice.assignment_state {
        Some(InvoiceState::Amount(a)) if a.value() > 0 => Ok(a.value()),
        _ => Err(invalid("positive fungible amount required")),
    }
}
fn check_authorization(
    a: &AssetSpendAuthorization,
    invoice: &RgbInvoice,
    amount: u64,
    psbt: &str,
) -> Result<()> {
    if a.purpose != rgb_service_api::AssetSpendPurpose::L1Transfer
        || a.asset_id != invoice.contract.context("missing contract")?.to_string()
        || a.amount != amount
        || a.recipient.as_deref() != Some(invoice.to_string().as_str())
        || a.anchor_psbt.as_deref() != Some(psbt)
    {
        return Err(invalid(
            "asset authorization must bind this contract, amount, canonical invoice and exact PSBT",
        ));
    }
    Ok(())
}
fn validate_carrier(
    psbt: &Psbt,
    account: &Address,
    invoice: &RgbInvoice,
    recipient: Option<u32>,
    change: u32,
) -> Result<()> {
    rgb_service_local::validate_rgb20_external_carrier(psbt).map_err(invalid)?;
    let tx = &psbt.unsigned_tx;
    if tx.input.is_empty()
        || tx.input.iter().any(|i| {
            i.sequence != bitcoin::Sequence::MAX
                || !i.script_sig.is_empty()
                || !i.witness.is_empty()
        })
    {
        return Err(invalid("non-RBF unsigned native SegWit inputs required"));
    }
    let unique = tx
        .input
        .iter()
        .map(|i| i.previous_output)
        .collect::<HashSet<_>>();
    if unique.len() != tx.input.len() {
        return Err(invalid("duplicate inputs"));
    }
    let change_output = tx
        .output
        .get(change as usize)
        .ok_or_else(|| invalid("change output missing"))?;
    if change_output.script_pubkey != account.script_pubkey() || change_output.value.to_sat() < 546
    {
        return Err(invalid("change must return to account, above dust"));
    }
    let expected = match invoice.beneficiary.clone().into_inner() {
        Beneficiary::WitnessVout(v, None) => {
            let n = recipient.ok_or_else(|| invalid("witness recipient_vout required"))?;
            if n == change
                || tx
                    .output
                    .get(n as usize)
                    .is_none_or(|o| o.script_pubkey != v.to_script() || o.value.to_sat() < 546)
            {
                return Err(invalid("recipient output does not match invoice"));
            }
            Some(n)
        }
        Beneficiary::BlindedSeal(_) => {
            if recipient.is_some() {
                return Err(invalid("blind transfer has no recipient_vout"));
            }
            None
        }
        _ => return Err(invalid("tapret recipient unsupported")),
    };
    for (n, o) in tx.output.iter().enumerate() {
        if n as u32 != change
            && Some(n as u32) != expected
            && (!o.script_pubkey.is_op_return() || o.value.to_sat() != 0)
        {
            return Err(invalid("unexpected carrier output"));
        }
    }
    let input_sum = psbt.inputs.iter().try_fold(0u64, |n, i| {
        n.checked_add(
            i.witness_utxo
                .as_ref()
                .ok_or_else(|| invalid("witness_utxo required"))?
                .value
                .to_sat(),
        )
        .ok_or_else(|| invalid("input overflow"))
    })?;
    let outputs = tx.output.iter().try_fold(0u64, |n, o| {
        n.checked_add(o.value.to_sat())
            .ok_or_else(|| invalid("output overflow"))
    })?;
    let fee = input_sum
        .checked_sub(outputs)
        .ok_or_else(|| invalid("negative fee"))?;
    if fee == 0 || fee > 100000 {
        return Err(invalid("fee must be 1..100000 sats; review in wallet"));
    }
    Ok(())
}
fn finalized_transaction(prepared: &Psbt, signed: &Psbt) -> Result<Transaction> {
    if signed.unsigned_tx != prepared.unsigned_tx || signed.inputs.len() != prepared.inputs.len() {
        return Err(invalid("signed transaction differs from prepared carrier"));
    }
    for (a, b) in prepared.inputs.iter().zip(&signed.inputs) {
        if a.witness_utxo != b.witness_utxo
            || b.final_script_sig.as_ref().is_some_and(|s| !s.is_empty())
            || b.final_script_witness.as_ref().is_none_or(|w| w.is_empty())
        {
            return Err(invalid(
                "fully finalized native SegWit PSBT with original prevouts required",
            ));
        }
    }
    let tx = signed.clone().extract_tx().map_err(invalid)?;
    verify_final_witnesses(prepared, &tx)?;
    Ok(tx)
}

fn verify_final_witnesses(prepared: &Psbt, tx: &Transaction) -> Result<()> {
    use bitcoin::sighash::{EcdsaSighashType, Prevouts, SighashCache, TapSighashType};
    let prevouts = prepared
        .inputs
        .iter()
        .map(|i| {
            i.witness_utxo
                .clone()
                .ok_or_else(|| invalid("missing prevout"))
        })
        .collect::<Result<Vec<_>>>()?;
    let secp = Secp256k1::verification_only();
    for (index, input) in tx.input.iter().enumerate() {
        let prev = &prevouts[index];
        let witness = input.witness.iter().collect::<Vec<_>>();
        if prev.script_pubkey.is_p2wpkh() && witness.len() == 2 {
            let key = PublicKey::from_slice(witness[1]).map_err(invalid)?;
            if Address::p2wpkh(&CompressedPublicKey(key), Network::Bitcoin).script_pubkey()
                != prev.script_pubkey
            {
                return Err(invalid("witness key does not own prevout"));
            }
            let signature = bitcoin::ecdsa::Signature::from_slice(witness[0]).map_err(invalid)?;
            if signature.sighash_type != EcdsaSighashType::All {
                return Err(invalid("SIGHASH_ALL required"));
            }
            let hash = SighashCache::new(tx)
                .p2wpkh_signature_hash(
                    index,
                    &prev.script_pubkey,
                    prev.value,
                    signature.sighash_type,
                )
                .map_err(invalid)?;
            secp.verify_ecdsa(
                &Message::from_digest(hash.to_byte_array()),
                &signature.signature,
                &key,
            )
            .map_err(invalid)?;
        } else if prev.script_pubkey.is_p2tr() && witness.len() == 1 {
            let signature = bitcoin::taproot::Signature::from_slice(witness[0]).map_err(invalid)?;
            if !matches!(
                signature.sighash_type,
                TapSighashType::Default | TapSighashType::All
            ) {
                return Err(invalid("Taproot DEFAULT or ALL required"));
            }
            let key =
                bitcoin::secp256k1::XOnlyPublicKey::from_slice(&prev.script_pubkey.as_bytes()[2..])
                    .map_err(invalid)?;
            let hash = SighashCache::new(tx)
                .taproot_key_spend_signature_hash(
                    index,
                    &Prevouts::All(&prevouts),
                    signature.sighash_type,
                )
                .map_err(invalid)?;
            secp.verify_schnorr(
                &signature.signature,
                &Message::from_digest(hash.to_byte_array()),
                &key,
            )
            .map_err(invalid)?;
        } else {
            return Err(invalid(
                "unsupported or malformed final native SegWit witness",
            ));
        }
    }
    Ok(())
}

fn proxy_result(method: &str, body: Value) -> Result<Value> {
    if !body["error"].is_null() {
        if matches!(method, "consignment.get" | "ack.get")
            && body["error"]["code"].as_i64() == Some(-400)
            && body["error"]["message"].as_str() == Some("Consignment file not found")
        {
            return Ok(Value::Null);
        }
        return Err(anyhow!("proxy RPC error: {}", body["error"]));
    }
    body.get("result").cloned().context("proxy result missing")
}

fn proxy_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value).map_err(invalid)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
        || url.port_or_known_default() != Some(443)
    {
        return Err(invalid(
            "proxy must be an HTTPS URL on port 443 without credentials, query or fragment",
        ));
    }
    Ok(url)
}
fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let o = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && !ip.is_documentation()
                && o[0] != 0
                && o[0] < 224
                && !(o[0] == 100 && (64..=127).contains(&o[1]))
                && !(o[0] == 198 && (o[1] == 18 || o[1] == 19))
                && !(o[0] == 192 && o[1] == 0)
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000
                && !(s[0] == 0x2001 && s[1] == 0x0db8)
                && !(s[0] == 0x2001 && s[1] < 0x0200)
        }
    }
}
fn response_bytes(response: reqwest::blocking::Response, limit: usize) -> Result<Vec<u8>> {
    let response = response.error_for_status()?;
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err(anyhow!("HTTP response exceeds size limit"));
    }
    let mut body = Vec::new();
    response.take(limit as u64 + 1).read_to_end(&mut body)?;
    anyhow::ensure!(body.len() <= limit, "HTTP response exceeds size limit");
    Ok(body)
}
impl Engine {
    fn http(&self) -> Result<reqwest::blocking::Client> {
        Ok(reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(self.config.request_timeout_secs))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?)
    }
    fn proxy_client(&self, url: &str) -> Result<reqwest::blocking::Client> {
        if !self.config.proxy_allowlist.iter().any(|s| s == url) {
            return Err(invalid("proxy not allowlisted"));
        }
        let parsed = proxy_url(url)?;
        let host = parsed.host_str().context("proxy hostname missing")?;
        let addresses = (host, 443).to_socket_addrs()?.collect::<Vec<_>>();
        anyhow::ensure!(
            !addresses.is_empty() && addresses.iter().all(|a| public_ip(a.ip())),
            "proxy resolved to a non-public address"
        );
        Ok(reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(self.config.request_timeout_secs))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .resolve_to_addrs(host, &addresses)
            .build()?)
    }
    fn rpc(&self, proxy: &str, method: &str, params: Value) -> Result<Value> {
        let response = self
            .proxy_client(proxy)?
            .post(proxy)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()?;
        let bytes = response_bytes(response, self.config.max_consignment_bytes * 2 + 8192)?;
        let body: Value = serde_json::from_slice(&bytes)?;
        proxy_result(method, body)
    }
    fn upload(&self, r: &Record) -> Result<()> {
        let params =
            json!({"recipient_id":r.view.recipient_id,"txid":r.view.txid,"vout":r.recipient_vout})
                .to_string();
        let form = reqwest::blocking::multipart::Form::new()
            .text("jsonrpc", "2.0")
            .text("id", "1")
            .text("method", "consignment.post")
            .text("params", params)
            .part(
                "file",
                reqwest::blocking::multipart::Part::bytes(r.proof.clone())
                    .file_name("transfer.rgbc"),
            );
        let response = self
            .proxy_client(&r.proxy)?
            .post(&r.proxy)
            .multipart(form)
            .send()?;
        let value: Value = serde_json::from_slice(&response_bytes(response, 8192)?)?;
        if value["result"] == true {
            return Ok(());
        }
        // An earlier upload may have succeeded even when its reply was lost.
        let stored = self.rpc(
            &r.proxy,
            "consignment.get",
            json!({"recipient_id":r.view.recipient_id}),
        )?;
        if stored["txid"] != r.view.txid.clone().unwrap_or_default()
            || STANDARD.decode(stored["consignment"].as_str().unwrap_or_default())? != r.proof
        {
            return Err(conflict("proxy slot contains a different consignment"));
        }
        Ok(())
    }
    fn chain_json(&self, path: &str) -> Result<Value> {
        let response = self.http()?.get(format!("{}{path}", self.chain)).send()?;
        Ok(serde_json::from_slice(&response_bytes(
            response,
            4 * 1024 * 1024,
        )?)?)
    }
    fn chain_tx(&self, txid: Txid) -> Result<Option<Transaction>> {
        let response = self
            .http()?
            .get(format!("{}/tx/{txid}/hex", self.chain))
            .send()?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let bytes = response_bytes(response, 8 * 1024 * 1024)?;
        let tx: Transaction =
            deserialize(&hex_decode(std::str::from_utf8(&bytes)?.trim()).map_err(|e| anyhow!(e))?)?;
        anyhow::ensure!(
            tx.compute_txid() == txid,
            "chain returned wrong transaction"
        );
        Ok(Some(tx))
    }
    fn confirmations(&self, txid: Txid) -> Result<u32> {
        let response = self
            .http()?
            .get(format!("{}/tx/{txid}/status", self.chain))
            .send()?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(0);
        }
        let status: Value = serde_json::from_slice(&response_bytes(response, 8192)?)?;
        if status["confirmed"].as_bool() != Some(true) {
            return Ok(0);
        }
        let height = status["block_height"]
            .as_u64()
            .context("confirmed transaction missing height")?;
        let response = self
            .http()?
            .get(format!("{}/blocks/tip/height", self.chain))
            .send()?;
        let bytes = response_bytes(response, 128)?;
        let tip = std::str::from_utf8(&bytes)?.trim().parse::<u64>()?;
        Ok(tip
            .checked_sub(height)
            .map(|n| (n + 1).min(u32::MAX as u64) as u32)
            .unwrap_or(0))
    }
    fn broadcast(&self, r: &Record) -> Result<()> {
        let txid = Txid::from_str(r.view.txid.as_deref().context("txid missing")?)?;
        if let Some(actual) = self.chain_tx(txid)? {
            let candidate: Transaction = deserialize(
                &hex_decode(r.candidate_hex.as_deref().context("candidate missing")?)
                    .map_err(|e| anyhow!(e))?,
            )?;
            let mut actual = actual;
            for i in &mut actual.input {
                i.witness.clear();
            }
            anyhow::ensure!(
                actual == candidate,
                "chain carrier differs from prepared transaction"
            );
            return Ok(());
        }
        let response = self
            .http()?
            .post(format!("{}/tx", self.chain))
            .header("content-type", "text/plain")
            .body(r.signed_hex.clone().context("signed transaction missing")?)
            .send()?;
        let bytes = response_bytes(response, 8192)?;
        anyhow::ensure!(
            std::str::from_utf8(&bytes)?.trim() == txid.to_string(),
            "unexpected broadcast response"
        );
        Ok(())
    }
    pub fn scan(&self) -> rgb_service_api::Result<()> {
        self.scan_operations().map_err(classify)
    }
    fn scan_operations(&self) -> Result<()> {
        // Snapshot keys; never retain a database write transaction over network I/O.
        let mut records = vec![];
        for entry in self.keys(OPS)?.as_ref().iter() {
            let (_, bytes) = entry.into_inner()?;
            let r: Record = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                r.schema_version == 1,
                "unsupported external operation schema"
            );
            if r.view.direction == "send" && r.view.status == "cancelled" {
                continue;
            }
            if r.next_retry_at <= timestamp()
                && r.view.status != "awaiting_signature"
                && r.view.status != "needs_review"
            {
                records.push((r.account, r.view.operation_id, r.next_retry_at));
            }
        }
        records.sort_by_key(|r| r.2);
        for (account, id, _) in records.into_iter().take(32) {
            let _lock = self.mutation.lock().map_err(|e| anyhow!(e.to_string()))?;
            let mut r = self.load(&account, &id)?;
            if r.next_retry_at > timestamp()
                || matches!(
                    r.view.status.as_str(),
                    "awaiting_signature" | "needs_review"
                )
                || (r.view.direction == "send" && r.view.status == "cancelled")
            {
                continue;
            }
            match self.advance(&mut r) {
                Ok(()) => {
                    r.attempts = 0;
                    r.view.last_error = None;
                    r.next_retry_at = timestamp() + 60;
                }
                Err(e) => {
                    r.attempts = r.attempts.saturating_add(1);
                    r.view.last_error = Some(format!("{e:#}"));
                    r.next_retry_at = timestamp() + 30u64.saturating_mul(1u64 << r.attempts.min(6));
                    r.view.retryable = r.view.status != "needs_review";
                }
            }
            r.view.updated_at = timestamp();
            self.save(&r, None, false)?;
        }
        Ok(())
    }
    fn advance(&self, r: &mut Record) -> Result<()> {
        if r.view.status == "settled" {
            let txid = Txid::from_str(r.view.txid.as_deref().context("missing settled txid")?)?;
            r.view.confirmations = self.confirmations(txid)?;
            if r.view.confirmations < r.view.required_confirmations {
                r.view.status = "needs_review".into();
                r.view.retryable = false;
                r.quarantine_account = true;
                self.mark_utxo_unconfirmed(r)?;
                return Err(conflict(
                    "settled carrier lost required confirmations; account requires review",
                ));
            }
            return Ok(());
        }
        if r.view.direction == "receive" {
            self.advance_receive(r)
        } else {
            self.advance_send(r)
        }
    }
    fn advance_send(&self, r: &mut Record) -> Result<()> {
        if r.view.status == "preparing" {
            let psbt = parse_psbt(
                r.candidate_hex
                    .as_deref()
                    .context("preparation input missing")?,
            )?;
            let invoice = RgbInvoice::from_str(&r.view.invoice)?;
            let seal = match invoice.beneficiary.into_inner() {
                Beneficiary::BlindedSeal(s) => ExternalRgbSeal::Blind(s),
                Beneficiary::WitnessVout(_, None) => {
                    ExternalRgbSeal::Witness(r.recipient_vout.context("recipient missing")?)
                }
                _ => return Err(invalid("unsupported beneficiary")),
            };
            return self.finish_preparation(r, psbt, seal);
        }
        if r.signed_hex.is_none() {
            return Ok(());
        }
        if r.view.delivery_status != "delivered" {
            // intent + signed transaction are already durable before this call.
            self.upload(r)?;
            r.view.delivery_status = "delivered".into();
            self.save(r, None, false)?;
        }
        if r.view.broadcast_status != "broadcast" {
            r.view.broadcast_status = "unknown".into();
            self.save(r, None, false)?;
            self.broadcast(r)?;
            r.view.broadcast_status = "broadcast".into();
            r.view.status = "waiting_confirmations".into();
            self.save(r, None, false)?;
        }
        let ack = self.rpc(
            &r.proxy,
            "ack.get",
            json!({"recipient_id":r.view.recipient_id}),
        )?;
        r.view.acknowledged = ack.as_bool();
        // A peer NACK does not make an already broadcast payment cancellable.
        let txid = Txid::from_str(r.view.txid.as_deref().context("missing send txid")?)?;
        r.view.confirmations = self.confirmations(txid)?;
        if r.view.confirmations < r.view.required_confirmations {
            return Ok(());
        }
        let stock = self.stock(&r.account);
        let store = LocalRgbStore::open(&stock)?;
        if !store
            .pending_status(txid)?
            .is_some_and(|s| s.status == "confirmed")
        {
            let fascia = rgb_service_local::decode_fascia_bytes(&r.fascia)?;
            stage_sender_fascia(&stock, txid, &fascia)?;
            scan_and_promote_confirmed_staged_rgb_stocks(
                &stock,
                self.network,
                std::slice::from_ref(&self.chain),
            )?;
            anyhow::ensure!(
                store
                    .pending_status(txid)?
                    .is_some_and(|s| s.status == "confirmed"),
                "sender RGB state not yet promoted"
            );
        }
        self.credit(
            r,
            OutPoint {
                txid,
                vout: r.change_vout.context("missing change vout")?,
            },
        )?;
        r.view.status = "settled".into();
        r.view.retryable = false;
        if r.view.acknowledged == Some(false) {
            r.view.status = "needs_review".into();
            return Err(conflict(
                "peer rejected a confirmed payment; inspect proof without releasing inputs",
            ));
        }
        Ok(())
    }
    fn advance_receive(&self, r: &mut Record) -> Result<()> {
        if r.proof.is_empty() {
            let result = self.rpc(
                &r.proxy,
                "consignment.get",
                json!({"recipient_id":r.view.recipient_id}),
            )?;
            if result.is_null() {
                return Ok(());
            }
            let encoded = result["consignment"]
                .as_str()
                .context("proxy consignment missing")?;
            let bytes = STANDARD.decode(encoded)?;
            if bytes.len() > self.config.max_consignment_bytes {
                return Err(invalid("consignment too large"));
            }
            let txid = Txid::from_str(result["txid"].as_str().context("proxy txid missing")?)?;
            r.proof = bytes;
            r.view.txid = Some(txid.to_string());
            if r.blind_secret.is_none() {
                let vout = result["vout"]
                    .as_u64()
                    .and_then(|v| u32::try_from(v).ok())
                    .context("proxy vout missing")?;
                r.receive_outpoint = Some(OutPoint { txid, vout }.to_string());
            }
            if timestamp() >= r.expires_at || r.view.status == "cancelled" {
                r.view.status = "needs_review".into();
                r.view.retryable = false;
                self.save(r, None, false)?;
                return Err(conflict(
                    "payment arrived for expired/cancelled invoice; proof retained",
                ));
            }
            r.view.status = "validating".into();
            self.save(r, None, false)?;
        }
        let txid = Txid::from_str(r.view.txid.as_deref().context("receive txid missing")?)?;
        let outpoint = OutPoint::from_str(
            r.receive_outpoint
                .as_deref()
                .context("receive outpoint missing")?,
        )?;
        if r.view.status == "validating" {
            let transfer = match decode_rgb20_transfer_consignment(&r.proof) {
                Ok(transfer) => transfer,
                Err(_) => return self.reject(r, "malformed RGB consignment"),
            };
            if transfer.contract_id().to_string() != r.view.asset_id {
                return self.reject(r, "consignment contract differs from invoice");
            }
            let candidate = transfer
                .bundles
                .iter()
                .find(|b| b.pub_witness.txid() == txid)
                .and_then(|b| b.pub_witness.tx().cloned());
            let candidate = match candidate {
                Some(t) => t,
                None => self.chain_tx(txid)?.context("carrier not yet available")?,
            };
            if r.blind_secret.is_none() {
                if candidate
                    .output
                    .get(outpoint.vout as usize)
                    .is_none_or(|o| {
                        o.script_pubkey
                            != self
                                .account_address(&r.account)
                                .map(|a| a.script_pubkey())
                                .unwrap_or_default()
                    })
                {
                    return self.reject(r, "carrier recipient differs from invoice account");
                }
            }
            let secret = r
                .blind_secret
                .as_deref()
                .map(GraphSeal::from_str)
                .transpose()?;
            if secret.is_some() {
                self.owned_unspent(outpoint, &self.account_address(&r.account)?)?;
            }

            if let Some(secret) = secret {
                if !transfer
                    .terminals
                    .values()
                    .any(|seals| seals.contains(&secret.conceal()))
                {
                    return self.reject(r, "proof does not target the invoice blind seal");
                }
            }
            let allocations = preview_external_rgb_receive(
                &self.stock(&r.account),
                self.network,
                &chain_source_from_url(self.chain.clone()),
                transfer,
                candidate.clone(),
                outpoint,
                secret,
            )?;
            let amount = allocations
                .iter()
                .filter(|a| a.contract_id.to_string() == r.view.asset_id)
                .try_fold(0u64, |n, a| {
                    n.checked_add(a.amount_raw).context("allocation overflow")
                })?;
            if amount != r.view.amount {
                return self.reject(
                    r,
                    "received amount differs from invoice; manual review required",
                );
            }
            r.candidate_hex = Some(hex_encode(&serialize(&candidate)));
            r.view.status = "waiting_confirmations".into();
            r.view.delivery_status = "validated".into();
            self.save(r, None, false)?;
        }
        if r.view.acknowledged != Some(true) {
            self.rpc(
                &r.proxy,
                "ack.post",
                json!({"recipient_id":r.view.recipient_id,"ack":true}),
            )?;
            let ack = self.rpc(
                &r.proxy,
                "ack.get",
                json!({"recipient_id":r.view.recipient_id}),
            )?;
            anyhow::ensure!(ack == true, "proxy ACK not recorded");
            r.view.acknowledged = Some(true);
            self.save(r, None, false)?;
        }
        r.view.confirmations = self.confirmations(txid)?;
        if r.view.confirmations < r.view.required_confirmations {
            return Ok(());
        }
        let actual = self.chain_tx(txid)?.context("confirmed carrier missing")?;
        let candidate: Transaction = deserialize(
            &hex_decode(
                r.candidate_hex
                    .as_deref()
                    .context("validated carrier missing")?,
            )
            .map_err(|e| anyhow!(e))?,
        )?;
        // Witness signatures may be absent from the consignment carrier; compare the txid-bound body.
        let mut actual = actual;
        let mut candidate = candidate;
        for i in &mut actual.input {
            i.witness.clear();
        }
        for i in &mut candidate.input {
            i.witness.clear();
        }
        if actual != candidate {
            return self.reject(r, "confirmed carrier differs from validated proof");
        }
        let stock = self.stock(&r.account);
        if let Some(secret) = r.blind_secret.as_deref() {
            store_external_rgb_receive_secret(&stock, GraphSeal::from_str(secret)?)?;
        }
        // accept_transfer is idempotent for the identical valid consignment; a crash
        // between stock commit and operation commit replays the same proof.
        rgb_service_local::accept_rgb20_transfer_with_chain_source(
            &stock,
            self.network,
            &chain_source_from_url(self.chain.clone()),
            decode_rgb20_transfer_consignment(&r.proof)?,
        )?;
        self.credit(r, outpoint)?;
        r.view.status = "settled".into();
        r.view.retryable = false;
        r.view.broadcast_status = "confirmed".into();
        Ok(())
    }
    fn reject(&self, r: &mut Record, message: &str) -> Result<()> {
        r.view.status = "needs_review".into();
        r.view.retryable = false;
        r.view.last_error = Some(message.into());
        self.save(r, None, false)?;
        // NACK only deterministic mismatches; transport/resolver errors stay retryable.
        self.rpc(
            &r.proxy,
            "ack.post",
            json!({"recipient_id":r.view.recipient_id,"ack":false}),
        )?;
        Err(conflict(message))
    }
    fn credit(&self, r: &Record, outpoint: OutPoint) -> Result<()> {
        let keyspace = self.keys("account_utxos")?;
        let ops = self.keys(OPS)?;
        let reservations = self.keys(RESERVED)?;
        let mut tx = self.db.write_tx();
        for input in &r.inputs {
            tx.remove(
                &keyspace,
                LocalDaemonService::account_utxo_key(&r.account, input),
            );
            if r.view.direction == "receive" {
                tx.remove(&reservations, input);
            }
        }
        let utxo = TrackedUtxo {
            outpoint: outpoint.to_string(),
            address: Some(r.account.clone()),
            confirmed: true,
        };
        tx.insert(
            &keyspace,
            LocalDaemonService::account_utxo_key(&r.account, &utxo.outpoint),
            serde_json::to_vec(&utxo)?,
        );
        let mut committed = r.clone();
        committed.view.status = "settled".into();
        committed.view.retryable = false;
        tx.insert(
            &ops,
            scoped(&r.account, &r.view.operation_id),
            serde_json::to_vec(&committed)?,
        );
        tx.commit()?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }
    fn mark_utxo_unconfirmed(&self, r: &Record) -> Result<()> {
        let outpoint = if r.view.direction == "send" {
            format!(
                "{}:{}",
                r.view.txid.as_deref().context("txid missing")?,
                r.change_vout.context("change missing")?
            )
        } else {
            r.receive_outpoint.clone().context("outpoint missing")?
        };
        let keys = self.keys("account_utxos")?;
        let key = LocalDaemonService::account_utxo_key(&r.account, &outpoint);
        if let Some(bytes) = keys.get(&key)? {
            let mut utxo: TrackedUtxo = serde_json::from_slice(&bytes)?;
            utxo.confirmed = false;
            let mut tx = self.db.write_tx();
            tx.insert(&keys, key, serde_json::to_vec(&utxo)?);
            tx.commit()?;
            self.db.persist(PersistMode::SyncAll)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Amount, Sequence, TxIn, Witness};
    const CONTRACT: &str = "rgb:f~9F4X0C-TiLOTvy-pALF29V-2xJ2p0m-hP3_vpW-Alj4G5Y";
    fn key(n: u8) -> bitcoin::secp256k1::SecretKey {
        bitcoin::secp256k1::SecretKey::from_slice(&[n; 32]).unwrap()
    }
    fn address(n: u8) -> Address {
        Address::p2wpkh(
            &CompressedPublicKey(PublicKey::from_secret_key(&Secp256k1::new(), &key(n))),
            Network::Signet,
        )
    }
    fn temp() -> PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        env::temp_dir().join(format!(
            "external-rgb-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ))
    }
    fn engine_at(path: PathBuf) -> Engine {
        fs::create_dir_all(&path).unwrap();
        let db = SingleWriterTxDatabase::builder(&path).open().unwrap();
        LocalRgbStore::register_database(&path, db.clone()).unwrap();
        Engine {
            db,
            config: ExternalConfig {
                enabled: true,
                proxy_allowlist: vec!["https://rgb-proxy.utexo.com/json-rpc".into()],
                ..Default::default()
            },
            network: Network::Signet,
            chain: "http://127.0.0.1:1".into(),
            kv: path,
            mutation: Arc::new(Mutex::new(())),
        }
    }
    fn receive(request_id: &str) -> ExternalRgbRequest {
        ExternalRgbRequest {
            account_id: address(1).to_string(),
            action: Action::Receive {
                request_id: request_id.into(),
                asset_id: CONTRACT.into(),
                amount: 1000000,
                expires_at: 4102444800,
                blind_outpoint: None,
            },
        }
    }
    fn valid_receive(request_id: &str) -> ExternalRgbRequest {
        let mut req = receive(request_id);
        if let Action::Receive { expires_at, .. } = &mut req.action {
            *expires_at = timestamp() + 3600;
        }
        req
    }
    fn fixture_psbt() -> Psbt {
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([3; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![
                TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::new_op_return([]),
                },
                TxOut {
                    value: Amount::from_sat(2000),
                    script_pubkey: address(2).script_pubkey(),
                },
                TxOut {
                    value: Amount::from_sat(2500),
                    script_pubkey: address(1).script_pubkey(),
                },
            ],
        };
        let mut psbt = Psbt::from_unsigned_tx(tx).unwrap();
        psbt.inputs[0].witness_utxo = Some(TxOut {
            value: Amount::from_sat(5000),
            script_pubkey: address(1).script_pubkey(),
        });
        psbt
    }
    fn signed(psbt: &Psbt) -> Psbt {
        use bitcoin::sighash::{EcdsaSighashType, SighashCache};
        let prev = psbt.inputs[0].witness_utxo.as_ref().unwrap();
        let hash = SighashCache::new(&psbt.unsigned_tx)
            .p2wpkh_signature_hash(0, &prev.script_pubkey, prev.value, EcdsaSighashType::All)
            .unwrap();
        let secp = Secp256k1::new();
        let pk = PublicKey::from_secret_key(&secp, &key(1));
        let signature = bitcoin::ecdsa::Signature::sighash_all(
            secp.sign_ecdsa(&Message::from_digest(hash.to_byte_array()), &key(1)),
        );
        let mut signed = psbt.clone();
        signed.inputs[0].final_script_witness = Some(Witness::p2wpkh(&signature, &pk));
        signed
    }
    #[test]
    fn ownership_binds_public_key_to_account_instead_of_signer_id() {
        let pk = PublicKey::from_secret_key(&Secp256k1::new(), &key(1));
        verify_account_key(&address(1).to_string(), &pk.to_string()).unwrap();
        assert!(verify_account_key(&address(2).to_string(), &pk.to_string()).is_err());
        let tr = Address::p2tr(
            &Secp256k1::new(),
            pk.x_only_public_key().0,
            None,
            Network::Signet,
        );
        verify_account_key(&tr.to_string(), &pk.to_string()).unwrap();
    }
    #[test]
    fn identical_receive_replays_but_changed_parameters_and_reused_witness_fail() {
        let engine = engine_at(temp());
        let req = valid_receive("receive-one");
        let first = engine.execute(req.clone()).unwrap();
        let replay = engine.execute(req.clone()).unwrap();
        assert_eq!(
            first.operations[0].operation_id,
            replay.operations[0].operation_id
        );
        let mut different = req;
        if let Action::Receive { amount, .. } = &mut different.action {
            *amount += 1;
        }
        assert!(matches!(
            engine.execute(different),
            Err(RgbServiceError::Conflict(_))
        ));
        assert!(matches!(
            engine.execute(valid_receive("receive-two")),
            Err(RgbServiceError::Conflict(_))
        ));
        assert!(guard_legacy_account(&engine.db, &address(1).to_string()).is_err());
        assert!(guard_legacy_account(&engine.db, &address(2).to_string()).is_ok());
    }
    #[test]
    fn records_are_account_scoped_and_limits_are_enforced() {
        let engine = engine_at(temp());
        let view = engine
            .execute(valid_receive("one"))
            .unwrap()
            .operations
            .remove(0);
        let req = ExternalRgbRequest {
            account_id: address(2).to_string(),
            action: Action::Get {
                operation_id: view.operation_id,
            },
        };
        assert!(matches!(
            engine.execute(req),
            Err(RgbServiceError::NotFound(_))
        ));
        assert!(engine
            .execute(ExternalRgbRequest {
                account_id: address(1).to_string(),
                action: Action::List {
                    after: None,
                    limit: 101
                }
            })
            .is_err());
        assert!(engine.execute(receive("too-long-expiry")).is_err());
    }
    #[test]
    fn input_reservation_and_idempotency_are_one_atomic_transaction() {
        let engine = engine_at(temp());
        let view = engine
            .execute(valid_receive("one"))
            .unwrap()
            .operations
            .remove(0);
        let mut first = engine
            .load(&address(1).to_string(), &view.operation_id)
            .unwrap();
        first.inputs = vec!["same-outpoint".into()];
        engine.save(&first, None, true).unwrap();
        let mut second = first.clone();
        second.view.operation_id = "competing".into();
        assert!(engine
            .save(&second, Some(("competing-key", "digest")), true)
            .is_err());
        assert!(engine
            .keys(IDEM)
            .unwrap()
            .get("competing-key")
            .unwrap()
            .is_none());
        assert!(engine.load(&first.account, "competing").is_err());
        engine.save(&first, None, true).unwrap();
    }
    #[test]
    fn concurrent_creates_return_the_same_operation() {
        let engine = engine_at(temp());
        let request = valid_receive("same");
        let handles = (0..4)
            .map(|_| {
                let e = engine.clone();
                let r = request.clone();
                std::thread::spawn(move || e.execute(r).unwrap().operations.remove(0).operation_id)
            })
            .collect::<Vec<_>>();
        let ids = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 1);
    }
    #[test]
    fn finalized_psbt_rejects_changed_carrier_invalid_signature_and_wrong_prevout() {
        let prepared = fixture_psbt();
        let final_psbt = signed(&prepared);
        finalized_transaction(&prepared, &final_psbt).unwrap();
        let mut changed = final_psbt.clone();
        changed.unsigned_tx.output[1].value = Amount::from_sat(1000);
        assert!(finalized_transaction(&prepared, &changed).is_err());
        let mut wrong = final_psbt.clone();
        wrong.inputs[0].witness_utxo.as_mut().unwrap().value = Amount::from_sat(6000);
        assert!(finalized_transaction(&prepared, &wrong).is_err());
        let mut bad = final_psbt;
        bad.inputs[0].final_script_witness = Some(Witness::from_slice(&[vec![0; 72], vec![2; 33]]));
        assert!(finalized_transaction(&prepared, &bad).is_err());
        assert!(finalized_transaction(&prepared, &prepared).is_err());
    }
    #[test]
    fn carrier_cannot_redirect_change_or_replace_invoice_recipient() {
        let engine = engine_at(temp());
        let mut req = valid_receive("target");
        req.account_id = address(2).to_string();
        let view = engine.execute(req).unwrap().operations.remove(0);
        let invoice = RgbInvoice::from_str(&view.invoice).unwrap();
        let mut psbt = fixture_psbt();
        validate_carrier(&psbt, &address(1), &invoice, Some(1), 2).unwrap();
        psbt.unsigned_tx.output[1].script_pubkey = address(3).script_pubkey();
        assert!(validate_carrier(&psbt, &address(1), &invoice, Some(1), 2).is_err());
        psbt = fixture_psbt();
        psbt.unsigned_tx.input[0].sequence = Sequence::ENABLE_RBF_NO_LOCKTIME;
        assert!(validate_carrier(&psbt, &address(1), &invoice, Some(1), 2).is_err());
        psbt = fixture_psbt();
        psbt.unsigned_tx.output[2].script_pubkey = address(3).script_pubkey();
        assert!(validate_carrier(&psbt, &address(1), &invoice, Some(1), 2).is_err());
    }
    #[test]
    fn proxy_rejects_private_targets_and_redirect_capable_schemes() {
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "::1",
            "fc00::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "{ip}");
        }
        assert!(public_ip("8.8.8.8".parse().unwrap()));
        for url in [
            "http://example.com/",
            "https://user@example.com/",
            "https://example.com:8443/",
            "https://example.com/#x",
        ] {
            assert!(proxy_url(url).is_err());
        }
        let engine = engine_at(temp());
        assert!(engine
            .invoice_proxy(
                &RgbInvoice::from_str(
                    include_str!("../../../tests/fixtures/utexo/reference-witness.invoice").trim()
                )
                .unwrap()
            )
            .is_ok());
    }
    #[test]
    fn cancellation_never_unlocks_an_exposed_psbt() {
        let engine = engine_at(temp());
        let view = engine
            .execute(valid_receive("one"))
            .unwrap()
            .operations
            .remove(0);
        let mut r = engine
            .load(&address(1).to_string(), &view.operation_id)
            .unwrap();
        r.view.direction = "send".into();
        r.view.status = "awaiting_signature".into();
        r.inputs = vec!["input".into()];
        engine.save(&r, None, true).unwrap();
        let request = ExternalRgbRequest {
            account_id: r.account.clone(),
            action: Action::Cancel {
                request_id: "cancel".into(),
                operation_id: r.view.operation_id.clone(),
            },
        };
        assert!(matches!(
            engine.execute(request),
            Err(RgbServiceError::Conflict(_))
        ));
        assert!(engine
            .keys(RESERVED)
            .unwrap()
            .contains_key("input")
            .unwrap());
    }
    #[test]
    fn durability_child() {
        let Ok(path) = env::var("RGB_EXTERNAL_TEST_CHILD") else {
            return;
        };
        let engine = engine_at(path.into());
        let view = engine
            .execute(valid_receive("durable"))
            .unwrap()
            .operations
            .remove(0);
        let mut r = engine
            .load(&address(1).to_string(), &view.operation_id)
            .unwrap();
        r.view.direction = "send".into();
        r.view.status = "delivering".into();
        r.inputs = vec!["reserved-before-network".into()];
        r.signed_hex = Some("exact-signed-transaction".into());
        r.proof = vec![1, 2, 3];
        engine.save(&r, None, true).unwrap();
    }
    #[test]
    fn process_restart_preserves_proof_signed_intent_and_reservation() {
        let path = temp();
        let status = std::process::Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "external::tests::durability_child",
                "--nocapture",
            ])
            .env("RGB_EXTERNAL_TEST_CHILD", &path)
            .status()
            .unwrap();
        assert!(status.success());
        let engine = engine_at(path);
        let id = digest(scoped(&address(1).to_string(), "receive/durable").as_bytes());
        let record = engine.load(&address(1).to_string(), &id).unwrap();
        assert_eq!(record.view.status, "delivering");
        assert_eq!(record.proof, vec![1, 2, 3]);
        assert_eq!(
            record.signed_hex.as_deref(),
            Some("exact-signed-transaction")
        );
        assert!(engine
            .keys(RESERVED)
            .unwrap()
            .contains_key("reserved-before-network")
            .unwrap());
    }
    fn authorization(invoice: &str, psbt: &str) -> AssetSpendAuthorization {
        AssetSpendAuthorization {
            asset_id: CONTRACT.into(),
            amount: 1000000,
            purpose: rgb_service_api::AssetSpendPurpose::L1Transfer,
            recipient: Some(invoice.into()),
            anchor_psbt: Some(psbt.into()),
            expires_at_ms: now_ms() + 60000,
            signature: RequestSignature {
                signer_id: String::new(),
                public_key: String::new(),
                scheme: SignatureScheme::Ecdsa,
                nonce: String::new(),
                timestamp_ms: now_ms(),
                signature: String::new(),
            },
        }
    }
    #[test]
    fn finalize_is_durable_and_retries_do_not_change_payment() {
        let engine = engine_at(temp());
        let mut req = valid_receive("seed");
        req.account_id = address(2).to_string();
        let view = engine.execute(req).unwrap().operations.remove(0);
        let mut record = engine
            .load(&address(2).to_string(), &view.operation_id)
            .unwrap();
        record.account = address(1).to_string();
        record.view.direction = "send".into();
        record.view.status = "awaiting_signature".into();
        record.view.operation_id = "prepared-send".into();
        let psbt = fixture_psbt();
        let encoded = hex_encode(&psbt.serialize());
        record.view.anchor_psbt = Some(encoded.clone());
        record.view.txid = Some(psbt.unsigned_tx.compute_txid().to_string());
        record.candidate_hex = Some(hex_encode(&serialize(&psbt.unsigned_tx)));
        record.inputs = vec![psbt.unsigned_tx.input[0].previous_output.to_string()];
        engine.save(&record, None, true).unwrap();
        let action = Action::Finalize {
            request_id: "once".into(),
            operation_id: record.view.operation_id.clone(),
            signed_anchor_psbt: hex_encode(&signed(&psbt).serialize()),
            asset_authorization: authorization(&record.view.invoice, &encoded),
        };
        let req = ExternalRgbRequest {
            account_id: record.account.clone(),
            action,
        };
        let first = engine.execute(req.clone()).unwrap();
        assert_eq!(first.operations[0].status, "delivering");
        let saved = engine
            .load(&record.account, &record.view.operation_id)
            .unwrap();
        assert!(saved.signed_hex.is_some());
        assert_eq!(saved.view.broadcast_status, "not_started");
        let replay = engine.execute(req.clone()).unwrap();
        assert_eq!(replay.operations[0].operation_id, record.view.operation_id);
        let mut changed = req;
        if let Action::Finalize {
            signed_anchor_psbt, ..
        } = &mut changed.action
        {
            *signed_anchor_psbt = "different".into();
        }
        assert!(matches!(
            engine.execute(changed),
            Err(RgbServiceError::Conflict(_))
        ));
    }
    #[test]
    fn asset_authorization_cannot_be_reused_for_another_invoice_or_amount() {
        let engine = engine_at(temp());
        let view = engine
            .execute(valid_receive("one"))
            .unwrap()
            .operations
            .remove(0);
        let invoice = RgbInvoice::from_str(&view.invoice).unwrap();
        let mut auth = authorization(&view.invoice, "psbt");
        check_authorization(&auth, &invoice, 1000000, "psbt").unwrap();
        assert!(check_authorization(&auth, &invoice, 1000001, "psbt").is_err());
        auth.recipient = Some("another invoice".into());
        assert!(check_authorization(&auth, &invoice, 1000000, "psbt").is_err());
    }
    #[test]
    fn taproot_final_signature_is_verified_against_prevout_key() {
        use bitcoin::{
            key::TapTweak,
            sighash::{Prevouts, SighashCache, TapSighashType},
        };
        let secp = Secp256k1::new();
        let pair = bitcoin::secp256k1::Keypair::from_secret_key(&secp, &key(1));
        let (internal, _) = pair.x_only_public_key();
        let mut psbt = fixture_psbt();
        psbt.inputs[0].witness_utxo.as_mut().unwrap().script_pubkey =
            Address::p2tr(&secp, internal, None, Network::Signet).script_pubkey();
        let prevouts = vec![psbt.inputs[0].witness_utxo.clone().unwrap()];
        let hash = SighashCache::new(&psbt.unsigned_tx)
            .taproot_key_spend_signature_hash(0, &Prevouts::All(&prevouts), TapSighashType::Default)
            .unwrap();
        let pair = pair.tap_tweak(&secp, None).to_keypair();
        let sig = secp.sign_schnorr_no_aux_rand(&Message::from_digest(hash.to_byte_array()), &pair);
        let mut final_psbt = psbt.clone();
        final_psbt.inputs[0].final_script_witness = Some(Witness::from_slice(&[sig.as_ref()]));
        finalized_transaction(&psbt, &final_psbt).unwrap();
        final_psbt.inputs[0].final_script_witness = Some(Witness::from_slice(&[vec![0; 64]]));
        assert!(finalized_transaction(&psbt, &final_psbt).is_err());
    }
    #[test]
    fn uncertain_broadcast_recovers_by_txid_without_another_payment() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = engine_at(temp());
        let view = engine
            .execute(valid_receive("seed"))
            .unwrap()
            .operations
            .remove(0);
        let mut record = engine
            .load(&address(1).to_string(), &view.operation_id)
            .unwrap();
        let psbt = fixture_psbt();
        let tx = finalized_transaction(&psbt, &signed(&psbt)).unwrap();
        let txhex = hex_encode(&serialize(&tx));
        record.view.txid = Some(tx.compute_txid().to_string());
        record.signed_hex = Some(txhex.clone());
        record.candidate_hex = Some(hex_encode(&serialize(&psbt.unsigned_tx)));
        let posts = Arc::new(AtomicUsize::new(0));
        let counter = posts.clone();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(8);
            let mut served = 0;
            while Instant::now() < deadline && served < 3 {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0; 4096];
                loop {
                    let n = stream.read(&mut buf).unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]).to_lowercase();
                        let len = headers
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length: "))
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + len {
                            break;
                        }
                    }
                }
                served += 1;
                if bytes.starts_with(b"POST ") {
                    counter.fetch_add(1, Ordering::SeqCst);
                    continue;
                } // accepted, reply lost
                let (status, body) = if counter.load(Ordering::SeqCst) == 0 {
                    ("404 Not Found", String::new())
                } else {
                    ("200 OK", txhex.clone())
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let mut engine = engine;
        engine.chain = format!("http://{addr}");
        engine.config.request_timeout_secs = 2;
        assert!(engine.broadcast(&record).is_err());
        engine.broadcast(&record).unwrap();
        server.join().unwrap();
        assert_eq!(posts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn http_rejects_cross_account_keys_tampering_and_unsigned_requests() {
        use tower::ServiceExt;
        let service = LocalDaemonService::new(DaemonConfig {
            service: ServiceConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                network: "signet".into(),
                data_dir: temp(),
                esplora_url: "http://127.0.0.1:1".into(),
                recovery_scan_interval_secs: None,
                external_rgb: ExternalConfig {
                    enabled: true,
                    proxy_allowlist: vec!["https://rgb-proxy.utexo.com/json-rpc".into()],
                    ..Default::default()
                },
            },
            daemon_rna: DaemonRnaConfig {
                issue_fee: 1,
                transfer_fee: 1,
                query_fee: 1,
            },
            legacy: LegacyConfig::default(),
        })
        .await
        .unwrap();
        let app = router(Arc::new(service), Arc::new(ConfiguredAuthVerifier));
        let req = valid_receive("http");
        let pk = PublicKey::from_secret_key(&Secp256k1::new(), &key(1));
        let time = now_ms();
        let message = ConfiguredAuthVerifier::signature_message(
            "external_rgb",
            &serde_json::to_vec(&req).unwrap(),
            "http-nonce",
            time,
        );
        let signature = Secp256k1::new().sign_ecdsa(&message, &key(1)).to_string();
        let signed = rgb_service_api::SignedRequest {
            payload: req,
            signature: RequestSignature {
                signer_id: address(1).to_string(),
                public_key: pk.to_string(),
                scheme: SignatureScheme::Ecdsa,
                nonce: "http-nonce".into(),
                timestamp_ms: time,
                signature,
            },
        };
        let send = |body: Value| {
            Request::builder()
                .method("POST")
                .uri("/v1/external/receives/create")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap()
        };
        let body = serde_json::to_value(signed).unwrap();
        assert_eq!(
            app.clone()
                .oneshot(send(body.clone()))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let mut cross = body.clone();
        cross["payload"]["account_id"] = json!(address(2).to_string());
        assert_eq!(
            app.clone().oneshot(send(cross)).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
        let mut altered = body.clone();
        altered["payload"]["action"]["amount"] = json!(1);
        assert_eq!(
            app.clone().oneshot(send(altered)).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.oneshot(send(body["payload"].clone()))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    #[test]
    fn missing_proxy_slot_is_pending_but_other_rpc_errors_are_not_hidden() {
        let missing = json!({"error":{"code":-400,"message":"Consignment file not found"}});
        assert!(proxy_result("consignment.get", missing.clone())
            .unwrap()
            .is_null());
        assert!(proxy_result("ack.get", missing.clone()).unwrap().is_null());
        assert!(proxy_result("consignment.post", missing).is_err());
        assert!(proxy_result(
            "consignment.get",
            json!({"error":{"code":-500,"message":"server failure"}})
        )
        .is_err());
        assert_eq!(
            proxy_result("ack.get", json!({"result":false})).unwrap(),
            false
        );
    }
    #[test]
    fn balance_projection_excludes_reserved_and_quarantined_coins_from_available() {
        let engine = engine_at(temp());
        let view = engine
            .execute(valid_receive("projection"))
            .unwrap()
            .operations
            .remove(0);
        let mut r = engine
            .load(&address(1).to_string(), &view.operation_id)
            .unwrap();
        r.view.direction = "send".into();
        r.view.status = "awaiting_signature".into();
        r.inputs = vec!["input".into()];
        engine.save(&r, None, true).unwrap();
        let projection = account_projection(&engine.db, &r.account).unwrap();
        assert_eq!(
            projection.allocation_status("input", true),
            AllocationStatus::Reserved
        );
        assert_eq!(
            projection.allocation_status("other", true),
            AllocationStatus::Available
        );
        assert_eq!(projection.pending[0].status, OperationStatus::Prepared);
        r.quarantine_account = true;
        r.view.status = "needs_review".into();
        engine.save(&r, None, false).unwrap();
        assert_eq!(
            account_projection(&engine.db, &r.account)
                .unwrap()
                .allocation_status("other", true),
            AllocationStatus::Locked
        );
    }
}
