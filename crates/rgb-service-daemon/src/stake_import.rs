use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use bdk_electrum::electrum_client::{self, ElectrumApi};
use bdk_esplora::esplora_client;
use bitcoin::{
    hashes::{sha256, Hash},
    OutPoint,
};
use fjall::{KeyspaceCreateOptions, PersistMode};
use rgb_service_local::{normalize_electrum_url, ChainSource};
use serde::{Deserialize, Serialize};
use serde_json::json;
use zip::ZipArchive;

use crate::{
    legacy::make_stake_script, now_ms, LocalDaemonService, StakeRedeemRecord,
    STAKE_REDEEM_SCHEMA_VERSION,
};

const MIGRATION_KEY: &[u8] = b"wallet_v2_stake_redeems_v1";
const IMPORT_SCHEMA_VERSION: u8 = 1;
const MAX_SQL_BYTES: u64 = 512 * 1024 * 1024;
const ESPLORA_WORKERS: usize = 4;

pub(crate) fn usage() -> &'static str {
    "usage: rgb-service import-wallet-v2-stake-redeems <config.toml> <wallet-v2.sql|wallet-v2.sql.zip> [--report <report.json>] [--dry-run]"
}

pub(crate) fn startup_usage() -> &'static str {
    "usage: rgb-service <config.toml> [--import-wallet-v2-stake-redeems <wallet-v2.sql|wallet-v2.sql.zip>] [--stake-import-report <report.json>]"
}

#[derive(Clone, Debug)]
pub(crate) struct StakeRedeemImportOptions {
    pub(crate) source: PathBuf,
    pub(crate) report_path: Option<PathBuf>,
    pub(crate) dry_run: bool,
}

pub(crate) fn parse_command_options(
    source: impl Into<PathBuf>,
    args: &[String],
) -> Result<StakeRedeemImportOptions> {
    let mut report_path = None;
    let mut dry_run = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--report" => {
                index += 1;
                report_path = Some(PathBuf::from(
                    args.get(index).ok_or_else(|| anyhow!(usage()))?,
                ));
            }
            "--dry-run" => dry_run = true,
            value => bail!("unknown stake redeem import option: {value}\n{}", usage()),
        }
        index += 1;
    }
    Ok(StakeRedeemImportOptions {
        source: source.into(),
        report_path,
        dry_run,
    })
}

pub(crate) fn parse_startup_options(args: &[String]) -> Result<Option<StakeRedeemImportOptions>> {
    if args.is_empty() {
        return Ok(None);
    }
    let mut source = None;
    let mut report_path = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--import-wallet-v2-stake-redeems" => {
                index += 1;
                if source.is_some() {
                    bail!("--import-wallet-v2-stake-redeems may only be specified once");
                }
                source = Some(PathBuf::from(
                    args.get(index).ok_or_else(|| anyhow!(startup_usage()))?,
                ));
            }
            "--stake-import-report" => {
                index += 1;
                report_path = Some(PathBuf::from(
                    args.get(index).ok_or_else(|| anyhow!(startup_usage()))?,
                ));
            }
            value => bail!(
                "unknown daemon startup option: {value}\n{}",
                startup_usage()
            ),
        }
        index += 1;
    }
    match source {
        Some(source) => Ok(Some(StakeRedeemImportOptions {
            source,
            report_path,
            dry_run: false,
        })),
        None if report_path.is_some() => bail!(
            "--stake-import-report requires --import-wallet-v2-stake-redeems\n{}",
            startup_usage()
        ),
        None => Ok(None),
    }
}

#[derive(Clone, Debug)]
struct LegacyRedeemRow {
    id: i64,
    transfer_id: i64,
    spend_txid: Option<String>,
    outpoint: String,
    sats: i64,
    height: i32,
    public_key: String,
    status: i16,
    create_time: String,
}

#[derive(Clone, Debug)]
struct LegacyTransferRow {
    id: i64,
    txid: String,
    status: i16,
    tx_type: i16,
    confirm_height: Option<i64>,
}

#[derive(Clone, Debug)]
struct LegacyRgbAssignRow {
    transfer_id: i64,
    desc: Option<String>,
    rgb_seal: String,
    assign_map: String,
}

#[derive(Clone, Debug)]
struct ParsedStakeDump {
    source_member: String,
    records: Vec<StakeRedeemRecord>,
    redeem_count: usize,
    active_count: usize,
    spent_count: usize,
    total_sats: u64,
    active_sats: u64,
    spent_sats: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StakeImportMarker {
    schema_version: u8,
    source_sha256: String,
    source_member: String,
    imported_at_ms: u64,
    record_count: usize,
    initial_active_count: usize,
    initial_spent_count: usize,
    immutable_digest: String,
    outpoints: Vec<String>,
    report_path: String,
}

#[derive(Clone, Debug, Serialize)]
struct ParsedPrecheckReport {
    redeem_rows: usize,
    records_projected: usize,
    active_records: usize,
    spent_records: usize,
    total_sats: u64,
    active_sats: u64,
    spent_sats: u64,
    missing_transfer_rows: usize,
    missing_rgb_assign_rows: usize,
    duplicate_rgb_assign_rows: usize,
    duplicate_outpoints: usize,
    invalid_records: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ActiveStakeChainAudit {
    outpoint: String,
    sats: u64,
    classification: String,
    transaction_confirmed: bool,
    output_exists: bool,
    amount_matches: bool,
    script_matches: bool,
    confirm_height_matches: bool,
    unspent: bool,
    chain_spend_txid: Option<String>,
    chain_confirm_height: Option<u64>,
    csv_height: u16,
    matures_at_height: Option<u64>,
    mature_for_next_block: bool,
    error: Option<String>,
}

impl ActiveStakeChainAudit {
    fn valid_for_import(&self) -> bool {
        self.error.is_none()
            && self.transaction_confirmed
            && self.output_exists
            && self.amount_matches
            && self.script_matches
            && self.confirm_height_matches
            && self.unspent
    }
}

#[derive(Clone, Debug, Default, Serialize)]
struct ChainPrecheckReport {
    backend: String,
    tip_height: Option<u64>,
    records_checked: usize,
    valid_unspent: usize,
    valid_unspent_sats: u64,
    redeemable_now: usize,
    redeemable_now_sats: u64,
    locked_unspent: usize,
    locked_unspent_sats: u64,
    spent_on_chain: usize,
    invalid: usize,
    records: Vec<ActiveStakeChainAudit>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct ImportWriteReport {
    attempted: usize,
    inserted: usize,
    already_equal: usize,
    marker_written: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
struct PostImportReport {
    records_verified: usize,
    indexes_verified: usize,
    immutable_digest_matches: bool,
    marker_verified: bool,
}

#[derive(Clone, Debug, Serialize)]
struct StakeRedeemImportReport {
    schema_version: u8,
    migration: &'static str,
    success: bool,
    phase: String,
    dry_run: bool,
    idempotent_replay: bool,
    started_at_ms: u64,
    completed_at_ms: u64,
    source_path: String,
    source_member: String,
    source_sha256: String,
    report_path: String,
    parsed_precheck: ParsedPrecheckReport,
    chain_precheck: ChainPrecheckReport,
    import: ImportWriteReport,
    post_import: PostImportReport,
    errors: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct StakeRedeemImportRunSummary {
    pub(crate) applied: bool,
    pub(crate) idempotent_replay: bool,
    pub(crate) record_count: usize,
    pub(crate) active_count: usize,
    pub(crate) spent_count: usize,
    pub(crate) report_path: PathBuf,
}

fn parse_required<T>(value: Option<String>, field: &str) -> Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let value = value.ok_or_else(|| anyhow!("{field} is NULL"))?;
    value
        .parse::<T>()
        .map_err(|err| anyhow!("invalid {field}: {err}"))
}

fn parse_required_string(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("{field} is NULL"))
}

fn parse_selected_sql_fields(
    line: &str,
    selected: &[usize],
) -> Result<(usize, Vec<Option<String>>)> {
    let marker = " VALUES (";
    let start = line
        .find(marker)
        .ok_or_else(|| anyhow!("INSERT is missing VALUES"))?
        + marker.len();
    let text = line
        .get(start..)
        .ok_or_else(|| anyhow!("invalid INSERT offset"))?
        .trim_end_matches(['\r', '\n']);
    let text = text
        .strip_suffix(");")
        .ok_or_else(|| anyhow!("INSERT does not end with );"))?;
    let selected_positions = selected
        .iter()
        .enumerate()
        .map(|(position, field)| (*field, position))
        .collect::<HashMap<_, _>>();
    let mut result = vec![None; selected.len()];
    let mut field_index = 0usize;
    let mut quoted = false;
    let mut field_was_quoted = false;
    let mut buffer = String::new();
    let mut chars = text.char_indices().peekable();

    let finish_field = |field_index: usize,
                        field_was_quoted: bool,
                        buffer: &mut String,
                        result: &mut Vec<Option<String>>|
     -> Result<()> {
        if let Some(position) = selected_positions.get(&field_index) {
            let value = if field_was_quoted {
                Some(std::mem::take(buffer))
            } else {
                let value = buffer.trim();
                let parsed = if value == "NULL" {
                    None
                } else {
                    Some(value.to_string())
                };
                buffer.clear();
                parsed
            };
            result[*position] = value;
        } else {
            buffer.clear();
        }
        Ok(())
    };

    while let Some((_offset, character)) = chars.next() {
        if quoted {
            if character == '\'' {
                if chars.peek().is_some_and(|(_, next)| *next == '\'') {
                    if selected_positions.contains_key(&field_index) {
                        buffer.push('\'');
                    }
                    chars.next();
                } else {
                    quoted = false;
                }
            } else if selected_positions.contains_key(&field_index) {
                buffer.push(character);
            }
            continue;
        }
        match character {
            '\'' => {
                if buffer.trim().is_empty() {
                    buffer.clear();
                }
                quoted = true;
                field_was_quoted = true;
            }
            ',' => {
                finish_field(field_index, field_was_quoted, &mut buffer, &mut result)?;
                field_index += 1;
                field_was_quoted = false;
            }
            other
                if selected_positions.contains_key(&field_index)
                    && (!field_was_quoted || !other.is_whitespace()) =>
            {
                buffer.push(other)
            }
            _ => {}
        }
    }
    if quoted {
        bail!("unterminated SQL string literal");
    }
    finish_field(field_index, field_was_quoted, &mut buffer, &mut result)?;
    Ok((field_index + 1, result))
}

fn parse_sql<R: BufRead>(reader: R, source_member: String) -> Result<ParsedStakeDump> {
    let mut redeems = BTreeMap::<i64, LegacyRedeemRow>::new();
    let mut transfers = HashMap::<i64, LegacyTransferRow>::new();
    let mut assignments = HashMap::<i64, LegacyRgbAssignRow>::new();
    let mut duplicate_assignment_ids = BTreeSet::new();
    let mut line = String::new();
    let mut reader = reader;
    let mut line_number = 0usize;
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }
        line_number += 1;
        if line.starts_with("INSERT INTO \"public\".\"redeem\" ") {
            let (field_count, fields) =
                parse_selected_sql_fields(&line, &[0, 1, 2, 3, 4, 5, 6, 7, 8])
                    .with_context(|| format!("parse redeem INSERT at line {line_number}"))?;
            if field_count != 9 {
                bail!("redeem INSERT at line {line_number} has {field_count} fields, expected 9");
            }
            let row = LegacyRedeemRow {
                id: parse_required(fields[0].clone(), "redeem.id")?,
                transfer_id: parse_required(fields[1].clone(), "redeem.transfer_id")?,
                spend_txid: fields[2].clone(),
                outpoint: parse_required_string(fields[3].clone(), "redeem.outpoint")?,
                sats: parse_required(fields[4].clone(), "redeem.sats")?,
                height: parse_required(fields[5].clone(), "redeem.height")?,
                public_key: parse_required_string(fields[6].clone(), "redeem.public_key")?,
                status: parse_required(fields[7].clone(), "redeem.status")?,
                create_time: parse_required_string(fields[8].clone(), "redeem.create_time")?,
            };
            if redeems.insert(row.id, row).is_some() {
                bail!("duplicate redeem.id at line {line_number}");
            }
        } else if line.starts_with("INSERT INTO \"public\".\"rgb_assign\" ") {
            let (field_count, fields) = parse_selected_sql_fields(&line, &[1, 2, 4, 5])
                .with_context(|| format!("parse rgb_assign INSERT at line {line_number}"))?;
            if field_count != 8 {
                bail!(
                    "rgb_assign INSERT at line {line_number} has {field_count} fields, expected 8"
                );
            }
            let row = LegacyRgbAssignRow {
                transfer_id: parse_required(fields[0].clone(), "rgb_assign.transfer_id")?,
                desc: fields[1].clone(),
                rgb_seal: parse_required_string(fields[2].clone(), "rgb_assign.rgb_seal")?,
                assign_map: parse_required_string(fields[3].clone(), "rgb_assign.assign_map")?,
            };
            if assignments.insert(row.transfer_id, row.clone()).is_some() {
                duplicate_assignment_ids.insert(row.transfer_id);
            }
        } else if line.starts_with("INSERT INTO \"public\".\"transfer\" ") {
            let (field_count, fields) = parse_selected_sql_fields(&line, &[0, 1, 6, 7, 8])
                .with_context(|| format!("parse transfer INSERT at line {line_number}"))?;
            if field_count != 11 {
                bail!(
                    "transfer INSERT at line {line_number} has {field_count} fields, expected 11"
                );
            }
            let row = LegacyTransferRow {
                id: parse_required(fields[0].clone(), "transfer.id")?,
                txid: parse_required_string(fields[1].clone(), "transfer.txid")?,
                status: parse_required(fields[2].clone(), "transfer.status")?,
                tx_type: parse_required(fields[3].clone(), "transfer.tx_type")?,
                confirm_height: fields[4]
                    .clone()
                    .map(|value| value.parse::<i64>())
                    .transpose()
                    .map_err(|err| anyhow!("invalid transfer.confirm_height: {err}"))?,
            };
            if transfers.insert(row.id, row).is_some() {
                bail!("duplicate transfer.id at line {line_number}");
            }
        }
    }
    if redeems.is_empty() {
        bail!("SQL dump contains no public.redeem rows");
    }

    let mut records = Vec::with_capacity(redeems.len());
    let mut outpoints = BTreeSet::new();
    let mut active_count = 0usize;
    let mut spent_count = 0usize;
    for redeem in redeems.values() {
        let transfer = transfers.get(&redeem.transfer_id).ok_or_else(|| {
            anyhow!(
                "redeem.id={} references missing transfer.id={}",
                redeem.id,
                redeem.transfer_id
            )
        })?;
        if duplicate_assignment_ids.contains(&redeem.transfer_id) {
            bail!(
                "redeem.id={} has multiple rgb_assign rows for transfer.id={}",
                redeem.id,
                redeem.transfer_id
            );
        }
        let assignment = assignments.get(&redeem.transfer_id).ok_or_else(|| {
            anyhow!(
                "redeem.id={} references missing rgb_assign.transfer_id={}",
                redeem.id,
                redeem.transfer_id
            )
        })?;
        if transfer.status != 2 {
            bail!(
                "redeem.id={} transfer.status={} is not Confirmed(2)",
                redeem.id,
                transfer.status
            );
        }
        if transfer.tx_type != 2 {
            bail!(
                "redeem.id={} transfer.tx_type={} is not Stake(2)",
                redeem.id,
                transfer.tx_type
            );
        }
        let outpoint = OutPoint::from_str(&redeem.outpoint)
            .with_context(|| format!("parse redeem.id={} outpoint", redeem.id))?;
        if outpoint.txid.to_string() != transfer.txid {
            bail!(
                "redeem.id={} outpoint txid does not match transfer.txid",
                redeem.id
            );
        }
        if !outpoints.insert(redeem.outpoint.clone()) {
            bail!("duplicate redeem outpoint: {}", redeem.outpoint);
        }
        let status = redeem.status;
        match status {
            0 if redeem.spend_txid.is_none() => active_count += 1,
            1 if redeem.spend_txid.is_some() => spent_count += 1,
            0 => bail!("redeem.id={} status=0 has spend_txid", redeem.id),
            1 => bail!("redeem.id={} status=1 is missing spend_txid", redeem.id),
            _ => bail!("redeem.id={} has unsupported status={status}", redeem.id),
        }
        let rgb_assignments: BTreeMap<String, u64> =
            serde_json::from_str(&assignment.assign_map)
                .with_context(|| format!("parse redeem.id={} rgb_assign.assign_map", redeem.id))?;
        if rgb_assignments.is_empty() {
            bail!("redeem.id={} rgb_assign.assign_map is empty", redeem.id);
        }
        let record = StakeRedeemRecord {
            schema_version: STAKE_REDEEM_SCHEMA_VERSION,
            stake_outpoint: redeem.outpoint.clone(),
            sats: u64::try_from(redeem.sats)
                .with_context(|| format!("redeem.id={} sats is negative", redeem.id))?,
            csv_height: u16::try_from(redeem.height)
                .with_context(|| format!("redeem.id={} height exceeds u16", redeem.id))?,
            public_key: redeem.public_key.clone(),
            owner_desc: Some(
                assignment
                    .desc
                    .clone()
                    .ok_or_else(|| anyhow!("redeem.id={} rgb_assign.desc is NULL", redeem.id))?,
            ),
            reward_seal: Some(assignment.rgb_seal.clone()),
            rgb_assignments,
            confirm_height: Some(
                u64::try_from(transfer.confirm_height.ok_or_else(|| {
                    anyhow!("redeem.id={} transfer.confirm_height is NULL", redeem.id)
                })?)
                .with_context(|| format!("redeem.id={} confirm_height is negative", redeem.id))?,
            ),
            redeem_spend_txid: redeem.spend_txid.clone(),
            status,
            created_at: redeem.create_time.clone(),
        };
        LocalDaemonService::validate_stake_redeem(&record)
            .map_err(|err| anyhow!("redeem.id={} validation failed: {err}", redeem.id))?;
        records.push(record);
    }
    records.sort_by(|left, right| left.stake_outpoint.cmp(&right.stake_outpoint));
    let total_sats = records.iter().map(|record| record.sats).sum();
    let active_sats = records
        .iter()
        .filter(|record| record.status == 0)
        .map(|record| record.sats)
        .sum();
    let spent_sats = records
        .iter()
        .filter(|record| record.status == 1)
        .map(|record| record.sats)
        .sum();
    Ok(ParsedStakeDump {
        source_member,
        records,
        redeem_count: redeems.len(),
        active_count,
        spent_count,
        total_sats,
        active_sats,
        spent_sats,
    })
}

fn is_macos_zip_metadata(entry_name: &str) -> bool {
    entry_name
        .split('/')
        .any(|component| component == "__MACOSX" || component.starts_with("._"))
}

fn parse_dump(path: &Path) -> Result<ParsedStakeDump> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("read stake import source metadata: {}", path.display()))?;
    if !metadata.is_file() {
        bail!("stake import source is not a file: {}", path.display());
    }
    if path.extension().is_some_and(|extension| extension == "zip") {
        let file = File::open(path)?;
        let mut archive = ZipArchive::new(file).context("open wallet-v2 SQL ZIP")?;
        let mut sql_entry = None;
        for index in 0..archive.len() {
            let entry = archive.by_index(index)?;
            let entry_name = entry.name();
            if entry.is_dir()
                || is_macos_zip_metadata(entry_name)
                || !entry_name.to_ascii_lowercase().ends_with(".sql")
            {
                continue;
            }
            if entry.size() > MAX_SQL_BYTES {
                bail!(
                    "SQL ZIP member exceeds {MAX_SQL_BYTES} bytes: {}",
                    entry.name()
                );
            }
            if sql_entry.is_some() {
                bail!("SQL ZIP contains more than one .sql member");
            }
            sql_entry = Some((index, entry.name().to_string()));
        }
        let (index, name) = sql_entry.ok_or_else(|| anyhow!("SQL ZIP contains no .sql member"))?;
        let entry = archive.by_index(index)?;
        parse_sql(BufReader::new(entry), name)
    } else {
        if metadata.len() > MAX_SQL_BYTES {
            bail!("SQL file exceeds {MAX_SQL_BYTES} bytes");
        }
        let file = File::open(path)?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("wallet-v2.sql")
            .to_string();
        parse_sql(BufReader::new(file), name)
    }
}

fn source_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(sha256::Hash::hash(&bytes).to_string())
}

fn immutable_records_digest(records: &[StakeRedeemRecord]) -> Result<String> {
    let mut values = records
        .iter()
        .map(|record| {
            json!({
                "schema_version": record.schema_version,
                "stake_outpoint": record.stake_outpoint,
                "sats": record.sats,
                "csv_height": record.csv_height,
                "public_key": record.public_key,
                "owner_desc": record.owner_desc,
                "reward_seal": record.reward_seal,
                "rgb_assignments": record.rgb_assignments,
                "confirm_height": record.confirm_height,
                "created_at": record.created_at,
            })
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        left["stake_outpoint"]
            .as_str()
            .cmp(&right["stake_outpoint"].as_str())
    });
    let bytes = serde_json::to_vec(&values)?;
    Ok(sha256::Hash::hash(&bytes).to_string())
}

fn retry_esplora<T, F>(context: &str, mut operation: F) -> Result<T>
where
    F: FnMut() -> std::result::Result<T, esplora_client::Error>,
{
    let mut last_error = None;
    for attempt in 0..5 {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) => {
                last_error = Some(error);
                if attempt < 4 {
                    thread::sleep(Duration::from_millis(250 * (1 << attempt)));
                }
            }
        }
    }
    Err(anyhow!(
        "{context} failed after retries: {}",
        last_error.expect("retry records an error")
    ))
}

fn empty_chain_audit(record: &StakeRedeemRecord) -> ActiveStakeChainAudit {
    ActiveStakeChainAudit {
        outpoint: record.stake_outpoint.clone(),
        sats: record.sats,
        classification: "audit_error".to_string(),
        transaction_confirmed: false,
        output_exists: false,
        amount_matches: false,
        script_matches: false,
        confirm_height_matches: false,
        unspent: false,
        chain_spend_txid: None,
        chain_confirm_height: None,
        csv_height: record.csv_height,
        matures_at_height: None,
        mature_for_next_block: false,
        error: None,
    }
}

fn finalize_chain_audit(
    mut audit: ActiveStakeChainAudit,
    tip_height: u64,
) -> ActiveStakeChainAudit {
    let structurally_valid = audit.error.is_none()
        && audit.transaction_confirmed
        && audit.output_exists
        && audit.amount_matches
        && audit.script_matches
        && audit.confirm_height_matches;
    audit.classification = if !structurally_valid {
        "invalid_record"
    } else if !audit.unspent {
        "spent_on_chain"
    } else if audit.mature_for_next_block {
        "redeemable_now"
    } else {
        "locked_unspent"
    }
    .to_string();
    if let Some(confirm_height) = audit.chain_confirm_height {
        let matures_at = confirm_height.saturating_add(u64::from(audit.csv_height));
        audit.matures_at_height = Some(matures_at);
        audit.mature_for_next_block = tip_height.saturating_add(1) >= matures_at;
        if structurally_valid && audit.unspent {
            audit.classification = if audit.mature_for_next_block {
                "redeemable_now"
            } else {
                "locked_unspent"
            }
            .to_string();
        }
    }
    audit
}

fn audit_esplora_record(
    client: &esplora_client::BlockingClient,
    record: &StakeRedeemRecord,
    tip_height: u64,
) -> ActiveStakeChainAudit {
    let mut audit = empty_chain_audit(record);
    let result = (|| -> Result<()> {
        let outpoint = OutPoint::from_str(&record.stake_outpoint)?;
        let info = retry_esplora("esplora get_tx_info", || client.get_tx_info(&outpoint.txid))?
            .ok_or_else(|| anyhow!("funding transaction not found"))?;
        audit.transaction_confirmed = info.status.confirmed;
        audit.chain_confirm_height = info.status.block_height.map(u64::from);
        audit.confirm_height_matches = audit.chain_confirm_height == record.confirm_height;
        let output = info.vout.get(outpoint.vout as usize);
        audit.output_exists = output.is_some();
        if let Some(output) = output {
            audit.amount_matches = output.value == record.sats;
            let public_key = bitcoin::PublicKey::from_str(&record.public_key)?;
            let expected_script = make_stake_script(public_key, record.csv_height).to_p2wsh();
            audit.script_matches = output.scriptpubkey == expected_script;
        }
        let output_status = retry_esplora("esplora get_output_status", || {
            client.get_output_status(&outpoint.txid, u64::from(outpoint.vout))
        })?
        .ok_or_else(|| anyhow!("output status not found"))?;
        audit.unspent = !output_status.spent;
        audit.chain_spend_txid = output_status.txid.map(|txid| txid.to_string());
        Ok(())
    })();
    if let Err(error) = result {
        audit.error = Some(format!("{error:#}"));
    }
    finalize_chain_audit(audit, tip_height)
}

fn audit_esplora_records(url: &str, records: &[StakeRedeemRecord]) -> Result<ChainPrecheckReport> {
    let tip_client = esplora_client::Builder::new(url)
        .timeout(30)
        .build_blocking();
    let tip_height = u64::from(retry_esplora("esplora get_height", || {
        tip_client.get_height()
    })?);
    let queue = Arc::new(Mutex::new(VecDeque::from(records.to_vec())));
    let (sender, receiver) = mpsc::channel();
    let workers = ESPLORA_WORKERS.min(records.len().max(1));
    thread::scope(|scope| {
        for _ in 0..workers {
            let queue = Arc::clone(&queue);
            let sender = sender.clone();
            let url = url.to_string();
            scope.spawn(move || {
                let client = esplora_client::Builder::new(&url)
                    .timeout(30)
                    .build_blocking();
                loop {
                    let record = match queue.lock() {
                        Ok(mut queue) => queue.pop_front(),
                        Err(_) => None,
                    };
                    let Some(record) = record else {
                        break;
                    };
                    let audit = audit_esplora_record(&client, &record, tip_height);
                    if sender.send(audit).is_err() {
                        break;
                    }
                }
            });
        }
        drop(sender);
        let mut audits = Vec::with_capacity(records.len());
        for (index, audit) in receiver.iter().enumerate() {
            audits.push(audit);
            if (index + 1) % 10 == 0 || index + 1 == records.len() {
                println!(
                    "wallet-v2 stake chain precheck: {}/{}",
                    index + 1,
                    records.len()
                );
            }
        }
        audits.sort_by(|left, right| left.outpoint.cmp(&right.outpoint));
        summarize_chain_audits("esplora", tip_height, audits)
    })
}

fn audit_electrum_records(
    service: &LocalDaemonService,
    url: &str,
    records: &[StakeRedeemRecord],
) -> Result<ChainPrecheckReport> {
    let normalized = normalize_electrum_url(url);
    let client = service.electrum_with_retry("stake import electrum client", || {
        let config = electrum_client::ConfigBuilder::new()
            .timeout(Some(crate::ELECTRUM_TIMEOUT_SECS))
            .build();
        electrum_client::Client::from_config(&normalized, config)
    })?;
    let tip_height = u64::try_from(
        service
            .electrum_with_retry("stake import electrum tip", || {
                client.block_headers_subscribe()
            })?
            .height,
    )?;
    let mut audits = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        let mut audit = empty_chain_audit(record);
        let result = (|| -> Result<()> {
            let outpoint = OutPoint::from_str(&record.stake_outpoint)?;
            let tx = service.electrum_with_retry("stake import electrum tx", || {
                client.transaction_get(&outpoint.txid)
            })?;
            audit.transaction_confirmed = true;
            let expected_height = record
                .confirm_height
                .ok_or_else(|| anyhow!("record has no confirm_height"))?;
            let merkle = service.electrum_with_retry("stake import electrum merkle", || {
                client.transaction_get_merkle(&outpoint.txid, expected_height as usize)
            })?;
            audit.chain_confirm_height = Some(u64::try_from(merkle.block_height)?);
            audit.confirm_height_matches = audit.chain_confirm_height == record.confirm_height;
            let output = tx.output.get(outpoint.vout as usize);
            audit.output_exists = output.is_some();
            if let Some(output) = output {
                audit.amount_matches = output.value.to_sat() == record.sats;
                let public_key = bitcoin::PublicKey::from_str(&record.public_key)?;
                let expected_script = make_stake_script(public_key, record.csv_height).to_p2wsh();
                audit.script_matches = output.script_pubkey == expected_script;
                let unspent = service
                    .electrum_with_retry("stake import electrum script_list_unspent", || {
                        client.script_list_unspent(output.script_pubkey.as_script())
                    })?;
                audit.unspent = unspent.iter().any(|entry| {
                    entry.tx_hash == outpoint.txid && entry.tx_pos == outpoint.vout as usize
                });
            }
            Ok(())
        })();
        if let Err(error) = result {
            audit.error = Some(format!("{error:#}"));
        }
        audits.push(finalize_chain_audit(audit, tip_height));
        if (index + 1) % 10 == 0 || index + 1 == records.len() {
            println!(
                "wallet-v2 stake chain precheck: {}/{}",
                index + 1,
                records.len()
            );
        }
    }
    summarize_chain_audits("electrum", tip_height, audits)
}

fn summarize_chain_audits(
    backend: &str,
    tip_height: u64,
    records: Vec<ActiveStakeChainAudit>,
) -> Result<ChainPrecheckReport> {
    let mut report = ChainPrecheckReport {
        backend: backend.to_string(),
        tip_height: Some(tip_height),
        records_checked: records.len(),
        records,
        ..Default::default()
    };
    for audit in &report.records {
        match audit.classification.as_str() {
            "redeemable_now" => {
                report.valid_unspent += 1;
                report.valid_unspent_sats += audit.sats;
                report.redeemable_now += 1;
                report.redeemable_now_sats += audit.sats;
            }
            "locked_unspent" => {
                report.valid_unspent += 1;
                report.valid_unspent_sats += audit.sats;
                report.locked_unspent += 1;
                report.locked_unspent_sats += audit.sats;
            }
            "spent_on_chain" => report.spent_on_chain += 1,
            _ => report.invalid += 1,
        }
    }
    Ok(report)
}

fn atomic_write_report(path: &Path, report: &StakeRedeemImportReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("stake-import-report.json");
    let temp = path.with_file_name(format!(".{filename}.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(report)?;
    fs::write(&temp, bytes)?;
    fs::rename(&temp, path)?;
    Ok(())
}

fn repair_preliminary_report(
    path: &Path,
    marker: &StakeImportMarker,
    postcheck: &PostImportReport,
) -> Result<bool> {
    let bytes = fs::read(path)
        .with_context(|| format!("read existing stake import report: {}", path.display()))?;
    let mut report: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse existing stake import report: {}", path.display()))?;
    let object = report.as_object_mut().ok_or_else(|| {
        anyhow!(
            "stake import report is not a JSON object: {}",
            path.display()
        )
    })?;
    if object.get("source_sha256").and_then(|value| value.as_str())
        != Some(marker.source_sha256.as_str())
    {
        bail!(
            "stake import report source sha256 does not match migration marker: {}",
            path.display()
        );
    }
    let report_complete = object
        .get("success")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
        && object
            .get("post_import")
            .and_then(|value| value.get("marker_verified"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
    if report_complete {
        return Ok(false);
    }

    object.insert("success".to_string(), json!(true));
    object.insert("phase".to_string(), json!("already_applied_verified"));
    object.insert("dry_run".to_string(), json!(false));
    object.insert("idempotent_replay".to_string(), json!(true));
    object.insert("completed_at_ms".to_string(), json!(now_ms()));
    object.insert(
        "import".to_string(),
        json!({
            "attempted": marker.record_count,
            "inserted": 0,
            "already_equal": marker.record_count,
            "marker_written": true
        }),
    );
    object.insert("post_import".to_string(), serde_json::to_value(postcheck)?);
    object.insert("errors".to_string(), json!([]));
    object.insert("recovered_from_marker".to_string(), json!(true));

    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("stake-import-report.json");
    let temp = path.with_file_name(format!(".{filename}.{}.tmp", std::process::id()));
    fs::write(&temp, serde_json::to_vec_pretty(&report)?)?;
    fs::rename(&temp, path)?;
    Ok(true)
}

impl LocalDaemonService {
    fn load_stake_import_marker(&self) -> Result<Option<StakeImportMarker>> {
        let migrations = self
            .db
            .keyspace("schema_migrations", KeyspaceCreateOptions::default)?;
        migrations
            .get(MIGRATION_KEY)?
            .map(|bytes| serde_json::from_slice(bytes.as_ref()).map_err(Into::into))
            .transpose()
    }

    fn default_stake_import_report_path(&self, source_sha256: &str) -> PathBuf {
        self.config.data_dir.join("reports").join(format!(
            "wallet-v2-stake-redeem-import-{}.json",
            &source_sha256[..12]
        ))
    }

    fn verify_marker(&self, marker: &StakeImportMarker) -> Result<PostImportReport> {
        if marker.schema_version != IMPORT_SCHEMA_VERSION {
            bail!(
                "unsupported wallet-v2 stake import marker version: {}",
                marker.schema_version
            );
        }
        let indexes = self.db.keyspace(
            "stake_redeems_by_public_key",
            KeyspaceCreateOptions::default,
        )?;
        let mut records = Vec::with_capacity(marker.outpoints.len());
        let mut indexes_verified = 0usize;
        for outpoint in &marker.outpoints {
            let record = self
                .get_stake_redeem(outpoint)
                .map_err(|err| anyhow!(err.to_string()))?
                .ok_or_else(|| anyhow!("imported stake record is missing: {outpoint}"))?;
            let index_key = Self::stake_redeem_index_key(&record.public_key, outpoint);
            let indexed_outpoint = indexes
                .get(index_key.as_bytes())?
                .ok_or_else(|| anyhow!("stake public-key index is missing: {outpoint}"))?;
            if indexed_outpoint.as_ref() != outpoint.as_bytes() {
                bail!("stake public-key index points to a different outpoint: {outpoint}");
            }
            indexes_verified += 1;
            records.push(record);
        }
        let immutable_digest_matches =
            immutable_records_digest(&records)? == marker.immutable_digest;
        if !immutable_digest_matches {
            bail!("imported stake immutable record digest no longer matches migration marker");
        }
        Ok(PostImportReport {
            records_verified: records.len(),
            indexes_verified,
            immutable_digest_matches,
            marker_verified: true,
        })
    }

    fn write_stake_records_atomically(
        &self,
        records: &[StakeRedeemRecord],
        marker: &StakeImportMarker,
    ) -> Result<ImportWriteReport> {
        let record_space = self
            .db
            .keyspace("stake_redeems", KeyspaceCreateOptions::default)?;
        let index_space = self.db.keyspace(
            "stake_redeems_by_public_key",
            KeyspaceCreateOptions::default,
        )?;
        let migrations = self
            .db
            .keyspace("schema_migrations", KeyspaceCreateOptions::default)?;
        let mut inserted = 0usize;
        let mut already_equal = 0usize;
        for record in records {
            match self
                .get_stake_redeem(&record.stake_outpoint)
                .map_err(|err| anyhow!(err.to_string()))?
            {
                Some(existing) if existing == *record => already_equal += 1,
                Some(_) => bail!(
                    "stake outpoint already exists with different data: {}",
                    record.stake_outpoint
                ),
                None => inserted += 1,
            }
            let index_key =
                Self::stake_redeem_index_key(&record.public_key, &record.stake_outpoint);
            if let Some(indexed) = index_space.get(index_key.as_bytes())? {
                if indexed.as_ref() != record.stake_outpoint.as_bytes() {
                    bail!(
                        "stake public-key index conflict for outpoint {}",
                        record.stake_outpoint
                    );
                }
            }
        }
        let mut tx = self.db.write_tx();
        for record in records {
            let bytes = serde_json::to_vec(record)?;
            let index_key =
                Self::stake_redeem_index_key(&record.public_key, &record.stake_outpoint);
            tx.insert(&record_space, record.stake_outpoint.as_bytes(), bytes);
            tx.insert(
                &index_space,
                index_key.as_bytes(),
                record.stake_outpoint.as_bytes(),
            );
        }
        tx.insert(&migrations, MIGRATION_KEY, serde_json::to_vec(marker)?);
        tx.commit()?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(ImportWriteReport {
            attempted: records.len(),
            inserted,
            already_equal,
            marker_written: true,
        })
    }

    pub(crate) fn import_wallet_v2_stake_redeems(
        &self,
        options: StakeRedeemImportOptions,
    ) -> Result<StakeRedeemImportRunSummary> {
        let started_at_ms = now_ms();
        let source = fs::canonicalize(&options.source).with_context(|| {
            format!(
                "canonicalize stake import source: {}",
                options.source.display()
            )
        })?;
        let source_hash = source_sha256(&source)?;
        if let Some(marker) = self.load_stake_import_marker()? {
            if marker.source_sha256 != source_hash {
                bail!(
                    "wallet-v2 stake redeems were already imported from source sha256 {}; refusing different source sha256 {}",
                    marker.source_sha256,
                    source_hash
                );
            }
            let postcheck = self.verify_marker(&marker)?;
            let report_path = PathBuf::from(&marker.report_path);
            let report_repaired = repair_preliminary_report(&report_path, &marker, &postcheck)?;
            self.logger.info(format!(
                "wallet-v2 stake import already applied source_sha256={} records={} postcheck_records={} report_repaired={} report={}",
                source_hash,
                marker.record_count,
                postcheck.records_verified,
                report_repaired,
                marker.report_path
            ));
            return Ok(StakeRedeemImportRunSummary {
                applied: false,
                idempotent_replay: true,
                record_count: marker.record_count,
                active_count: marker.initial_active_count,
                spent_count: marker.initial_spent_count,
                report_path,
            });
        }

        let parsed = parse_dump(&source)?;
        let report_path = options
            .report_path
            .clone()
            .unwrap_or_else(|| self.default_stake_import_report_path(&source_hash));
        let parsed_precheck = ParsedPrecheckReport {
            redeem_rows: parsed.redeem_count,
            records_projected: parsed.records.len(),
            active_records: parsed.active_count,
            spent_records: parsed.spent_count,
            total_sats: parsed.total_sats,
            active_sats: parsed.active_sats,
            spent_sats: parsed.spent_sats,
            missing_transfer_rows: 0,
            missing_rgb_assign_rows: 0,
            duplicate_rgb_assign_rows: 0,
            duplicate_outpoints: 0,
            invalid_records: 0,
        };
        let active_records = parsed
            .records
            .iter()
            .filter(|record| record.status == 0)
            .cloned()
            .collect::<Vec<_>>();
        let chain_precheck = match self.chain_source() {
            ChainSource::Esplora(config) => audit_esplora_records(&config.url, &active_records)?,
            ChainSource::Electrum(config) => {
                audit_electrum_records(self, &config.url, &active_records)?
            }
        };
        let chain_errors = chain_precheck
            .records
            .iter()
            .filter(|record| !record.valid_for_import())
            .map(|record| {
                format!(
                    "{}: {}{}",
                    record.outpoint,
                    record.classification,
                    record
                        .error
                        .as_deref()
                        .map(|error| format!(" ({error})"))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>();
        let mut report = StakeRedeemImportReport {
            schema_version: IMPORT_SCHEMA_VERSION,
            migration: "wallet_v2_stake_redeems_v1",
            success: false,
            phase: "prechecked".to_string(),
            dry_run: options.dry_run,
            idempotent_replay: false,
            started_at_ms,
            completed_at_ms: now_ms(),
            source_path: source.display().to_string(),
            source_member: parsed.source_member.clone(),
            source_sha256: source_hash.clone(),
            report_path: report_path.display().to_string(),
            parsed_precheck,
            chain_precheck,
            import: ImportWriteReport::default(),
            post_import: PostImportReport::default(),
            errors: chain_errors.clone(),
        };
        if !chain_errors.is_empty() {
            report.phase = "precheck_failed".to_string();
            report.completed_at_ms = now_ms();
            atomic_write_report(&report_path, &report)?;
            bail!(
                "wallet-v2 stake import precheck failed for {} active records; report={}",
                chain_errors.len(),
                report_path.display()
            );
        }
        if options.dry_run {
            report.success = true;
            report.phase = "dry_run_complete".to_string();
            report.completed_at_ms = now_ms();
            atomic_write_report(&report_path, &report)?;
            return Ok(StakeRedeemImportRunSummary {
                applied: false,
                idempotent_replay: false,
                record_count: parsed.records.len(),
                active_count: parsed.active_count,
                spent_count: parsed.spent_count,
                report_path,
            });
        }

        // Write a durable pre-import report before mutating Fjall. A crash after
        // the atomic database commit is recovered by the migration marker on
        // the next startup; the marker and all records are committed together.
        atomic_write_report(&report_path, &report)?;
        let immutable_digest = immutable_records_digest(&parsed.records)?;
        let marker = StakeImportMarker {
            schema_version: IMPORT_SCHEMA_VERSION,
            source_sha256: source_hash,
            source_member: parsed.source_member,
            imported_at_ms: now_ms(),
            record_count: parsed.records.len(),
            initial_active_count: parsed.active_count,
            initial_spent_count: parsed.spent_count,
            immutable_digest,
            outpoints: parsed
                .records
                .iter()
                .map(|record| record.stake_outpoint.clone())
                .collect(),
            report_path: report_path.display().to_string(),
        };
        report.import = self.write_stake_records_atomically(&parsed.records, &marker)?;
        report.post_import = self.verify_marker(&marker)?;
        report.success = true;
        report.phase = "imported_and_verified".to_string();
        report.completed_at_ms = now_ms();
        atomic_write_report(&report_path, &report)?;
        self.logger.info(format!(
            "wallet-v2 stake import complete records={} active={} spent={} report={}",
            parsed.records.len(),
            parsed.active_count,
            parsed.spent_count,
            report_path.display()
        ));
        Ok(StakeRedeemImportRunSummary {
            applied: true,
            idempotent_replay: false,
            record_count: parsed.records.len(),
            active_count: parsed.active_count,
            spent_count: parsed.spent_count,
            report_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_parser_rejects_duplicate_transfer_ids() {
        let sql = concat!(
            "INSERT INTO \"public\".\"transfer\" VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', NULL, NULL, NULL, NULL, 2, 2, 100, NULL, NULL);\n",
            "INSERT INTO \"public\".\"transfer\" VALUES (1, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', NULL, NULL, NULL, NULL, 2, 2, 100, NULL, NULL);\n"
        );
        let error = parse_sql(std::io::Cursor::new(sql), "duplicate.sql".to_string())
            .expect_err("duplicate transfer ids must not be silently overwritten");
        assert!(error
            .to_string()
            .contains("duplicate transfer.id at line 2"));
    }

    #[test]
    fn preliminary_report_is_repaired_from_committed_marker() {
        let path = std::env::temp_dir().join(format!(
            "stake-import-report-repair-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        fs::write(
            &path,
            serde_json::to_vec_pretty(&json!({
                "success": false,
                "phase": "prechecked",
                "source_sha256": source_sha256,
                "post_import": { "marker_verified": false },
                "errors": []
            }))
            .unwrap(),
        )
        .unwrap();
        let marker = StakeImportMarker {
            schema_version: IMPORT_SCHEMA_VERSION,
            source_sha256: source_sha256.to_string(),
            source_member: "wallet-v2.sql".to_string(),
            imported_at_ms: 1,
            record_count: 358,
            initial_active_count: 116,
            initial_spent_count: 242,
            immutable_digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_string(),
            outpoints: Vec::new(),
            report_path: path.display().to_string(),
        };
        let postcheck = PostImportReport {
            records_verified: 358,
            indexes_verified: 358,
            immutable_digest_matches: true,
            marker_verified: true,
        };

        assert!(repair_preliminary_report(&path, &marker, &postcheck).unwrap());
        let repaired: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(repaired["success"], json!(true));
        assert_eq!(repaired["phase"], json!("already_applied_verified"));
        assert_eq!(repaired["idempotent_replay"], json!(true));
        assert_eq!(repaired["import"]["attempted"], json!(358));
        assert_eq!(repaired["import"]["already_equal"], json!(358));
        assert_eq!(repaired["post_import"]["marker_verified"], json!(true));
        assert_eq!(repaired["recovered_from_marker"], json!(true));
        assert!(!repair_preliminary_report(&path, &marker, &postcheck).unwrap());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn selected_sql_parser_handles_commas_null_and_doubled_quotes() {
        let line = "INSERT INTO \"public\".\"x\" (\"a\", \"b\", \"c\", \"d\") VALUES (1, 'hello, world', NULL, '84''/0''');\n";
        let (count, fields) = parse_selected_sql_fields(line, &[0, 1, 2, 3]).unwrap();
        assert_eq!(count, 4);
        assert_eq!(fields[0].as_deref(), Some("1"));
        assert_eq!(fields[1].as_deref(), Some("hello, world"));
        assert_eq!(fields[2], None);
        assert_eq!(fields[3].as_deref(), Some("84'/0'"));
    }

    #[test]
    fn macos_zip_resource_fork_is_not_a_sql_source() {
        assert!(is_macos_zip_metadata("__MACOSX/._wallet-v2-public.sql"));
        assert!(!is_macos_zip_metadata("wallet-v2-public.sql"));
    }

    #[test]
    fn immutable_digest_ignores_redeem_status_changes() {
        let mut record = StakeRedeemRecord {
            schema_version: STAKE_REDEEM_SCHEMA_VERSION,
            stake_outpoint: format!("{}:0", bitcoin::Txid::all_zeros()),
            sats: 20_000,
            csv_height: 10,
            public_key: "03b9175c5dbb731da31fec7b9b0d04d5c4a7d098e06d1e2cbaacaef58d687e963b"
                .to_string(),
            owner_desc: Some("wpkh(test)".to_string()),
            reward_seal: Some("seal".to_string()),
            rgb_assignments: BTreeMap::from([("rgb:test".to_string(), 42)]),
            confirm_height: Some(100),
            redeem_spend_txid: None,
            status: 0,
            created_at: "2026-08-18T00:00:00Z".to_string(),
        };
        let before = immutable_records_digest(&[record.clone()]).unwrap();
        record.status = 1;
        record.redeem_spend_txid = Some(bitcoin::Txid::all_zeros().to_string());
        let after = immutable_records_digest(&[record]).unwrap();
        assert_eq!(before, after);
    }
}
