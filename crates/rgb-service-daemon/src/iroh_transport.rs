use std::time::Duration;

use anyhow::{Context, Result, bail};
use bitcoin::hashes::{Hash as BitcoinHash, sha256};
use bitcoin::{OutPoint, Txid};
use iroh::endpoint::{Connection, RecvStream, SendStream, presets};
use iroh::{Endpoint, EndpointAddr, SecretKey};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

pub const RGB_ASSIGNMENT_ALPN: &[u8] = b"bihelix/rgb-assignment/1";
const ASSIGNMENT_MAGIC: &[u8; 12] = b"BHRGBASN1\0\0\0";
const ACK_MAGIC: &[u8; 12] = b"BHRGBACK1\0\0\0";
const MAX_HEADER_LEN: usize = 64 * 1024;
const MAX_ASSIGNMENT_BYTES: u64 = 128 * 1024 * 1024;
const ASSIGNMENT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const STREAM_FINISH_ACK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IrohAssignmentEnvelope {
    pub version: u16,
    pub kind: String,
    pub transfer_id: String,
    pub txid: String,
    pub contract_id: String,
    pub recipient_outpoint: String,
    #[serde(default)]
    pub topic: Option<String>,
    pub payload_len: u64,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IrohAssignmentAck {
    pub version: u16,
    pub transfer_id: String,
    pub accepted: bool,
    pub message: String,
    pub payload_sha256: String,
}

#[derive(Clone, Debug)]
pub struct IrohAssignment {
    pub envelope: IrohAssignmentEnvelope,
    pub payload: Vec<u8>,
}

pub fn secret_key_from_hex(value: &str) -> Result<SecretKey> {
    let bytes = hex_decode(value)?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("iroh.secret_key_hex must decode to exactly 32 bytes"))?;
    Ok(SecretKey::from_bytes(&bytes))
}

pub async fn bind_assignment_endpoint(secret_key: SecretKey) -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![RGB_ASSIGNMENT_ALPN.to_vec()])
        .bind()
        .await
        .context("failed to bind Iroh assignment endpoint")
}

pub fn assignment_from_consignment_bytes(
    consignment: Vec<u8>,
    transfer_id: String,
    txid: Txid,
    contract_id: String,
    recipient_outpoint: OutPoint,
    topic: Option<String>,
) -> IrohAssignment {
    let payload_sha256 = sha256_hex(&consignment);
    IrohAssignment {
        envelope: IrohAssignmentEnvelope {
            version: 1,
            kind: "rgb20.transfer.consignment".to_string(),
            transfer_id,
            txid: txid.to_string(),
            contract_id,
            recipient_outpoint: recipient_outpoint.to_string(),
            topic,
            payload_len: consignment.len() as u64,
            payload_sha256,
        },
        payload: consignment,
    }
}

pub async fn send_assignment_with_retry(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
    assignment: &IrohAssignment,
    attempts: u32,
    delay: Duration,
) -> Result<IrohAssignmentAck> {
    let attempts = attempts.max(1);
    let mut last_error = None;
    for attempt in 1..=attempts {
        match tokio::time::timeout(
            ASSIGNMENT_ATTEMPT_TIMEOUT,
            send_assignment(endpoint, remote_addr.clone(), assignment),
        )
        .await
        {
            Ok(Ok(ack)) if ack.accepted => return Ok(ack),
            Ok(Ok(ack)) => {
                last_error = Some(anyhow::anyhow!(
                    "remote rejected Iroh assignment attempt {attempt}/{attempts}: {}",
                    ack.message
                ));
            }
            Ok(Err(err)) => {
                last_error = Some(err.context(format!(
                    "Iroh assignment attempt {attempt}/{attempts} failed"
                )));
            }
            Err(_) => {
                last_error = Some(anyhow::anyhow!(
                    "Iroh assignment attempt {attempt}/{attempts} timed out"
                ));
            }
        }
        if attempt < attempts {
            tokio::time::sleep(delay).await;
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Iroh assignment retry failed")))
}

pub async fn receive_assignment_once<F>(endpoint: &Endpoint, handler: F) -> Result<IrohAssignmentAck>
where
    F: FnOnce(&IrohAssignmentEnvelope, &[u8]) -> Result<()>,
{
    let connecting = endpoint
        .accept()
        .await
        .context("Iroh endpoint was closed while waiting for assignment")?;
    let conn = connecting
        .await
        .context("failed to complete incoming Iroh assignment connection")?;
    receive_assignment_connection(conn, handler).await
}

async fn send_assignment(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
    assignment: &IrohAssignment,
) -> Result<IrohAssignmentAck> {
    validate_assignment(assignment)?;
    let conn = endpoint
        .connect(remote_addr, RGB_ASSIGNMENT_ALPN)
        .await
        .context("failed to connect to remote Iroh endpoint")?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .context("failed to open Iroh assignment stream")?;
    write_assignment(&mut send, assignment).await?;
    finish_stream(&mut send, "Iroh assignment stream").await?;
    let ack = read_ack(&mut recv).await?;
    if ack.transfer_id != assignment.envelope.transfer_id {
        bail!(
            "Iroh assignment ack transfer_id mismatch: expected {}, got {}",
            assignment.envelope.transfer_id,
            ack.transfer_id
        );
    }
    if ack.payload_sha256 != assignment.envelope.payload_sha256 {
        bail!(
            "Iroh assignment ack sha256 mismatch: expected {}, got {}",
            assignment.envelope.payload_sha256,
            ack.payload_sha256
        );
    }
    Ok(ack)
}

async fn receive_assignment_connection<F>(conn: Connection, handler: F) -> Result<IrohAssignmentAck>
where
    F: FnOnce(&IrohAssignmentEnvelope, &[u8]) -> Result<()>,
{
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .context("failed to accept incoming Iroh assignment stream")?;
    let assignment = read_assignment(&mut recv).await?;
    let ack = match handler(&assignment.envelope, &assignment.payload) {
        Ok(()) => IrohAssignmentAck {
            version: 1,
            transfer_id: assignment.envelope.transfer_id.clone(),
            accepted: true,
            message: "accepted".to_string(),
            payload_sha256: assignment.envelope.payload_sha256.clone(),
        },
        Err(err) => IrohAssignmentAck {
            version: 1,
            transfer_id: assignment.envelope.transfer_id.clone(),
            accepted: false,
            message: err.to_string(),
            payload_sha256: assignment.envelope.payload_sha256.clone(),
        },
    };
    write_ack(&mut send, &ack).await?;
    finish_stream(&mut send, "Iroh assignment ack stream").await?;
    Ok(ack)
}

fn validate_assignment(assignment: &IrohAssignment) -> Result<()> {
    validate_envelope(&assignment.envelope)?;
    if assignment.envelope.payload_len != assignment.payload.len() as u64 {
        bail!(
            "Iroh assignment payload length mismatch: header {}, actual {}",
            assignment.envelope.payload_len,
            assignment.payload.len()
        );
    }
    if assignment.envelope.payload_sha256 != sha256_hex(&assignment.payload) {
        bail!("Iroh assignment payload sha256 mismatch");
    }
    Ok(())
}

fn validate_envelope(envelope: &IrohAssignmentEnvelope) -> Result<()> {
    if envelope.version != 1 {
        bail!("unsupported Iroh assignment version: {}", envelope.version);
    }
    if envelope.kind != "rgb20.transfer.consignment" && envelope.kind != "rgb.transfer.consignment" {
        bail!("unsupported Iroh assignment kind: {}", envelope.kind);
    }
    if envelope.payload_len > MAX_ASSIGNMENT_BYTES {
        bail!("Iroh assignment payload too large: {} bytes", envelope.payload_len);
    }
    let _txid: Txid = envelope
        .txid
        .parse()
        .with_context(|| format!("invalid assignment txid: {}", envelope.txid))?;
    let _recipient_outpoint: OutPoint = envelope.recipient_outpoint.parse().with_context(|| {
        format!(
            "invalid assignment recipient_outpoint: {}",
            envelope.recipient_outpoint
        )
    })?;
    Ok(())
}

async fn write_assignment(send: &mut SendStream, assignment: &IrohAssignment) -> Result<()> {
    let header = serde_json::to_vec(&assignment.envelope)
        .context("failed to encode Iroh assignment header")?;
    if header.len() > MAX_HEADER_LEN {
        bail!("Iroh assignment header too large: {} bytes", header.len());
    }
    send.write_all(ASSIGNMENT_MAGIC)
        .await
        .context("failed to write Iroh assignment magic")?;
    write_u32(send, header.len() as u32).await?;
    send.write_all(&header)
        .await
        .context("failed to write Iroh assignment header")?;
    send.write_all(&assignment.payload)
        .await
        .context("failed to write Iroh assignment payload")?;
    send.flush()
        .await
        .context("failed to flush Iroh assignment stream")?;
    Ok(())
}

async fn read_assignment(recv: &mut RecvStream) -> Result<IrohAssignment> {
    read_magic(recv, ASSIGNMENT_MAGIC).await?;
    let header_len = read_u32(recv).await? as usize;
    if header_len > MAX_HEADER_LEN {
        bail!("Iroh assignment header too large: {header_len} bytes");
    }
    let mut header = vec![0; header_len];
    recv.read_exact(&mut header)
        .await
        .context("failed to read Iroh assignment header")?;
    let envelope: IrohAssignmentEnvelope =
        serde_json::from_slice(&header).context("failed to decode Iroh assignment header")?;
    validate_envelope(&envelope)?;
    let mut payload = vec![0; envelope.payload_len as usize];
    recv.read_exact(&mut payload)
        .await
        .context("failed to read Iroh assignment payload")?;
    let assignment = IrohAssignment { envelope, payload };
    validate_assignment(&assignment)?;
    Ok(assignment)
}

async fn write_ack(send: &mut SendStream, ack: &IrohAssignmentAck) -> Result<()> {
    let bytes = serde_json::to_vec(ack).context("failed to encode Iroh assignment ack")?;
    if bytes.len() > MAX_HEADER_LEN {
        bail!("Iroh assignment ack too large: {} bytes", bytes.len());
    }
    send.write_all(ACK_MAGIC)
        .await
        .context("failed to write Iroh assignment ack magic")?;
    write_u32(send, bytes.len() as u32).await?;
    send.write_all(&bytes)
        .await
        .context("failed to write Iroh assignment ack")?;
    send.flush()
        .await
        .context("failed to flush Iroh assignment ack stream")?;
    Ok(())
}

async fn read_ack(recv: &mut RecvStream) -> Result<IrohAssignmentAck> {
    read_magic(recv, ACK_MAGIC).await?;
    let len = read_u32(recv).await? as usize;
    if len > MAX_HEADER_LEN {
        bail!("Iroh assignment ack too large: {len} bytes");
    }
    let mut bytes = vec![0; len];
    recv.read_exact(&mut bytes)
        .await
        .context("failed to read Iroh assignment ack")?;
    serde_json::from_slice(&bytes).context("failed to decode Iroh assignment ack")
}

async fn finish_stream(send: &mut SendStream, label: &str) -> Result<()> {
    send.finish()
        .with_context(|| format!("failed to finish {label}"))?;
    let stopped = tokio::time::timeout(STREAM_FINISH_ACK_TIMEOUT, send.stopped())
        .await
        .with_context(|| format!("timed out waiting for peer to receive {label}"))?
        .with_context(|| format!("failed while waiting for peer to receive {label}"))?;
    if let Some(error_code) = stopped {
        bail!("{label} was stopped by peer with error code {error_code}");
    }
    Ok(())
}

async fn read_magic(recv: &mut RecvStream, expected: &[u8; 12]) -> Result<()> {
    let mut actual = [0; 12];
    recv.read_exact(&mut actual)
        .await
        .context("failed to read Iroh assignment frame magic")?;
    if &actual != expected {
        bail!("invalid Iroh assignment frame magic");
    }
    Ok(())
}

async fn write_u32(send: &mut SendStream, value: u32) -> Result<()> {
    send.write_all(&value.to_be_bytes())
        .await
        .context("failed to write Iroh assignment frame length")
}

async fn read_u32(recv: &mut RecvStream) -> Result<u32> {
    let mut bytes = [0; 4];
    recv.read_exact(&mut bytes)
        .await
        .context("failed to read Iroh assignment frame length")?;
    Ok(u32::from_be_bytes(bytes))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_encode(sha256::Hash::hash(bytes).as_byte_array())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    let value = value.trim();
    if value.len() % 2 != 0 {
        bail!("hex string must have an even length");
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .with_context(|| format!("invalid hex at byte {}", index / 2))
        })
        .collect()
}
