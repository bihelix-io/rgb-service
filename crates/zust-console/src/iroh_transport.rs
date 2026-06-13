use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bip39::{Language, Mnemonic};
use bitcoin::hashes::{sha256, Hash as BitcoinHash};
use bitcoin::{Network, OutPoint, Txid};
use iroh::endpoint::{presets, Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use rgbstd::containers::ConsignmentExt;
use rgbstd::ContractId;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::rgb20::{
    accept_rgb20_transfer_bytes_staged, encode_rgb20_transfer_consignment,
    validate_rgb20_transfer_bytes, Rgb20TransferResult,
};

pub const RGB_ASSIGNMENT_ALPN: &[u8] = b"bihelix/rgb-assignment/1";
pub const RGB_LN_TCP_TUNNEL_ALPN: &[u8] = b"rgb-ln/tcp-tunnel/1";
pub const RGB_LN_HEALTH_ALPN: &[u8] = b"rgb-ln/health/1";
const ASSIGNMENT_MAGIC: &[u8; 12] = b"BHRGBASN1\0\0\0";
const ACK_MAGIC: &[u8; 12] = b"BHRGBACK1\0\0\0";
const HEALTH_PING: &[u8; 8] = b"BHLNPING";
const HEALTH_PONG: &[u8; 8] = b"BHLNPONG";
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

#[derive(Clone, Debug)]
pub struct IrohAssignmentReceipt {
    pub remote_id: EndpointId,
    pub assignment: IrohAssignment,
    pub ack: IrohAssignmentAck,
}

pub fn iroh_secret_from_mnemonic(mnemonic: &str, network: Network) -> Result<SecretKey> {
    let mnemonic = Mnemonic::parse_in_normalized(Language::English, mnemonic)
        .context("invalid mnemonic for Iroh endpoint")?;
    let seed = mnemonic.to_seed_normalized("");
    let mut material = Vec::with_capacity(64 + 64);
    material.extend_from_slice(b"bihelix-iroh-endpoint-v1");
    material.extend_from_slice(network.to_string().as_bytes());
    material.extend_from_slice(&seed);
    let digest = sha256::Hash::hash(&material);
    Ok(SecretKey::from_bytes(digest.as_byte_array()))
}

pub async fn bind_iroh_endpoint(secret_key: SecretKey) -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![
            RGB_ASSIGNMENT_ALPN.to_vec(),
            RGB_LN_TCP_TUNNEL_ALPN.to_vec(),
            RGB_LN_HEALTH_ALPN.to_vec(),
        ])
        .bind()
        .await
        .context("failed to bind Iroh endpoint")
}

pub async fn bind_iroh_endpoint_from_mnemonic(
    mnemonic: &str,
    network: Network,
) -> Result<Endpoint> {
    bind_iroh_endpoint(iroh_secret_from_mnemonic(mnemonic, network)?).await
}

pub async fn wait_iroh_online(endpoint: &Endpoint) -> EndpointAddr {
    endpoint.online().await;
    endpoint.addr()
}

pub fn rgb20_transfer_assignment(result: &Rgb20TransferResult) -> Result<IrohAssignment> {
    rgb20_transfer_assignment_from_consignment(
        &result.consignment,
        result.txid,
        result.recipient_outpoint,
    )
}

pub fn rgb20_transfer_assignment_from_consignment(
    consignment: &rgbstd::containers::Transfer,
    txid: Txid,
    recipient_outpoint: OutPoint,
) -> Result<IrohAssignment> {
    let payload = encode_rgb20_transfer_consignment(consignment)?;
    let payload_sha256 = sha256_hex(&payload);
    Ok(IrohAssignment {
        envelope: IrohAssignmentEnvelope {
            version: 1,
            kind: "rgb20.transfer.consignment".to_string(),
            transfer_id: txid.to_string(),
            txid: txid.to_string(),
            contract_id: consignment.contract_id().to_string(),
            recipient_outpoint: recipient_outpoint.to_string(),
            payload_len: payload.len() as u64,
            payload_sha256,
        },
        payload,
    })
}

pub async fn send_assignment(
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

pub async fn send_assignment_with_retry_or_reject(
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
            Ok(Ok(ack)) => return Ok(ack),
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

pub async fn send_rgb20_transfer_assignment(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
    transfer: &Rgb20TransferResult,
) -> Result<IrohAssignmentAck> {
    let assignment = rgb20_transfer_assignment(transfer)?;
    send_assignment(endpoint, remote_addr, &assignment).await
}

pub async fn send_rgb20_transfer_assignment_with_retry(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
    transfer: &Rgb20TransferResult,
    attempts: u32,
    delay: Duration,
) -> Result<IrohAssignmentAck> {
    let assignment = rgb20_transfer_assignment(transfer)?;
    send_assignment_with_retry(endpoint, remote_addr, &assignment, attempts, delay).await
}

pub async fn probe_assignment_receiver(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
) -> Result<IrohAssignmentAck> {
    send_assignment(endpoint, remote_addr, &probe_assignment()).await
}

pub async fn probe_assignment_receiver_with_retry(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
    attempts: u32,
    delay: Duration,
) -> Result<IrohAssignmentAck> {
    let assignment = probe_assignment();
    send_assignment_with_retry(endpoint, remote_addr, &assignment, attempts, delay).await
}

fn probe_assignment() -> IrohAssignment {
    let assignment = IrohAssignment {
        envelope: IrohAssignmentEnvelope {
            version: 1,
            kind: "bihelix.probe".to_string(),
            transfer_id: format!("probe-{}", now()),
            txid: String::new(),
            contract_id: String::new(),
            recipient_outpoint: String::new(),
            payload_len: 0,
            payload_sha256: sha256_hex(&[]),
        },
        payload: Vec::new(),
    };
    assignment
}

pub async fn receive_probe_once(endpoint: &Endpoint) -> Result<IrohAssignmentReceipt> {
    receive_assignment_once(endpoint, |envelope, payload| {
        if envelope.kind != "bihelix.probe" {
            bail!("expected bihelix.probe, got {}", envelope.kind);
        }
        if !payload.is_empty() {
            bail!("probe payload must be empty");
        }
        Ok(())
    })
    .await
}

pub async fn receive_probe_with_retry(
    endpoint: &Endpoint,
    attempts: u32,
    delay: Duration,
) -> Result<IrohAssignmentReceipt> {
    let attempts = attempts.max(1);
    let mut last_error = None;
    for attempt in 1..=attempts {
        match tokio::time::timeout(ASSIGNMENT_ATTEMPT_TIMEOUT, receive_probe_once(endpoint)).await {
            Ok(Ok(receipt)) if receipt.ack.accepted => return Ok(receipt),
            Ok(Ok(receipt)) => {
                last_error = Some(anyhow::anyhow!(
                    "Iroh probe receiver rejected attempt {attempt}/{attempts}: {}",
                    receipt.ack.message
                ));
            }
            Ok(Err(err)) => {
                last_error = Some(err.context(format!(
                    "Iroh probe receiver attempt {attempt}/{attempts} failed"
                )));
            }
            Err(_) => {
                last_error = Some(anyhow::anyhow!(
                    "Iroh probe receiver attempt {attempt}/{attempts} timed out"
                ));
            }
        }
        if attempt < attempts {
            tokio::time::sleep(delay).await;
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Iroh probe receiver retry failed")))
}

pub async fn receive_assignment_once<F>(
    endpoint: &Endpoint,
    handler: F,
) -> Result<IrohAssignmentReceipt>
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

pub async fn receive_assignment_connection<F>(
    conn: Connection,
    handler: F,
) -> Result<IrohAssignmentReceipt>
where
    F: FnOnce(&IrohAssignmentEnvelope, &[u8]) -> Result<()>,
{
    let remote_id = conn.remote_id();
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
    Ok(IrohAssignmentReceipt {
        remote_id,
        assignment,
        ack,
    })
}

pub async fn run_iroh_tcp_tunnel_connection(conn: Connection, target: SocketAddr) -> Result<()> {
    let (send, recv) = conn
        .accept_bi()
        .await
        .context("failed to accept incoming Iroh TCP tunnel stream")?;
    proxy_iroh_stream_to_tcp(send, recv, target).await
}

pub async fn connect_iroh_tcp_tunnel(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
    tcp_stream: TcpStream,
) -> Result<()> {
    let conn = endpoint
        .connect(remote_addr, RGB_LN_TCP_TUNNEL_ALPN)
        .await
        .context("failed to connect Iroh TCP tunnel endpoint")?;
    let (send, recv) = conn
        .open_bi()
        .await
        .context("failed to open Iroh TCP tunnel stream")?;
    proxy_tcp_to_iroh_stream(tcp_stream, send, recv).await
}

pub async fn handle_iroh_health_connection(conn: Connection) -> Result<EndpointId> {
    let remote_id = conn.remote_id();
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .context("failed to accept incoming Iroh health stream")?;
    let mut ping = [0_u8; HEALTH_PING.len()];
    recv.read_exact(&mut ping)
        .await
        .context("failed to read Iroh health ping")?;
    if ping != *HEALTH_PING {
        bail!("invalid Iroh health ping");
    }
    send.write_all(HEALTH_PONG)
        .await
        .context("failed to write Iroh health pong")?;
    send.flush()
        .await
        .context("failed to flush Iroh health pong")?;
    finish_stream(&mut send, "Iroh health stream").await?;
    Ok(remote_id)
}

pub async fn probe_iroh_health(
    endpoint: &Endpoint,
    remote_addr: EndpointAddr,
) -> Result<EndpointId> {
    let remote_id = remote_addr.id;
    let conn = endpoint
        .connect(remote_addr, RGB_LN_HEALTH_ALPN)
        .await
        .context("failed to connect Iroh health endpoint")?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .context("failed to open Iroh health stream")?;
    send.write_all(HEALTH_PING)
        .await
        .context("failed to write Iroh health ping")?;
    send.flush()
        .await
        .context("failed to flush Iroh health ping")?;
    finish_stream(&mut send, "Iroh health ping stream").await?;
    let mut pong = [0_u8; HEALTH_PONG.len()];
    recv.read_exact(&mut pong)
        .await
        .context("failed to read Iroh health pong")?;
    if pong != *HEALTH_PONG {
        bail!("invalid Iroh health pong");
    }
    Ok(remote_id)
}

async fn proxy_iroh_stream_to_tcp(
    send: SendStream,
    recv: RecvStream,
    target: SocketAddr,
) -> Result<()> {
    let tcp_stream = TcpStream::connect(target)
        .await
        .with_context(|| format!("connect Iroh TCP tunnel target {target}"))?;
    proxy_tcp_to_iroh_stream(tcp_stream, send, recv).await
}

async fn proxy_tcp_to_iroh_stream(
    tcp_stream: TcpStream,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();
    let iroh_to_tcp = async {
        let copied = tokio::io::copy(&mut recv, &mut tcp_write)
            .await
            .context("copy Iroh tunnel bytes to TCP")?;
        tcp_write
            .shutdown()
            .await
            .context("shutdown TCP tunnel write half")?;
        Ok::<u64, anyhow::Error>(copied)
    };
    let tcp_to_iroh = async {
        let copied = tokio::io::copy(&mut tcp_read, &mut send)
            .await
            .context("copy TCP tunnel bytes to Iroh")?;
        send.shutdown()
            .await
            .context("shutdown Iroh tunnel write half")?;
        Ok::<u64, anyhow::Error>(copied)
    };
    tokio::try_join!(iroh_to_tcp, tcp_to_iroh)?;
    Ok(())
}

pub async fn receive_rgb20_transfer_assignment_once(
    endpoint: &Endpoint,
    receiver_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
) -> Result<IrohAssignmentReceipt> {
    receive_assignment_once(endpoint, |envelope, payload| {
        accept_rgb20_transfer_assignment_payload(
            receiver_stock_dir,
            network,
            esplora_url,
            envelope,
            payload,
        )
    })
    .await
}

pub async fn receive_rgb20_transfer_assignment_connection(
    conn: Connection,
    receiver_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
) -> Result<IrohAssignmentReceipt> {
    receive_assignment_connection(conn, |envelope, payload| {
        accept_rgb20_transfer_assignment_payload(
            receiver_stock_dir,
            network,
            esplora_url,
            envelope,
            payload,
        )
    })
    .await
}

fn accept_rgb20_transfer_assignment_payload(
    receiver_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
    envelope: &IrohAssignmentEnvelope,
    payload: &[u8],
) -> Result<()> {
    validate_rgb20_transfer_envelope(envelope)?;
    let txid = envelope
        .txid
        .parse()
        .with_context(|| format!("invalid assignment txid: {}", envelope.txid))?;
    accept_rgb20_transfer_bytes_staged(receiver_stock_dir, network, esplora_url, txid, payload)
        .map(|_| ())
}

pub async fn receive_rgb20_transfer_assignment_validation_once(
    endpoint: &Endpoint,
    network: Network,
    esplora_url: &str,
) -> Result<IrohAssignmentReceipt> {
    receive_assignment_once(endpoint, |envelope, payload| {
        validate_rgb20_transfer_envelope(envelope)?;
        validate_rgb20_transfer_bytes(network, esplora_url, payload).map(|_| ())
    })
    .await
}

pub async fn receive_rgb20_transfer_assignment_validation_checked_once<F>(
    endpoint: &Endpoint,
    network: Network,
    esplora_url: &str,
    check: F,
) -> Result<IrohAssignmentReceipt>
where
    F: FnOnce(&IrohAssignmentEnvelope, &[u8]) -> Result<()>,
{
    receive_assignment_once(endpoint, |envelope, payload| {
        validate_rgb20_transfer_envelope(envelope)?;
        validate_rgb20_transfer_bytes(network, esplora_url, payload).map(|_| ())?;
        check(envelope, payload)
    })
    .await
}

pub async fn receive_rgb20_transfer_assignment_with_retry(
    endpoint: &Endpoint,
    receiver_stock_dir: &Path,
    network: Network,
    esplora_url: &str,
    attempts: u32,
    delay: Duration,
) -> Result<IrohAssignmentReceipt> {
    let attempts = attempts.max(1);
    let mut last_error = None;
    for attempt in 1..=attempts {
        match tokio::time::timeout(
            ASSIGNMENT_ATTEMPT_TIMEOUT,
            receive_rgb20_transfer_assignment_once(
                endpoint,
                receiver_stock_dir,
                network,
                esplora_url,
            ),
        )
        .await
        {
            Ok(Ok(receipt)) if receipt.ack.accepted => return Ok(receipt),
            Ok(Ok(receipt)) => {
                last_error = Some(anyhow::anyhow!(
                    "Iroh receiver rejected assignment attempt {attempt}/{attempts}: {}",
                    receipt.ack.message
                ));
            }
            Ok(Err(err)) => {
                last_error =
                    Some(err.context(format!("Iroh receiver attempt {attempt}/{attempts} failed")));
            }
            Err(_) => {
                last_error = Some(anyhow::anyhow!(
                    "Iroh receiver attempt {attempt}/{attempts} timed out"
                ));
            }
        }
        if attempt < attempts {
            tokio::time::sleep(delay).await;
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Iroh receiver retry failed")))
}

pub async fn receive_rgb20_transfer_assignment_validation_with_retry(
    endpoint: &Endpoint,
    network: Network,
    esplora_url: &str,
    attempts: u32,
    delay: Duration,
) -> Result<IrohAssignmentReceipt> {
    let attempts = attempts.max(1);
    let mut last_error = None;
    for attempt in 1..=attempts {
        match tokio::time::timeout(
            ASSIGNMENT_ATTEMPT_TIMEOUT,
            receive_rgb20_transfer_assignment_validation_once(endpoint, network, esplora_url),
        )
        .await
        {
            Ok(Ok(receipt)) if receipt.ack.accepted => return Ok(receipt),
            Ok(Ok(receipt)) => {
                last_error = Some(anyhow::anyhow!(
                    "Iroh receiver rejected validation attempt {attempt}/{attempts}: {}",
                    receipt.ack.message
                ));
            }
            Ok(Err(err)) => {
                last_error = Some(err.context(format!(
                    "Iroh receiver validation attempt {attempt}/{attempts} failed"
                )));
            }
            Err(_) => {
                last_error = Some(anyhow::anyhow!(
                    "Iroh receiver validation attempt {attempt}/{attempts} timed out"
                ));
            }
        }
        if attempt < attempts {
            tokio::time::sleep(delay).await;
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Iroh receiver validation retry failed")))
}

pub async fn receive_rgb20_transfer_assignment_validation_checked_with_retry_or_reject<F>(
    endpoint: &Endpoint,
    network: Network,
    esplora_url: &str,
    attempts: u32,
    delay: Duration,
    check: F,
) -> Result<IrohAssignmentReceipt>
where
    F: Fn(&IrohAssignmentEnvelope, &[u8]) -> Result<()>,
{
    let attempts = attempts.max(1);
    let mut last_error = None;
    for attempt in 1..=attempts {
        match tokio::time::timeout(
            ASSIGNMENT_ATTEMPT_TIMEOUT,
            receive_rgb20_transfer_assignment_validation_checked_once(
                endpoint,
                network,
                esplora_url,
                |envelope, payload| check(envelope, payload),
            ),
        )
        .await
        {
            Ok(Ok(receipt)) => return Ok(receipt),
            Ok(Err(err)) => {
                last_error = Some(err.context(format!(
                    "Iroh receiver validation attempt {attempt}/{attempts} failed"
                )));
            }
            Err(_) => {
                last_error = Some(anyhow::anyhow!(
                    "Iroh receiver validation attempt {attempt}/{attempts} timed out"
                ));
            }
        }
        if attempt < attempts {
            tokio::time::sleep(delay).await;
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Iroh receiver validation retry failed")))
}

fn validate_rgb20_transfer_envelope(envelope: &IrohAssignmentEnvelope) -> Result<()> {
    if envelope.kind != "rgb20.transfer.consignment" && envelope.kind != "rgb.transfer.consignment"
    {
        bail!("unsupported Iroh assignment kind: {}", envelope.kind);
    }
    let _txid: Txid = envelope
        .txid
        .parse()
        .with_context(|| format!("invalid assignment txid: {}", envelope.txid))?;
    let _contract_id: ContractId = envelope
        .contract_id
        .parse()
        .with_context(|| format!("invalid assignment contract_id: {}", envelope.contract_id))?;
    let _recipient_outpoint: OutPoint = envelope.recipient_outpoint.parse().with_context(|| {
        format!(
            "invalid assignment recipient_outpoint: {}",
            envelope.recipient_outpoint
        )
    })?;
    Ok(())
}

fn validate_assignment(assignment: &IrohAssignment) -> Result<()> {
    if assignment.envelope.version != 1 {
        bail!(
            "unsupported Iroh assignment version: {}",
            assignment.envelope.version
        );
    }
    if assignment.envelope.payload_len != assignment.payload.len() as u64 {
        bail!(
            "Iroh assignment payload length mismatch: header {}, actual {}",
            assignment.envelope.payload_len,
            assignment.payload.len()
        );
    }
    if assignment.envelope.payload_len > MAX_ASSIGNMENT_BYTES {
        bail!(
            "Iroh assignment payload too large: {} bytes",
            assignment.envelope.payload_len
        );
    }
    let actual_sha256 = sha256_hex(&assignment.payload);
    if assignment.envelope.payload_sha256 != actual_sha256 {
        bail!(
            "Iroh assignment payload sha256 mismatch: header {}, actual {}",
            assignment.envelope.payload_sha256,
            actual_sha256
        );
    }
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
    if envelope.payload_len > MAX_ASSIGNMENT_BYTES {
        bail!(
            "Iroh assignment payload too large: {} bytes",
            envelope.payload_len
        );
    }
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
    sha256::Hash::hash(bytes).to_string()
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
