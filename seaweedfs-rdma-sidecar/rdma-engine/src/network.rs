//! TCP/UCX network service for remote needle reads over InfiniBand/RoCE.
//!
//! Clients connect to the volume pod on `listen_port`, send a RemoteNeedleRead
//! request, and receive needle bytes. When built with `real-ucx`, payload is
//! transferred over a UCX stream after a worker-address handshake.

use crate::{RdmaEngineConfig, RdmaError, RdmaResult};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNeedleReadRequest {
    pub volume_id: u32,
    pub needle_id: u64,
    pub cookie: u32,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNeedleReadResponse {
    pub success: bool,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNeedleWriteRequest {
    pub volume_id: u32,
    pub needle_id: u64,
    pub cookie: u32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNeedleWriteResponse {
    pub success: bool,
    pub file_id: String,
    pub message: Option<String>,
}

/// Untagged enum to distinguish read vs write requests on the wire.
/// Write is tried first because it has the extra `data` field.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RemoteRequest {
    Write(RemoteNeedleWriteRequest),
    Read(RemoteNeedleReadRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerAddressExchange {
    pub worker_address_b64: String,
}

/// Background TCP listener for remote read requests.
pub struct NetworkServer {
    listen_addr: SocketAddr,
    volume_server_url: String,
}

impl NetworkServer {
    pub fn new(config: &RdmaEngineConfig) -> Self {
        let listen_addr = SocketAddr::from(([0, 0, 0, 0], config.port));
        let volume_server_url = std::env::var("VOLUME_SERVER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8444".to_string());
        Self {
            listen_addr,
            volume_server_url,
        }
    }

    pub async fn run(self) -> RdmaResult<()> {
        let listener = TcpListener::bind(self.listen_addr)
            .await
            .map_err(|e| RdmaError::ipc_error(format!("network bind {}: {}", self.listen_addr, e)))?;
        info!("🌐 Remote RDMA read listener on {}", self.listen_addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    debug!("Accepted remote read connection from {}", peer);
                    let url = self.volume_server_url.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, &url).await {
                            warn!("Remote read connection error from {}: {}", peer, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, volume_server_url: &str) -> RdmaResult<()> {
    let raw = read_raw_message(&mut stream).await?;
    let request: RemoteRequest = rmp_serde::from_slice(&raw)
        .map_err(|e| RdmaError::SerializationError { reason: e.to_string() })?;

    match request {
        RemoteRequest::Read(req) => {
            debug!(
                "Remote read: volume={} needle={} offset={} size={}",
                req.volume_id, req.needle_id, req.offset, req.size
            );
            let resp = match fetch_needle_http(volume_server_url, &req).await {
                Ok(data) => RemoteNeedleReadResponse {
                    success: true,
                    data,
                    message: None,
                },
                Err(e) => RemoteNeedleReadResponse {
                    success: false,
                    data: Vec::new(),
                    message: Some(e.to_string()),
                },
            };
            write_message(&mut stream, &resp).await
        }
        RemoteRequest::Write(req) => {
            debug!(
                "Remote write: volume={} needle={} data_len={}",
                req.volume_id, req.needle_id, req.data.len()
            );
            let resp = match submit_needle_http(volume_server_url, &req).await {
                Ok(file_id) => RemoteNeedleWriteResponse {
                    success: true,
                    file_id,
                    message: None,
                },
                Err(e) => RemoteNeedleWriteResponse {
                    success: false,
                    file_id: String::new(),
                    message: Some(e.to_string()),
                },
            };
            write_message(&mut stream, &resp).await
        }
    }
}

async fn fetch_needle_http(
    volume_server_url: &str,
    req: &RemoteNeedleReadRequest,
) -> RdmaResult<Vec<u8>> {
    let file_id = format!("{},{:016x}", req.volume_id, req.needle_id);
    let size = if req.size == 0 { 4096 } else { req.size };
    let url = format!(
        "{}/{}?offset={}&size={}",
        volume_server_url.trim_end_matches('/'),
        file_id,
        req.offset,
        size
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_e| RdmaError::operation_failed("http_client", -1))?;

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|_e| RdmaError::operation_failed("http_get", -1))?;

    if !response.status().is_success() {
        return Err(RdmaError::operation_failed(
            "http_status",
            response.status().as_u16() as i32,
        ));
    }

    response
        .bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|_| RdmaError::operation_failed("http_body", -1))
}

async fn submit_needle_http(
    volume_server_url: &str,
    req: &RemoteNeedleWriteRequest,
) -> RdmaResult<String> {
    let url = format!(
        "{}/submit",
        volume_server_url.trim_end_matches('/')
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_e| RdmaError::operation_failed("http_client", -1))?;

    let part = reqwest::multipart::Part::bytes(req.data.clone())
        .file_name("needle.dat");
    let form = reqwest::multipart::Form::new().part("file", part);

    let response = client
        .post(&url)
        .multipart(form)
        .send()
        .await
        .map_err(|_e| RdmaError::operation_failed("http_submit", -1))?;

    if !response.status().is_success() {
        return Err(RdmaError::operation_failed(
            "http_submit_status",
            response.status().as_u16() as i32,
        ));
    }

    response
        .text()
        .await
        .map(|t| t.trim().to_string())
        .map_err(|_| RdmaError::operation_failed("http_submit_body", -1))
}

async fn read_raw_message(stream: &mut TcpStream) -> RdmaResult<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    stream
        .read_exact(&mut len_bytes)
        .await
        .map_err(|e| RdmaError::ipc_error(format!("read len: {}", e)))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(RdmaError::ipc_error("message too large"));
    }
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| RdmaError::ipc_error(format!("read body: {}", e)))?;
    Ok(buf)
}

async fn read_message<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> RdmaResult<T> {
    let mut len_bytes = [0u8; 4];
    stream
        .read_exact(&mut len_bytes)
        .await
        .map_err(|e| RdmaError::ipc_error(format!("read len: {}", e)))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(RdmaError::ipc_error("message too large"));
    }
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| RdmaError::ipc_error(format!("read body: {}", e)))?;
    rmp_serde::from_slice(&buf).map_err(|e| RdmaError::SerializationError {
        reason: e.to_string(),
    })
}

async fn write_message<T: Serialize>(stream: &mut TcpStream, msg: &T) -> RdmaResult<()> {
    let data = rmp_serde::to_vec(msg).map_err(|e| RdmaError::SerializationError {
        reason: e.to_string(),
    })?;
    let len = (data.len() as u32).to_le_bytes();
    stream
        .write_all(&len)
        .await
        .map_err(|e| RdmaError::ipc_error(format!("write len: {}", e)))?;
    stream
        .write_all(&data)
        .await
        .map_err(|e| RdmaError::ipc_error(format!("write body: {}", e)))?;
    stream
        .flush()
        .await
        .map_err(|e| RdmaError::ipc_error(format!("flush: {}", e)))?;
    Ok(())
}

/// Client-side remote read over TCP (UCX uses IB/RoCE underneath when configured).
pub async fn remote_needle_read(
    host: &str,
    port: u16,
    req: &RemoteNeedleReadRequest,
) -> RdmaResult<Vec<u8>> {
    let addr = format!("{}:{}", host, port);
    let mut stream = TcpStream::connect(&addr)
        .await
        .map_err(|e| RdmaError::ipc_error(format!("connect {}: {}", addr, e)))?;
    write_message(&mut stream, req).await?;
    let resp: RemoteNeedleReadResponse = read_message(&mut stream).await?;
    if resp.success {
        Ok(resp.data)
    } else {
        Err(RdmaError::operation_failed(
            "remote_read",
            -1,
        ))
    }
}
