//! TCP/UCX network service for remote needle reads over InfiniBand/RoCE.
//!
//! Clients connect to the volume pod on `listen_port`, send a RemoteNeedleRead
//! request, and receive needle bytes. When built with `real-ucx`, payload is
//! transferred over a UCX stream after a worker-address handshake.

use crate::{
    buffer_pool::RegisteredBufferPool, ipc::MAX_IPC_MESSAGE_SIZE, local_volume::LocalVolumeReader,
    rdma::RdmaContext, volume_grpc, RdmaEngineConfig, RdmaError, RdmaResult,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use reqwest::header::{CONTENT_RANGE, RANGE};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

static NEXT_NETWORK_WR_ID: AtomicU64 = AtomicU64::new(1_000_000);

/// Format SeaweedFS file id: `{volume_id},{needle_id+cookie hex}` (matches Go needle.FileId).
pub(crate) fn format_seaweed_file_id(volume_id: u32, needle_id: u64, cookie: u32) -> String {
    let mut bytes = [0u8; 12];
    bytes[0..8].copy_from_slice(&needle_id.to_be_bytes());
    bytes[8..12].copy_from_slice(&cookie.to_be_bytes());
    let mut start = 0usize;
    while start < 8 && bytes[start] == 0 {
        start += 1;
    }
    let mut hex_id = String::with_capacity((12 - start) * 2);
    for b in &bytes[start..] {
        use std::fmt::Write;
        let _ = write!(hex_id, "{:02x}", b);
    }
    format!("{},{}", volume_id, hex_id)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNeedleReadRequest {
    pub volume_id: u32,
    pub needle_id: u64,
    pub cookie: u32,
    pub offset: u64,
    pub size: u64,
    #[serde(default)]
    pub worker_address_b64: String,
    #[serde(default)]
    pub remote_addr: u64,
    #[serde(default)]
    pub remote_key_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNeedleReadResponse {
    pub success: bool,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    #[serde(default)]
    pub size: u64,
    #[serde(default = "default_transport")]
    pub transport: String,
    #[serde(default)]
    pub real_rdma: bool,
    #[serde(default)]
    pub source: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNeedleWriteRequest {
    pub volume_id: u32,
    pub needle_id: u64,
    pub cookie: u32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub worker_address_b64: String,
    #[serde(default)]
    pub remote_addr: u64,
    #[serde(default)]
    pub remote_key_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNeedleWriteResponse {
    pub success: bool,
    pub file_id: String,
    #[serde(default = "default_transport")]
    pub transport: String,
    #[serde(default)]
    pub real_rdma: bool,
    #[serde(default)]
    pub source: String,
    pub message: Option<String>,
}

fn default_transport() -> String {
    "tcp".to_string()
}

fn peer_cache_key(prefix: &str, volume_id: u32, needle_id: u64, worker_address: &[u8]) -> String {
    let mut hasher = DefaultHasher::new();
    worker_address.hash(&mut hasher);
    format!(
        "{}-{}-{}-{:016x}",
        prefix,
        volume_id,
        needle_id,
        hasher.finish()
    )
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
    volume_write_url: String,
    rdma_context: Arc<RdmaContext>,
    local_reader: Option<Arc<LocalVolumeReader>>,
    registered_buffers: Arc<RegisteredBufferPool>,
}

impl NetworkServer {
    pub fn new(config: &RdmaEngineConfig, rdma_context: Arc<RdmaContext>) -> Self {
        let listen_addr = SocketAddr::from(([0, 0, 0, 0], config.port));
        let volume_server_url = std::env::var("VOLUME_SERVER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8444".to_string());
        let volume_write_url = std::env::var("VOLUME_SERVER_GRPC_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| volume_server_url.clone());
        let local_reader = LocalVolumeReader::from_env().map(Arc::new);
        Self {
            listen_addr,
            volume_server_url,
            volume_write_url,
            rdma_context,
            local_reader,
            registered_buffers: Arc::new(RegisteredBufferPool::from_env()),
        }
    }

    pub async fn run(self) -> RdmaResult<()> {
        let listener = TcpListener::bind(self.listen_addr).await.map_err(|e| {
            RdmaError::ipc_error(format!("network bind {}: {}", self.listen_addr, e))
        })?;
        info!("🌐 Remote RDMA read listener on {}", self.listen_addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    debug!("Accepted remote read connection from {}", peer);
                    let url = self.volume_server_url.clone();
                    let write_url = self.volume_write_url.clone();
                    let rdma_context = self.rdma_context.clone();
                    let local_reader = self.local_reader.clone();
                    let registered_buffers = self.registered_buffers.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(
                            stream,
                            &url,
                            &write_url,
                            local_reader,
                            rdma_context,
                            registered_buffers,
                        )
                        .await
                        {
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

async fn handle_connection(
    mut stream: TcpStream,
    volume_server_url: &str,
    volume_write_url: &str,
    local_reader: Option<Arc<LocalVolumeReader>>,
    rdma_context: Arc<RdmaContext>,
    registered_buffers: Arc<RegisteredBufferPool>,
) -> RdmaResult<()> {
    loop {
        let raw = match read_raw_message(&mut stream).await {
            Ok(raw) => raw,
            Err(e) if is_clean_connection_close(&e) => return Ok(()),
            Err(e) => return Err(e),
        };
        let request: RemoteRequest =
            rmp_serde::from_slice(&raw).map_err(|e| RdmaError::SerializationError {
                reason: e.to_string(),
            })?;

        match request {
            RemoteRequest::Read(req) => {
                debug!(
                    "Remote read: volume={} needle={} offset={} size={}",
                    req.volume_id, req.needle_id, req.offset, req.size
                );
                let resp =
                    match fetch_needle_data(volume_server_url, local_reader.as_deref(), &req).await {
                        Ok((data, source)) => {
                            match put_read_payload_rdma(&rdma_context, &registered_buffers, &req, &data)
                                .await
                            {
                                Ok(true) => RemoteNeedleReadResponse {
                                    success: true,
                                    data: Vec::new(),
                                    size: data.len() as u64,
                                    transport: "rdma".to_string(),
                                    real_rdma: true,
                                    source,
                                    message: None,
                                },
                                Ok(false) => RemoteNeedleReadResponse {
                                    success: true,
                                    size: data.len() as u64,
                                    data,
                                    transport: default_transport(),
                                    real_rdma: false,
                                    source,
                                    message: None,
                                },
                                Err(e) => {
                                    warn!(
                                        "RDMA read payload PUT failed, falling back to TCP payload: {}",
                                        e
                                    );
                                    RemoteNeedleReadResponse {
                                        success: true,
                                        size: data.len() as u64,
                                        data,
                                        transport: default_transport(),
                                        real_rdma: false,
                                        source,
                                        message: Some(e.to_string()),
                                    }
                                }
                            }
                        }
                        Err(e) => RemoteNeedleReadResponse {
                            success: false,
                            data: Vec::new(),
                            size: 0,
                            transport: default_transport(),
                            real_rdma: false,
                            source: String::new(),
                            message: Some(e.to_string()),
                        },
                    };
                write_message(&mut stream, &resp).await?;
            }
            RemoteRequest::Write(req) => {
                debug!(
                    "Remote write: volume={} needle={} data_len={}",
                    req.volume_id,
                    req.needle_id,
                    req.data.len()
                );
                let resp = match resolve_write_payload(&rdma_context, &registered_buffers, &req).await {
                    Ok((data, used_rdma)) => {
                        match submit_needle(
                            volume_server_url,
                            volume_write_url,
                            local_reader.as_deref(),
                            &req,
                            data,
                        )
                        .await
                        {
                            Ok((file_id, source)) => RemoteNeedleWriteResponse {
                                success: true,
                                file_id,
                                transport: if used_rdma {
                                    "rdma".to_string()
                                } else {
                                    default_transport()
                                },
                                real_rdma: used_rdma,
                                source,
                                message: None,
                            },
                            Err(e) => RemoteNeedleWriteResponse {
                                success: false,
                                file_id: String::new(),
                                transport: if used_rdma {
                                    "rdma".to_string()
                                } else {
                                    default_transport()
                                },
                                real_rdma: used_rdma,
                                source: "volume-grpc".to_string(),
                                message: Some(e.to_string()),
                            },
                        }
                    }
                    Err(e) if !req.data.is_empty() => {
                        warn!(
                            "RDMA write payload GET failed, falling back to TCP payload: {}",
                            e
                        );
                        match submit_needle(
                            volume_server_url,
                            volume_write_url,
                            local_reader.as_deref(),
                            &req,
                            req.data.clone(),
                        )
                        .await
                        {
                            Ok((file_id, source)) => RemoteNeedleWriteResponse {
                                success: true,
                                file_id,
                                transport: default_transport(),
                                real_rdma: false,
                                source,
                                message: Some(e.to_string()),
                            },
                            Err(upload_err) => RemoteNeedleWriteResponse {
                                success: false,
                                file_id: String::new(),
                                transport: default_transport(),
                                real_rdma: false,
                                source: "local-volume-http".to_string(),
                                message: Some(upload_err.to_string()),
                            },
                        }
                    }
                    Err(e) => RemoteNeedleWriteResponse {
                        success: false,
                        file_id: String::new(),
                        transport: default_transport(),
                        real_rdma: false,
                        source: String::new(),
                        message: Some(e.to_string()),
                    },
                };
                write_message(&mut stream, &resp).await?;
            }
        }
    }
}

fn is_clean_connection_close(err: &RdmaError) -> bool {
    match err {
        RdmaError::IpcError { reason } => {
            reason.contains("early eof")
                || reason.contains("unexpected end of file")
                || reason.contains("connection reset")
                || reason.contains("Connection reset")
        }
        _ => false,
    }
}

async fn put_read_payload_rdma(
    rdma_context: &Arc<RdmaContext>,
    registered_buffers: &Arc<RegisteredBufferPool>,
    req: &RemoteNeedleReadRequest,
    data: &[u8],
) -> RdmaResult<bool> {
    if !rdma_context.is_real_rdma()
        || !has_rdma_descriptor(
            &req.worker_address_b64,
            req.remote_addr,
            &req.remote_key_b64,
        )
    {
        return Ok(false);
    }
    if data.is_empty() {
        return Ok(false);
    }
    if req.size > 0 && data.len() > req.size as usize {
        return Err(RdmaError::invalid_request(
            "read payload larger than requested RDMA buffer",
        ));
    }

    let worker_address = STANDARD
        .decode(&req.worker_address_b64)
        .map_err(|e| RdmaError::invalid_request(format!("invalid worker address: {}", e)))?;
    let remote_rkey = STANDARD
        .decode(&req.remote_key_b64)
        .map_err(|e| RdmaError::invalid_request(format!("invalid remote rkey: {}", e)))?;
    let mut buffer = registered_buffers.acquire(rdma_context, data.len()).await?;
    buffer.data[..data.len()].copy_from_slice(data);
    let local_addr = buffer.region.addr;
    let wr_id = NEXT_NETWORK_WR_ID.fetch_add(1, Ordering::Relaxed);
    let peer_key = peer_cache_key("read", req.volume_id, req.needle_id, &worker_address);
    let result = rdma_context
        .post_write_peer(
            &peer_key,
            &worker_address,
            local_addr,
            req.remote_addr,
            &remote_rkey,
            data.len(),
            wr_id,
        )
        .await;
    registered_buffers.release(rdma_context, buffer).await;
    result?;
    Ok(true)
}

async fn resolve_write_payload(
    rdma_context: &Arc<RdmaContext>,
    registered_buffers: &Arc<RegisteredBufferPool>,
    req: &RemoteNeedleWriteRequest,
) -> RdmaResult<(Vec<u8>, bool)> {
    if !rdma_context.is_real_rdma()
        || !has_rdma_descriptor(
            &req.worker_address_b64,
            req.remote_addr,
            &req.remote_key_b64,
        )
    {
        return Ok((req.data.clone(), false));
    }

    let size = if req.size > 0 {
        req.size as usize
    } else {
        req.data.len()
    };
    if size == 0 {
        return Err(RdmaError::invalid_request("empty RDMA write payload"));
    }

    let worker_address = STANDARD
        .decode(&req.worker_address_b64)
        .map_err(|e| RdmaError::invalid_request(format!("invalid worker address: {}", e)))?;
    let remote_rkey = STANDARD
        .decode(&req.remote_key_b64)
        .map_err(|e| RdmaError::invalid_request(format!("invalid remote rkey: {}", e)))?;
    let buffer = registered_buffers.acquire(rdma_context, size).await?;
    let local_addr = buffer.region.addr;
    let wr_id = NEXT_NETWORK_WR_ID.fetch_add(1, Ordering::Relaxed);
    let peer_key = peer_cache_key("write", req.volume_id, req.needle_id, &worker_address);
    let result = rdma_context
        .post_read_peer(
            &peer_key,
            &worker_address,
            local_addr,
            req.remote_addr,
            &remote_rkey,
            size,
            wr_id,
        )
        .await;
    if let Err(err) = result {
        registered_buffers.release(rdma_context, buffer).await;
        return Err(err);
    }
    let data = buffer.data[..size].to_vec();
    registered_buffers.release(rdma_context, buffer).await;
    Ok((data, true))
}

fn has_rdma_descriptor(worker_address_b64: &str, remote_addr: u64, remote_key_b64: &str) -> bool {
    !worker_address_b64.is_empty() && remote_addr != 0 && !remote_key_b64.is_empty()
}

async fn fetch_needle_data(
    volume_server_url: &str,
    local_reader: Option<&LocalVolumeReader>,
    req: &RemoteNeedleReadRequest,
) -> RdmaResult<(Vec<u8>, String)> {
    if let Some(reader) = local_reader {
        match reader.read_needle(
            req.volume_id,
            req.needle_id,
            req.cookie,
            req.offset,
            req.size,
        ) {
            Ok(data) => return Ok((data, "local-volume-rust".to_string())),
            Err(e) => {
                warn!(
                    volume_id = req.volume_id,
                    needle_id = req.needle_id,
                    error = %e,
                    "local Rust volume read failed, falling back to HTTP"
                );
            }
        }
    }

    fetch_needle_http(volume_server_url, req)
        .await
        .map(|data| (data, "local-volume-http".to_string()))
}

async fn fetch_needle_http(
    volume_server_url: &str,
    req: &RemoteNeedleReadRequest,
) -> RdmaResult<Vec<u8>> {
    let file_id = format_seaweed_file_id(req.volume_id, req.needle_id, req.cookie);
    let url = format!("{}/{}", volume_server_url.trim_end_matches('/'), file_id);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_e| RdmaError::operation_failed("http_client", -1))?;

    let mut http_req = client.get(&url);
    if req.size > 0 {
        let end = req.offset.saturating_add(req.size).saturating_sub(1);
        http_req = http_req.header(RANGE, format!("bytes={}-{}", req.offset, end));
    }

    let response = http_req
        .send()
        .await
        .map_err(|_e| RdmaError::operation_failed("http_get", -1))?;

    if !response.status().is_success() {
        return Err(RdmaError::operation_failed(
            "http_status",
            response.status().as_u16() as i32,
        ));
    }

    let range_applied = response.status() == reqwest::StatusCode::PARTIAL_CONTENT
        || response.headers().contains_key(CONTENT_RANGE);

    response
        .bytes()
        .await
        .map(|b| normalize_http_read_data(b.to_vec(), req, range_applied))
        .map_err(|_| RdmaError::operation_failed("http_body", -1))
}

fn normalize_http_read_data(
    mut data: Vec<u8>,
    req: &RemoteNeedleReadRequest,
    range_applied: bool,
) -> Vec<u8> {
    if req.size == 0 {
        return data;
    }

    let requested = req.size as usize;
    if data.len() <= requested {
        return data;
    }

    if range_applied {
        data.truncate(requested);
        return data;
    }

    let start = req.offset as usize;
    if start >= data.len() {
        data.clear();
        return data;
    }
    let end = start.saturating_add(requested).min(data.len());
    data[start..end].to_vec()
}

async fn submit_needle(
    volume_server_url: &str,
    volume_write_url: &str,
    local_reader: Option<&LocalVolumeReader>,
    req: &RemoteNeedleWriteRequest,
    data: Vec<u8>,
) -> RdmaResult<(String, String)> {
    match volume_grpc::write_needle_blob(volume_write_url, local_reader, req, data.clone()).await {
        Ok(file_id) => {
            if let Some(reader) = local_reader {
                reader.invalidate(req.volume_id);
            }
            Ok((file_id, "volume-grpc".to_string()))
        }
        Err(err) => {
            warn!(
                volume_id = req.volume_id,
                needle_id = req.needle_id,
                error = %err,
                "direct volume gRPC write failed, falling back to local-volume HTTP"
            );
            submit_needle_http(volume_server_url, req, data)
                .await
                .map(|file_id| (file_id, "local-volume-http".to_string()))
        }
    }
}

async fn submit_needle_http(
    volume_server_url: &str,
    req: &RemoteNeedleWriteRequest,
    data: Vec<u8>,
) -> RdmaResult<String> {
    let file_id = format_seaweed_file_id(req.volume_id, req.needle_id, req.cookie);
    let url = format!("{}/{}", volume_server_url.trim_end_matches('/'), file_id);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_e| RdmaError::operation_failed("http_client", -1))?;

    let response = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .body(data)
        .send()
        .await
        .map_err(|_e| RdmaError::operation_failed("http_upload", -1))?;

    if !response.status().is_success() {
        return Err(RdmaError::operation_failed(
            "http_upload_status",
            response.status().as_u16() as i32,
        ));
    }

    Ok(file_id)
}

async fn read_raw_message(stream: &mut TcpStream) -> RdmaResult<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    stream
        .read_exact(&mut len_bytes)
        .await
        .map_err(|e| RdmaError::ipc_error(format!("read len: {}", e)))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > MAX_IPC_MESSAGE_SIZE {
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
    if len > MAX_IPC_MESSAGE_SIZE {
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
        Err(RdmaError::operation_failed("remote_read", -1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_req(offset: u64, size: u64) -> RemoteNeedleReadRequest {
        RemoteNeedleReadRequest {
            volume_id: 1,
            needle_id: 2,
            cookie: 3,
            offset,
            size,
            worker_address_b64: String::new(),
            remote_addr: 0,
            remote_key_b64: String::new(),
        }
    }

    #[test]
    fn normalize_keeps_range_response_to_requested_size() {
        let data = b"abcdef".to_vec();
        let out = normalize_http_read_data(data, &read_req(2, 3), true);
        assert_eq!(out, b"abc");
    }

    #[test]
    fn normalize_slices_full_needle_when_range_was_not_applied() {
        let data = b"0123456789".to_vec();
        let out = normalize_http_read_data(data, &read_req(3, 4), false);
        assert_eq!(out, b"3456");
    }

    #[test]
    fn normalize_keeps_full_read_when_size_is_zero() {
        let data = b"0123456789".to_vec();
        let out = normalize_http_read_data(data.clone(), &read_req(3, 0), false);
        assert_eq!(out, data);
    }

    #[test]
    fn peer_cache_key_is_stable_for_same_worker() {
        let key1 = peer_cache_key("read", 7, 42, b"worker-a");
        let key2 = peer_cache_key("read", 7, 42, b"worker-a");
        assert_eq!(key1, key2);
    }

    #[test]
    fn peer_cache_key_changes_with_worker() {
        let key1 = peer_cache_key("read", 7, 42, b"worker-a");
        let key2 = peer_cache_key("read", 7, 42, b"worker-b");
        assert_ne!(key1, key2);
    }

    #[test]
    fn peer_cache_key_separates_read_and_write() {
        let read_key = peer_cache_key("read", 7, 42, b"worker-a");
        let write_key = peer_cache_key("write", 7, 42, b"worker-a");
        assert_ne!(read_key, write_key);
    }
}
