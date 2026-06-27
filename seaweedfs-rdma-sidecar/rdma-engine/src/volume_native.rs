use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

pub const ABI_VERSION: u32 = 1;
pub const LINK_INFINIBAND: u32 = 1;
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

const DEFAULT_MOCK_BASE_ADDR: u64 = 0x7f00_0000_0000;
const DEFAULT_MOCK_ADDR_STRIDE: u64 = 0x0100_0000;
const DEFAULT_MOCK_RKEY: u32 = 0x5eed_0001;

#[derive(Debug, Clone)]
pub struct VolumeEngineConfig {
    pub socket_path: String,
    pub device: String,
    pub port: u32,
    pub qpn: u32,
    pub psn: u32,
    pub lid: u32,
    pub gid_index: u32,
    pub gid: String,
    pub link_layer: u32,
    pub mock_base_addr: u64,
    pub mock_addr_stride: u64,
    pub mock_rkey: u32,
}

impl VolumeEngineConfig {
    pub fn mock(socket_path: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            device: "mock-volume-rdma".to_string(),
            port: 1,
            qpn: 0x1001,
            psn: 0xabcdef,
            lid: 1,
            gid_index: 0,
            gid: "00000000000000000000000000000000".to_string(),
            link_layer: LINK_INFINIBAND,
            mock_base_addr: DEFAULT_MOCK_BASE_ADDR,
            mock_addr_stride: DEFAULT_MOCK_ADDR_STRIDE,
            mock_rkey: DEFAULT_MOCK_RKEY,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeRdmaEndpointInfo {
    pub abi_version: u32,
    pub flags: u32,
    pub device: String,
    pub port: u32,
    pub qp_num: u32,
    pub psn: u32,
    pub qp_state: u32,
    pub lid: u32,
    pub sm_lid: u32,
    pub port_state: u32,
    pub active_mtu: u32,
    pub gid_index: u32,
    pub link_layer: u32,
    pub gid: String,
    pub kernel_enabled: bool,
    pub endpoint_ready: bool,
    pub qp_connected: bool,
    pub unsafe_global_rkey: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeRdmaRemoteInfo {
    pub abi_version: u32,
    pub flags: u32,
    pub qpn: u32,
    pub lid: u32,
    pub psn: u32,
    pub port: u32,
    pub gid_index: u32,
    pub sl: u32,
    pub gid: [u8; 16],
    pub reserved: [u64; 8],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeRdmaDataDesc {
    #[serde(rename = "RemoteAddr")]
    pub remote_addr: u64,
    #[serde(rename = "RKey")]
    pub rkey: u32,
    #[serde(rename = "Length")]
    pub length: u32,
    #[serde(rename = "Reserved")]
    pub reserved: [u64; 4],
}

#[derive(Debug, Deserialize)]
pub struct EngineRequest {
    pub op: String,
    #[serde(default)]
    pub remote: Option<VolumeRdmaRemoteInfo>,
    #[serde(default)]
    pub session_id: u64,
    #[serde(default, deserialize_with = "deserialize_go_json_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize)]
pub struct EngineResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<VolumeRdmaEndpointInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<VolumeRdmaDataDesc>,
    #[serde(skip_serializing_if = "is_zero")]
    pub session_id: u64,
}

#[derive(Debug)]
struct ReadLease {
    data: Vec<u8>,
    desc: VolumeRdmaDataDesc,
}

#[derive(Debug)]
struct EngineState {
    connected_remote: Option<VolumeRdmaRemoteInfo>,
    leases: HashMap<u64, ReadLease>,
}

#[derive(Debug)]
pub struct VolumeNativeEngine {
    config: VolumeEngineConfig,
    next_session_id: AtomicU64,
    state: Mutex<EngineState>,
}

impl VolumeNativeEngine {
    pub fn new(config: VolumeEngineConfig) -> Self {
        Self {
            config,
            next_session_id: AtomicU64::new(1),
            state: Mutex::new(EngineState {
                connected_remote: None,
                leases: HashMap::new(),
            }),
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let socket_path = self.config.socket_path.clone();
        if Path::new(&socket_path).exists() {
            std::fs::remove_file(&socket_path)
                .with_context(|| format!("remove existing socket {}", socket_path))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind volume RDMA engine socket {}", socket_path))?;
        #[cfg(unix)]
        {
            std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o777))
                .with_context(|| format!("chmod volume RDMA engine socket {}", socket_path))?;
        }

        info!("volume RDMA engine listening on {}", socket_path);
        loop {
            let (stream, _) = listener.accept().await?;
            let engine = self.clone();
            tokio::spawn(async move {
                if let Err(err) = engine.handle_stream(stream).await {
                    warn!("volume RDMA engine connection failed: {err:#}");
                }
            });
        }
    }

    async fn handle_stream(self: Arc<Self>, mut stream: UnixStream) -> Result<()> {
        let frame = read_frame(&mut stream).await?;
        let request: EngineRequest =
            serde_json::from_slice(&frame).context("decode volume RDMA engine request")?;
        debug!("volume RDMA engine request: {:?}", request);
        let response = self.process_request(request).await;
        let payload =
            serde_json::to_vec(&response).context("encode volume RDMA engine response")?;
        write_frame(&mut stream, &payload).await?;
        Ok(())
    }

    pub async fn process_request(&self, request: EngineRequest) -> EngineResponse {
        match request.op.as_str() {
            "local" => EngineResponse::ok_endpoint(self.local_endpoint().await),
            "connect" => match request.remote {
                Some(remote) => {
                    self.state.lock().await.connected_remote = Some(remote);
                    EngineResponse::ok()
                }
                None => EngineResponse::error("connect requires remote"),
            },
            "register_read" => {
                if request.data.is_empty() {
                    return EngineResponse::error("register_read requires data");
                }
                self.register_read(request.data).await
            }
            "release" => {
                if request.session_id == 0 {
                    return EngineResponse::error("release requires session_id");
                }
                let mut state = self.state.lock().await;
                state.leases.remove(&request.session_id);
                EngineResponse::ok()
            }
            other => EngineResponse::error(format!("unknown op {other}")),
        }
    }

    async fn local_endpoint(&self) -> VolumeRdmaEndpointInfo {
        VolumeRdmaEndpointInfo {
            abi_version: ABI_VERSION,
            flags: 0,
            device: self.config.device.clone(),
            port: self.config.port,
            qp_num: self.config.qpn,
            psn: self.config.psn,
            qp_state: 0,
            lid: self.config.lid,
            sm_lid: 0,
            port_state: 0,
            active_mtu: 0,
            gid_index: self.config.gid_index,
            link_layer: self.config.link_layer,
            gid: self.config.gid.clone(),
            kernel_enabled: true,
            endpoint_ready: true,
            qp_connected: self.state.lock().await.connected_remote.is_some(),
            unsafe_global_rkey: false,
        }
    }

    async fn register_read(&self, data: Vec<u8>) -> EngineResponse {
        if data.len() > u32::MAX as usize {
            return EngineResponse::error("register_read data too large");
        }
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        if session_id == 0 {
            return EngineResponse::error("session id overflow");
        }
        let remote_addr = self
            .config
            .mock_base_addr
            .saturating_add(session_id.saturating_mul(self.config.mock_addr_stride));
        let desc = VolumeRdmaDataDesc {
            remote_addr,
            rkey: self.config.mock_rkey,
            length: data.len() as u32,
            reserved: [0; 4],
        };
        self.state.lock().await.leases.insert(
            session_id,
            ReadLease {
                data,
                desc: desc.clone(),
            },
        );
        EngineResponse {
            ok: true,
            error: None,
            endpoint: None,
            desc: Some(desc),
            session_id,
        }
    }

    pub async fn lease_count(&self) -> usize {
        self.state.lock().await.leases.len()
    }

    pub async fn lease_data(&self, session_id: u64) -> Option<Vec<u8>> {
        self.state
            .lock()
            .await
            .leases
            .get(&session_id)
            .map(|lease| lease.data.clone())
    }

    pub async fn lease_desc(&self, session_id: u64) -> Option<VolumeRdmaDataDesc> {
        self.state
            .lock()
            .await
            .leases
            .get(&session_id)
            .map(|lease| lease.desc.clone())
    }
}

impl EngineResponse {
    fn ok() -> Self {
        Self {
            ok: true,
            error: None,
            endpoint: None,
            desc: None,
            session_id: 0,
        }
    }

    fn ok_endpoint(endpoint: VolumeRdmaEndpointInfo) -> Self {
        Self {
            ok: true,
            error: None,
            endpoint: Some(endpoint),
            desc: None,
            session_id: 0,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(message.into()),
            endpoint: None,
            desc: None,
            session_id: 0,
        }
    }
}

pub async fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .context("read frame header")?;
    let size = u32::from_le_bytes(header) as usize;
    if size > MAX_FRAME_SIZE {
        return Err(anyhow!("frame too large: {size}"));
    }
    let mut payload = vec![0u8; size];
    stream
        .read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(payload)
}

pub async fn write_frame(stream: &mut UnixStream, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_FRAME_SIZE {
        return Err(anyhow!("frame too large: {}", payload.len()));
    }
    stream
        .write_all(&(payload.len() as u32).to_le_bytes())
        .await
        .context("write frame header")?;
    stream
        .write_all(payload)
        .await
        .context("write frame payload")?;
    stream.flush().await.context("flush frame")?;
    Ok(())
}

fn deserialize_go_json_bytes<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BytesVisitor;

    impl<'de> serde::de::Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a Go JSON []byte base64 string or byte array")
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            STANDARD.decode(value).map_err(E::custom)
        }

        fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_str(&value)
        }

        fn visit_bytes<E>(self, value: &[u8]) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_vec())
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(byte) = seq.next_element::<u8>()? {
                out.push(byte);
            }
            Ok(out)
        }
    }

    deserializer.deserialize_any(BytesVisitor)
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn process_local_and_connect() {
        let engine = VolumeNativeEngine::new(VolumeEngineConfig::mock("/tmp/test.sock"));
        let local = engine
            .process_request(EngineRequest {
                op: "local".to_string(),
                remote: None,
                session_id: 0,
                data: Vec::new(),
            })
            .await;
        assert!(local.ok);
        assert!(local.endpoint.unwrap().endpoint_ready);

        let connect = engine
            .process_request(EngineRequest {
                op: "connect".to_string(),
                remote: Some(VolumeRdmaRemoteInfo {
                    abi_version: ABI_VERSION,
                    flags: 0,
                    qpn: 10,
                    lid: 1,
                    psn: 2,
                    port: 1,
                    gid_index: 0,
                    sl: 0,
                    gid: [0; 16],
                    reserved: [0; 8],
                }),
                session_id: 0,
                data: Vec::new(),
            })
            .await;
        assert!(connect.ok);
        assert!(engine.local_endpoint().await.qp_connected);
    }

    #[tokio::test]
    async fn register_read_and_release() {
        let engine = VolumeNativeEngine::new(VolumeEngineConfig::mock("/tmp/test.sock"));
        let response = engine
            .process_request(EngineRequest {
                op: "register_read".to_string(),
                remote: None,
                session_id: 0,
                data: b"needle-data".to_vec(),
            })
            .await;
        assert!(response.ok);
        let session_id = response.session_id;
        assert_ne!(session_id, 0);
        let desc = response.desc.unwrap();
        assert_ne!(desc.remote_addr, 0);
        assert_ne!(desc.rkey, 0);
        assert_eq!(desc.length, 11);
        assert_eq!(engine.lease_count().await, 1);
        assert_eq!(engine.lease_data(session_id).await.unwrap(), b"needle-data");
        assert_eq!(engine.lease_desc(session_id).await.unwrap(), desc);

        let release = engine
            .process_request(EngineRequest {
                op: "release".to_string(),
                remote: None,
                session_id,
                data: Vec::new(),
            })
            .await;
        assert!(release.ok);
        assert_eq!(engine.lease_count().await, 0);
    }

    #[test]
    fn deserializes_go_base64_byte_field() {
        let request: EngineRequest =
            serde_json::from_str(r#"{"op":"register_read","data":"bmVlZGxl"}"#).unwrap();
        assert_eq!(request.data, b"needle");
    }

    #[test]
    fn deserializes_byte_array_field() {
        let request: EngineRequest =
            serde_json::from_str(r#"{"op":"register_read","data":[1,2,3]}"#).unwrap();
        assert_eq!(request.data, vec![1, 2, 3]);
    }

    #[test]
    fn serializes_go_desc_field_names() {
        let desc = VolumeRdmaDataDesc {
            remote_addr: 1,
            rkey: 2,
            length: 3,
            reserved: [4, 0, 0, 0],
        };
        let encoded = serde_json::to_string(&desc).unwrap();
        assert!(encoded.contains("RemoteAddr"));
        assert!(encoded.contains("RKey"));
        assert!(encoded.contains("Length"));
        assert!(encoded.contains("Reserved"));
    }
}
