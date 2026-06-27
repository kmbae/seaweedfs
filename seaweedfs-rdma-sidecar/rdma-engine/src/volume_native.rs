use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
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
pub const LINK_ETHERNET: u32 = 2;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeRdmaRegisteredRead {
    pub session_id: u64,
    pub desc: VolumeRdmaDataDesc,
}

#[async_trait]
pub trait VolumeRdmaProvider: std::fmt::Debug + Send + Sync {
    async fn local_endpoint(&self, connection_id: u64) -> Result<(u64, VolumeRdmaEndpointInfo)>;
    async fn connect_endpoint(
        &self,
        connection_id: u64,
        remote: VolumeRdmaRemoteInfo,
    ) -> Result<()>;
    async fn register_read(
        &self,
        connection_id: u64,
        data: Vec<u8>,
    ) -> Result<VolumeRdmaRegisteredRead>;
    async fn release_read(&self, session_id: u64) -> Result<()>;
}

#[async_trait]
pub trait VolumeRdmaRequester: std::fmt::Debug + Send + Sync {
    async fn local_endpoint(&self, connection_id: u64) -> Result<(u64, VolumeRdmaEndpointInfo)>;
    async fn connect_endpoint(
        &self,
        connection_id: u64,
        remote: VolumeRdmaRemoteInfo,
    ) -> Result<()>;
    async fn read_remote(
        &self,
        connection_id: u64,
        desc: VolumeRdmaDataDesc,
        timeout_ms: u64,
    ) -> Result<Vec<u8>>;
}

#[derive(Debug, Deserialize)]
pub struct EngineRequest {
    pub op: String,
    #[serde(default)]
    pub connection_id: u64,
    #[serde(default)]
    pub remote: Option<VolumeRdmaRemoteInfo>,
    #[serde(default)]
    pub desc: Option<VolumeRdmaDataDesc>,
    #[serde(default)]
    pub session_id: u64,
    #[serde(default)]
    pub timeout_ms: u64,
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
    pub connection_id: u64,
    #[serde(skip_serializing_if = "is_zero")]
    pub session_id: u64,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_go_json_bytes"
    )]
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct VolumeNativeEngine {
    socket_path: String,
    provider: Arc<dyn VolumeRdmaProvider>,
    requester: Option<Arc<dyn VolumeRdmaRequester>>,
}

impl VolumeNativeEngine {
    pub fn new(config: VolumeEngineConfig) -> Self {
        let socket_path = config.socket_path.clone();
        let provider = Arc::new(MockVolumeRdmaProvider::new(config));
        let requester = Arc::new(MockVolumeRdmaRequester::new(VolumeEngineConfig::mock(
            socket_path.clone(),
        )));
        Self::with_provider_and_requester(socket_path, provider, Some(requester))
    }

    pub fn with_provider(
        socket_path: impl Into<String>,
        provider: Arc<dyn VolumeRdmaProvider>,
    ) -> Self {
        Self::with_provider_and_requester(socket_path, provider, None)
    }

    pub fn with_provider_and_requester(
        socket_path: impl Into<String>,
        provider: Arc<dyn VolumeRdmaProvider>,
        requester: Option<Arc<dyn VolumeRdmaRequester>>,
    ) -> Self {
        Self {
            socket_path: socket_path.into(),
            provider,
            requester,
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let socket_path = self.socket_path.clone();
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
            "local" => match self.provider.local_endpoint(request.connection_id).await {
                Ok((connection_id, endpoint)) => {
                    EngineResponse::ok_endpoint(connection_id, endpoint)
                }
                Err(err) => EngineResponse::error(format!("local endpoint failed: {err:#}")),
            },
            "connect" => match request.remote {
                Some(remote) => match self
                    .provider
                    .connect_endpoint(request.connection_id, remote)
                    .await
                {
                    Ok(()) => EngineResponse::ok(),
                    Err(err) => EngineResponse::error(format!("connect failed: {err:#}")),
                },
                None => EngineResponse::error("connect requires remote"),
            },
            "register_read" => {
                if request.data.is_empty() {
                    return EngineResponse::error("register_read requires data");
                }
                match self
                    .provider
                    .register_read(request.connection_id, request.data)
                    .await
                {
                    Ok(read) => EngineResponse::ok_registered_read(read),
                    Err(err) => EngineResponse::error(format!("register_read failed: {err:#}")),
                }
            }
            "release" => {
                if request.session_id == 0 {
                    return EngineResponse::error("release requires session_id");
                }
                match self.provider.release_read(request.session_id).await {
                    Ok(()) => EngineResponse::ok(),
                    Err(err) => EngineResponse::error(format!("release failed: {err:#}")),
                }
            }
            "requester_local" => match self.requester.as_ref() {
                Some(requester) => match requester.local_endpoint(request.connection_id).await {
                    Ok((connection_id, endpoint)) => {
                        EngineResponse::ok_endpoint(connection_id, endpoint)
                    }
                    Err(err) => {
                        EngineResponse::error(format!("requester local endpoint failed: {err:#}"))
                    }
                },
                None => EngineResponse::error("requester is not configured"),
            },
            "requester_connect" => match (self.requester.as_ref(), request.remote) {
                (Some(requester), Some(remote)) => {
                    match requester
                        .connect_endpoint(request.connection_id, remote)
                        .await
                    {
                        Ok(()) => EngineResponse::ok(),
                        Err(err) => {
                            EngineResponse::error(format!("requester connect failed: {err:#}"))
                        }
                    }
                }
                (None, _) => EngineResponse::error("requester is not configured"),
                (_, None) => EngineResponse::error("requester_connect requires remote"),
            },
            "read_remote" => match (self.requester.as_ref(), request.desc) {
                (Some(requester), Some(desc)) => {
                    match requester
                        .read_remote(request.connection_id, desc, request.timeout_ms)
                        .await
                    {
                        Ok(data) => EngineResponse::ok_data(data),
                        Err(err) => EngineResponse::error(format!("read_remote failed: {err:#}")),
                    }
                }
                (None, _) => EngineResponse::error("requester is not configured"),
                (_, None) => EngineResponse::error("read_remote requires desc"),
            },
            other => EngineResponse::error(format!("unknown op {other}")),
        }
    }
}

impl EngineResponse {
    fn ok() -> Self {
        Self {
            ok: true,
            error: None,
            endpoint: None,
            desc: None,
            connection_id: 0,
            session_id: 0,
            data: Vec::new(),
        }
    }

    fn ok_endpoint(connection_id: u64, endpoint: VolumeRdmaEndpointInfo) -> Self {
        Self {
            ok: true,
            error: None,
            endpoint: Some(endpoint),
            desc: None,
            connection_id,
            session_id: 0,
            data: Vec::new(),
        }
    }

    fn ok_registered_read(read: VolumeRdmaRegisteredRead) -> Self {
        Self {
            ok: true,
            error: None,
            endpoint: None,
            desc: Some(read.desc),
            connection_id: 0,
            session_id: read.session_id,
            data: Vec::new(),
        }
    }

    fn ok_data(data: Vec<u8>) -> Self {
        Self {
            ok: true,
            error: None,
            endpoint: None,
            desc: None,
            connection_id: 0,
            session_id: 0,
            data,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(message.into()),
            endpoint: None,
            desc: None,
            connection_id: 0,
            session_id: 0,
            data: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct ReadLease {
    data: Vec<u8>,
    desc: VolumeRdmaDataDesc,
}

#[derive(Debug)]
struct MockProviderState {
    connections: HashMap<u64, Option<VolumeRdmaRemoteInfo>>,
    leases: HashMap<u64, ReadLease>,
}

#[derive(Debug)]
pub struct MockVolumeRdmaProvider {
    config: VolumeEngineConfig,
    next_connection_id: AtomicU64,
    next_session_id: AtomicU64,
    state: Mutex<MockProviderState>,
}

#[derive(Debug)]
pub struct MockVolumeRdmaRequester {
    config: VolumeEngineConfig,
    next_connection_id: AtomicU64,
    connections: Mutex<HashMap<u64, Option<VolumeRdmaRemoteInfo>>>,
}

impl MockVolumeRdmaRequester {
    pub fn new(config: VolumeEngineConfig) -> Self {
        Self {
            config,
            next_connection_id: AtomicU64::new(1),
            connections: Mutex::new(HashMap::new()),
        }
    }
}

impl MockVolumeRdmaProvider {
    pub fn new(config: VolumeEngineConfig) -> Self {
        Self {
            config,
            next_connection_id: AtomicU64::new(1),
            next_session_id: AtomicU64::new(1),
            state: Mutex::new(MockProviderState {
                connections: HashMap::new(),
                leases: HashMap::new(),
            }),
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

#[async_trait]
impl VolumeRdmaProvider for MockVolumeRdmaProvider {
    async fn local_endpoint(&self, connection_id: u64) -> Result<(u64, VolumeRdmaEndpointInfo)> {
        let connection_id = mock_connection_id(&self.next_connection_id, connection_id)?;
        let mut state = self.state.lock().await;
        let connected = state
            .connections
            .entry(connection_id)
            .or_insert(None)
            .is_some();
        Ok((
            connection_id,
            VolumeRdmaEndpointInfo {
                abi_version: ABI_VERSION,
                flags: 0,
                device: self.config.device.clone(),
                port: self.config.port,
                qp_num: self.config.qpn.saturating_add(connection_id as u32),
                psn: self.config.psn.wrapping_add(connection_id as u32) & 0x00ff_ffff,
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
                qp_connected: connected,
                unsafe_global_rkey: false,
            },
        ))
    }

    async fn connect_endpoint(
        &self,
        connection_id: u64,
        remote: VolumeRdmaRemoteInfo,
    ) -> Result<()> {
        if connection_id == 0 {
            return Err(anyhow!("connect requires connection_id"));
        }
        self.state
            .lock()
            .await
            .connections
            .insert(connection_id, Some(remote));
        Ok(())
    }

    async fn register_read(
        &self,
        connection_id: u64,
        data: Vec<u8>,
    ) -> Result<VolumeRdmaRegisteredRead> {
        if data.is_empty() {
            return Err(anyhow!("register_read requires data"));
        }
        if data.len() > u32::MAX as usize {
            return Err(anyhow!("register_read data too large"));
        }

        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        if session_id == 0 {
            return Err(anyhow!("session id overflow"));
        }
        if connection_id == 0 {
            return Err(anyhow!("register_read requires connection_id"));
        }
        let mut state = self.state.lock().await;
        if !matches!(state.connections.get(&connection_id), Some(Some(_))) {
            return Err(anyhow!(
                "register_read requires connected connection_id {connection_id}"
            ));
        }

        let remote_addr = self
            .config
            .mock_base_addr
            .saturating_add(session_id.saturating_mul(self.config.mock_addr_stride));
        let mut desc = VolumeRdmaDataDesc {
            remote_addr,
            rkey: self.config.mock_rkey,
            length: data.len() as u32,
            reserved: [0; 4],
        };
        desc.reserved[1] = connection_id;

        state.leases.insert(
            session_id,
            ReadLease {
                data,
                desc: desc.clone(),
            },
        );

        Ok(VolumeRdmaRegisteredRead { session_id, desc })
    }

    async fn release_read(&self, session_id: u64) -> Result<()> {
        self.state.lock().await.leases.remove(&session_id);
        Ok(())
    }
}

#[async_trait]
impl VolumeRdmaRequester for MockVolumeRdmaRequester {
    async fn local_endpoint(&self, connection_id: u64) -> Result<(u64, VolumeRdmaEndpointInfo)> {
        let connection_id = mock_connection_id(&self.next_connection_id, connection_id)?;
        let mut connections = self.connections.lock().await;
        let connected = connections.entry(connection_id).or_insert(None).is_some();
        Ok((
            connection_id,
            VolumeRdmaEndpointInfo {
                abi_version: ABI_VERSION,
                flags: 0,
                device: self.config.device.clone(),
                port: self.config.port,
                qp_num: self.config.qpn.saturating_add(connection_id as u32),
                psn: self
                    .config
                    .psn
                    .saturating_add(1)
                    .wrapping_add(connection_id as u32)
                    & 0x00ff_ffff,
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
                qp_connected: connected,
                unsafe_global_rkey: false,
            },
        ))
    }

    async fn connect_endpoint(
        &self,
        connection_id: u64,
        remote: VolumeRdmaRemoteInfo,
    ) -> Result<()> {
        if connection_id == 0 {
            return Err(anyhow!("requester_connect requires connection_id"));
        }
        self.connections
            .lock()
            .await
            .insert(connection_id, Some(remote));
        Ok(())
    }

    async fn read_remote(
        &self,
        connection_id: u64,
        desc: VolumeRdmaDataDesc,
        _timeout_ms: u64,
    ) -> Result<Vec<u8>> {
        if connection_id == 0 {
            return Err(anyhow!("read_remote requires connection_id"));
        }
        if !matches!(
            self.connections.lock().await.get(&connection_id),
            Some(Some(_))
        ) {
            return Err(anyhow!(
                "read_remote requires connected connection_id {connection_id}"
            ));
        }
        if desc.remote_addr == 0 || desc.length == 0 {
            return Err(anyhow!("read_remote requires exportable descriptor"));
        }
        let mut out = vec![0u8; desc.length as usize];
        for (idx, byte) in out.iter_mut().enumerate() {
            *byte = (idx % 256) as u8;
        }
        Ok(out)
    }
}

fn mock_connection_id(next: &AtomicU64, requested: u64) -> Result<u64> {
    if requested != 0 {
        return Ok(requested);
    }
    let id = next.fetch_add(1, Ordering::Relaxed);
    if id == 0 {
        return Err(anyhow!("connection id overflow"));
    }
    Ok(id)
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

fn serialize_go_json_bytes<S>(value: &[u8], serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&STANDARD.encode(value))
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_engine() -> (
        VolumeNativeEngine,
        Arc<MockVolumeRdmaProvider>,
        Arc<MockVolumeRdmaRequester>,
    ) {
        let config = VolumeEngineConfig::mock("/tmp/test.sock");
        let provider = Arc::new(MockVolumeRdmaProvider::new(config.clone()));
        let requester = Arc::new(MockVolumeRdmaRequester::new(config));
        let engine = VolumeNativeEngine::with_provider_and_requester(
            "/tmp/test.sock",
            provider.clone(),
            Some(requester.clone()),
        );
        (engine, provider, requester)
    }

    #[tokio::test]
    async fn process_local_and_connect() {
        let (engine, provider, _) = mock_engine();
        let local = engine
            .process_request(EngineRequest {
                op: "local".to_string(),
                connection_id: 0,
                remote: None,
                desc: None,
                session_id: 0,
                timeout_ms: 0,
                data: Vec::new(),
            })
            .await;
        assert!(local.ok);
        let connection_id = local.connection_id;
        assert_ne!(connection_id, 0);
        assert!(local.endpoint.unwrap().endpoint_ready);

        let connect = engine
            .process_request(EngineRequest {
                op: "connect".to_string(),
                connection_id,
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
                desc: None,
                session_id: 0,
                timeout_ms: 0,
                data: Vec::new(),
            })
            .await;
        assert!(connect.ok);
        assert!(
            provider
                .local_endpoint(connection_id)
                .await
                .unwrap()
                .1
                .qp_connected
        );
    }

    #[tokio::test]
    async fn register_read_and_release() {
        let (engine, provider, _) = mock_engine();
        let local = engine
            .process_request(EngineRequest {
                op: "local".to_string(),
                connection_id: 0,
                remote: None,
                desc: None,
                session_id: 0,
                timeout_ms: 0,
                data: Vec::new(),
            })
            .await;
        assert!(local.ok);
        let connection_id = local.connection_id;
        let connect = engine
            .process_request(EngineRequest {
                op: "connect".to_string(),
                connection_id,
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
                desc: None,
                session_id: 0,
                timeout_ms: 0,
                data: Vec::new(),
            })
            .await;
        assert!(connect.ok);
        let response = engine
            .process_request(EngineRequest {
                op: "register_read".to_string(),
                connection_id,
                remote: None,
                desc: None,
                session_id: 0,
                timeout_ms: 0,
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
        assert_eq!(provider.lease_count().await, 1);
        assert_eq!(
            provider.lease_data(session_id).await.unwrap(),
            b"needle-data"
        );
        assert_eq!(provider.lease_desc(session_id).await.unwrap(), desc);

        let release = engine
            .process_request(EngineRequest {
                op: "release".to_string(),
                connection_id: 0,
                remote: None,
                desc: None,
                session_id,
                timeout_ms: 0,
                data: Vec::new(),
            })
            .await;
        assert!(release.ok);
        assert_eq!(provider.lease_count().await, 0);
    }

    #[tokio::test]
    async fn engine_new_uses_mock_provider() {
        let engine = VolumeNativeEngine::new(VolumeEngineConfig::mock("/tmp/test.sock"));
        let local = engine
            .process_request(EngineRequest {
                op: "local".to_string(),
                connection_id: 0,
                remote: None,
                desc: None,
                session_id: 0,
                timeout_ms: 0,
                data: Vec::new(),
            })
            .await;
        assert!(local.ok);
        assert_eq!(local.endpoint.unwrap().device, "mock-volume-rdma");
    }

    #[tokio::test]
    async fn requester_local_connect_and_read_remote() {
        let (engine, _, requester) = mock_engine();
        let local = engine
            .process_request(EngineRequest {
                op: "requester_local".to_string(),
                connection_id: 0,
                remote: None,
                desc: None,
                session_id: 0,
                timeout_ms: 0,
                data: Vec::new(),
            })
            .await;
        assert!(local.ok);
        let connection_id = local.connection_id;
        assert_ne!(connection_id, 0);
        assert_eq!(local.endpoint.unwrap().qp_num, 0x1002);

        let connect = engine
            .process_request(EngineRequest {
                op: "requester_connect".to_string(),
                connection_id,
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
                desc: None,
                session_id: 0,
                timeout_ms: 0,
                data: Vec::new(),
            })
            .await;
        assert!(connect.ok);
        assert!(
            requester
                .local_endpoint(connection_id)
                .await
                .unwrap()
                .1
                .qp_connected
        );

        let read = engine
            .process_request(EngineRequest {
                op: "read_remote".to_string(),
                connection_id,
                remote: None,
                desc: Some(VolumeRdmaDataDesc {
                    remote_addr: 0xbeef,
                    rkey: 0,
                    length: 4,
                    reserved: [0; 4],
                }),
                session_id: 0,
                timeout_ms: 10,
                data: Vec::new(),
            })
            .await;
        assert!(read.ok);
        assert_eq!(read.data, vec![0, 1, 2, 3]);
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
