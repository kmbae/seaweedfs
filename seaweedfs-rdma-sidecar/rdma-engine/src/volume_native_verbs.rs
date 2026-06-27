use crate::volume_native::{
    VolumeRdmaDataDesc, VolumeRdmaEndpointInfo, VolumeRdmaProvider, VolumeRdmaRegisteredRead,
    VolumeRdmaRemoteInfo, ABI_VERSION, LINK_ETHERNET, LINK_INFINIBAND,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use libc::{c_char, c_int, c_void, size_t};
use libloading::Library;
use std::collections::HashMap;
use std::ffi::CStr;
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

const IBV_QPT_RC: c_int = 2;
const IBV_PORT_ACTIVE: c_int = 4;
const IBV_LINK_LAYER_INFINIBAND: u8 = 1;
const IBV_LINK_LAYER_ETHERNET: u8 = 2;
const IBV_QPS_INIT: c_int = 1;
const IBV_QPS_RTR: c_int = 2;
const IBV_QPS_RTS: c_int = 3;
const IBV_ACCESS_LOCAL_WRITE: c_int = 1;
const IBV_ACCESS_REMOTE_WRITE: c_int = 1 << 1;
const IBV_ACCESS_REMOTE_READ: c_int = 1 << 2;
const IBV_QP_STATE: c_int = 1 << 0;
const IBV_QP_ACCESS_FLAGS: c_int = 1 << 3;
const IBV_QP_PKEY_INDEX: c_int = 1 << 4;
const IBV_QP_PORT: c_int = 1 << 5;
const IBV_QP_AV: c_int = 1 << 7;
const IBV_QP_PATH_MTU: c_int = 1 << 8;
const IBV_QP_TIMEOUT: c_int = 1 << 9;
const IBV_QP_RETRY_CNT: c_int = 1 << 10;
const IBV_QP_RNR_RETRY: c_int = 1 << 11;
const IBV_QP_RQ_PSN: c_int = 1 << 12;
const IBV_QP_MAX_QP_RD_ATOMIC: c_int = 1 << 13;
const IBV_QP_MIN_RNR_TIMER: c_int = 1 << 15;
const IBV_QP_SQ_PSN: c_int = 1 << 16;
const IBV_QP_MAX_DEST_RD_ATOMIC: c_int = 1 << 17;
const IBV_QP_DEST_QPN: c_int = 1 << 20;
const VOLUME_RDMA_REMOTE_F_GID_VALID: u32 = 1 << 0;
const VOLUME_RDMA_REMOTE_F_GRH_REQUIRED: u32 = 1 << 1;
const DEFAULT_RNR_TIMER: u8 = 12;
const DEFAULT_QP_TIMEOUT: u8 = 14;
const DEFAULT_RETRY_COUNT: u8 = 7;
const DEFAULT_RNR_RETRY: u8 = 7;
const DEFAULT_RD_ATOMIC: u8 = 1;
const DEFAULT_GRH_HOP_LIMIT: u8 = 64;

#[derive(Debug, Clone)]
pub struct VolumeVerbsConfig {
    pub device: String,
    pub port: u8,
    pub gid_index: c_int,
    pub psn: u32,
    pub cq_entries: c_int,
    pub max_send_wr: u32,
    pub max_recv_wr: u32,
    pub max_send_sge: u32,
    pub max_recv_sge: u32,
}

impl Default for VolumeVerbsConfig {
    fn default() -> Self {
        Self {
            device: "auto".to_string(),
            port: 1,
            gid_index: 0,
            psn: 0xabcdef,
            cq_entries: 128,
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
        }
    }
}

#[derive(Debug)]
pub struct RealVerbsVolumeRdmaProvider {
    state: Mutex<VerbsProviderState>,
}

impl RealVerbsVolumeRdmaProvider {
    pub fn new(config: VolumeVerbsConfig) -> Result<Self> {
        let resources = VerbsResources::open(config)?;
        let endpoint = resources.endpoint.clone();
        Ok(Self {
            state: Mutex::new(VerbsProviderState {
                _resources: resources,
                endpoint,
                connected_remote: None,
                next_session_id: 1,
                leases: HashMap::new(),
            }),
        })
    }
}

#[async_trait]
impl VolumeRdmaProvider for RealVerbsVolumeRdmaProvider {
    async fn local_endpoint(&self) -> Result<VolumeRdmaEndpointInfo> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("verbs provider mutex poisoned"))?;
        let mut endpoint = state.endpoint.clone();
        endpoint.qp_connected = state.connected_remote.is_some();
        Ok(endpoint)
    }

    async fn connect_endpoint(&self, remote: VolumeRdmaRemoteInfo) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("verbs provider mutex poisoned"))?;
        if let Some(existing) = &state.connected_remote {
            if existing == &remote {
                return Ok(());
            }
            return Err(anyhow!(
                "verbs QP is already connected to qpn={} lid={} psn={}",
                existing.qpn,
                existing.lid,
                existing.psn
            ));
        }
        state._resources.connect(&remote)?;
        state.endpoint = state._resources.endpoint.clone();
        state.connected_remote = Some(remote);
        Ok(())
    }

    async fn register_read(&self, data: Vec<u8>) -> Result<VolumeRdmaRegisteredRead> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("verbs provider mutex poisoned"))?;
        validate_register_read_ready(
            &state.endpoint,
            state.connected_remote.is_some(),
            data.len(),
        )?;

        let session_id = state.next_session_id()?;
        let lease = state._resources.register_read_lease(session_id, data)?;
        let desc = lease.desc.clone();
        state.leases.insert(session_id, lease);

        Ok(VolumeRdmaRegisteredRead { session_id, desc })
    }

    async fn release_read(&self, session_id: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("verbs provider mutex poisoned"))?;
        if let Some(lease) = state.leases.remove(&session_id) {
            lease.release()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct VerbsProviderState {
    _resources: VerbsResources,
    endpoint: VolumeRdmaEndpointInfo,
    connected_remote: Option<VolumeRdmaRemoteInfo>,
    next_session_id: u64,
    leases: HashMap<u64, VerbsReadLease>,
}

impl VerbsProviderState {
    fn next_session_id(&mut self) -> Result<u64> {
        let session_id = self.next_session_id;
        if session_id == 0 {
            return Err(anyhow!("verbs read session id overflow"));
        }
        self.next_session_id = session_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("verbs read session id overflow"))?;
        Ok(session_id)
    }
}

impl Drop for VerbsProviderState {
    fn drop(&mut self) {
        self.leases.clear();
    }
}

#[derive(Debug)]
struct VerbsResources {
    api: Arc<VerbsApi>,
    context: *mut IbvContext,
    pd: *mut IbvPd,
    cq: *mut IbvCq,
    qp: *mut IbvQp,
    endpoint: VolumeRdmaEndpointInfo,
}

// libibverbs resources are process-local handles protected by the provider mutex.
unsafe impl Send for VerbsResources {}

impl VerbsResources {
    fn open(config: VolumeVerbsConfig) -> Result<Self> {
        let api = Arc::new(VerbsApi::load()?);
        let mut count: c_int = 0;
        let list = unsafe { (api.ibv_get_device_list)(&mut count as *mut c_int) };
        if list.is_null() {
            return Err(anyhow!(
                "ibv_get_device_list failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let device_list = DeviceList {
            api: api.clone(),
            list,
        };

        let (device, device_name) = select_device(&api, device_list.list, count, &config.device)?;
        let context = unsafe { (api.ibv_open_device)(device) };
        if context.is_null() {
            return Err(anyhow!(
                "ibv_open_device({device_name}) failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        let resources = unsafe { Self::open_after_context(api, context, device_name, config) };
        match resources {
            Ok(resources) => Ok(resources),
            Err(err) => {
                unsafe {
                    (device_list.api.ibv_close_device)(context);
                }
                Err(err)
            }
        }
    }

    unsafe fn open_after_context(
        api: Arc<VerbsApi>,
        context: *mut IbvContext,
        device_name: String,
        config: VolumeVerbsConfig,
    ) -> Result<Self> {
        let mut port_attr = MaybeUninit::<IbvPortAttr>::zeroed();
        let rc = (api.ibv_query_port)(context, config.port, port_attr.as_mut_ptr());
        if rc != 0 {
            return Err(anyhow!(
                "ibv_query_port({}, {}) failed: {}",
                device_name,
                config.port,
                std::io::Error::last_os_error()
            ));
        }
        let port_attr = port_attr.assume_init();

        let mut gid = MaybeUninit::<IbvGid>::zeroed();
        let rc = (api.ibv_query_gid)(context, config.port, config.gid_index, gid.as_mut_ptr());
        if rc != 0 {
            return Err(anyhow!(
                "ibv_query_gid({}, port={}, gid_index={}) failed: {}",
                device_name,
                config.port,
                config.gid_index,
                std::io::Error::last_os_error()
            ));
        }
        let gid = gid.assume_init();

        let pd = (api.ibv_alloc_pd)(context);
        if pd.is_null() {
            return Err(anyhow!(
                "ibv_alloc_pd({device_name}) failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        let cq = (api.ibv_create_cq)(
            context,
            config.cq_entries,
            ptr::null_mut(),
            ptr::null_mut(),
            0,
        );
        if cq.is_null() {
            (api.ibv_dealloc_pd)(pd);
            return Err(anyhow!(
                "ibv_create_cq({device_name}) failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        let mut qp_init = IbvQpInitAttr {
            qp_context: ptr::null_mut(),
            send_cq: cq,
            recv_cq: cq,
            srq: ptr::null_mut(),
            cap: IbvQpCap {
                max_send_wr: config.max_send_wr,
                max_recv_wr: config.max_recv_wr,
                max_send_sge: config.max_send_sge,
                max_recv_sge: config.max_recv_sge,
                max_inline_data: 0,
            },
            qp_type: IBV_QPT_RC,
            sq_sig_all: 1,
        };
        let qp = (api.ibv_create_qp)(pd, &mut qp_init as *mut IbvQpInitAttr);
        if qp.is_null() {
            (api.ibv_destroy_cq)(cq);
            (api.ibv_dealloc_pd)(pd);
            return Err(anyhow!(
                "ibv_create_qp({device_name}) failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        let qp_prefix = &*(qp as *const IbvQpPrefix);
        let link_layer = map_link_layer(port_attr.link_layer);
        let gid_hex = gid_to_hex(&gid.raw);
        let endpoint_ready = endpoint_ready_for_link(
            port_attr.state,
            link_layer,
            qp_prefix.qp_num,
            port_attr.lid,
            &gid.raw,
        );
        let endpoint = VolumeRdmaEndpointInfo {
            abi_version: ABI_VERSION,
            flags: 0,
            device: device_name.clone(),
            port: config.port as u32,
            qp_num: qp_prefix.qp_num,
            psn: config.psn & 0x00ff_ffff,
            qp_state: qp_prefix.state as u32,
            lid: port_attr.lid as u32,
            sm_lid: port_attr.sm_lid as u32,
            port_state: port_attr.state as u32,
            active_mtu: port_attr.active_mtu as u32,
            gid_index: config.gid_index as u32,
            link_layer,
            gid: gid_hex,
            kernel_enabled: true,
            endpoint_ready,
            qp_connected: false,
            unsafe_global_rkey: false,
        };

        info!(
            "opened verbs volume RDMA endpoint device={} port={} qpn={} lid={} gid_index={} ready={}",
            endpoint.device,
            endpoint.port,
            endpoint.qp_num,
            endpoint.lid,
            endpoint.gid_index,
            endpoint.endpoint_ready
        );

        Ok(Self {
            api,
            context,
            pd,
            cq,
            qp,
            endpoint,
        })
    }

    fn connect(&mut self, remote: &VolumeRdmaRemoteInfo) -> Result<()> {
        validate_remote(&self.endpoint, remote)?;
        unsafe {
            self.modify_to_init()?;
            self.modify_to_rtr(remote)?;
            self.modify_to_rts()?;
        }
        self.endpoint.qp_state = IBV_QPS_RTS as u32;
        self.endpoint.qp_connected = true;
        Ok(())
    }

    fn register_read_lease(&self, session_id: u64, data: Vec<u8>) -> Result<VerbsReadLease> {
        VerbsReadLease::register(self.api.clone(), self.pd, session_id, data)
    }

    unsafe fn modify_to_init(&mut self) -> Result<()> {
        let mut attr = zeroed_qp_attr();
        attr.qp_state = IBV_QPS_INIT;
        attr.pkey_index = 0;
        attr.port_num = self.endpoint.port as u8;
        attr.qp_access_flags =
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        self.modify_qp(
            &mut attr,
            IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS,
            "INIT",
        )?;
        self.endpoint.qp_state = IBV_QPS_INIT as u32;
        Ok(())
    }

    unsafe fn modify_to_rtr(&mut self, remote: &VolumeRdmaRemoteInfo) -> Result<()> {
        let mut attr = zeroed_qp_attr();
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu = path_mtu_for_endpoint(&self.endpoint);
        attr.dest_qp_num = remote.qpn;
        attr.rq_psn = remote.psn & 0x00ff_ffff;
        attr.max_dest_rd_atomic = DEFAULT_RD_ATOMIC;
        attr.min_rnr_timer = DEFAULT_RNR_TIMER;
        attr.ah_attr = build_ah_attr(&self.endpoint, remote)?;
        self.modify_qp(
            &mut attr,
            IBV_QP_STATE
                | IBV_QP_AV
                | IBV_QP_PATH_MTU
                | IBV_QP_DEST_QPN
                | IBV_QP_RQ_PSN
                | IBV_QP_MAX_DEST_RD_ATOMIC
                | IBV_QP_MIN_RNR_TIMER,
            "RTR",
        )?;
        self.endpoint.qp_state = IBV_QPS_RTR as u32;
        Ok(())
    }

    unsafe fn modify_to_rts(&mut self) -> Result<()> {
        let mut attr = zeroed_qp_attr();
        attr.qp_state = IBV_QPS_RTS;
        attr.timeout = DEFAULT_QP_TIMEOUT;
        attr.retry_cnt = DEFAULT_RETRY_COUNT;
        attr.rnr_retry = DEFAULT_RNR_RETRY;
        attr.sq_psn = self.endpoint.psn & 0x00ff_ffff;
        attr.max_rd_atomic = DEFAULT_RD_ATOMIC;
        self.modify_qp(
            &mut attr,
            IBV_QP_STATE
                | IBV_QP_TIMEOUT
                | IBV_QP_RETRY_CNT
                | IBV_QP_RNR_RETRY
                | IBV_QP_SQ_PSN
                | IBV_QP_MAX_QP_RD_ATOMIC,
            "RTS",
        )?;
        Ok(())
    }

    unsafe fn modify_qp(&self, attr: &mut IbvQpAttr, attr_mask: c_int, target: &str) -> Result<()> {
        let rc = (self.api.ibv_modify_qp)(self.qp, attr as *mut IbvQpAttr, attr_mask);
        if rc != 0 {
            return Err(anyhow!(
                "ibv_modify_qp({} -> {}) failed: rc={} errno={}",
                self.endpoint.device,
                target,
                rc,
                std::io::Error::last_os_error()
            ));
        }
        debug!(
            "ibv_modify_qp device={} qpn={} target={} mask=0x{:x}",
            self.endpoint.device, self.endpoint.qp_num, target, attr_mask
        );
        Ok(())
    }
}

struct VerbsReadLease {
    api: Arc<VerbsApi>,
    mr: *mut IbvMr,
    data: Vec<u8>,
    desc: VolumeRdmaDataDesc,
}

// Memory regions are owned by this provider and accessed under its mutex.
unsafe impl Send for VerbsReadLease {}

impl fmt::Debug for VerbsReadLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerbsReadLease")
            .field("mr", &self.mr)
            .field("len", &self.data.len())
            .field("desc", &self.desc)
            .finish()
    }
}

impl VerbsReadLease {
    fn register(
        api: Arc<VerbsApi>,
        pd: *mut IbvPd,
        session_id: u64,
        mut data: Vec<u8>,
    ) -> Result<Self> {
        let access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ;
        let mr = unsafe {
            (api.ibv_reg_mr)(
                pd,
                data.as_mut_ptr() as *mut c_void,
                data.len() as size_t,
                access,
            )
        };
        if mr.is_null() {
            return Err(anyhow!(
                "ibv_reg_mr(read session {}, len={}) failed: {}",
                session_id,
                data.len(),
                std::io::Error::last_os_error()
            ));
        }

        let mr_prefix = unsafe { &*(mr as *const IbvMrPrefix) };
        let desc = read_desc_from_mr(session_id, data.len(), mr_prefix)?;
        debug!(
            "registered verbs read MR session={} addr=0x{:x} rkey={} len={}",
            session_id, desc.remote_addr, desc.rkey, desc.length
        );

        Ok(Self {
            api,
            mr,
            data,
            desc,
        })
    }

    fn release(mut self) -> Result<()> {
        self.deregister("release")
    }

    fn deregister(&mut self, reason: &str) -> Result<()> {
        if self.mr.is_null() {
            return Ok(());
        }
        let rc = unsafe { (self.api.ibv_dereg_mr)(self.mr) };
        if rc != 0 {
            return Err(anyhow!(
                "ibv_dereg_mr({reason}, addr=0x{:x}, rkey={}) failed: rc={} errno={}",
                self.desc.remote_addr,
                self.desc.rkey,
                rc,
                std::io::Error::last_os_error()
            ));
        }
        debug!(
            "deregistered verbs read MR reason={} addr=0x{:x} rkey={} len={}",
            reason, self.desc.remote_addr, self.desc.rkey, self.desc.length
        );
        self.mr = ptr::null_mut();
        Ok(())
    }
}

impl Drop for VerbsReadLease {
    fn drop(&mut self) {
        if let Err(err) = self.deregister("drop") {
            warn!("failed to deregister verbs read MR during drop: {err:#}");
        }
    }
}

impl Drop for VerbsResources {
    fn drop(&mut self) {
        unsafe {
            if !self.qp.is_null() {
                let rc = (self.api.ibv_destroy_qp)(self.qp);
                debug!("ibv_destroy_qp rc={}", rc);
            }
            if !self.cq.is_null() {
                let rc = (self.api.ibv_destroy_cq)(self.cq);
                debug!("ibv_destroy_cq rc={}", rc);
            }
            if !self.pd.is_null() {
                let rc = (self.api.ibv_dealloc_pd)(self.pd);
                debug!("ibv_dealloc_pd rc={}", rc);
            }
            if !self.context.is_null() {
                let rc = (self.api.ibv_close_device)(self.context);
                debug!("ibv_close_device rc={}", rc);
            }
        }
    }
}

struct DeviceList {
    api: Arc<VerbsApi>,
    list: *mut *mut IbvDevice,
}

impl Drop for DeviceList {
    fn drop(&mut self) {
        unsafe {
            (self.api.ibv_free_device_list)(self.list);
        }
    }
}

fn select_device(
    api: &VerbsApi,
    list: *mut *mut IbvDevice,
    count: c_int,
    requested: &str,
) -> Result<(*mut IbvDevice, String)> {
    if count <= 0 {
        return Err(anyhow!("no RDMA devices returned by ibv_get_device_list"));
    }
    let requested = requested.trim();
    let use_auto = requested.is_empty() || requested == "auto";
    let mut names = Vec::new();

    for idx in 0..count as usize {
        let device = unsafe { *list.add(idx) };
        if device.is_null() {
            continue;
        }
        let name = device_name(api, device)?;
        names.push(name.clone());
        if use_auto || name == requested {
            return Ok((device, name));
        }
    }

    Err(anyhow!(
        "RDMA device '{}' not found; available devices: {}",
        requested,
        names.join(", ")
    ))
}

fn device_name(api: &VerbsApi, device: *mut IbvDevice) -> Result<String> {
    let ptr = unsafe { (api.ibv_get_device_name)(device) };
    if ptr.is_null() {
        return Err(anyhow!("ibv_get_device_name returned null"));
    }
    Ok(unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned())
}

fn map_link_layer(link_layer: u8) -> u32 {
    match link_layer {
        IBV_LINK_LAYER_INFINIBAND => LINK_INFINIBAND,
        IBV_LINK_LAYER_ETHERNET => LINK_ETHERNET,
        _ => 0,
    }
}

fn gid_to_hex(raw: &[u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for byte in raw {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

struct VerbsApi {
    _lib: &'static Library,
    lib_name: &'static str,
    ibv_get_device_list: unsafe extern "C" fn(*mut c_int) -> *mut *mut IbvDevice,
    ibv_free_device_list: unsafe extern "C" fn(*mut *mut IbvDevice),
    ibv_get_device_name: unsafe extern "C" fn(*mut IbvDevice) -> *const c_char,
    ibv_open_device: unsafe extern "C" fn(*mut IbvDevice) -> *mut IbvContext,
    ibv_close_device: unsafe extern "C" fn(*mut IbvContext) -> c_int,
    ibv_query_port: unsafe extern "C" fn(*mut IbvContext, u8, *mut IbvPortAttr) -> c_int,
    ibv_query_gid: unsafe extern "C" fn(*mut IbvContext, u8, c_int, *mut IbvGid) -> c_int,
    ibv_alloc_pd: unsafe extern "C" fn(*mut IbvContext) -> *mut IbvPd,
    ibv_dealloc_pd: unsafe extern "C" fn(*mut IbvPd) -> c_int,
    ibv_create_cq:
        unsafe extern "C" fn(*mut IbvContext, c_int, *mut c_void, *mut c_void, c_int) -> *mut IbvCq,
    ibv_destroy_cq: unsafe extern "C" fn(*mut IbvCq) -> c_int,
    ibv_create_qp: unsafe extern "C" fn(*mut IbvPd, *mut IbvQpInitAttr) -> *mut IbvQp,
    ibv_modify_qp: unsafe extern "C" fn(*mut IbvQp, *mut IbvQpAttr, c_int) -> c_int,
    ibv_destroy_qp: unsafe extern "C" fn(*mut IbvQp) -> c_int,
    ibv_reg_mr: unsafe extern "C" fn(*mut IbvPd, *mut c_void, size_t, c_int) -> *mut IbvMr,
    ibv_dereg_mr: unsafe extern "C" fn(*mut IbvMr) -> c_int,
}

impl fmt::Debug for VerbsApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerbsApi")
            .field("lib_name", &self.lib_name)
            .finish_non_exhaustive()
    }
}

impl VerbsApi {
    fn load() -> Result<Self> {
        let names = [
            "libibverbs.so.1",
            "libibverbs.so",
            "/usr/lib/x86_64-linux-gnu/libibverbs.so.1",
            "/usr/lib64/libibverbs.so.1",
            "/lib/x86_64-linux-gnu/libibverbs.so.1",
        ];
        let (lib_name, lib) = names
            .iter()
            .find_map(|name| match unsafe { Library::new(name) } {
                Ok(lib) => Some((*name, lib)),
                Err(err) => {
                    debug!("failed to load {}: {}", name, err);
                    None
                }
            })
            .ok_or_else(|| anyhow!("libibverbs shared library not found"))?;
        let lib = Box::leak(Box::new(lib));

        unsafe {
            Ok(Self {
                _lib: lib,
                lib_name,
                ibv_get_device_list: *lib
                    .get(b"ibv_get_device_list")
                    .context("load ibv_get_device_list")?,
                ibv_free_device_list: *lib
                    .get(b"ibv_free_device_list")
                    .context("load ibv_free_device_list")?,
                ibv_get_device_name: *lib
                    .get(b"ibv_get_device_name")
                    .context("load ibv_get_device_name")?,
                ibv_open_device: *lib
                    .get(b"ibv_open_device")
                    .context("load ibv_open_device")?,
                ibv_close_device: *lib
                    .get(b"ibv_close_device")
                    .context("load ibv_close_device")?,
                ibv_query_port: *lib.get(b"ibv_query_port").context("load ibv_query_port")?,
                ibv_query_gid: *lib.get(b"ibv_query_gid").context("load ibv_query_gid")?,
                ibv_alloc_pd: *lib.get(b"ibv_alloc_pd").context("load ibv_alloc_pd")?,
                ibv_dealloc_pd: *lib.get(b"ibv_dealloc_pd").context("load ibv_dealloc_pd")?,
                ibv_create_cq: *lib.get(b"ibv_create_cq").context("load ibv_create_cq")?,
                ibv_destroy_cq: *lib.get(b"ibv_destroy_cq").context("load ibv_destroy_cq")?,
                ibv_create_qp: *lib.get(b"ibv_create_qp").context("load ibv_create_qp")?,
                ibv_modify_qp: *lib.get(b"ibv_modify_qp").context("load ibv_modify_qp")?,
                ibv_destroy_qp: *lib.get(b"ibv_destroy_qp").context("load ibv_destroy_qp")?,
                ibv_reg_mr: *lib.get(b"ibv_reg_mr").context("load ibv_reg_mr")?,
                ibv_dereg_mr: *lib.get(b"ibv_dereg_mr").context("load ibv_dereg_mr")?,
            })
        }
    }
}

#[repr(C)]
struct IbvDevice {
    _private: [u8; 0],
}

#[repr(C)]
struct IbvContext {
    _private: [u8; 0],
}

#[repr(C)]
struct IbvPd {
    _private: [u8; 0],
}

#[repr(C)]
struct IbvCq {
    _private: [u8; 0],
}

#[repr(C)]
struct IbvQp {
    _private: [u8; 0],
}

#[repr(C)]
struct IbvSrq {
    _private: [u8; 0],
}

#[repr(C)]
struct IbvMr {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IbvGid {
    raw: [u8; 16],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IbvGlobalRoute {
    dgid: IbvGid,
    flow_label: u32,
    sgid_index: u8,
    hop_limit: u8,
    traffic_class: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IbvAhAttr {
    grh: IbvGlobalRoute,
    dlid: u16,
    sl: u8,
    src_path_bits: u8,
    static_rate: u8,
    is_global: u8,
    port_num: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IbvPortAttr {
    state: c_int,
    max_mtu: c_int,
    active_mtu: c_int,
    gid_tbl_len: c_int,
    port_cap_flags: u32,
    max_msg_sz: u32,
    bad_pkey_cntr: u32,
    qkey_viol_cntr: u32,
    pkey_tbl_len: u16,
    lid: u16,
    sm_lid: u16,
    lmc: u8,
    max_vl_num: u8,
    sm_sl: u8,
    subnet_timeout: u8,
    init_type_reply: u8,
    active_width: u8,
    active_speed: u8,
    phys_state: u8,
    link_layer: u8,
    flags: u8,
    port_cap_flags2: u16,
    active_speed_ex: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IbvQpCap {
    max_send_wr: u32,
    max_recv_wr: u32,
    max_send_sge: u32,
    max_recv_sge: u32,
    max_inline_data: u32,
}

#[repr(C)]
struct IbvQpInitAttr {
    qp_context: *mut c_void,
    send_cq: *mut IbvCq,
    recv_cq: *mut IbvCq,
    srq: *mut IbvSrq,
    cap: IbvQpCap,
    qp_type: c_int,
    sq_sig_all: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IbvQpAttr {
    qp_state: c_int,
    cur_qp_state: c_int,
    path_mtu: c_int,
    path_mig_state: c_int,
    qkey: u32,
    rq_psn: u32,
    sq_psn: u32,
    dest_qp_num: u32,
    qp_access_flags: c_int,
    cap: IbvQpCap,
    ah_attr: IbvAhAttr,
    alt_ah_attr: IbvAhAttr,
    pkey_index: u16,
    alt_pkey_index: u16,
    en_sqd_async_notify: u8,
    sq_draining: u8,
    max_rd_atomic: u8,
    max_dest_rd_atomic: u8,
    min_rnr_timer: u8,
    port_num: u8,
    timeout: u8,
    retry_cnt: u8,
    rnr_retry: u8,
    alt_port_num: u8,
    alt_timeout: u8,
    rate_limit: u32,
}

#[repr(C)]
struct IbvQpPrefix {
    context: *mut IbvContext,
    qp_context: *mut c_void,
    pd: *mut IbvPd,
    send_cq: *mut IbvCq,
    recv_cq: *mut IbvCq,
    srq: *mut IbvSrq,
    handle: u32,
    qp_num: u32,
    state: c_int,
    qp_type: c_int,
}

#[repr(C)]
struct IbvMrPrefix {
    context: *mut IbvContext,
    pd: *mut IbvPd,
    addr: *mut c_void,
    length: size_t,
    handle: u32,
    lkey: u32,
    rkey: u32,
}

fn zeroed_qp_attr() -> IbvQpAttr {
    unsafe { MaybeUninit::<IbvQpAttr>::zeroed().assume_init() }
}

fn validate_register_read_ready(
    endpoint: &VolumeRdmaEndpointInfo,
    connected_remote: bool,
    data_len: usize,
) -> Result<()> {
    if data_len == 0 {
        return Err(anyhow!("register_read requires data"));
    }
    if data_len > u32::MAX as usize {
        return Err(anyhow!("register_read data too large"));
    }
    if !connected_remote || !endpoint.qp_connected || endpoint.qp_state != IBV_QPS_RTS as u32 {
        return Err(anyhow!(
            "register_read requires connected RTS QP: qpn={} qp_connected={} qp_state={} remote_connected={}",
            endpoint.qp_num,
            endpoint.qp_connected,
            endpoint.qp_state,
            connected_remote
        ));
    }
    Ok(())
}

fn read_desc_from_mr(
    session_id: u64,
    data_len: usize,
    mr: &IbvMrPrefix,
) -> Result<VolumeRdmaDataDesc> {
    if data_len == 0 {
        return Err(anyhow!("register_read requires data"));
    }
    if data_len > u32::MAX as usize {
        return Err(anyhow!("register_read data too large"));
    }
    if mr.addr.is_null() {
        return Err(anyhow!("ibv_reg_mr returned null address"));
    }
    if mr.length < data_len as size_t {
        return Err(anyhow!(
            "ibv_reg_mr length {} is shorter than data length {}",
            mr.length,
            data_len
        ));
    }

    Ok(VolumeRdmaDataDesc {
        remote_addr: mr.addr as u64,
        rkey: mr.rkey,
        length: data_len as u32,
        reserved: [session_id, 0, 0, 0],
    })
}

fn validate_remote(local: &VolumeRdmaEndpointInfo, remote: &VolumeRdmaRemoteInfo) -> Result<()> {
    if remote.abi_version != ABI_VERSION {
        return Err(anyhow!(
            "unsupported remote ABI version {}, expected {}",
            remote.abi_version,
            ABI_VERSION
        ));
    }
    if !local.endpoint_ready {
        return Err(anyhow!(
            "local RDMA endpoint is not ready: device={} qpn={} lid={} port_state={} link_layer={}",
            local.device,
            local.qp_num,
            local.lid,
            local.port_state,
            local.link_layer
        ));
    }
    if remote.qpn == 0 {
        return Err(anyhow!("remote QPN is required"));
    }
    if remote.psn > 0x00ff_ffff {
        return Err(anyhow!("remote PSN must fit in 24 bits: {}", remote.psn));
    }
    if remote.port == 0 {
        return Err(anyhow!("remote port is required"));
    }
    if remote.sl > 15 {
        return Err(anyhow!("remote service level must be 0..15: {}", remote.sl));
    }
    if local.link_layer == LINK_INFINIBAND && remote.lid == 0 {
        return Err(anyhow!("remote LID is required for InfiniBand link layer"));
    }
    if requires_grh(local, remote) && !remote_gid_is_valid(remote) {
        return Err(anyhow!(
            "remote GID is required for Ethernet/RoCE or GRH-required path"
        ));
    }
    Ok(())
}

fn requires_grh(local: &VolumeRdmaEndpointInfo, remote: &VolumeRdmaRemoteInfo) -> bool {
    local.link_layer == LINK_ETHERNET || (remote.flags & VOLUME_RDMA_REMOTE_F_GRH_REQUIRED) != 0
}

fn remote_gid_is_valid(remote: &VolumeRdmaRemoteInfo) -> bool {
    (remote.flags & VOLUME_RDMA_REMOTE_F_GID_VALID) != 0 && gid_has_value(&remote.gid)
}

fn gid_has_value(gid: &[u8; 16]) -> bool {
    gid.iter().any(|byte| *byte != 0)
}

fn endpoint_ready_for_link(
    port_state: c_int,
    link_layer: u32,
    qp_num: u32,
    lid: u16,
    gid: &[u8; 16],
) -> bool {
    if port_state != IBV_PORT_ACTIVE || qp_num == 0 {
        return false;
    }
    match link_layer {
        LINK_INFINIBAND => lid != 0,
        LINK_ETHERNET => gid_has_value(gid),
        _ => false,
    }
}

fn path_mtu_for_endpoint(endpoint: &VolumeRdmaEndpointInfo) -> c_int {
    match endpoint.active_mtu {
        1..=5 => endpoint.active_mtu as c_int,
        _ => 3,
    }
}

fn build_ah_attr(
    local: &VolumeRdmaEndpointInfo,
    remote: &VolumeRdmaRemoteInfo,
) -> Result<IbvAhAttr> {
    validate_remote(local, remote)?;
    let mut attr = zeroed_ah_attr();
    attr.dlid = remote.lid as u16;
    attr.sl = remote.sl as u8;
    attr.src_path_bits = 0;
    attr.static_rate = 0;
    attr.port_num = local.port as u8;

    if requires_grh(local, remote) {
        attr.is_global = 1;
        attr.grh.dgid = IbvGid { raw: remote.gid };
        attr.grh.flow_label = 0;
        attr.grh.sgid_index = local.gid_index as u8;
        attr.grh.hop_limit = DEFAULT_GRH_HOP_LIMIT;
        attr.grh.traffic_class = 0;
    }

    Ok(attr)
}

fn zeroed_ah_attr() -> IbvAhAttr {
    unsafe { MaybeUninit::<IbvAhAttr>::zeroed().assume_init() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_gid_as_go_hex() {
        let gid = [
            0x20, 0x01, 0x0d, 0xb8, 0, 1, 2, 3, 4, 5, 6, 7, 0xaa, 0xbb, 0xcc, 0xdd,
        ];
        assert_eq!(gid_to_hex(&gid), "20010db80001020304050607aabbccdd");
    }

    #[test]
    fn maps_verbs_link_layers() {
        assert_eq!(map_link_layer(IBV_LINK_LAYER_INFINIBAND), LINK_INFINIBAND);
        assert_eq!(map_link_layer(IBV_LINK_LAYER_ETHERNET), LINK_ETHERNET);
        assert_eq!(map_link_layer(99), 0);
    }

    fn local_endpoint(link_layer: u32) -> VolumeRdmaEndpointInfo {
        VolumeRdmaEndpointInfo {
            abi_version: ABI_VERSION,
            flags: 0,
            device: "mlx5_0".to_string(),
            port: 1,
            qp_num: 10,
            psn: 0x123456,
            qp_state: 0,
            lid: if link_layer == LINK_INFINIBAND { 7 } else { 0 },
            sm_lid: 1,
            port_state: IBV_PORT_ACTIVE as u32,
            active_mtu: 4,
            gid_index: 3,
            link_layer,
            gid: "20010db80001020304050607aabbccdd".to_string(),
            kernel_enabled: true,
            endpoint_ready: true,
            qp_connected: false,
            unsafe_global_rkey: false,
        }
    }

    fn remote_info(link_layer: u32) -> VolumeRdmaRemoteInfo {
        VolumeRdmaRemoteInfo {
            abi_version: ABI_VERSION,
            flags: VOLUME_RDMA_REMOTE_F_GID_VALID
                | if link_layer == LINK_ETHERNET {
                    VOLUME_RDMA_REMOTE_F_GRH_REQUIRED
                } else {
                    0
                },
            qpn: 99,
            lid: if link_layer == LINK_INFINIBAND { 8 } else { 0 },
            psn: 0x654321,
            port: 1,
            gid_index: 2,
            sl: 3,
            gid: [
                0x20, 0x01, 0x0d, 0xb8, 0, 1, 2, 3, 4, 5, 6, 7, 0xaa, 0xbb, 0xcc, 0xdd,
            ],
            reserved: [0; 8],
        }
    }

    #[test]
    fn validates_remote_for_infiniband() {
        let local = local_endpoint(LINK_INFINIBAND);
        let remote = remote_info(LINK_INFINIBAND);
        validate_remote(&local, &remote).unwrap();

        let mut missing_lid = remote.clone();
        missing_lid.lid = 0;
        assert!(validate_remote(&local, &missing_lid)
            .unwrap_err()
            .to_string()
            .contains("LID"));
    }

    #[test]
    fn validates_remote_for_roce_gid() {
        let local = local_endpoint(LINK_ETHERNET);
        let remote = remote_info(LINK_ETHERNET);
        validate_remote(&local, &remote).unwrap();

        let mut missing_gid = remote.clone();
        missing_gid.flags = 0;
        assert!(validate_remote(&local, &missing_gid)
            .unwrap_err()
            .to_string()
            .contains("GID"));
    }

    #[test]
    fn rejects_bad_remote_metadata() {
        let local = local_endpoint(LINK_INFINIBAND);
        let mut remote = remote_info(LINK_INFINIBAND);

        remote.qpn = 0;
        assert!(validate_remote(&local, &remote)
            .unwrap_err()
            .to_string()
            .contains("QPN"));

        remote = remote_info(LINK_INFINIBAND);
        remote.psn = 0x0100_0000;
        assert!(validate_remote(&local, &remote)
            .unwrap_err()
            .to_string()
            .contains("PSN"));

        remote = remote_info(LINK_INFINIBAND);
        remote.sl = 16;
        assert!(validate_remote(&local, &remote)
            .unwrap_err()
            .to_string()
            .contains("service level"));
    }

    #[test]
    fn builds_infiniband_ah_attr_without_grh() {
        let local = local_endpoint(LINK_INFINIBAND);
        let remote = remote_info(LINK_INFINIBAND);
        let ah = build_ah_attr(&local, &remote).unwrap();
        assert_eq!(ah.is_global, 0);
        assert_eq!(ah.dlid, remote.lid as u16);
        assert_eq!(ah.sl, remote.sl as u8);
        assert_eq!(ah.port_num, local.port as u8);
    }

    #[test]
    fn builds_roce_ah_attr_with_grh() {
        let local = local_endpoint(LINK_ETHERNET);
        let remote = remote_info(LINK_ETHERNET);
        let ah = build_ah_attr(&local, &remote).unwrap();
        assert_eq!(ah.is_global, 1);
        assert_eq!(ah.grh.dgid.raw, remote.gid);
        assert_eq!(ah.grh.sgid_index, local.gid_index as u8);
        assert_eq!(ah.grh.hop_limit, DEFAULT_GRH_HOP_LIMIT);
    }

    #[test]
    fn endpoint_ready_matches_link_layer_requirements() {
        let gid = [1; 16];
        let zero_gid = [0; 16];
        assert!(endpoint_ready_for_link(
            IBV_PORT_ACTIVE,
            LINK_INFINIBAND,
            12,
            7,
            &zero_gid
        ));
        assert!(endpoint_ready_for_link(
            IBV_PORT_ACTIVE,
            LINK_ETHERNET,
            12,
            0,
            &gid
        ));
        assert!(!endpoint_ready_for_link(
            IBV_PORT_ACTIVE,
            LINK_ETHERNET,
            12,
            0,
            &zero_gid
        ));
    }

    #[test]
    fn path_mtu_uses_active_mtu_or_default() {
        let mut endpoint = local_endpoint(LINK_INFINIBAND);
        endpoint.active_mtu = 5;
        assert_eq!(path_mtu_for_endpoint(&endpoint), 5);
        endpoint.active_mtu = 0;
        assert_eq!(path_mtu_for_endpoint(&endpoint), 3);
    }

    #[test]
    fn validates_register_read_ready_state() {
        let mut endpoint = local_endpoint(LINK_INFINIBAND);
        endpoint.qp_state = IBV_QPS_RTS as u32;
        endpoint.qp_connected = true;
        validate_register_read_ready(&endpoint, true, 4096).unwrap();

        assert!(validate_register_read_ready(&endpoint, true, 0)
            .unwrap_err()
            .to_string()
            .contains("requires data"));

        endpoint.qp_connected = false;
        assert!(validate_register_read_ready(&endpoint, true, 4096)
            .unwrap_err()
            .to_string()
            .contains("connected RTS QP"));

        endpoint.qp_connected = true;
        endpoint.qp_state = IBV_QPS_RTR as u32;
        assert!(validate_register_read_ready(&endpoint, true, 4096)
            .unwrap_err()
            .to_string()
            .contains("connected RTS QP"));

        endpoint.qp_state = IBV_QPS_RTS as u32;
        assert!(validate_register_read_ready(&endpoint, false, 4096)
            .unwrap_err()
            .to_string()
            .contains("remote_connected=false"));
    }

    #[test]
    fn builds_read_desc_from_mr_prefix() {
        let mut payload = vec![1u8; 32];
        let mr = IbvMrPrefix {
            context: ptr::null_mut(),
            pd: ptr::null_mut(),
            addr: payload.as_mut_ptr() as *mut c_void,
            length: payload.len() as size_t,
            handle: 9,
            lkey: 10,
            rkey: 11,
        };

        let desc = read_desc_from_mr(77, payload.len(), &mr).unwrap();
        assert_eq!(desc.remote_addr, payload.as_mut_ptr() as u64);
        assert_eq!(desc.rkey, 11);
        assert_eq!(desc.length, payload.len() as u32);
        assert_eq!(desc.reserved, [77, 0, 0, 0]);
    }

    #[test]
    fn rejects_bad_mr_prefix_values() {
        let mr = IbvMrPrefix {
            context: ptr::null_mut(),
            pd: ptr::null_mut(),
            addr: ptr::null_mut(),
            length: 8,
            handle: 0,
            lkey: 0,
            rkey: 1,
        };
        assert!(read_desc_from_mr(1, 8, &mr)
            .unwrap_err()
            .to_string()
            .contains("null address"));

        let mut payload = vec![1u8; 8];
        let mr = IbvMrPrefix {
            context: ptr::null_mut(),
            pd: ptr::null_mut(),
            addr: payload.as_mut_ptr() as *mut c_void,
            length: 7,
            handle: 0,
            lkey: 0,
            rkey: 1,
        };
        assert!(read_desc_from_mr(1, 8, &mr)
            .unwrap_err()
            .to_string()
            .contains("shorter"));
    }
}
