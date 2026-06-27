use crate::volume_native::{
    VolumeRdmaEndpointInfo, VolumeRdmaProvider, VolumeRdmaRegisteredRead, VolumeRdmaRemoteInfo,
    ABI_VERSION, LINK_ETHERNET, LINK_INFINIBAND,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use libc::{c_char, c_int, c_void};
use libloading::Library;
use std::ffi::CStr;
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

const IBV_QPT_RC: c_int = 2;
const IBV_PORT_ACTIVE: c_int = 4;
const IBV_LINK_LAYER_INFINIBAND: u8 = 1;
const IBV_LINK_LAYER_ETHERNET: u8 = 2;

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
        if remote.abi_version != ABI_VERSION {
            return Err(anyhow!(
                "unsupported remote ABI version {}, expected {}",
                remote.abi_version,
                ABI_VERSION
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("verbs provider mutex poisoned"))?;
        state.connected_remote = Some(remote);
        Ok(())
    }

    async fn register_read(&self, _data: Vec<u8>) -> Result<VolumeRdmaRegisteredRead> {
        Err(anyhow!(
            "verbs register_read is not implemented yet; next step is ibv_reg_mr-backed read leases"
        ))
    }

    async fn release_read(&self, _session_id: u64) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct VerbsProviderState {
    _resources: VerbsResources,
    endpoint: VolumeRdmaEndpointInfo,
    connected_remote: Option<VolumeRdmaRemoteInfo>,
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
        let endpoint_ready =
            port_attr.state == IBV_PORT_ACTIVE && qp_prefix.qp_num != 0 && port_attr.lid != 0;
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
            link_layer: map_link_layer(port_attr.link_layer),
            gid: gid_to_hex(&gid.raw),
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
    ibv_destroy_qp: unsafe extern "C" fn(*mut IbvQp) -> c_int,
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
                ibv_destroy_qp: *lib.get(b"ibv_destroy_qp").context("load ibv_destroy_qp")?,
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
#[derive(Clone, Copy)]
struct IbvGid {
    raw: [u8; 16],
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
}
