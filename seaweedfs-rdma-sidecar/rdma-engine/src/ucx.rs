//! UCX (Unified Communication X) FFI bindings and high-level wrapper
//!
//! UCX is a superior alternative to direct libibverbs for RDMA programming.
//! It provides production-proven abstractions and automatic transport selection.
//!
//! References:
//! - UCX Documentation: https://openucx.readthedocs.io/
//! - UCX GitHub: https://github.com/openucx/ucx
//! - UCX Paper: "UCX: an open source framework for HPC network APIs and beyond"

use crate::{RdmaError, RdmaResult};
use libc::{c_char, c_int, c_void, size_t};
use libloading::{Library, Symbol};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ffi::CStr;
use std::ptr;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// UCX context handle
pub type UcpContext = *mut c_void;
/// UCX worker handle  
pub type UcpWorker = *mut c_void;
/// UCX endpoint handle
pub type UcpEp = *mut c_void;
/// UCX memory handle
pub type UcpMem = *mut c_void;
/// UCX remote key handle
pub type UcpRkey = *mut c_void;
/// UCX request handle
pub type UcpRequest = *mut c_void;

/// UCX configuration parameters
#[repr(C)]
pub struct UcpParams {
    pub field_mask: u64,
    pub features: u64,
    pub request_size: size_t,
    pub request_init: extern "C" fn(*mut c_void),
    pub request_cleanup: extern "C" fn(*mut c_void),
    pub tag_sender_mask: u64,
}

/// UCX worker parameters
#[repr(C)]
pub struct UcpWorkerParams {
    pub field_mask: u64,
    pub thread_mode: c_int,
    pub cpu_mask: u64,
    pub events: c_int,
    pub user_data: *mut c_void,
}

/// UCX endpoint parameters
#[repr(C)]
pub struct UcpEpParams {
    pub field_mask: u64,
    pub address: *const c_void,
    pub flags: u64,
    pub sock_addr: *const c_void,
    pub err_handler: UcpErrHandler,
    pub user_data: *mut c_void,
}

/// UCX memory mapping parameters
#[repr(C)]
pub struct UcpMemMapParams {
    pub field_mask: u64,
    pub address: *mut c_void,
    pub length: size_t,
    pub flags: u64,
    pub prot: c_int,
}

/// UCX error handler callback
pub type UcpErrHandler = extern "C" fn(arg: *mut c_void, ep: UcpEp, status: c_int);

/// UCX request callback
pub type UcpSendCallback =
    extern "C" fn(request: *mut c_void, status: c_int, user_data: *mut c_void);

/// UCX feature flags
pub const UCP_FEATURE_TAG: u64 = 1 << 0;
pub const UCP_FEATURE_RMA: u64 = 1 << 1;
pub const UCP_FEATURE_ATOMIC32: u64 = 1 << 2;
pub const UCP_FEATURE_ATOMIC64: u64 = 1 << 3;
pub const UCP_FEATURE_WAKEUP: u64 = 1 << 4;
pub const UCP_FEATURE_STREAM: u64 = 1 << 5;

/// UCX parameter field masks
pub const UCP_PARAM_FIELD_FEATURES: u64 = 1 << 0;
pub const UCP_PARAM_FIELD_REQUEST_SIZE: u64 = 1 << 1;
pub const UCP_PARAM_FIELD_REQUEST_INIT: u64 = 1 << 2;
pub const UCP_PARAM_FIELD_REQUEST_CLEANUP: u64 = 1 << 3;
pub const UCP_PARAM_FIELD_TAG_SENDER_MASK: u64 = 1 << 4;

pub const UCP_WORKER_PARAM_FIELD_THREAD_MODE: u64 = 1 << 0;
pub const UCP_WORKER_PARAM_FIELD_CPU_MASK: u64 = 1 << 1;
pub const UCP_WORKER_PARAM_FIELD_EVENTS: u64 = 1 << 2;
pub const UCP_WORKER_PARAM_FIELD_USER_DATA: u64 = 1 << 3;

pub const UCP_EP_PARAM_FIELD_REMOTE_ADDRESS: u64 = 1 << 0;
pub const UCP_EP_PARAM_FIELD_FLAGS: u64 = 1 << 1;
pub const UCP_EP_PARAM_FIELD_SOCK_ADDR: u64 = 1 << 2;
pub const UCP_EP_PARAM_FIELD_ERR_HANDLER: u64 = 1 << 3;
pub const UCP_EP_PARAM_FIELD_USER_DATA: u64 = 1 << 4;

pub const UCP_MEM_MAP_PARAM_FIELD_ADDRESS: u64 = 1 << 0;
pub const UCP_MEM_MAP_PARAM_FIELD_LENGTH: u64 = 1 << 1;
pub const UCP_MEM_MAP_PARAM_FIELD_FLAGS: u64 = 1 << 2;
pub const UCP_MEM_MAP_PARAM_FIELD_PROT: u64 = 1 << 3;

/// UCX status codes
pub const UCS_OK: c_int = 0;
pub const UCS_INPROGRESS: c_int = 1;
pub const UCS_ERR_NO_MESSAGE: c_int = -1;
pub const UCS_ERR_NO_RESOURCE: c_int = -2;
pub const UCS_ERR_IO_ERROR: c_int = -3;
pub const UCS_ERR_NO_MEMORY: c_int = -4;
pub const UCS_ERR_INVALID_PARAM: c_int = -5;
pub const UCS_ERR_UNREACHABLE: c_int = -6;
pub const UCS_ERR_INVALID_ADDR: c_int = -7;
pub const UCS_ERR_NOT_IMPLEMENTED: c_int = -8;
pub const UCS_ERR_MESSAGE_TRUNCATED: c_int = -9;
pub const UCS_ERR_NO_PROGRESS: c_int = -10;
pub const UCS_ERR_BUFFER_TOO_SMALL: c_int = -11;
pub const UCS_ERR_NO_ELEM: c_int = -12;
pub const UCS_ERR_SOME_CONNECTS_FAILED: c_int = -13;
pub const UCS_ERR_NO_DEVICE: c_int = -14;
pub const UCS_ERR_BUSY: c_int = -15;
pub const UCS_ERR_CANCELED: c_int = -16;
pub const UCS_ERR_SHMEM_SEGMENT: c_int = -17;
pub const UCS_ERR_ALREADY_EXISTS: c_int = -18;
pub const UCS_ERR_OUT_OF_RANGE: c_int = -19;
pub const UCS_ERR_TIMED_OUT: c_int = -20;

/// UCX memory protection flags  
pub const UCP_MEM_MAP_NONBLOCK: u64 = 1 << 0;
pub const UCP_MEM_MAP_ALLOCATE: u64 = 1 << 1;
pub const UCP_MEM_MAP_FIXED: u64 = 1 << 2;

/// UCX API version — must match the installed UCX headers (UCP_API_MAJOR, UCP_API_MINOR).
const UCP_API_MAJOR: libc::c_uint = 1;
const UCP_API_MINOR: libc::c_uint = 19;

/// UCX FFI function signatures
pub struct UcxApi {
    pub ucp_init_version: Symbol<
        'static,
        unsafe extern "C" fn(
            libc::c_uint,
            libc::c_uint,
            *const UcpParams,
            *const c_void,
            *mut UcpContext,
        ) -> c_int,
    >,
    pub ucp_cleanup: Symbol<'static, unsafe extern "C" fn(UcpContext)>,
    pub ucp_worker_create: Symbol<
        'static,
        unsafe extern "C" fn(UcpContext, *const UcpWorkerParams, *mut UcpWorker) -> c_int,
    >,
    pub ucp_worker_destroy: Symbol<'static, unsafe extern "C" fn(UcpWorker)>,
    pub ucp_ep_create:
        Symbol<'static, unsafe extern "C" fn(UcpWorker, *const UcpEpParams, *mut UcpEp) -> c_int>,
    pub ucp_ep_destroy: Symbol<'static, unsafe extern "C" fn(UcpEp)>,
    pub ucp_mem_map: Symbol<
        'static,
        unsafe extern "C" fn(UcpContext, *const UcpMemMapParams, *mut UcpMem) -> c_int,
    >,
    pub ucp_mem_unmap: Symbol<'static, unsafe extern "C" fn(UcpContext, UcpMem) -> c_int>,
    pub ucp_rkey_pack: Symbol<
        'static,
        unsafe extern "C" fn(UcpContext, UcpMem, *mut *mut c_void, *mut size_t) -> c_int,
    >,
    pub ucp_rkey_buffer_release: Symbol<'static, unsafe extern "C" fn(*mut c_void)>,
    pub ucp_ep_rkey_unpack:
        Symbol<'static, unsafe extern "C" fn(UcpEp, *const c_void, *mut UcpRkey) -> c_int>,
    pub ucp_rkey_destroy: Symbol<'static, unsafe extern "C" fn(UcpRkey)>,
    pub ucp_put_nb: Symbol<
        'static,
        unsafe extern "C" fn(
            UcpEp,
            *const c_void,
            size_t,
            u64,
            UcpRkey,
            UcpSendCallback,
        ) -> UcpRequest,
    >,
    pub ucp_get_nb: Symbol<
        'static,
        unsafe extern "C" fn(
            UcpEp,
            *mut c_void,
            size_t,
            u64,
            UcpRkey,
            UcpSendCallback,
        ) -> UcpRequest,
    >,
    pub ucp_worker_progress: Symbol<'static, unsafe extern "C" fn(UcpWorker) -> c_int>,
    pub ucp_request_check_status: Symbol<'static, unsafe extern "C" fn(UcpRequest) -> c_int>,
    pub ucp_request_free: Symbol<'static, unsafe extern "C" fn(UcpRequest)>,
    pub ucp_worker_get_address:
        Symbol<'static, unsafe extern "C" fn(UcpWorker, *mut *mut c_void, *mut size_t) -> c_int>,
    pub ucp_worker_release_address: Symbol<'static, unsafe extern "C" fn(UcpWorker, *mut c_void)>,
    pub ucs_status_string: Symbol<'static, unsafe extern "C" fn(c_int) -> *const c_char>,
}

impl UcxApi {
    /// Load UCX libraries and resolve symbols.
    ///
    /// `libucp.so` provides the UCP transport API while `libucs.so` contains
    /// utility symbols such as `ucs_status_string`.  The real shared-library
    /// export is `ucp_init_version` (the C header's `ucp_init` is an inline
    /// wrapper around it).
    pub fn load() -> RdmaResult<Self> {
        info!("🔗 Loading UCX library");

        let ucp_names = [
            "libucp.so.0",
            "libucp.so",
            "libucp.dylib",
            "/usr/lib/x86_64-linux-gnu/libucp.so.0",
            "/usr/lib64/libucp.so.0",
            "/lib/libucp.so.0",
        ];

        let ucp_lib = ucp_names
            .iter()
            .find_map(|name| {
                debug!("Trying to load UCX library: {}", name);
                match unsafe { Library::new(name) } {
                    Ok(lib) => {
                        info!("✅ Successfully loaded UCX library: {}", name);
                        Some(lib)
                    }
                    Err(e) => {
                        debug!("Failed to load {}: {}", name, e);
                        None
                    }
                }
            })
            .ok_or_else(|| RdmaError::context_init_failed("UCX libucp library not found"))?;

        let ucs_names = [
            "libucs.so.0",
            "libucs.so",
            "/usr/lib/x86_64-linux-gnu/libucs.so.0",
            "/usr/lib64/libucs.so.0",
            "/lib/libucs.so.0",
        ];

        let ucs_lib = ucs_names
            .iter()
            .find_map(|name| {
                debug!("Trying to load UCS library: {}", name);
                match unsafe { Library::new(name) } {
                    Ok(lib) => {
                        info!("✅ Successfully loaded UCS library: {}", name);
                        Some(lib)
                    }
                    Err(e) => {
                        debug!("Failed to load {}: {}", name, e);
                        None
                    }
                }
            })
            .ok_or_else(|| RdmaError::context_init_failed("UCX libucs library not found"))?;

        let ucp_lib: &'static Library = Box::leak(Box::new(ucp_lib));
        let ucs_lib: &'static Library = Box::leak(Box::new(ucs_lib));

        unsafe {
            Ok(UcxApi {
                ucp_init_version: ucp_lib.get(b"ucp_init_version").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_init_version symbol: {}", e))
                })?,
                ucp_cleanup: ucp_lib.get(b"ucp_cleanup").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_cleanup symbol: {}", e))
                })?,
                ucp_worker_create: ucp_lib.get(b"ucp_worker_create").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_worker_create symbol: {}", e))
                })?,
                ucp_worker_destroy: ucp_lib.get(b"ucp_worker_destroy").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_worker_destroy symbol: {}", e))
                })?,
                ucp_ep_create: ucp_lib.get(b"ucp_ep_create").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_ep_create symbol: {}", e))
                })?,
                ucp_ep_destroy: ucp_lib.get(b"ucp_ep_destroy").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_ep_destroy symbol: {}", e))
                })?,
                ucp_mem_map: ucp_lib.get(b"ucp_mem_map").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_mem_map symbol: {}", e))
                })?,
                ucp_mem_unmap: ucp_lib.get(b"ucp_mem_unmap").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_mem_unmap symbol: {}", e))
                })?,
                ucp_rkey_pack: ucp_lib.get(b"ucp_rkey_pack").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_rkey_pack symbol: {}", e))
                })?,
                ucp_rkey_buffer_release: ucp_lib.get(b"ucp_rkey_buffer_release").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_rkey_buffer_release symbol: {}", e))
                })?,
                ucp_ep_rkey_unpack: ucp_lib.get(b"ucp_ep_rkey_unpack").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_ep_rkey_unpack symbol: {}", e))
                })?,
                ucp_rkey_destroy: ucp_lib.get(b"ucp_rkey_destroy").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_rkey_destroy symbol: {}", e))
                })?,
                ucp_put_nb: ucp_lib.get(b"ucp_put_nb").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_put_nb symbol: {}", e))
                })?,
                ucp_get_nb: ucp_lib.get(b"ucp_get_nb").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_get_nb symbol: {}", e))
                })?,
                ucp_worker_progress: ucp_lib.get(b"ucp_worker_progress").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_worker_progress symbol: {}", e))
                })?,
                ucp_request_check_status: ucp_lib.get(b"ucp_request_check_status").map_err(
                    |e| {
                        RdmaError::context_init_failed(format!(
                            "ucp_request_check_status symbol: {}",
                            e
                        ))
                    },
                )?,
                ucp_request_free: ucp_lib.get(b"ucp_request_free").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_request_free symbol: {}", e))
                })?,
                ucp_worker_get_address: ucp_lib.get(b"ucp_worker_get_address").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucp_worker_get_address symbol: {}", e))
                })?,
                ucp_worker_release_address: ucp_lib.get(b"ucp_worker_release_address").map_err(
                    |e| {
                        RdmaError::context_init_failed(format!(
                            "ucp_worker_release_address symbol: {}",
                            e
                        ))
                    },
                )?,
                ucs_status_string: ucs_lib.get(b"ucs_status_string").map_err(|e| {
                    RdmaError::context_init_failed(format!("ucs_status_string symbol: {}", e))
                })?,
            })
        }
    }

    /// Convert UCX status code to human-readable string
    pub fn status_string(&self, status: c_int) -> String {
        unsafe {
            let c_str = (self.ucs_status_string)(status);
            if c_str.is_null() {
                format!("Unknown status: {}", status)
            } else {
                CStr::from_ptr(c_str).to_string_lossy().to_string()
            }
        }
    }
}

/// High-level UCX context wrapper
pub struct UcxContext {
    api: Arc<UcxApi>,
    context: UcpContext,
    worker: UcpWorker,
    worker_address: Vec<u8>,
    endpoints: Mutex<HashMap<String, UcpEp>>,
    memory_regions: Mutex<HashMap<u64, UcpMem>>,
    worker_ops: Mutex<()>,
}

// Safety: UCX handles are created once and never moved. Endpoints and memory regions
// are guarded by Mutex; the worker is configured single-threaded (see UcpWorkerParams).
unsafe impl Send for UcxContext {}
unsafe impl Sync for UcxContext {}

impl UcxContext {
    /// Initialize UCX context with RMA support
    pub fn new() -> RdmaResult<Self> {
        info!("🚀 Initializing UCX context for RDMA operations");

        let api = Arc::new(UcxApi::load()?);

        // Initialize UCP context
        let params = UcpParams {
            field_mask: UCP_PARAM_FIELD_FEATURES,
            features: UCP_FEATURE_RMA | UCP_FEATURE_WAKEUP,
            request_size: 0,
            request_init: request_init_cb,
            request_cleanup: request_cleanup_cb,
            tag_sender_mask: 0,
        };

        let mut context = ptr::null_mut();
        let status = unsafe {
            (api.ucp_init_version)(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params,
                ptr::null(),
                &mut context,
            )
        };
        if status != UCS_OK {
            return Err(RdmaError::context_init_failed(format!(
                "ucp_init_version failed: {} ({})",
                api.status_string(status),
                status
            )));
        }

        info!("✅ UCX context initialized successfully");

        // Create worker
        let worker_params = UcpWorkerParams {
            field_mask: UCP_WORKER_PARAM_FIELD_THREAD_MODE,
            thread_mode: 0, // Single-threaded
            cpu_mask: 0,
            events: 0,
            user_data: ptr::null_mut(),
        };

        let mut worker = ptr::null_mut();
        let status = unsafe { (api.ucp_worker_create)(context, &worker_params, &mut worker) };
        if status != UCS_OK {
            unsafe { (api.ucp_cleanup)(context) };
            return Err(RdmaError::context_init_failed(format!(
                "ucp_worker_create failed: {} ({})",
                api.status_string(status),
                status
            )));
        }

        info!("✅ UCX worker created successfully");

        // Get worker address for connection establishment
        let mut address_ptr = ptr::null_mut();
        let mut address_len = 0;
        let status =
            unsafe { (api.ucp_worker_get_address)(worker, &mut address_ptr, &mut address_len) };
        if status != UCS_OK {
            unsafe {
                (api.ucp_worker_destroy)(worker);
                (api.ucp_cleanup)(context);
            }
            return Err(RdmaError::context_init_failed(format!(
                "ucp_worker_get_address failed: {} ({})",
                api.status_string(status),
                status
            )));
        }

        let worker_address =
            unsafe { std::slice::from_raw_parts(address_ptr as *const u8, address_len).to_vec() };

        unsafe { (api.ucp_worker_release_address)(worker, address_ptr) };

        info!(
            "✅ UCX worker address obtained ({} bytes)",
            worker_address.len()
        );

        Ok(UcxContext {
            api,
            context,
            worker,
            worker_address,
            endpoints: Mutex::new(HashMap::new()),
            memory_regions: Mutex::new(HashMap::new()),
            worker_ops: Mutex::new(()),
        })
    }

    /// Map memory for RDMA operations
    pub fn map_memory(&self, addr: u64, size: usize) -> RdmaResult<Vec<u8>> {
        debug!(
            "📍 Mapping memory for RDMA: addr=0x{:x}, size={}",
            addr, size
        );

        let params = UcpMemMapParams {
            field_mask: UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH,
            address: addr as *mut c_void,
            length: size,
            flags: 0,
            prot: libc::PROT_READ | libc::PROT_WRITE,
        };

        let mut mem_handle = ptr::null_mut();
        let status = unsafe { (self.api.ucp_mem_map)(self.context, &params, &mut mem_handle) };

        if status != UCS_OK {
            return Err(RdmaError::memory_reg_failed(format!(
                "ucp_mem_map failed: {} ({})",
                self.api.status_string(status),
                status
            )));
        }

        let mut rkey_buffer = ptr::null_mut();
        let mut rkey_size: size_t = 0;
        let status = unsafe {
            (self.api.ucp_rkey_pack)(self.context, mem_handle, &mut rkey_buffer, &mut rkey_size)
        };
        if status != UCS_OK {
            unsafe { (self.api.ucp_mem_unmap)(self.context, mem_handle) };
            return Err(RdmaError::memory_reg_failed(format!(
                "ucp_rkey_pack failed: {} ({})",
                self.api.status_string(status),
                status
            )));
        }

        let packed_rkey =
            unsafe { std::slice::from_raw_parts(rkey_buffer as *const u8, rkey_size).to_vec() };
        unsafe { (self.api.ucp_rkey_buffer_release)(rkey_buffer) };

        // Store memory handle for cleanup
        {
            let mut regions = self.memory_regions.lock();
            regions.insert(addr, mem_handle);
        }

        info!(
            "✅ Memory mapped successfully: addr=0x{:x}, size={}, rkey_bytes={}",
            addr,
            size,
            packed_rkey.len()
        );
        Ok(packed_rkey)
    }

    /// Unmap memory
    pub fn unmap_memory(&self, addr: u64) -> RdmaResult<()> {
        debug!("🗑️ Unmapping memory: addr=0x{:x}", addr);

        let mem_handle = {
            let mut regions = self.memory_regions.lock();
            regions.remove(&addr)
        };

        if let Some(handle) = mem_handle {
            let status = unsafe { (self.api.ucp_mem_unmap)(self.context, handle) };
            if status != UCS_OK {
                warn!(
                    "ucp_mem_unmap failed: {} ({})",
                    self.api.status_string(status),
                    status
                );
            }
        }

        Ok(())
    }

    /// Perform RDMA GET (read from remote memory)
    pub fn get(&self, local_addr: u64, remote_addr: u64, size: usize) -> RdmaResult<()> {
        debug!(
            "📥 RDMA GET: local=0x{:x}, remote=0x{:x}, size={}",
            local_addr, remote_addr, size
        );
        let _worker_guard = self.worker_ops.lock();

        // For now, use a simple synchronous approach
        // In production, this would be properly async with completion callbacks

        // Find or create endpoint (simplified - would need proper address resolution)
        let ep = self.get_or_create_endpoint_unlocked("default")?;

        let request = unsafe {
            (self.api.ucp_get_nb)(
                ep,
                local_addr as *mut c_void,
                size,
                remote_addr,
                ptr::null_mut(),
                get_completion_cb,
            )
        };

        // Wait for completion
        if !request.is_null() {
            loop {
                let status = unsafe { (self.api.ucp_request_check_status)(request) };
                if status != UCS_INPROGRESS {
                    unsafe { (self.api.ucp_request_free)(request) };
                    if status == UCS_OK {
                        break;
                    } else {
                        return Err(RdmaError::operation_failed("RDMA GET", status));
                    }
                }

                // Progress the worker
                unsafe { (self.api.ucp_worker_progress)(self.worker) };
                std::thread::yield_now();
            }
        }

        info!("✅ RDMA GET completed successfully");
        Ok(())
    }

    /// Perform RDMA PUT (write to remote memory)
    pub fn put(&self, local_addr: u64, remote_addr: u64, size: usize) -> RdmaResult<()> {
        debug!(
            "📤 RDMA PUT: local=0x{:x}, remote=0x{:x}, size={}",
            local_addr, remote_addr, size
        );
        let _worker_guard = self.worker_ops.lock();

        let ep = self.get_or_create_endpoint_unlocked("default")?;

        let request = unsafe {
            (self.api.ucp_put_nb)(
                ep,
                local_addr as *const c_void,
                size,
                remote_addr,
                ptr::null_mut(),
                put_completion_cb,
            )
        };

        // Wait for completion (same pattern as GET)
        if !request.is_null() {
            loop {
                let status = unsafe { (self.api.ucp_request_check_status)(request) };
                if status != UCS_INPROGRESS {
                    unsafe { (self.api.ucp_request_free)(request) };
                    if status == UCS_OK {
                        break;
                    } else {
                        return Err(RdmaError::operation_failed("RDMA PUT", status));
                    }
                }

                unsafe { (self.api.ucp_worker_progress)(self.worker) };
                std::thread::yield_now();
            }
        }

        info!("✅ RDMA PUT completed successfully");
        Ok(())
    }

    /// Perform RDMA GET from a named UCX peer using its worker address and packed rkey.
    pub fn get_from_peer(
        &self,
        peer_key: &str,
        worker_address: &[u8],
        local_addr: u64,
        remote_addr: u64,
        remote_rkey: &[u8],
        size: usize,
    ) -> RdmaResult<()> {
        debug!(
            "📥 RDMA GET peer={} local=0x{:x}, remote=0x{:x}, size={}",
            peer_key, local_addr, remote_addr, size
        );

        let _worker_guard = self.worker_ops.lock();
        let ep = self.connect_peer_unlocked(peer_key, worker_address)?;
        let rkey = self.unpack_rkey(ep, remote_rkey)?;
        let request = unsafe {
            (self.api.ucp_get_nb)(
                ep,
                local_addr as *mut c_void,
                size,
                remote_addr,
                rkey,
                get_completion_cb,
            )
        };
        let result = self.wait_request_unlocked(request, "RDMA GET peer");
        unsafe { (self.api.ucp_rkey_destroy)(rkey) };
        result?;
        info!("✅ RDMA GET from peer completed successfully");
        Ok(())
    }

    /// Perform RDMA PUT to a named UCX peer using its worker address and packed rkey.
    pub fn put_to_peer(
        &self,
        peer_key: &str,
        worker_address: &[u8],
        local_addr: u64,
        remote_addr: u64,
        remote_rkey: &[u8],
        size: usize,
    ) -> RdmaResult<()> {
        debug!(
            "📤 RDMA PUT peer={} local=0x{:x}, remote=0x{:x}, size={}",
            peer_key, local_addr, remote_addr, size
        );

        let _worker_guard = self.worker_ops.lock();
        let ep = self.connect_peer_unlocked(peer_key, worker_address)?;
        let rkey = self.unpack_rkey(ep, remote_rkey)?;
        let request = unsafe {
            (self.api.ucp_put_nb)(
                ep,
                local_addr as *const c_void,
                size,
                remote_addr,
                rkey,
                put_completion_cb,
            )
        };
        let result = self.wait_request_unlocked(request, "RDMA PUT peer");
        unsafe { (self.api.ucp_rkey_destroy)(rkey) };
        result?;
        info!("✅ RDMA PUT to peer completed successfully");
        Ok(())
    }

    fn unpack_rkey(&self, ep: UcpEp, remote_rkey: &[u8]) -> RdmaResult<UcpRkey> {
        if remote_rkey.is_empty() {
            return Err(RdmaError::invalid_request("empty remote rkey"));
        }

        let mut rkey = ptr::null_mut();
        let status = unsafe {
            (self.api.ucp_ep_rkey_unpack)(ep, remote_rkey.as_ptr() as *const c_void, &mut rkey)
        };
        if status != UCS_OK {
            return Err(RdmaError::operation_failed("ucp_ep_rkey_unpack", status));
        }
        Ok(rkey)
    }

    fn wait_request_unlocked(&self, request: UcpRequest, operation: &str) -> RdmaResult<()> {
        if request.is_null() {
            return Ok(());
        }

        loop {
            let status = unsafe { (self.api.ucp_request_check_status)(request) };
            if status != UCS_INPROGRESS {
                unsafe { (self.api.ucp_request_free)(request) };
                if status == UCS_OK {
                    return Ok(());
                }
                return Err(RdmaError::operation_failed(operation, status));
            }

            unsafe { (self.api.ucp_worker_progress)(self.worker) };
            std::thread::yield_now();
        }
    }

    /// Get worker address for connection establishment
    pub fn worker_address(&self) -> &[u8] {
        &self.worker_address
    }

    /// Create endpoint to a remote UCX peer using its worker address bytes.
    pub fn connect_peer(&self, key: &str, worker_address: &[u8]) -> RdmaResult<UcpEp> {
        let _worker_guard = self.worker_ops.lock();
        self.connect_peer_unlocked(key, worker_address)
    }

    fn connect_peer_unlocked(&self, key: &str, worker_address: &[u8]) -> RdmaResult<UcpEp> {
        let mut endpoints = self.endpoints.lock();
        if let Some(&ep) = endpoints.get(key) {
            return Ok(ep);
        }

        let ep_params = UcpEpParams {
            field_mask: UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
            address: worker_address.as_ptr() as *const c_void,
            flags: 0,
            sock_addr: ptr::null(),
            err_handler: error_handler_cb,
            user_data: ptr::null_mut(),
        };

        let mut endpoint = ptr::null_mut();
        let status = unsafe { (self.api.ucp_ep_create)(self.worker, &ep_params, &mut endpoint) };
        if status != UCS_OK {
            return Err(RdmaError::context_init_failed(format!(
                "ucp_ep_create to peer failed: {} ({})",
                self.api.status_string(status),
                status
            )));
        }

        info!(
            "✅ UCX peer endpoint created for key={} ({} bytes worker addr)",
            key,
            worker_address.len()
        );
        endpoints.insert(key.to_string(), endpoint);
        Ok(endpoint)
    }

    /// Create endpoint for communication (legacy default key — local only)
    fn get_or_create_endpoint_unlocked(&self, key: &str) -> RdmaResult<UcpEp> {
        let endpoints = self.endpoints.lock();

        if let Some(&ep) = endpoints.get(key) {
            return Ok(ep);
        }

        // Without a remote worker address, cannot create a useful peer endpoint.
        Err(RdmaError::context_init_failed(
            "no peer worker address; call connect_peer first".to_string(),
        ))
    }
}

impl Drop for UcxContext {
    fn drop(&mut self) {
        info!("🧹 Cleaning up UCX context");

        // Clean up endpoints
        {
            let mut endpoints = self.endpoints.lock();
            for (_, ep) in endpoints.drain() {
                unsafe { (self.api.ucp_ep_destroy)(ep) };
            }
        }

        // Clean up memory regions
        {
            let mut regions = self.memory_regions.lock();
            for (_, handle) in regions.drain() {
                unsafe { (self.api.ucp_mem_unmap)(self.context, handle) };
            }
        }

        // Clean up worker and context
        unsafe {
            (self.api.ucp_worker_destroy)(self.worker);
            (self.api.ucp_cleanup)(self.context);
        }

        info!("✅ UCX context cleanup completed");
    }
}

// UCX callback functions
extern "C" fn request_init_cb(_request: *mut c_void) {
    // Request initialization callback
}

extern "C" fn request_cleanup_cb(_request: *mut c_void) {
    // Request cleanup callback
}

extern "C" fn get_completion_cb(_request: *mut c_void, status: c_int, _user_data: *mut c_void) {
    if status != UCS_OK {
        error!("RDMA GET completion error: {}", status);
    }
}

extern "C" fn put_completion_cb(_request: *mut c_void, status: c_int, _user_data: *mut c_void) {
    if status != UCS_OK {
        error!("RDMA PUT completion error: {}", status);
    }
}

extern "C" fn error_handler_cb(_arg: *mut c_void, _ep: UcpEp, status: c_int) {
    error!("UCX endpoint error: {}", status);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ucx_api_loading() {
        // This test will fail without UCX installed, which is expected
        match UcxApi::load() {
            Ok(api) => {
                info!("UCX API loaded successfully");
                assert_eq!(api.status_string(UCS_OK), "Success");
            }
            Err(_) => {
                warn!("UCX library not found - expected in development environment");
            }
        }
    }

    #[tokio::test]
    async fn test_ucx_context_mock() {
        // This would test the mock implementation
        // Real test requires UCX installation
    }
}
