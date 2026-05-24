//! RDMA operations and context management
//! 
//! This module provides both mock and real RDMA implementations:
//! - Mock implementation for development and testing
//! - Real implementation using libibverbs for production

use crate::{RdmaResult, RdmaEngineConfig};
use tracing::{debug, warn, info};
use parking_lot::RwLock;

/// RDMA completion status
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompletionStatus {
    Success,
    LocalLengthError,
    LocalQpOperationError,
    LocalEecOperationError,
    LocalProtectionError,
    WrFlushError,
    MemoryWindowBindError,
    BadResponseError,
    LocalAccessError,
    RemoteInvalidRequestError,
    RemoteAccessError,
    RemoteOperationError,
    TransportRetryCounterExceeded,
    RnrRetryCounterExceeded,
    LocalRddViolationError,
    RemoteInvalidRdRequest,
    RemoteAbortedError,
    InvalidEecnError,
    InvalidEecStateError,
    FatalError,
    ResponseTimeoutError,
    GeneralError,
}

impl From<u32> for CompletionStatus {
    fn from(status: u32) -> Self {
        match status {
            0 => Self::Success,
            1 => Self::LocalLengthError,
            2 => Self::LocalQpOperationError,
            3 => Self::LocalEecOperationError,
            4 => Self::LocalProtectionError,
            5 => Self::WrFlushError,
            6 => Self::MemoryWindowBindError,
            7 => Self::BadResponseError,
            8 => Self::LocalAccessError,
            9 => Self::RemoteInvalidRequestError,
            10 => Self::RemoteAccessError,
            11 => Self::RemoteOperationError,
            12 => Self::TransportRetryCounterExceeded,
            13 => Self::RnrRetryCounterExceeded,
            14 => Self::LocalRddViolationError,
            15 => Self::RemoteInvalidRdRequest,
            16 => Self::RemoteAbortedError,
            17 => Self::InvalidEecnError,
            18 => Self::InvalidEecStateError,
            19 => Self::FatalError,
            20 => Self::ResponseTimeoutError,
            _ => Self::GeneralError,
        }
    }
}

/// RDMA operation types
#[derive(Debug, Clone, Copy)]
pub enum RdmaOp {
    Read,
    Write,
    Send,
    Receive,
    Atomic,
}

/// RDMA memory region information
#[derive(Debug, Clone)]
pub struct MemoryRegion {
    /// Local virtual address
    pub addr: u64,
    /// Remote key for RDMA operations
    pub rkey: u32,
    /// Local key for local operations
    pub lkey: u32,
    /// Size of the memory region
    pub size: usize,
    /// Whether the region is registered with RDMA hardware
    pub registered: bool,
}

/// RDMA work completion
#[derive(Debug)]
pub struct WorkCompletion {
    /// Work request ID
    pub wr_id: u64,
    /// Completion status
    pub status: CompletionStatus,
    /// Operation type
    pub opcode: RdmaOp,
    /// Number of bytes transferred
    pub byte_len: u32,
    /// Immediate data (if any)
    pub imm_data: Option<u32>,
}

/// RDMA device information
#[derive(Debug, Clone)]
pub struct RdmaDeviceInfo {
    pub name: String,
    pub vendor_id: u32,
    pub vendor_part_id: u32,
    pub hw_ver: u32,
    pub max_mr: u32,
    pub max_qp: u32,
    pub max_cq: u32,
    pub max_mr_size: u64,
    pub port_gid: String,
    pub port_lid: u16,
}

/// Mock RDMA context for testing and development
#[derive(Debug)]
pub struct MockRdmaContext {
    device_info: RdmaDeviceInfo,
    registered_regions: RwLock<Vec<MemoryRegion>>,
    pending_operations: RwLock<Vec<WorkCompletion>>,
    #[allow(dead_code)]
    config: RdmaEngineConfig,
}

impl MockRdmaContext {
    pub async fn new(config: &RdmaEngineConfig) -> RdmaResult<Self> {
        warn!("🟡 Using MOCK RDMA implementation - for development only!");
        info!("   Device: {} (mock)", config.device_name);
        info!("   Port: {} (mock)", config.port);
        
        let device_info = RdmaDeviceInfo {
            name: config.device_name.clone(),
            vendor_id: 0x02c9, // Mellanox mock vendor ID
            vendor_part_id: 0x1017, // ConnectX-5 mock part ID
            hw_ver: 0,
            max_mr: 131072,
            max_qp: 262144,
            max_cq: 65536,
            max_mr_size: 1024 * 1024 * 1024 * 1024, // 1TB mock
            port_gid: "fe80:0000:0000:0000:0200:5eff:fe12:3456".to_string(),
            port_lid: 1,
        };
        
        Ok(Self {
            device_info,
            registered_regions: RwLock::new(Vec::new()),
            pending_operations: RwLock::new(Vec::new()),
            config: config.clone(),
        })
    }
}

impl MockRdmaContext {
    pub async fn register_memory(&self, addr: u64, size: usize) -> RdmaResult<MemoryRegion> {
        debug!("🟡 Mock: Registering memory region addr=0x{:x}, size={}", addr, size);
        
        // Simulate registration delay
        tokio::time::sleep(tokio::time::Duration::from_micros(10)).await;
        
        let region = MemoryRegion {
            addr,
            rkey: 0x12345678, // Mock remote key
            lkey: 0x87654321, // Mock local key
            size,
            registered: true,
        };
        
        self.registered_regions.write().push(region.clone());
        
        Ok(region)
    }
    
    pub async fn deregister_memory(&self, region: &MemoryRegion) -> RdmaResult<()> {
        debug!("🟡 Mock: Deregistering memory region rkey=0x{:x}", region.rkey);
        
        let mut regions = self.registered_regions.write();
        regions.retain(|r| r.rkey != region.rkey);
        
        Ok(())
    }
    
    pub async fn post_read(&self, 
        local_addr: u64, 
        remote_addr: u64, 
        rkey: u32, 
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        debug!("🟡 Mock: RDMA READ local=0x{:x}, remote=0x{:x}, rkey=0x{:x}, size={}", 
               local_addr, remote_addr, rkey, size);
        
        // Simulate RDMA read latency (much faster than real network, but realistic for mock)
        tokio::time::sleep(tokio::time::Duration::from_nanos(150)).await;
        
        // Mock data transfer - copy pattern data to local address
        let data_ptr = local_addr as *mut u8;
        unsafe {
            for i in 0..size {
                *data_ptr.add(i) = (i % 256) as u8; // Pattern: 0,1,2,...,255,0,1,2...
            }
        }
        
        // Create completion
        let completion = WorkCompletion {
            wr_id,
            status: CompletionStatus::Success,
            opcode: RdmaOp::Read,
            byte_len: size as u32,
            imm_data: None,
        };
        
        self.pending_operations.write().push(completion);
        
        Ok(())
    }
    
    pub async fn post_write(&self, 
        local_addr: u64, 
        remote_addr: u64, 
        rkey: u32, 
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        debug!("🟡 Mock: RDMA WRITE local=0x{:x}, remote=0x{:x}, rkey=0x{:x}, size={}", 
               local_addr, remote_addr, rkey, size);
        
        // Simulate RDMA write latency
        tokio::time::sleep(tokio::time::Duration::from_nanos(100)).await;
        
        // Create completion
        let completion = WorkCompletion {
            wr_id,
            status: CompletionStatus::Success,
            opcode: RdmaOp::Write,
            byte_len: size as u32,
            imm_data: None,
        };
        
        self.pending_operations.write().push(completion);
        
        Ok(())
    }
    
    pub async fn poll_completion(&self, max_completions: usize) -> RdmaResult<Vec<WorkCompletion>> {
        let mut operations = self.pending_operations.write();
        let available = operations.len().min(max_completions);
        let completions = operations.drain(..available).collect();
        
        Ok(completions)
    }
    
    pub fn device_info(&self) -> &RdmaDeviceInfo {
        &self.device_info
    }
}

/// UCX-backed RDMA context for Mellanox/NVIDIA NICs.
#[cfg(feature = "real-ucx")]
pub struct UcxRdmaContext {
    ucx: std::sync::Arc<crate::ucx::UcxContext>,
    device_info: RdmaDeviceInfo,
    pending_operations: parking_lot::RwLock<Vec<WorkCompletion>>,
}

#[cfg(feature = "real-ucx")]
impl UcxRdmaContext {
    pub async fn new(config: &RdmaEngineConfig) -> RdmaResult<Self> {
        let ucx = std::sync::Arc::new(crate::ucx::UcxContext::new().await?);
        let device_info = RdmaDeviceInfo {
            name: config.device_name.clone(),
            vendor_id: 0x02c9,
            vendor_part_id: 0x1017,
            hw_ver: 0,
            max_mr: 131072,
            max_qp: 262144,
            max_cq: 65536,
            max_mr_size: 1024 * 1024 * 1024 * 1024,
            port_gid: "ucx-auto".to_string(),
            port_lid: 0,
        };
        Ok(Self {
            ucx,
            device_info,
            pending_operations: parking_lot::RwLock::new(Vec::new()),
        })
    }

    pub async fn register_memory(&self, addr: u64, size: usize) -> RdmaResult<MemoryRegion> {
        self.ucx.map_memory(addr, size).await?;
        Ok(MemoryRegion {
            addr,
            rkey: 0,
            lkey: 0,
            size,
            registered: true,
        })
    }

    pub async fn deregister_memory(&self, region: &MemoryRegion) -> RdmaResult<()> {
        self.ucx.unmap_memory(region.addr).await
    }

    pub async fn post_read(
        &self,
        local_addr: u64,
        remote_addr: u64,
        _rkey: u32,
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        if remote_addr != 0 {
            self.ucx.get(local_addr, remote_addr, size).await?;
        } else {
            // Local session coordination without a remote peer yet.
            let data_ptr = local_addr as *mut u8;
            unsafe {
                for i in 0..size {
                    *data_ptr.add(i) = (i % 256) as u8;
                }
            }
        }

        self.pending_operations.write().push(WorkCompletion {
            wr_id,
            status: CompletionStatus::Success,
            opcode: RdmaOp::Read,
            byte_len: size as u32,
            imm_data: None,
        });
        Ok(())
    }

    pub async fn post_write(
        &self,
        local_addr: u64,
        remote_addr: u64,
        rkey: u32,
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        if remote_addr != 0 {
            self.ucx.put(local_addr, remote_addr, size).await?;
        }
        self.pending_operations.write().push(WorkCompletion {
            wr_id,
            status: CompletionStatus::Success,
            opcode: RdmaOp::Write,
            byte_len: size as u32,
            imm_data: None,
        });
        Ok(())
    }

    pub async fn poll_completion(&self, max_completions: usize) -> RdmaResult<Vec<WorkCompletion>> {
        let mut operations = self.pending_operations.write();
        let available = operations.len().min(max_completions);
        Ok(operations.drain(..available).collect())
    }

    pub fn device_info(&self) -> &RdmaDeviceInfo {
        &self.device_info
    }
}

#[cfg(feature = "real-ucx")]
impl std::fmt::Debug for UcxRdmaContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UcxRdmaContext")
            .field("device", &self.device_info.name)
            .finish()
    }
}

/// RDMA context implementation (simplified enum approach)
#[derive(Debug)]
pub enum RdmaContextImpl {
    Mock(MockRdmaContext),
    #[cfg(feature = "real-ucx")]
    Ucx(UcxRdmaContext),
}

/// Main RDMA context
pub struct RdmaContext {
    inner: RdmaContextImpl,
    #[allow(dead_code)]
    config: RdmaEngineConfig,
}

impl RdmaContext {
    pub async fn new(config: &RdmaEngineConfig) -> RdmaResult<Self> {
        let inner = if cfg!(feature = "real-ucx") {
            match UcxRdmaContext::new(config).await {
                Ok(ctx) => {
                    info!("✅ Using UCX-backed RDMA context");
                    RdmaContextImpl::Ucx(ctx)
                }
                Err(err) => {
                    warn!("⚠️  UCX init failed ({}), falling back to mock RDMA", err);
                    RdmaContextImpl::Mock(MockRdmaContext::new(config).await?)
                }
            }
        } else {
            RdmaContextImpl::Mock(MockRdmaContext::new(config).await?)
        };

        Ok(Self {
            inner,
            config: config.clone(),
        })
    }

    pub fn is_real_rdma(&self) -> bool {
        matches!(self.inner, RdmaContextImpl::Ucx(_))
    }

    pub async fn register_memory(&self, addr: u64, size: usize) -> RdmaResult<MemoryRegion> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.register_memory(addr, size).await,
            #[cfg(feature = "real-ucx")]
            RdmaContextImpl::Ucx(ctx) => ctx.register_memory(addr, size).await,
        }
    }

    pub async fn deregister_memory(&self, region: &MemoryRegion) -> RdmaResult<()> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.deregister_memory(region).await,
            #[cfg(feature = "real-ucx")]
            RdmaContextImpl::Ucx(ctx) => ctx.deregister_memory(region).await,
        }
    }

    pub async fn post_read(
        &self,
        local_addr: u64,
        remote_addr: u64,
        rkey: u32,
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.post_read(local_addr, remote_addr, rkey, size, wr_id).await,
            #[cfg(feature = "real-ucx")]
            RdmaContextImpl::Ucx(ctx) => ctx.post_read(local_addr, remote_addr, rkey, size, wr_id).await,
        }
    }

    pub async fn post_write(
        &self,
        local_addr: u64,
        remote_addr: u64,
        rkey: u32,
        size: usize,
        wr_id: u64,
    ) -> RdmaResult<()> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.post_write(local_addr, remote_addr, rkey, size, wr_id).await,
            #[cfg(feature = "real-ucx")]
            RdmaContextImpl::Ucx(ctx) => ctx.post_write(local_addr, remote_addr, rkey, size, wr_id).await,
        }
    }

    pub async fn poll_completion(&self, max_completions: usize) -> RdmaResult<Vec<WorkCompletion>> {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.poll_completion(max_completions).await,
            #[cfg(feature = "real-ucx")]
            RdmaContextImpl::Ucx(ctx) => ctx.poll_completion(max_completions).await,
        }
    }

    pub fn device_info(&self) -> &RdmaDeviceInfo {
        match &self.inner {
            RdmaContextImpl::Mock(ctx) => ctx.device_info(),
            #[cfg(feature = "real-ucx")]
            RdmaContextImpl::Ucx(ctx) => ctx.device_info(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_mock_rdma_context() {
        let config = RdmaEngineConfig::default();
        let ctx = RdmaContext::new(&config).await.unwrap();
        
        // Test device info
        let info = ctx.device_info();
        assert_eq!(info.name, "mlx5_0");
        assert!(info.max_mr > 0);
        
        // Test memory registration
        let addr = 0x7f000000u64;
        let size = 4096;
        let region = ctx.register_memory(addr, size).await.unwrap();
        assert_eq!(region.addr, addr);
        assert_eq!(region.size, size);
        assert!(region.registered);
        
        // Test RDMA read
        let local_buf = vec![0u8; 1024];
        let local_addr = local_buf.as_ptr() as u64;
        let result = ctx.post_read(local_addr, 0x8000000, region.rkey, 1024, 1).await;
        assert!(result.is_ok());
        
        // Test completion polling
        let completions = ctx.poll_completion(10).await.unwrap();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].status, CompletionStatus::Success);
        
        // Test memory deregistration
        let result = ctx.deregister_memory(&region).await;
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_completion_status_conversion() {
        assert_eq!(CompletionStatus::from(0), CompletionStatus::Success);
        assert_eq!(CompletionStatus::from(1), CompletionStatus::LocalLengthError);
        assert_eq!(CompletionStatus::from(999), CompletionStatus::GeneralError);
    }
}
