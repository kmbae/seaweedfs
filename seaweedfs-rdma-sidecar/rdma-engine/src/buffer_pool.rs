//! Reusable RDMA-registered buffers.
//!
//! UCX memory registration is expensive enough that per-chunk register/unregister
//! dominates small and medium transfers. This pool keeps a bounded set of
//! already-registered Vec-backed buffers and hands them to read/write paths.

use crate::{
    ipc::MAX_IPC_MESSAGE_SIZE,
    rdma::{MemoryRegion, RdmaContext},
    RdmaError, RdmaResult,
};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::debug;

const DEFAULT_REGISTERED_BUFFER_POOL_SIZE: usize = 16;
pub const MIN_REGISTERED_BUFFER_SIZE: usize = 64 * 1024;

pub struct RegisteredBufferPool {
    idle: Mutex<Vec<RegisteredBuffer>>,
    max_idle: usize,
}

pub struct RegisteredBuffer {
    pub data: Vec<u8>,
    pub region: MemoryRegion,
}

impl RegisteredBufferPool {
    pub fn from_env() -> Self {
        let max_idle = std::env::var("RDMA_REGISTERED_BUFFER_POOL_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_REGISTERED_BUFFER_POOL_SIZE);
        Self::new(max_idle)
    }

    pub fn new(max_idle: usize) -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
            max_idle,
        }
    }

    pub async fn acquire(
        &self,
        rdma_context: &Arc<RdmaContext>,
        size: usize,
    ) -> RdmaResult<RegisteredBuffer> {
        let capacity = registered_buffer_capacity(size)?;
        if let Some(buffer) = self.take_idle(capacity) {
            debug!(
                "♻️ Reusing registered RDMA buffer: addr=0x{:x}, capacity={}, requested={}",
                buffer.region.addr,
                buffer.data.len(),
                size
            );
            return Ok(buffer);
        }

        let mut data = vec![0u8; capacity];
        let local_addr = data.as_mut_ptr() as u64;
        let region = rdma_context.register_memory(local_addr, capacity).await?;
        debug!(
            "📌 Registered new pooled RDMA buffer: addr=0x{:x}, capacity={}, requested={}",
            region.addr, capacity, size
        );
        Ok(RegisteredBuffer { data, region })
    }

    fn take_idle(&self, capacity: usize) -> Option<RegisteredBuffer> {
        let mut idle = self.idle.lock();
        let index = idle
            .iter()
            .position(|buffer| buffer.data.len() >= capacity)?;
        Some(idle.swap_remove(index))
    }

    pub async fn release(&self, rdma_context: &Arc<RdmaContext>, buffer: RegisteredBuffer) {
        let mut discard = Some(buffer);
        {
            let mut idle = self.idle.lock();
            if idle.len() < self.max_idle {
                idle.push(discard.take().expect("registered buffer"));
            }
        }
        if let Some(buffer) = discard {
            let _ = rdma_context.deregister_memory(&buffer.region).await;
        }
    }
}

pub fn registered_buffer_capacity(size: usize) -> RdmaResult<usize> {
    if size == 0 {
        return Err(RdmaError::invalid_request("registered buffer size is zero"));
    }
    if size > MAX_IPC_MESSAGE_SIZE {
        return Err(RdmaError::ipc_error(format!(
            "registered buffer too large: {} bytes",
            size
        )));
    }
    Ok(size
        .max(MIN_REGISTERED_BUFFER_SIZE)
        .next_power_of_two()
        .min(MAX_IPC_MESSAGE_SIZE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_buffer_capacity_rounds_for_reuse() {
        assert_eq!(
            registered_buffer_capacity(1).unwrap(),
            MIN_REGISTERED_BUFFER_SIZE
        );
        assert_eq!(
            registered_buffer_capacity(MIN_REGISTERED_BUFFER_SIZE + 1).unwrap(),
            MIN_REGISTERED_BUFFER_SIZE * 2
        );
        assert_eq!(
            registered_buffer_capacity(MAX_IPC_MESSAGE_SIZE).unwrap(),
            MAX_IPC_MESSAGE_SIZE
        );
        assert!(registered_buffer_capacity(MAX_IPC_MESSAGE_SIZE + 1).is_err());
    }
}
