use clap::Parser;
use rdma_engine::volume_native_verbs::{run_verbs_loopback_read_selftest, VolumeVerbsConfig};
use std::time::Duration;
use tracing::info;
use tracing_subscriber::{fmt::layer, prelude::*, EnvFilter};

#[derive(Parser, Debug)]
#[command(
    name = "volume-rdma-read-selftest",
    about = "Loopback native verbs RDMA READ selftest for the SeaweedFS volume data path",
    version = env!("CARGO_PKG_VERSION")
)]
struct Args {
    /// RDMA device name. Use 'auto' to select the first verbs device.
    #[arg(long, default_value = "auto")]
    device: String,

    /// RDMA HCA port number.
    #[arg(long, default_value_t = 1)]
    port: u8,

    /// RDMA GID table index.
    #[arg(long, default_value_t = 0)]
    gid_index: i32,

    /// Source QP packet sequence number.
    #[arg(long, default_value_t = 0xabcdef)]
    psn: u32,

    /// RDMA service level.
    #[arg(long, default_value_t = 0)]
    service_level: u32,

    /// Payload size to register on the source side and read from requester side.
    #[arg(long, default_value_t = 1 << 20)]
    size: usize,

    /// Completion timeout in milliseconds.
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,

    /// Enable debug logging.
    #[arg(long)]
    debug: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing(args.debug);

    let payload = make_payload(args.size)?;
    let report = run_verbs_loopback_read_selftest(
        VolumeVerbsConfig {
            device: args.device,
            port: args.port,
            gid_index: args.gid_index,
            psn: args.psn,
            ..VolumeVerbsConfig::default()
        },
        payload,
        args.service_level,
        Duration::from_millis(args.timeout_ms),
    )?;

    info!(
        "RDMA READ selftest passed: bytes={} source_qpn={} requester_qpn={} session={} remote_addr=0x{:x} rkey={}",
        report.bytes,
        report.source_qpn,
        report.requester_qpn,
        report.session_id,
        report.remote_addr,
        report.rkey
    );
    println!(
        "ok bytes={} source_qpn={} requester_qpn={} session={} remote_addr=0x{:x} rkey={}",
        report.bytes,
        report.source_qpn,
        report.requester_qpn,
        report.session_id,
        report.remote_addr,
        report.rkey
    );
    Ok(())
}

fn make_payload(size: usize) -> anyhow::Result<Vec<u8>> {
    if size == 0 {
        anyhow::bail!("size must be greater than zero");
    }
    if size > u32::MAX as usize {
        anyhow::bail!("size too large: {}", size);
    }
    let mut payload = vec![0u8; size];
    for (idx, byte) in payload.iter_mut().enumerate() {
        *byte = ((idx * 31 + 17) & 0xff) as u8;
    }
    Ok(payload)
}

fn init_tracing(debug: bool) {
    let filter = if debug {
        EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("debug"))
            .unwrap()
    } else {
        EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("info"))
            .unwrap()
    };
    tracing_subscriber::registry()
        .with(layer().with_target(false))
        .with(filter)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_is_valid() {
        Args::command().debug_assert();
    }

    #[test]
    fn payload_pattern_is_deterministic() {
        let payload = make_payload(4).unwrap();
        assert_eq!(payload, vec![17, 48, 79, 110]);
    }

    #[test]
    fn rejects_empty_payload() {
        assert!(make_payload(0).is_err());
    }
}
