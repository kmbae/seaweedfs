use clap::Parser;
use rdma_engine::volume_native::{VolumeEngineConfig, VolumeNativeEngine, LINK_INFINIBAND};
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::{fmt::layer, prelude::*, EnvFilter};

#[derive(Parser, Debug)]
#[command(
    name = "volume-rdma-engine",
    about = "Native volume RDMA engine for SeaweedFS volume servers",
    version = env!("CARGO_PKG_VERSION")
)]
struct Args {
    /// Unix socket consumed by weed volume -volume.rdma.engineSocket
    #[arg(long, default_value = "/tmp/volume-rdma-engine.sock")]
    socket: String,

    /// RDMA device name reported to SeaweedFS. Mock mode does not open it.
    #[arg(long, default_value = "mock-volume-rdma")]
    device: String,

    /// RDMA port number reported to SeaweedFS.
    #[arg(long, default_value_t = 1)]
    port: u32,

    /// Mock QP number reported by local endpoint.
    #[arg(long, default_value_t = 0x1001)]
    qpn: u32,

    /// Mock packet sequence number reported by local endpoint.
    #[arg(long, default_value_t = 0xabcdef)]
    psn: u32,

    /// Mock LID reported by local endpoint.
    #[arg(long, default_value_t = 1)]
    lid: u32,

    /// Mock GID index reported by local endpoint.
    #[arg(long, default_value_t = 0)]
    gid_index: u32,

    /// Mock GID as 32 hex characters.
    #[arg(long, default_value = "00000000000000000000000000000000")]
    gid: String,

    /// Synthetic base address for mock read descriptors.
    #[arg(long, default_value_t = 0x7f00_0000_0000)]
    mock_base_addr: u64,

    /// Synthetic address stride between mock read descriptor sessions.
    #[arg(long, default_value_t = 0x0100_0000)]
    mock_addr_stride: u64,

    /// Synthetic rkey for mock read descriptors.
    #[arg(long, default_value_t = 0x5eed_0001)]
    mock_rkey: u32,

    /// Enable debug logging.
    #[arg(long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing(args.debug);

    warn!("volume-rdma-engine currently runs a mock provider; descriptors validate IPC flow, not hardware RDMA performance");
    info!("starting volume RDMA engine on {}", args.socket);

    let config = VolumeEngineConfig {
        socket_path: args.socket.clone(),
        device: args.device,
        port: args.port,
        qpn: args.qpn,
        psn: args.psn,
        lid: args.lid,
        gid_index: args.gid_index,
        gid: args.gid,
        link_layer: LINK_INFINIBAND,
        mock_base_addr: args.mock_base_addr,
        mock_addr_stride: args.mock_addr_stride,
        mock_rkey: args.mock_rkey,
    };
    let engine = Arc::new(VolumeNativeEngine::new(config));
    let engine_task = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run().await })
    };

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    tokio::select! {
        _ = sigterm.recv() => {
            info!("received SIGTERM, shutting down volume RDMA engine");
        }
        _ = sigint.recv() => {
            info!("received SIGINT, shutting down volume RDMA engine");
        }
        result = engine_task => {
            match result {
                Ok(Ok(())) => info!("volume RDMA engine exited"),
                Ok(Err(err)) => {
                    error!("volume RDMA engine failed: {err:#}");
                    return Err(err);
                }
                Err(err) => {
                    error!("volume RDMA engine task failed: {err}");
                    return Err(anyhow::anyhow!("volume RDMA engine task failed: {err}"));
                }
            }
        }
    }

    Ok(())
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
    fn parses_socket() {
        let args = Args::try_parse_from(["volume-rdma-engine", "--socket", "/tmp/v.sock"]).unwrap();
        assert_eq!(args.socket, "/tmp/v.sock");
    }
}
