use clap::{Parser, ValueEnum};
use rdma_engine::volume_native::{
    MockVolumeRdmaProvider, VolumeEngineConfig, VolumeNativeEngine, VolumeRdmaProvider,
    LINK_INFINIBAND,
};
use rdma_engine::volume_native_verbs::{RealVerbsVolumeRdmaProvider, VolumeVerbsConfig};
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::{fmt::layer, prelude::*, EnvFilter};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum ProviderMode {
    Mock,
    Verbs,
}

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

    /// Provider backend for endpoint and memory registration.
    #[arg(long, value_enum, default_value_t = ProviderMode::Mock)]
    provider: ProviderMode,

    /// RDMA device name. Use 'auto' to select the first verbs device.
    #[arg(long, default_value = "auto")]
    device: String,

    /// RDMA HCA port number.
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

    /// Fall back to mock provider if verbs initialization fails.
    #[arg(long)]
    fallback_mock: bool,

    /// Enable debug logging.
    #[arg(long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing(args.debug);

    let config = VolumeEngineConfig {
        socket_path: args.socket.clone(),
        device: mock_device_name(&args.device),
        port: args.port,
        qpn: args.qpn,
        psn: args.psn,
        lid: args.lid,
        gid_index: args.gid_index,
        gid: args.gid.clone(),
        link_layer: LINK_INFINIBAND,
        mock_base_addr: args.mock_base_addr,
        mock_addr_stride: args.mock_addr_stride,
        mock_rkey: args.mock_rkey,
    };
    let provider = build_provider(&args, config)?;

    info!(
        "starting volume RDMA engine on {} with {:?} provider",
        args.socket, args.provider
    );
    let engine = Arc::new(VolumeNativeEngine::with_provider(
        args.socket.clone(),
        provider,
    ));
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

fn build_provider(
    args: &Args,
    mock_config: VolumeEngineConfig,
) -> anyhow::Result<Arc<dyn VolumeRdmaProvider>> {
    match args.provider {
        ProviderMode::Mock => {
            warn!("volume-rdma-engine mock provider validates IPC flow, not hardware RDMA performance");
            Ok(Arc::new(MockVolumeRdmaProvider::new(mock_config)))
        }
        ProviderMode::Verbs => {
            let port = u8::try_from(args.port)
                .map_err(|_| anyhow::anyhow!("verbs port must fit in u8: {}", args.port))?;
            let provider = RealVerbsVolumeRdmaProvider::new(VolumeVerbsConfig {
                device: args.device.clone(),
                port,
                gid_index: args.gid_index as libc::c_int,
                psn: args.psn,
                ..VolumeVerbsConfig::default()
            });
            match provider {
                Ok(provider) => Ok(Arc::new(provider)),
                Err(err) if args.fallback_mock => {
                    warn!("verbs provider initialization failed, falling back to mock: {err:#}");
                    Ok(Arc::new(MockVolumeRdmaProvider::new(mock_config)))
                }
                Err(err) => Err(anyhow::anyhow!(
                    "verbs provider initialization failed; use --fallback-mock only for IPC tests: {err:#}"
                )),
            }
        }
    }
}

fn mock_device_name(raw: &str) -> String {
    if raw.trim().is_empty() || raw.trim() == "auto" {
        "mock-volume-rdma".to_string()
    } else {
        raw.to_string()
    }
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

    #[test]
    fn parses_provider_verbs() {
        let args = Args::try_parse_from(["volume-rdma-engine", "--provider", "verbs"]).unwrap();
        assert_eq!(args.provider, ProviderMode::Verbs);
    }

    #[test]
    fn auto_device_maps_to_mock_name_for_mock_provider() {
        assert_eq!(mock_device_name("auto"), "mock-volume-rdma");
        assert_eq!(mock_device_name("mlx5_0"), "mlx5_0");
    }
}
