use crate::{
    local_volume::LocalVolumeReader,
    needle_blob::encode_payload_blob,
    network::{format_seaweed_file_id, RemoteNeedleReadRequest, RemoteNeedleWriteRequest},
    RdmaError, RdmaResult,
};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

pub mod volume_server_pb {
    tonic::include_proto!("volume_server_pb");
}

use volume_server_pb::{
    volume_server_client::VolumeServerClient, ReadNeedleRangeRequest, WriteNeedleBlobRequest,
};

pub async fn read_needle_range(
    volume_server_url: &str,
    req: &RemoteNeedleReadRequest,
) -> RdmaResult<Vec<u8>> {
    let endpoint = grpc_endpoint(volume_server_url)?;
    let channel = connect(endpoint).await?;
    let mut client = VolumeServerClient::new(channel);
    let mut data = client
        .read_needle_range(tonic::Request::new(ReadNeedleRangeRequest {
            volume_id: req.volume_id,
            needle_id: req.needle_id,
            cookie: req.cookie,
            offset: req.offset,
            size: req.size,
        }))
        .await
        .map_err(|e| RdmaError::ipc_error(format!("volume ReadNeedleRange gRPC: {}", e)))?
        .into_inner()
        .data;
    if req.size > 0 && data.len() > req.size as usize {
        data.truncate(req.size as usize);
    }
    Ok(data)
}

pub async fn write_needle_blob(
    volume_server_url: &str,
    local_reader: Option<&LocalVolumeReader>,
    req: &RemoteNeedleWriteRequest,
    data: Vec<u8>,
) -> RdmaResult<String> {
    let version = match local_reader {
        Some(reader) => reader.volume_version(req.volume_id)?,
        None => 3,
    };
    let encoded = encode_payload_blob(req.needle_id, req.cookie, &data, version)?;
    let endpoint = grpc_endpoint(volume_server_url)?;
    let channel = connect(endpoint).await?;
    let mut client = VolumeServerClient::new(channel);
    client
        .write_needle_blob(tonic::Request::new(WriteNeedleBlobRequest {
            volume_id: req.volume_id,
            needle_id: req.needle_id,
            size: encoded.size,
            needle_blob: encoded.blob,
        }))
        .await
        .map_err(|e| RdmaError::ipc_error(format!("volume WriteNeedleBlob gRPC: {}", e)))?;

    Ok(format_seaweed_file_id(req.volume_id, req.needle_id, req.cookie))
}

async fn connect(endpoint: String) -> RdmaResult<Channel> {
    Endpoint::from_shared(endpoint)
        .map_err(|e| RdmaError::invalid_request(format!("invalid volume gRPC endpoint: {}", e)))?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .map_err(|e| RdmaError::ipc_error(format!("connect volume gRPC: {}", e)))
}

fn grpc_endpoint(volume_server_url: &str) -> RdmaResult<String> {
    let value = std::env::var("VOLUME_SERVER_GRPC_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| volume_server_url.to_string());
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        let without_scheme = trimmed
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
        let grpc_authority = grpc_authority_from_http(authority)?;
        return Ok(format!("http://{}", grpc_authority));
    }
    if trimmed.contains("://") {
        return Err(RdmaError::invalid_request(format!(
            "unsupported volume gRPC URL {}",
            trimmed
        )));
    }
    Ok(format!("http://{}", trimmed))
}

fn grpc_authority_from_http(authority: &str) -> RdmaResult<String> {
    if authority.is_empty() {
        return Err(RdmaError::invalid_request("empty volume server URL"));
    }
    if authority.starts_with('[') {
        let Some(end) = authority.rfind("]:") else {
            return Ok(authority.to_string());
        };
        let host = &authority[..=end];
        let port = &authority[end + 2..];
        return Ok(format!("{}:{}", host, grpc_port(port)?));
    }
    let Some((host, port)) = authority.rsplit_once(':') else {
        return Ok(authority.to_string());
    };
    Ok(format!("{}:{}", host, grpc_port(port)?))
}

fn grpc_port(port: &str) -> RdmaResult<u16> {
    if let Some((_, grpc_port)) = port.rsplit_once('.') {
        return grpc_port
            .parse::<u16>()
            .map_err(|e| RdmaError::invalid_request(format!("invalid gRPC port: {}", e)));
    }
    let http_port = port
        .parse::<u16>()
        .map_err(|e| RdmaError::invalid_request(format!("invalid HTTP port: {}", e)))?;
    http_port
        .checked_add(10_000)
        .ok_or_else(|| RdmaError::invalid_request("gRPC port overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_grpc_endpoint_from_http_url() {
        assert_eq!(
            grpc_endpoint("http://127.0.0.1:8444/local-volume").unwrap(),
            "http://127.0.0.1:18444"
        );
        assert_eq!(
            grpc_endpoint("http://volume:8080.18080").unwrap(),
            "http://volume:18080"
        );
        assert_eq!(
            grpc_endpoint("http://[2001:db8::1]:8080").unwrap(),
            "http://[2001:db8::1]:18080"
        );
    }
}
