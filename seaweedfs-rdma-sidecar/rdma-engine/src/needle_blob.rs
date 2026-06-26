use crate::{local_volume::crc32c, RdmaError, RdmaResult};
use std::time::{SystemTime, UNIX_EPOCH};

const NEEDLE_HEADER_SIZE: usize = 16;
const DATA_SIZE_SIZE: usize = 4;
const NEEDLE_CHECKSUM_SIZE: usize = 4;
const TIMESTAMP_SIZE: usize = 8;
const NEEDLE_PADDING_SIZE: i32 = 8;
const VERSION_3: u8 = 3;
const FLAG_HAS_LAST_MODIFIED_DATE: u8 = 0x08;
const LAST_MODIFIED_BYTES_LENGTH: usize = 5;

#[derive(Debug, Clone)]
pub struct EncodedNeedleBlob {
    pub blob: Vec<u8>,
    pub size: i32,
}

pub fn encode_payload_blob(
    needle_id: u64,
    cookie: u32,
    data: &[u8],
    version: u8,
) -> RdmaResult<EncodedNeedleBlob> {
    if data.is_empty() {
        return Err(RdmaError::invalid_request("empty write payload"));
    }
    if version != VERSION_3 {
        return Err(RdmaError::invalid_request(format!(
            "unsupported direct write volume version {}",
            version
        )));
    }

    let data_len = i32::try_from(data.len())
        .map_err(|_| RdmaError::invalid_request("write payload too large"))?;
    let size = DATA_SIZE_SIZE as i32 + data_len + 1 + LAST_MODIFIED_BYTES_LENGTH as i32;
    let mut blob = Vec::with_capacity(actual_size(size, version));
    blob.extend_from_slice(&cookie.to_be_bytes());
    blob.extend_from_slice(&needle_id.to_be_bytes());
    blob.extend_from_slice(&size.to_be_bytes());
    blob.extend_from_slice(&(data.len() as u32).to_be_bytes());
    blob.extend_from_slice(data);
    blob.push(FLAG_HAS_LAST_MODIFIED_DATE);
    blob.extend_from_slice(&last_modified_seconds()[3..8]);
    blob.extend_from_slice(&crc32c(data).to_be_bytes());
    blob.extend_from_slice(&0u64.to_be_bytes());
    let padding = padding_length(size, version) as usize;
    blob.extend(std::iter::repeat(0).take(padding));

    Ok(EncodedNeedleBlob { blob, size })
}

fn last_modified_seconds() -> [u8; 8] {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    seconds.to_be_bytes()
}

fn actual_size(size: i32, version: u8) -> usize {
    NEEDLE_HEADER_SIZE
        + size as usize
        + NEEDLE_CHECKSUM_SIZE
        + TIMESTAMP_SIZE
        + padding_length(size, version) as usize
}

fn padding_length(size: i32, version: u8) -> i32 {
    if version == VERSION_3 {
        NEEDLE_PADDING_SIZE
            - ((NEEDLE_HEADER_SIZE as i32
                + size
                + NEEDLE_CHECKSUM_SIZE as i32
                + TIMESTAMP_SIZE as i32)
                % NEEDLE_PADDING_SIZE)
    } else {
        NEEDLE_PADDING_SIZE
            - ((NEEDLE_HEADER_SIZE as i32 + size + NEEDLE_CHECKSUM_SIZE as i32)
                % NEEDLE_PADDING_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_v3_payload_blob_shape() {
        let encoded = encode_payload_blob(0x0102, 0x1122_3344, b"payload", VERSION_3).unwrap();
        assert_eq!(encoded.size, 17);
        assert_eq!(&encoded.blob[0..4], &0x1122_3344u32.to_be_bytes());
        assert_eq!(&encoded.blob[4..12], &0x0102u64.to_be_bytes());
        assert_eq!(&encoded.blob[12..16], &17i32.to_be_bytes());
        assert_eq!(&encoded.blob[16..20], &7u32.to_be_bytes());
        assert_eq!(&encoded.blob[20..27], b"payload");
        assert_eq!(encoded.blob[27], FLAG_HAS_LAST_MODIFIED_DATE);
        assert_eq!(encoded.blob.len(), actual_size(encoded.size, VERSION_3));
    }

    #[test]
    fn rejects_unsupported_versions() {
        let err = encode_payload_blob(1, 2, b"payload", 2).unwrap_err();
        assert!(err.to_string().contains("unsupported direct write volume version"));
    }
}
