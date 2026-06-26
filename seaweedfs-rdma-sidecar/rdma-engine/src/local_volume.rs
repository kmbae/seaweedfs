//! Minimal read-only SeaweedFS volume reader for the RDMA engine.
//!
//! This keeps the remote read fast path inside the Rust engine:
//! `.idx/.dat -> registered buffer -> RDMA PUT`, without bouncing through the
//! colocated Go sidecar's HTTP `/local-volume` endpoint. It intentionally only
//! handles plain local volume files. Higher-level SeaweedFS features such as
//! chunk manifests and compressed needles remain on the existing HTTP fallback.

use crate::{RdmaError, RdmaResult};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

const SUPER_BLOCK_SIZE: usize = 8;
const NEEDLE_ID_SIZE: usize = 8;
const OFFSET_SIZE_5_BYTES: usize = 5;
const SIZE_SIZE: usize = 4;
const NEEDLE_MAP_ENTRY_SIZE_5_BYTES: usize = NEEDLE_ID_SIZE + OFFSET_SIZE_5_BYTES + SIZE_SIZE;
const NEEDLE_HEADER_SIZE: usize = 16;
const DATA_SIZE_SIZE: usize = 4;
const NEEDLE_CHECKSUM_SIZE: usize = 4;
const TIMESTAMP_SIZE: usize = 8;
const NEEDLE_PADDING_SIZE: i32 = 8;

const VERSION_1: u8 = 1;
const VERSION_2: u8 = 2;
const VERSION_3: u8 = 3;

const FLAG_IS_COMPRESSED: u8 = 0x01;
const FLAG_IS_CHUNK_MANIFEST: u8 = 0x80;

/// Read-only direct reader for local SeaweedFS `.dat/.idx` files.
pub struct LocalVolumeReader {
    data_dir: PathBuf,
    idx_dir: PathBuf,
    collection: String,
    volumes: Mutex<HashMap<u32, Arc<VolumeIndex>>>,
}

#[derive(Debug)]
struct VolumeIndex {
    dat_path: PathBuf,
    idx_path: PathBuf,
    idx_meta: IndexMeta,
    version: u8,
    entries: HashMap<u64, IndexEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IndexEntry {
    offset: u64,
    size: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexMeta {
    len: u64,
    modified: Option<SystemTime>,
}

impl LocalVolumeReader {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        idx_dir: impl Into<PathBuf>,
        collection: impl Into<String>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            idx_dir: idx_dir.into(),
            collection: collection.into(),
            volumes: Mutex::new(HashMap::new()),
        }
    }

    pub fn from_env() -> Option<Self> {
        let data_dir = std::env::var("VOLUME_DATA_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())?;
        let idx_dir = std::env::var("VOLUME_IDX_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| data_dir.clone());
        let collection = std::env::var("VOLUME_COLLECTION").unwrap_or_default();
        Some(Self::new(data_dir, idx_dir, collection))
    }

    pub fn read_needle(
        &self,
        volume_id: u32,
        needle_id: u64,
        cookie: u32,
        offset: u64,
        size: u64,
    ) -> RdmaResult<Vec<u8>> {
        let index = self.get_volume_index(volume_id)?;
        match index.read_needle(needle_id, cookie, offset, size) {
            Ok(data) => Ok(data),
            Err(err) if index.idx_changed() => {
                self.invalidate(volume_id);
                let index = self.get_volume_index(volume_id)?;
                index
                    .read_needle(needle_id, cookie, offset, size)
                    .map_err(|e| e.with_context(err))
            }
            Err(err) => Err(err.into()),
        }
    }

    fn get_volume_index(&self, volume_id: u32) -> RdmaResult<Arc<VolumeIndex>> {
        {
            let volumes = self.volumes.lock();
            if let Some(index) = volumes.get(&volume_id) {
                if !index.idx_changed() {
                    return Ok(index.clone());
                }
            }
        }

        let index = Arc::new(VolumeIndex::load(
            self.volume_base(&self.data_dir, volume_id),
            self.volume_base(&self.idx_dir, volume_id),
        )?);
        self.volumes.lock().insert(volume_id, index.clone());
        Ok(index)
    }

    fn invalidate(&self, volume_id: u32) {
        self.volumes.lock().remove(&volume_id);
    }

    fn volume_base(&self, dir: &Path, volume_id: u32) -> PathBuf {
        let file_name = if self.collection.is_empty() {
            volume_id.to_string()
        } else {
            format!("{}_{}", self.collection, volume_id)
        };
        dir.join(file_name)
    }
}

impl VolumeIndex {
    fn load(dat_base: PathBuf, idx_base: PathBuf) -> RdmaResult<Self> {
        let dat_path = dat_base.with_extension("dat");
        let idx_path = idx_base.with_extension("idx");
        let version = read_volume_version(&dat_path)?;
        let idx_meta = IndexMeta::from_path(&idx_path)?;
        let entries = load_index_entries(&idx_path)?;
        Ok(Self {
            dat_path,
            idx_path,
            idx_meta,
            version,
            entries,
        })
    }

    fn idx_changed(&self) -> bool {
        IndexMeta::from_path(&self.idx_path)
            .map(|meta| meta != self.idx_meta)
            .unwrap_or(true)
    }

    fn read_needle(
        &self,
        needle_id: u64,
        cookie: u32,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, LocalVolumeError> {
        let entry = self
            .entries
            .get(&needle_id)
            .copied()
            .ok_or(LocalVolumeError::NotFound)?;
        if entry.offset == 0 || entry.size <= 0 {
            return Err(LocalVolumeError::NotFound);
        }

        let dat = File::open(&self.dat_path)?;
        let header = read_exact_at(&dat, entry.offset, NEEDLE_HEADER_SIZE)?;
        let found_cookie = be_u32(&header[0..4]);
        let found_id = be_u64(&header[4..12]);
        let header_size = be_u32(&header[12..16]) as i32;
        if found_id != needle_id {
            return Err(LocalVolumeError::Corrupt(format!(
                "needle id mismatch at offset {}: got {}, want {}",
                entry.offset, found_id, needle_id
            )));
        }
        if found_cookie != cookie {
            return Err(LocalVolumeError::Corrupt(format!(
                "cookie mismatch for needle {}: got {:#x}, want {:#x}",
                needle_id, found_cookie, cookie
            )));
        }
        if header_size != entry.size {
            return Err(LocalVolumeError::Corrupt(format!(
                "needle size mismatch for {}: header {}, index {}",
                needle_id, header_size, entry.size
            )));
        }

        let body_len = needle_body_length(entry.size, self.version)?;
        let body = read_exact_at(
            &dat,
            entry.offset + NEEDLE_HEADER_SIZE as u64,
            body_len as usize,
        )?;
        let data = extract_plain_data(&body, entry.size, self.version)?;

        let start = offset as usize;
        if start >= data.len() {
            return Ok(Vec::new());
        }
        let end = if size == 0 {
            data.len()
        } else {
            start.saturating_add(size as usize).min(data.len())
        };
        Ok(data[start..end].to_vec())
    }
}

impl IndexMeta {
    fn from_path(path: &Path) -> RdmaResult<Self> {
        let meta = fs::metadata(path)?;
        Ok(Self {
            len: meta.len(),
            modified: meta.modified().ok(),
        })
    }
}

#[derive(Debug)]
enum LocalVolumeError {
    Io(io::Error),
    NotFound,
    UnsupportedVersion(u8),
    UnsupportedNeedle(String),
    Corrupt(String),
}

impl LocalVolumeError {
    fn with_context(self, first: LocalVolumeError) -> RdmaError {
        RdmaError::internal(format!(
            "local volume reload failed after {:?}: {:?}",
            first, self
        ))
    }
}

impl From<io::Error> for LocalVolumeError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<LocalVolumeError> for RdmaError {
    fn from(value: LocalVolumeError) -> Self {
        match value {
            LocalVolumeError::Io(err) => RdmaError::Io(err),
            LocalVolumeError::NotFound => {
                RdmaError::operation_failed("local_volume_not_found", 404)
            }
            LocalVolumeError::UnsupportedVersion(version) => RdmaError::invalid_request(format!(
                "unsupported SeaweedFS volume version {}",
                version
            )),
            LocalVolumeError::UnsupportedNeedle(reason) => RdmaError::invalid_request(reason),
            LocalVolumeError::Corrupt(reason) => RdmaError::internal(reason),
        }
    }
}

fn read_volume_version(dat_path: &Path) -> RdmaResult<u8> {
    let dat = File::open(dat_path)?;
    let header = read_exact_at(&dat, 0, SUPER_BLOCK_SIZE)?;
    let version = header[0];
    if !(VERSION_1..=VERSION_3).contains(&version) {
        return Err(RdmaError::invalid_request(format!(
            "unsupported SeaweedFS volume version {} in {}",
            version,
            dat_path.display()
        )));
    }
    Ok(version)
}

fn load_index_entries(path: &Path) -> RdmaResult<HashMap<u64, IndexEntry>> {
    let data = fs::read(path)?;
    let mut entries = HashMap::with_capacity(data.len() / NEEDLE_MAP_ENTRY_SIZE_5_BYTES);
    for chunk in data.chunks_exact(NEEDLE_MAP_ENTRY_SIZE_5_BYTES) {
        let needle_id = be_u64(&chunk[..8]);
        let offset = offset_5_to_actual(&chunk[8..13]);
        let size = be_i32(&chunk[13..17]);
        if size < 0 {
            entries.remove(&needle_id);
        } else {
            entries.insert(needle_id, IndexEntry { offset, size });
        }
    }
    Ok(entries)
}

fn extract_plain_data(
    body: &[u8],
    needle_size: i32,
    version: u8,
) -> Result<Vec<u8>, LocalVolumeError> {
    if needle_size < 0 {
        return Err(LocalVolumeError::NotFound);
    }
    let needle_size = needle_size as usize;
    if body.len() < needle_size + NEEDLE_CHECKSUM_SIZE {
        return Err(LocalVolumeError::Corrupt(
            "needle body too short".to_string(),
        ));
    }

    let data = match version {
        VERSION_1 => body[..needle_size].to_vec(),
        VERSION_2 | VERSION_3 => {
            if needle_size == 0 {
                Vec::new()
            } else {
                if needle_size < DATA_SIZE_SIZE + 1 {
                    return Err(LocalVolumeError::Corrupt(
                        "v2/v3 needle body too short".to_string(),
                    ));
                }
                let data_size = be_u32(&body[..DATA_SIZE_SIZE]) as usize;
                let data_start = DATA_SIZE_SIZE;
                let data_end = data_start + data_size;
                if data_end > needle_size {
                    return Err(LocalVolumeError::Corrupt(
                        "needle data_size exceeds needle body".to_string(),
                    ));
                }
                let flags = body
                    .get(data_end)
                    .copied()
                    .ok_or_else(|| LocalVolumeError::Corrupt("needle flags missing".to_string()))?;
                if flags & FLAG_IS_COMPRESSED != 0 {
                    return Err(LocalVolumeError::UnsupportedNeedle(
                        "compressed needle requires SeaweedFS fallback".to_string(),
                    ));
                }
                if flags & FLAG_IS_CHUNK_MANIFEST != 0 {
                    return Err(LocalVolumeError::UnsupportedNeedle(
                        "chunk manifest requires SeaweedFS fallback".to_string(),
                    ));
                }
                body[data_start..data_end].to_vec()
            }
        }
        other => return Err(LocalVolumeError::UnsupportedVersion(other)),
    };

    let checksum_offset = needle_size;
    let expected = be_u32(&body[checksum_offset..checksum_offset + NEEDLE_CHECKSUM_SIZE]);
    let actual = crc32c(&data);
    if expected != actual && expected != legacy_crc_value(actual) {
        return Err(LocalVolumeError::Corrupt(format!(
            "invalid CRC for needle data: got {actual:08x}, want {expected:08x}"
        )));
    }
    Ok(data)
}

fn needle_body_length(size: i32, version: u8) -> Result<u64, LocalVolumeError> {
    if size < 0 {
        return Err(LocalVolumeError::NotFound);
    }
    let size = size as i64;
    let footer = if version == VERSION_3 {
        NEEDLE_CHECKSUM_SIZE as i64 + TIMESTAMP_SIZE as i64
    } else {
        NEEDLE_CHECKSUM_SIZE as i64
    };
    let padding = padding_length(size as i32, version) as i64;
    Ok((size + footer + padding) as u64)
}

fn padding_length(size: i32, version: u8) -> i32 {
    let footer = if version == VERSION_3 {
        NEEDLE_CHECKSUM_SIZE as i32 + TIMESTAMP_SIZE as i32
    } else {
        NEEDLE_CHECKSUM_SIZE as i32
    };
    let used = NEEDLE_HEADER_SIZE as i32 + size + footer;
    NEEDLE_PADDING_SIZE - (used % NEEDLE_PADDING_SIZE)
}

fn read_exact_at(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    read_exact_at_into(file, &mut buf, offset)?;
    Ok(buf)
}

#[cfg(unix)]
fn read_exact_at_into(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut read = 0usize;
    while read < buf.len() {
        let n = file.read_at(&mut buf[read..], offset + read as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF in read_at",
            ));
        }
        read += n;
    }
    Ok(())
}

#[cfg(unix)]
#[cfg(test)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.write_at(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write all bytes with write_at",
            ));
        }
        written += n;
    }
    Ok(())
}

fn be_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("u64 byte slice"))
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("u32 byte slice"))
}

fn be_i32(bytes: &[u8]) -> i32 {
    i32::from_be_bytes(bytes.try_into().expect("i32 byte slice"))
}

fn offset_5_to_actual(bytes: &[u8]) -> u64 {
    let compact = bytes[3] as u64
        | (bytes[2] as u64) << 8
        | (bytes[1] as u64) << 16
        | (bytes[0] as u64) << 24
        | (bytes[4] as u64) << 32;
    compact * NEEDLE_PADDING_SIZE as u64
}

#[cfg(test)]
fn actual_to_offset_5(actual: u64) -> [u8; 5] {
    let compact = actual / NEEDLE_PADDING_SIZE as u64;
    [
        (compact >> 24) as u8,
        (compact >> 16) as u8,
        (compact >> 8) as u8,
        compact as u8,
        (compact >> 32) as u8,
    ]
}

static CRC32C_TABLE: LazyLock<[u32; 256]> = LazyLock::new(|| {
    let mut table = [0u32; 256];
    for (i, slot) in table.iter_mut().enumerate() {
        let mut crc = i as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82f6_3b78;
            } else {
                crc >>= 1;
            }
        }
        *slot = crc;
    }
    table
});

fn crc32c(data: &[u8]) -> u32 {
    let mut crc = 0u32;
    for b in data {
        let idx = ((crc ^ *b as u32) & 0xff) as usize;
        crc = CRC32C_TABLE[idx] ^ (crc >> 8);
    }
    crc
}

fn legacy_crc_value(raw: u32) -> u32 {
    raw.rotate_right(15).wrapping_add(0xa282_ead8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn reads_v3_local_needle_range() {
        let dir = tempdir().unwrap();
        let data = b"hello direct rdma local volume";
        write_test_volume(dir.path(), 7, 42, 0x1122_3344, data, 3);

        let reader = LocalVolumeReader::new(dir.path(), dir.path(), "");
        let got = reader.read_needle(7, 42, 0x1122_3344, 6, 11).unwrap();
        assert_eq!(got, b"direct rdma");
    }

    #[test]
    fn reloads_index_when_idx_changes() {
        let dir = tempdir().unwrap();
        write_test_volume(dir.path(), 7, 42, 0x1122_3344, b"first", 3);
        let reader = LocalVolumeReader::new(dir.path(), dir.path(), "");
        assert_eq!(
            reader.read_needle(7, 42, 0x1122_3344, 0, 0).unwrap(),
            b"first"
        );

        append_test_needle(dir.path(), 7, 42, 0x1122_3344, b"second", 3);
        assert_eq!(
            reader.read_needle(7, 42, 0x1122_3344, 0, 0).unwrap(),
            b"second"
        );
    }

    #[test]
    fn rejects_bad_crc() {
        let dir = tempdir().unwrap();
        write_test_volume(dir.path(), 7, 42, 0x1122_3344, b"hello", 3);
        let dat_path = dir.path().join("7.dat");
        let dat = std::fs::OpenOptions::new()
            .write(true)
            .open(dat_path)
            .unwrap();
        write_all_at(
            &dat,
            &[b'X'],
            SUPER_BLOCK_SIZE as u64 + NEEDLE_HEADER_SIZE as u64 + DATA_SIZE_SIZE as u64,
        )
        .unwrap();

        let reader = LocalVolumeReader::new(dir.path(), dir.path(), "");
        let err = reader.read_needle(7, 42, 0x1122_3344, 0, 0).unwrap_err();
        assert!(format!("{err}").contains("invalid CRC"));
    }

    fn write_test_volume(
        dir: &Path,
        volume_id: u32,
        needle_id: u64,
        cookie: u32,
        data: &[u8],
        version: u8,
    ) {
        let dat_path = dir.join(format!("{volume_id}.dat"));
        let idx_path = dir.join(format!("{volume_id}.idx"));
        let mut dat = File::create(&dat_path).unwrap();
        dat.write_all(&[version, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        drop(dat);
        File::create(&idx_path).unwrap();
        append_test_needle(dir, volume_id, needle_id, cookie, data, version);
    }

    fn append_test_needle(
        dir: &Path,
        volume_id: u32,
        needle_id: u64,
        cookie: u32,
        data: &[u8],
        version: u8,
    ) {
        let dat_path = dir.join(format!("{volume_id}.dat"));
        let idx_path = dir.join(format!("{volume_id}.idx"));
        let mut dat = std::fs::OpenOptions::new()
            .append(true)
            .read(true)
            .open(&dat_path)
            .unwrap();
        let offset = dat.metadata().unwrap().len();
        let blob = encode_v3_needle(needle_id, cookie, data, version);
        dat.write_all(&blob).unwrap();

        let mut idx = std::fs::OpenOptions::new()
            .append(true)
            .open(&idx_path)
            .unwrap();
        idx.write_all(&needle_id.to_be_bytes()).unwrap();
        idx.write_all(&actual_to_offset_5(offset)).unwrap();
        let size = (DATA_SIZE_SIZE + data.len() + 1) as i32;
        idx.write_all(&size.to_be_bytes()).unwrap();
    }

    fn encode_v3_needle(needle_id: u64, cookie: u32, data: &[u8], version: u8) -> Vec<u8> {
        let size = (DATA_SIZE_SIZE + data.len() + 1) as i32;
        let mut out = Vec::new();
        out.extend_from_slice(&cookie.to_be_bytes());
        out.extend_from_slice(&needle_id.to_be_bytes());
        out.extend_from_slice(&size.to_be_bytes());
        out.extend_from_slice(&(data.len() as u32).to_be_bytes());
        out.extend_from_slice(data);
        out.push(0);
        out.extend_from_slice(&crc32c(data).to_be_bytes());
        if version == VERSION_3 {
            out.extend_from_slice(&123u64.to_be_bytes());
        }
        let padding = padding_length(size, version) as usize;
        out.extend(std::iter::repeat(0).take(padding));
        out
    }
}
