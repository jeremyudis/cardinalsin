//! Write-ahead log (WAL) implementation for the ingester.

use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use crc32fast::Hasher;
use std::fs::File as StdFile;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::warn;

const MAGIC: &[u8; 4] = b"CSWA";
const VERSION: u8 = 1;
const FLAG_COMPRESSED: u8 = 0x01;
const HEADER_LEN: usize = 22;
const SEGMENT_PREFIX: &str = "segment-";
const SEGMENT_SUFFIX: &str = ".wal";

/// WAL configuration.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory for WAL segment files.
    pub wal_dir: PathBuf,
    /// Maximum segment size before rotation.
    pub max_segment_size: usize,
    /// fsync strategy.
    pub sync_mode: WalSyncMode,
    /// Enable or disable WAL.
    pub enabled: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_dir: PathBuf::from("/var/lib/cardinalsin/wal"),
            max_segment_size: 64 * 1024 * 1024,
            sync_mode: WalSyncMode::Interval(Duration::from_millis(100)),
            enabled: true,
        }
    }
}

/// WAL fsync strategy.
#[derive(Debug, Clone, Copy)]
pub enum WalSyncMode {
    /// fsync after every write batch.
    EveryWrite,
    /// fsync on an interval.
    Interval(Duration),
    /// fsync only on segment rotation.
    OnRotation,
    /// No fsync.
    None,
}

impl WalSyncMode {
    /// Parse from a string. Accepts:
    /// - "every_write"
    /// - "interval_100ms", "interval_1s", "interval_500ms"
    /// - "on_rotation"
    /// - "none"
    pub fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_lowercase().as_str() {
            "every_write" => Ok(Self::EveryWrite),
            "on_rotation" => Ok(Self::OnRotation),
            "none" => Ok(Self::None),
            s if s.starts_with("interval_") => {
                let suffix = &s["interval_".len()..];
                let millis = if let Some(ms) = suffix.strip_suffix("ms") {
                    ms.parse::<u64>().map_err(|e| format!("invalid interval: {e}"))?
                } else if let Some(secs) = suffix.strip_suffix('s') {
                    secs.parse::<u64>().map_err(|e| format!("invalid interval: {e}"))? * 1000
                } else {
                    return Err(format!("invalid sync mode: {s} (use e.g. interval_100ms)"));
                };
                Ok(Self::Interval(Duration::from_millis(millis)))
            }
            other => Err(format!(
                "unknown WAL sync mode: '{other}'. Use: every_write, interval_100ms, on_rotation, none"
            )),
        }
    }
}

/// WAL entry metadata.
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Sequence number.
    pub seq: u64,
    /// Header flags.
    pub flags: u8,
    /// Serialized payload.
    pub payload: Vec<u8>,
}

impl WalEntry {
    /// Decode payload into record batches.
    pub fn batches(&self) -> Result<Vec<RecordBatch>> {
        let cursor = io::Cursor::new(self.payload.clone());
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|err| Error::Serialization(err.to_string()))?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|err| Error::Serialization(err.to_string()))?);
        }
        Ok(batches)
    }
}

/// Write-ahead log with segmented files.
pub struct WriteAheadLog {
    config: WalConfig,
    current_segment_id: u64,
    current_size: u64,
    current_path: PathBuf,
    file: tokio::fs::File,
    next_seq: u64,
    last_sync: Instant,
}

impl WriteAheadLog {
    /// Open or create a WAL at the configured directory.
    pub async fn open(config: WalConfig) -> Result<Self> {
        fs::create_dir_all(&config.wal_dir)
            .await
            .map_err(map_io_error)?;
        let segments = list_segments(&config.wal_dir)?;
        let (segment_id, segment_path) = if let Some(last) = segments.last() {
            (last.id, last.path.clone())
        } else {
            let id = 1;
            (id, config.wal_dir.join(segment_file_name(id)))
        };

        let file = open_segment(&segment_path).await?;
        let current_size = file.metadata().await.map_err(map_io_error)?.len();
        let next_seq = if current_size == 0 {
            1
        } else {
            last_sequence_for_segment(&segment_path)?.unwrap_or(0) + 1
        };

        Ok(Self {
            config,
            current_segment_id: segment_id,
            current_size,
            current_path: segment_path,
            file,
            next_seq,
            last_sync: Instant::now(),
        })
    }

    /// Append a record batch to the WAL.
    pub async fn append(&mut self, batch: &RecordBatch) -> Result<u64> {
        let payload = encode_record_batch(batch)?;
        self.append_payload(payload).await
    }

    /// Read all entries across segments.
    pub fn read_entries(&self) -> Result<Vec<WalEntry>> {
        let segments = list_segments(&self.config.wal_dir)?;
        let mut entries = Vec::new();
        for segment in segments {
            let segment_entries = read_entries_from_path(&segment.path)?;
            entries.extend(segment_entries);
        }
        Ok(entries)
    }

    /// Read entries with sequence number strictly greater than `after_seq`.
    pub fn read_entries_after(&self, after_seq: u64) -> Result<Vec<WalEntry>> {
        let segments = list_segments(&self.config.wal_dir)?;
        let mut entries = Vec::new();
        for segment in segments {
            let segment_entries = read_entries_from_path(&segment.path)?;
            for entry in segment_entries {
                if entry.seq > after_seq {
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    /// Get the current next sequence number (the seq that will be assigned to the next append).
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Truncate segments whose max sequence is strictly before `seq`.
    pub async fn truncate_before(&mut self, seq: u64) -> Result<()> {
        let segments = list_segments(&self.config.wal_dir)?;
        for segment in segments {
            if segment.id >= self.current_segment_id {
                break;
            }
            if let Some(last_seq) = last_sequence_for_segment(&segment.path)? {
                if last_seq < seq {
                    fs::remove_file(&segment.path).await.map_err(map_io_error)?;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn append_payload(&mut self, payload: Vec<u8>) -> Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let header = encode_header(seq, 0, &payload);
        let entry_size = (header.len() + payload.len()) as u64;

        if self.config.max_segment_size > 0
            && self.current_size + entry_size > self.config.max_segment_size as u64
        {
            self.rotate().await?;
        }

        self.file.write_all(&header).await.map_err(map_io_error)?;
        self.file.write_all(&payload).await.map_err(map_io_error)?;
        self.current_size += entry_size;

        match self.config.sync_mode {
            WalSyncMode::EveryWrite => {
                self.sync_current().await?;
            }
            WalSyncMode::Interval(interval) => {
                if interval.is_zero() || self.last_sync.elapsed() >= interval {
                    self.sync_current().await?;
                }
            }
            WalSyncMode::OnRotation | WalSyncMode::None => {}
        }

        Ok(seq)
    }

    async fn rotate(&mut self) -> Result<()> {
        if matches!(self.config.sync_mode, WalSyncMode::OnRotation) {
            self.sync_current().await?;
        }

        self.current_segment_id += 1;
        self.current_path = self
            .config
            .wal_dir
            .join(segment_file_name(self.current_segment_id));
        self.file = open_segment(&self.current_path).await?;
        self.current_size = 0;
        Ok(())
    }

    async fn sync_current(&mut self) -> Result<()> {
        self.file.sync_data().await.map_err(map_io_error)?;
        self.last_sync = Instant::now();
        Ok(())
    }
}

fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let schema = batch.schema();
    let mut writer = StreamWriter::try_new(&mut buffer, &schema)
        .map_err(|err| Error::Serialization(err.to_string()))?;
    writer
        .write(batch)
        .map_err(|err| Error::Serialization(err.to_string()))?;
    writer
        .finish()
        .map_err(|err| Error::Serialization(err.to_string()))?;
    Ok(buffer)
}

fn encode_header(seq: u64, flags: u8, payload: &[u8]) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(MAGIC);
    header[4] = VERSION;
    header[5] = flags;
    header[6..14].copy_from_slice(&seq.to_le_bytes());
    header[14..18].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    let mut hasher = Hasher::new();
    hasher.update(payload);
    header[18..22].copy_from_slice(&hasher.finalize().to_le_bytes());
    header
}

fn decode_header(header: &[u8; HEADER_LEN]) -> Result<(u64, u8, usize, u32)> {
    if &header[0..4] != MAGIC {
        return Err(Error::Serialization("Invalid WAL magic".to_string()));
    }
    if header[4] != VERSION {
        return Err(Error::Serialization("Unsupported WAL version".to_string()));
    }
    let flags = header[5];
    if flags & FLAG_COMPRESSED != 0 {
        return Err(Error::Serialization(
            "WAL compression not supported".to_string(),
        ));
    }
    let seq = u64::from_le_bytes(header[6..14].try_into().unwrap());
    let len = u32::from_le_bytes(header[14..18].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(header[18..22].try_into().unwrap());
    Ok((seq, flags, len, crc))
}

fn read_entries_from_path(path: &Path) -> Result<Vec<WalEntry>> {
    let file = StdFile::open(path).map_err(map_io_error)?;
    let mut reader = BufReader::new(file);
    let mut entries = Vec::new();
    loop {
        let mut header = [0u8; HEADER_LEN];
        match read_exact_or_eof(&mut reader, &mut header) {
            Ok(false) => break, // Clean EOF
            Ok(true) => {}
            Err(_) => {
                // Truncated header at end of segment = crash point. Recover entries before it.
                warn!(
                    "Truncated WAL header in {:?} after {} entries - treating as crash point",
                    path,
                    entries.len()
                );
                break;
            }
        }
        let (seq, flags, len, expected_crc) = match decode_header(&header) {
            Ok(v) => v,
            Err(_) => {
                warn!(
                    "Corrupt WAL header in {:?} after {} entries - stopping recovery",
                    path,
                    entries.len()
                );
                break;
            }
        };
        let mut payload = vec![0u8; len];
        match read_exact_or_eof(&mut reader, &mut payload) {
            Ok(true) => {}
            _ => {
                // Truncated payload = crash during write. Discard this entry.
                warn!(
                    "Truncated WAL payload (seq={}) in {:?} - discarding incomplete entry",
                    seq, path
                );
                break;
            }
        }
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            // CRC mismatch on last entry = crash during write with partial data.
            warn!(
                "WAL CRC mismatch (seq={}) in {:?} - discarding corrupt trailing entry",
                seq, path
            );
            break;
        }
        entries.push(WalEntry {
            seq,
            flags,
            payload,
        });
    }
    Ok(entries)
}

fn read_exact_or_eof<R: Read>(reader: &mut R, buffer: &mut [u8]) -> Result<bool> {
    let mut offset = 0;
    while offset < buffer.len() {
        let read = reader.read(&mut buffer[offset..]).map_err(map_io_error)?;
        if read == 0 {
            if offset == 0 {
                return Ok(false);
            }
            return Err(Error::Serialization("Truncated WAL entry".to_string()));
        }
        offset += read;
    }
    Ok(true)
}

fn segment_file_name(id: u64) -> String {
    format!("{}{:06}{}", SEGMENT_PREFIX, id, SEGMENT_SUFFIX)
}

#[derive(Debug)]
struct SegmentInfo {
    id: u64,
    path: PathBuf,
}

fn list_segments(dir: &Path) -> Result<Vec<SegmentInfo>> {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(map_io_error)? {
        let entry = entry.map_err(map_io_error)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = parse_segment_id(&name) {
            segments.push(SegmentInfo {
                id,
                path: entry.path(),
            });
        }
    }
    segments.sort_by_key(|segment| segment.id);
    Ok(segments)
}

fn parse_segment_id(name: &str) -> Option<u64> {
    if !name.starts_with(SEGMENT_PREFIX) || !name.ends_with(SEGMENT_SUFFIX) {
        return None;
    }
    let id_str = &name[SEGMENT_PREFIX.len()..name.len() - SEGMENT_SUFFIX.len()];
    id_str.parse::<u64>().ok()
}

fn last_sequence_for_segment(path: &Path) -> Result<Option<u64>> {
    let entries = read_entries_from_path(path)?;
    Ok(entries.last().map(|entry| entry.seq))
}

async fn open_segment(path: &Path) -> Result<tokio::fs::File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .await
        .map_err(map_io_error)
}

const FLUSHED_SEQ_FILE: &str = "flushed_seq";

/// Persist the last flushed WAL sequence number to a file in the WAL directory.
pub fn persist_flushed_seq(wal_dir: &Path, seq: u64) -> Result<()> {
    let path = wal_dir.join(FLUSHED_SEQ_FILE);
    std::fs::write(&path, seq.to_le_bytes()).map_err(map_io_error)?;
    Ok(())
}

/// Load the last flushed WAL sequence number from the WAL directory.
/// Returns 0 if the file does not exist.
pub fn load_flushed_seq(wal_dir: &Path) -> Result<u64> {
    let path = wal_dir.join(FLUSHED_SEQ_FILE);
    match std::fs::read(&path) {
        Ok(bytes) if bytes.len() == 8 => Ok(u64::from_le_bytes(bytes.try_into().unwrap())),
        Ok(_) => {
            warn!("Corrupt flushed_seq file, resetting to 0");
            Ok(0)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(map_io_error(e)),
    }
}

fn map_io_error(error: io::Error) -> Error {
    // ENOSPC (28) on Unix and ERROR_DISK_FULL (112) on Windows.
    if matches!(error.raw_os_error(), Some(28) | Some(112)) {
        return Error::WalFull;
    }
    Error::Io(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let array = Int64Array::from(vec![1, 2, 3]);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    fn make_config(dir: &TempDir, max_segment_size: usize) -> WalConfig {
        WalConfig {
            wal_dir: dir.path().to_path_buf(),
            max_segment_size,
            sync_mode: WalSyncMode::None,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn test_crc_roundtrip() {
        let dir = TempDir::new().unwrap();
        let batch = make_batch();
        let payload = encode_record_batch(&batch).unwrap();
        let mut wal = WriteAheadLog::open(make_config(&dir, 1024 * 1024))
            .await
            .unwrap();
        wal.append(&batch).await.unwrap();
        wal.file.flush().await.unwrap();
        wal.file.sync_all().await.unwrap();
        drop(wal);

        let segment = list_segments(dir.path()).unwrap().pop().unwrap();
        let mut file = StdFile::open(segment.path).unwrap();
        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header).unwrap();
        let (_, _, len, crc) = decode_header(&header).unwrap();
        assert_eq!(len, payload.len());
        let mut stored_payload = vec![0u8; len];
        file.read_exact(&mut stored_payload).unwrap();
        let mut hasher = Hasher::new();
        hasher.update(&stored_payload);
        assert_eq!(crc, hasher.finalize());
    }

    #[tokio::test]
    async fn test_rotation() {
        let dir = TempDir::new().unwrap();
        let batch = make_batch();
        let entry_size = HEADER_LEN + encode_record_batch(&batch).unwrap().len();
        let mut wal = WriteAheadLog::open(make_config(&dir, entry_size + 1))
            .await
            .unwrap();

        wal.append(&batch).await.unwrap();
        wal.append(&batch).await.unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 2);
    }

    #[tokio::test]
    async fn test_truncation() {
        let dir = TempDir::new().unwrap();
        let batch = make_batch();
        let entry_size = HEADER_LEN + encode_record_batch(&batch).unwrap().len();
        let mut wal = WriteAheadLog::open(make_config(&dir, entry_size + 1))
            .await
            .unwrap();

        wal.append(&batch).await.unwrap();
        let second_seq = wal.append(&batch).await.unwrap();
        wal.truncate_before(second_seq).await.unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id, wal.current_segment_id);
    }

    #[tokio::test]
    async fn test_corruption_detection() {
        let dir = TempDir::new().unwrap();
        let batch = make_batch();
        let mut wal = WriteAheadLog::open(make_config(&dir, 1024 * 1024))
            .await
            .unwrap();
        wal.append(&batch).await.unwrap();
        wal.file.flush().await.unwrap();
        wal.file.sync_all().await.unwrap();
        drop(wal);

        let segment = list_segments(dir.path()).unwrap().pop().unwrap();
        let mut file = StdFile::options()
            .read(true)
            .write(true)
            .open(&segment.path)
            .unwrap();
        file.seek(SeekFrom::Start(HEADER_LEN as u64)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(HEADER_LEN as u64)).unwrap();
        byte[0] ^= 0xFF;
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();

        // After crash-tolerant recovery, a corrupt trailing entry is discarded
        // (logged as warning) and entries before it are returned.
        // Since there's only one entry and it's corrupt, we get an empty vec.
        let wal = WriteAheadLog::open(make_config(&dir, 1024 * 1024))
            .await
            .unwrap();
        let entries = wal.read_entries().unwrap();
        assert!(
            entries.is_empty(),
            "corrupt entry should be discarded during recovery"
        );
    }

    #[test]
    fn test_sync_mode_from_str() {
        assert!(matches!(
            WalSyncMode::from_str("every_write").unwrap(),
            WalSyncMode::EveryWrite
        ));
        assert!(matches!(
            WalSyncMode::from_str("on_rotation").unwrap(),
            WalSyncMode::OnRotation
        ));
        assert!(matches!(
            WalSyncMode::from_str("none").unwrap(),
            WalSyncMode::None
        ));
        assert!(matches!(
            WalSyncMode::from_str("EVERY_WRITE").unwrap(),
            WalSyncMode::EveryWrite
        ));

        match WalSyncMode::from_str("interval_100ms").unwrap() {
            WalSyncMode::Interval(d) => assert_eq!(d, Duration::from_millis(100)),
            other => panic!("expected Interval, got {:?}", other),
        }
        match WalSyncMode::from_str("interval_1s").unwrap() {
            WalSyncMode::Interval(d) => assert_eq!(d, Duration::from_secs(1)),
            other => panic!("expected Interval, got {:?}", other),
        }
        match WalSyncMode::from_str("interval_500ms").unwrap() {
            WalSyncMode::Interval(d) => assert_eq!(d, Duration::from_millis(500)),
            other => panic!("expected Interval, got {:?}", other),
        }

        assert!(WalSyncMode::from_str("invalid").is_err());
        assert!(WalSyncMode::from_str("interval_abc").is_err());
    }
}
