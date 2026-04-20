//! Binary `.csi` segment format: read and write.
//!
//! Column section layout:
//! ```text
//! [FST len u64][FST bytes]
//! [postings_count u32]
//! [posting offsets: u64 × (count + 1)]   // offset into postings blob, trailing sentinel = blob_len
//! [postings blob: RoaringBitmap serialize() bytes, concatenated]
//! ```
//! The sentinel (N+1)th offset gives the length of the postings blob; term
//! `i` spans `blob[offsets[i] .. offsets[i + 1]]`, so a single `lookup()`
//! deserializes exactly one bitmap — no scan of prior postings.
//!
//! File layout:
//! ```text
//! [Magic "CSIX" 4B][Version u16][Flags u16][Section count u32]
//! [Chunk Ordinal Table: count u32, entries (ordinal u32, path_len u16, path bytes)]
//! [Section Directory: type u8, col_name_len u16, col_name, offset u64, length u64, crc32 u32]
//! [Section Data: as described above]
//! [Footer: file CRC32 u32 + Magic "CSIX" 4B]
//! ```

use crate::{Error, Result};

use super::fst_builder::ColumnFstData;
use super::postings::PostingsList;

use std::collections::HashMap;

/// Magic bytes identifying a CSI segment file.
pub const CSI_MAGIC: &[u8; 4] = b"CSIX";
/// Current segment format version.
pub const CSI_VERSION: u16 = 1;
/// Section type: FST + postings for a column.
const SECTION_TYPE_COLUMN: u8 = 1;

/// Maps ordinals to chunk paths within a segment.
///
/// Ordinals are dense `0..N` — `paths[ord as usize]` is the chunk path.
/// Sparse ordinal tables (as may arise mid-merge before ordinals are
/// renumbered) are padded with empty strings on construction.
#[derive(Debug, Clone)]
pub struct ChunkOrdinalTable {
    paths: Vec<String>,
}

impl ChunkOrdinalTable {
    /// Build from `(ordinal, path)` pairs. Ordinals need not be sorted.
    /// Gaps are padded with empty strings so `paths[ord]` resolves in O(1).
    pub fn new(entries: Vec<(u32, String)>) -> Self {
        let max = entries.iter().map(|(o, _)| *o).max().unwrap_or(0) as usize;
        let mut paths = vec![String::new(); max + 1];
        for (ord, path) in entries {
            paths[ord as usize] = path;
        }
        // Strip trailing empty slots so len() reflects the real ordinal count
        // when callers build from a genuinely dense sequence.
        while paths.last().map(|s| s.is_empty()).unwrap_or(false) {
            paths.pop();
        }
        Self { paths }
    }

    /// Look up the path for a given ordinal. O(1).
    pub fn resolve(&self, ordinal: u32) -> Option<&str> {
        let s = self.paths.get(ordinal as usize)?;
        if s.is_empty() {
            None
        } else {
            Some(s.as_str())
        }
    }

    /// Return all (non-empty) paths in the table.
    pub fn paths(&self) -> Vec<&str> {
        self.paths
            .iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.as_str())
            .collect()
    }

    /// Number of ordinal slots (including any empty pad slots up to max).
    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Iterate `(ordinal, path)` pairs, skipping empty slots.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &str)> {
        self.paths
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.is_empty())
            .map(|(i, s)| (i as u32, s.as_str()))
    }
}

/// Parsed, ready-to-query column section. Held by `SegmentReader` after open
/// so that `lookup()` does not reparse the FST or deserialize unrelated
/// postings on every call.
pub(crate) struct ParsedSection {
    pub fst: fst::Map<Vec<u8>>,
    /// Offsets into the postings blob. Length is `postings_count + 1`;
    /// posting `i` lives in `blob[offsets[i] .. offsets[i + 1]]`.
    pub offsets: Vec<u64>,
    /// Byte range of the postings blob inside the segment's `data`.
    pub blob_start: usize,
    pub blob_end: usize,
}

impl ParsedSection {
    fn posting_slice<'a>(&self, data: &'a [u8], idx: usize) -> Result<&'a [u8]> {
        if idx + 1 >= self.offsets.len() {
            return Err(Error::IndexCorrupt(format!(
                "Posting index {idx} out of range ({} offsets)",
                self.offsets.len()
            )));
        }
        let start = self.blob_start + self.offsets[idx] as usize;
        let end = self.blob_start + self.offsets[idx + 1] as usize;
        if end > data.len() || end > self.blob_end {
            return Err(Error::IndexCorrupt("Posting slice out of bounds".into()));
        }
        Ok(&data[start..end])
    }
}

/// Writes a `.csi` segment to bytes.
pub struct SegmentWriter;

impl SegmentWriter {
    /// Serialize a segment to bytes.
    pub fn write_segment(
        ordinal_table: &ChunkOrdinalTable,
        columns: &[ColumnFstData],
    ) -> Result<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::new();

        // Header
        buf.extend_from_slice(CSI_MAGIC);
        buf.extend_from_slice(&CSI_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags
        buf.extend_from_slice(&(columns.len() as u32).to_le_bytes());

        // Chunk ordinal table (ordinal, path)
        let ord_entries: Vec<(u32, &str)> = ordinal_table.iter().collect();
        buf.extend_from_slice(&(ord_entries.len() as u32).to_le_bytes());
        for (ordinal, path) in ord_entries {
            buf.extend_from_slice(&ordinal.to_le_bytes());
            let path_bytes = path.as_bytes();
            buf.extend_from_slice(&(path_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(path_bytes);
        }

        // Build section data first so we know offsets
        let mut section_data_parts: Vec<(String, Vec<u8>)> = Vec::new();
        for col in columns {
            let mut section_buf: Vec<u8> = Vec::new();

            // FST bytes with u64 length prefix
            section_buf.extend_from_slice(&(col.fst_bytes.len() as u64).to_le_bytes());
            section_buf.extend_from_slice(&col.fst_bytes);

            // Postings count
            section_buf.extend_from_slice(&(col.postings.len() as u32).to_le_bytes());

            // Serialize postings into a blob + offsets table (count + 1 entries,
            // trailing sentinel = blob length).
            let mut blob: Vec<u8> = Vec::new();
            let mut offsets: Vec<u64> = Vec::with_capacity(col.postings.len() + 1);
            for posting in &col.postings {
                offsets.push(blob.len() as u64);
                blob.extend_from_slice(&posting.serialize());
            }
            offsets.push(blob.len() as u64);

            for off in &offsets {
                section_buf.extend_from_slice(&off.to_le_bytes());
            }
            section_buf.extend_from_slice(&blob);

            section_data_parts.push((col.column_name.clone(), section_buf));
        }

        // Calculate section directory size to compute offsets
        let dir_size: usize = section_data_parts
            .iter()
            .map(|(name, _)| {
                1 + 2 + name.len() // type + name_len + name
                + 8 + 8 + 4 // offset + length + crc32
            })
            .sum();

        let data_start = buf.len() + dir_size;
        let mut current_offset = data_start as u64;

        // Write section directory
        for (name, data) in &section_data_parts {
            buf.push(SECTION_TYPE_COLUMN);
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&current_offset.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u64).to_le_bytes());

            let crc = crc32fast::hash(data);
            buf.extend_from_slice(&crc.to_le_bytes());

            current_offset += data.len() as u64;
        }

        // Write section data
        for (_, data) in &section_data_parts {
            buf.extend_from_slice(data);
        }

        // Footer: file CRC32 + magic
        let file_crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&file_crc.to_le_bytes());
        buf.extend_from_slice(CSI_MAGIC);

        Ok(buf)
    }
}

/// Reads and queries a `.csi` segment.
///
/// All integrity checks (file CRC, section CRCs, FST validity, offsets
/// in-bounds) run once at `open()`; `lookup()` is pure O(log term) +
/// one posting deserialize.
pub struct SegmentReader {
    ordinal_table: ChunkOrdinalTable,
    parsed: HashMap<String, ParsedSection>,
    indexed_column_order: Vec<String>,
    data: Vec<u8>,
}

impl SegmentReader {
    /// Open and validate a CSI segment from raw bytes.
    pub fn open(data: Vec<u8>) -> Result<Self> {
        if data.len() < 12 {
            return Err(Error::IndexCorrupt("Segment too short".into()));
        }

        // Validate footer magic
        let footer_magic = &data[data.len() - 4..];
        if footer_magic != CSI_MAGIC {
            return Err(Error::IndexCorrupt("Invalid footer magic".into()));
        }

        // Validate file CRC32
        let stored_crc =
            u32::from_le_bytes(data[data.len() - 8..data.len() - 4].try_into().unwrap());
        let computed_crc = crc32fast::hash(&data[..data.len() - 8]);
        if stored_crc != computed_crc {
            return Err(Error::IndexCorrupt(format!(
                "File CRC mismatch: stored={stored_crc:#x}, computed={computed_crc:#x}"
            )));
        }

        // Parse header
        let header_magic = &data[0..4];
        if header_magic != CSI_MAGIC {
            return Err(Error::IndexCorrupt("Invalid header magic".into()));
        }

        let version = u16::from_le_bytes(data[4..6].try_into().unwrap());
        if version != CSI_VERSION {
            return Err(Error::IndexCorrupt(format!(
                "Unsupported version: {version}"
            )));
        }
        // flags at 6..8 (currently unused)
        let section_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

        // Parse chunk ordinal table
        let mut pos = 12;
        let entry_count = read_u32(&data, &mut pos)?;
        let mut entries = Vec::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let ordinal = read_u32(&data, &mut pos)?;
            let path_len = read_u16(&data, &mut pos)? as usize;
            if pos + path_len > data.len() {
                return Err(Error::IndexCorrupt("Ordinal table path overflow".into()));
            }
            let path = String::from_utf8(data[pos..pos + path_len].to_vec())
                .map_err(|e| Error::IndexCorrupt(format!("Invalid UTF-8 in ordinal table: {e}")))?;
            pos += path_len;
            entries.push((ordinal, path));
        }
        let ordinal_table = ChunkOrdinalTable::new(entries);

        // Parse section directory
        struct SectionHeader {
            column_name: String,
            offset: u64,
            length: u64,
            expected_crc: u32,
        }

        let mut section_headers = Vec::with_capacity(section_count);
        for _ in 0..section_count {
            if pos >= data.len() {
                return Err(Error::IndexCorrupt("Truncated section directory".into()));
            }
            let _section_type = data[pos];
            pos += 1;
            let name_len = read_u16(&data, &mut pos)? as usize;
            if pos + name_len > data.len() {
                return Err(Error::IndexCorrupt(
                    "Section directory name overflow".into(),
                ));
            }
            let column_name = String::from_utf8(data[pos..pos + name_len].to_vec())
                .map_err(|e| Error::IndexCorrupt(format!("Invalid UTF-8 in section name: {e}")))?;
            pos += name_len;

            let offset = read_u64(&data, &mut pos)?;
            let length = read_u64(&data, &mut pos)?;
            let expected_crc = read_u32(&data, &mut pos)?;

            section_headers.push(SectionHeader {
                column_name,
                offset,
                length,
                expected_crc,
            });
        }

        // Parse each section eagerly — CRC check, FST load, offsets table.
        let mut parsed = HashMap::with_capacity(section_headers.len());
        let mut indexed_column_order = Vec::with_capacity(section_headers.len());

        for header in section_headers {
            let start = header.offset as usize;
            let end = start + header.length as usize;
            if end > data.len() {
                return Err(Error::IndexCorrupt(format!(
                    "Section '{}' out of bounds",
                    header.column_name
                )));
            }

            let crc = crc32fast::hash(&data[start..end]);
            if crc != header.expected_crc {
                return Err(Error::IndexCorrupt(format!(
                    "Section CRC mismatch for '{}': stored={:#x}, computed={:#x}",
                    header.column_name, header.expected_crc, crc
                )));
            }

            let section_data = &data[start..end];
            let mut sp = 0usize;

            let fst_len = read_u64_slice(section_data, &mut sp)? as usize;
            if sp + fst_len > section_data.len() {
                return Err(Error::IndexCorrupt(format!(
                    "FST overflow in section '{}'",
                    header.column_name
                )));
            }
            let fst_bytes = section_data[sp..sp + fst_len].to_vec();
            sp += fst_len;

            let fst = fst::Map::new(fst_bytes).map_err(|e| {
                Error::IndexCorrupt(format!("Invalid FST in '{}': {e}", header.column_name))
            })?;

            let postings_count = read_u32_slice(section_data, &mut sp)? as usize;

            // Offsets table — count + 1 entries.
            let offsets_bytes = (postings_count + 1) * 8;
            if sp + offsets_bytes > section_data.len() {
                return Err(Error::IndexCorrupt(format!(
                    "Offsets table overflow in '{}'",
                    header.column_name
                )));
            }
            let mut offsets = Vec::with_capacity(postings_count + 1);
            for _ in 0..=postings_count {
                offsets.push(read_u64_slice(section_data, &mut sp)?);
            }

            // Remaining bytes are the postings blob.
            let blob_start_rel = sp;
            let blob_len = section_data.len() - blob_start_rel;
            // Sentinel offset must equal blob length.
            if offsets[postings_count] as usize != blob_len {
                return Err(Error::IndexCorrupt(format!(
                    "Offset sentinel mismatch in '{}': sentinel={}, blob_len={}",
                    header.column_name, offsets[postings_count], blob_len
                )));
            }
            // Offsets must be monotonically non-decreasing.
            for w in offsets.windows(2) {
                if w[0] > w[1] {
                    return Err(Error::IndexCorrupt(format!(
                        "Non-monotonic posting offset in '{}'",
                        header.column_name
                    )));
                }
            }

            let blob_abs_start = start + blob_start_rel;
            let blob_abs_end = end;

            indexed_column_order.push(header.column_name.clone());
            parsed.insert(
                header.column_name,
                ParsedSection {
                    fst,
                    offsets,
                    blob_start: blob_abs_start,
                    blob_end: blob_abs_end,
                },
            );
        }

        Ok(Self {
            ordinal_table,
            parsed,
            indexed_column_order,
            data,
        })
    }

    /// Look up a single column=value predicate, returning matching ordinals.
    pub fn lookup(&self, col: &str, val: &str) -> Result<Option<PostingsList>> {
        let section = match self.parsed.get(col) {
            Some(s) => s,
            None => return Ok(None), // Column not indexed
        };

        match section.fst.get(val.as_bytes()) {
            Some(idx) => {
                let idx = idx as usize;
                let bytes = section.posting_slice(&self.data, idx)?;
                Ok(Some(PostingsList::deserialize(bytes)?))
            }
            None => Ok(Some(PostingsList::new())), // Value not found = empty result
        }
    }

    /// Look up a column IN (val1, val2, ...) predicate, returning the union.
    pub fn lookup_in(&self, col: &str, vals: &[String]) -> Result<Option<PostingsList>> {
        let section = match self.parsed.get(col) {
            Some(s) => s,
            None => return Ok(None),
        };

        let mut result = PostingsList::new();
        for val in vals {
            if let Some(idx) = section.fst.get(val.as_bytes()) {
                let idx = idx as usize;
                let bytes = section.posting_slice(&self.data, idx)?;
                let posting = PostingsList::deserialize(bytes)?;
                result = result.union(&posting);
            }
        }

        Ok(Some(result))
    }

    /// Resolve ordinals to chunk paths.
    pub fn resolve_ordinals(&self, ordinals: &[u32]) -> Vec<String> {
        ordinals
            .iter()
            .filter_map(|o| self.ordinal_table.resolve(*o).map(|s| s.to_string()))
            .collect()
    }

    /// Return the names of all indexed columns.
    pub fn indexed_columns(&self) -> Vec<String> {
        self.indexed_column_order.clone()
    }

    /// Return a reference to the ordinal table.
    pub fn ordinal_table(&self) -> &ChunkOrdinalTable {
        &self.ordinal_table
    }

    /// Return a reference to a pre-parsed section by column name. Used by
    /// the merge module for streaming k-way merges without re-parsing.
    pub(crate) fn parsed_section(&self, col_name: &str) -> Option<&ParsedSection> {
        self.parsed.get(col_name)
    }

    /// Return the underlying file bytes so callers holding a ParsedSection
    /// can slice the postings blob.
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }
}

// ── byte reading helpers ─────────────────────────────────────────────

fn read_u16(data: &[u8], pos: &mut usize) -> Result<u16> {
    if *pos + 2 > data.len() {
        return Err(Error::IndexCorrupt("Unexpected EOF reading u16".into()));
    }
    let val = u16::from_le_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(val)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > data.len() {
        return Err(Error::IndexCorrupt("Unexpected EOF reading u32".into()));
    }
    let val = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(val)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos + 8 > data.len() {
        return Err(Error::IndexCorrupt("Unexpected EOF reading u64".into()));
    }
    let val = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(val)
}

fn read_u32_slice(data: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > data.len() {
        return Err(Error::IndexCorrupt("Unexpected EOF reading u32".into()));
    }
    let val = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(val)
}

fn read_u64_slice(data: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos + 8 > data.len() {
        return Err(Error::IndexCorrupt("Unexpected EOF reading u64".into()));
    }
    let val = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(val)
}
