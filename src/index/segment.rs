//! Binary `.csi` segment format: read and write.
//!
//! Layout:
//! ```text
//! [Magic "CSIX" 4B][Version u16][Flags u16][Section count u32]
//! [Chunk Ordinal Table: count u32, entries (ordinal u32, path_len u16, path bytes)]
//! [Section Directory: type u8, col_name_len u16, col_name, offset u64, length u64, crc32 u32]
//! [Section Data: FST bytes (u64 len prefix) + postings count u32 + postings (u32 len prefix each)]
//! [Footer: file CRC32 u32 + Magic "CSIX" 4B]
//! ```

use crate::{Error, Result};

use super::fst_builder::ColumnFstData;
use super::postings::PostingsList;

/// Magic bytes identifying a CSI segment file.
pub const CSI_MAGIC: &[u8; 4] = b"CSIX";
/// Current segment format version.
pub const CSI_VERSION: u16 = 1;
/// Section type: FST + postings for a column.
const SECTION_TYPE_COLUMN: u8 = 1;

/// Maps ordinals to chunk paths within a segment.
#[derive(Debug, Clone)]
pub struct ChunkOrdinalTable {
    pub entries: Vec<(u32, String)>,
}

impl ChunkOrdinalTable {
    pub fn new(entries: Vec<(u32, String)>) -> Self {
        Self { entries }
    }

    /// Look up the path for a given ordinal.
    pub fn resolve(&self, ordinal: u32) -> Option<&str> {
        self.entries
            .iter()
            .find(|(o, _)| *o == ordinal)
            .map(|(_, p)| p.as_str())
    }

    /// Return all paths in the table.
    pub fn paths(&self) -> Vec<&str> {
        self.entries.iter().map(|(_, p)| p.as_str()).collect()
    }
}

/// Parsed section directory entry (kept in memory after open).
#[derive(Debug)]
struct SectionEntry {
    column_name: String,
    offset: u64,
    length: u64,
    expected_crc: u32,
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

        // Chunk ordinal table
        buf.extend_from_slice(&(ordinal_table.entries.len() as u32).to_le_bytes());
        for (ordinal, path) in &ordinal_table.entries {
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

            // Postings count + serialized postings
            section_buf.extend_from_slice(&(col.postings.len() as u32).to_le_bytes());
            for posting in &col.postings {
                let serialized = posting.serialize();
                section_buf.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
                section_buf.extend_from_slice(&serialized);
            }

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
pub struct SegmentReader {
    ordinal_table: ChunkOrdinalTable,
    sections: Vec<SectionEntry>,
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
        let mut sections = Vec::with_capacity(section_count);
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

            sections.push(SectionEntry {
                column_name,
                offset,
                length,
                expected_crc,
            });
        }

        Ok(Self {
            ordinal_table,
            sections,
            data,
        })
    }

    /// Look up a single column=value predicate, returning matching ordinals.
    pub fn lookup(&self, col: &str, val: &str) -> Result<Option<PostingsList>> {
        let section = match self.sections.iter().find(|s| s.column_name == col) {
            Some(s) => s,
            None => return Ok(None), // Column not indexed
        };

        let section_data = self.read_section(section)?;
        let (fst_map, postings) = Self::parse_section_data(&section_data)?;

        match fst_map.get(val.as_bytes()) {
            Some(idx) => {
                let idx = idx as usize;
                if idx < postings.len() {
                    Ok(Some(postings[idx].clone()))
                } else {
                    Err(Error::IndexCorrupt(format!(
                        "FST index {idx} out of range for column '{col}'"
                    )))
                }
            }
            None => Ok(Some(PostingsList::new())), // Value not found = empty result
        }
    }

    /// Look up a column IN (val1, val2, ...) predicate, returning the union.
    pub fn lookup_in(&self, col: &str, vals: &[String]) -> Result<Option<PostingsList>> {
        let section = match self.sections.iter().find(|s| s.column_name == col) {
            Some(s) => s,
            None => return Ok(None), // Column not indexed
        };

        let section_data = self.read_section(section)?;
        let (fst_map, postings) = Self::parse_section_data(&section_data)?;

        let mut result = PostingsList::new();
        for val in vals {
            if let Some(idx) = fst_map.get(val.as_bytes()) {
                let idx = idx as usize;
                if idx < postings.len() {
                    result = result.union(&postings[idx]);
                }
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
        self.sections
            .iter()
            .map(|s| s.column_name.clone())
            .collect()
    }

    /// Return a reference to the ordinal table.
    pub fn ordinal_table(&self) -> &ChunkOrdinalTable {
        &self.ordinal_table
    }

    /// Read a section's raw data by column name. Used by the merge module.
    pub fn read_section_by_name(&self, col_name: &str) -> Result<Option<Vec<u8>>> {
        match self.sections.iter().find(|s| s.column_name == col_name) {
            Some(section) => {
                let data = self.read_section(section)?;
                Ok(Some(data.to_vec()))
            }
            None => Ok(None),
        }
    }

    // ── internal helpers ─────────────────────────────────────────────

    fn read_section(&self, section: &SectionEntry) -> Result<&[u8]> {
        let start = section.offset as usize;
        let end = start + section.length as usize;
        if end > self.data.len() {
            return Err(Error::IndexCorrupt("Section data out of bounds".into()));
        }

        let section_data = &self.data[start..end];

        // Validate section CRC
        let crc = crc32fast::hash(section_data);
        if crc != section.expected_crc {
            return Err(Error::IndexCorrupt(format!(
                "Section CRC mismatch for '{}': stored={:#x}, computed={:#x}",
                section.column_name, section.expected_crc, crc
            )));
        }

        Ok(section_data)
    }

    fn parse_section_data(data: &[u8]) -> Result<(fst::Map<Vec<u8>>, Vec<PostingsList>)> {
        let mut pos = 0usize;

        // FST with u64 length prefix
        let fst_len = read_u64_slice(data, &mut pos)? as usize;
        if pos + fst_len > data.len() {
            return Err(Error::IndexCorrupt("FST data overflow".into()));
        }
        let fst_bytes = data[pos..pos + fst_len].to_vec();
        pos += fst_len;

        let fst_map = fst::Map::new(fst_bytes)
            .map_err(|e| Error::IndexCorrupt(format!("Invalid FST: {e}")))?;

        // Postings
        let postings_count = read_u32_slice(data, &mut pos)? as usize;
        let mut postings = Vec::with_capacity(postings_count);
        for _ in 0..postings_count {
            let posting_len = read_u32_slice(data, &mut pos)? as usize;
            if pos + posting_len > data.len() {
                return Err(Error::IndexCorrupt("Postings data overflow".into()));
            }
            let posting = PostingsList::deserialize(&data[pos..pos + posting_len])?;
            pos += posting_len;
            postings.push(posting);
        }

        Ok((fst_map, postings))
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
