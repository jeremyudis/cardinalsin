//! Segment merge for compaction: k-way merge of N source segments into 1.

use crate::{Error, Result};

use super::fst_builder::ColumnFstData;
use super::postings::PostingsList;
use super::segment::{ChunkOrdinalTable, SegmentReader, SegmentWriter};

use std::collections::{BTreeMap, HashSet};

/// Merges multiple CSI segments into a single consolidated segment.
pub struct SegmentMerger;

impl SegmentMerger {
    /// Merge N source segments into 1 consolidated segment.
    ///
    /// Algorithm (Lucene-style k-way merge):
    /// 1. Build new ordinal table from output chunk path(s)
    /// 2. For each column present in any source segment:
    ///    a. Collect all terms across source segments
    ///    b. For each term: union roaring bitmaps, remap ordinals
    ///    c. Build new FST with merged terms + remapped postings
    /// 3. Write merged .csi via SegmentWriter
    pub fn merge_segments(
        source_segments: &[SegmentReader],
        output_chunk_paths: &[String],
    ) -> Result<Vec<u8>> {
        // 1. Build new ordinal table
        let mut new_entries: Vec<(u32, String)> = Vec::new();
        for (idx, path) in output_chunk_paths.iter().enumerate() {
            new_entries.push((idx as u32, path.clone()));
        }
        let new_ordinal_table = ChunkOrdinalTable::new(new_entries);

        // Build remap: for each source segment, map old ordinals to new ordinals.
        // Since compaction merges N source chunks into 1 output chunk,
        // all ordinals from source segments map to ordinal 0 (if single output).
        let remaps: Vec<BTreeMap<u32, u32>> = source_segments
            .iter()
            .map(|seg| {
                let mut remap = BTreeMap::new();
                for (old_ord, old_path) in &seg.ordinal_table().entries {
                    // Find which new ordinal this path maps to
                    if let Some(new_ord) = output_chunk_paths
                        .iter()
                        .position(|p| p == old_path)
                        .map(|i| i as u32)
                    {
                        remap.insert(*old_ord, new_ord);
                    } else {
                        // Source path not in output -- map to 0 (merged into single output)
                        remap.insert(*old_ord, 0);
                    }
                }
                remap
            })
            .collect();

        // 2. Collect all indexed columns across all segments
        let all_columns: HashSet<String> = source_segments
            .iter()
            .flat_map(|seg| seg.indexed_columns())
            .collect();

        // 3. Per-column k-way merge
        let mut merged_columns: Vec<ColumnFstData> = Vec::new();

        for col_name in &all_columns {
            let mut term_postings: BTreeMap<String, PostingsList> = BTreeMap::new();

            for (seg_idx, seg) in source_segments.iter().enumerate() {
                // Try to get all terms for this column from this segment
                let columns = seg.indexed_columns();
                if !columns.contains(col_name) {
                    continue;
                }

                // We need to iterate all terms in this segment's FST for this column.
                // Use the segment's lookup to check each known term.
                // For efficiency, we extract all terms by streaming the FST.
                if let Some(terms) = Self::extract_all_terms(seg, col_name)? {
                    for (term, posting) in terms {
                        let remapped = Self::remap_postings(&posting, &remaps[seg_idx]);
                        term_postings
                            .entry(term)
                            .and_modify(|existing| *existing = existing.union(&remapped))
                            .or_insert(remapped);
                    }
                }
            }

            if term_postings.is_empty() {
                continue;
            }

            // Build FST from merged terms
            let mut fst_builder = fst::MapBuilder::memory();
            let mut postings = Vec::with_capacity(term_postings.len());

            for (idx, (term, posting)) in term_postings.into_iter().enumerate() {
                fst_builder
                    .insert(term.as_bytes(), idx as u64)
                    .map_err(|e| Error::Index(format!("FST merge insert failed: {e}")))?;
                postings.push(posting);
            }

            let fst_bytes = fst_builder
                .into_inner()
                .map_err(|e| Error::Index(format!("FST merge build failed: {e}")))?;

            merged_columns.push(ColumnFstData {
                column_name: col_name.clone(),
                fst_bytes,
                postings,
            });
        }

        // 4. Write merged segment
        SegmentWriter::write_segment(&new_ordinal_table, &merged_columns)
    }

    /// Extract all terms and their postings from a segment for a specific column.
    fn extract_all_terms(
        seg: &SegmentReader,
        col_name: &str,
    ) -> Result<Option<Vec<(String, PostingsList)>>> {
        // Access the segment's raw section data for this column
        let section = seg.indexed_columns().iter().position(|c| c == col_name);

        let _section_idx = match section {
            Some(idx) => idx,
            None => return Ok(None),
        };

        // We need to read the FST and iterate over all keys.
        // The SegmentReader exposes lookup but not iteration, so we use the FST directly.
        // Re-parse the section data to get the FST map.
        // This is a bit redundant but keeps the segment module encapsulated.
        // For production, we'd add an iterator method to SegmentReader.

        // Use fst::Map stream to iterate all terms
        let section_data = seg.read_section_by_name(col_name)?;
        if section_data.is_none() {
            return Ok(None);
        }
        let section_data = section_data.unwrap();

        let mut pos = 0usize;
        let fst_len = read_u64(&section_data, &mut pos)? as usize;
        if pos + fst_len > section_data.len() {
            return Err(Error::IndexCorrupt("FST data overflow in merge".into()));
        }
        let fst_bytes = section_data[pos..pos + fst_len].to_vec();
        pos += fst_len;

        let fst_map = fst::Map::new(fst_bytes)
            .map_err(|e| Error::IndexCorrupt(format!("Invalid FST in merge: {e}")))?;

        let postings_count = read_u32(&section_data, &mut pos)? as usize;
        let mut postings = Vec::with_capacity(postings_count);
        for _ in 0..postings_count {
            let posting_len = read_u32(&section_data, &mut pos)? as usize;
            if pos + posting_len > section_data.len() {
                return Err(Error::IndexCorrupt(
                    "Postings data overflow in merge".into(),
                ));
            }
            let posting = PostingsList::deserialize(&section_data[pos..pos + posting_len])?;
            pos += posting_len;
            postings.push(posting);
        }

        // Stream all keys from FST
        use fst::Streamer;
        let mut stream = fst_map.stream();
        let mut result = Vec::new();
        while let Some((key, idx)) = stream.next() {
            let term = String::from_utf8(key.to_vec())
                .map_err(|e| Error::IndexCorrupt(format!("Invalid UTF-8 term in merge: {e}")))?;
            let idx = idx as usize;
            if idx < postings.len() {
                result.push((term, postings[idx].clone()));
            }
        }

        Ok(Some(result))
    }

    /// Remap ordinals in a postings list using the provided remap table.
    fn remap_postings(posting: &PostingsList, remap: &BTreeMap<u32, u32>) -> PostingsList {
        let mut remapped = PostingsList::new();
        for ord in posting.ordinals() {
            if let Some(&new_ord) = remap.get(&ord) {
                remapped.add(new_ord);
            } else {
                // If not in remap, map to 0 (single output chunk)
                remapped.add(0);
            }
        }
        remapped
    }
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
