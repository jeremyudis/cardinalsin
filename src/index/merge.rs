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
                for (old_ord, old_path) in seg.ordinal_table().iter() {
                    // Find which new ordinal this path maps to
                    if let Some(new_ord) = output_chunk_paths
                        .iter()
                        .position(|p| p == old_path)
                        .map(|i| i as u32)
                    {
                        remap.insert(old_ord, new_ord);
                    } else {
                        // Source path not in output -- map to 0 (merged into single output)
                        remap.insert(old_ord, 0);
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

        // 3. Per-column streaming k-way merge via fst::map::OpBuilder::union.
        //
        // Peak memory per column is bounded by one active term's live posting
        // union (≤ k source postings), not the full term×posting matrix.
        let mut merged_columns: Vec<ColumnFstData> = Vec::new();

        for col_name in &all_columns {
            // Gather (parsed_section, remap) for every source segment that
            // indexes this column. We keep references only — nothing is
            // copied out of the segment payload.
            let mut source_refs: Vec<(&super::segment::ParsedSection, &BTreeMap<u32, u32>, &[u8])> =
                Vec::new();
            for (seg_idx, seg) in source_segments.iter().enumerate() {
                if let Some(section) = seg.parsed_section(col_name) {
                    source_refs.push((section, &remaps[seg_idx], seg.data()));
                }
            }
            if source_refs.is_empty() {
                continue;
            }

            let mut op_builder = fst::map::OpBuilder::new();
            for (section, _, _) in &source_refs {
                op_builder = op_builder.add(&section.fst);
            }

            use fst::Streamer;
            let mut union_stream = op_builder.union();

            let mut fst_builder = fst::MapBuilder::memory();
            let mut postings: Vec<PostingsList> = Vec::new();
            let mut idx: u64 = 0;

            while let Some((key, indexed_values)) = union_stream.next() {
                // Union the matching postings across source segments.
                let mut merged = PostingsList::new();
                for iv in indexed_values {
                    let (section, remap, data) = &source_refs[iv.index];
                    let posting_idx = iv.value as usize;
                    if posting_idx + 1 >= section.offsets.len() {
                        return Err(Error::IndexCorrupt(format!(
                            "FST term index {posting_idx} out of range in merge"
                        )));
                    }
                    let start = section.blob_start + section.offsets[posting_idx] as usize;
                    let end = section.blob_start + section.offsets[posting_idx + 1] as usize;
                    let posting = PostingsList::deserialize(&data[start..end])?;
                    let remapped = Self::remap_postings(&posting, remap);
                    merged = merged.union(&remapped);
                }

                fst_builder
                    .insert(key, idx)
                    .map_err(|e| Error::Index(format!("FST merge insert failed: {e}")))?;
                postings.push(merged);
                idx += 1;
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
