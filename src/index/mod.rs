//! Production-grade inverted index for CardinalSin.
//!
//! Implements a Lucene-inspired FST term dictionaries + Roaring bitmap postings
//! in immutable `.csi` segments.
//!
//! - **Ingester** builds a per-chunk `.csi` segment at flush time
//! - **Compactor** merges N segments into 1 during leveled compaction
//! - **Query node** reads manifest, loads segments, ANDs bitmaps to prune chunks

pub mod config;
pub mod fst_builder;
pub mod manifest;
pub mod merge;
pub mod postings;
pub mod segment;

pub use config::IndexConfig;
pub use manifest::{IndexManifest, ManifestClient, SegmentEntry};
pub use merge::SegmentMerger;
pub use postings::PostingsList;
pub use segment::{ChunkOrdinalTable, SegmentReader, SegmentWriter};

use crate::metadata::predicates::{ColumnPredicate, PredicateValue};
use crate::metadata::TimeIndexEntry;
use crate::{Error, Result};

use arrow_array::RecordBatch;
use metrics::{counter, histogram};
use object_store::ObjectStore;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, warn};

struct PendingBatch {
    batches: Vec<RecordBatch>,
    paths: Vec<String>,
    row_count: usize,
    min_time_ns: i64,
    max_time_ns: i64,
    enqueued_at: Instant,
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            paths: Vec::new(),
            row_count: 0,
            min_time_ns: i64::MAX,
            max_time_ns: i64::MIN,
            enqueued_at: Instant::now(),
        }
    }
}

/// Builds and publishes index segments. Used by the ingester and compactor.
pub struct IndexBuilder {
    object_store: Arc<dyn ObjectStore>,
    manifest_client: ManifestClient,
    config: IndexConfig,
    tenant_id: String,
    pending: Mutex<HashMap<String, PendingBatch>>,
}

impl IndexBuilder {
    pub fn new(object_store: Arc<dyn ObjectStore>, tenant_id: &str, config: IndexConfig) -> Self {
        let manifest_client = ManifestClient::new(object_store.clone(), tenant_id);
        Self {
            object_store,
            manifest_client,
            config,
            tenant_id: tenant_id.to_string(),
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Enqueue a freshly flushed chunk into the per-shard batcher. When the
    /// batch exceeds any of the configured thresholds (chunks/rows/age) the
    /// accumulated chunks are concatenated, indexed, and published as a single
    /// `.csi` segment covering all of them.
    ///
    /// Called by the ingester at flush time in place of per-chunk segment
    /// building. On error: logs warn and returns Ok(()) — index build failures
    /// never fail the flush.
    pub async fn enqueue_chunk(
        &self,
        batch: RecordBatch,
        chunk_path: &str,
        shard_id: &str,
        level: u32,
        time_range: (i64, i64),
    ) -> Result<()> {
        let row_count = batch.num_rows();
        let (min_ts, max_ts) = time_range;

        let drained = {
            let mut pending = self.pending.lock().await;
            let entry = pending
                .entry(shard_id.to_string())
                .or_insert_with(PendingBatch::new);
            if entry.batches.is_empty() {
                entry.enqueued_at = Instant::now();
            }
            entry.batches.push(batch);
            entry.paths.push(chunk_path.to_string());
            entry.row_count += row_count;
            entry.min_time_ns = entry.min_time_ns.min(min_ts);
            entry.max_time_ns = entry.max_time_ns.max(max_ts);

            if self.should_flush(entry) {
                pending.remove(shard_id)
            } else {
                None
            }
        };

        if let Some(pending) = drained {
            self.flush_pending(shard_id, level, pending).await?;
        }
        Ok(())
    }

    /// Force-flush any pending batch for the given shard.
    pub async fn flush_shard(&self, shard_id: &str, level: u32) -> Result<()> {
        let drained = {
            let mut pending = self.pending.lock().await;
            pending.remove(shard_id)
        };
        if let Some(pending) = drained {
            self.flush_pending(shard_id, level, pending).await?;
        }
        Ok(())
    }

    /// Force-flush every pending shard batch. Call from shutdown hooks.
    pub async fn flush_all(&self, level: u32) -> Result<()> {
        let drained: Vec<(String, PendingBatch)> = {
            let mut pending = self.pending.lock().await;
            pending.drain().collect()
        };
        for (shard_id, batch) in drained {
            if let Err(e) = self.flush_pending(&shard_id, level, batch).await {
                warn!(shard = %shard_id, error = %e, "flush_all: shard batch failed");
            }
        }
        Ok(())
    }

    fn should_flush(&self, entry: &PendingBatch) -> bool {
        entry.paths.len() >= self.config.batch_max_chunks
            || entry.row_count >= self.config.batch_max_rows
            || entry.enqueued_at.elapsed() >= Duration::from_secs(self.config.batch_max_age_secs)
    }

    async fn flush_pending(&self, shard_id: &str, level: u32, pending: PendingBatch) -> Result<()> {
        if pending.batches.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let chunk_count = pending.paths.len();

        let schema = pending.batches[0].schema();
        let combined = arrow::compute::concat_batches(&schema, pending.batches.iter())
            .map_err(|e| Error::Index(format!("concat_batches failed: {e}")))?;

        // Assign one ordinal per source chunk and build a row→ordinal map.
        let mut chunk_ordinals: Vec<u32> = Vec::with_capacity(combined.num_rows());
        for (ord, batch) in pending.batches.iter().enumerate() {
            chunk_ordinals.extend(std::iter::repeat(ord as u32).take(batch.num_rows()));
        }

        let columns = fst_builder::FstTermBuilder::build_all_columns(
            &combined,
            &chunk_ordinals,
            &self.config.skip_columns,
            self.config.max_cardinality_for_inverted,
        )?;

        if columns.is_empty() {
            debug!(shard = %shard_id, chunks = chunk_count, "No indexable columns in batch, skipping");
            return Ok(());
        }

        let ordinal_entries: Vec<(u32, String)> = pending
            .paths
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u32, p.clone()))
            .collect();
        let ordinal_table = ChunkOrdinalTable::new(ordinal_entries);

        let segment_bytes = SegmentWriter::write_segment(&ordinal_table, &columns)?;
        let segment_size = segment_bytes.len() as u64;

        let segment_path = format!(
            "{}/indexes/shard={}/segments/{}.csi",
            self.tenant_id,
            shard_id,
            uuid::Uuid::new_v4()
        );
        self.object_store
            .put(&segment_path.clone().into(), segment_bytes.into())
            .await?;

        self.add_segment_to_manifest(
            shard_id,
            SegmentEntry {
                path: segment_path,
                min_time_ns: pending.min_time_ns,
                max_time_ns: pending.max_time_ns,
                chunk_count: chunk_count as u32,
                level,
                size_bytes: segment_size,
                created_at: chrono::Utc::now().to_rfc3339(),
            },
        )
        .await?;

        let elapsed = start.elapsed().as_secs_f64();
        histogram!(
            "cardinalsin_index_segment_build_duration_seconds",
            "source" => "batcher"
        )
        .record(elapsed);
        counter!(
            "cardinalsin_index_segment_builds_total",
            "result" => "ok",
            "source" => "batcher"
        )
        .increment(1);

        debug!(
            shard = %shard_id,
            chunks = chunk_count,
            rows = combined.num_rows(),
            elapsed_ms = (elapsed * 1000.0) as u64,
            "Batched index segment built"
        );
        Ok(())
    }

    /// Build a per-chunk `.csi` segment from a flushed RecordBatch.
    ///
    /// Called by the ingester at flush time.
    /// On error: logs warn, returns Ok(()). Never fails the flush.
    pub async fn build_and_publish_segment(
        &self,
        batch: &RecordBatch,
        chunk_path: &str,
        shard_id: &str,
        level: u32,
        time_range: (i64, i64),
    ) -> Result<()> {
        let start = std::time::Instant::now();

        // Build FSTs for all eligible columns (single chunk → ordinal 0)
        let chunk_ordinals = vec![0u32; batch.num_rows()];
        let columns = fst_builder::FstTermBuilder::build_all_columns(
            batch,
            &chunk_ordinals,
            &self.config.skip_columns,
            self.config.max_cardinality_for_inverted,
        )?;

        if columns.is_empty() {
            debug!(chunk = %chunk_path, "No indexable columns, skipping segment build");
            return Ok(());
        }

        // Build ordinal table (single chunk)
        let ordinal_table = ChunkOrdinalTable::new(vec![(0, chunk_path.to_string())]);

        // Serialize segment
        let segment_bytes = SegmentWriter::write_segment(&ordinal_table, &columns)?;
        let segment_size = segment_bytes.len() as u64;

        // Upload .csi to S3
        let segment_path = format!(
            "{}/indexes/shard={}/segments/{}.csi",
            self.tenant_id,
            shard_id,
            uuid::Uuid::new_v4()
        );
        self.object_store
            .put(&segment_path.clone().into(), segment_bytes.into())
            .await?;

        // Update manifest with CAS retry
        self.add_segment_to_manifest(
            shard_id,
            SegmentEntry {
                path: segment_path,
                min_time_ns: time_range.0,
                max_time_ns: time_range.1,
                chunk_count: 1,
                level,
                size_bytes: segment_size,
                created_at: chrono::Utc::now().to_rfc3339(),
            },
        )
        .await?;

        let elapsed = start.elapsed().as_secs_f64();
        histogram!(
            "cardinalsin_index_segment_build_duration_seconds",
            "source" => "ingester"
        )
        .record(elapsed);
        counter!(
            "cardinalsin_index_segment_builds_total",
            "result" => "ok",
            "source" => "ingester"
        )
        .increment(1);

        debug!(chunk = %chunk_path, elapsed_ms = (elapsed * 1000.0) as u64, "Index segment built");
        Ok(())
    }

    /// Merge N source segments into 1, publish merged segment, update manifest.
    ///
    /// Called by the compactor after merging Parquet chunks.
    /// On error: logs warn, returns Ok(()). Never fails compaction.
    pub async fn merge_and_publish_segments(
        &self,
        source_chunk_paths: &[String],
        output_chunk_path: &str,
        shard_id: &str,
        level: u32,
        time_range: (i64, i64),
    ) -> Result<()> {
        let start = std::time::Instant::now();

        // Load manifest to find source segment entries
        let (manifest, _etag) = match self.manifest_client.load_manifest(shard_id).await? {
            Some(m) => m,
            None => {
                debug!(shard = %shard_id, "No manifest found, skipping segment merge");
                return Ok(());
            }
        };

        if manifest.frozen {
            debug!(shard = %shard_id, "Manifest is frozen, skipping segment merge");
            return Ok(());
        }

        // Find segments that cover any of the source chunk paths
        let source_paths_set: HashSet<&str> =
            source_chunk_paths.iter().map(|s| s.as_str()).collect();

        let mut source_segments = Vec::new();
        let mut source_segment_paths = Vec::new();

        for entry in &manifest.segments {
            // Load each segment and check if it covers any source chunk
            match self.load_segment(&entry.path).await {
                Ok(reader) => {
                    let seg_paths: HashSet<&str> =
                        reader.ordinal_table().paths().into_iter().collect();
                    if seg_paths.iter().any(|p| source_paths_set.contains(p)) {
                        source_segment_paths.push(entry.path.clone());
                        source_segments.push(reader);
                    }
                }
                Err(e) => {
                    warn!(path = %entry.path, error = %e, "Failed to load source segment for merge");
                }
            }
        }

        if source_segments.is_empty() {
            debug!(shard = %shard_id, "No source segments found for merge");
            return Ok(());
        }

        // Merge segments
        let merged_bytes =
            SegmentMerger::merge_segments(&source_segments, &[output_chunk_path.to_string()])?;
        let merged_size = merged_bytes.len() as u64;

        // Upload merged segment.
        // Path is deterministic on the sorted set of source segment paths so a
        // CAS-retry after a transient failure re-uploads the same bytes to the
        // same key — no orphaned .csi blob on manifest conflict.
        let merged_path = {
            let mut sorted = source_segment_paths.clone();
            sorted.sort();
            let digest_input = sorted.join("\n");
            let digest = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, digest_input.as_bytes());
            format!(
                "{}/indexes/shard={}/segments/merged-{}.csi",
                self.tenant_id, shard_id, digest
            )
        };
        self.object_store
            .put(&merged_path.clone().into(), merged_bytes.into())
            .await?;

        // Update manifest: remove source entries, add merged entry
        self.replace_segments_in_manifest(
            shard_id,
            &source_segment_paths,
            SegmentEntry {
                path: merged_path,
                min_time_ns: time_range.0,
                max_time_ns: time_range.1,
                chunk_count: 1,
                level,
                size_bytes: merged_size,
                created_at: chrono::Utc::now().to_rfc3339(),
            },
        )
        .await?;

        // Schedule source .csi files for deletion (best-effort)
        for path in &source_segment_paths {
            if let Err(e) = self.object_store.delete(&path.clone().into()).await {
                debug!(path = %path, error = %e, "Failed to delete source segment (non-fatal)");
            }
        }

        let elapsed = start.elapsed().as_secs_f64();
        histogram!(
            "cardinalsin_index_segment_build_duration_seconds",
            "source" => "compactor"
        )
        .record(elapsed);
        counter!(
            "cardinalsin_index_segment_merges_total",
            "result" => "ok"
        )
        .increment(1);

        debug!(
            shard = %shard_id,
            source_count = source_segments.len(),
            elapsed_ms = (elapsed * 1000.0) as u64,
            "Index segments merged"
        );
        Ok(())
    }

    // ── helpers ──────────────────────────────────────────────────────

    async fn load_segment(&self, path: &str) -> Result<SegmentReader> {
        let result = self.object_store.get(&path.into()).await?;
        let bytes = result.bytes().await?;
        SegmentReader::open(bytes.to_vec())
    }

    /// Add a segment entry to the manifest with CAS retry.
    async fn add_segment_to_manifest(&self, shard_id: &str, entry: SegmentEntry) -> Result<()> {
        for attempt in 0..5u32 {
            let (mut manifest, etag) = match self.manifest_client.load_manifest(shard_id).await? {
                Some(m) => m,
                None => {
                    // Create new manifest
                    let mut m = IndexManifest::new(shard_id);
                    m.segments.push(entry.clone());
                    if entry.max_time_ns > m.indexed_through_ns {
                        m.indexed_through_ns = entry.max_time_ns;
                    }
                    self.manifest_client.create_manifest(&m).await?;
                    return Ok(());
                }
            };

            if manifest.frozen {
                return Err(Error::Index(format!(
                    "Manifest for shard '{shard_id}' is frozen"
                )));
            }

            manifest.segments.push(entry.clone());
            manifest.generation += 1;
            if entry.max_time_ns > manifest.indexed_through_ns {
                manifest.indexed_through_ns = entry.max_time_ns;
            }

            match self.manifest_client.save_manifest(&manifest, &etag).await {
                Ok(()) => return Ok(()),
                Err(Error::Conflict) => {
                    let backoff_ms = 100 * 2u64.pow(attempt);
                    debug!(attempt, backoff_ms, "Manifest CAS conflict, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::TooManyRetries)
    }

    /// Replace N source segments with 1 merged segment in the manifest.
    async fn replace_segments_in_manifest(
        &self,
        shard_id: &str,
        source_paths: &[String],
        merged_entry: SegmentEntry,
    ) -> Result<()> {
        let source_set: HashSet<&str> = source_paths.iter().map(|s| s.as_str()).collect();

        for attempt in 0..5u32 {
            let (mut manifest, etag) = self
                .manifest_client
                .load_manifest(shard_id)
                .await?
                .ok_or_else(|| Error::Index(format!("No manifest for shard '{shard_id}'")))?;

            if manifest.frozen {
                return Err(Error::Index(format!(
                    "Manifest for shard '{shard_id}' is frozen"
                )));
            }

            // Remove source entries
            manifest
                .segments
                .retain(|s| !source_set.contains(s.path.as_str()));

            // Add merged entry
            manifest.segments.push(merged_entry.clone());
            manifest.generation += 1;

            match self.manifest_client.save_manifest(&manifest, &etag).await {
                Ok(()) => return Ok(()),
                Err(Error::Conflict) => {
                    let backoff_ms = 100 * 2u64.pow(attempt);
                    debug!(attempt, backoff_ms, "Manifest CAS conflict, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::TooManyRetries)
    }
}

/// Prunes chunks using the inverted index. Used by the query node.
pub struct IndexPrefilter {
    object_store: Arc<dyn ObjectStore>,
    manifest_client: ManifestClient,
    #[allow(dead_code)]
    tenant_id: String,
    segment_cache: moka::future::Cache<String, Arc<SegmentReader>>,
}

impl IndexPrefilter {
    pub fn new(object_store: Arc<dyn ObjectStore>, tenant_id: &str) -> Self {
        Self::with_config(object_store, tenant_id, &IndexConfig::default())
    }

    pub fn with_config(
        object_store: Arc<dyn ObjectStore>,
        tenant_id: &str,
        config: &IndexConfig,
    ) -> Self {
        let manifest_client = ManifestClient::new(object_store.clone(), tenant_id);
        let segment_cache = moka::future::Cache::builder()
            .max_capacity(config.segment_cache_capacity)
            .time_to_idle(std::time::Duration::from_secs(
                config.segment_cache_idle_ttl_secs,
            ))
            .build();
        Self {
            object_store,
            manifest_client,
            tenant_id: tenant_id.to_string(),
            segment_cache,
        }
    }

    /// Prune chunks using inverted index. On any error, returns ALL chunks unchanged.
    pub async fn prune(
        &self,
        chunks: &[TimeIndexEntry],
        predicates: &[ColumnPredicate],
    ) -> Vec<TimeIndexEntry> {
        match self.prune_inner(chunks, predicates).await {
            Ok(pruned) => {
                let pruned_count = chunks.len() - pruned.len();
                counter!(
                    "cardinalsin_index_pruned_chunks_total",
                    "result" => "pruned"
                )
                .increment(pruned_count as u64);
                counter!(
                    "cardinalsin_index_pruned_chunks_total",
                    "result" => "passed"
                )
                .increment(pruned.len() as u64);
                pruned
            }
            Err(e) => {
                warn!(error = %e, "Index pruning failed, returning all chunks");
                counter!(
                    "cardinalsin_index_pruned_chunks_total",
                    "result" => "passed"
                )
                .increment(chunks.len() as u64);
                chunks.to_vec()
            }
        }
    }

    async fn prune_inner(
        &self,
        chunks: &[TimeIndexEntry],
        predicates: &[ColumnPredicate],
    ) -> Result<Vec<TimeIndexEntry>> {
        let start = std::time::Instant::now();

        // Extract indexable leaves, descending into And(...) trees so that
        // compound `WHERE a='x' AND b='y'` predicates AND their bitmaps.
        let indexable: Vec<&ColumnPredicate> = predicates
            .iter()
            .flat_map(extract_indexable_predicates)
            .collect();

        if indexable.is_empty() {
            return Ok(chunks.to_vec());
        }

        // Group chunks by shard_id
        let default_shard = "default".to_string();
        let mut shard_chunks: std::collections::HashMap<&str, Vec<&TimeIndexEntry>> =
            std::collections::HashMap::new();
        for chunk in chunks {
            let shard = chunk.shard_id.as_deref().unwrap_or(&default_shard);
            shard_chunks.entry(shard).or_default().push(chunk);
        }

        let mut matching_paths: HashSet<String> = HashSet::new();
        let mut indexed_paths: HashSet<String> = HashSet::new();

        for (shard_id, shard_chunk_list) in &shard_chunks {
            // Load manifest for this shard
            let (manifest, _etag) = match self.manifest_client.load_manifest(shard_id).await? {
                Some(m) => m,
                None => {
                    // No index for this shard -- pass all chunks through
                    for chunk in shard_chunk_list {
                        matching_paths.insert(chunk.chunk_path.clone());
                    }
                    continue;
                }
            };

            if manifest.frozen {
                // Frozen shard -- pass all through
                for chunk in shard_chunk_list {
                    matching_paths.insert(chunk.chunk_path.clone());
                }
                continue;
            }

            // For each segment with time range overlap
            for entry in &manifest.segments {
                // Check time overlap with any chunk in this shard
                let has_time_overlap = shard_chunk_list.iter().any(|c| {
                    c.min_timestamp <= entry.max_time_ns && c.max_timestamp >= entry.min_time_ns
                });
                if !has_time_overlap {
                    continue;
                }

                // Load segment
                let reader = match self.load_segment(&entry.path).await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(path = %entry.path, error = %e, "Failed to load segment, passing chunks through");
                        for chunk in shard_chunk_list {
                            matching_paths.insert(chunk.chunk_path.clone());
                        }
                        continue;
                    }
                };

                // Track which paths are indexed by this segment
                for path in reader.ordinal_table().paths() {
                    indexed_paths.insert(path.to_string());
                }

                // Build a "full" bitmap containing all ordinals in this segment.
                // Used when a predicate targets a column not indexed in this segment:
                // the predicate cannot prune anything, so all chunks are potential matches.
                let full_bitmap = {
                    let mut bm = PostingsList::new();
                    for (ordinal, _) in reader.ordinal_table().iter() {
                        bm.add(ordinal);
                    }
                    bm
                };

                // AND bitmaps across all indexable predicates
                let mut combined: Option<PostingsList> = None;

                for pred in &indexable {
                    let bitmap = match pred {
                        ColumnPredicate::Eq(col, PredicateValue::String(val)) => {
                            reader.lookup(col, val)?
                        }
                        ColumnPredicate::In(col, values) => {
                            let string_vals: Vec<String> = values
                                .iter()
                                .filter_map(|v| {
                                    if let PredicateValue::String(s) = v {
                                        Some(s.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            reader.lookup_in(col, &string_vals)?
                        }
                        _ => None,
                    };

                    // If lookup returned None (column not indexed in this segment),
                    // use the full bitmap so this predicate doesn't falsely prune chunks.
                    let bitmap = bitmap.unwrap_or_else(|| full_bitmap.clone());

                    combined = Some(match combined {
                        Some(existing) => existing.intersect(&bitmap),
                        None => bitmap,
                    });
                }

                // Resolve matching ordinals to paths
                if let Some(combined) = combined {
                    let paths = reader.resolve_ordinals(&combined.ordinals());
                    for path in paths {
                        matching_paths.insert(path);
                    }
                }
            }
        }

        // Build result: include chunks that either matched or weren't indexed (safety net)
        let result: Vec<TimeIndexEntry> = chunks
            .iter()
            .filter(|c| {
                matching_paths.contains(&c.chunk_path) || !indexed_paths.contains(&c.chunk_path)
            })
            .cloned()
            .collect();

        let elapsed = start.elapsed().as_secs_f64();
        histogram!("cardinalsin_index_lookup_latency_seconds").record(elapsed);

        Ok(result)
    }

    async fn load_segment(&self, path: &str) -> Result<Arc<SegmentReader>> {
        let object_store = self.object_store.clone();
        let path_owned = path.to_string();
        self.segment_cache
            .try_get_with(path_owned.clone(), async move {
                let result = object_store.get(&path_owned.as_str().into()).await?;
                let bytes = result.bytes().await?;
                SegmentReader::open(bytes.to_vec()).map(Arc::new)
            })
            .await
            .map_err(|e: Arc<Error>| Error::Index(format!("segment cache load failed: {e}")))
    }
}

/// Flatten a predicate tree into indexable leaves.
///
/// Descends into `And(l, r)` and concatenates the indexable leaves from
/// both sides — callers AND the resulting bitmaps, which is the correct
/// semantics for AND. `Or`, `Not`, numeric, and unsupported predicates
/// return nothing (conservative: no pruning from this branch).
fn extract_indexable_predicates(pred: &ColumnPredicate) -> Vec<&ColumnPredicate> {
    match pred {
        ColumnPredicate::Eq(_, PredicateValue::String(_)) => vec![pred],
        ColumnPredicate::In(_, values)
            if values
                .iter()
                .all(|v| matches!(v, PredicateValue::String(_))) =>
        {
            vec![pred]
        }
        ColumnPredicate::And(l, r) => {
            let mut out = extract_indexable_predicates(l);
            out.extend(extract_indexable_predicates(r));
            out
        }
        _ => Vec::new(),
    }
}
