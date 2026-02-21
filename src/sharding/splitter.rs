//! Five-phase shard splitting protocol with crash-safe persistent state machine
//!
//! Each phase transition is persisted to object storage so that a crash at any
//! point can be recovered by calling `resume_split()`.

use super::{ShardId, ShardMetadata, ShardState, TimeBucket};
use crate::ingester::ChunkMetadata;
use crate::metadata::MetadataClient;
use crate::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use bytes::Bytes;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// Split phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SplitPhase {
    /// Preparation phase - creating new shard metadata
    Preparation,
    /// Dual-write phase - writing to both old and new shards
    DualWrite,
    /// Backfill phase - copying historical data
    Backfill,
    /// Cutover phase - atomic switch to new shards
    Cutover,
    /// Cleanup phase - removing old shard
    Cleanup,
}

/// Persistent progress tracker for a shard split.
///
/// Persisted to `metadata/split-progress/{shard}.json` in object storage.
/// Enables crash recovery from any point in the 5-phase split protocol.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SplitProgress {
    /// Unique fence token for this split attempt.
    pub fence_token: String,
    /// The shard being split.
    pub old_shard: String,
    /// IDs of the two new shards.
    pub new_shards: Vec<String>,
    /// The split point (serialized timestamp bytes).
    pub split_point: Vec<u8>,
    /// Last fully completed phase (None = not started).
    pub completed_phase: Option<SplitPhase>,
    /// Cutover sub-step tracking (for Phase 4 fencing).
    pub shard_a_created: bool,
    pub shard_b_created: bool,
    pub old_shard_deactivated: bool,
    /// Source chunk paths fully backfilled to new shards.
    #[serde(default)]
    pub backfilled_chunks: BTreeSet<String>,
    /// Number of source chunks observed when backfill last ran.
    #[serde(default)]
    pub backfill_total_chunks: usize,
}

impl SplitProgress {
    fn new(old_shard: &str, new_shards: Vec<String>, split_point: Vec<u8>) -> Self {
        Self {
            fence_token: uuid::Uuid::new_v4().to_string(),
            old_shard: old_shard.to_string(),
            new_shards,
            split_point,
            completed_phase: None,
            shard_a_created: false,
            shard_b_created: false,
            old_shard_deactivated: false,
            backfilled_chunks: BTreeSet::new(),
            backfill_total_chunks: 0,
        }
    }

    /// Returns the next phase to execute, or None if all phases are done.
    fn next_phase(&self) -> Option<SplitPhase> {
        match self.completed_phase {
            None => Some(SplitPhase::Preparation),
            Some(SplitPhase::Preparation) => Some(SplitPhase::DualWrite),
            Some(SplitPhase::DualWrite) => Some(SplitPhase::Backfill),
            Some(SplitPhase::Backfill) => Some(SplitPhase::Cutover),
            Some(SplitPhase::Cutover) => Some(SplitPhase::Cleanup),
            Some(SplitPhase::Cleanup) => None,
        }
    }
}

/// Pending shard (not yet active)
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PendingShard {
    pub id: ShardId,
    pub range: (Vec<u8>, Vec<u8>),
}

/// Shard splitter for zero-downtime splitting with crash recovery
pub struct ShardSplitter {
    metadata: Arc<dyn MetadataClient>,
    object_store: Arc<dyn ObjectStore>,
}

impl ShardSplitter {
    /// Create a new shard splitter
    pub fn new(metadata: Arc<dyn MetadataClient>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            metadata,
            object_store,
        }
    }

    // ── Persistence helpers ──────────────────────────────────────────

    fn progress_path(old_shard: &str) -> String {
        format!("metadata/split-progress/{}.json", old_shard)
    }

    async fn persist_progress(&self, progress: &SplitProgress) -> Result<()> {
        let path = Self::progress_path(&progress.old_shard);
        let json =
            serde_json::to_vec(progress).map_err(|e| crate::Error::Serialization(e.to_string()))?;
        let obj_path: object_store::path::Path = path.as_str().into();
        self.object_store
            .put(&obj_path, Bytes::from(json).into())
            .await?;
        Ok(())
    }

    /// Load split progress for a shard (returns None if no in-flight split).
    pub async fn load_progress(&self, old_shard: &str) -> Result<Option<SplitProgress>> {
        let path = Self::progress_path(old_shard);
        let obj_path: object_store::path::Path = path.as_str().into();
        match self.object_store.get(&obj_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let progress: SplitProgress = serde_json::from_slice(&bytes)
                    .map_err(|e| crate::Error::Serialization(e.to_string()))?;
                Ok(Some(progress))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn remove_progress(&self, old_shard: &str) -> Result<()> {
        let path = Self::progress_path(old_shard);
        let obj_path: object_store::path::Path = path.as_str().into();
        match self.object_store.delete(&obj_path).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    // ── Public API ───────────────────────────────────────────────────

    /// Execute full 5-phase split with persistent progress tracking.
    ///
    /// If the process crashes at any point, call `resume_split()` with
    /// the same `old_shard` to continue from where it left off.
    pub async fn execute_split_with_monitoring(&self, shard: &ShardMetadata) -> Result<()> {
        let shard_id = &shard.shard_id;
        info!("Starting 5-phase split for shard: {}", shard_id);

        // Phase 1: Preparation
        let (new_shard_a, new_shard_b) = self.split_shard(shard).await?;
        let split_point = self.calculate_split_point(shard);

        // Persist initial progress before any metadata changes
        let mut progress = SplitProgress::new(
            shard_id,
            vec![new_shard_a.clone(), new_shard_b.clone()],
            split_point.clone(),
        );
        self.persist_progress(&progress).await?;

        // Store split state in metadata
        self.metadata
            .start_split(
                shard_id,
                vec![new_shard_a.clone(), new_shard_b.clone()],
                split_point.clone(),
            )
            .await?;

        progress.completed_phase = Some(SplitPhase::Preparation);
        self.persist_progress(&progress).await?;
        info!(
            "Phase 1/5 complete: Created shards {} and {}",
            new_shard_a, new_shard_b
        );

        // Run remaining phases
        self.run_from_phase(&mut progress, SplitPhase::DualWrite)
            .await
    }

    /// Execute full 5-phase split (legacy API, delegates to monitored version)
    pub async fn execute_split(&self, shard: &ShardMetadata) -> Result<()> {
        self.execute_split_with_monitoring(shard).await
    }

    /// Resume an incomplete split after a crash.
    ///
    /// Returns `true` if a split was found and resumed, `false` if there
    /// was nothing to resume.
    pub async fn resume_split(&self, old_shard: &str) -> Result<bool> {
        let mut progress = match self.load_progress(old_shard).await? {
            Some(p) => p,
            None => return Ok(false),
        };

        let next = match progress.next_phase() {
            Some(p) => p,
            None => {
                // All phases done, just clean up the progress file
                self.remove_progress(old_shard).await?;
                return Ok(true);
            }
        };

        info!(
            "Resuming split for shard {} (fence={}) from phase {:?}",
            old_shard, progress.fence_token, next
        );

        self.run_from_phase(&mut progress, next).await?;
        Ok(true)
    }

    // ── Phase execution engine ───────────────────────────────────────

    /// Execute phases starting from `start_phase` through Cleanup.
    async fn run_from_phase(
        &self,
        progress: &mut SplitProgress,
        start_phase: SplitPhase,
    ) -> Result<()> {
        let old_shard = progress.old_shard.clone();
        let phases = [
            SplitPhase::DualWrite,
            SplitPhase::Backfill,
            SplitPhase::Cutover,
            SplitPhase::Cleanup,
        ];

        for &phase in &phases {
            if (phase as u8) < (start_phase as u8) {
                continue; // Skip already-completed phases
            }

            match phase {
                SplitPhase::DualWrite => {
                    info!("Phase 2/5: Dual-write enabled");
                    self.metadata
                        .update_split_progress(&old_shard, 0.0, SplitPhase::DualWrite)
                        .await?;
                    // Give ingesters time to discover the split state
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    progress.completed_phase = Some(SplitPhase::DualWrite);
                    self.persist_progress(progress).await?;
                    info!("Phase 2/5 complete");
                }
                SplitPhase::Backfill => {
                    info!("Phase 3/5: Backfilling historical data");
                    self.run_backfill_with_progress(progress).await?;
                    progress.completed_phase = Some(SplitPhase::Backfill);
                    self.persist_progress(progress).await?;
                    info!("Phase 3/5 complete");
                }
                SplitPhase::Cutover => {
                    info!("Phase 4/5: Atomic cutover");
                    self.run_cutover(progress).await?;
                    progress.completed_phase = Some(SplitPhase::Cutover);
                    self.persist_progress(progress).await?;
                    info!("Phase 4/5 complete");
                }
                SplitPhase::Cleanup => {
                    info!("Phase 5/5: Cleanup (grace period: 300s)");
                    self.cleanup(&old_shard, Duration::from_secs(300)).await?;
                    progress.completed_phase = Some(SplitPhase::Cleanup);
                    // Remove progress file — split is fully done
                    self.remove_progress(&old_shard).await?;
                    info!("Phase 5/5 complete");
                }
                SplitPhase::Preparation => {} // Handled before run_from_phase
            }
        }

        info!(
            "Split complete: {} -> {} + {}",
            old_shard, progress.new_shards[0], progress.new_shards[1]
        );
        Ok(())
    }

    /// Phase 4 cutover with per-step fencing for crash safety.
    async fn run_cutover(&self, progress: &mut SplitProgress) -> Result<()> {
        let old_shard = &progress.old_shard.clone();

        let split_state = self
            .metadata
            .get_split_state(old_shard)
            .await?
            .ok_or_else(|| crate::Error::Internal("No split in progress".to_string()))?;

        if split_state.new_shards.len() != 2 {
            return Err(crate::Error::Internal(format!(
                "Invalid split state: expected 2 new shards, got {}",
                split_state.new_shards.len()
            )));
        }

        if split_state.backfill_progress < 1.0 {
            return Err(crate::Error::Internal(format!(
                "Backfill only {:.1}% complete",
                split_state.backfill_progress * 100.0
            )));
        }

        let old_metadata = self
            .metadata
            .get_shard_metadata(old_shard)
            .await?
            .ok_or_else(|| crate::Error::ShardNotFound(old_shard.to_string()))?;

        let current_generation = old_metadata.generation;
        let split_ts =
            i64::from_be_bytes(split_state.split_point[..8].try_into().unwrap_or([0u8; 8]));

        // Step 1: Create shard A (idempotent — skipped if already done)
        if !progress.shard_a_created {
            let new_shard_a = ShardMetadata {
                shard_id: split_state.new_shards[0].clone(),
                generation: 0,
                key_range: (
                    old_metadata.key_range.0.clone(),
                    split_state.split_point.clone(),
                ),
                replicas: old_metadata.replicas.clone(),
                state: ShardState::Active,
                min_time: old_metadata.min_time,
                max_time: split_ts,
            };
            self.metadata
                .update_shard_metadata(&new_shard_a.shard_id, &new_shard_a, 0)
                .await?;
            progress.shard_a_created = true;
            self.persist_progress(progress).await?;
        }

        // Step 2: Create shard B
        if !progress.shard_b_created {
            let new_shard_b = ShardMetadata {
                shard_id: split_state.new_shards[1].clone(),
                generation: 0,
                key_range: (
                    split_state.split_point.clone(),
                    old_metadata.key_range.1.clone(),
                ),
                replicas: old_metadata.replicas.clone(),
                state: ShardState::Active,
                min_time: split_ts,
                max_time: old_metadata.max_time,
            };
            self.metadata
                .update_shard_metadata(&new_shard_b.shard_id, &new_shard_b, 0)
                .await?;
            progress.shard_b_created = true;
            self.persist_progress(progress).await?;
        }

        // Step 3: Deactivate old shard
        if !progress.old_shard_deactivated {
            let mut old_updated = old_metadata.clone();
            old_updated.state = ShardState::PendingDeletion {
                delete_after: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                    + 300_000_000_000, // 5 minutes
            };
            self.metadata
                .update_shard_metadata(old_shard, &old_updated, current_generation)
                .await?;
            progress.old_shard_deactivated = true;
            self.persist_progress(progress).await?;
        }

        // All cutover steps done — complete the split in metadata
        self.metadata.complete_split(old_shard).await?;

        info!(
            "Cutover complete (fence={}). New shards: {}, {}",
            progress.fence_token, progress.new_shards[0], progress.new_shards[1]
        );
        Ok(())
    }

    // ── Individual phase implementations ─────────────────────────────

    /// Split a hot shard into two (Phase 1: Preparation)
    pub async fn split_shard(&self, shard: &ShardMetadata) -> Result<(ShardId, ShardId)> {
        let split_point = self.calculate_split_point(shard);

        let new_shard_a = ShardId::from(uuid::Uuid::new_v4().to_string());
        let new_shard_b = ShardId::from(uuid::Uuid::new_v4().to_string());

        let _pending_shards = [
            PendingShard {
                id: new_shard_a.clone(),
                range: (shard.key_range.0.clone(), split_point.clone()),
            },
            PendingShard {
                id: new_shard_b.clone(),
                range: (split_point, shard.key_range.1.clone()),
            },
        ];

        info!(
            "Phase 1: Created new shards {} and {} for split of {}",
            new_shard_a, new_shard_b, shard.shard_id
        );

        Ok((new_shard_a, new_shard_b))
    }

    /// Run backfill phase: copy historical data to new shards (Phase 3)
    pub async fn run_backfill(
        &self,
        old_shard: &str,
        new_shards: &[String],
        split_point: &[u8],
    ) -> Result<()> {
        let mut progress = match self.load_progress(old_shard).await? {
            Some(progress) => progress,
            None => {
                // Standalone backfill calls (outside execute_split_with_monitoring) still
                // need persisted chunk-level progress for resume/idempotency.
                let mut progress =
                    SplitProgress::new(old_shard, new_shards.to_vec(), split_point.to_vec());
                progress.completed_phase = Some(SplitPhase::DualWrite);
                self.persist_progress(&progress).await?;
                progress
            }
        };

        if progress.new_shards != new_shards || progress.split_point != split_point {
            warn!(
                "run_backfill input differs from persisted split progress for {}; using persisted values",
                old_shard
            );
        }

        self.run_backfill_with_progress(&mut progress).await
    }

    async fn run_backfill_with_progress(&self, progress: &mut SplitProgress) -> Result<()> {
        let old_shard = progress.old_shard.clone();
        info!(
            "Phase 3: Starting backfill for shard {} -> {:?}",
            old_shard, progress.new_shards
        );

        let mut chunks = self.metadata.get_chunks_for_shard(&old_shard).await?;
        chunks.sort_by(|a, b| a.chunk_path.cmp(&b.chunk_path));

        let current_chunk_paths: BTreeSet<String> = chunks
            .iter()
            .map(|chunk| chunk.chunk_path.clone())
            .collect();
        progress
            .backfilled_chunks
            .retain(|path| current_chunk_paths.contains(path));

        let total_chunks = chunks.len();
        progress.backfill_total_chunks = total_chunks;
        self.persist_progress(progress).await?;

        if total_chunks == 0 {
            info!("No chunks to backfill for shard {}", old_shard);
            self.metadata
                .update_split_progress(&old_shard, 1.0, SplitPhase::Backfill)
                .await?;
            return Ok(());
        }

        let mut completed_chunks = progress.backfilled_chunks.len();
        let initial_progress = completed_chunks as f64 / total_chunks as f64;
        self.metadata
            .update_split_progress(&old_shard, initial_progress, SplitPhase::Backfill)
            .await?;

        if completed_chunks == total_chunks {
            info!("Backfill already complete for shard {}", old_shard);
            return Ok(());
        }

        for chunk_entry in &chunks {
            if progress.backfilled_chunks.contains(&chunk_entry.chunk_path) {
                continue;
            }

            let path: object_store::path::Path = chunk_entry.chunk_path.as_str().into();
            let bytes = self.object_store.get(&path).await?.bytes().await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?
                .with_batch_size(8192)
                .build()?;

            for (batch_idx, batch_result) in reader.enumerate() {
                let batch = batch_result?;
                let (batch_a, batch_b) = self.split_batch(&batch, &progress.split_point)?;

                if batch_a.num_rows() > 0 {
                    let path_a = Self::backfill_chunk_path(
                        &progress.new_shards[0],
                        &chunk_entry.chunk_path,
                        batch_idx,
                        "a",
                    );
                    self.write_chunk_to_path(&path_a, batch_a).await?;
                }
                if batch_b.num_rows() > 0 {
                    let path_b = Self::backfill_chunk_path(
                        &progress.new_shards[1],
                        &chunk_entry.chunk_path,
                        batch_idx,
                        "b",
                    );
                    self.write_chunk_to_path(&path_b, batch_b).await?;
                }
            }

            progress
                .backfilled_chunks
                .insert(chunk_entry.chunk_path.clone());
            self.persist_progress(progress).await?;

            completed_chunks += 1;
            let progress_fraction = completed_chunks as f64 / total_chunks as f64;
            self.metadata
                .update_split_progress(&old_shard, progress_fraction, SplitPhase::Backfill)
                .await?;

            if completed_chunks % 10 == 0 || completed_chunks == total_chunks {
                info!(
                    "Backfill progress: {:.1}% ({}/{})",
                    progress_fraction * 100.0,
                    completed_chunks,
                    total_chunks,
                );
            }
        }

        info!("Backfill complete for shard {}", old_shard);
        Ok(())
    }

    /// Public cutover (delegates to the fenced implementation)
    pub async fn cutover(&self, old_shard: &str) -> Result<()> {
        // Load or create progress for standalone cutover calls
        let mut progress = match self.load_progress(old_shard).await? {
            Some(p) => p,
            None => {
                let split_state = self
                    .metadata
                    .get_split_state(old_shard)
                    .await?
                    .ok_or_else(|| crate::Error::Internal("No split in progress".to_string()))?;
                let mut p = SplitProgress::new(
                    old_shard,
                    split_state.new_shards.clone(),
                    split_state.split_point.clone(),
                );
                p.completed_phase = Some(SplitPhase::Backfill);
                self.persist_progress(&p).await?;
                p
            }
        };
        self.run_cutover(&mut progress).await
    }

    /// Clean up old shard data after split (Phase 5)
    pub async fn cleanup(&self, old_shard: &str, grace_period: Duration) -> Result<()> {
        info!(
            "Phase 5: Starting cleanup for old shard {} (grace period: {:?})",
            old_shard, grace_period
        );

        tokio::time::sleep(grace_period).await;

        let chunks = self.metadata.get_chunks_for_shard(old_shard).await?;

        info!("Deleting {} chunks from old shard", chunks.len());

        for chunk in chunks.iter() {
            let path: object_store::path::Path = chunk.chunk_path.as_str().into();

            match self.object_store.delete(&path).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to delete chunk {}: {}", chunk.chunk_path, e);
                }
            }

            if let Err(e) = self.metadata.delete_chunk(&chunk.chunk_path).await {
                warn!(
                    "Failed to delete chunk metadata {}: {}",
                    chunk.chunk_path, e
                );
            }
        }

        info!("Cleanup complete for shard {}", old_shard);
        Ok(())
    }

    // ── Helpers ──────────────────────────────────────────────────────

    /// Split a RecordBatch into two based on timestamp split point
    fn split_batch(
        &self,
        batch: &RecordBatch,
        split_point: &[u8],
    ) -> Result<(RecordBatch, RecordBatch)> {
        let ts_column = batch
            .column_by_name("timestamp")
            .ok_or_else(|| crate::Error::Internal("Missing timestamp column".to_string()))?;

        let ts_array = if let DataType::Int64 = ts_column.data_type() {
            ts_column
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| crate::Error::Internal("Timestamp not Int64".to_string()))?
        } else {
            return Err(crate::Error::Internal(
                "Timestamp column is not Int64".to_string(),
            ));
        };

        let mut indices_a = Vec::new();
        let mut indices_b = Vec::new();

        let split_ts = i64::from_be_bytes(
            split_point
                .try_into()
                .map_err(|_| crate::Error::Internal("Invalid split point".to_string()))?,
        );

        for i in 0..batch.num_rows() {
            let ts = ts_array.value(i);
            if ts < split_ts {
                indices_a.push(i as u32);
            } else {
                indices_b.push(i as u32);
            }
        }

        let indices_a_array = arrow::array::UInt32Array::from(indices_a);
        let indices_b_array = arrow::array::UInt32Array::from(indices_b);

        let batch_a = arrow::compute::take_record_batch(batch, &indices_a_array)?;
        let batch_b = arrow::compute::take_record_batch(batch, &indices_b_array)?;

        Ok((batch_a, batch_b))
    }

    /// Write a batch to an explicit chunk path.
    async fn write_chunk_to_path(&self, path: &str, batch: RecordBatch) -> Result<()> {
        let mut buffer = Vec::new();
        {
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .build();

            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        let object_path: object_store::path::Path = path.into();
        let bytes_len = buffer.len();
        self.object_store
            .put(&object_path, Bytes::from(buffer).into())
            .await?;

        let ts_column = batch
            .column_by_name("timestamp")
            .ok_or_else(|| crate::Error::Internal("Missing timestamp column".to_string()))?;
        let ts_array = ts_column
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| crate::Error::Internal("Timestamp not Int64".to_string()))?;

        let min_timestamp = (0..batch.num_rows())
            .map(|i| ts_array.value(i))
            .min()
            .unwrap_or(0);
        let max_timestamp = (0..batch.num_rows())
            .map(|i| ts_array.value(i))
            .max()
            .unwrap_or(0);

        let metadata = ChunkMetadata {
            path: path.to_string(),
            min_timestamp,
            max_timestamp,
            row_count: batch.num_rows() as u64,
            size_bytes: bytes_len as u64,
        };
        self.metadata.register_chunk(path, &metadata).await?;

        Ok(())
    }

    fn backfill_chunk_path(
        target_shard: &str,
        source_chunk_path: &str,
        batch_idx: usize,
        partition: &str,
    ) -> String {
        let source_key = Self::hex_encode(source_chunk_path.as_bytes());
        format!(
            "{}/backfill_{}_{}_{}.parquet",
            target_shard, source_key, batch_idx, partition
        )
    }

    fn hex_encode(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    }

    /// Calculate the split point based on time
    fn calculate_split_point(&self, shard: &ShardMetadata) -> Vec<u8> {
        let time_range = shard.max_time - shard.min_time;
        let mid_time = shard.min_time + time_range / 2;

        let rounded = TimeBucket::round_to_5min(mid_time);
        rounded.to_be_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::super::ReplicaInfo;
    use super::*;
    use crate::metadata::LocalMetadataClient;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_split_shard() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let shard = ShardMetadata {
            shard_id: "shard-1".to_string(),
            generation: 1,
            key_range: (vec![0, 0, 0, 0], vec![255, 255, 255, 255]),
            replicas: vec![ReplicaInfo {
                replica_id: "replica-1".to_string(),
                node_id: "node-1".to_string(),
                is_leader: true,
            }],
            state: ShardState::Active,
            min_time: 1000000000,
            max_time: 2000000000,
        };

        let (shard_a, shard_b) = splitter.split_shard(&shard).await.unwrap();

        assert_ne!(shard_a, shard_b);
        assert!(!shard_a.is_empty());
        assert!(!shard_b.is_empty());
    }

    #[tokio::test]
    async fn test_split_point_calculation() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let shard = ShardMetadata {
            shard_id: "shard-1".to_string(),
            generation: 1,
            key_range: (vec![0, 0, 0, 0], vec![255, 255, 255, 255]),
            replicas: vec![],
            state: ShardState::Active,
            min_time: 0,
            max_time: 1000000000000, // 1000 seconds
        };

        let split_point = splitter.calculate_split_point(&shard);

        assert!(!split_point.is_empty());
        assert_eq!(split_point.len(), 8); // i64 bytes
    }

    #[tokio::test]
    async fn test_progress_persistence_roundtrip() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let mut progress = SplitProgress::new(
            "shard-old",
            vec!["shard-a".to_string(), "shard-b".to_string()],
            vec![0, 0, 0, 0, 0, 0, 0, 42],
        );
        progress.completed_phase = Some(SplitPhase::DualWrite);
        progress.shard_a_created = true;

        splitter.persist_progress(&progress).await.unwrap();

        let loaded = splitter.load_progress("shard-old").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.fence_token, progress.fence_token);
        assert_eq!(loaded.completed_phase, Some(SplitPhase::DualWrite));
        assert!(loaded.shard_a_created);
        assert!(!loaded.shard_b_created);
        assert!(!loaded.old_shard_deactivated);
        assert_eq!(loaded.new_shards, vec!["shard-a", "shard-b"]);
    }

    #[tokio::test]
    async fn test_progress_removal() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let progress = SplitProgress::new(
            "shard-rm",
            vec!["a".to_string(), "b".to_string()],
            vec![0; 8],
        );
        splitter.persist_progress(&progress).await.unwrap();

        splitter.remove_progress("shard-rm").await.unwrap();

        let loaded = splitter.load_progress("shard-rm").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_load_nonexistent_progress() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let loaded = splitter.load_progress("does-not-exist").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_resume_split_no_progress() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let resumed = splitter.resume_split("nonexistent").await.unwrap();
        assert!(!resumed);
    }

    #[tokio::test]
    async fn test_next_phase_progression() {
        let mut p = SplitProgress::new("s", vec![], vec![]);

        assert_eq!(p.next_phase(), Some(SplitPhase::Preparation));
        p.completed_phase = Some(SplitPhase::Preparation);
        assert_eq!(p.next_phase(), Some(SplitPhase::DualWrite));
        p.completed_phase = Some(SplitPhase::DualWrite);
        assert_eq!(p.next_phase(), Some(SplitPhase::Backfill));
        p.completed_phase = Some(SplitPhase::Backfill);
        assert_eq!(p.next_phase(), Some(SplitPhase::Cutover));
        p.completed_phase = Some(SplitPhase::Cutover);
        assert_eq!(p.next_phase(), Some(SplitPhase::Cleanup));
        p.completed_phase = Some(SplitPhase::Cleanup);
        assert_eq!(p.next_phase(), None);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_progress_is_ok() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        // Should not error
        splitter.remove_progress("never-existed").await.unwrap();
    }
}
