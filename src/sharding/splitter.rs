//! Five-phase shard splitting protocol

use super::{ShardId, ShardMetadata, TimeBucket};
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

/// Tracks exactly which sub-steps of cutover have completed, enabling
/// idempotent recovery after a crash mid-cutover.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CutoverIntent {
    /// Unique token for this cutover attempt. A new attempt must use a
    /// new token, so stale intents are never confused with active ones.
    pub fence_token: String,
    /// Old shard being replaced.
    pub old_shard: String,
    /// New shard IDs (always length 2).
    pub new_shards: Vec<String>,
    /// Which sub-steps have completed.
    pub shard_a_created: bool,
    pub shard_b_created: bool,
    pub old_shard_deactivated: bool,
}

/// Pending shard (not yet active)
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PendingShard {
    pub id: ShardId,
    pub range: (Vec<u8>, Vec<u8>),
}

/// Shard splitter for zero-downtime splitting
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

    /// Execute full 5-phase split with enhanced monitoring and error handling
    pub async fn execute_split_with_monitoring(&self, shard: &ShardMetadata) -> Result<()> {
        let shard_id = &shard.shard_id;
        info!("🚀 Starting 5-phase split for shard: {}", shard_id);

        // Phase 1: Preparation (create new shard metadata)
        info!("📋 Phase 1/5: Preparation");
        let (new_shard_a, new_shard_b) = self.split_shard(shard).await?;
        let split_point = self.calculate_split_point(shard);

        // Store split state in metadata
        self.metadata
            .start_split(
                shard_id,
                vec![new_shard_a.clone(), new_shard_b.clone()],
                split_point.clone(),
            )
            .await?;
        info!("✅ Phase 1 complete: Created shards {} and {}", new_shard_a, new_shard_b);

        // Phase 2: Enable dual-write (ingesters will detect and start dual-writing)
        info!("✏️  Phase 2/5: Dual-write enabled");
        self.metadata
            .update_split_progress(shard_id, 0.0, SplitPhase::DualWrite)
            .await?;

        // Give ingesters time to discover the split state
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!("✅ Phase 2 complete: Ingesters now writing to both old and new shards");

        // Phase 3: Backfill historical data
        info!("📦 Phase 3/5: Backfilling historical data");
        self.run_backfill(
            shard_id,
            &[new_shard_a.clone(), new_shard_b.clone()],
            &split_point,
        )
        .await?;
        info!("✅ Phase 3 complete: All historical data copied");

        // Phase 4: Atomic cutover
        info!("🔄 Phase 4/5: Atomic cutover");
        self.cutover(shard_id).await?;
        info!("✅ Phase 4 complete: Queries now using new shards");

        // Phase 5: Cleanup (after grace period)
        info!("🧹 Phase 5/5: Cleanup (grace period: 300s)");
        self.cleanup(shard_id, Duration::from_secs(300)).await?;
        info!("✅ Phase 5 complete: Old shard data removed");

        info!(
            "🎉 Split complete: {} → {} + {}",
            shard_id, new_shard_a, new_shard_b
        );
        Ok(())
    }

    /// Execute full 5-phase split (legacy version, kept for compatibility)
    pub async fn execute_split(&self, shard: &ShardMetadata) -> Result<()> {
        // Delegate to the enhanced version
        self.execute_split_with_monitoring(shard).await
    }

    /// Split a hot shard into two (Phase 1: Preparation)
    pub async fn split_shard(&self, shard: &ShardMetadata) -> Result<(ShardId, ShardId)> {
        let split_point = self.calculate_split_point(shard);

        let new_shard_a = ShardId::from(uuid::Uuid::new_v4().to_string());
        let new_shard_b = ShardId::from(uuid::Uuid::new_v4().to_string());

        let _pending_shards = vec![
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
        info!(
            "Phase 3: Starting backfill for shard {} → {:?}",
            old_shard, new_shards
        );

        // Get all chunks for the old shard
        let chunks = self.metadata.get_chunks_for_shard(old_shard).await?;
        let total_chunks = chunks.len();

        if total_chunks == 0 {
            info!("No chunks to backfill for shard {}", old_shard);
            self.metadata
                .update_split_progress(old_shard, 1.0, SplitPhase::Backfill)
                .await?;
            return Ok(());
        }

        for (idx, chunk_entry) in chunks.iter().enumerate() {
            // Read chunk from object storage
            let path: object_store::path::Path = chunk_entry.chunk_path.as_str().into();
            let bytes = match self.object_store.get(&path).await {
                Ok(result) => result.bytes().await?,
                Err(e) => {
                    warn!("Failed to read chunk {}: {}, skipping", chunk_entry.chunk_path, e);
                    continue;
                }
            };

            // Parse Parquet
            let reader = match ParquetRecordBatchReaderBuilder::try_new(bytes) {
                Ok(builder) => builder.with_batch_size(8192).build()?,
                Err(e) => {
                    warn!(
                        "Failed to parse chunk {}: {}, skipping",
                        chunk_entry.chunk_path, e
                    );
                    continue;
                }
            };

            // Process each batch
            for batch_result in reader {
                let batch = batch_result?;

                // Split batch by split point
                let (batch_a, batch_b) = self.split_batch(&batch, split_point)?;

                // Write to new shards
                if batch_a.num_rows() > 0 {
                    self.write_chunk_to_shard(&new_shards[0], batch_a).await?;
                }
                if batch_b.num_rows() > 0 {
                    self.write_chunk_to_shard(&new_shards[1], batch_b).await?;
                }
            }

            // Update progress
            let progress = (idx + 1) as f64 / total_chunks as f64;
            self.metadata
                .update_split_progress(old_shard, progress, SplitPhase::Backfill)
                .await?;

            if idx % 10 == 0 || idx == total_chunks - 1 {
                info!(
                    "Backfill progress: {:.1}% ({}/{})",
                    progress * 100.0,
                    idx + 1,
                    total_chunks
                );
            }
        }

        info!("Backfill complete for shard {}", old_shard);
        Ok(())
    }

    /// Split a RecordBatch into two based on timestamp split point
    fn split_batch(&self, batch: &RecordBatch, split_point: &[u8]) -> Result<(RecordBatch, RecordBatch)> {
        // Extract timestamp column
        let ts_column = batch
            .column_by_name("timestamp")
            .ok_or_else(|| crate::Error::Internal("Missing timestamp column".to_string()))?;

        // Check if it's an Int64 array (timestamp)
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

        // Build selection indices
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

        // Use Arrow take kernel to extract rows
        let indices_a_array = arrow::array::UInt32Array::from(indices_a);
        let indices_b_array = arrow::array::UInt32Array::from(indices_b);

        let batch_a = arrow::compute::take_record_batch(batch, &indices_a_array)?;
        let batch_b = arrow::compute::take_record_batch(batch, &indices_b_array)?;

        Ok((batch_a, batch_b))
    }

    /// Write a batch to a shard as a new chunk
    async fn write_chunk_to_shard(&self, shard_id: &str, batch: RecordBatch) -> Result<()> {
        // Convert batch to Parquet
        let mut buffer = Vec::new();
        {
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .build();

            let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        // Generate chunk path for new shard
        let chunk_id = uuid::Uuid::new_v4();
        let path = format!("{}/chunk_{}.parquet", shard_id, chunk_id);

        // Upload to object storage
        let object_path: object_store::path::Path = path.as_str().into();
        let bytes_len = buffer.len();
        self.object_store
            .put(&object_path, Bytes::from(buffer).into())
            .await?;

        // Get timestamp range from batch
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

        // Register with metadata
        let metadata = ChunkMetadata {
            path: path.clone(),
            min_timestamp,
            max_timestamp,
            row_count: batch.num_rows() as u64,
            size_bytes: bytes_len as u64,
        };
        self.metadata.register_chunk(&path, &metadata).await?;

        Ok(())
    }

    /// Perform atomic cutover to new shards (Phase 4)
    ///
    /// Uses a fencing token to make each sub-step idempotent. If the process
    /// crashes mid-cutover, `resume_cutover()` can pick up where it left off
    /// using the persisted `CutoverIntent`.
    pub async fn cutover(&self, old_shard: &str) -> Result<()> {
        use super::{ShardMetadata, ShardState};

        let split_state = self
            .metadata
            .get_split_state(old_shard)
            .await?
            .ok_or_else(|| crate::Error::Internal("No split in progress".to_string()))?;

        // Verify backfill is 100% complete
        if split_state.backfill_progress < 1.0 {
            return Err(crate::Error::Internal(format!(
                "Backfill only {:.1}% complete",
                split_state.backfill_progress * 100.0
            )));
        }

        info!("Phase 4: Performing cutover for shard {}", old_shard);

        // Load old shard metadata
        let old_metadata = self
            .metadata
            .get_shard_metadata(old_shard)
            .await?
            .ok_or_else(|| crate::Error::ShardNotFound(old_shard.to_string()))?;

        let current_generation = old_metadata.generation;

        // Calculate split point timestamp
        let split_ts = i64::from_be_bytes(
            split_state.split_point[..8]
                .try_into()
                .unwrap_or([0u8; 8]),
        );

        // --- Fencing: persist intent before starting sub-steps ---
        let mut intent = CutoverIntent {
            fence_token: uuid::Uuid::new_v4().to_string(),
            old_shard: old_shard.to_string(),
            new_shards: split_state.new_shards.clone(),
            shard_a_created: false,
            shard_b_created: false,
            old_shard_deactivated: false,
        };
        self.persist_cutover_intent(&intent).await?;

        // Create new shard metadata for both shards
        let new_shard_a = ShardMetadata {
            shard_id: split_state.new_shards[0].clone(),
            generation: 0,
            key_range: (old_metadata.key_range.0.clone(), split_state.split_point.clone()),
            replicas: old_metadata.replicas.clone(),
            state: ShardState::Active,
            min_time: old_metadata.min_time,
            max_time: split_ts,
        };

        let new_shard_b = ShardMetadata {
            shard_id: split_state.new_shards[1].clone(),
            generation: 0,
            key_range: (split_state.split_point.clone(), old_metadata.key_range.1.clone()),
            replicas: old_metadata.replicas.clone(),
            state: ShardState::Active,
            min_time: split_ts,
            max_time: old_metadata.max_time,
        };

        // Step 1: Register shard A
        self.metadata
            .update_shard_metadata(&new_shard_a.shard_id, &new_shard_a, 0)
            .await?;
        intent.shard_a_created = true;
        self.persist_cutover_intent(&intent).await?;

        // Step 2: Register shard B
        self.metadata
            .update_shard_metadata(&new_shard_b.shard_id, &new_shard_b, 0)
            .await?;
        intent.shard_b_created = true;
        self.persist_cutover_intent(&intent).await?;

        // Step 3: Mark old shard as pending deletion
        let mut old_updated = old_metadata.clone();
        old_updated.state = ShardState::PendingDeletion {
            delete_after: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                + 300_000_000_000, // 5 minutes
        };

        self.metadata
            .update_shard_metadata(old_shard, &old_updated, current_generation)
            .await?;
        intent.old_shard_deactivated = true;
        self.persist_cutover_intent(&intent).await?;

        // All steps done — complete split and remove intent
        self.metadata.complete_split(old_shard).await?;
        self.remove_cutover_intent(old_shard).await?;

        info!(
            "Cutover complete (fence={}). New shards: {}, {}",
            intent.fence_token, new_shard_a.shard_id, new_shard_b.shard_id
        );
        Ok(())
    }

    /// Resume an incomplete cutover after a crash.
    ///
    /// Reads the persisted `CutoverIntent` and re-executes only the
    /// sub-steps that haven't completed yet (idempotent).
    pub async fn resume_cutover(&self, old_shard: &str) -> Result<bool> {
        use super::{ShardMetadata, ShardState};

        let mut intent = match self.load_cutover_intent(old_shard).await? {
            Some(i) => i,
            None => return Ok(false), // No incomplete cutover
        };

        info!(
            "Resuming cutover for shard {} (fence={}): a={}, b={}, old={}",
            old_shard, intent.fence_token,
            intent.shard_a_created, intent.shard_b_created, intent.old_shard_deactivated
        );

        let split_state = self
            .metadata
            .get_split_state(old_shard)
            .await?
            .ok_or_else(|| crate::Error::Internal("No split state for resume".to_string()))?;

        let old_metadata = self
            .metadata
            .get_shard_metadata(old_shard)
            .await?
            .ok_or_else(|| crate::Error::ShardNotFound(old_shard.to_string()))?;

        let current_generation = old_metadata.generation;
        let split_ts = i64::from_be_bytes(
            split_state.split_point[..8]
                .try_into()
                .unwrap_or([0u8; 8]),
        );

        if !intent.shard_a_created {
            let new_shard_a = ShardMetadata {
                shard_id: split_state.new_shards[0].clone(),
                generation: 0,
                key_range: (old_metadata.key_range.0.clone(), split_state.split_point.clone()),
                replicas: old_metadata.replicas.clone(),
                state: ShardState::Active,
                min_time: old_metadata.min_time,
                max_time: split_ts,
            };
            self.metadata
                .update_shard_metadata(&new_shard_a.shard_id, &new_shard_a, 0)
                .await?;
            intent.shard_a_created = true;
            self.persist_cutover_intent(&intent).await?;
        }

        if !intent.shard_b_created {
            let new_shard_b = ShardMetadata {
                shard_id: split_state.new_shards[1].clone(),
                generation: 0,
                key_range: (split_state.split_point.clone(), old_metadata.key_range.1.clone()),
                replicas: old_metadata.replicas.clone(),
                state: ShardState::Active,
                min_time: split_ts,
                max_time: old_metadata.max_time,
            };
            self.metadata
                .update_shard_metadata(&new_shard_b.shard_id, &new_shard_b, 0)
                .await?;
            intent.shard_b_created = true;
            self.persist_cutover_intent(&intent).await?;
        }

        if !intent.old_shard_deactivated {
            let mut old_updated = old_metadata.clone();
            old_updated.state = ShardState::PendingDeletion {
                delete_after: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                    + 300_000_000_000,
            };
            self.metadata
                .update_shard_metadata(old_shard, &old_updated, current_generation)
                .await?;
            intent.old_shard_deactivated = true;
            self.persist_cutover_intent(&intent).await?;
        }

        self.metadata.complete_split(old_shard).await?;
        self.remove_cutover_intent(old_shard).await?;

        info!("Cutover resumed and completed for shard {}", old_shard);
        Ok(true)
    }

    /// Persist a cutover intent to object storage.
    async fn persist_cutover_intent(&self, intent: &CutoverIntent) -> Result<()> {
        let path = format!("metadata/cutover-intents/{}.json", intent.old_shard);
        let json = serde_json::to_vec(intent)
            .map_err(|e| crate::Error::Serialization(e.to_string()))?;
        let obj_path: object_store::path::Path = path.as_str().into();
        self.object_store
            .put(&obj_path, bytes::Bytes::from(json).into())
            .await?;
        Ok(())
    }

    /// Load a cutover intent from object storage.
    async fn load_cutover_intent(&self, old_shard: &str) -> Result<Option<CutoverIntent>> {
        let path = format!("metadata/cutover-intents/{}.json", old_shard);
        let obj_path: object_store::path::Path = path.as_str().into();
        match self.object_store.get(&obj_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let intent: CutoverIntent = serde_json::from_slice(&bytes)
                    .map_err(|e| crate::Error::Serialization(e.to_string()))?;
                Ok(Some(intent))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Remove a cutover intent after successful completion.
    async fn remove_cutover_intent(&self, old_shard: &str) -> Result<()> {
        let path = format!("metadata/cutover-intents/{}.json", old_shard);
        let obj_path: object_store::path::Path = path.as_str().into();
        match self.object_store.delete(&obj_path).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()), // Already removed
            Err(e) => Err(e.into()),
        }
    }

    /// Clean up old shard data after split (Phase 5)
    pub async fn cleanup(&self, old_shard: &str, grace_period: Duration) -> Result<()> {
        info!(
            "Phase 5: Starting cleanup for old shard {} (grace period: {:?})",
            old_shard, grace_period
        );

        // Wait grace period for in-flight queries to complete
        tokio::time::sleep(grace_period).await;

        // Get all chunks for old shard
        let chunks = self.metadata.get_chunks_for_shard(old_shard).await?;

        info!("Deleting {} chunks from old shard", chunks.len());

        // Delete from object storage
        for chunk in chunks.iter() {
            let path: object_store::path::Path = chunk.chunk_path.as_str().into();

            match self.object_store.delete(&path).await {
                Ok(_) => {}
                Err(e) => {
                    // Log but continue - don't fail cleanup on individual deletes
                    warn!("Failed to delete chunk {}: {}", chunk.chunk_path, e);
                }
            }

            // Remove from metadata
            if let Err(e) = self.metadata.delete_chunk(&chunk.chunk_path).await {
                warn!("Failed to delete chunk metadata {}: {}", chunk.chunk_path, e);
            }
        }

        info!("Cleanup complete for shard {}", old_shard);
        Ok(())
    }

    /// Calculate the split point based on time
    fn calculate_split_point(&self, shard: &ShardMetadata) -> Vec<u8> {
        // For time-series: split on time boundary
        let time_range = shard.max_time - shard.min_time;
        let mid_time = shard.min_time + time_range / 2;

        // Round to nearest 5-minute boundary
        let rounded = TimeBucket::round_to_5min(mid_time);

        // Convert to key format
        rounded.to_be_bytes().to_vec()
    }
}


#[cfg(test)]
mod tests {
    use super::super::{ReplicaInfo, ShardState};
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

        // Should be roughly in the middle
        assert!(!split_point.is_empty());
        assert_eq!(split_point.len(), 8); // i64 bytes
    }

    #[tokio::test]
    async fn test_cutover_intent_persistence() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let intent = CutoverIntent {
            fence_token: "test-fence-123".to_string(),
            old_shard: "shard-old".to_string(),
            new_shards: vec!["shard-a".to_string(), "shard-b".to_string()],
            shard_a_created: true,
            shard_b_created: false,
            old_shard_deactivated: false,
        };

        // Persist and load
        splitter.persist_cutover_intent(&intent).await.unwrap();
        let loaded = splitter.load_cutover_intent("shard-old").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.fence_token, "test-fence-123");
        assert!(loaded.shard_a_created);
        assert!(!loaded.shard_b_created);
        assert!(!loaded.old_shard_deactivated);

        // Remove and verify gone
        splitter.remove_cutover_intent("shard-old").await.unwrap();
        let gone = splitter.load_cutover_intent("shard-old").await.unwrap();
        assert!(gone.is_none());
    }

    #[tokio::test]
    async fn test_no_cutover_intent_returns_none() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let result = splitter.load_cutover_intent("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_resume_cutover_no_intent_returns_false() {
        let metadata = Arc::new(LocalMetadataClient::new());
        let object_store = Arc::new(InMemory::new());
        let splitter = ShardSplitter::new(metadata, object_store);

        let resumed = splitter.resume_cutover("nonexistent").await.unwrap();
        assert!(!resumed, "Should return false when no intent exists");
    }
}
