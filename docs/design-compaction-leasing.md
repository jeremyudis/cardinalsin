# Design: Compaction Chunk Leasing

**Date**: 2026-02-06
**Author**: Distributed Systems Architect Agent
**Status**: Proposal
**Addresses**: Architecture Audit Finding 10 (No Compaction Mutual Exclusion)

---

## 1. Problem Statement

The compactor has no mechanism to prevent two instances from selecting and compacting the same chunks simultaneously. When concurrent compactors call `get_l0_candidates()` or `get_level_candidates()`, they receive the same list of chunks. Both proceed to read, merge, and upload new chunks. When both call `complete_compaction()`, the first succeeds via CAS and the second gets an `Error::Conflict`. The second compactor's merged chunk is orphaned on S3.

This wastes compute, network bandwidth, and S3 storage. Under heavy compaction load, the wasted work can exceed the useful work.

See Architecture Audit Finding 10 for the full analysis.

---

## 2. Can We Use S3 ETags Alone?

**Short answer**: Yes, with a carefully designed protocol. No external lock service is required.

**Rationale**: S3 conditional PUTs (ETag-based CAS) provide compare-and-swap semantics on individual objects. This is sufficient for implementing a lease mechanism as long as:

1. The lease state is stored in an S3 object with its own ETag.
2. Lease acquisition uses conditional PUT to atomically claim chunks.
3. Lease renewal and expiry use timestamps embedded in the lease object.

An external lock service (etcd, ZooKeeper, DynamoDB) would provide stronger primitives (TTL-based leases with server-enforced expiry), but adds operational complexity and a new dependency. Since CardinalSin already depends on S3 CAS for all metadata operations, extending this pattern to compaction leasing is consistent and avoids new infrastructure.

**Limitation**: S3-based leases rely on the lease holder to renew or release. If a compactor crashes without releasing its lease, the lease expires after the TTL (based on wall-clock time). Clock skew between compactors could cause premature expiry or zombie leases. This is acceptable for compaction (the cost of a duplicate compaction is wasted work, not data loss).

---

## 3. Lease Design

### 3.1 Lease Object

Store compaction leases in a dedicated S3 object: `metadata/compaction-leases.json`

```json
{
  "leases": {
    "lease-abc123": {
      "lease_id": "lease-abc123",
      "holder_id": "compactor-node-1",
      "chunks": ["chunk_a.parquet", "chunk_b.parquet", "chunk_c.parquet"],
      "acquired_at": "2026-02-06T10:00:00Z",
      "expires_at": "2026-02-06T10:05:00Z",
      "level": 0,
      "status": "active"
    }
  }
}
```

### 3.2 Rust Types

```rust
/// Compaction lease for mutual exclusion
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionLease {
    /// Unique lease ID
    pub lease_id: String,
    /// ID of the compactor holding this lease
    pub holder_id: String,
    /// Chunks claimed by this lease
    pub chunks: Vec<String>,
    /// When the lease was acquired
    pub acquired_at: chrono::DateTime<chrono::Utc>,
    /// When the lease expires (must be renewed before this)
    pub expires_at: chrono::DateTime<chrono::Utc>,
    /// Compaction level being targeted
    pub level: u32,
    /// Lease status
    pub status: LeaseStatus,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LeaseStatus {
    /// Lease is active, chunks are being compacted
    Active,
    /// Compaction completed successfully
    Completed,
    /// Compaction failed, chunks should be released
    Failed,
}

/// All compaction leases
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct CompactionLeases {
    pub leases: HashMap<String, CompactionLease>,
}
```

### 3.3 Lease Lifecycle

```
acquire_lease() --> [Active] --> complete_lease() --> [Completed] --> cleanup
                        |
                        +--> fail_lease() --> [Failed] --> cleanup
                        |
                        +--> (crash/timeout) --> [Expired] --> scavenged by other compactors
```

---

## 4. Protocol

### 4.1 Lease Acquisition

Before starting compaction, the compactor must acquire a lease on the target chunks:

```rust
async fn acquire_lease(
    &self,
    chunks: &[String],
    level: u32,
) -> Result<CompactionLease> {
    let lease_ttl = Duration::from_secs(300); // 5 minutes

    for retry in 0..MAX_CAS_RETRIES {
        // Load current leases with ETag
        let (mut leases, etag) = self.load_leases_with_etag().await?;

        // Scavenge expired leases
        let now = chrono::Utc::now();
        leases.leases.retain(|_, lease| {
            lease.expires_at > now || lease.status != LeaseStatus::Active
        });

        // Check if any requested chunks are already leased
        let leased_chunks: HashSet<&str> = leases.leases.values()
            .filter(|l| l.status == LeaseStatus::Active && l.expires_at > now)
            .flat_map(|l| l.chunks.iter().map(|c| c.as_str()))
            .collect();

        let conflicts: Vec<&str> = chunks.iter()
            .filter(|c| leased_chunks.contains(c.as_str()))
            .map(|c| c.as_str())
            .collect();

        if !conflicts.is_empty() {
            debug!("Chunks already leased: {:?}, skipping", conflicts);
            return Err(Error::ChunksAlreadyLeased(conflicts.iter().map(|s| s.to_string()).collect()));
        }

        // Create new lease
        let lease = CompactionLease {
            lease_id: uuid::Uuid::new_v4().to_string(),
            holder_id: self.node_id.clone(),
            chunks: chunks.to_vec(),
            acquired_at: now,
            expires_at: now + chrono::Duration::from_std(lease_ttl).unwrap(),
            level,
            status: LeaseStatus::Active,
        };

        leases.leases.insert(lease.lease_id.clone(), lease.clone());

        // Atomic save with CAS
        match self.atomic_save_leases(&leases, etag).await {
            Ok(_) => return Ok(lease),
            Err(Error::Conflict) => {
                let backoff_ms = BASE_BACKOFF_MS * 2u64.pow(retry);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Err(Error::TooManyRetries)
}
```

### 4.2 Lease Completion

After successful compaction:

```rust
async fn complete_lease(&self, lease_id: &str) -> Result<()> {
    // CAS loop: update lease status to Completed
    for retry in 0..MAX_CAS_RETRIES {
        let (mut leases, etag) = self.load_leases_with_etag().await?;

        if let Some(lease) = leases.leases.get_mut(lease_id) {
            lease.status = LeaseStatus::Completed;
        } else {
            // Lease was scavenged (expired) -- compaction result may be orphaned
            warn!("Lease {} not found during completion -- may have expired", lease_id);
            return Ok(());
        }

        match self.atomic_save_leases(&leases, etag).await {
            Ok(_) => return Ok(()),
            Err(Error::Conflict) => {
                let backoff_ms = BASE_BACKOFF_MS * 2u64.pow(retry);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Err(Error::TooManyRetries)
}
```

### 4.3 Lease Failure

On compaction error:

```rust
async fn fail_lease(&self, lease_id: &str) -> Result<()> {
    // Same as complete_lease but sets status to Failed
    // Failed leases are cleaned up by the periodic scavenger
}
```

### 4.4 Lease Renewal

For long-running compactions that may exceed the TTL:

```rust
async fn renew_lease(&self, lease_id: &str) -> Result<()> {
    let extension = Duration::from_secs(300); // Extend by 5 minutes

    for retry in 0..MAX_CAS_RETRIES {
        let (mut leases, etag) = self.load_leases_with_etag().await?;

        if let Some(lease) = leases.leases.get_mut(lease_id) {
            if lease.status != LeaseStatus::Active {
                return Err(Error::Internal("Cannot renew non-active lease".into()));
            }
            lease.expires_at = chrono::Utc::now()
                + chrono::Duration::from_std(extension).unwrap();
        } else {
            return Err(Error::Internal(format!("Lease {} not found", lease_id)));
        }

        match self.atomic_save_leases(&leases, etag).await {
            Ok(_) => return Ok(()),
            Err(Error::Conflict) => {
                let backoff_ms = BASE_BACKOFF_MS * 2u64.pow(retry);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Err(Error::TooManyRetries)
}
```

---

## 5. Integration with Compactor

### 5.1 Modified Compaction Flow

Current flow:
```
get_candidates() -> merge_chunks() -> complete_compaction() -> schedule_deletion()
```

New flow:
```
get_candidates() -> filter_unleased() -> acquire_lease() -> merge_chunks()
    -> complete_compaction() -> complete_lease() -> schedule_deletion()
```

### 5.2 Modified `compact_l0()`

```rust
async fn compact_l0(&self) -> Result<()> {
    let candidates = self.metadata
        .get_l0_candidates(self.config.l0_merge_threshold)
        .await?;

    for group in candidates {
        if !self.has_capacity() {
            break;
        }

        // NEW: Acquire lease before compaction
        let lease = match self.acquire_lease(&group, 0).await {
            Ok(lease) => lease,
            Err(Error::ChunksAlreadyLeased(_)) => {
                debug!("Chunks already leased by another compactor, skipping group");
                continue; // Try next group
            }
            Err(e) => return Err(e),
        };

        self.active_compactions.fetch_add(1, Ordering::Relaxed);

        // Spawn a lease renewal task for long-running compactions
        let lease_id = lease.lease_id.clone();
        let renewal_handle = self.spawn_lease_renewal(lease_id.clone());

        let job = CompactionJob { /* ... */ };
        self.metadata.create_compaction_job(job.clone()).await?;

        match self.merge_chunks(&group, Level::L0).await {
            Ok(target_path) => {
                self.metadata.complete_compaction(&group, &target_path).await?;
                self.metadata.update_compaction_status(&job.id, CompactionStatus::Completed).await?;
                self.complete_lease(&lease_id).await?; // NEW

                for path in &group {
                    self.schedule_deletion(path);
                }
            }
            Err(e) => {
                self.metadata.update_compaction_status(&job.id, CompactionStatus::Failed).await?;
                self.fail_lease(&lease_id).await?; // NEW
                error!("L0 compaction failed: {}", e);
            }
        }

        renewal_handle.abort(); // Stop renewal task
        self.active_compactions.fetch_sub(1, Ordering::Relaxed);
    }

    Ok(())
}
```

### 5.3 Lease Renewal Background Task

```rust
fn spawn_lease_renewal(&self, lease_id: String) -> tokio::task::JoinHandle<()> {
    let renewal_interval = Duration::from_secs(120); // Renew every 2 minutes (TTL is 5 minutes)
    let metadata = self.metadata.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(renewal_interval);
        loop {
            interval.tick().await;
            if let Err(e) = /* renew_lease call */ {
                warn!("Failed to renew lease {}: {}", lease_id, e);
                break;
            }
        }
    })
}
```

---

## 6. Candidate Filtering

### 6.1 Filter Already-Leased Chunks

Add a pre-filter step to `get_l0_candidates()` and `get_level_candidates()` that excludes chunks currently under an active lease:

```rust
async fn filter_unleased(&self, groups: Vec<Vec<String>>) -> Result<Vec<Vec<String>>> {
    let (leases, _) = self.load_leases_with_etag().await?;
    let now = chrono::Utc::now();

    let leased_chunks: HashSet<String> = leases.leases.values()
        .filter(|l| l.status == LeaseStatus::Active && l.expires_at > now)
        .flat_map(|l| l.chunks.clone())
        .collect();

    let filtered: Vec<Vec<String>> = groups.into_iter()
        .map(|group| {
            group.into_iter()
                .filter(|chunk| !leased_chunks.contains(chunk))
                .collect::<Vec<_>>()
        })
        .filter(|group| group.len() >= 2) // Still viable after filtering
        .collect();

    Ok(filtered)
}
```

**Alternative approach**: Perform the filtering inside `acquire_lease()` itself (as shown in Section 4.1). This is simpler but means we only discover conflicts at acquisition time, which wastes the candidate selection work. The pre-filter approach is more efficient when many groups are available.

**Recommended**: Use both. Pre-filter for efficiency, then check again atomically inside `acquire_lease()` for correctness (another compactor may have leased chunks between our filter and our acquire).

---

## 7. Contention Analysis

### 7.1 Lease File Contention

The `compaction-leases.json` file will experience CAS contention when multiple compactors acquire or release leases concurrently. Expected pattern:

| Scenario | Writers | Ops/sec | Conflict Rate |
|----------|---------|---------|---------------|
| Single compactor | 1 | ~0.1 | 0% |
| 2 compactors | 2 | ~0.2 | <2% |
| 5 compactors | 5 | ~0.5 | <5% |

With 5 retries and exponential backoff, all operations succeed within the retry budget.

### 7.2 Interaction with Metadata CAS

Lease acquisition and metadata updates (`complete_compaction`) are on separate S3 objects (`compaction-leases.json` vs `metadata.json`/`catalog.json`). They do not contend with each other.

After the metadata merge (Phase C), the compaction flow will be:
1. `acquire_lease()` -- CAS on `compaction-leases.json`
2. `merge_chunks()` -- pure compute + S3 reads/writes (no CAS)
3. `complete_compaction()` -- CAS on `catalog.json`
4. `complete_lease()` -- CAS on `compaction-leases.json`

Steps 1, 3, and 4 are on different files and never conflict with each other.

---

## 8. Edge Cases

### 8.1 Lease Expires During Compaction

If a compaction takes longer than the 5-minute TTL and the renewal task fails (e.g., network partition), the lease expires. Another compactor may scavenge the lease and start compacting the same chunks.

**Outcome**: Both compactors produce merged chunks. The first to call `complete_compaction()` succeeds. The second gets `Error::Conflict` because the source chunks are already removed from metadata. The second compactor's merged chunk is orphaned.

**Mitigation**: The compactor should check its lease is still valid before calling `complete_compaction()`. If the lease has been scavenged, abort and clean up the merged chunk.

```rust
// Before completing compaction:
let leases = self.load_leases().await?;
if !leases.leases.contains_key(&lease_id)
    || leases.leases[&lease_id].status != LeaseStatus::Active
{
    warn!("Lease {} expired or scavenged, aborting compaction", lease_id);
    // Delete the orphaned merged chunk
    self.object_store.delete(&target_path.into()).await?;
    return Ok(());
}
```

### 8.2 Compactor Crashes Without Releasing Lease

The lease expires after 5 minutes. The next compactor run scavenges expired leases and the chunks become available for compaction again.

**Orphaned merged chunk**: If the compactor crashed after uploading the merged chunk but before `complete_compaction()`, the merged chunk exists on S3 but is not in the metadata. This is handled by existing orphan cleanup (or the future orphan reconciliation tool).

### 8.3 Lease File Becomes Corrupt

Same handling as other metadata files -- if `serde_json::from_str` fails, the compactor logs a warning and treats it as an empty lease set. This is safe because:
- Without leases, compactors fall back to the current (no-lease) behavior.
- The CAS on `complete_compaction()` still prevents double-application.
- The only cost is wasted work from duplicate compactions.

### 8.4 Clock Skew Between Compactors

If compactor A's clock is 2 minutes ahead of compactor B's clock:
- A acquires a lease with `expires_at = now_A + 5min`.
- B sees the lease and checks `expires_at > now_B`. Since `now_B` is 2 minutes behind, B sees the lease as valid for 7 minutes.
- This is conservative (B respects the lease longer than intended), which is correct.

The reverse case: B's clock is ahead. B may consider A's lease expired 2 minutes early, causing premature scavenging. With a 5-minute TTL and typical NTP-synchronized clocks (skew <1 second), this is not a practical concern.

**If clock skew is a concern**: Add a `skew_tolerance` of 30 seconds to the expiry check:
```rust
lease.expires_at + Duration::from_secs(30) > now
```

---

## 9. Implementation Checklist

- [ ] Add `CompactionLease`, `LeaseStatus`, `CompactionLeases` types to `src/compactor/mod.rs`
- [ ] Add `compaction_leases_path()` to `S3MetadataClient`
- [ ] Implement `load_leases_with_etag()` and `atomic_save_leases()`
- [ ] Implement `acquire_lease()`, `complete_lease()`, `fail_lease()`, `renew_lease()`
- [ ] Add `node_id` to `Compactor` struct (e.g., from hostname or UUID)
- [ ] Modify `compact_l0()` to acquire lease before compaction
- [ ] Modify `compact_level()` to acquire lease before compaction
- [ ] Add lease validity check before `complete_compaction()`
- [ ] Add lease renewal background task
- [ ] Add `Error::ChunksAlreadyLeased` variant to `src/error.rs`
- [ ] Add scavenging of expired/completed/failed leases to `run_compaction_cycle()`
- [ ] Tests: concurrent lease acquisition
- [ ] Tests: lease expiry and scavenging
- [ ] Tests: lease renewal
- [ ] Tests: compactor crash recovery (lease timeout)

---

## 10. Summary

Compaction chunk leasing can be implemented purely with S3 ETags, using a dedicated `compaction-leases.json` file with CAS-based lease acquisition. The protocol provides mutual exclusion without external dependencies while gracefully handling crashes (TTL-based expiry), clock skew (conservative tolerance), and concurrent compactors (CAS retries).

The key insight is that compaction mutual exclusion is an optimization (preventing wasted work), not a correctness requirement -- `complete_compaction()` already uses CAS to prevent data corruption. The lease mechanism reduces wasted work to near-zero while maintaining the existing safety guarantees.
