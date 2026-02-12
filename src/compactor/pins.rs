//! Chunk pin registry for preventing GC during active queries
//!
//! Tracks which chunks are actively being read by queries so the
//! compactor's garbage collector skips them. Uses RAII guards that
//! automatically release pins when queries complete.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// Registry tracking chunks pinned by active queries.
///
/// Shared between the query node and compactor when they run in the
/// same process. For multi-process deployments, the extended GC grace
/// period (5 minutes) provides the safety margin instead.
///
/// Uses reference counting so overlapping queries pinning the same
/// chunk are handled correctly.
#[derive(Debug, Default, Clone)]
pub struct ChunkPinRegistry {
    /// Map from chunk path to pin count
    pinned: Arc<RwLock<HashMap<String, usize>>>,
}

impl ChunkPinRegistry {
    pub fn new() -> Self {
        Self {
            pinned: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Pin a set of chunk paths, returning a guard that unpins on drop.
    pub fn pin(&self, paths: Vec<String>) -> PinGuard {
        {
            let mut pinned = self.pinned.write();
            for path in &paths {
                *pinned.entry(path.clone()).or_insert(0) += 1;
            }
        }
        PinGuard {
            pinned: Arc::clone(&self.pinned),
            paths,
        }
    }

    /// Check if a chunk path is currently pinned by any active query.
    pub fn is_pinned(&self, path: &str) -> bool {
        self.pinned.read().get(path).copied().unwrap_or(0) > 0
    }

    /// Get the count of distinct chunks currently pinned.
    pub fn pinned_count(&self) -> usize {
        self.pinned.read().values().filter(|&&v| v > 0).count()
    }
}

/// RAII guard that automatically unpins chunks when dropped.
///
/// Created by [`ChunkPinRegistry::pin`]. Decrements the reference count
/// for each pinned chunk when the query completes.
pub struct PinGuard {
    pinned: Arc<RwLock<HashMap<String, usize>>>,
    paths: Vec<String>,
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        let mut pinned = self.pinned.write();
        for path in &self.paths {
            if let Some(count) = pinned.get_mut(path) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    pinned.remove(path);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pin_and_unpin() {
        let registry = ChunkPinRegistry::new();

        let paths = vec!["chunk_a.parquet".to_string(), "chunk_b.parquet".to_string()];

        {
            let _guard = registry.pin(paths.clone());
            assert!(registry.is_pinned("chunk_a.parquet"));
            assert!(registry.is_pinned("chunk_b.parquet"));
            assert!(!registry.is_pinned("chunk_c.parquet"));
            assert_eq!(registry.pinned_count(), 2);
        }

        // After guard is dropped, chunks are unpinned
        assert!(!registry.is_pinned("chunk_a.parquet"));
        assert!(!registry.is_pinned("chunk_b.parquet"));
        assert_eq!(registry.pinned_count(), 0);
    }

    #[test]
    fn test_overlapping_pins_refcounted() {
        let registry = ChunkPinRegistry::new();

        let guard1 = registry.pin(vec!["chunk_a.parquet".to_string(), "chunk_b.parquet".to_string()]);
        let guard2 = registry.pin(vec!["chunk_b.parquet".to_string(), "chunk_c.parquet".to_string()]);

        assert!(registry.is_pinned("chunk_a.parquet"));
        assert!(registry.is_pinned("chunk_b.parquet"));
        assert!(registry.is_pinned("chunk_c.parquet"));
        assert_eq!(registry.pinned_count(), 3);

        drop(guard1);
        // chunk_b is still pinned by guard2
        assert!(!registry.is_pinned("chunk_a.parquet"));
        assert!(registry.is_pinned("chunk_b.parquet"));
        assert!(registry.is_pinned("chunk_c.parquet"));
        assert_eq!(registry.pinned_count(), 2);

        drop(guard2);
        assert_eq!(registry.pinned_count(), 0);
    }

    #[test]
    fn test_empty_pin() {
        let registry = ChunkPinRegistry::new();
        let _guard = registry.pin(vec![]);
        assert_eq!(registry.pinned_count(), 0);
    }
}
