//! Tiered caching system (RAM → NVMe → S3)

use super::CacheStats;
use crate::Result;

use arrow_array::RecordBatch;
use bytes::Bytes;
use moka::future::Cache;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Instant;

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// L1 cache size (RAM) in bytes
    pub l1_size: usize,
    /// L2 cache size (NVMe) in bytes
    pub l2_size: usize,
    /// L2 cache directory
    pub l2_dir: Option<String>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            l1_size: 1024 * 1024 * 1024,      // 1GB
            l2_size: 10 * 1024 * 1024 * 1024, // 10GB
            l2_dir: None,
        }
    }
}

/// Entry in L2 cache with access tracking for LRU eviction
#[derive(Clone)]
struct L2Entry {
    bytes: Bytes,
    last_access: Instant,
}

/// Tiered cache with L1 (RAM) and L2 (NVMe)
///
/// L1: In-memory cache using moka (with automatic eviction)
/// L2: Memory cache with LRU eviction (replace with foyer for production NVMe)
/// L3: Object storage (source of truth)
pub struct TieredCache {
    /// L1 in-memory cache
    l1: Cache<String, Arc<CachedData>>,
    /// L2 cache with LRU tracking
    l2: RwLock<HashMap<String, L2Entry>>,
    /// L2 current size
    l2_current_size: AtomicU64,
    /// L2 max size
    l2_max_size: u64,
    /// High water mark for eviction trigger (90% of max)
    l2_evict_threshold: u64,
    /// Target size after eviction (80% of max)
    l2_evict_target: u64,
    /// Statistics
    stats: CacheStatistics,
}

/// Cached data wrapper
#[derive(Debug, Clone)]
pub struct CachedData {
    /// Raw Parquet bytes
    pub bytes: Bytes,
    /// Decoded record batches (lazily populated)
    pub batches: Option<Vec<RecordBatch>>,
}

/// Internal cache statistics
struct CacheStatistics {
    l1_hits: AtomicU64,
    l1_misses: AtomicU64,
    l2_hits: AtomicU64,
    l2_misses: AtomicU64,
}

impl TieredCache {
    /// Create a new tiered cache
    pub fn new(config: CacheConfig) -> Result<Self> {
        // Build L1 cache with moka's automatic eviction
        let l1 = Cache::builder()
            .max_capacity(config.l1_size as u64)
            .weigher(|_key: &String, value: &Arc<CachedData>| {
                value.bytes.len() as u32
            })
            .build();

        let l2_max = config.l2_size as u64;

        Ok(Self {
            l1,
            l2: RwLock::new(HashMap::new()),
            l2_current_size: AtomicU64::new(0),
            l2_max_size: l2_max,
            l2_evict_threshold: (l2_max as f64 * 0.9) as u64, // Trigger eviction at 90%
            l2_evict_target: (l2_max as f64 * 0.8) as u64,    // Evict down to 80%
            stats: CacheStatistics {
                l1_hits: AtomicU64::new(0),
                l1_misses: AtomicU64::new(0),
                l2_hits: AtomicU64::new(0),
                l2_misses: AtomicU64::new(0),
            },
        })
    }

    /// Get data from cache, or fetch from source
    pub async fn get_or_fetch<F, Fut>(
        &self,
        key: &str,
        fetch: F,
    ) -> Result<Arc<CachedData>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Bytes>>,
    {
        // Try L1 (RAM)
        if let Some(data) = self.l1.get(key).await {
            self.stats.l1_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(data);
        }
        self.stats.l1_misses.fetch_add(1, Ordering::Relaxed);

        // Try L2 (NVMe/disk)
        {
            let mut l2 = self.l2.write();
            if let Some(entry) = l2.get_mut(key) {
                self.stats.l2_hits.fetch_add(1, Ordering::Relaxed);
                // Update last access time for LRU tracking
                entry.last_access = Instant::now();
                let data = Arc::new(CachedData {
                    bytes: entry.bytes.clone(),
                    batches: None,
                });
                drop(l2); // Release lock before L1 insert
                // Promote to L1
                self.l1.insert(key.to_string(), data.clone()).await;
                return Ok(data);
            }
        }
        self.stats.l2_misses.fetch_add(1, Ordering::Relaxed);

        // Fetch from source (S3)
        let bytes = fetch().await?;
        let data = Arc::new(CachedData {
            bytes: bytes.clone(),
            batches: None,
        });

        // Insert into L2
        self.insert_l2(key, bytes);

        // Insert into L1
        self.l1.insert(key.to_string(), data.clone()).await;

        Ok(data)
    }

    /// Insert directly into cache
    pub async fn insert(&self, key: &str, bytes: Bytes) {
        let data = Arc::new(CachedData {
            bytes: bytes.clone(),
            batches: None,
        });

        self.insert_l2(key, bytes);
        self.l1.insert(key.to_string(), data).await;
    }

    /// Insert into L2 cache with LRU eviction
    fn insert_l2(&self, key: &str, bytes: Bytes) {
        let size = bytes.len() as u64;

        // Don't cache items larger than eviction headroom
        if size > self.l2_max_size / 10 {
            return; // Skip very large items
        }

        let current = self.l2_current_size.load(Ordering::Relaxed);

        // Check if we need to evict
        if current + size > self.l2_evict_threshold {
            self.evict_l2_lru();
        }

        let mut l2 = self.l2.write();
        if !l2.contains_key(key) {
            l2.insert(key.to_string(), L2Entry {
                bytes,
                last_access: Instant::now(),
            });
            self.l2_current_size.fetch_add(size, Ordering::Relaxed);
        }
    }

    /// Evict oldest entries from L2 using LRU policy
    fn evict_l2_lru(&self) {
        let mut l2 = self.l2.write();
        let target = self.l2_evict_target;

        // Collect entries with their access times
        let mut entries: Vec<(String, Instant, u64)> = l2
            .iter()
            .map(|(k, v)| (k.clone(), v.last_access, v.bytes.len() as u64))
            .collect();

        // Sort by last access time (oldest first)
        entries.sort_by_key(|(_, access, _)| *access);

        // Evict oldest entries until we're below target
        let mut current = self.l2_current_size.load(Ordering::Relaxed);
        for (key, _, size) in entries {
            if current <= target {
                break;
            }
            if l2.remove(&key).is_some() {
                current -= size;
            }
        }

        self.l2_current_size.store(current, Ordering::Relaxed);
    }

    /// Invalidate a cache entry
    pub async fn invalidate(&self, key: &str) {
        self.l1.remove(key).await;

        let mut l2 = self.l2.write();
        if let Some(entry) = l2.remove(key) {
            self.l2_current_size.fetch_sub(entry.bytes.len() as u64, Ordering::Relaxed);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            l1_hits: self.stats.l1_hits.load(Ordering::Relaxed),
            l1_misses: self.stats.l1_misses.load(Ordering::Relaxed),
            l2_hits: self.stats.l2_hits.load(Ordering::Relaxed),
            l2_misses: self.stats.l2_misses.load(Ordering::Relaxed),
            l1_size_bytes: self.l1.weighted_size() as usize,
            l2_size_bytes: self.l2_current_size.load(Ordering::Relaxed) as usize,
        }
    }

    /// Clear all caches
    pub async fn clear(&self) {
        self.l1.invalidate_all();
        self.l1.run_pending_tasks().await;

        let mut l2 = self.l2.write();
        l2.clear();
        self.l2_current_size.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_hit() {
        let cache = TieredCache::new(CacheConfig::default()).unwrap();

        // Insert data
        cache.insert("key1", Bytes::from("data1")).await;

        // Get should hit L1
        let result = cache.get_or_fetch("key1", || async {
            panic!("Should not fetch");
        }).await.unwrap();

        assert_eq!(result.bytes.as_ref(), b"data1");

        let stats = cache.stats();
        assert_eq!(stats.l1_hits, 1);
        assert_eq!(stats.l1_misses, 0);
    }

    #[tokio::test]
    async fn test_cache_miss_fetch() {
        let cache = TieredCache::new(CacheConfig::default()).unwrap();

        // Get should miss and fetch
        let result = cache.get_or_fetch("key1", || async {
            Ok(Bytes::from("fetched_data"))
        }).await.unwrap();

        assert_eq!(result.bytes.as_ref(), b"fetched_data");

        let stats = cache.stats();
        assert_eq!(stats.l1_hits, 0);
        assert_eq!(stats.l1_misses, 1);
        assert_eq!(stats.l2_misses, 1);

        // Second get should hit L1
        let result = cache.get_or_fetch("key1", || async {
            panic!("Should not fetch");
        }).await.unwrap();

        assert_eq!(result.bytes.as_ref(), b"fetched_data");

        let stats = cache.stats();
        assert_eq!(stats.l1_hits, 1);
    }

    #[tokio::test]
    async fn test_invalidate() {
        let cache = TieredCache::new(CacheConfig::default()).unwrap();

        cache.insert("key1", Bytes::from("data1")).await;
        cache.invalidate("key1").await;

        // Should miss now
        let result = cache.get_or_fetch("key1", || async {
            Ok(Bytes::from("refetched"))
        }).await.unwrap();

        assert_eq!(result.bytes.as_ref(), b"refetched");
    }
}
