//! Tiered caching system (RAM → NVMe → S3)

use super::CacheStats;
use crate::{Error, Result};

use arrow_array::RecordBatch;
use bytes::Bytes;
use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder, LruConfig};
use moka::future::Cache;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

/// Tiered cache with L1 (RAM) and optional L2 (NVMe)
///
/// L1: In-memory cache using moka for hot data.
/// L2: NVMe-backed cache using foyer for warm data (optional).
/// L3: Object storage (source of truth).
pub struct TieredCache {
    /// L1 in-memory cache for decoded Arrow data
    l1: Cache<String, Arc<CachedData>>,
    /// L2 NVMe-backed cache for raw Parquet bytes (Vec<u8> for serializability)
    /// Optional - only available if l2_dir is configured
    l2: Option<HybridCache<String, Vec<u8>>>,
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
    pub async fn new(config: CacheConfig) -> Result<Self> {
        // Build L1 cache with moka's automatic eviction
        let l1 = Cache::builder()
            .max_capacity(config.l1_size as u64)
            .weigher(|_key: &String, value: &Arc<CachedData>| {
                // Weigh by memory usage of both bytes and decoded batches
                let bytes_size = value.bytes.len();
                let batches_size = value
                    .batches
                    .as_ref()
                    .map(|b| b.iter().map(|rb| rb.get_array_memory_size()).sum::<usize>())
                    .unwrap_or(0);
                (bytes_size + batches_size) as u32
            })
            .build();

        // Build L2 cache with foyer 0.12 API (optional)
        let l2 = if let Some(l2_dir) = config.l2_dir {
            let cache: HybridCache<String, Vec<u8>> = HybridCacheBuilder::new()
                .memory(config.l1_size)
                .with_weighter(|_key: &String, value: &Vec<u8>| value.len())
                .with_eviction_config(LruConfig::default())
                .storage(Engine::Large)
                .with_device_options(
                    DirectFsDeviceOptions::new(PathBuf::from(&l2_dir))
                        .with_capacity(config.l2_size),
                )
                .build()
                .await
                .map_err(|e| Error::Config(format!("Failed to build L2 cache: {}", e)))?;
            Some(cache)
        } else {
            None
        };

        Ok(Self {
            l1,
            l2,
            stats: CacheStatistics {
                l1_hits: AtomicU64::new(0),
                l1_misses: AtomicU64::new(0),
                l2_hits: AtomicU64::new(0),
                l2_misses: AtomicU64::new(0),
            },
        })
    }

    /// Get data from cache, or fetch from source
    pub async fn get_or_fetch<F, Fut>(&self, key: &str, fetch: F) -> Result<Arc<CachedData>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Bytes>>,
    {
        // Try L1 (RAM) - moka's get returns Option directly, not a Future
        if let Some(data) = self.l1.get(key).await {
            self.stats.l1_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(data);
        }
        self.stats.l1_misses.fetch_add(1, Ordering::Relaxed);

        // Try L2 (NVMe) if available - foyer 0.12 returns Option<HybridCacheEntry>
        if let Some(ref l2) = self.l2 {
            if let Some(entry) = l2
                .get(&key.to_string())
                .await
                .map_err(|e| Error::Internal(format!("L2 cache error: {}", e)))?
            {
                self.stats.l2_hits.fetch_add(1, Ordering::Relaxed);
                let bytes = Bytes::from(entry.value().clone());
                let data = Arc::new(CachedData {
                    bytes,
                    batches: None,
                });
                // Promote to L1
                self.l1.insert(key.to_string(), data.clone()).await;
                return Ok(data);
            }
            self.stats.l2_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Fetch from source (S3)
        let bytes = fetch().await?;
        let data = Arc::new(CachedData {
            bytes: bytes.clone(),
            batches: None,
        });

        // Insert into both caches (convert Bytes to Vec<u8> for L2)
        if let Some(ref l2) = self.l2 {
            l2.insert(key.to_string(), bytes.to_vec());
        }
        self.l1.insert(key.to_string(), data.clone()).await;

        Ok(data)
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        // foyer 0.12 uses different stats API
        CacheStats {
            l1_hits: self.stats.l1_hits.load(Ordering::Relaxed),
            l1_misses: self.stats.l1_misses.load(Ordering::Relaxed),
            l2_hits: self.stats.l2_hits.load(Ordering::Relaxed),
            l2_misses: self.stats.l2_misses.load(Ordering::Relaxed),
            l1_size_bytes: self.l1.weighted_size() as usize,
            l2_size_bytes: 0, // foyer 0.12 doesn't expose store_bytes directly
        }
    }

    /// Invalidate a specific key from both caches
    pub async fn invalidate(&self, key: &str) {
        self.l1.invalidate(key).await;
        if let Some(ref l2) = self.l2 {
            l2.remove(&key.to_string());
        }
    }

    /// Clear all caches
    pub async fn clear(&self) {
        self.l1.invalidate_all();
        self.l1.run_pending_tasks().await;
        if let Some(ref l2) = self.l2 {
            l2.close().await.ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_cache() -> TieredCache {
        let dir = tempdir().unwrap();
        let config = CacheConfig {
            l1_size: 10 * 1024 * 1024, // 10MB
            l2_size: 50 * 1024 * 1024, // 50MB
            l2_dir: Some(dir.keep().to_str().unwrap().to_string()),
        };
        TieredCache::new(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_cache_hit_l1() {
        let cache = create_test_cache().await;

        // Insert data
        let data = Arc::new(CachedData {
            bytes: Bytes::from("data1"),
            batches: None,
        });
        cache.l1.insert("key1".to_string(), data).await;

        // Get should hit L1
        let result = cache
            .get_or_fetch("key1", || async {
                panic!("Should not fetch");
            })
            .await
            .unwrap();

        assert_eq!(result.bytes.as_ref(), b"data1");

        let stats = cache.stats();
        assert_eq!(stats.l1_hits, 1);
        assert_eq!(stats.l1_misses, 0);
    }

    #[tokio::test]
    async fn test_cache_hit_l2() {
        let cache = create_test_cache().await;

        // Insert into L2 - foyer 0.12 insert is not async, uses Vec<u8>
        if let Some(ref l2) = cache.l2 {
            l2.insert("key2".to_string(), b"data2".to_vec());

            // Get should miss L1, hit L2
            let result = cache
                .get_or_fetch("key2", || async {
                    panic!("Should not fetch from source");
                })
                .await
                .unwrap();

            assert_eq!(result.bytes.as_ref(), b"data2");

            let stats = cache.stats();
            assert_eq!(stats.l1_misses, 1);
            assert_eq!(stats.l2_hits, 1);
            assert_eq!(stats.l2_misses, 0);

            // Second get should now hit L1 (promoted)
            let _ = cache
                .get_or_fetch("key2", || async {
                    panic!("Should not fetch");
                })
                .await
                .unwrap();
            let stats = cache.stats();
            assert_eq!(stats.l1_hits, 1);
        } else {
            panic!("L2 cache should be configured for this test");
        }
    }

    #[tokio::test]
    async fn test_cache_miss_fetch() {
        let cache = create_test_cache().await;

        // Get should miss both and fetch
        let result = cache
            .get_or_fetch("key3", || async { Ok(Bytes::from("fetched_data")) })
            .await
            .unwrap();

        assert_eq!(result.bytes.as_ref(), b"fetched_data");

        let stats = cache.stats();
        assert_eq!(stats.l1_misses, 1);
        assert_eq!(stats.l2_misses, 1);
        assert_eq!(stats.l1_hits, 0);
        assert_eq!(stats.l2_hits, 0);

        // Second get should hit L1
        let result2 = cache
            .get_or_fetch("key3", || async {
                panic!("Should not fetch");
            })
            .await
            .unwrap();
        assert_eq!(result2.bytes.as_ref(), b"fetched_data");

        let stats = cache.stats();
        assert_eq!(stats.l1_hits, 1);
    }
}
