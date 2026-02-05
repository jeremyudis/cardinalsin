//! Query routing with shard awareness

use crate::metadata::TimeRange;
use std::collections::HashMap;
use parking_lot::RwLock;

/// Shard information
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub shard_id: String,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub leader_node: String,
    pub generation: u64,
}

/// Cached routing information
#[derive(Debug, Clone)]
pub struct RoutingEntry {
    pub shard: ShardInfo,
    pub cached_at: std::time::Instant,
}

/// Query router with generation-aware caching
pub struct QueryRouter {
    /// Cached shard routing
    cache: RwLock<HashMap<String, RoutingEntry>>,
    /// Cache TTL
    cache_ttl: std::time::Duration,
}

impl QueryRouter {
    /// Create a new query router
    pub fn new(cache_ttl: std::time::Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            cache_ttl,
        }
    }

    /// Get the shard for a given key
    pub fn get_shard(&self, key: &[u8]) -> Option<ShardInfo> {
        let cache = self.cache.read();

        for (_, entry) in cache.iter() {
            if entry.cached_at.elapsed() > self.cache_ttl {
                continue;
            }

            if key >= entry.shard.min_key.as_slice()
                && key < entry.shard.max_key.as_slice()
            {
                return Some(entry.shard.clone());
            }
        }

        None
    }

    /// Update routing cache
    pub fn update_routing(&self, shard: ShardInfo) {
        let mut cache = self.cache.write();
        cache.insert(shard.shard_id.clone(), RoutingEntry {
            shard,
            cached_at: std::time::Instant::now(),
        });
    }

    /// Invalidate routing for a shard
    pub fn invalidate(&self, shard_id: &str) {
        let mut cache = self.cache.write();
        cache.remove(shard_id);
    }

    /// Invalidate all routing entries
    pub fn invalidate_all(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }

    /// Get all shards that overlap with a time range
    pub fn get_shards_for_time_range(&self, range: TimeRange) -> Vec<ShardInfo> {
        let cache = self.cache.read();
        let mut shards = Vec::new();

        // For time-series, the key typically starts with timestamp
        let start_key = range.start.to_be_bytes().to_vec();
        let end_key = range.end.to_be_bytes().to_vec();

        for (_, entry) in cache.iter() {
            if entry.cached_at.elapsed() > self.cache_ttl {
                continue;
            }

            // Check for overlap
            if entry.shard.min_key <= end_key && entry.shard.max_key >= start_key {
                shards.push(entry.shard.clone());
            }
        }

        shards
    }
}

impl Default for QueryRouter {
    fn default() -> Self {
        Self::new(std::time::Duration::from_secs(60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_caching() {
        let router = QueryRouter::new(std::time::Duration::from_secs(60));

        let shard = ShardInfo {
            shard_id: "shard-1".to_string(),
            min_key: vec![0, 0, 0, 0],
            max_key: vec![255, 255, 255, 255],
            leader_node: "node-1".to_string(),
            generation: 1,
        };

        router.update_routing(shard.clone());

        let found = router.get_shard(&[100, 100, 100, 100]);
        assert!(found.is_some());
        assert_eq!(found.unwrap().shard_id, "shard-1");
    }

    #[test]
    fn test_router_invalidation() {
        let router = QueryRouter::new(std::time::Duration::from_secs(60));

        let shard = ShardInfo {
            shard_id: "shard-1".to_string(),
            min_key: vec![0, 0, 0, 0],
            max_key: vec![255, 255, 255, 255],
            leader_node: "node-1".to_string(),
            generation: 1,
        };

        router.update_routing(shard);
        router.invalidate("shard-1");

        let found = router.get_shard(&[100, 100, 100, 100]);
        assert!(found.is_none());
    }
}
