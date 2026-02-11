//! Shard-aware query routing

use super::{ShardId, ShardMetadata, ShardKey};
use crate::{Error, Result};
use dashmap::DashMap;
use std::time::{Duration, Instant};

/// Cached routing entry
#[derive(Debug, Clone)]
struct RoutingEntry {
    shard: ShardMetadata,
    cached_at: Instant,
}

/// Shard router with generation-aware caching
pub struct ShardRouter {
    /// Routing cache
    cache: DashMap<ShardId, RoutingEntry>,
    /// Cache TTL
    ttl: Duration,
}

impl ShardRouter {
    /// Create a new shard router
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: DashMap::new(),
            ttl,
        }
    }

    /// Get shard for a key
    pub fn get_shard(&self, key: &ShardKey) -> Option<ShardMetadata> {
        let key_bytes = key.to_bytes();

        for entry in self.cache.iter() {
            if entry.cached_at.elapsed() > self.ttl {
                continue;
            }

            let shard = &entry.shard;
            if key_bytes >= shard.key_range.0 && key_bytes < shard.key_range.1 {
                return Some(shard.clone());
            }
        }

        None
    }

    /// Update routing for a shard
    pub fn update_routing(&self, shard: ShardMetadata) {
        self.cache.insert(shard.shard_id.clone(), RoutingEntry {
            shard,
            cached_at: Instant::now(),
        });
    }

    /// Invalidate routing for a shard
    pub fn invalidate(&self, shard_id: &ShardId) {
        self.cache.remove(shard_id);
    }

    /// Invalidate all routing
    pub fn invalidate_all(&self) {
        self.cache.clear();
    }

    /// Handle stale generation error
    pub fn handle_stale_generation(&self, shard_id: &ShardId) {
        self.invalidate(shard_id);
    }

    /// Handle shard moved error
    pub fn handle_shard_moved(&self, shard_id: &ShardId, new_shard: ShardMetadata) {
        self.invalidate(shard_id);
        self.update_routing(new_shard);
    }

    /// Route a query with automatic retry on shard moves
    pub async fn route_query<F, Fut, T>(
        &self,
        key: &ShardKey,
        execute: F,
    ) -> Result<T>
    where
        F: Fn(ShardMetadata) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        const MAX_RETRIES: usize = 3;

        for attempt in 0..MAX_RETRIES {
            let shard = self.get_shard(key)
                .ok_or_else(|| Error::ShardNotFound(
                    format!("No shard found for key: {:?}", key)
                ))?;

            match execute(shard.clone()).await {
                Ok(result) => return Ok(result),
                Err(Error::StaleGeneration { .. }) => {
                    self.invalidate(&shard.shard_id);
                    if attempt < MAX_RETRIES - 1 {
                        continue;
                    }
                }
                Err(Error::ShardMoved { new_location: _ }) => {
                    self.invalidate(&shard.shard_id);
                    // In production, fetch new shard info from metadata
                    if attempt < MAX_RETRIES - 1 {
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Err(Error::Internal("Max retries exceeded".into()))
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::ShardState;

    #[test]
    fn test_routing() {
        let router = ShardRouter::default();

        let shard = ShardMetadata {
            shard_id: "shard-1".to_string(),
            generation: 1,
            key_range: (vec![0, 0, 0, 0], vec![255, 255, 255, 255]),
            replicas: vec![],
            state: ShardState::Active,
            min_time: 0,
            max_time: 0,
        };

        router.update_routing(shard);

        let key = ShardKey::new(1, "cpu", 1000000000);
        let found = router.get_shard(&key);

        assert!(found.is_some());
        assert_eq!(found.unwrap().shard_id, "shard-1");
    }

    #[test]
    fn test_invalidation() {
        let router = ShardRouter::default();

        let shard = ShardMetadata {
            shard_id: "shard-1".to_string(),
            generation: 1,
            key_range: (vec![0, 0, 0, 0], vec![255, 255, 255, 255]),
            replicas: vec![],
            state: ShardState::Active,
            min_time: 0,
            max_time: 0,
        };

        router.update_routing(shard);
        router.invalidate(&"shard-1".to_string());

        let key = ShardKey::new(1, "cpu", 1000000000);
        let found = router.get_shard(&key);

        assert!(found.is_none());
    }
}
