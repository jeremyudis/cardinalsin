//! Rate limiting and tenant quota management
//!
//! Provides:
//! - Per-tenant rate limiting for writes and queries
//! - Configurable quotas (requests/sec, bytes/sec, concurrent queries)
//! - Token bucket algorithm for smooth rate limiting

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;

/// Tenant quota configuration
#[derive(Debug, Clone)]
pub struct TenantQuota {
    /// Maximum write requests per second
    pub max_write_rps: u64,
    /// Maximum write bytes per second
    pub max_write_bytes_per_sec: u64,
    /// Maximum query requests per second
    pub max_query_rps: u64,
    /// Maximum concurrent queries
    pub max_concurrent_queries: u64,
    /// Maximum storage bytes (total)
    pub max_storage_bytes: u64,
    /// Maximum indexes
    pub max_indexes: u32,
}

impl Default for TenantQuota {
    fn default() -> Self {
        Self {
            max_write_rps: 10_000,
            max_write_bytes_per_sec: 100 * 1024 * 1024, // 100 MB/s
            max_query_rps: 1_000,
            max_concurrent_queries: 100,
            max_storage_bytes: 1024 * 1024 * 1024 * 1024, // 1 TB
            max_indexes: 50,
        }
    }
}

/// Token bucket for rate limiting
#[derive(Debug)]
struct TokenBucket {
    /// Maximum tokens (bucket capacity)
    capacity: u64,
    /// Current tokens available
    tokens: AtomicU64,
    /// Tokens added per second
    refill_rate: u64,
    /// Last refill time
    last_refill: RwLock<Instant>,
}

impl TokenBucket {
    fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            capacity,
            tokens: AtomicU64::new(capacity),
            refill_rate,
            last_refill: RwLock::new(Instant::now()),
        }
    }

    /// Try to acquire tokens, returns true if successful
    fn try_acquire(&self, tokens: u64) -> bool {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < tokens {
                return false;
            }
            if self.tokens.compare_exchange(
                current,
                current - tokens,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ).is_ok() {
                return true;
            }
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let mut last_refill = self.last_refill.write();
        let elapsed = last_refill.elapsed();
        let new_tokens = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;

        if new_tokens > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_value = (current + new_tokens).min(self.capacity);
            self.tokens.store(new_value, Ordering::Relaxed);
            *last_refill = Instant::now();
        }
    }

    /// Get current token count
    fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }
}

/// Per-tenant rate limiter state
struct TenantRateLimiter {
    /// Write RPS bucket
    write_rps: TokenBucket,
    /// Write bytes bucket
    write_bytes: TokenBucket,
    /// Query RPS bucket
    query_rps: TokenBucket,
    /// Current concurrent queries
    concurrent_queries: AtomicU64,
    /// Maximum concurrent queries
    max_concurrent_queries: u64,
    /// Quota configuration
    quota: TenantQuota,
}

impl TenantRateLimiter {
    fn new(quota: TenantQuota) -> Self {
        Self {
            write_rps: TokenBucket::new(quota.max_write_rps, quota.max_write_rps),
            write_bytes: TokenBucket::new(
                quota.max_write_bytes_per_sec,
                quota.max_write_bytes_per_sec,
            ),
            query_rps: TokenBucket::new(quota.max_query_rps, quota.max_query_rps),
            concurrent_queries: AtomicU64::new(0),
            max_concurrent_queries: quota.max_concurrent_queries,
            quota,
        }
    }
}

/// Rate limiter result
#[derive(Debug, Clone)]
pub enum RateLimitResult {
    /// Request allowed
    Allowed,
    /// Request denied with reason
    Denied(RateLimitDenial),
}

/// Reason for rate limit denial
#[derive(Debug, Clone)]
pub enum RateLimitDenial {
    WriteRpsExceeded { limit: u64, retry_after_ms: u64 },
    WriteBytesExceeded { limit: u64, retry_after_ms: u64 },
    QueryRpsExceeded { limit: u64, retry_after_ms: u64 },
    ConcurrentQueriesExceeded { current: u64, max: u64 },
    StorageQuotaExceeded { current: u64, max: u64 },
}

impl RateLimitResult {
    pub fn is_allowed(&self) -> bool {
        matches!(self, RateLimitResult::Allowed)
    }
}

/// Rate limiter service
pub struct RateLimiter {
    /// Per-tenant rate limiters
    tenants: DashMap<String, Arc<TenantRateLimiter>>,
    /// Default quota for new tenants
    default_quota: TenantQuota,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(default_quota: TenantQuota) -> Self {
        Self {
            tenants: DashMap::new(),
            default_quota,
        }
    }

    /// Get or create rate limiter for a tenant
    fn get_or_create(&self, tenant_id: &str) -> Arc<TenantRateLimiter> {
        self.tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::new(TenantRateLimiter::new(self.default_quota.clone())))
            .clone()
    }

    /// Set custom quota for a tenant
    pub fn set_tenant_quota(&self, tenant_id: &str, quota: TenantQuota) {
        self.tenants.insert(
            tenant_id.to_string(),
            Arc::new(TenantRateLimiter::new(quota)),
        );
    }

    /// Check if a write request is allowed
    pub fn check_write(&self, tenant_id: &str, bytes: u64) -> RateLimitResult {
        let limiter = self.get_or_create(tenant_id);

        // Check write RPS
        if !limiter.write_rps.try_acquire(1) {
            let retry_after = 1000 / limiter.quota.max_write_rps.max(1);
            return RateLimitResult::Denied(RateLimitDenial::WriteRpsExceeded {
                limit: limiter.quota.max_write_rps,
                retry_after_ms: retry_after,
            });
        }

        // Check write bytes
        if !limiter.write_bytes.try_acquire(bytes) {
            let retry_after = (bytes * 1000) / limiter.quota.max_write_bytes_per_sec.max(1);
            return RateLimitResult::Denied(RateLimitDenial::WriteBytesExceeded {
                limit: limiter.quota.max_write_bytes_per_sec,
                retry_after_ms: retry_after,
            });
        }

        RateLimitResult::Allowed
    }

    /// Check if a query request is allowed
    pub fn check_query(&self, tenant_id: &str) -> RateLimitResult {
        let limiter = self.get_or_create(tenant_id);

        // Check query RPS
        if !limiter.query_rps.try_acquire(1) {
            let retry_after = 1000 / limiter.quota.max_query_rps.max(1);
            return RateLimitResult::Denied(RateLimitDenial::QueryRpsExceeded {
                limit: limiter.quota.max_query_rps,
                retry_after_ms: retry_after,
            });
        }

        // Check concurrent queries
        let current = limiter.concurrent_queries.fetch_add(1, Ordering::SeqCst);
        if current >= limiter.max_concurrent_queries {
            limiter.concurrent_queries.fetch_sub(1, Ordering::SeqCst);
            return RateLimitResult::Denied(RateLimitDenial::ConcurrentQueriesExceeded {
                current,
                max: limiter.max_concurrent_queries,
            });
        }

        RateLimitResult::Allowed
    }

    /// Mark a query as completed (decrements concurrent count)
    pub fn query_completed(&self, tenant_id: &str) {
        if let Some(limiter) = self.tenants.get(tenant_id) {
            limiter.concurrent_queries.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Get current usage statistics for a tenant
    pub fn get_usage(&self, tenant_id: &str) -> Option<TenantUsage> {
        self.tenants.get(tenant_id).map(|limiter| TenantUsage {
            write_rps_available: limiter.write_rps.available(),
            write_bytes_available: limiter.write_bytes.available(),
            query_rps_available: limiter.query_rps.available(),
            concurrent_queries: limiter.concurrent_queries.load(Ordering::Relaxed),
            quota: limiter.quota.clone(),
        })
    }
}

/// Tenant usage statistics
#[derive(Debug, Clone)]
pub struct TenantUsage {
    pub write_rps_available: u64,
    pub write_bytes_available: u64,
    pub query_rps_available: u64,
    pub concurrent_queries: u64,
    pub quota: TenantQuota,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket() {
        let bucket = TokenBucket::new(10, 10);

        // Should have full capacity
        assert!(bucket.try_acquire(5));
        assert!(bucket.try_acquire(5));

        // Should be empty
        assert!(!bucket.try_acquire(1));
    }

    #[test]
    fn test_rate_limiter_write() {
        let limiter = RateLimiter::new(TenantQuota {
            max_write_rps: 2,
            ..Default::default()
        });

        // First two writes should pass
        assert!(limiter.check_write("tenant1", 100).is_allowed());
        assert!(limiter.check_write("tenant1", 100).is_allowed());

        // Third should fail
        assert!(!limiter.check_write("tenant1", 100).is_allowed());
    }

    #[test]
    fn test_rate_limiter_concurrent_queries() {
        let limiter = RateLimiter::new(TenantQuota {
            max_query_rps: 1000,
            max_concurrent_queries: 2,
            ..Default::default()
        });

        // First two queries should pass
        assert!(limiter.check_query("tenant1").is_allowed());
        assert!(limiter.check_query("tenant1").is_allowed());

        // Third should fail
        assert!(!limiter.check_query("tenant1").is_allowed());

        // Complete one query
        limiter.query_completed("tenant1");

        // Now should pass
        assert!(limiter.check_query("tenant1").is_allowed());
    }
}
