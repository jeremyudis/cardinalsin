//! Monotonic clock source with skew mitigation
//!
//! Provides a wall-clock timestamp that never goes backward,
//! and a configurable safety margin for retention/GC decisions.

use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI64, Ordering};

/// A clock source that guarantees monotonically increasing timestamps
/// and provides skew-aware operations for retention decisions.
pub struct BoundedClock {
    /// High-water mark: the largest timestamp we've ever returned (nanos)
    high_water_ns: AtomicI64,
    /// Maximum tolerated clock skew (nanos). Applied as a safety margin
    /// when computing retention cutoffs to avoid premature deletion.
    max_skew_ns: i64,
}

impl BoundedClock {
    /// Create a new BoundedClock with the given maximum skew tolerance.
    pub fn new(max_skew: std::time::Duration) -> Self {
        Self {
            high_water_ns: AtomicI64::new(0),
            max_skew_ns: max_skew.as_nanos() as i64,
        }
    }

    /// Returns a monotonically increasing nanosecond timestamp.
    ///
    /// If the wall clock has gone backward (e.g. NTP adjustment),
    /// returns the previous high-water mark + 1ns instead.
    pub fn now_nanos(&self) -> i64 {
        let wall = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        loop {
            let prev = self.high_water_ns.load(Ordering::Acquire);
            let ts = wall.max(prev + 1);
            match self.high_water_ns.compare_exchange_weak(
                prev,
                ts,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return ts,
                Err(_) => continue, // CAS failed, retry
            }
        }
    }

    /// Returns `chrono::DateTime<Utc>` from the monotonic clock.
    pub fn now(&self) -> DateTime<Utc> {
        let ns = self.now_nanos();
        DateTime::from_timestamp_nanos(ns)
    }

    /// Returns a retention cutoff timestamp that accounts for clock skew.
    ///
    /// The cutoff is shifted earlier by `max_skew` so that chunks whose
    /// timestamps were recorded on a clock running ahead are not deleted
    /// prematurely.
    pub fn retention_cutoff_nanos(&self, retention_nanos: i64) -> i64 {
        let now = self.now_nanos();
        now - retention_nanos - self.max_skew_ns
    }

    /// Returns the configured max skew tolerance.
    pub fn max_skew(&self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.max_skew_ns as u64)
    }
}

impl Default for BoundedClock {
    fn default() -> Self {
        // 30 second default — generous enough for most NTP-synced environments
        Self::new(std::time::Duration::from_secs(30))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_increasing() {
        let clock = BoundedClock::default();
        let mut prev = 0i64;
        for _ in 0..100 {
            let ts = clock.now_nanos();
            assert!(ts > prev, "timestamps must be strictly increasing");
            prev = ts;
        }
    }

    #[test]
    fn test_retention_cutoff_includes_skew_margin() {
        let skew = std::time::Duration::from_secs(60);
        let clock = BoundedClock::new(skew);
        let retention = 86400_i64 * 1_000_000_000; // 1 day in nanos

        let cutoff = clock.retention_cutoff_nanos(retention);
        let now = clock.now_nanos();

        // cutoff should be at least retention + skew before now
        let expected_min_gap = retention + skew.as_nanos() as i64;
        assert!(
            now - cutoff >= expected_min_gap,
            "cutoff must include skew margin: gap={}, expected>={}",
            now - cutoff,
            expected_min_gap,
        );
    }

    #[test]
    fn test_now_returns_valid_datetime() {
        let clock = BoundedClock::default();
        let dt = clock.now();
        // Should be a reasonable time (after 2020)
        assert!(dt.timestamp() > 1_577_836_800, "timestamp should be after 2020");
    }

    #[test]
    fn test_concurrent_monotonicity() {
        use std::sync::Arc;
        let clock = Arc::new(BoundedClock::default());
        let mut handles = vec![];

        for _ in 0..4 {
            let c = clock.clone();
            handles.push(std::thread::spawn(move || {
                let mut prev = 0i64;
                for _ in 0..1000 {
                    let ts = c.now_nanos();
                    // Each thread's own sequence should be increasing
                    assert!(ts > prev);
                    prev = ts;
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
