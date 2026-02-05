//! E2E Smoke Tests for CardinalSin
//!
//! This test file provides end-to-end validation of the CardinalSin stack.
//!
//! ## Running the tests
//!
//! These tests require a running CardinalSin deployment (docker-compose stack).
//!
//! ```bash
//! # Start the stack
//! cd deploy && docker compose up -d
//!
//! # Wait for services to be ready
//! curl http://localhost:8081/health
//! curl http://localhost:8080/health
//!
//! # Run smoke tests
//! cargo test --test e2e_smoke -- --nocapture --ignored
//!
//! # Tear down
//! cd deploy && docker compose down
//! ```
//!
//! ## Environment Variables
//!
//! - `CARDINALSIN_INGESTER_URL`: Ingester service URL (default: http://localhost:8081)
//! - `CARDINALSIN_QUERY_URL`: Query node URL (default: http://localhost:8080)

mod e2e;

// Re-export for use in tests
pub use e2e::*;
