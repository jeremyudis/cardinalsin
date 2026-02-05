//! End-to-end test infrastructure for CardinalSin
//!
//! This module provides test harness and utilities for validating
//! the full CardinalSin stack against a deployed docker-compose environment.

pub mod harness;
pub mod smoke;

pub use harness::*;
