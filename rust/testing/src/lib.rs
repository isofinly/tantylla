//! Integration testing harness for Tantylla.
//!
//! This crate focuses on deterministic, end-to-end tests with a configurable
//! service topology and ScyllaDB CDC setup. The current implementation assumes
//! a shared ScyllaDB instance for speed; a future extension can swap in a
//! container-based Scylla cluster for full infrastructure failure testing.

pub mod cluster;
pub mod process;
pub mod trace;

#[cfg(test)]
mod tests;
