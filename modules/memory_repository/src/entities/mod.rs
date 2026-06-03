//! Data entities for the memory repository.
//!
//! Entities are organized by storage backend: `db` for PostgreSQL-persisted
//! data and `redis` for cached or ephemeral state.

pub mod db;
pub mod redis;
