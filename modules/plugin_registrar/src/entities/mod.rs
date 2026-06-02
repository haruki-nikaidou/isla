//! Storage entities for the plugin registrar.
//!
//! - [`db`] — durable Postgres-backed records (registered plugins, declared
//!   tools, skills, memory queries, dependencies, namespaces). These tables
//!   live in the `plugin_reg` Postgres schema.
//! - [`redis`] — ephemeral Redis-backed records (heartbeat liveness, routing
//!   caches, in-flight RPC correlation state). Lost on Redis restart by
//!   design; everything stored here can be rebuilt from the database plus a
//!   fresh round of heartbeats.

pub mod db;
pub mod redis;
