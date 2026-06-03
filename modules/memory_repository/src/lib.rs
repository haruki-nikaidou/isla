//! # `memory_repository` module
//!
//! Storage for Isla's memory and conversation history.
//!
//! This module is the single source of truth for everything the agent
//! "remembers": raw conversation transcripts, summarized long-term memory,
//! and plugin-provided structured memory indexes. Other modules query it
//! through gRPC; plugins query it indirectly via AMQP memory-query events
//! routed through `plugin_registrar`.
//!
//! Persistent storage lives in PostgreSQL.
//!
//! ## Status
//!
//! Pre-alpha. Nothing implemented yet.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod config;
pub mod entities;
pub mod events;
pub mod hooks;
pub mod rpc;
pub mod services;
mod utils;
