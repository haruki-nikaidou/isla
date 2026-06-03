//! # `plugin_registrar` module
//!
//! Service discovery and registration for plugins.
//!
//! Plugins are out-of-process, potentially in any language, and communicate
//! with the cluster as JSON messages over RabbitMQ. This module is the
//! cluster-side bookkeeper for that ecosystem:
//!
//! - Accepts plugin registration (namespace, metadata, declared tools,
//!   declared memory queries, declared dependencies).
//! - Answers service-discovery queries from the rest of the cluster ("which
//!   plugin handles namespace `office.gmail`?").
//! - Routes inbound plugin events (tool-use callbacks, memory queries,
//!   wake-up signals) to the right consumers.
//! - Drives the service-discovery heartbeat that lets plugins announce
//!   themselves and lets the cluster notice when a plugin disappears.
//!
//! See the top-level `README.md` for the plugin protocol contract.
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
