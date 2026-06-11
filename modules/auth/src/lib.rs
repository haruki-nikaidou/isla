//! # `auth` module
//!
//! Authentication provider for Isla.
//!
//! Owns user identities and credentials for two audiences:
//!
//! - **Administrators**, who reach the cluster through `dashboard`.
//! - **WebUI end users**, who reach the cluster through `webui`.
//!
//! Other core modules call into this module via gRPC (see [`rpc`]) to validate
//! tokens, look up principals, and enforce access control. The module owns
//! its own persistent state in PostgreSQL and uses Redis for short-lived
//! session / token material.
//!
//! ## Submodules
//!
//! - [`entities`] - domain types (users, sessions, credentials, …).
//! - [`services`] - business logic, transaction boundaries.
//! - [`events`]   - AMQP event payloads emitted by this module.
//! - [`hooks`]    - extension points consumed by other modules.
//! - [`rpc`]      - gRPC service surface used by other modules and the
//!                dashboard.
//! - [`config`]   - runtime configuration.
//!
//! ## Status
//!
//! Pre-alpha. Most submodules are still empty.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod config;
pub mod entities;
pub mod events;
pub mod hooks;
pub mod rpc;
pub mod services;
mod utils;
