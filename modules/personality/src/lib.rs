//! # `personality` module
//!
//! Decides *what* is sent to the LLM and routes what comes back.
//!
//! Responsibilities:
//!
//! - Assemble the normalized request (system prompt from personality facets,
//!   recent history, and the available tool catalog) from `memory_repository`
//!   and `plugin_registrar`.
//! - Drive the agent turn loop: call the model (via `ai_caller`), route any
//!   tool calls to internal handlers or external plugins, feed results back,
//!   and repeat until the turn ends.
//!
//! The actual upstream LLM API call lives in `ai_caller`, which this module
//! depends on; this module never talks to a model vendor directly.
//!
//! ## Status
//!
//! Pre-alpha. Nothing wired into the binary yet.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod config;
pub mod entities;
pub mod events;
pub mod hooks;
pub mod rpc;
pub mod services;
