//! # `ai_caller` module
//!
//! Calls into upstream LLM APIs and dispatches tool use.
//!
//! Responsibilities:
//!
//! - Build provider-specific requests (OpenAI-compatible, Anthropic, …) from
//!   a normalized internal representation.
//! - Stream responses back to the conversation owner.
//! - When the model emits a tool call, route it to the appropriate handler:
//!   either an internal handler in another module (for example, "send
//!   message" lives in `interface`) or an external plugin reached via
//!   `plugin_registrar` over AMQP.
//! - Pull API credentials from `vault` rather than holding them itself.
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
pub mod model;
pub mod rpc;
pub mod services;
mod utils;
