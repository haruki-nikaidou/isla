//! # `ai_caller` module
//!
//! Calls into upstream LLM APIs.
//!
//! Responsibilities:
//!
//! - Build provider-specific requests (OpenAI-compatible, Anthropic, …) from
//!   the normalized internal representation in [`model`], and parse the vendor
//!   response back into it.
//! - Stream responses back to the caller.
//! - Pull API credentials from `vault` rather than holding them itself.
//!
//! Deciding *what* to send (personality + memory) and routing the model's tool
//! calls live in the `personality` module, which depends on this one.
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
