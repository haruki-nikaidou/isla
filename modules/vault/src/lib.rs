//! # `vault` module
//!
//! End-to-end encrypted vault for secret material.
//!
//! Two broad categories of secrets live here:
//!
//! 1. **Credentials the AI uses on the user's behalf** — service tokens,
//!    passwords, OAuth refresh tokens, etc., for the things the agent is
//!    allowed to act on.
//! 2. **Credentials Isla uses to call upstream AI APIs** — the keys the
//!    `ai_caller` module needs in order to talk to model providers.
//!
//! Ciphertext is stored in PostgreSQL; key material never leaves the vault
//! in plaintext. Other modules access secrets through this module's gRPC
//! API rather than reading the database directly.
//!
//! ## Status
//!
//! Pre-alpha. Nothing implemented yet.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod entities;
pub mod scopes;
