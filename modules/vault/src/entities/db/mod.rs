//! PostgreSQL-backed entity definitions.
//!
//! These entities represent rows in the vault's database tables. All
//! sensitive fields are encrypted before storage; the plaintext never
//! leaves the vault service.

pub mod ai_account;
pub mod rolling_key;
pub mod secret;
pub mod secret_read_log;
pub mod config;
