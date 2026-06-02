//! Postgres entities for the plugin registrar.
//!
//! Every entity in this module is persisted in the `plugin_reg` Postgres
//! schema, created by migration `20260602201114_create_plugin_registrar_tables`.
//!
//! The shape of the data follows the plugin contract described in the
//! top-level `README.md`:
//!
//! - [`plugin`] — a registered plugin, its identity, its message-exchange
//!   binding, and the per-plugin metadata and dependency declarations.
//! - [`plugin_namespace`] — the namespaces a plugin claims (used to route
//!   tool-use and memory-query calls; e.g. `office.gmail`).
//! - [`plugin_tool`] — tools the plugin exposes to the LLM, with their
//!   parameter JSON Schema and AMQP routing key.
//! - [`plugin_skills`] — skill files (markdown / scripts) the plugin ships to
//!   the agent, plus any additional bundled files.
//! - [`plugin_memory`] — declared structured-memory query sets the plugin can
//!   answer, with their parameter and return JSON Schemas.

pub mod plugin;
pub mod plugin_namespace;
pub mod plugin_tool;
pub mod plugin_skills;
pub mod plugin_memory;
