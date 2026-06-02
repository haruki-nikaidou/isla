//! Declared structured-memory queries a plugin can answer.
//!
//! Maps to tables `plugin_reg.plugin_memory_set` and
//! `plugin_reg.plugin_memory_entry`.

use uuid::Uuid;

/// A logical grouping of memory queries the plugin owns (e.g. one set per
/// data source / table). A plugin may declare any number of sets.
pub struct PluginMemorySetEntity {
    pub id: i64,
    pub plugin_id: Uuid,
    pub name: String,
    pub description: String,
}

/// A single named, schema-typed query inside a memory set.
///
/// Other plugins (and the AI caller) discover queries through the registrar
/// and invoke them over AMQP; the registrar validates calls against
/// [`query_params`](Self::query_params) and responses against
/// [`return_schema`](Self::return_schema).
pub struct PluginMemoryEntryEntity {
    pub id: i64,
    pub memory_set_id: i64,
    pub name: String,
    pub description: String,
    /// JSON Schema for the call's input parameters.
    pub query_params: serde_json::Value,
    /// JSON Schema for the call's response.
    pub return_schema: serde_json::Value,
}
