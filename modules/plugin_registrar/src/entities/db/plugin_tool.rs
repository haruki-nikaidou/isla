//! Tools a plugin exposes to the LLM.
//!
//! Maps to table `plugin_reg.plugin_tool`.

use uuid::Uuid;

/// A single tool a plugin advertises.
///
/// At tool-use time, the AI caller looks up the row for the chosen
/// `(plugin, tool name)` pair and publishes the call onto the plugin's
/// message exchange using [`routing_key`](Self::routing_key).
pub struct PluginToolEntity {
    pub id: i64,
    pub plugin_id: Uuid,
    /// Tool name as the LLM sees it; unique per plugin.
    pub name: String,
    /// Human-readable name for operator surfaces.
    pub display_name: String,
    /// Tool description shown to the LLM.
    pub description: String,
    /// JSON Schema describing the tool's input parameters.
    pub parameters_schema: serde_json::Value,
    /// AMQP routing key the cluster uses to dispatch tool-use calls to this
    /// tool's handler inside the plugin.
    pub routing_key: String,
}
