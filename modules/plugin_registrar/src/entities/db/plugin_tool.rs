//! Tools a plugin exposes to the LLM.
//!
//! Maps to table `plugin_reg.plugin_tool`.

use kanau::processor::Processor;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

/// A single tool a plugin advertises.
///
/// At tool-use time, the AI caller looks up the row for the chosen
/// `(plugin, tool name)` pair and publishes the call onto the plugin's
/// message exchange using [`routing_key`](Self::routing_key).
#[derive(Debug, Clone)]
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

/// All tools advertised by a single plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListPluginTools {
    pub plugin_id: Uuid,
}

impl Processor<ListPluginTools> for DatabaseProcessor {
    type Output = Vec<PluginToolEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListPluginTools", err, fields(plugin_id = %input.plugin_id))]
    async fn process(&self, input: ListPluginTools) -> Result<Vec<PluginToolEntity>, sqlx::Error> {
        sqlx::query_as!(
            PluginToolEntity,
            r#"
            SELECT id, plugin_id, name, display_name, description,
                   parameters_schema as "parameters_schema: serde_json::Value", routing_key
            FROM plugin_reg.plugin_tool
            WHERE plugin_id = $1
            ORDER BY name
            "#,
            input.plugin_id,
        )
        .fetch_all(self.db())
        .await
    }
}

/// Every tool across every registered plugin (the cluster-wide tool catalog).
#[derive(Debug, Clone, Copy, Default)]
pub struct ListAllPluginTools;

impl Processor<ListAllPluginTools> for DatabaseProcessor {
    type Output = Vec<PluginToolEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListAllPluginTools", err)]
    async fn process(
        &self,
        _input: ListAllPluginTools,
    ) -> Result<Vec<PluginToolEntity>, sqlx::Error> {
        sqlx::query_as!(
            PluginToolEntity,
            r#"
            SELECT id, plugin_id, name, display_name, description,
                   parameters_schema as "parameters_schema: serde_json::Value", routing_key
            FROM plugin_reg.plugin_tool
            ORDER BY plugin_id, name
            "#,
        )
        .fetch_all(self.db())
        .await
    }
}
