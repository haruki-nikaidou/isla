use uuid::Uuid;

pub struct PluginToolEntity {
    pub id: i64,
    pub plugin_id: Uuid,
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub parameters_schema: serde_json::Value,
    pub routing_key: String,
}