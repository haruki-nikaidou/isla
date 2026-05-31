use uuid::Uuid;

pub struct PluginMemorySetEntity {
    pub id: i64,
    pub plugin_id: Uuid,
    pub name: String,
    pub description: String,
}

pub struct PluginMemoryEntryEntity {
    pub id: i64,
    pub memory_set_id: i64,
    pub name: String,
    pub description: String,
    pub query_params: serde_json::Value,
    pub return_schema: serde_json::Value,
}