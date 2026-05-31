use time::PrimitiveDateTime;
use uuid::Uuid;

pub struct PluginEntity {
    pub id: Uuid,
    pub name: String,
    pub registered_at: PrimitiveDateTime,
    pub scope_range: Vec<String>,
    pub description: String,
    pub jwt_secret: String,
    pub server_message_signature_key: Vec<u8>,
    pub message_exchange: String,
}

pub struct PluginMetadataEntity {
    pub plugin_id: Uuid,
    pub display_name: String,
    pub author: String,
    pub author_url: Option<String>,
    pub repository: Option<String>,
    pub license: Option<String>,
    pub version: String,
}

pub struct PluginDependencyEntity {
    pub id: i64,
    pub plugin_id: Uuid,
    pub require_plugin: String,
    pub require_version: String,
}