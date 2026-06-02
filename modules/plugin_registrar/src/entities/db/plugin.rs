//! Core plugin record and the per-plugin metadata / dependency rows.
//!
//! Maps to tables `plugin_reg.plugin`, `plugin_reg.plugin_metadata`, and
//! `plugin_reg.plugin_dependency`.

use time::PrimitiveDateTime;
use uuid::Uuid;

/// A registered plugin.
///
/// One row per plugin instance. The [`id`](Self::id) is the cluster-issued
/// identity used as the AMQP correlation principal and as the foreign key
/// target for every other table in `plugin_reg`.
pub struct PluginEntity {
    /// Cluster-issued plugin identity.
    pub id: Uuid,
    /// Stable machine name (e.g. `gmail`). Unique across the cluster.
    pub name: String,
    /// When the plugin first registered with the cluster.
    pub registered_at: PrimitiveDateTime,
    /// Authorization scopes the plugin is allowed to act under.
    pub scope_range: Vec<String>,
    /// Human-readable description shown in operator surfaces.
    pub description: String,
    /// Shared secret used to sign JWTs the plugin presents to the cluster.
    pub jwt_secret: String,
    /// Public key (or raw key material) used by plugin to verify
    /// signed messages sent by the cluster.
    pub server_message_signature_key: Vec<u8>,
    /// AMQP exchange name the cluster uses to publish messages addressed to
    /// this plugin.
    pub message_exchange: String,
}

/// Optional, mostly-cosmetic metadata for a plugin. One row per plugin.
pub struct PluginMetadataEntity {
    pub plugin_id: Uuid,
    pub display_name: String,
    pub author: String,
    pub author_url: Option<String>,
    pub repository: Option<String>,
    pub license: Option<String>,
    pub version: String,
}

/// A "this plugin requires that plugin" declaration.
///
/// Used by service discovery to refuse to bring a plugin online until its
/// declared dependencies are themselves registered and healthy.
pub struct PluginDependencyEntity {
    pub id: i64,
    pub plugin_id: Uuid,
    /// Name (`plugin.name`) of the required plugin.
    pub require_plugin: String,
    /// SemVer range string the required plugin's version must satisfy.
    pub require_version: String,
}
