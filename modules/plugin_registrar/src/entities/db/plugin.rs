//! Core plugin record and the per-plugin metadata / dependency rows.
//!
//! Maps to tables `plugin_reg.plugin`, `plugin_reg.plugin_metadata`, and
//! `plugin_reg.plugin_dependency`.

use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

/// A registered plugin.
///
/// One row per plugin instance. The [`id`](Self::id) is the cluster-issued
/// identity used as the AMQP correlation principal and as the foreign key
/// target for every other table in `plugin_reg`.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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

/// A tool declaration supplied at registration time, before the plugin row
/// exists. Used by [`RegisterPluginRow`] to fan out into `plugin_reg.plugin_tool`.
#[derive(Debug, Clone)]
pub struct NewPluginTool {
    /// Tool name as the LLM sees it; unique per plugin.
    pub name: String,
    /// Human-readable name for operator surfaces.
    pub display_name: String,
    /// Tool description shown to the LLM.
    pub description: String,
    /// JSON Schema describing the tool's input parameters.
    pub parameters_schema: serde_json::Value,
    /// AMQP routing key the cluster uses to dispatch tool-use calls.
    pub routing_key: String,
}

/// A skill declaration supplied at registration time. Used by
/// [`RegisterPluginRow`] to fan out into `plugin_reg.plugin_skill`.
#[derive(Debug, Clone)]
pub struct NewPluginSkill {
    /// Skill name; unique per plugin.
    pub name: String,
    pub display_name: String,
    pub description: String,
    /// Contents of the skill's main file (typically a `SKILL.md`).
    pub main_file_text: String,
}

/// Fetch a plugin by its cluster-issued [`id`](PluginEntity::id).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindPluginById {
    pub id: Uuid,
}

impl Processor<FindPluginById> for DatabaseProcessor {
    type Output = Option<PluginEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindPluginById", err, fields(id = %input.id))]
    async fn process(&self, input: FindPluginById) -> Result<Option<PluginEntity>, sqlx::Error> {
        sqlx::query_as!(
            PluginEntity,
            r#"
            SELECT id, name, registered_at, scope_range, description, jwt_secret,
                   server_message_signature_key, message_exchange
            FROM plugin_reg.plugin
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Fetch a plugin by its unique machine [`name`](PluginEntity::name).
#[derive(Debug, Clone)]
pub struct FindPluginByName {
    pub name: String,
}

impl Processor<FindPluginByName> for DatabaseProcessor {
    type Output = Option<PluginEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindPluginByName", err, fields(name = %input.name))]
    async fn process(&self, input: FindPluginByName) -> Result<Option<PluginEntity>, sqlx::Error> {
        sqlx::query_as!(
            PluginEntity,
            r#"
            SELECT id, name, registered_at, scope_range, description, jwt_secret,
                   server_message_signature_key, message_exchange
            FROM plugin_reg.plugin
            WHERE name = $1
            LIMIT 1
            "#,
            input.name,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Paginated list of registered plugins, newest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListPlugins {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListPlugins> for DatabaseProcessor {
    type Output = Vec<PluginEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListPlugins", err, fields(limit = input.limit, offset = input.offset))]
    async fn process(&self, input: ListPlugins) -> Result<Vec<PluginEntity>, sqlx::Error> {
        sqlx::query_as!(
            PluginEntity,
            r#"
            SELECT id, name, registered_at, scope_range, description, jwt_secret,
                   server_message_signature_key, message_exchange
            FROM plugin_reg.plugin
            ORDER BY registered_at DESC
            LIMIT $1 OFFSET $2
            "#,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}

/// Fetch only the JWT signing secret for a plugin, by id.
///
/// Used by the plugin-auth layer to verify per-message plugin tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindPluginJwtSecret {
    pub plugin_id: Uuid,
}

impl Processor<FindPluginJwtSecret> for DatabaseProcessor {
    type Output = Option<String>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindPluginJwtSecret", err, fields(plugin_id = %input.plugin_id))]
    async fn process(&self, input: FindPluginJwtSecret) -> Result<Option<String>, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            SELECT jwt_secret
            FROM plugin_reg.plugin
            WHERE id = $1
            "#,
            input.plugin_id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Delete a plugin by id. Cascades to every dependent row in `plugin_reg`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletePlugin {
    pub id: Uuid,
}

impl Processor<DeletePlugin> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeletePlugin", err, fields(id = %input.id))]
    async fn process(&self, input: DeletePlugin) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            DELETE FROM plugin_reg.plugin
            WHERE id = $1
            "#,
            input.id,
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}

/// Atomically register a plugin and all of its declared sub-records.
///
/// Inserts the core plugin row, optional metadata, and the dependency,
/// namespace, tool, and skill rows supplied in the corresponding vectors. The
/// whole insert runs in one transaction, so a failure on any row rolls back
/// the entire registration.
#[derive(Debug, Clone)]
pub struct RegisterPluginRow {
    pub id: Uuid,
    pub name: String,
    pub scope_range: Vec<String>,
    pub description: String,
    pub jwt_secret: String,
    pub server_message_signature_key: Vec<u8>,
    pub message_exchange: String,
    pub metadata: Option<PluginMetadataEntity>,
    /// `(require_plugin, require_version)` pairs.
    pub dependencies: Vec<(String, String)>,
    /// Each namespace as its ordered segment list.
    pub namespaces: Vec<Vec<String>>,
    pub tools: Vec<NewPluginTool>,
    pub skills: Vec<NewPluginSkill>,
}

impl Processor<RegisterPluginRow> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL-Transaction:RegisterPluginRow", err, fields(id = %input.id, name = %input.name))]
    async fn process(&self, input: RegisterPluginRow) -> Result<(), sqlx::Error> {
        let mut tx = self.db().begin().await?;

        let _registered_at = sqlx::query_scalar!(
            r#"
            INSERT INTO plugin_reg.plugin
                (id, name, scope_range, description, jwt_secret,
                 server_message_signature_key, message_exchange)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING registered_at
            "#,
            input.id,
            input.name,
            &input.scope_range,
            input.description,
            input.jwt_secret,
            input.server_message_signature_key,
            input.message_exchange,
        )
        .fetch_one(&mut *tx)
        .await?;

        if let Some(metadata) = input.metadata {
            sqlx::query!(
                r#"
                INSERT INTO plugin_reg.plugin_metadata
                    (plugin_id, display_name, author, author_url, repository, license, version)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
                input.id,
                metadata.display_name,
                metadata.author,
                metadata.author_url,
                metadata.repository,
                metadata.license,
                metadata.version,
            )
            .execute(&mut *tx)
            .await?;
        }

        for (require_plugin, require_version) in input.dependencies {
            sqlx::query!(
                r#"
                INSERT INTO plugin_reg.plugin_dependency
                    (plugin_id, require_plugin, require_version)
                VALUES ($1, $2, $3)
                "#,
                input.id,
                require_plugin,
                require_version,
            )
            .execute(&mut *tx)
            .await?;
        }

        for segments in input.namespaces {
            sqlx::query!(
                r#"
                INSERT INTO plugin_reg.plugin_namespace (plugin_id, name)
                VALUES ($1, $2)
                "#,
                input.id,
                &segments,
            )
            .execute(&mut *tx)
            .await?;
        }

        for tool in input.tools {
            sqlx::query!(
                r#"
                INSERT INTO plugin_reg.plugin_tool
                    (plugin_id, name, display_name, description, parameters_schema, routing_key)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
                input.id,
                tool.name,
                tool.display_name,
                tool.description,
                tool.parameters_schema,
                tool.routing_key,
            )
            .execute(&mut *tx)
            .await?;
        }

        for skill in input.skills {
            sqlx::query!(
                r#"
                INSERT INTO plugin_reg.plugin_skill
                    (plugin_id, name, display_name, description, main_file_text)
                VALUES ($1, $2, $3, $4, $5)
                "#,
                input.id,
                skill.name,
                skill.display_name,
                skill.description,
                skill.main_file_text,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
