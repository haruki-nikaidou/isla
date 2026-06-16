//! Namespace claims for a registered plugin.
//!
//! Maps to table `plugin_reg.plugin_namespace`.

use kanau::processor::Processor;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

/// One namespace claimed by a plugin.
///
/// Namespaces are dotted identifiers such as `office.gmail` or
/// `life.accuweather`; in the database they are stored as the segment array
/// (`{"office", "gmail"}`) so the registrar can match by prefix without
/// having to parse the dotted form. A plugin may claim more than one
/// namespace.
pub struct PluginNamespaceEntity {
    pub id: i64,
    pub plugin_id: uuid::Uuid,
    /// Namespace segments, in order. E.g. `["office", "gmail"]`.
    pub name: Vec<String>,
}

/// Resolve which plugin (if any) claims the given namespace segment list.
#[derive(Debug, Clone)]
pub struct FindPluginIdByNamespace {
    /// Namespace segments, in order. E.g. `["office", "gmail"]`.
    pub segments: Vec<String>,
}

impl Processor<FindPluginIdByNamespace> for DatabaseProcessor {
    type Output = Option<Uuid>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindPluginIdByNamespace", err)]
    async fn process(&self, input: FindPluginIdByNamespace) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            SELECT plugin_id
            FROM plugin_reg.plugin_namespace
            WHERE name = $1
            "#,
            &input.segments,
        )
        .fetch_optional(self.db())
        .await
    }
}
