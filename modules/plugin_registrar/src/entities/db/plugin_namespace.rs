//! Namespace claims for a registered plugin.
//!
//! Maps to table `plugin_reg.plugin_namespace`.

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
