//! Skill files a plugin ships to the agent.
//!
//! Maps to tables `plugin_reg.plugin_skill` and
//! `plugin_reg.plugin_skill_extent_file`.

use uuid::Uuid;

/// A single skill (one logical capability with a main markdown / script
/// file) declared by a plugin.
pub struct PluginSkillEntity {
    pub id: i64,
    pub plugin_id: Uuid,
    /// Skill name; unique per plugin.
    pub name: String,
    pub display_name: String,
    pub description: String,
    /// Contents of the skill's main file (typically a `SKILL.md`).
    pub main_file_text: String,
}

/// Auxiliary file bundled with a skill (templates, helper scripts, etc.).
pub struct PluginSkillExtentFileEntity {
    pub id: i64,
    pub skill_id: i64,
    pub file_name: String,
    pub content: String,
}
