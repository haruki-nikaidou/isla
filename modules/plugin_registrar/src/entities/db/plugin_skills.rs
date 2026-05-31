use uuid::Uuid;

pub struct PluginSkillEntity {
    pub id: i64,
    pub plugin_id: Uuid,
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub main_file_text: String,
}

pub struct PluginSkillExtentFileEntity {
    pub id: i64,
    pub skill_id: i64,
    pub file_name: String,
    pub content: String,
}