use vault::scopes::Scope;

#[derive(serde::Deserialize, serde::Serialize, Default)]
pub struct AuthModuleConfig {
    pub session: SessionConfig,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct SessionConfig {
    /// Session TTL in seconds
    #[serde(default = "default_ttl")]
    pub ttl: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_SESSION_TTL,
        }
    }
}

fn default_ttl() -> u64 {
    DEFAULT_SESSION_TTL
}

const DEFAULT_SESSION_TTL: u64 = 60 * 60 * 24; // 1 day

impl vault::module_config::ModuleConfig for AuthModuleConfig {
    const SCOPE: Scope = vault::scopes::AUTH_SCOPE;
    const CONFIG_NAME: &'static str = "auth_config";
}
