//! Module configuration for the OpenRouter-backed provider.
//!
//! Loaded from `vault` under [`AI_CALLER_SCOPE`](vault::scopes::AI_CALLER_SCOPE)
//! with the name `ai_caller_config`. The API key itself is *not* part of this
//! config; it is fetched separately as a secret and passed to
//! [`OpenRouterProvider::new`](crate::services::openrouter::OpenRouterProvider::new).

use serde::{Deserialize, Serialize};

/// Tunables for talking to OpenRouter.
///
/// Every field has a sensible default, so an empty stored config still
/// deserializes into a working configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenRouterConfig {
    /// Model slug to request, e.g. `anthropic/claude-sonnet-4`.
    #[serde(default = "default_model")]
    pub model: String,
    /// Base URL of the OpenRouter-compatible API.
    #[serde(default = "default_base_url")]
    pub base_url: String,
    /// Upper bound on tokens generated per response.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    /// Sampling temperature; `None` defers to the model default.
    #[serde(default)]
    pub temperature: Option<f32>,
}

/// Default model slug.
fn default_model() -> String {
    "anthropic/claude-sonnet-4".to_string()
}

/// Default OpenRouter API base URL.
fn default_base_url() -> String {
    "https://openrouter.ai/api/v1".to_string()
}

/// Default per-response token ceiling.
fn default_max_tokens() -> u32 {
    4096
}

impl Default for OpenRouterConfig {
    fn default() -> Self {
        Self {
            model: default_model(),
            base_url: default_base_url(),
            max_tokens: default_max_tokens(),
            temperature: None,
        }
    }
}

impl vault::module_config::ModuleConfig for OpenRouterConfig {
    const SCOPE: vault::scopes::Scope = vault::scopes::AI_CALLER_SCOPE;
    const CONFIG_NAME: &'static str = "ai_caller_config";
}
