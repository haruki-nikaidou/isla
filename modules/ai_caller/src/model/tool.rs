//! Tool advertisements and the calls the model produces against them.

use serde::{Deserialize, Serialize};

/// A tool the model is allowed to call, advertised in an
/// [`LlmRequest`](super::turn::LlmRequest).
///
/// `namespace` is what the tool router keys on: internal namespaces (such as
/// `interface`) are handled in-cluster, while a plugin namespace (such as
/// `office.gmail`) is dispatched to that plugin over AMQP.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    /// Routing namespace, e.g. `interface` or `office.gmail`.
    pub namespace: String,
    /// Tool name within the namespace.
    pub name: String,
    /// Human/model-readable description of what the tool does.
    pub description: String,
    /// JSON Schema describing the tool's accepted arguments.
    pub input_schema: serde_json::Value,
}

/// A single tool invocation emitted by the model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    /// Provider-assigned id correlating this call with its
    /// [`ContentBlock::ToolResult`](super::content::ContentBlock::ToolResult).
    pub id: String,
    /// Routing namespace (mirrors [`ToolSpec::namespace`]).
    pub namespace: String,
    /// Tool name within the namespace.
    pub name: String,
    /// Arguments the model passed, as raw JSON.
    pub input: serde_json::Value,
}
