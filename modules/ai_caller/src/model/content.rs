//! Multimodal content blocks that make up messages and responses.

use serde::{Deserialize, Serialize};

use super::tool::ToolCall;

/// One block of content within a message or model response.
///
/// Mirrors the persisted `memory::ContentModality` shape without depending on
/// it, plus the tool-use/tool-result blocks that only exist in flight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ContentBlock {
    /// Plain text.
    Text(String),
    /// An image referenced by its object-storage hash.
    Image {
        /// 32-byte content hash into `memory.object_storage`.
        object_hash: Vec<u8>,
        /// Optional detail hint (e.g. `low`, `high`, `auto`).
        detail: Option<String>,
    },
    /// The model is requesting a tool invocation.
    ToolUse(ToolCall),
    /// The result of a previously requested tool invocation, fed back in.
    ToolResult {
        /// Id of the [`ToolCall`] this answers.
        id: String,
        /// Tool output, as raw JSON.
        output: serde_json::Value,
    },
}
