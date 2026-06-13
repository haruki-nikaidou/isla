//! The request/response pair for a single round-trip with the model.

use serde::{Deserialize, Serialize};

use super::content::ContentBlock;
use super::tool::{ToolCall, ToolSpec};

/// Role of a message in a turn. Mirrors `memory::MessageRole`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// System instructions or context.
    System,
    /// Message from the human user.
    User,
    /// Response from the model.
    Assistant,
    /// Output fed back from a tool invocation.
    Tool,
}

/// A single message in the conversation passed to the model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmMessage {
    /// Who produced this message.
    pub role: Role,
    /// Ordered content blocks making up the message.
    pub content: Vec<ContentBlock>,
}

/// One standalone block of the system prompt.
///
/// The system prompt is assembled from many independently-sourced blocks
/// (personality facets, memory summaries, …) so each can be cached and reasoned
/// about on its own. The provider concatenates them at the edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemBlock {
    /// The instruction text for this block.
    pub text: String,
}

/// Decoding parameters for a request.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GenerationParams {
    /// Maximum number of tokens to generate.
    pub max_tokens: u32,
    /// Sampling temperature; `None` defers to the provider default.
    pub temperature: Option<f32>,
}

impl Default for GenerationParams {
    fn default() -> Self {
        Self {
            max_tokens: 4096,
            temperature: None,
        }
    }
}

/// A normalized, provider-agnostic request for one model round-trip.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmRequest {
    /// System prompt, as independently-sourced blocks.
    pub system: Vec<SystemBlock>,
    /// Conversation history in chronological order.
    pub messages: Vec<LlmMessage>,
    /// Tools the model may call this turn.
    pub tools: Vec<ToolSpec>,
    /// Decoding parameters.
    pub params: GenerationParams,
}

/// Why the model stopped generating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopReason {
    /// The model finished its turn normally.
    EndTurn,
    /// The model is requesting tool calls before it can continue.
    ToolUse,
    /// The model hit the output token limit.
    MaxTokens,
}

/// A normalized, provider-agnostic response from one model round-trip.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmResponse {
    /// Content blocks the model produced.
    pub content: Vec<ContentBlock>,
    /// Why generation stopped.
    pub stop_reason: StopReason,
}

impl LlmResponse {
    /// Iterate over every tool call the model emitted in this response.
    pub fn tool_calls(&self) -> impl Iterator<Item = &ToolCall> {
        self.content.iter().filter_map(|block| match block {
            ContentBlock::ToolUse(call) => Some(call),
            _ => None,
        })
    }
}
