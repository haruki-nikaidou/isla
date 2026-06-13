//! Normalized, provider-agnostic representation of an LLM turn.
//!
//! These are pure value objects shared across the provider, the context
//! assembler, and the tool router. They deliberately mirror the persisted
//! `memory` types (roles, content modalities) without depending on them, so the
//! provider layer can translate to and from any upstream API
//! (Anthropic, OpenAI-compatible, …) at the edge.

pub mod content;
pub mod tool;
pub mod turn;

pub use content::ContentBlock;
pub use tool::{ToolCall, ToolSpec};
pub use turn::{
    GenerationParams, LlmMessage, LlmRequest, LlmResponse, Role, StopReason, SystemBlock,
};
