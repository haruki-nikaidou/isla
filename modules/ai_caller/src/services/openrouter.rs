//! The real, OpenRouter-backed [`LlmProvider`](super::provider::LlmProvider).
//!
//! Translates the normalized [`LlmRequest`]/[`LlmResponse`] model at the edge
//! into OpenRouter's OpenAI-compatible chat-completions shape. Because the
//! blanket `impl LlmProvider for T` lives in [`super::provider`], implementing
//! [`Processor<LlmRequest>`] here is all it takes for [`OpenRouterProvider`] to
//! be usable wherever an `LlmProvider` is expected.

use std::collections::HashMap;

use kanau::processor::Processor;
use openrouter_rs::OpenRouterClient;
use openrouter_rs::api::chat::{ChatCompletionRequest, Message};
use openrouter_rs::types::completion::{self, Choice, FinishReason};
use openrouter_rs::types::{Role as OrRole, Tool, ToolChoice};
use tracing::instrument;
use wakuwaku::Error;

use crate::config::OpenRouterConfig;
use crate::model::{ContentBlock, LlmRequest, LlmResponse, Role, StopReason, ToolCall, ToolSpec};

/// Marker inserted into a message's text where an [`ContentBlock::Image`] block
/// appeared. Resolving the image bytes requires object storage, which is not
/// wired through this seam yet, so only the marker is forwarded.
const IMAGE_MARKER: &str = "[image]";

/// An [`LlmProvider`](super::provider::LlmProvider) backed by the OpenRouter
/// chat-completions API.
///
/// Construct with [`OpenRouterProvider::new`], passing the API key (a secret
/// fetched from `vault`) and the loaded [`OpenRouterConfig`].
pub struct OpenRouterProvider {
    client: OpenRouterClient,
    model: String,
    max_tokens: u32,
    temperature: Option<f32>,
}

impl OpenRouterProvider {
    /// Build a provider against `config`, authenticating with `api_key`.
    ///
    /// The API key is the secret pulled from `vault`; everything else comes
    /// from the stored [`OpenRouterConfig`].
    pub fn new(api_key: String, config: &OpenRouterConfig) -> Result<Self, Error> {
        let client = OpenRouterClient::builder()
            .api_key(api_key)
            .base_url(config.base_url.clone())
            .build()
            .map_err(|e| Error::BusinessPanic(anyhow::anyhow!(e)))?;
        Ok(Self {
            client,
            model: config.model.clone(),
            max_tokens: config.max_tokens,
            temperature: config.temperature,
        })
    }
}

impl Processor<LlmRequest> for OpenRouterProvider {
    type Output = LlmResponse;
    type Error = Error;

    #[instrument(skip_all, name = "OpenRouterProvider", err)]
    async fn process(&self, input: LlmRequest) -> Result<LlmResponse, Error> {
        let messages = build_or_messages(&input);
        let (tools, decode) = build_or_tools(&input.tools);

        let mut builder = ChatCompletionRequest::builder();
        builder
            .model(self.model.clone())
            .messages(messages)
            .max_tokens(self.max_tokens);
        if let Some(temperature) = self.temperature {
            builder.temperature(f64::from(temperature));
        }
        if !tools.is_empty() {
            builder.tools(tools).tool_choice(ToolChoice::auto());
        }
        let request = builder
            .build()
            .map_err(|e| Error::BusinessPanic(anyhow::anyhow!(e)))?;

        let response = self
            .client
            .chat()
            .create(&request)
            .await
            .map_err(|e| Error::Io(anyhow::anyhow!(e)))?;

        let choice = response
            .choices
            .first()
            .ok_or_else(|| Error::Io(anyhow::anyhow!("openrouter returned no choices")))?;
        parse_choice(choice, &decode)
    }
}

/// Encode a `(namespace, name)` pair into a single OpenRouter function name.
///
/// The result only contains characters OpenRouter accepts (`[A-Za-z0-9_-]`) and
/// is capped at 64 bytes. The two halves are joined with a double underscore;
/// any other character is replaced by a single underscore. The encoding is not
/// string-reversible (it is lossy), so the original pair is recovered through
/// the decode map produced by [`build_or_tools`] instead.
fn model_tool_name(namespace: &str, name: &str) -> String {
    fn push_sanitized(part: &str, out: &mut String) {
        for c in part.chars() {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                out.push(c);
            } else {
                out.push('_');
            }
        }
    }

    let mut out = String::with_capacity(namespace.len() + name.len() + 2);
    push_sanitized(namespace, &mut out);
    out.push_str("__");
    push_sanitized(name, &mut out);
    out.truncate(64);
    out
}

/// Map a normalized [`Role`] onto the OpenRouter [`OrRole`].
fn map_role(role: Role) -> OrRole {
    match role {
        Role::System => OrRole::System,
        Role::User => OrRole::User,
        Role::Assistant => OrRole::Assistant,
        Role::Tool => OrRole::Tool,
    }
}

/// Render a tool output value as the plain text OpenRouter expects.
///
/// JSON strings are passed through verbatim; anything else is serialized.
fn output_to_text(output: &serde_json::Value) -> String {
    match output {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Parse tool-call argument text into JSON, defaulting empty arguments to `{}`.
fn parse_arguments(arguments: &str) -> Result<serde_json::Value, Error> {
    if arguments.is_empty() {
        Ok(serde_json::json!({}))
    } else {
        serde_json::from_str(arguments).map_err(|e| Error::BusinessPanic(anyhow::anyhow!(e)))
    }
}

/// Translate the normalized request into the ordered OpenRouter message list.
fn build_or_messages(req: &LlmRequest) -> Vec<Message> {
    let mut messages = Vec::new();

    if !req.system.is_empty() {
        let system_text = req
            .system
            .iter()
            .map(|block| block.text.as_str())
            .collect::<Vec<_>>()
            .join("\n\n");
        messages.push(Message::new(OrRole::System, system_text));
    }

    for msg in &req.messages {
        let mut text_parts: Vec<String> = Vec::new();
        let mut tool_uses: Vec<&ToolCall> = Vec::new();
        let mut tool_results: Vec<(&String, &serde_json::Value)> = Vec::new();

        for block in &msg.content {
            match block {
                ContentBlock::Text(text) => text_parts.push(text.clone()),
                ContentBlock::Image { .. } => text_parts.push(IMAGE_MARKER.to_string()),
                ContentBlock::ToolUse(call) => tool_uses.push(call),
                ContentBlock::ToolResult { id, output } => tool_results.push((id, output)),
            }
        }

        let text = text_parts.join("\n");

        if !tool_results.is_empty() {
            for (id, output) in tool_results {
                messages.push(Message::tool_response(id, output_to_text(output)));
            }
        } else if msg.role == Role::Assistant && !tool_uses.is_empty() {
            let or_tool_calls = tool_uses
                .iter()
                .map(|call| {
                    completion::ToolCall::new(
                        call.id.clone(),
                        model_tool_name(&call.namespace, &call.name),
                        call.input.to_string(),
                    )
                })
                .collect::<Vec<_>>();
            messages.push(Message::assistant_with_tool_calls(text, or_tool_calls));
        } else {
            messages.push(Message::new(map_role(msg.role), text));
        }
    }

    messages
}

/// Translate advertised [`ToolSpec`]s into OpenRouter [`Tool`]s, returning the
/// decode map from encoded function name back to `(namespace, name)`.
fn build_or_tools(tools: &[ToolSpec]) -> (Vec<Tool>, HashMap<String, (String, String)>) {
    let mut or_tools = Vec::with_capacity(tools.len());
    let mut decode = HashMap::with_capacity(tools.len());
    for spec in tools {
        let key = model_tool_name(&spec.namespace, &spec.name);
        or_tools.push(Tool::new(
            &key,
            &spec.description,
            spec.input_schema.clone(),
        ));
        decode.insert(key, (spec.namespace.clone(), spec.name.clone()));
    }
    (or_tools, decode)
}

/// Decide the normalized [`StopReason`] from OpenRouter's finish reason.
///
/// Present tool calls always win (they imply the model wants a tool round-trip).
/// The [`FinishReason::Error`] case is handled separately by [`parse_choice`],
/// which surfaces it as an error; here it collapses to [`StopReason::EndTurn`].
fn map_finish_reason(reason: Option<&FinishReason>, has_tool_calls: bool) -> StopReason {
    if has_tool_calls {
        return StopReason::ToolUse;
    }
    match reason {
        Some(FinishReason::Length) => StopReason::MaxTokens,
        Some(FinishReason::ToolCalls) => StopReason::ToolUse,
        // Stop, ContentFilter, Error, None and any future variant end the turn.
        _ => StopReason::EndTurn,
    }
}

/// Parse a single OpenRouter [`Choice`] into the normalized [`LlmResponse`].
fn parse_choice(
    choice: &Choice,
    decode: &HashMap<String, (String, String)>,
) -> Result<LlmResponse, Error> {
    let mut content = Vec::new();

    if let Some(text) = choice.content()
        && !text.is_empty()
    {
        content.push(ContentBlock::Text(text.to_string()));
    }

    let tool_calls = choice.tool_calls().unwrap_or(&[]);
    for call in tool_calls {
        let (namespace, name) = decode
            .get(&call.function.name)
            .cloned()
            .unwrap_or_else(|| (String::new(), call.function.name.clone()));
        let input = parse_arguments(&call.function.arguments)?;
        content.push(ContentBlock::ToolUse(ToolCall {
            id: call.id.clone(),
            namespace,
            name,
            input,
        }));
    }

    let has_tool_calls = !tool_calls.is_empty();
    if !has_tool_calls && matches!(choice.finish_reason(), Some(FinishReason::Error)) {
        return Err(Error::Io(anyhow::anyhow!(
            "openrouter returned finish_reason=error"
        )));
    }

    let stop_reason = map_finish_reason(choice.finish_reason(), has_tool_calls);
    Ok(LlmResponse {
        content,
        stop_reason,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::model::{GenerationParams, LlmMessage, SystemBlock};

    #[test]
    fn model_tool_name_joins_and_sanitizes() {
        assert_eq!(
            model_tool_name("office.gmail", "send_email"),
            "office_gmail__send_email"
        );
        // Already-valid characters are preserved.
        assert_eq!(
            model_tool_name("interface", "reply-now"),
            "interface__reply-now"
        );
    }

    #[test]
    fn model_tool_name_caps_at_64_bytes() {
        let long = "x".repeat(100);
        assert!(model_tool_name(&long, &long).len() <= 64);
    }

    #[test]
    fn build_or_tools_decode_map_round_trips() {
        let tools = vec![ToolSpec {
            namespace: "office.gmail".to_string(),
            name: "send_email".to_string(),
            description: "Send an email".to_string(),
            input_schema: serde_json::json!({"type": "object"}),
        }];
        let (or_tools, decode) = build_or_tools(&tools);
        assert_eq!(or_tools.len(), 1);

        let key = model_tool_name("office.gmail", "send_email");
        let (namespace, name) = decode
            .get(&key)
            .expect("encoded key should be in decode map");
        assert_eq!(namespace, "office.gmail");
        assert_eq!(name, "send_email");
    }

    #[test]
    fn build_or_messages_maps_each_block_kind() {
        let req = LlmRequest {
            system: vec![
                SystemBlock {
                    text: "first".to_string(),
                },
                SystemBlock {
                    text: "second".to_string(),
                },
            ],
            messages: vec![
                LlmMessage {
                    role: Role::User,
                    content: vec![ContentBlock::Text("hello".to_string())],
                },
                LlmMessage {
                    role: Role::Assistant,
                    content: vec![ContentBlock::ToolUse(ToolCall {
                        id: "call_1".to_string(),
                        namespace: "office.gmail".to_string(),
                        name: "send_email".to_string(),
                        input: serde_json::json!({"to": "a@b.c"}),
                    })],
                },
                LlmMessage {
                    role: Role::Tool,
                    content: vec![ContentBlock::ToolResult {
                        id: "call_1".to_string(),
                        output: serde_json::json!("sent"),
                    }],
                },
            ],
            tools: Vec::new(),
            params: GenerationParams::default(),
        };

        let messages = build_or_messages(&req);

        // system + user + assistant(with tool calls) + tool response
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].role, OrRole::System);
        assert_eq!(messages[1].role, OrRole::User);
        assert_eq!(messages[2].role, OrRole::Assistant);
        assert!(messages[2].tool_calls.is_some());
        assert_eq!(messages[3].role, OrRole::Tool);
        assert_eq!(messages[3].tool_call_id.as_deref(), Some("call_1"));
    }

    #[test]
    fn build_or_messages_concatenates_system_blocks() {
        let req = LlmRequest {
            system: vec![
                SystemBlock {
                    text: "a".to_string(),
                },
                SystemBlock {
                    text: "b".to_string(),
                },
            ],
            messages: Vec::new(),
            tools: Vec::new(),
            params: GenerationParams::default(),
        };
        let messages = build_or_messages(&req);
        assert_eq!(messages.len(), 1);
        match &messages[0].content {
            openrouter_rs::Content::Text(text) => assert_eq!(text, "a\n\nb"),
            other => panic!("expected joined system text, got {other:?}"),
        }
    }

    #[test]
    fn output_to_text_passes_strings_through_and_serializes_others() {
        assert_eq!(output_to_text(&serde_json::json!("hi")), "hi");
        assert_eq!(output_to_text(&serde_json::json!(42)), "42");
        assert_eq!(
            output_to_text(&serde_json::json!({"ok": true})),
            "{\"ok\":true}"
        );
    }

    #[test]
    fn parse_arguments_defaults_empty_to_object() {
        assert_eq!(parse_arguments("").unwrap(), serde_json::json!({}));
        assert_eq!(
            parse_arguments("{\"a\":1}").unwrap(),
            serde_json::json!({"a": 1})
        );
        assert!(parse_arguments("not json").is_err());
    }

    #[test]
    fn map_finish_reason_prioritizes_tool_calls() {
        assert_eq!(
            map_finish_reason(Some(&FinishReason::Stop), true),
            StopReason::ToolUse
        );
        assert_eq!(
            map_finish_reason(Some(&FinishReason::Stop), false),
            StopReason::EndTurn
        );
        assert_eq!(
            map_finish_reason(Some(&FinishReason::Length), false),
            StopReason::MaxTokens
        );
        assert_eq!(
            map_finish_reason(Some(&FinishReason::ToolCalls), false),
            StopReason::ToolUse
        );
        assert_eq!(
            map_finish_reason(Some(&FinishReason::ContentFilter), false),
            StopReason::EndTurn
        );
        assert_eq!(map_finish_reason(None, false), StopReason::EndTurn);
    }
}
