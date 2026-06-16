//! The `interface.send_message` tool exposed to the model.
//!
//! This is the one tool every channel needs and it is fundamental rather than
//! optional, so it lives in the cluster (here) instead of in a plugin. The model
//! calls it with `{ platform, chat_id, text }`; the call is turned into an
//! [`OutboundUserMessage`] and published onto the bus, where the interface
//! delivers it to the right adapter.

use ai_caller::model::{ToolCall, ToolSpec};
use kanau::processor::Processor;
use serde_json::{Value, json};
use tracing::instrument;
use wakuwaku::Error;
use wakuwaku::amqp::{AmqpMessageSend, AmqpPool};

use crate::events::OutboundUserMessage;

/// Tool namespace handled in-cluster by the interface module.
pub const SEND_MESSAGE_NAMESPACE: &str = "interface";
/// Tool name the model uses to send a message to the user.
pub const SEND_MESSAGE_NAME: &str = "send_message";

/// Executes the model's `interface.send_message` tool calls.
#[derive(Clone)]
pub struct SendMessageTool {
    mq: AmqpPool,
}

impl SendMessageTool {
    /// Build the tool over an AMQP pool used to publish outbound messages.
    pub fn new(mq: AmqpPool) -> Self {
        Self { mq }
    }
}

/// Read a required string field from the tool-call input.
fn str_field(input: &Value, key: &str) -> Result<String, Error> {
    input
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(Error::InvalidInput)
}

/// Parse the model's tool-call input into an [`OutboundUserMessage`].
fn parse_send_message(input: &Value) -> Result<OutboundUserMessage, Error> {
    Ok(OutboundUserMessage {
        platform: str_field(input, "platform")?,
        chat_id: str_field(input, "chat_id")?,
        text: str_field(input, "text")?,
    })
}

impl Processor<ToolCall> for SendMessageTool {
    type Output = Value;
    type Error = Error;

    #[instrument(skip_all, name = "SendMessageTool", err)]
    async fn process(&self, input: ToolCall) -> Result<Value, Error> {
        let message = parse_send_message(&input.input)?;
        message.send(&self.mq).await?;
        Ok(json!({ "delivered": true }))
    }
}

/// The model-facing declaration of the `interface.send_message` tool.
///
/// A tool catalog (see `personality`'s assembler) advertises this so the model
/// knows it can reply to the user.
pub fn send_message_tool_spec() -> ToolSpec {
    ToolSpec {
        namespace: SEND_MESSAGE_NAMESPACE.to_owned(),
        name: SEND_MESSAGE_NAME.to_owned(),
        description: "Send a text message back to the user on their channel.".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "platform": { "type": "string", "description": "channel adapter, e.g. telegram" },
                "chat_id": { "type": "string", "description": "the user's chat/conversation address" },
                "text": { "type": "string", "description": "message text to send" }
            },
            "required": ["platform", "chat_id", "text"]
        }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_complete_input() {
        let input = json!({ "platform": "telegram", "chat_id": "42", "text": "hi" });
        let message = parse_send_message(&input).expect("valid input");
        assert_eq!(message.platform, "telegram");
        assert_eq!(message.chat_id, "42");
        assert_eq!(message.text, "hi");
    }

    #[test]
    fn missing_field_is_invalid_input() {
        let input = json!({ "platform": "telegram", "text": "hi" });
        assert!(matches!(
            parse_send_message(&input),
            Err(Error::InvalidInput)
        ));
    }

    #[test]
    fn non_string_field_is_invalid_input() {
        let input = json!({ "platform": "telegram", "chat_id": 42, "text": "hi" });
        assert!(matches!(
            parse_send_message(&input),
            Err(Error::InvalidInput)
        ));
    }

    #[test]
    fn tool_spec_addresses_interface_namespace() {
        let spec = send_message_tool_spec();
        assert_eq!(spec.namespace, SEND_MESSAGE_NAMESPACE);
        assert_eq!(spec.name, SEND_MESSAGE_NAME);
    }
}
