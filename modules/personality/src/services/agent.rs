//! The agent turn loop: call the model, route any tool use, repeat until done.
//!
//! This is the orchestration core of Stage 1. With a mocked provider and a
//! mocked tool router it runs end to end with no network and no broker, which
//! is exactly what makes the four Stage 1 goals testable today.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::Error;

use ai_caller::model::{
    ContentBlock, LlmMessage, LlmRequest, LlmResponse, Role, StopReason, ToolCall,
};

/// Upper bound on provider round-trips in a single turn, so a model that keeps
/// asking for tools can never loop forever. Defensive by design.
const MAX_ROUNDS: u32 = 16;

/// Drives one user turn to completion: assemble → call model → route tools →
/// feed results back → repeat until the model ends its turn.
///
/// Generic over the provider and the tool router so tests inject mocks and the
/// real wiring injects the live provider + AMQP router without touching the
/// loop.
#[derive(Clone)]
pub struct AgentTurnService<P, R> {
    /// The upstream model.
    pub provider: P,
    /// The tool-call router.
    pub router: R,
}

/// Input to one agent turn: the fully-assembled request to start from.
///
/// In Stage 1 the caller supplies the [`LlmRequest`] directly. Once the context
/// assembler lands (Phase C) the request is built from personality + memory and
/// this becomes `{ conversation_id, environment }`.
#[derive(Debug, Clone)]
pub struct AgentTurnRequest {
    /// The assembled request to send on the first round.
    pub request: LlmRequest,
}

/// The result of an agent turn.
#[derive(Debug, Clone)]
pub struct AgentTurnOutcome {
    /// Messages produced during the turn (assistant responses and tool
    /// results), in chronological order, ready to persist as conversation
    /// history.
    pub produced: Vec<LlmMessage>,
    /// Number of provider round-trips taken.
    pub rounds: u32,
}

impl<P, R> Processor<AgentTurnRequest> for AgentTurnService<P, R>
where
    P: Processor<LlmRequest, Output = LlmResponse, Error = Error> + Sync,
    R: Processor<ToolCall, Output = ContentBlock, Error = Error> + Sync,
{
    type Output = AgentTurnOutcome;
    type Error = Error;

    #[instrument(skip_all, name = "AgentTurnRequest", err)]
    async fn process(&self, input: AgentTurnRequest) -> Result<AgentTurnOutcome, Error> {
        let mut request = input.request;
        let mut produced = Vec::new();

        for round in 1..=MAX_ROUNDS {
            let response = self.provider.process(request.clone()).await?;

            // Record the model's output as an assistant message.
            let assistant = LlmMessage {
                role: Role::Assistant,
                content: response.content.clone(),
            };
            produced.push(assistant.clone());
            request.messages.push(assistant);

            let calls: Vec<ToolCall> = response.tool_calls().cloned().collect();
            if calls.is_empty() {
                return if response.stop_reason != StopReason::ToolUse {
                    Ok(AgentTurnOutcome {
                        produced,
                        rounds: round,
                    })
                } else {
                    Err(Error::InvalidInput)
                };
            }

            // Route every requested tool call and feed the results back as a
            // single tool message before the next round.
            let mut results = Vec::with_capacity(calls.len());
            for call in calls {
                results.push(self.router.process(call).await?);
            }
            let tool_message = LlmMessage {
                role: Role::Tool,
                content: results,
            };
            produced.push(tool_message.clone());
            request.messages.push(tool_message);
        }

        Err(Error::BusinessPanic(anyhow::anyhow!(
            "agent turn exceeded {MAX_ROUNDS} rounds without ending"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::tool_router::ToolRouterService;
    use crate::services::tool_router::mock::MockToolHandler;
    use ai_caller::model::{GenerationParams, ToolSpec};
    use ai_caller::services::provider::MockProvider;
    use serde_json::json;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn router() -> ToolRouterService<MockToolHandler, MockToolHandler> {
        ToolRouterService {
            internal_namespaces: Arc::new(HashSet::from(["interface".to_string()])),
            internal: MockToolHandler {
                response: json!({ "sent": true }),
            },
            plugin: MockToolHandler {
                response: json!({ "by": "plugin" }),
            },
        }
    }

    fn user_request() -> LlmRequest {
        LlmRequest {
            system: Vec::new(),
            messages: vec![LlmMessage {
                role: Role::User,
                content: vec![ContentBlock::Text("say hi to me".into())],
            }],
            tools: vec![ToolSpec {
                namespace: "interface".into(),
                name: "send_message".into(),
                description: "send a message to the user".into(),
                input_schema: json!({ "type": "object" }),
            }],
            params: GenerationParams::default(),
        }
    }

    /// Stage 1 acceptance: mocked user input → model asks for a tool →
    /// router dispatches it → second model response ends the turn.
    /// Exercises all four Stage 1 goals in one pass.
    #[tokio::test]
    async fn full_turn_with_one_tool_call() {
        let provider = MockProvider::new([
            LlmResponse {
                content: vec![ContentBlock::ToolUse(ToolCall {
                    id: "call-1".into(),
                    namespace: "interface".into(),
                    name: "send_message".into(),
                    input: json!({ "text": "hi" }),
                })],
                stop_reason: StopReason::ToolUse,
            },
            LlmResponse {
                content: vec![ContentBlock::Text("done".into())],
                stop_reason: StopReason::EndTurn,
            },
        ]);

        let agent = AgentTurnService {
            provider,
            router: router(),
        };
        let outcome = agent
            .process(AgentTurnRequest {
                request: user_request(),
            })
            .await
            .expect("turn completes");

        assert_eq!(outcome.rounds, 2);
        // assistant(tool_use), tool(result), assistant(text)
        assert_eq!(outcome.produced.len(), 3);
        assert_eq!(outcome.produced[1].role, Role::Tool);
        assert_eq!(
            outcome.produced[1].content,
            vec![ContentBlock::ToolResult {
                id: "call-1".into(),
                output: json!({ "sent": true }),
            }]
        );
        assert_eq!(
            outcome.produced[2].content,
            vec![ContentBlock::Text("done".into())]
        );
    }

    #[tokio::test]
    async fn turn_without_tools_ends_in_one_round() {
        let provider = MockProvider::new([LlmResponse {
            content: vec![ContentBlock::Text("hello".into())],
            stop_reason: StopReason::EndTurn,
        }]);
        let agent = AgentTurnService {
            provider,
            router: router(),
        };
        let outcome = agent
            .process(AgentTurnRequest {
                request: user_request(),
            })
            .await
            .expect("turn completes");
        assert_eq!(outcome.rounds, 1);
        assert_eq!(outcome.produced.len(), 1);
    }
}
