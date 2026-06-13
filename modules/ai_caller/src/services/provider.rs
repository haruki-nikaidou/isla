//! The seam between Isla and any upstream LLM vendor.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::Error;

use crate::model::{LlmRequest, LlmResponse};

/// An upstream LLM endpoint: turns a normalized [`LlmRequest`] into an
/// [`LlmResponse`].
///
/// This is the single seam between Isla and any model vendor. Real
/// implementations (`AnthropicProvider`, `OpenAiProvider`, …) will build a
/// vendor-specific HTTP request with the workspace `reqwest` client, pull the
/// API key from `vault`, and parse the vendor response back into the normalized
/// [`LlmResponse`]. None of that exists yet — [`MockProvider`] is enough to
/// drive and test the agent loop end to end.
pub trait LlmProvider: Processor<LlmRequest, Output = LlmResponse, Error = Error> {}

impl<T> LlmProvider for T where T: Processor<LlmRequest, Output = LlmResponse, Error = Error> {}

/// A provider that replays a fixed script of responses in order.
///
/// Each call to [`process`](Processor::process) pops the next scripted
/// [`LlmResponse`]. Exhausting the script is a [`Error::BusinessPanic`] rather
/// than a panic, honoring the crate's `#![deny(clippy::panic)]` contract.
#[derive(Clone, Default)]
pub struct MockProvider {
    responses: Arc<Mutex<VecDeque<LlmResponse>>>,
}

impl MockProvider {
    /// Build a mock that will hand back `responses` in the given order.
    pub fn new(responses: impl IntoIterator<Item = LlmResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        }
    }
}

impl Processor<LlmRequest> for MockProvider {
    type Output = LlmResponse;
    type Error = Error;

    #[instrument(skip_all, name = "MockProvider", err)]
    async fn process(&self, _input: LlmRequest) -> Result<LlmResponse, Error> {
        let mut queue = self
            .responses
            .lock()
            .map_err(|_| Error::BusinessPanic(anyhow::anyhow!("mock provider mutex poisoned")))?;
        queue
            .pop_front()
            .ok_or_else(|| Error::BusinessPanic(anyhow::anyhow!("mock provider script exhausted")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentBlock, GenerationParams, LlmRequest, StopReason};

    fn empty_request() -> LlmRequest {
        LlmRequest {
            system: Vec::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            params: GenerationParams::default(),
        }
    }

    #[tokio::test]
    async fn replays_scripted_responses_in_order() {
        let provider = MockProvider::new([
            LlmResponse {
                content: vec![ContentBlock::Text("first".into())],
                stop_reason: StopReason::EndTurn,
            },
            LlmResponse {
                content: vec![ContentBlock::Text("second".into())],
                stop_reason: StopReason::EndTurn,
            },
        ]);

        let first = provider.process(empty_request()).await.expect("first");
        assert_eq!(first.content, vec![ContentBlock::Text("first".into())]);
        let second = provider.process(empty_request()).await.expect("second");
        assert_eq!(second.content, vec![ContentBlock::Text("second".into())]);
    }

    #[tokio::test]
    async fn exhausted_script_is_an_error_not_a_panic() {
        let provider = MockProvider::new([]);
        assert!(provider.process(empty_request()).await.is_err());
    }
}
