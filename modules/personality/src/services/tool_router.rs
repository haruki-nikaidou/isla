//! Routes a model-emitted [`ToolCall`] to whoever can satisfy it.
//!
//! Two destinations exist:
//!
//! - **Internal** namespaces (e.g. `interface`, whose `send_message` tool lives
//!   in the `interface` module) are handled in-cluster.
//! - **Plugin** namespaces (e.g. `office.gmail`) are dispatched to the owning
//!   plugin. In production that is a
//!   [`SagaCallRequest`](dynamic_message_extension::dynamic_saga::SagaCallRequest)
//!   published over AMQP and correlated back by `context_id`; for Stage 1 the
//!   plugin destination is just another [`ToolHandler`] so the path is testable
//!   without a broker.

use std::collections::HashSet;
use std::sync::Arc;

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::Error;

use ai_caller::model::{ContentBlock, ToolCall};

/// Something that can execute a [`ToolCall`] and return its JSON output.
///
/// Any [`Processor<ToolCall>`] yielding `serde_json::Value` is a handler; this
/// alias just names the bound for readability.
pub trait ToolHandler: Processor<ToolCall, Output = serde_json::Value, Error = Error> {}

impl<T> ToolHandler for T where T: Processor<ToolCall, Output = serde_json::Value, Error = Error> {}

/// Dispatches each [`ToolCall`] to the internal handler or the plugin handler
/// based on its namespace, and wraps the result as a
/// [`ContentBlock::ToolResult`] ready to feed back to the model.
#[derive(Clone)]
pub struct ToolRouterService<I, P> {
    /// Namespaces served in-cluster. Anything not listed is treated as a plugin.
    pub internal_namespaces: Arc<HashSet<String>>,
    /// Handler for internal namespaces.
    pub internal: I,
    /// Handler for plugin namespaces (AMQP saga in production).
    pub plugin: P,
}

impl<I, P> Processor<ToolCall> for ToolRouterService<I, P>
where
    I: ToolHandler + Sync,
    P: ToolHandler + Sync,
{
    type Output = ContentBlock;
    type Error = Error;

    #[instrument(skip_all, name = "ToolCall", err, fields(namespace = %input.namespace, name = %input.name))]
    async fn process(&self, input: ToolCall) -> Result<ContentBlock, Error> {
        let id = input.id.clone();
        let output = if self.internal_namespaces.contains(&input.namespace) {
            self.internal.process(input).await?
        } else {
            self.plugin.process(input).await?
        };
        Ok(ContentBlock::ToolResult { id, output })
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use super::*;

    /// A handler that ignores its input and returns a fixed JSON value, so tests
    /// can assert which branch of the router fired.
    #[derive(Clone)]
    pub struct MockToolHandler {
        pub response: serde_json::Value,
    }

    impl Processor<ToolCall> for MockToolHandler {
        type Output = serde_json::Value;
        type Error = Error;

        #[instrument(skip_all, name = "ToolCall", err, ret)]
        async fn process(&self, _input: ToolCall) -> Result<serde_json::Value, Error> {
            Ok(self.response.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mock::MockToolHandler;
    use super::*;
    use serde_json::json;

    fn router() -> ToolRouterService<MockToolHandler, MockToolHandler> {
        ToolRouterService {
            internal_namespaces: Arc::new(HashSet::from(["interface".to_string()])),
            internal: MockToolHandler {
                response: json!({ "by": "internal" }),
            },
            plugin: MockToolHandler {
                response: json!({ "by": "plugin" }),
            },
        }
    }

    #[tokio::test]
    async fn internal_namespace_goes_to_internal_handler() {
        let block = router()
            .process(ToolCall {
                id: "t1".into(),
                namespace: "interface".into(),
                name: "send_message".into(),
                input: json!({}),
            })
            .await
            .expect("routed");
        assert_eq!(
            block,
            ContentBlock::ToolResult {
                id: "t1".into(),
                output: json!({ "by": "internal" }),
            }
        );
    }

    #[tokio::test]
    async fn unknown_namespace_goes_to_plugin_handler() {
        let block = router()
            .process(ToolCall {
                id: "t2".into(),
                namespace: "office.gmail".into(),
                name: "list_threads".into(),
                input: json!({}),
            })
            .await
            .expect("routed");
        assert_eq!(
            block,
            ContentBlock::ToolResult {
                id: "t2".into(),
                output: json!({ "by": "plugin" }),
            }
        );
    }
}
