//! Builds the [`LlmRequest`] for a turn from personality, memory, and tools.
//!
//! This is goal 1 of Stage 1: *what is sent to the LLM is decided by
//! personality + memory*. The assembler pulls each ingredient through a seam
//! trait so it can be mocked in tests today and backed by the real
//! `memory_repository` / `plugin_registrar` services (in-process now, over
//! AMQP/gRPC later) without changing this code.

use kanau::processor::Processor;
use memory_repository::entities::db::contact_identity::Relationship;
use tracing::instrument;
use wakuwaku::Error;

use crate::model::{GenerationParams, LlmMessage, LlmRequest, SystemBlock, ToolSpec};

/// The conversational context that decides which personality facets and which
/// memories are in scope for a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Environment {
    /// Conversation whose history should be loaded.
    pub conversation_id: i64,
    /// Relationship of the peer the agent is talking to; drives facet
    /// selection (private vs. public behavior).
    pub relationship: Relationship,
}

/// Seam: source of personality facet text applicable to a peer, pre-ordered.
///
/// Backed by `memory_repository::services::personality::PersonalityService`.
pub trait PersonalitySource:
    Processor<FacetRequest, Output = Vec<String>, Error = Error>
{
}
impl<T> PersonalitySource for T where T: Processor<FacetRequest, Output = Vec<String>, Error = Error> {}

/// Request for the personality facets applicable to a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FacetRequest {
    pub relationship: Relationship,
}

/// Seam: source of recent conversation history as normalized messages.
///
/// Backed by `memory_repository::services::conversation::ConversationService`.
pub trait HistorySource:
    Processor<HistoryRequest, Output = Vec<LlmMessage>, Error = Error>
{
}
impl<T> HistorySource for T where T: Processor<HistoryRequest, Output = Vec<LlmMessage>, Error = Error>
{}

/// Request for the most recent messages of a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryRequest {
    pub conversation_id: i64,
    pub limit: i64,
}

/// Seam: source of the tools the model may call this turn.
///
/// Backed by `plugin_registrar` plus the internal interface tools.
pub trait ToolCatalog: Processor<ToolCatalogRequest, Output = Vec<ToolSpec>, Error = Error> {}
impl<T> ToolCatalog for T where T: Processor<ToolCatalogRequest, Output = Vec<ToolSpec>, Error = Error>
{}

/// Request for the available tool catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ToolCatalogRequest;

/// Assembles an [`LlmRequest`] from the three seams.
#[derive(Clone)]
pub struct ContextAssemblerService<Pe, Hi, To> {
    pub personality: Pe,
    pub history: Hi,
    pub tools: To,
    /// Decoding parameters applied to every assembled request.
    pub params: GenerationParams,
}

/// Request to assemble the context for one turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssembleContextRequest {
    pub environment: Environment,
    /// How many recent messages to load into context.
    pub history_limit: i64,
}

impl<Pe, Hi, To> Processor<AssembleContextRequest> for ContextAssemblerService<Pe, Hi, To>
where
    Pe: PersonalitySource + Sync,
    Hi: HistorySource + Sync,
    To: ToolCatalog + Sync,
{
    type Output = LlmRequest;
    type Error = Error;

    #[instrument(skip_all, name = "AssembleContextRequest", err,
        fields(conversation_id = input.environment.conversation_id))]
    async fn process(&self, input: AssembleContextRequest) -> Result<LlmRequest, Error> {
        let facets = self
            .personality
            .process(FacetRequest {
                relationship: input.environment.relationship,
            })
            .await?;
        let messages = self
            .history
            .process(HistoryRequest {
                conversation_id: input.environment.conversation_id,
                limit: input.history_limit,
            })
            .await?;
        let tools = self.tools.process(ToolCatalogRequest).await?;

        let system = facets
            .into_iter()
            .map(|text| SystemBlock { text })
            .collect();

        Ok(LlmRequest {
            system,
            messages,
            tools,
            params: self.params,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentBlock, Role};

    /// Mock personality source: Master also gets an intimate facet, others
    /// only the shared core — modelling "different behavior per audience".
    #[derive(Clone)]
    struct MockPersonality;
    impl Processor<FacetRequest> for MockPersonality {
        type Output = Vec<String>;
        type Error = Error;
        async fn process(&self, input: FacetRequest) -> Result<Vec<String>, Error> {
            let mut facets = vec!["I am Isla.".to_string()];
            if input.relationship == Relationship::Master {
                facets.push("I am warm and candid with you.".to_string());
            }
            Ok(facets)
        }
    }

    #[derive(Clone)]
    struct MockHistory;
    impl Processor<HistoryRequest> for MockHistory {
        type Output = Vec<LlmMessage>;
        type Error = Error;
        async fn process(&self, _input: HistoryRequest) -> Result<Vec<LlmMessage>, Error> {
            Ok(vec![LlmMessage {
                role: Role::User,
                content: vec![ContentBlock::Text("hi".into())],
            }])
        }
    }

    #[derive(Clone)]
    struct MockTools;
    impl Processor<ToolCatalogRequest> for MockTools {
        type Output = Vec<ToolSpec>;
        type Error = Error;
        async fn process(&self, _input: ToolCatalogRequest) -> Result<Vec<ToolSpec>, Error> {
            Ok(Vec::new())
        }
    }

    fn assembler() -> ContextAssemblerService<MockPersonality, MockHistory, MockTools> {
        ContextAssemblerService {
            personality: MockPersonality,
            history: MockHistory,
            tools: MockTools,
            params: GenerationParams::default(),
        }
    }

    #[tokio::test]
    async fn master_and_stranger_get_different_system_prompts() {
        let master = assembler()
            .process(AssembleContextRequest {
                environment: Environment {
                    conversation_id: 1,
                    relationship: Relationship::Master,
                },
                history_limit: 20,
            })
            .await
            .expect("assembled");
        let stranger = assembler()
            .process(AssembleContextRequest {
                environment: Environment {
                    conversation_id: 1,
                    relationship: Relationship::Stranger,
                },
                history_limit: 20,
            })
            .await
            .expect("assembled");

        assert_eq!(master.system.len(), 2);
        assert_eq!(stranger.system.len(), 1);
        // History and tools are wired through regardless of audience.
        assert_eq!(master.messages.len(), 1);
    }
}
