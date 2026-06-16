//! Lifecycle hooks for memory operations.
//!
//! Hooks consume AMQP events and keep derived state in sync. For the summary
//! segment tree this is [`MessageMetricHook`]: it scores each appended message,
//! records its topical-shift metric, and registers (as empty placeholders) the
//! nodes whose buckets the append filled. Summaries themselves are produced
//! lazily by the read-path saga in
//! [`SummaryService`](crate::services::summary::SummaryService), not eagerly
//! here, so an append no longer cascades a chain of resummarizations.
//!
//! Event structs carry only metadata plus [`ContextRef`] pointers; each hook
//! resolves the content pointer against Redis before forwarding to its inner
//! service.

use dynamic_message_extension::dynamic_context::{ContextRef, RedisPool};
use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::Error;
use wakuwaku::amqp::AmqpMessageProcessor;

use crate::events::publish::{EntryUpserted, MessageAppended};
use crate::services::embedding::{Embedder, EmbeddingService, IndexEntry};
use crate::services::segment_tree::{RecordMessage, SegmentTreeService, SemanticScorer};

/// Scores an appended message, records its segment-tree metric, and registers
/// the now-full nodes as placeholders — running the LLM scorer off the
/// synchronous write path. Summarizing the registered nodes is deferred to the
/// read-path saga.
#[derive(Clone)]
pub struct MessageMetricHook<Sc, Su> {
    pub service: SegmentTreeService<Sc, Su>,
    pub redis: RedisPool,
}

impl<Sc, Su> AmqpMessageProcessor<MessageAppended> for MessageMetricHook<Sc, Su>
where
    Sc: SemanticScorer + Send + Sync,
    Su: Send + Sync,
{
    const QUEUE: &'static str = "memory_message_metric";
}

impl<Sc, Su> Processor<MessageAppended> for MessageMetricHook<Sc, Su>
where
    Sc: SemanticScorer + Send + Sync,
    Su: Send + Sync,
{
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "MessageAppended", err,
        fields(conversation_id = event.conversation_id, message_id = event.message_id))]
    async fn process(&self, event: MessageAppended) -> Result<(), Error> {
        let excerpt = ContextRef::resolve(&self.redis, event.current_excerpt).await?;
        self.service
            .process(RecordMessage {
                conversation_id: event.conversation_id,
                message_id: event.message_id,
                previous_excerpt: excerpt.previous_excerpt,
                current_excerpt: excerpt.current_excerpt,
            })
            .await?;
        Ok(())
    }
}

/// Keeps the RAG embedding index in sync with source entries by (re)embedding
/// on [`EntryUpserted`].
#[derive(Clone)]
pub struct EmbeddingUpdateHook<E> {
    pub service: EmbeddingService<E>,
    pub redis: RedisPool,
}

impl<E> AmqpMessageProcessor<EntryUpserted> for EmbeddingUpdateHook<E>
where
    E: Embedder + Send + Sync,
{
    const QUEUE: &'static str = "memory_embedding_upsert";
}

impl<E> Processor<EntryUpserted> for EmbeddingUpdateHook<E>
where
    E: Embedder + Send + Sync,
{
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "EntryUpserted", err)]
    async fn process(&self, event: EntryUpserted) -> Result<(), Error> {
        let content = ContextRef::resolve(&self.redis, event.content).await?;
        self.service
            .process(IndexEntry {
                reference: content.reference,
                privacy: content.privacy,
                content: content.content,
            })
            .await?;
        Ok(())
    }
}

