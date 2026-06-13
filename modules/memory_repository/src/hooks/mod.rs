//! Lifecycle hooks for memory operations.
//!
//! Hooks consume AMQP events and keep derived state in sync. Today this is the
//! summary segment tree: [`SegmentTreeHook`] reacts to
//! [`SummaryNodesDirty`](crate::events::publish::SummaryNodesDirty) by
//! recomputing the finalized nodes.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::Error;
use wakuwaku::amqp::{AmqpMessageProcessor, AmqpMessageSend, AmqpPool};
use wakuwaku::sqlx::DatabaseProcessor;

use crate::entities::db::embedding::{DeleteEmbeddingByRef, UpdateEmbeddingPrivacyByRef};
use crate::events::publish::{
    EntryPrivacyChanged, EntryRemoved, EntryUpserted, MessageAppended, SummaryNodesDirty,
};
use crate::services::embedding::{Embedder, EmbeddingService, IndexEntry};
use crate::services::segment_tree::{
    RecomputeNode, RecordMessage, SegmentTreeService, SemanticScorer, Summarizer,
};

/// Recomputes summary segment-tree nodes when an append finalizes them.
#[derive(Debug, Clone)]
pub struct SegmentTreeHook<Sc, Su> {
    pub service: SegmentTreeService<Sc, Su>,
}

impl<Sc, Su> AmqpMessageProcessor<SummaryNodesDirty> for SegmentTreeHook<Sc, Su>
where
    Sc: Send + Sync,
    Su: Summarizer + Send + Sync,
{
    const QUEUE: &'static str = "memory_segment_tree_recompute";
}

impl<Sc, Su> Processor<SummaryNodesDirty> for SegmentTreeHook<Sc, Su>
where
    Sc: Send + Sync,
    Su: Summarizer + Send + Sync,
{
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "SummaryNodesDirty", err,
        fields(conversation_id = event.conversation_id, nodes = event.nodes.len()))]
    async fn process(&self, event: SummaryNodesDirty) -> Result<(), Error> {
        // `nodes` arrives children-before-parents, so a sequential recompute
        // sees each child already finalized before its parent combines them.
        for node in event.nodes {
            self.service
                .process(RecomputeNode {
                    conversation_id: event.conversation_id,
                    node,
                })
                .await?;
        }
        Ok(())
    }
}

/// Scores an appended message, records its segment-tree metric, and emits
/// [`SummaryNodesDirty`] for any nodes the append finalized — running the LLM
/// scorer off the synchronous write path.
#[derive(Clone)]
pub struct MessageMetricHook<Sc, Su> {
    pub service: SegmentTreeService<Sc, Su>,
    pub mq: AmqpPool,
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
        let nodes = self
            .service
            .process(RecordMessage {
                conversation_id: event.conversation_id,
                message_id: event.message_id,
                previous_excerpt: String::new(),
                current_excerpt: event.current_excerpt,
            })
            .await?;
        if !nodes.is_empty() {
            SummaryNodesDirty {
                conversation_id: event.conversation_id,
                nodes,
            }
            .send(&self.mq)
            .await?;
        }
        Ok(())
    }
}

/// Keeps the RAG embedding index in sync with source entries: (re)embeds on
/// [`EntryUpserted`] and removes on [`EntryRemoved`].
#[derive(Debug, Clone)]
pub struct EmbeddingUpdateHook<E> {
    pub service: EmbeddingService<E>,
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
        self.service
            .process(IndexEntry {
                reference: event.reference,
                privacy: event.privacy,
                content: event.content,
            })
            .await?;
        Ok(())
    }
}

impl<E> AmqpMessageProcessor<EntryRemoved> for EmbeddingUpdateHook<E>
where
    E: Embedder + Send + Sync,
{
    const QUEUE: &'static str = "memory_embedding_remove";
}

impl<E> Processor<EntryRemoved> for EmbeddingUpdateHook<E>
where
    E: Embedder + Send + Sync,
{
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "EntryRemoved", err)]
    async fn process(&self, event: EntryRemoved) -> Result<(), Error> {
        self.service
            .database
            .process(DeleteEmbeddingByRef {
                reference: event.reference,
            })
            .await?;
        Ok(())
    }
}

/// Propagates a source entry's privacy change to its embedding row.
#[derive(Debug, Clone)]
pub struct EmbeddingPrivacyHook {
    pub database: DatabaseProcessor,
}

impl AmqpMessageProcessor<EntryPrivacyChanged> for EmbeddingPrivacyHook {
    const QUEUE: &'static str = "memory_embedding_privacy";
}

impl Processor<EntryPrivacyChanged> for EmbeddingPrivacyHook {
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "EntryPrivacyChanged", err)]
    async fn process(&self, event: EntryPrivacyChanged) -> Result<(), Error> {
        self.database
            .process(UpdateEmbeddingPrivacyByRef {
                reference: event.reference,
                privacy: event.privacy,
            })
            .await?;
        Ok(())
    }
}
