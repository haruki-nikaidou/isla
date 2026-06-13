//! Lifecycle hooks for memory operations.
//!
//! Hooks consume AMQP events and keep derived state in sync. Today this is the
//! summary segment tree: [`SegmentTreeHook`] reacts to
//! [`SummaryNodesDirty`](crate::events::publish::SummaryNodesDirty) by
//! recomputing the finalized nodes.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::Error;
use wakuwaku::amqp::AmqpMessageProcessor;

use crate::events::publish::SummaryNodesDirty;
use crate::services::segment_tree::{RecomputeNode, SegmentTreeService, Summarizer};

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
