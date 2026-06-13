//! AMQP events published by `memory_repository`.

use kanau::message::{MessageDe, MessageSer};
use serde::{Deserialize, Serialize};
use wakuwaku::amqp::{AmqpExchangeType, AmqpMessageSend, AmqpRouting};

use crate::services::segment_tree::NodeAddr;

/// Emitted when appending a message finalizes one or more summary segment-tree
/// nodes that now need (re)summarizing.
///
/// Consumed by [`SegmentTreeHook`](crate::hooks::SegmentTreeHook), which
/// recomputes each node bottom-up via the summary monoid. `nodes` is ordered
/// children-before-parents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryNodesDirty {
    pub conversation_id: i64,
    pub nodes: Vec<NodeAddr>,
}

impl AmqpRouting for SummaryNodesDirty {
    const EXCHANGE: &'static str = "memory.events";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
    const ROUTING_KEY: &'static str = "conversation.summary.dirty";
}

impl AmqpMessageSend for SummaryNodesDirty {}

impl MessageSer for SummaryNodesDirty {
    type SerError = serde_json::Error;
    fn to_bytes(self) -> Result<Box<[u8]>, Self::SerError> {
        serde_json::to_vec(&self).map(Vec::into_boxed_slice)
    }
}

impl MessageDe for SummaryNodesDirty {
    type DeError = serde_json::Error;
    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeError> {
        serde_json::from_slice(bytes)
    }
}
