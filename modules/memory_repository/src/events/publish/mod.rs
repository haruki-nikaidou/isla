//! AMQP events published by `memory_repository`.
//!
//! These structs are the event *bodies*; they are not sent inline. Producers
//! park each body in the Redis context store and publish only a
//! [`ContextRef`](dynamic_message_extension::dynamic_context::ContextRef)
//! pointer onto the bus, so an oversized field (a summary, an excerpt) never
//! rides on AMQP. A consumer recovers the body by wrapping its processor in
//! [`ContextRefWrap`](dynamic_message_extension::dynamic_context::ContextRefWrap).

use kanau::{JsonMessageDe, JsonMessageSer};
use serde::{Deserialize, Serialize};
use wakuwaku::amqp::{AmqpExchangeType, AmqpMessageSend, AmqpRouting};

use crate::entities::db::PrivacyControlFlag;
use crate::entities::db::embedding::EntryRef;

/// Emitted when a source entry is created or its indexed content changes; the
/// embedding index must (re)embed it. Consumed by `EmbeddingUpdateHook`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct EntryUpserted {
    pub reference: EntryRef,
    pub privacy: PrivacyControlFlag,
    pub content: String,
}

impl AmqpRouting for EntryUpserted {
    const EXCHANGE: &'static str = "memory.events";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
    const ROUTING_KEY: &'static str = "entry.upserted";
}
impl AmqpMessageSend for EntryUpserted {}

/// Emitted when a source entry's privacy changes; the index inherits it.
/// Consumed by `EmbeddingPrivacyHook`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct EntryPrivacyChanged {
    pub reference: EntryRef,
    pub privacy: PrivacyControlFlag,
}

impl AmqpRouting for EntryPrivacyChanged {
    const EXCHANGE: &'static str = "memory.events";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
    const ROUTING_KEY: &'static str = "entry.privacy_changed";
}
impl AmqpMessageSend for EntryPrivacyChanged {}

/// Emitted when a source entry is deleted; its embedding must be removed.
/// Consumed by `EmbeddingUpdateHook`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct EntryRemoved {
    pub reference: EntryRef,
}

impl AmqpRouting for EntryRemoved {
    const EXCHANGE: &'static str = "memory.events";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
    const ROUTING_KEY: &'static str = "entry.removed";
}
impl AmqpMessageSend for EntryRemoved {}

/// Emitted when a message is appended to a conversation. Consumed by
/// `MessageMetricHook`, which scores the topical shift, records the metric, and
/// registers placeholders for any nodes the append filled. Keeps the LLM scorer
/// out of the synchronous write path. Node summaries are produced lazily by the
/// read-path saga, so no resummarization is triggered here.
#[derive(Debug, Clone, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct MessageAppended {
    pub conversation_id: i64,
    pub message_id: i64,
    /// Text of the appended message, for scoring.
    pub current_excerpt: String,
}

impl AmqpRouting for MessageAppended {
    const EXCHANGE: &'static str = "memory.events";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
    const ROUTING_KEY: &'static str = "conversation.message.appended";
}
impl AmqpMessageSend for MessageAppended {}
