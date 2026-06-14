//! AMQP events published by `memory_repository`.
//!
//! These structs carry only *metadata*; any large payload (entry text, message
//! excerpt) is stored separately in the Redis context store and referenced here
//! via a
//! [`ContextRef`](dynamic_message_extension::dynamic_context::ContextRef)
//! field. Producers park each context body in Redis and publish only a second
//! [`ContextRef`](dynamic_message_extension::dynamic_context::ContextRef)
//! pointer for the whole event onto the bus, so an oversized field never
//! rides on AMQP. A consumer recovers the body by wrapping its processor in
//! [`ContextRefWrap`](dynamic_message_extension::dynamic_context::ContextRefWrap).

use dynamic_message_extension::dynamic_context::ContextRef;
use kanau::{JsonMessageDe, JsonMessageSer};
use serde::{Deserialize, Serialize};
use wakuwaku::amqp::{AmqpExchangeType, AmqpMessageSend, AmqpRouting};

use crate::entities::db::PrivacyControlFlag;
use crate::entities::db::embedding::EntryRef;
use crate::entities::redis::{EntryContent, MessageExcerpt};

/// Emitted when a source entry is created or its indexed content changes; the
/// embedding index must (re)embed it. Consumed by `EmbeddingUpdateHook`.
///
/// The actual entry text is stored in Redis under `content`; the event carries
/// only metadata plus a [`ContextRef<EntryContent>`] pointer.
#[derive(Debug, Clone, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct EntryUpserted {
    pub reference: EntryRef,
    pub privacy: PrivacyControlFlag,
    /// Pointer to the [`EntryContent`] body stored in Redis.
    pub content: ContextRef<EntryContent>,
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
///
/// The message text is stored in Redis under `current_excerpt`; the event
/// carries only metadata plus a [`ContextRef<MessageExcerpt>`] pointer.
#[derive(Debug, Clone, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct MessageAppended {
    pub conversation_id: i64,
    pub message_id: i64,
    /// Pointer to the [`MessageExcerpt`] body stored in Redis.
    pub current_excerpt: ContextRef<MessageExcerpt>,
}

impl AmqpRouting for MessageAppended {
    const EXCHANGE: &'static str = "memory.events";
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
    const ROUTING_KEY: &'static str = "conversation.message.appended";
}
impl AmqpMessageSend for MessageAppended {}
