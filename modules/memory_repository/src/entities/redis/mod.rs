//! Redis-backed entity definitions for cached and ephemeral state.
//!
//! Reserved for entities that benefit from Redis's speed or pub/sub
//! capabilities, such as active conversation state or real-time presence.
//!
//! Context bodies stored here are published via
//! [`ContextRef`](dynamic_message_extension::dynamic_context::ContextRef):
//! the large payload is written to Redis under a fresh UUID, and only that
//! pointer travels on the AMQP bus inside the event struct.

use kanau::{JsonMessageDe, JsonMessageSer};
use serde::{Deserialize, Serialize};

use crate::entities::db::PrivacyControlFlag;
use crate::entities::db::embedding::EntryRef;

/// Body of an [`EntryUpserted`](crate::events::publish::EntryUpserted) context:
/// the full text that the embedding index should (re)embed.
///
/// Stored in Redis by the publisher; resolved on the consumer side via
/// [`ContextRef<EntryContent>`](dynamic_message_extension::dynamic_context::ContextRef).
#[derive(Debug, Clone, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct EntryContent {
    pub reference: EntryRef,
    pub privacy: PrivacyControlFlag,
    /// Full text of the source entry to be embedded.
    pub content: String,
}

/// Body of a [`MessageAppended`](crate::events::publish::MessageAppended) context:
/// the text excerpt used by the segment-tree scorer.
///
/// Stored in Redis by the publisher; resolved on the consumer side via
/// [`ContextRef<MessageExcerpt>`](dynamic_message_extension::dynamic_context::ContextRef).
#[derive(Debug, Clone, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct MessageExcerpt {
    /// Text of the appended message, for scoring.
    pub current_excerpt: String,
}
