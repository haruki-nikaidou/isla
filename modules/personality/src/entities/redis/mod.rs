//! Redis-backed ephemeral state for the caller.
//!
//! Currently holds the **hot personality**: the assembled system prompt for a
//! given conversation peer. Re-deriving it every turn means re-reading and
//! re-filtering every personality facet; caching the finished
//! [`SystemBlock`]s keyed by audience keeps the common path warm. The cache is
//! a pure optimization — a miss just rebuilds via the assembler.

use kanau::{JsonMessageDe, JsonMessageSer};
use memory_repository::entities::db::contact_identity::Relationship;
use serde::{Deserialize, Serialize};
use wakuwaku::redis::{KeyValue, KeyValueRead, KeyValueWrite, RedisKey};

use ai_caller::model::SystemBlock;

/// The cached, fully-assembled system prompt for one audience.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonMessageSer, JsonMessageDe)]
pub struct CachedPersona {
    /// Ordered system blocks ready to hand to the provider.
    pub system: Vec<SystemBlock>,
}

/// Cache entry: the hot personality for a `(conversation, relationship)` pair.
pub struct PersonaCache {
    /// Redis key, built via [`PersonaCache::key_for`].
    pub key: RedisKey,
    /// Cached system blocks.
    pub value: CachedPersona,
}

impl PersonaCache {
    /// Build the cache key for a conversation peer.
    ///
    /// The relationship is part of the key because the same conversation
    /// surfaces different facets to different audiences.
    #[must_use]
    pub fn key_for(conversation_id: i64, relationship: Relationship) -> RedisKey {
        format!("isla:persona:{conversation_id}:{relationship:?}").into()
    }
}

impl KeyValue for PersonaCache {
    type Key = RedisKey;
    type Value = CachedPersona;

    fn key(&self) -> Self::Key {
        self.key.clone()
    }
    fn value(&self) -> Self::Value {
        self.value.clone()
    }
    fn into_value(self) -> Self::Value {
        self.value
    }
    fn new(key: Self::Key, value: Self::Value) -> Self {
        Self { key, value }
    }
}

impl KeyValueRead for PersonaCache {}
impl KeyValueWrite for PersonaCache {}
