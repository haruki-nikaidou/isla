//! Segment-tree nodes for hierarchical conversation summaries.
//!
//! Each node is addressed by `(conversation_id, level, bucket)` where
//! `bucket = floor(cumulative_distance / base^level)`. A node's summary is the
//! monoid combination of its children: level-1 nodes combine raw messages in
//! their distance range; higher levels combine the child nodes one level down.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::sqlx::DatabaseProcessor;

/// One node of a conversation's summary segment tree.
#[derive(Debug, Clone)]
pub struct ConversationSummaryNodeEntity {
    pub id: i64,
    pub conversation_id: i64,
    /// Tree level; 1 is the finest summary level (0 = raw messages).
    pub level: i32,
    /// Bucket index along the cumulative-distance axis.
    pub bucket: i64,
    /// First message id covered (marker, not a foreign key).
    pub covers_from_message_id: i64,
    /// Last message id covered (marker, not a foreign key).
    pub covers_to_message_id: i64,
    /// Inclusive lower bound of the covered cumulative-distance span.
    pub distance_from: i64,
    /// Exclusive upper bound of the covered cumulative-distance span.
    pub distance_to: i64,
    /// The summary text (monoid value; empty string is the identity).
    pub summary: String,
    /// Whether the node's range is still growing (the rightmost node at its
    /// level). Finalized nodes never change again.
    pub is_open: bool,
    /// Number of children (raw messages for level 1, child nodes above) folded
    /// into the current `summary`. The node is up to date iff this equals its
    /// current child count; a larger current count means the summary is stale.
    /// `0` marks a placeholder whose summary has not been computed yet.
    pub last_summary_children: i64,
    /// Unix timestamp of the last (re)summarization.
    pub updated_at: i64,
}

/// Insert or refresh a node addressed by `(conversation_id, level, bucket)`.
#[derive(Debug, Clone)]
pub struct UpsertConversationSummaryNode {
    pub conversation_id: i64,
    pub level: i32,
    pub bucket: i64,
    pub covers_from_message_id: i64,
    pub covers_to_message_id: i64,
    pub distance_from: i64,
    pub distance_to: i64,
    pub summary: String,
    pub is_open: bool,
    pub last_summary_children: i64,
    pub updated_at: i64,
}

impl Processor<UpsertConversationSummaryNode> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpsertConversationSummaryNode", err,
        fields(conversation_id = input.conversation_id, level = input.level, bucket = input.bucket))]
    async fn process(&self, input: UpsertConversationSummaryNode) -> Result<i64, sqlx::Error> {
        sqlx::query_file_scalar!(
            "sql/upsert_summary_node.sql",
            input.conversation_id,
            input.level,
            input.bucket,
            input.covers_from_message_id,
            input.covers_to_message_id,
            input.distance_from,
            input.distance_to,
            input.summary,
            input.is_open,
            input.last_summary_children,
            input.updated_at,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Find a node by its `(conversation_id, level, bucket)` address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindSummaryNodeByAddr {
    pub conversation_id: i64,
    pub level: i32,
    pub bucket: i64,
}

impl Processor<FindSummaryNodeByAddr> for DatabaseProcessor {
    type Output = Option<ConversationSummaryNodeEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindSummaryNodeByAddr", err,
        fields(conversation_id = input.conversation_id, level = input.level, bucket = input.bucket))]
    async fn process(
        &self,
        input: FindSummaryNodeByAddr,
    ) -> Result<Option<ConversationSummaryNodeEntity>, sqlx::Error> {
        sqlx::query_as!(
            ConversationSummaryNodeEntity,
            r#"
            SELECT id, conversation_id, level, bucket, covers_from_message_id,
                   covers_to_message_id, distance_from, distance_to, summary,
                   is_open, last_summary_children, updated_at
            FROM memory.conversation_summary_node
            WHERE conversation_id = $1 AND level = $2 AND bucket = $3
            LIMIT 1
            "#,
            input.conversation_id,
            input.level,
            input.bucket,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// All nodes of a conversation, coarsest level first then oldest span first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListSummaryNodesByConversation {
    pub conversation_id: i64,
}

impl Processor<ListSummaryNodesByConversation> for DatabaseProcessor {
    type Output = Vec<ConversationSummaryNodeEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListSummaryNodesByConversation", err,
        fields(conversation_id = input.conversation_id))]
    async fn process(
        &self,
        input: ListSummaryNodesByConversation,
    ) -> Result<Vec<ConversationSummaryNodeEntity>, sqlx::Error> {
        sqlx::query_as!(
            ConversationSummaryNodeEntity,
            r#"
            SELECT id, conversation_id, level, bucket, covers_from_message_id,
                   covers_to_message_id, distance_from, distance_to, summary,
                   is_open, last_summary_children, updated_at
            FROM memory.conversation_summary_node
            WHERE conversation_id = $1
            ORDER BY level DESC, bucket ASC
            "#,
            input.conversation_id,
        )
        .fetch_all(self.db())
        .await
    }
}

/// Fetch the children of a node: the level-(`level` − 1) nodes within its
/// cumulative-distance span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListChildSummaryNodes {
    pub conversation_id: i64,
    pub level: i32,
    pub distance_from: i64,
    pub distance_to: i64,
}

impl Processor<ListChildSummaryNodes> for DatabaseProcessor {
    type Output = Vec<ConversationSummaryNodeEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListChildSummaryNodes", err,
        fields(conversation_id = input.conversation_id, level = input.level))]
    async fn process(
        &self,
        input: ListChildSummaryNodes,
    ) -> Result<Vec<ConversationSummaryNodeEntity>, sqlx::Error> {
        sqlx::query_file_as!(
            ConversationSummaryNodeEntity,
            "sql/child_summary_nodes.sql",
            input.conversation_id,
            input.level,
            input.distance_from,
            input.distance_to,
        )
        .fetch_all(self.db())
        .await
    }
}

/// A message's concatenated text, used as a leaf when summarizing a level-1
/// node.
#[derive(Debug, Clone)]
pub struct MessageText {
    pub message_id: i64,
    pub text: String,
}

/// Fetch the raw message texts whose cumulative distance is in `[from, to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessagesInDistanceRange {
    pub conversation_id: i64,
    pub distance_from: i64,
    pub distance_to: i64,
}

impl Processor<MessagesInDistanceRange> for DatabaseProcessor {
    type Output = Vec<MessageText>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:MessagesInDistanceRange", err,
        fields(conversation_id = input.conversation_id))]
    async fn process(
        &self,
        input: MessagesInDistanceRange,
    ) -> Result<Vec<MessageText>, sqlx::Error> {
        sqlx::query_file_as!(
            MessageText,
            "sql/messages_in_distance_range.sql",
            input.conversation_id,
            input.distance_from,
            input.distance_to,
        )
        .fetch_all(self.db())
        .await
    }
}

/// Register a node whose bucket just became full, without summarizing it.
///
/// Inserts an empty placeholder (zero child watermark) so the read-path saga can
/// discover the node and refresh it lazily. A no-op if the node already exists,
/// so an already-summarized node is never reset to a placeholder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertSummaryNodePlaceholder {
    pub conversation_id: i64,
    pub level: i32,
    pub bucket: i64,
    pub covers_from_message_id: i64,
    pub covers_to_message_id: i64,
    pub distance_from: i64,
    pub distance_to: i64,
    pub updated_at: i64,
}

impl Processor<InsertSummaryNodePlaceholder> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:InsertSummaryNodePlaceholder", err,
        fields(conversation_id = input.conversation_id, level = input.level, bucket = input.bucket))]
    async fn process(&self, input: InsertSummaryNodePlaceholder) -> Result<(), sqlx::Error> {
        sqlx::query_file!(
            "sql/insert_summary_placeholder.sql",
            input.conversation_id,
            input.level,
            input.bucket,
            input.covers_from_message_id,
            input.covers_to_message_id,
            input.distance_from,
            input.distance_to,
            input.updated_at,
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}

/// The first and last message id covered by a cumulative-distance span (`0, 0`
/// when empty). Used to stamp a newly-full node's covered-message markers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageIdBounds {
    pub from_id: i64,
    pub to_id: i64,
}

/// Fetch the covered-message marker bounds for a cumulative-distance span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageIdBoundsInDistanceRange {
    pub conversation_id: i64,
    pub distance_from: i64,
    pub distance_to: i64,
}

impl Processor<MessageIdBoundsInDistanceRange> for DatabaseProcessor {
    type Output = MessageIdBounds;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:MessageIdBoundsInDistanceRange", err,
        fields(conversation_id = input.conversation_id))]
    async fn process(
        &self,
        input: MessageIdBoundsInDistanceRange,
    ) -> Result<MessageIdBounds, sqlx::Error> {
        sqlx::query_file_as!(
            MessageIdBounds,
            "sql/message_id_bounds_in_distance_range.sql",
            input.conversation_id,
            input.distance_from,
            input.distance_to,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Count the children currently underneath a node, to compare against its
/// `last_summary_children` watermark. Level-1 nodes count raw messages in their
/// distance span; higher levels count their child nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CountSummaryNodeChildren {
    pub conversation_id: i64,
    pub level: i32,
    pub distance_from: i64,
    pub distance_to: i64,
}

impl Processor<CountSummaryNodeChildren> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CountSummaryNodeChildren", err,
        fields(conversation_id = input.conversation_id, level = input.level))]
    async fn process(&self, input: CountSummaryNodeChildren) -> Result<i64, sqlx::Error> {
        if input.level <= 1 {
            sqlx::query_file_scalar!(
                "sql/count_messages_in_distance_range.sql",
                input.conversation_id,
                input.distance_from,
                input.distance_to,
            )
            .fetch_one(self.db())
            .await
        } else {
            sqlx::query_file_scalar!(
                "sql/count_child_summary_nodes.sql",
                input.conversation_id,
                input.level,
                input.distance_from,
                input.distance_to,
            )
            .fetch_one(self.db())
            .await
        }
    }
}
