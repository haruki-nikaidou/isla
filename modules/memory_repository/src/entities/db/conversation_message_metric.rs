//! Per-message topical-shift score and its running cumulative sum.
//!
//! This is derived metadata that sits beside the read-only conversation history.
//! `semantic_distance` is a 1..25 score (same topic / same conversation / same
//! task), and `cumulative_distance` is the running sum — the axis the summary
//! segment tree is bucketed on.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::sqlx::DatabaseProcessor;

/// The topical-shift metric attached to one conversation message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversationMessageMetricEntity {
    /// Message this metric describes.
    pub message_id: i64,
    /// Topical-shift score in `1..=25`.
    pub semantic_distance: i16,
    /// Running sum of `semantic_distance` within the conversation, up to and
    /// including this message.
    pub cumulative_distance: i64,
}

/// Record the metric for a freshly appended message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateConversationMessageMetric {
    pub message_id: i64,
    pub semantic_distance: i16,
    pub cumulative_distance: i64,
}

impl Processor<CreateConversationMessageMetric> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateConversationMessageMetric", err,
        fields(message_id = input.message_id))]
    async fn process(&self, input: CreateConversationMessageMetric) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO memory.conversation_message_metric
                (message_id, semantic_distance, cumulative_distance)
            VALUES ($1, $2, $3)
            "#,
            input.message_id,
            input.semantic_distance,
            input.cumulative_distance,
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}

/// The largest cumulative distance recorded for a conversation, or `0` when it
/// has no scored messages yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatestCumulativeDistance {
    pub conversation_id: i64,
}

impl Processor<LatestCumulativeDistance> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:LatestCumulativeDistance", err,
        fields(conversation_id = input.conversation_id))]
    async fn process(&self, input: LatestCumulativeDistance) -> Result<i64, sqlx::Error> {
        sqlx::query_file_scalar!("sql/latest_cumulative_distance.sql", input.conversation_id,)
            .fetch_one(self.db())
            .await
    }
}
