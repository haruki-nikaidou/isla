//! The conversation summary segment tree.
//!
//! Summaries form a segment tree whose combine operation is "summarize" — a
//! monoid with the empty summary as identity. The tree is bucketed along
//! *cumulative semantic distance*: a message at cumulative distance `S` belongs
//! to bucket `floor(S / base^level)` at each level. A node is **finalized**
//! (summarized once, never to change) when the cumulative distance crosses the
//! upper boundary of its bucket; the still-growing tail is represented by raw
//! recent messages instead.
//!
//! [`finalized_nodes`] is a pure function (the intellectual core) and is unit
//! tested without a database. [`SegmentTreeService`] records per-message metrics
//! and recomputes nodes via the [`SemanticScorer`] and [`Summarizer`] seams,
//! which are LLM-backed in production and mocked in tests — mirroring the
//! `LlmProvider` seam in `ai_caller`.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::conversation_message_metric::{
    CreateConversationMessageMetric, LatestCumulativeDistance,
};
use crate::entities::db::conversation_summary_node::{
    ListChildSummaryNodes, MessagesInDistanceRange, UpsertConversationSummaryNode,
};

/// Branching factor of the tree: a new node is created each time the cumulative
/// semantic distance crosses a multiple of `base^level`.
pub const SEGMENT_BASE: i64 = 100;

/// Address of a segment-tree node within a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeAddr {
    /// Tree level (1 = finest summary level).
    pub level: i32,
    /// Bucket index along the cumulative-distance axis.
    pub bucket: i64,
}

/// The nodes finalized by advancing the cumulative distance from `cum_prev` to
/// `cum_new`.
///
/// A node `(level, k)` is finalized when the cumulative distance leaves its
/// bucket `[k·base^level, (k+1)·base^level)`. Returned bottom-up (ascending
/// level) so a parent is recomputed only after its children, since crossing a
/// parent boundary always coincides with crossing its last child's boundary.
pub fn finalized_nodes(cum_prev: i64, cum_new: i64, base: i64) -> Vec<NodeAddr> {
    let mut out = Vec::new();
    if base < 2 || cum_new <= cum_prev {
        return out;
    }
    let mut level: i32 = 1;
    let mut width: i64 = base;
    while width <= cum_new {
        let bucket_prev = cum_prev / width;
        let bucket_new = cum_new / width;
        let mut k = bucket_prev;
        while k < bucket_new {
            out.push(NodeAddr { level, bucket: k });
            k += 1;
        }
        match width.checked_mul(base) {
            Some(next) => {
                width = next;
                level += 1;
            }
            None => break,
        }
    }
    out
}

/// `base^level`, or an error on overflow (a conversation deep enough to overflow
/// `i64` is not a real scenario).
fn node_width(base: i64, level: i32) -> Result<i64, Error> {
    let exp = u32::try_from(level).map_err(|_| Error::InvalidInput)?;
    base.checked_pow(exp)
        .ok_or_else(|| Error::BusinessPanic(anyhow::anyhow!("segment-tree level {level} overflows")))
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ─── Seams ──────────────────────────────────────────────────────────────────

/// Scores how much a new message shifts the topic/conversation/task, `1..=25`.
pub trait SemanticScorer: Processor<ScoreMessage, Output = i16, Error = Error> {}
impl<T> SemanticScorer for T where T: Processor<ScoreMessage, Output = i16, Error = Error> {}

/// Input to the scorer: the prior context excerpt and the new message excerpt.
#[derive(Debug, Clone)]
pub struct ScoreMessage {
    pub previous: String,
    pub current: String,
}

/// The summary monoid: combine the parts (child summaries, or raw message
/// texts) into a single summary. The empty input is the identity.
pub trait Summarizer: Processor<Summarize, Output = String, Error = Error> {}
impl<T> Summarizer for T where T: Processor<Summarize, Output = String, Error = Error> {}

/// Input to the summarizer.
#[derive(Debug, Clone)]
pub struct Summarize {
    /// Level of the node being produced (1 combines raw messages).
    pub level: i32,
    /// The pieces to combine, in order.
    pub parts: Vec<String>,
}

// ─── Service ──────────────────────────────────────────────────────────────────

/// Maintains a conversation's summary segment tree.
#[derive(Debug, Clone)]
pub struct SegmentTreeService<Sc, Su> {
    pub database: DatabaseProcessor,
    pub scorer: Sc,
    pub summarizer: Su,
}

/// Score and record a newly appended message, returning the segment-tree nodes
/// that this append finalized (and therefore need (re)summarizing).
#[derive(Debug, Clone)]
pub struct RecordMessage {
    pub conversation_id: i64,
    pub message_id: i64,
    /// Excerpt of the prior context, for scoring topical shift.
    pub previous_excerpt: String,
    /// Excerpt of the new message, for scoring topical shift.
    pub current_excerpt: String,
}

impl<Sc, Su> Processor<RecordMessage> for SegmentTreeService<Sc, Su>
where
    Sc: SemanticScorer + Sync,
    Su: Sync,
{
    type Output = Vec<NodeAddr>;
    type Error = Error;

    #[instrument(skip_all, name = "RecordMessage", err,
        fields(conversation_id = input.conversation_id, message_id = input.message_id))]
    async fn process(&self, input: RecordMessage) -> Result<Vec<NodeAddr>, Error> {
        let score = self
            .scorer
            .process(ScoreMessage {
                previous: input.previous_excerpt,
                current: input.current_excerpt,
            })
            .await?
            .clamp(1, 25);

        let cum_prev = self
            .database
            .process(LatestCumulativeDistance {
                conversation_id: input.conversation_id,
            })
            .await?;
        let cum_new = cum_prev + i64::from(score);

        self.database
            .process(CreateConversationMessageMetric {
                message_id: input.message_id,
                semantic_distance: score,
                cumulative_distance: cum_new,
            })
            .await?;

        Ok(finalized_nodes(cum_prev, cum_new, SEGMENT_BASE))
    }
}

/// Recompute one segment-tree node by combining its children (the summary
/// monoid) and upserting it as finalized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecomputeNode {
    pub conversation_id: i64,
    pub node: NodeAddr,
}

impl<Sc, Su> Processor<RecomputeNode> for SegmentTreeService<Sc, Su>
where
    Sc: Sync,
    Su: Summarizer + Sync,
{
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "RecomputeNode", err,
        fields(conversation_id = input.conversation_id, level = input.node.level, bucket = input.node.bucket))]
    async fn process(&self, input: RecomputeNode) -> Result<i64, Error> {
        let NodeAddr { level, bucket } = input.node;
        let width = node_width(SEGMENT_BASE, level)?;
        let distance_from = bucket.saturating_mul(width);
        let distance_to = distance_from.saturating_add(width);

        let (parts, covers_from, covers_to) = if level <= 1 {
            let messages = self
                .database
                .process(MessagesInDistanceRange {
                    conversation_id: input.conversation_id,
                    distance_from,
                    distance_to,
                })
                .await?;
            let covers_from = messages.first().map(|m| m.message_id).unwrap_or_default();
            let covers_to = messages.last().map(|m| m.message_id).unwrap_or_default();
            let parts = messages.into_iter().map(|m| m.text).collect();
            (parts, covers_from, covers_to)
        } else {
            let children = self
                .database
                .process(ListChildSummaryNodes {
                    conversation_id: input.conversation_id,
                    level,
                    distance_from,
                    distance_to,
                })
                .await?;
            let covers_from = children
                .iter()
                .map(|c| c.covers_from_message_id)
                .min()
                .unwrap_or_default();
            let covers_to = children
                .iter()
                .map(|c| c.covers_to_message_id)
                .max()
                .unwrap_or_default();
            let parts = children.into_iter().map(|c| c.summary).collect();
            (parts, covers_from, covers_to)
        };

        let summary = self.summarizer.process(Summarize { level, parts }).await?;

        let id = self
            .database
            .process(UpsertConversationSummaryNode {
                conversation_id: input.conversation_id,
                level,
                bucket,
                covers_from_message_id: covers_from,
                covers_to_message_id: covers_to,
                distance_from,
                distance_to,
                summary,
                is_open: false,
                updated_at: now_unix(),
            })
            .await?;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(level: i32, bucket: i64) -> NodeAddr {
        NodeAddr { level, bucket }
    }

    #[test]
    fn no_nodes_finalized_below_the_first_boundary() {
        assert!(finalized_nodes(0, 50, 100).is_empty());
        assert!(finalized_nodes(50, 70, 100).is_empty());
    }

    #[test]
    fn crossing_a_level_1_boundary_finalizes_one_node() {
        assert_eq!(finalized_nodes(99, 100, 100), vec![addr(1, 0)]);
        assert_eq!(finalized_nodes(180, 205, 100), vec![addr(1, 1)]);
    }

    #[test]
    fn no_boundary_crossed_within_a_bucket() {
        assert!(finalized_nodes(150, 170, 100).is_empty());
    }

    #[test]
    fn crossing_a_higher_boundary_cascades_bottom_up() {
        // Crossing 10_000 finalizes level-1 bucket 99 and level-2 bucket 0,
        // emitted bottom-up.
        assert_eq!(
            finalized_nodes(9_999, 10_000, 100),
            vec![addr(1, 99), addr(2, 0)]
        );
    }

    #[test]
    fn handles_multiple_buckets_in_one_jump() {
        // A jump wider than one bucket finalizes every fully-passed bucket.
        assert_eq!(
            finalized_nodes(90, 320, 100),
            vec![addr(1, 0), addr(1, 1), addr(1, 2)]
        );
    }

    #[test]
    fn node_width_is_base_to_the_level() {
        assert_eq!(node_width(100, 1).unwrap(), 100);
        assert_eq!(node_width(100, 2).unwrap(), 10_000);
        assert!(node_width(100, 100).is_err());
    }
}
