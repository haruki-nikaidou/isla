//! Context selection over the summary tree.
//!
//! Given a token budget, keep the most recent raw messages and replace the
//! older history with the smallest, most-compressed set of summary nodes that
//! still covers it. [`select_context`] is a pure function so the budgeting rules
//! can be unit-tested without a database; [`SummaryService`] loads the tree and
//! applies it.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::conversation_summary_node::ListSummaryNodesByConversation;

/// Rough token estimate for a piece of text (≈ 4 characters per token).
fn estimate_tokens(text: &str) -> u32 {
    (text.len() / 4 + 1) as u32
}

/// The token cost of one raw message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageCost {
    pub id: i64,
    pub tokens: u32,
}

/// A summary node competing for the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SummaryCandidate {
    pub id: i64,
    pub level: i32,
    pub from_id: i64,
    pub to_id: i64,
    pub tokens: u32,
}

/// What to load into context for a turn.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContextPlan {
    /// Raw message ids to include, newest-first.
    pub raw_message_ids: Vec<i64>,
    /// Summary node ids to include for the older, compressed history.
    pub summary_ids: Vec<i64>,
}

/// Two inclusive id ranges overlap.
fn overlaps(a: (i64, i64), b: (i64, i64)) -> bool {
    a.0 <= b.1 && b.0 <= a.1
}

/// Select the context that fits `budget`.
///
/// `recent_newest_first` must be ordered newest message first. The most recent
/// messages are kept raw while they fit; the remaining budget is then spent on
/// summary nodes covering only the older (dropped) region, preferring
/// higher-level (more compressed) nodes and never double-covering a span.
pub fn select_context(
    budget: u32,
    recent_newest_first: &[MessageCost],
    summaries: &[SummaryCandidate],
) -> ContextPlan {
    let mut used: u32 = 0;
    let mut raw_message_ids = Vec::new();
    for msg in recent_newest_first {
        match used.checked_add(msg.tokens) {
            Some(next) if next <= budget => {
                used = next;
                raw_message_ids.push(msg.id);
            }
            _ => break,
        }
    }

    // Everything older than the oldest kept raw message is eligible for
    // summary coverage. With no raw message kept, the whole history is eligible.
    let cutoff = raw_message_ids.last().copied().unwrap_or(i64::MAX);

    // Prefer more-compressed nodes first, then oldest span first for stability.
    let mut ordered: Vec<&SummaryCandidate> = summaries
        .iter()
        .filter(|s| s.to_id < cutoff)
        .collect();
    ordered.sort_by(|a, b| b.level.cmp(&a.level).then(a.from_id.cmp(&b.from_id)));

    let mut summary_ids = Vec::new();
    let mut covered: Vec<(i64, i64)> = Vec::new();
    for cand in ordered {
        let range = (cand.from_id, cand.to_id);
        if covered.iter().any(|c| overlaps(*c, range)) {
            continue;
        }
        match used.checked_add(cand.tokens) {
            Some(next) if next <= budget => {
                used = next;
                summary_ids.push(cand.id);
                covered.push(range);
            }
            _ => continue,
        }
    }

    ContextPlan {
        raw_message_ids,
        summary_ids,
    }
}

#[derive(Debug, Clone)]
pub struct SummaryService {
    pub database: DatabaseProcessor,
}

/// Select the context plan for a conversation given a token budget and the cost
/// of its most recent messages (newest-first).
#[derive(Debug, Clone)]
pub struct SelectContextRequest {
    pub conversation_id: i64,
    pub budget: u32,
    pub recent_message_costs: Vec<MessageCost>,
}

impl Processor<SelectContextRequest> for SummaryService {
    type Output = ContextPlan;
    type Error = Error;

    #[instrument(skip_all, name = "SelectContextRequest", err,
        fields(conversation_id = input.conversation_id, budget = input.budget))]
    async fn process(&self, input: SelectContextRequest) -> Result<ContextPlan, Error> {
        let nodes = self
            .database
            .process(ListSummaryNodesByConversation {
                conversation_id: input.conversation_id,
            })
            .await?;
        let candidates: Vec<SummaryCandidate> = nodes
            .iter()
            .map(|n| SummaryCandidate {
                id: n.id,
                level: n.level,
                from_id: n.covers_from_message_id,
                to_id: n.covers_to_message_id,
                tokens: estimate_tokens(&n.summary),
            })
            .collect();
        Ok(select_context(
            input.budget,
            &input.recent_message_costs,
            &candidates,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: i64, tokens: u32) -> MessageCost {
        MessageCost { id, tokens }
    }

    #[test]
    fn keeps_recent_raw_messages_within_budget() {
        // ids 10 (newest) .. 7 (oldest), 30 tokens each, budget 100 -> 3 fit.
        let recent = [msg(10, 30), msg(9, 30), msg(8, 30), msg(7, 30)];
        let plan = select_context(100, &recent, &[]);
        assert_eq!(plan.raw_message_ids, vec![10, 9, 8]);
        assert!(plan.summary_ids.is_empty());
    }

    #[test]
    fn replaces_dropped_older_messages_with_a_summary() {
        let recent = [msg(10, 60), msg(9, 60), msg(8, 60)];
        // Only the newest raw fits (60). A summary of the older span (ids < 10)
        // costs 20 and should be added.
        let summaries = [SummaryCandidate {
            id: 1,
            level: 0,
            from_id: 1,
            to_id: 9,
            tokens: 20,
        }];
        let plan = select_context(100, &recent, &summaries);
        assert_eq!(plan.raw_message_ids, vec![10]);
        assert_eq!(plan.summary_ids, vec![1]);
    }

    #[test]
    fn prefers_higher_level_summary_over_overlapping_lower_level() {
        let recent = [msg(100, 50)];
        let summaries = [
            SummaryCandidate {
                id: 1,
                level: 0,
                from_id: 1,
                to_id: 20,
                tokens: 30,
            },
            SummaryCandidate {
                id: 2,
                level: 1,
                from_id: 1,
                to_id: 50,
                tokens: 30,
            },
        ];
        let plan = select_context(100, &recent, &summaries);
        // The level-1 node covers the same (and more) span; the level-0 node
        // overlaps it and must be skipped.
        assert_eq!(plan.summary_ids, vec![2]);
    }

    #[test]
    fn never_exceeds_budget() {
        let recent = [msg(10, 40), msg(9, 40)];
        let summaries = [
            SummaryCandidate {
                id: 1,
                level: 1,
                from_id: 1,
                to_id: 8,
                tokens: 40,
            },
            SummaryCandidate {
                id: 2,
                level: 0,
                from_id: 1,
                to_id: 4,
                tokens: 40,
            },
        ];
        let plan = select_context(100, &recent, &summaries);
        // 40 + 40 raw = 80, only 20 left -> no 40-token summary fits.
        assert_eq!(plan.raw_message_ids, vec![10, 9]);
        assert!(plan.summary_ids.is_empty());
    }
}
