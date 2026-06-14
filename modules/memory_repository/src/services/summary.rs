//! Context selection over the summary tree.
//!
//! Given a token budget, keep the most recent raw messages and replace the
//! older history with the smallest, most-compressed set of summary nodes that
//! still covers it. [`select_context`] is a pure function so the budgeting rules
//! can be unit-tested without a database; [`SummaryService`] loads the tree and
//! applies it.
//!
//! Summaries are maintained lazily: a node is (re)summarized only when its
//! bucket fills or when it is requested here. Because of that, a node selected
//! for the context may be **stale** — appended messages have grown its children
//! past the `last_summary_children` watermark recorded in its summary, or it is
//! still an empty placeholder. The summarizer is LLM-backed and lives in the API
//! caller (`ai_caller`), not in this read path, so [`SummaryService`] cannot
//! refresh a stale node itself. Instead it runs a **saga**: when a selected node
//! is stale it returns [`ContextResolution::Pending`] naming the nodes the caller
//! must recompute (via `RecomputeNode`); the caller refreshes them and retries
//! the request until it resolves to [`ContextResolution::Ready`]. Each round
//! makes at least one node fresh and freshness is permanent (history is
//! append-only), so the saga terminates.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::conversation_summary_node::{
    CountSummaryNodeChildren, ListSummaryNodesByConversation,
};
use crate::services::segment_tree::NodeAddr;

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

/// Outcome of a [`SelectContextRequest`].
///
/// The read path cannot summarize stale nodes itself (the LLM summarizer lives
/// in the API caller), so resolution may take more than one round — see the
/// module docs for the saga.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextResolution {
    /// Every selected summary is up to date; here is the context plan to load.
    Ready(ContextPlan),
    /// One or more selected summaries are stale. The caller must recompute these
    /// nodes (via `RecomputeNode`) and re-issue the request before a plan can be
    /// returned.
    Pending(Vec<NodeAddr>),
}

/// A summary node the context plan selected, paired with the two child counts
/// that decide its freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedNode {
    /// Address used to recompute the node if it turns out stale.
    pub node: NodeAddr,
    /// Children folded into the node's stored summary (`0` for a placeholder).
    pub last_summary_children: i64,
    /// Children currently underneath the node.
    pub current_children: i64,
}

/// Decide a [`ContextResolution`] from the chosen plan and the freshness of each
/// selected summary node.
///
/// A node is **stale** when its current child count differs from the watermark
/// its summary was built over — which, since history is append-only, means more
/// children have arrived since (a placeholder starts at watermark `0`). If any
/// selected node is stale the plan is withheld and the stale nodes are returned,
/// in selection order, for the caller to recompute; otherwise the plan is ready.
fn resolve_context(plan: ContextPlan, selected: &[SelectedNode]) -> ContextResolution {
    let pending: Vec<NodeAddr> = selected
        .iter()
        .filter(|s| s.current_children != s.last_summary_children)
        .map(|s| s.node)
        .collect();
    if pending.is_empty() {
        ContextResolution::Ready(plan)
    } else {
        ContextResolution::Pending(pending)
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
    type Output = ContextResolution;
    type Error = Error;

    #[instrument(skip_all, name = "SelectContextRequest", err,
        fields(conversation_id = input.conversation_id, budget = input.budget))]
    async fn process(&self, input: SelectContextRequest) -> Result<ContextResolution, Error> {
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
        let plan = select_context(input.budget, &input.recent_message_costs, &candidates);

        // Count the live children of only the nodes we actually picked, so the
        // staleness decision stays cheap. The pure `resolve_context` then turns
        // those counts into a ready plan or the saga's recompute list.
        let mut selected = Vec::new();
        for id in &plan.summary_ids {
            let Some(node) = nodes.iter().find(|n| n.id == *id) else {
                continue;
            };
            let current_children = self
                .database
                .process(CountSummaryNodeChildren {
                    conversation_id: input.conversation_id,
                    level: node.level,
                    distance_from: node.distance_from,
                    distance_to: node.distance_to,
                })
                .await?;
            selected.push(SelectedNode {
                node: NodeAddr {
                    level: node.level,
                    bucket: node.bucket,
                },
                last_summary_children: node.last_summary_children,
                current_children,
            });
        }

        Ok(resolve_context(plan, &selected))
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

    fn addr(level: i32, bucket: i64) -> NodeAddr {
        NodeAddr { level, bucket }
    }

    fn sel(level: i32, bucket: i64, last: i64, current: i64) -> SelectedNode {
        SelectedNode {
            node: addr(level, bucket),
            last_summary_children: last,
            current_children: current,
        }
    }

    #[test]
    fn ready_when_no_summaries_were_selected() {
        let plan = ContextPlan {
            raw_message_ids: vec![10, 9],
            summary_ids: vec![],
        };
        assert_eq!(
            resolve_context(plan.clone(), &[]),
            ContextResolution::Ready(plan)
        );
    }

    #[test]
    fn ready_when_every_selected_node_is_fresh() {
        // current child count matches the watermark the summary was built over.
        let selected = [sel(1, 0, 100, 100), sel(2, 0, 100, 100)];
        let plan = ContextPlan {
            raw_message_ids: vec![10],
            summary_ids: vec![1, 2],
        };
        assert_eq!(
            resolve_context(plan.clone(), &selected),
            ContextResolution::Ready(plan)
        );
    }

    #[test]
    fn pending_lists_a_placeholders_address() {
        // A placeholder has watermark 0 but real children -> must be summarized.
        let selected = [sel(1, 3, 0, 17)];
        let plan = ContextPlan {
            raw_message_ids: vec![],
            summary_ids: vec![1],
        };
        assert_eq!(
            resolve_context(plan, &selected),
            ContextResolution::Pending(vec![addr(1, 3)])
        );
    }

    #[test]
    fn pending_holds_only_the_stale_nodes_in_selection_order() {
        // Fresh, stale (grew), fresh, stale (placeholder) — keep just the stale
        // two, in the order they were selected.
        let selected = [
            sel(2, 0, 100, 100),
            sel(1, 5, 80, 95),
            sel(1, 4, 100, 100),
            sel(1, 6, 0, 12),
        ];
        let plan = ContextPlan {
            raw_message_ids: vec![20],
            summary_ids: vec![10, 11, 12, 13],
        };
        assert_eq!(
            resolve_context(plan, &selected),
            ContextResolution::Pending(vec![addr(1, 5), addr(1, 6)])
        );
    }
}
