---
name: query-memory-repository
description: Recall past conversations, search semantic memory, fetch conversation history, select context within a token budget, or load applicable personality facets. Use when the agent must remember what was said before, do RAG / semantic lookup over memory, page through a conversation's messages or content blocks, fit the most relevant history into a token budget, or pick which personality facets apply to the current peer.
---

# Query the Memory Repository

The `memory_repository` crate is Isla's long-term memory. Everything is reached through **service-layer `Processor` impls** — call them in-process with `service.process(Dto).await?`. All services return `Result<_, wakuwaku::Error>`. SQL lives only in the entity layer beneath them; never reach past a service into raw queries.

> These services are already wired into the turn pipeline: `personality::services::assembler` exposes `PersonalitySource` (backed by `PersonalityService`) and `HistorySource` (backed by `ConversationService`) seams, so a normal turn does not call them directly. Use this skill when you need memory **outside** the standard assemble path — explicit recall, search, or auditing history.

## Privacy model

Search and facet selection are filtered by the peer's `Relationship`
(`memory::entities::db::contact_identity::Relationship`): `Stranger`, `Master`, `Acquaintance`, `Dude`, `Ignored`. Pass the relationship of the peer the agent is currently talking to; the service drops anything that relationship is not allowed to see.

## EmbeddingService<E> — semantic / RAG search

`E` is the embedder seam (LLM-backed in prod). One read operation:

| DTO | Fields | Output |
|---|---|---|
| `SearchMemory` | `query: String`, `relationship: Relationship`, `limit: i64` | `Vec<EmbeddingHit>` |

`EmbeddingHit` carries `id: i64`, the source refs `calender_event/conversation/contact/diary: Option<..>`, `privacy: PrivacyControlFlag`, `content: String`, and `distance: f64` (cosine distance, smaller = closer). `hit.reference()` returns the `EntryRef` it points at. Results are privacy-filtered by `relationship`.

```rust
use memory_repository::services::embedding::{EmbeddingService, SearchMemory};
use memory_repository::entities::db::contact_identity::Relationship;

let hits = embedding_service
    .process(SearchMemory {
        query: "what did we decide about the trip?".to_string(),
        relationship: Relationship::Master,
        limit: 8,
    })
    .await?;
for hit in &hits {
    tracing::debug!(id = hit.id, distance = hit.distance, "recalled");
}
```

## ConversationService — history & content retrieval

Read operations (all return `wakuwaku::Error` on failure):

| DTO | Fields | Output |
|---|---|---|
| `FindConversationRequest` | `id: i64` | `Option<ConversationEntity>` |
| `ListRecentConversationsRequest` | `limit: i64`, `offset: i64` | `Vec<ConversationEntity>` |
| `FindMessageRequest` | `id: i64` | `Option<ConversationMessageEntity>` |
| `ListMessagesRequest` | `conversation_id: i64`, `only_current_branch: bool`, `limit: i64`, `offset: i64` | `Vec<ConversationMessageEntity>` |
| `ListContentsForMessageRequest` | `message_id: i64` | `Vec<ConversationContentEntity>` |
| `FindContentRequest` | `id: i64` | `Option<ConversationContentEntity>` |

Set `only_current_branch = true` to exclude messages from discarded edit branches. Listings are paginated newest-first; page with `limit`/`offset`.

```rust
use memory_repository::services::conversation::ListMessagesRequest;

let messages = conversation_service
    .process(ListMessagesRequest {
        conversation_id,
        only_current_branch: true,
        limit: 50,
        offset: 0,
    })
    .await?;
```

## SummaryService — budgeted context selection

| DTO | Fields | Output |
|---|---|---|
| `SelectContextRequest` | `conversation_id: i64`, `budget: u32`, `recent_message_costs: Vec<MessageCost>` | `ContextResolution` |

`ContextResolution` is a saga outcome:
- `Ready(ContextPlan)` — every selected summary is fresh; `ContextPlan` holds `raw_message_ids: Vec<i64>` (kept verbatim, newest-first) plus the summary node ids covering the older, compressed history.
- `Pending(Vec<NodeAddr>)` — one or more selected summaries are stale. The caller must recompute those nodes (the LLM summarizer lives in `ai_caller`, not here) and **re-issue** the request until it resolves to `Ready`. History is append-only, so each round makes progress and the loop terminates.

Pass `recent_message_costs` newest-first; the most recent messages are kept raw while they fit `budget`, then the remaining budget is spent on the smallest set of summary nodes covering the dropped span.

```rust
use memory_repository::services::summary::{SelectContextRequest, ContextResolution};

loop {
    match summary_service
        .process(SelectContextRequest { conversation_id, budget: 8000, recent_message_costs: costs.clone() })
        .await?
    {
        ContextResolution::Ready(plan) => break plan,
        ContextResolution::Pending(stale) => recompute_nodes(stale).await?,
    }
};
```

## PersonalityService — applicable facets

| DTO | Fields | Output |
|---|---|---|
| `ListApplicableFacets` | `relationship: Relationship` | `Vec<PersonalityFacetEntity>` |

Returns facets ordered by descending priority, keeping each facet that is part of the invariant character base (`is_core`) or whose privacy permits the peer's relationship.

```rust
use memory_repository::services::personality::{PersonalityService, ListApplicableFacets};

let facets = personality_service
    .process(ListApplicableFacets { relationship })
    .await?;
```

## Definition of done

- [ ] Memory reached only through `service.process(Dto).await?`; no raw SQL.
- [ ] The peer's real `Relationship` is passed to `SearchMemory` / `ListApplicableFacets`.
- [ ] `SelectContextRequest` callers handle both `Ready` and `Pending`, looping on `Pending`.
- [ ] `?`-propagated `wakuwaku::Error`; no `unwrap`/`expect`/`panic`.
