-- Conversation summaries as a *segment tree*.
--
-- Summaries form a segment tree whose combine operation is "summarize" (a
-- monoid; the identity element is the empty summary, "no conversation in this
-- segment"). The tree is laid out along *cumulative semantic distance* rather
-- than message count.
--
-- Semantic distance is a 1..25 topical-shift score (are these messages about the
-- same topic / in the same conversation / doing the same task), produced by an
-- LLM judge when a message is appended. Conversation history is read-only; this
-- score is derived metadata kept in a sidecar so the history tables are
-- untouched.

-- Per-message topical-shift score and the running cumulative sum within the
-- conversation. The cumulative sum is the axis the segment tree is bucketed on.
CREATE TABLE memory.conversation_message_metric (
    message_id          bigint PRIMARY KEY
                        REFERENCES memory.conversation_message (id) ON DELETE CASCADE,
    semantic_distance   smallint NOT NULL,
    cumulative_distance bigint   NOT NULL,
    CONSTRAINT conversation_message_metric_distance_range
        CHECK (semantic_distance BETWEEN 1 AND 25)
);

CREATE INDEX conversation_message_metric_cumulative_idx
    ON memory.conversation_message_metric (cumulative_distance);

-- Segment-tree nodes, addressed by (level n, bucket k = floor(cumulative/100^n)).
-- Level 0 is the raw messages (not stored here); nodes start at level 1.
-- `covers_from_message_id` / `covers_to_message_id` are markers, NOT foreign
-- keys, so a summary outlives any pruning of the messages it replaces.
CREATE TABLE memory.conversation_summary_node (
    id                     bigserial PRIMARY KEY,
    conversation_id        bigint NOT NULL REFERENCES memory.conversation (id) ON DELETE CASCADE,
    level                  integer NOT NULL,
    bucket                 bigint  NOT NULL,
    covers_from_message_id bigint NOT NULL,
    covers_to_message_id   bigint NOT NULL,
    distance_from          bigint NOT NULL,
    distance_to            bigint NOT NULL,
    summary                text NOT NULL,
    is_open                boolean NOT NULL DEFAULT TRUE,
    updated_at             bigint NOT NULL,
    CONSTRAINT conversation_summary_node_level_positive CHECK (level >= 1),
    CONSTRAINT conversation_summary_node_addr_unique UNIQUE (conversation_id, level, bucket)
);

CREATE INDEX conversation_summary_node_conv_level_idx
    ON memory.conversation_summary_node (conversation_id, level);
