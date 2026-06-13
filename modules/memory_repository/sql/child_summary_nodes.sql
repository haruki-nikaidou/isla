-- The children of a segment-tree node: the level-($2 - 1) nodes whose
-- cumulative-distance span falls inside the parent's [$3, $4) range. These are
-- combined (the summary monoid) to produce the parent's summary.
SELECT
    id,
    conversation_id,
    level,
    bucket,
    covers_from_message_id,
    covers_to_message_id,
    distance_from,
    distance_to,
    summary,
    is_open,
    updated_at
FROM memory.conversation_summary_node
WHERE conversation_id = $1
  AND level = $2 - 1
  AND distance_from >= $3
  AND distance_to   <= $4
ORDER BY bucket ASC
