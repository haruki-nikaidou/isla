-- Insert or refresh a segment-tree node addressed by (conversation, level,
-- bucket). On re-summarization the covered range, distances, summary text,
-- open-flag and timestamp are overwritten.
INSERT INTO memory.conversation_summary_node
    (conversation_id, level, bucket, covers_from_message_id, covers_to_message_id,
     distance_from, distance_to, summary, is_open, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (conversation_id, level, bucket) DO UPDATE SET
    covers_from_message_id = EXCLUDED.covers_from_message_id,
    covers_to_message_id   = EXCLUDED.covers_to_message_id,
    distance_from          = EXCLUDED.distance_from,
    distance_to            = EXCLUDED.distance_to,
    summary                = EXCLUDED.summary,
    is_open                = EXCLUDED.is_open,
    updated_at             = EXCLUDED.updated_at
RETURNING id
