-- Register a segment-tree node whose bucket just became full, *without*
-- summarizing it. Summarization is deferred (lazy): the read-path saga refreshes
-- the node on demand. Here we only record that the node exists -- with an empty
-- summary and a zero child watermark, so the read path sees it as stale and
-- knows to refresh it before use. A no-op if the node already exists, so an
-- already-summarized node is never clobbered back to a placeholder.
INSERT INTO memory.conversation_summary_node
    (conversation_id, level, bucket, covers_from_message_id, covers_to_message_id,
     distance_from, distance_to, summary, is_open, last_summary_children, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, '', FALSE, 0, $8)
ON CONFLICT (conversation_id, level, bucket) DO NOTHING
