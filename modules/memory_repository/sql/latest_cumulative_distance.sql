-- The largest cumulative semantic distance recorded for a conversation, or 0
-- when it has no scored messages yet. Used to compute the next message's
-- cumulative position on the segment-tree axis.
SELECT COALESCE(MAX(m.cumulative_distance), 0) AS "cumulative!"
FROM memory.conversation_message_metric m
JOIN memory.conversation_message cm ON cm.id = m.message_id
WHERE cm.conversation_id = $1
