-- The first and last message id whose cumulative semantic distance falls in
-- [$2, $3) for a conversation, or (0, 0) when the range is empty. Used to stamp
-- a newly-full segment-tree node's covered-message markers cheaply, without
-- loading any message text.
SELECT
    COALESCE(MIN(cm.id), 0) AS "from_id!",
    COALESCE(MAX(cm.id), 0) AS "to_id!"
FROM memory.conversation_message cm
JOIN memory.conversation_message_metric m ON m.message_id = cm.id
WHERE cm.conversation_id = $1
  AND m.cumulative_distance >= $2
  AND m.cumulative_distance < $3
