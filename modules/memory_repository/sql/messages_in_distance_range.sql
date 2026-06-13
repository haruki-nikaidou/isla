-- The raw text of the messages whose cumulative semantic distance falls in
-- [$2, $3) for a conversation. These are the leaves combined to summarize a
-- level-1 segment-tree node. Non-text content blocks are ignored here.
SELECT
    cm.id AS "message_id!",
    COALESCE(string_agg(cc.text, ' ' ORDER BY cc.position), '') AS "text!"
FROM memory.conversation_message cm
JOIN memory.conversation_message_metric m ON m.message_id = cm.id
LEFT JOIN memory.conversation_content cc
       ON cc.message_id = cm.id AND cc.modality = 'Text'
WHERE cm.conversation_id = $1
  AND m.cumulative_distance >= $2
  AND m.cumulative_distance < $3
GROUP BY cm.id
ORDER BY cm.id ASC
