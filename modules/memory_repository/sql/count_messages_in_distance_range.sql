-- How many raw messages currently fall in a level-1 node's cumulative-distance
-- span [$2, $3). These are the node's children; comparing this against the
-- node's `last_summary_children` tells the read path whether its summary is
-- stale (the count can only grow, since history is append-only).
SELECT COUNT(*) AS "count!"
FROM memory.conversation_message cm
JOIN memory.conversation_message_metric m ON m.message_id = cm.id
WHERE cm.conversation_id = $1
  AND m.cumulative_distance >= $2
  AND m.cumulative_distance < $3
