-- How many child nodes (level $2 - 1) currently fall inside a node's
-- cumulative-distance span [$3, $4). These are a higher-level node's children;
-- comparing this against its `last_summary_children` tells the read path whether
-- its summary is stale.
SELECT COUNT(*) AS "count!"
FROM memory.conversation_summary_node
WHERE conversation_id = $1
  AND level = $2 - 1
  AND distance_from >= $3
  AND distance_to   <= $4
