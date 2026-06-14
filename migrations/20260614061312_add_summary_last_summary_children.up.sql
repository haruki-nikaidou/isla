-- Lazy summary maintenance watermark.
--
-- Summaries are no longer recomputed eagerly on every append. Instead a node is
-- (re)summarized only when it is *requested* (the read path) or when its bucket
-- becomes *full*. `last_summary_children` records how many children (raw
-- messages for level 1, child nodes for higher levels) were folded into the
-- node's current `summary`. Because conversation history is append-only, a
-- node's children only ever grow, so the node is up to date iff its current
-- child count equals `last_summary_children`; a larger current count means the
-- summary is stale and must be refreshed before use.
ALTER TABLE memory.conversation_summary_node
    ADD COLUMN last_summary_children bigint NOT NULL DEFAULT 0;
