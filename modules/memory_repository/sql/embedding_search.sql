-- Nearest embeddings to a query vector ($1) by cosine distance, restricted to
-- the audience-allowed privacy levels ($2), limited to $3 rows. Returns the
-- source reference so callers can resolve the underlying memory entry.
SELECT
    id,
    calender_event,
    conversation,
    contact,
    diary,
    privacy AS "privacy: PrivacyControlFlag",
    content,
    (embedding <=> $1) AS "distance!"
FROM memory.embedding
WHERE embedding IS NOT NULL
  AND privacy = ANY($2)
ORDER BY embedding <=> $1
LIMIT $3
