-- Upsert the embedding row for a source entry identified by its product-of-
-- nullable-FK reference (exactly one of $1..$4 is non-null). `IS NOT DISTINCT
-- FROM` matches the single existing row for that reference (NULLs included)
-- without needing a dynamic ON CONFLICT target.
WITH updated AS (
    UPDATE memory.embedding
    SET privacy = $5, content = $6, embedding = $7, updated_at = $8
    WHERE calender_event IS NOT DISTINCT FROM $1
      AND conversation   IS NOT DISTINCT FROM $2
      AND contact        IS NOT DISTINCT FROM $3
      AND diary          IS NOT DISTINCT FROM $4
    RETURNING id
),
inserted AS (
    INSERT INTO memory.embedding
        (calender_event, conversation, contact, diary, privacy, content, embedding, updated_at)
    SELECT $1, $2, $3, $4, $5, $6, $7, $8
    WHERE NOT EXISTS (SELECT 1 FROM updated)
    RETURNING id
)
SELECT id AS "id!" FROM updated
UNION ALL
SELECT id AS "id!" FROM inserted
