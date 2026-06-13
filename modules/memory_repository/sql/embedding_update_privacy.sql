-- Update the inherited privacy of the embedding row for a source entry,
-- identified by its product-of-nullable-FK reference.
UPDATE memory.embedding
SET privacy = $5
WHERE calender_event IS NOT DISTINCT FROM $1
  AND conversation   IS NOT DISTINCT FROM $2
  AND contact        IS NOT DISTINCT FROM $3
  AND diary          IS NOT DISTINCT FROM $4
