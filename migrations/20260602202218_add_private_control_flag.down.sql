DROP INDEX IF EXISTS memory.calender_task_privacy_idx;
DROP INDEX IF EXISTS memory.calender_daily_event_privacy_idx;
DROP INDEX IF EXISTS memory.calender_event_privacy_idx;
DROP INDEX IF EXISTS memory.contact_story_privacy_idx;
DROP INDEX IF EXISTS memory.contact_identity_privacy_idx;
DROP INDEX IF EXISTS memory.diary_privacy_idx;
DROP INDEX IF EXISTS memory.conversation_privacy_idx;

ALTER TABLE memory.calender_task          DROP COLUMN IF EXISTS privacy;
ALTER TABLE memory.calender_daily_event   DROP COLUMN IF EXISTS privacy;
ALTER TABLE memory.calender_event         DROP COLUMN IF EXISTS privacy;
ALTER TABLE memory.contact_story          DROP COLUMN IF EXISTS privacy;
ALTER TABLE memory.contact_identity       DROP COLUMN IF EXISTS privacy;
ALTER TABLE memory.diary                  DROP COLUMN IF EXISTS privacy;
ALTER TABLE memory.conversation           DROP COLUMN IF EXISTS privacy;

DROP TYPE IF EXISTS memory.privacy_control;
