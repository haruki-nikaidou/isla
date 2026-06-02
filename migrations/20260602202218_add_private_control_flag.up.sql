-- Privacy control flag for memory entities.
--
-- The flag controls which audience the agent may surface a memory to when
-- assembling LLM context:
--   * Private   - only visible during conversations with the Master.
--   * Protected - visible to Master, Dude, and Acquaintance relationships.
--   * Public    - visible to all relationships, including Strangers.
--
-- This is the storage-side primitive; runtime filtering happens in the
-- memory_repository service layer based on the current conversation peer.

CREATE TYPE memory.privacy_control AS ENUM ('Private', 'Protected', 'Public');

-- Apply the flag to every table that stores content which may end up in the
-- LLM input context. Container tables (calender, conversation_message,
-- conversation_content, contact) inherit visibility from their leaves /
-- their owning identity, so they intentionally omit the column.

ALTER TABLE memory.conversation
    ADD COLUMN privacy memory.privacy_control NOT NULL DEFAULT 'Protected';

ALTER TABLE memory.diary
    ADD COLUMN privacy memory.privacy_control NOT NULL DEFAULT 'Protected';

ALTER TABLE memory.contact_identity
    ADD COLUMN privacy memory.privacy_control NOT NULL DEFAULT 'Protected';

ALTER TABLE memory.contact_story
    ADD COLUMN privacy memory.privacy_control NOT NULL DEFAULT 'Protected';

ALTER TABLE memory.calender_event
    ADD COLUMN privacy memory.privacy_control NOT NULL DEFAULT 'Protected';

ALTER TABLE memory.calender_daily_event
    ADD COLUMN privacy memory.privacy_control NOT NULL DEFAULT 'Protected';

ALTER TABLE memory.calender_task
    ADD COLUMN privacy memory.privacy_control NOT NULL DEFAULT 'Protected';

CREATE INDEX conversation_privacy_idx       ON memory.conversation       (privacy);
CREATE INDEX diary_privacy_idx              ON memory.diary              (privacy);
CREATE INDEX contact_identity_privacy_idx   ON memory.contact_identity   (privacy);
CREATE INDEX contact_story_privacy_idx      ON memory.contact_story      (privacy);
CREATE INDEX calender_event_privacy_idx     ON memory.calender_event     (privacy);
CREATE INDEX calender_daily_event_privacy_idx ON memory.calender_daily_event (privacy);
CREATE INDEX calender_task_privacy_idx      ON memory.calender_task      (privacy);
