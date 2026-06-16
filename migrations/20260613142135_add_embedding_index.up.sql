-- Unified RAG embedding index over indexable memory entries.
--
-- One row per source entry (a calendar event, conversation, contact identity,
-- or diary entry). The entry type is a *product of nullable foreign keys*, not
-- an enum discriminant: exactly one ref column is non-null (enforced by the
-- CHECK), and each ref is unique so an entry has at most one embedding.
--
-- `privacy` is inherited from the source entry and kept in sync by a hook, so
-- semantic search can filter by audience using the same visibility matrix as
-- the rest of memory. The vector is `halfvec(2048)` — the only width that is
-- HNSW-indexable at 2048 dimensions — searched by cosine distance.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE memory.embedding (
    id             bigserial PRIMARY KEY,
    calender_event bigint REFERENCES memory.calender_event (id)   ON DELETE CASCADE,
    conversation   bigint REFERENCES memory.conversation (id)     ON DELETE CASCADE,
    contact        uuid   REFERENCES memory.contact_identity (id) ON DELETE CASCADE,
    diary          bigint REFERENCES memory.diary (id)            ON DELETE CASCADE,
    privacy        memory.privacy_control NOT NULL,
    content        text NOT NULL,
    embedding      halfvec(2048),
    updated_at     bigint NOT NULL,
    CONSTRAINT embedding_exactly_one_ref
        CHECK (num_nonnulls(calender_event, conversation, contact, diary) = 1)
);

CREATE UNIQUE INDEX embedding_calender_event_uniq
    ON memory.embedding (calender_event) WHERE calender_event IS NOT NULL;
CREATE UNIQUE INDEX embedding_conversation_uniq
    ON memory.embedding (conversation) WHERE conversation IS NOT NULL;
CREATE UNIQUE INDEX embedding_contact_uniq
    ON memory.embedding (contact) WHERE contact IS NOT NULL;
CREATE UNIQUE INDEX embedding_diary_uniq
    ON memory.embedding (diary) WHERE diary IS NOT NULL;

CREATE INDEX embedding_hnsw
    ON memory.embedding USING hnsw (embedding halfvec_cosine_ops);
CREATE INDEX embedding_privacy_idx ON memory.embedding (privacy);
