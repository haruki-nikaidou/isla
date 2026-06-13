-- Personality facets: the building blocks of Isla's character.
--
-- The system prompt is not one fixed blob. Like a person, Isla presents
-- different facets of herself depending on who she is talking to. Each facet
-- carries a `privacy` flag reusing `memory.privacy_control`, so the same
-- visibility matrix that gates memories also gates which character traits are
-- surfaced:
--   * Private   - only shown when talking to the Master.
--   * Protected - shown to Master and known relationships.
--   * Public    - shown to everyone, including strangers.
--
-- `is_core` facets bypass the privacy gate entirely: they are the invariant
-- base of her character, always included regardless of audience.
--
-- Runtime selection (which facets apply to the current peer) happens in the
-- memory_repository service layer; `priority` orders the surviving facets when
-- they are concatenated into the system prompt (higher first).

CREATE TABLE memory.personality_facet (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL,
    content     text NOT NULL,
    priority    integer NOT NULL DEFAULT 0,
    is_core     boolean NOT NULL DEFAULT FALSE,
    privacy     memory.privacy_control NOT NULL DEFAULT 'Protected',
    created_at  bigint NOT NULL,
    updated_at  bigint NOT NULL
);

CREATE INDEX personality_facet_privacy_idx ON memory.personality_facet (privacy);
