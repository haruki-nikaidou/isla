-- Create the `plugin_reg` schema and every table backing the
-- `plugin_registrar` module's `entities::db` records.
--
-- See `modules/plugin_registrar/src/entities/db/` for the matching Rust
-- entities and field-level documentation.

CREATE SCHEMA IF NOT EXISTS plugin_reg;

-- =============================================================================
-- Core plugin record
-- =============================================================================

-- A registered plugin. One row per plugin; `id` is the cluster-issued
-- identity that every other table in this schema references.
CREATE TABLE plugin_reg.plugin (
    id                            uuid PRIMARY KEY,
    name                          text NOT NULL UNIQUE,
    registered_at                 timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    scope_range                   text[] NOT NULL DEFAULT '{}',
    description                   text NOT NULL DEFAULT '',
    jwt_secret                    text NOT NULL,
    server_message_signature_key  bytea NOT NULL,
    message_exchange              text NOT NULL
);

-- Optional cosmetic metadata. One row per plugin.
CREATE TABLE plugin_reg.plugin_metadata (
    plugin_id    uuid PRIMARY KEY REFERENCES plugin_reg.plugin (id) ON DELETE CASCADE,
    display_name text NOT NULL,
    author       text NOT NULL,
    author_url   text,
    repository   text,
    license      text,
    version      text NOT NULL
);

-- "This plugin requires that plugin (at this SemVer range)".
CREATE TABLE plugin_reg.plugin_dependency (
    id              bigserial PRIMARY KEY,
    plugin_id       uuid NOT NULL REFERENCES plugin_reg.plugin (id) ON DELETE CASCADE,
    require_plugin  text NOT NULL,
    require_version text NOT NULL,
    CONSTRAINT plugin_dependency_unique UNIQUE (plugin_id, require_plugin)
);

CREATE INDEX plugin_dependency_plugin_idx
    ON plugin_reg.plugin_dependency (plugin_id);
CREATE INDEX plugin_dependency_require_plugin_idx
    ON plugin_reg.plugin_dependency (require_plugin);

-- =============================================================================
-- Namespaces
-- =============================================================================

-- A namespace a plugin claims, stored as the segment array (e.g.
-- {"office","gmail"}) so the registrar can match by prefix without parsing
-- the dotted form.
CREATE TABLE plugin_reg.plugin_namespace (
    id        bigserial PRIMARY KEY,
    plugin_id uuid NOT NULL REFERENCES plugin_reg.plugin (id) ON DELETE CASCADE,
    name      text[] NOT NULL,
    CONSTRAINT plugin_namespace_name_unique UNIQUE (name),
    CONSTRAINT plugin_namespace_name_nonempty CHECK (cardinality(name) > 0)
);

CREATE INDEX plugin_namespace_plugin_idx
    ON plugin_reg.plugin_namespace (plugin_id);

-- =============================================================================
-- Tools
-- =============================================================================

-- A tool a plugin advertises to the LLM. `parameters_schema` is the JSON
-- Schema for the call's input; `routing_key` is the AMQP routing key the
-- cluster uses to dispatch the call into the plugin.
CREATE TABLE plugin_reg.plugin_tool (
    id                bigserial PRIMARY KEY,
    plugin_id         uuid NOT NULL REFERENCES plugin_reg.plugin (id) ON DELETE CASCADE,
    name              text NOT NULL,
    display_name      text NOT NULL,
    description       text NOT NULL DEFAULT '',
    parameters_schema jsonb NOT NULL,
    routing_key       text NOT NULL,
    CONSTRAINT plugin_tool_name_unique UNIQUE (plugin_id, name)
);

CREATE INDEX plugin_tool_plugin_idx ON plugin_reg.plugin_tool (plugin_id);

-- =============================================================================
-- Skills
-- =============================================================================

-- A single skill a plugin ships to the agent.
CREATE TABLE plugin_reg.plugin_skill (
    id             bigserial PRIMARY KEY,
    plugin_id      uuid NOT NULL REFERENCES plugin_reg.plugin (id) ON DELETE CASCADE,
    name           text NOT NULL,
    display_name   text NOT NULL,
    description    text NOT NULL DEFAULT '',
    main_file_text text NOT NULL,
    CONSTRAINT plugin_skill_name_unique UNIQUE (plugin_id, name)
);

CREATE INDEX plugin_skill_plugin_idx ON plugin_reg.plugin_skill (plugin_id);

-- Auxiliary file bundled with a skill (templates, helpers, etc.).
CREATE TABLE plugin_reg.plugin_skill_extent_file (
    id        bigserial PRIMARY KEY,
    skill_id  bigint NOT NULL REFERENCES plugin_reg.plugin_skill (id) ON DELETE CASCADE,
    file_name text NOT NULL,
    content   text NOT NULL,
    CONSTRAINT plugin_skill_extent_file_unique UNIQUE (skill_id, file_name)
);

CREATE INDEX plugin_skill_extent_file_skill_idx
    ON plugin_reg.plugin_skill_extent_file (skill_id);

-- =============================================================================
-- Structured memory queries
-- =============================================================================

-- A logical grouping of memory queries the plugin owns.
CREATE TABLE plugin_reg.plugin_memory_set (
    id          bigserial PRIMARY KEY,
    plugin_id   uuid NOT NULL REFERENCES plugin_reg.plugin (id) ON DELETE CASCADE,
    name        text NOT NULL,
    description text NOT NULL DEFAULT '',
    CONSTRAINT plugin_memory_set_name_unique UNIQUE (plugin_id, name)
);

CREATE INDEX plugin_memory_set_plugin_idx
    ON plugin_reg.plugin_memory_set (plugin_id);

-- A single named, schema-typed query inside a memory set.
CREATE TABLE plugin_reg.plugin_memory_entry (
    id             bigserial PRIMARY KEY,
    memory_set_id  bigint NOT NULL REFERENCES plugin_reg.plugin_memory_set (id) ON DELETE CASCADE,
    name           text NOT NULL,
    description    text NOT NULL DEFAULT '',
    query_params   jsonb NOT NULL,
    return_schema  jsonb NOT NULL,
    CONSTRAINT plugin_memory_entry_name_unique UNIQUE (memory_set_id, name)
);

CREATE INDEX plugin_memory_entry_memory_set_idx
    ON plugin_reg.plugin_memory_entry (memory_set_id);
