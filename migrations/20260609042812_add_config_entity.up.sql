-- Add up migration script here

CREATE TABLE vault.config
(
    id      SERIAL PRIMARY KEY,
    scope   VARCHAR NOT NULL,
    config_name    VARCHAR NOT NULL,
    content JSONB   NOT NULL,
    CONSTRAINT config_key_unique UNIQUE (scope, config_name)
);
