-- Revert the plugin_registrar schema migration. Dropping the schema with
-- CASCADE removes every table, sequence, and index it contains.

DROP SCHEMA IF EXISTS plugin_reg CASCADE;
