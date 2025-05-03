-- migrations/postgres/000001_initial_schema.down.sql

DROP TABLE IF EXISTS replicated_collections;
DROP TABLE IF EXISTS oplog;
DROP TABLE IF EXISTS resume_tokens;
