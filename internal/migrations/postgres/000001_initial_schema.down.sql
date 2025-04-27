-- migrations/postgres/000001_initial_schema.down.sql

DROP TABLE IF EXISTS replicated_collections; -- Drop dependent indexes automatically
DROP TABLE IF EXISTS oplog; -- Drop dependent indexes automatically
DROP TABLE IF EXISTS resume_tokens; -- Drop dependent indexes automatically
