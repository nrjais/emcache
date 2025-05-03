-- migrations/postgres/000001_initial_schema.up.sql

-- Table for storing MongoDB change stream resume tokens
CREATE TABLE resume_tokens (
    collection  VARCHAR(255) PRIMARY KEY,
    token       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table for storing the normalized operation log
CREATE TABLE oplog (
    id          BIGSERIAL PRIMARY KEY,
    operation   VARCHAR(10) NOT NULL,
    doc_id      VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    collection  VARCHAR(255) NOT NULL,
    doc         JSONB NULL,
    version     INT NOT NULL DEFAULT 0
);
CREATE INDEX idx_oplog_collection_id ON oplog(collection, version, id);

-- Table for storing the list of collections to be replicated
CREATE TABLE replicated_collections (
    collection_name VARCHAR(255) PRIMARY KEY,
    current_version INT NOT NULL DEFAULT 1,
    shape JSONB NOT NULL
);
