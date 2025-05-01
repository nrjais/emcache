-- migrations/postgres/000001_initial_schema.up.sql

-- Table for storing MongoDB change stream resume tokens
CREATE TABLE resume_tokens (
    collection  VARCHAR(255) PRIMARY KEY,
    token       TEXT NOT NULL,        -- Store the resume token (JSON marshaled)
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_resume_tokens_updated_at ON resume_tokens(updated_at);

-- Table for storing the normalized operation log
CREATE TABLE oplog (
    id          BIGSERIAL PRIMARY KEY,     -- Auto-incrementing ID
    operation   VARCHAR(10) NOT NULL,    -- 'UPSERT' or 'DELETE'
    doc_id      VARCHAR(255) NOT NULL,   -- Document identifier (id)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Timestamp of oplog entry creation
    collection  VARCHAR(255) NOT NULL,   -- MongoDB collection name
    doc         JSONB NULL,              -- Full document data for UPSERTs (NULL for DELETE)
    version     INT NOT NULL DEFAULT 0   -- Collection version this entry belongs to
);
CREATE INDEX idx_oplog_collection_id ON oplog(collection, version, id);

-- Table for storing the list of collections to be replicated
CREATE TABLE replicated_collections (
    collection_name VARCHAR(255) PRIMARY KEY,
    current_version INT NOT NULL DEFAULT 1 -- Tracks the latest data version for the collection
);
