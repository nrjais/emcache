-- Initial EMCache Database Schema
-- Migration: 001_initial_schema
-- Entities table
CREATE TABLE entities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    client VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL,
    shape JSONB NOT NULL,
    created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW () NOT NULL
);

-- Oplog table for main oplogs
CREATE TABLE oplog (
    id BIGSERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    doc_id VARCHAR(255) NOT NULL,
    entity VARCHAR(255) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW () NOT NULL,
        CONSTRAINT oplog_operation_check CHECK (operation IN ('upsert', 'delete', 'start_resync', 'end_resync'))
);

-- Resume tokens table for MongoDB change streams
CREATE TABLE mongo_resume_tokens (
    entity VARCHAR(255) NOT NULL PRIMARY KEY,
    token_data JSONB NOT NULL
);

-- Oplog indexes
CREATE INDEX idx_oplog_entity_id ON oplog (entity, id);

CREATE INDEX idx_oplog_entity_doc_id ON oplog (entity, doc_id);

-- Resume tokens indexes
CREATE INDEX idx_mongo_resume_tokens_entity_collection_database ON mongo_resume_tokens (entity);
