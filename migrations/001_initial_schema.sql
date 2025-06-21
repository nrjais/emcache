-- Initial EMCachers Database Schema
-- Migration: 001_initial_schema

-- Create custom types
CREATE TYPE resume_token_status AS ENUM ('scanning', 'live', 'failed');

-- Entities table
CREATE TABLE entities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    entity_name VARCHAR(255) NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    columns JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT entities_status_check CHECK (status IN ('pending', 'scanning', 'live', 'error'))
);

-- Oplog table for main oplogs
CREATE TABLE oplog (
    id BIGSERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    doc_id VARCHAR(255) NOT NULL,
    entity VARCHAR(255) NOT NULL,
    data JSONB NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT oplog_operation_check CHECK (operation IN ('upsert', 'delete'))
);

-- Oplog staging table for new entities being scanned
CREATE TABLE oplog_staging (
    id BIGSERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    doc_id VARCHAR(255) NOT NULL,
    entity VARCHAR(255) NOT NULL,
    data JSONB NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT oplog_staging_operation_check CHECK (operation IN ('upsert', 'delete'))
);

-- Resume tokens table for MongoDB change streams
CREATE TABLE resume_tokens (
    id SERIAL PRIMARY KEY,
    entity_name VARCHAR(255) NOT NULL UNIQUE,
    token_data TEXT NOT NULL,
    status resume_token_status NOT NULL DEFAULT 'live',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_used_at TIMESTAMP WITH TIME ZONE,
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

-- Metadata table for system configuration and state
CREATE TABLE metadata (
    key VARCHAR(255) PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance

-- Entities indexes
CREATE INDEX idx_entities_name ON entities(name);
CREATE INDEX idx_entities_status ON entities(status);
CREATE INDEX idx_entities_created_at ON entities(created_at);

-- Oplog indexes
CREATE INDEX idx_oplog_entity ON oplog(entity);
CREATE INDEX idx_oplog_created_at ON oplog(created_at);
CREATE INDEX idx_oplog_entity_created ON oplog(entity, created_at);
CREATE INDEX idx_oplog_doc_id ON oplog(doc_id);
CREATE INDEX idx_oplog_operation ON oplog(operation);

-- Oplog staging indexes
CREATE INDEX idx_oplog_staging_entity ON oplog_staging(entity);
CREATE INDEX idx_oplog_staging_created_at ON oplog_staging(created_at);
CREATE INDEX idx_oplog_staging_entity_created ON oplog_staging(entity, created_at);

-- Resume tokens indexes
CREATE INDEX idx_resume_tokens_entity_name ON resume_tokens(entity_name);
CREATE INDEX idx_resume_tokens_status ON resume_tokens(status);
CREATE INDEX idx_resume_tokens_last_used ON resume_tokens(last_used_at);

-- Metadata indexes
CREATE INDEX idx_metadata_key ON metadata(key);

-- Create functions for automatic timestamp updates
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for automatic timestamp updates
CREATE TRIGGER update_entities_updated_at
    BEFORE UPDATE ON entities
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_resume_tokens_updated_at
    BEFORE UPDATE ON resume_tokens
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_metadata_updated_at
    BEFORE UPDATE ON metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Insert default metadata
INSERT INTO metadata (key, value) VALUES
    ('schema_version', '1.0'),
    ('initialization_time', NOW()::TEXT),
    ('cache_replication_offset', '0');
