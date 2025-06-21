# EMCachers System Design

## Overview
Cache replication system that synchronizes data from MongoDB to local SQLite databases via PostgreSQL with idempotent oplog-based replication.

## Core Data Structures

### Oplog
Type defining a generic oplog entry:
```
type Oplog (
  ID         int64      -- Incrementing number
  Operation  enum       -- Upsert/Delete
  DocID      string
  CreatedAt  timestamp
  Entity     string
  Data       json
  Version    int
)
```

### Entity Schema
Type defining entity structure:
```
enum DataType (
  JSONB
  Any
  Bool
  Number
  Integer
  Text
)

type Column (
  Name string
  Type DataType
  Path string
)

type Shape (
  EntityName string
  SourceName string
  Columns []Column
)
```
**Note**: Version removed from Shape schema. New shapes will be added when changes are needed.

### Metadata Table
Local SQLite metadata storage:
```
key string
value json
```

## System Components

### 1. Job Server
**Purpose**: Manage list of jobs and run them based on provided interval/duration

**Functionality**:
- Manage list of jobs with configurable intervals
- Handle cancellation on server shutdown by sending cancellation signals
- Allow jobs to gracefully close
- Implement exponential backoff retry mechanism for failed jobs

**Configuration**:
- Timing intervals configurable with reasonable defaults
- No hot-reloading required initially

### 2. Entity Manager
**Purpose**: Manage entities for which replication needs to happen

**Functionality**:
- Store entities in PostgreSQL
- Create job to refresh entities regularly from database
- Support hard refresh operations
- Handle entity deletion with local SQLite cleanup

**Entity Management**:
- **Initial Population**: Manual entity setup
- **Future Enhancement**: HTTP API for entity management
- **Shape Changes**: Entity shapes are immutable; new shapes created for changes
- **Entity Deletion**: Remove entity from system and delete local SQLite databases

### 3. Mongo Change Stream Listener
**Purpose**: Listen to MongoDB change stream and publish to channel with backpressure

**Functionality**:
- Listen to MongoDB change stream with backpressure handling
- Format entries as Oplog and send over channel
- Filter oplogs for known entities only
- Extract data as JSON with keys picked from entity shape
- Handle acknowledgment channel for processed changes
- Update acknowledged change resume token to PostgreSQL after interval/batch
- Store resume token in metadata table with key `mongo_resume_token`

**Initial Scan Logic**:
- When resume token not found on first run:
  - Get all entities
  - Scan each source collection
  - Create upsert oplog entry for each document
  - Start change listener and save resume token before scanning
  - Wait for initial scan completion before resuming change stream

**Resume Token Management**:
- Track resume token versions with status (scanning, live)
- **On resume token loss**: Trigger full scan again
- Implement version-based resume token tracking

**Error Handling**:
- MongoDB connection loss: Retry with exponential backoff, fail after configured time
- Configurable retry limits and timeouts

### 4. Oplog Manager
**Purpose**: Listen on oplog channel and manage PostgreSQL oplog storage

**Functionality**:
- Listen on oplog channel from change stream listener
- Create entries in PostgreSQL oplog table with batch processing
- Track status of each entity for oplog insertion
- Handle new entities by pushing oplogs to staging and marking as pending
- Create task for full collection scan of new collections
- Move staging oplogs to main table once collection scan is complete
- Handle version tracking for scanning/live status

### 5. Cache Replicator
**Purpose**: Scan PostgreSQL oplog table and apply changes to cache databases

**Functionality**:
- Scan PostgreSQL oplog table regularly for new entries
- Track scan start offset from local metadata DB (start from 0 if none found)
- Process oplogs in batches, grouped by entity name
- Get corresponding cache DB from Cache DB Manager for each entity group
- Apply oplogs to cache databases (idempotent operations)
- Save oplog index to cache DB metadata table
- Save PostgreSQL oplog index to local metadata DB

**Batch Processing**:
- Configurable batch sizes for different operations
- Default batch size: 1000
- Group processing by entity for efficiency

**Corrupted Cache Handling**:
- Recreate database and replay oplogs from PostgreSQL when corruption detected

### 6. Cache DB Manager
**Purpose**: Manage SQLite database connections for each entity

**Functionality**:
- Maintain SQLite connection pool per entity
- Initialize databases when they don't exist
- Clean up unused connections after expiry time
- Delete local SQLite DB directory when entity is deleted
- Handle database recreation for corrupted cache databases

**File Structure**:
- File format: `dbs/{entity_name}/db.sqlite` (version removed from path)
- Automatic cleanup of unused entity databases

## REST API Implementation

**Entity Management API**:
- CRUD operations for entities
- Entity validation and error handling

**Batch Oplog API**:
- Get oplogs for multiple entities
- Efficient batch retrieval

**Health Check API**:
- System status and component health
- Individual component status monitoring

**API Characteristics**:
- No authentication/authorization required
- Proper HTTP error handling and status codes
- API documentation/OpenAPI specs

## Configuration Management

**Connection Management**:
- Standard MongoDB and PostgreSQL connection string formats
- Database connection pooling with configurable pool sizes
- Support URL-based authentication parameters

**Configuration Sources**:
- Configuration files with environment variable overrides
- No hot-reloading support initially

**Default Values**:
- Batch size: 1000
- Intervals based on use case requirements
- Connection pool sizes configurable
- Retry parameters (backoff, max attempts, timeouts)

## Error Handling & Resilience

**Retry Strategy**:
- Exponential backoff retry for all database connections
- MongoDB connection failures: Retry with exponential backoff, fail after configured time
- PostgreSQL connection failures: Retry with exponential backoff, fail after configured time
- Configurable retry limits and timeouts

**Data Consistency**:
- **No exactly-once processing**: Oplogs are idempotent
- **No duplicate detection**: Idempotent operations handle duplicates naturally
- **Corrupted cache databases**: Recreate and replay oplogs from PostgreSQL

**Graceful Degradation**:
- Circuit breaker pattern for external dependencies
- Graceful shutdown handling with cancellation contexts

## Database Schemas & Migrations

**PostgreSQL Tables**:
- Oplog table with full schema
- Entities table for entity definitions
- Resume token and metadata storage
- Run migrations on startup

**SQLite Databases**:
- Cache databases per entity
- Local metadata database (key-value store)
- Initialize when missing, no migrations needed

## Deployment & Infrastructure

**Packaging**:
- Single binary deployment
- Docker container support
- External dependencies: SQLite, MongoDB, PostgreSQL only

**Monitoring & Observability**:
- Structured logging
- Health check endpoints
- Component status monitoring
- Performance metrics logging
- Error rate tracking

**Security**:
- No specific security requirements for database connections
- No API authentication required
- Configuration via files and environment variables


## System Characteristics

**Idempotent Operations**:
- All oplog operations are idempotent
- No exactly-once processing guarantees needed
- Duplicate handling not required

**Version Management**:
- Entity shapes are immutable
- New versions created for shape changes
- Version-based resume token tracking

**Scalability Considerations**:
- Connection pooling for all database connections
- Batch processing for efficiency
- Memory-efficient processing patterns
- Automatic resource cleanup
