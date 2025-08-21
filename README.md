# EMCache

A high-performance cache replication system that synchronizes data from MongoDB to local SQLite databases via PostgreSQL with real-time oplog-based replication.

## Overview

EMCache is a distributed caching system designed to replicate MongoDB data to fast local SQLite databases. It provides real-time synchronization through MongoDB change streams and maintains consistency through an oplog-based architecture stored in PostgreSQL.

## Features

- ✅ **Real-time Replication**: Instant synchronization using MongoDB change streams
- ✅ **Fault Tolerance**: Robust error handling with retry mechanisms and graceful degradation
- ✅ **High Performance**: Optimized SQLite caches with configurable indexing
- ✅ **Flexible Schema**: JSON-based entity definitions with JSONPath field mapping
- ✅ **Comprehensive API**: Full CRUD operations with detailed system monitoring
- ✅ **Production Ready**: Complete with health checks, metrics, and deployment configuration
- ✅ **Snapshot Support**: Point-in-time data snapshots for backup and recovery
- ✅ **Automatic Cleanup**: Daily oplog cleanup with tombstone-based client redownload

### Oplog Cleanup & Tombstone Feature

EMCache automatically manages oplog storage with a configurable cleanup system:

- **Daily Cleanup**: Removes oplogs older than the configured retention period (default: 7 days)
- **Tombstone Oplogs**: When old oplogs are deleted, a tombstone oplog is created for each entity
- **Automatic Client Recovery**: When clients receive a tombstone oplog, they automatically:
  - Redownload the complete database snapshot
  - Resume synchronization from the tombstone point
  - Continue normal operation seamlessly

This ensures efficient storage management while maintaining data consistency and automatic client recovery.

## Quick Start

### Prerequisites

- **Rust** 1.75+ (using edition 2024)
- **PostgreSQL** 13+
- **MongoDB** 4.4+ (with change stream support)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd emcachers
   ```

2. **Configure the application**:
   ```bash
   cp config.toml.example config.toml
   # Edit config.toml with your database connections
   ```

3. **Build and run**:
   ```bash
   cargo build --release
   ./target/release/emcache
   ```

### Using Docker

```bash
docker-compose up -d
```

## Configuration

Edit `config.toml` to configure your setup:

```toml
[server]
host = "0.0.0.0"
port = 8080
shutdown_timeout = 10

[logging]
level = "info"

[database.postgres]
uri = "postgresql://postgres:password@localhost:5432/emcache"
max_connections = 20
min_connections = 5
connection_timeout = 10

[sources.main]
uri = "mongodb://localhost:27017/myapp"
database = "myapp"

[cache]
base_dir = "caches"
replication_interval = 10
entity_refresh_interval = 10

[snapshot]
check_interval = 10
min_lag = 100

[oplog]
cleanup_interval = 86400  # 24 hours in seconds
retention_days = 7
```

## API Reference

### Health Check

```bash
GET /health
```

### Entity Management

#### Create Entity

```bash
POST /api/entity
Content-Type: application/json

{
  "name": "users",
  "client": "main",
  "source": "users",
  "shape": {
    "id_column": {
      "path": "$._id",
      "type": "string"
    },
    "columns": [
      {
        "name": "email",
        "type": "string",
        "path": "$.email"
      },
      {
        "name": "name",
        "type": "string",
        "path": "$.name"
      },
      {
        "name": "age",
        "type": "integer",
        "path": "$.age"
      }
    ],
    "indexes": [
      {
        "name": "email_idx",
        "columns": ["email"]
      }
    ]
  }
}
```

#### List Entities

```bash
GET /api/entity
```

#### Get Entity

```bash
GET /api/entity/{name}
```

#### Delete Entity

```bash
DELETE /api/entity/{name}
```

### Oplog Operations

#### Get Oplogs for Entity

```bash
GET /api/oplogs/{entity}?limit=100&offset=0
```

#### Batch Oplog Query

```bash
POST /api/oplogs/batch
Content-Type: application/json

{
  "entities": ["users", "products"],
  "limit": 100,
  "offset": 0
}
```

### Snapshot Management

#### Create Snapshot

```bash
POST /api/snapshots/{entity}
```

#### Get Snapshot

```bash
GET /api/snapshots/{entity}
```

## Entity Schema Definition

Entities define how MongoDB documents are mapped to SQLite tables:

### Data Types

- `string`: Text data
- `integer`: Integer numbers
- `number`: Floating point numbers
- `bool`: Boolean values
- `jsonb`: JSON objects
- `any`: Any JSON value

### ID Column Types

- `string`: String identifiers
- `number`: Numeric identifiers

### JSONPath Mapping

Use JSONPath expressions to map MongoDB fields to SQLite columns:

```json
{
  "name": "user_email",
  "type": "string",
  "path": "$.contact.email"
}
```

## Development

### Running Tests

```bash
cargo test
```

### Code Quality

```bash
cargo fmt
cargo clippy
```

### Logging

Set the `RUST_LOG` environment variable for detailed logging:

```bash
RUST_LOG=debug cargo run
```

### Database Setup

1. **PostgreSQL**: Create database and run migrations
2. **MongoDB**: Ensure replica set is configured for change streams

## Deployment

### Environment Variables

Key configuration can be overridden with environment variables:

- `EMCACHE_SERVER__HOST`: Server bind address
- `EMCACHE_SERVER__PORT`: Server port
- `EMCACHE_DATABASE__POSTGRES__URI`: PostgreSQL connection string
- `EMCACHE_SOURCES__MAIN__URI`: MongoDB connection string

### Production Checklist

- [ ] Configure appropriate database connection pools
- [ ] Set up monitoring and alerting
- [ ] Configure log rotation
- [ ] Set resource limits
- [ ] Configure backup strategies
- [ ] Set up high availability for databases


## Architecture Details

### Data Flow

1. **MongoDB Change Streams**: Capture document changes in real-time
2. **Oplog Storage**: Store changes in PostgreSQL with metadata
3. **Cache Replication**: Apply oplogs to SQLite databases
4. **Snapshot Management**: Create point-in-time snapshots
5. **API Access**: Provide REST endpoints for management

### Fault Tolerance

- Automatic retry mechanisms with exponential backoff
- Graceful degradation during database failures
- Transaction-based consistency guarantees
- Health monitoring and alerting
