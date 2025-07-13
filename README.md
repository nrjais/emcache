## Design
# Oplog
* Type defining a generic oplog entry
```
type Oplog (
  ID         int64 -- Incrementing number
  Operation  enum Upsert/Delete
  DocID      string
  CreatedAt  timestamp
  Entity     string
  Data       json
)
```
* Type definign entity
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
* Metadata table local sqlite
```
key string
value json
```

# Job server
* Manage list of jobs and run them based on provided interval/duration
* Manage cancellation on server shutdown, by sending cancellation signal to jobs so that they can gracefully close

# Entity Manager
* Should manage entities for which replication has to happen
* Entities are stored in postgres
* Create and job to refresh entities regularly from database
* Also option to hard refresh

# Mongo Change Stream Listener - Split it in multiple modules
* Listen to mongo change stream and publish to a channel with backpressure
* Entry send over channel should be formatted as Oplog
* If the oplog is not for known entity ignore it
* the data in oplog is json of the actual doc with keys picked from entity shape
* On an acknowledment channle recieve message about what changes are processed
* After an interval or batch size update the acknowledged change resume token to potgres table
* Resume token in metadata table with key `mongo_resume_token`
* When resume token is not found on first time, get all entities and for each entity scan the source collection
* When scanning source collection for each row create and upsert oplog entry
* Before starting scan start the change listener and save resume token and then wait for initial scan to finish before resuming

# Oplog Manager
* Listen on the oplog channel
* For each message create an entry in oplog postgres table
* Track the status of each entity for which oplog is being insert
* If a new entity comes, push its oplog to staging and mark it as pending
* Create and task to start full collection scane of that collection
* Once the collection is scanned mark it as `scanned` and move the staging oplogs to main table

# Cache Replicator
* Scan postgres oplog table regularly for new entries
* Get scane start offset from local metadata db, start from 0 if none found
* For every batch on oplogs from postgres, group then by entity name
* For each group of entry get the corrrespoing cache db from cache db manager
* Apply oplogs in the cache db
* Save the oplog index to cache db in meta table
* Save the postgres oplog index to local metadata db

# Cache DB manager
* For each entity manage and sqlite db connection
* If the request entity db does not exists, initialize it first
* Regularly clean up unused connection after some expiry time, to release stale entities
* If and entity is deleted from db delete its local sqlite db directory
* File name format is `dbs/{entity_name}/db.sqlite`

# EMCachers

A high-performance cache replication system that synchronizes data from MongoDB to local SQLite databases via PostgreSQL with idempotent oplog-based replication.

## Architecture Overview

EMCachers implements a distributed cache replication system with the following components:

- **MongoDB Change Stream Listener**: Captures real-time changes from MongoDB collections
- **Oplog Manager**: Processes and stores change events in PostgreSQL with staging support
- **Cache Replicator**: Applies oplogs to local SQLite cache databases
- **Entity Manager**: Manages entity definitions and lifecycle
- **REST API Server**: Provides management endpoints and health checks
- **Job Scheduler**: Handles background tasks and maintenance

## Key Features

- ✅ **Idempotent Replication**: No duplicate handling needed, operations are naturally idempotent
- ✅ **Resilient Architecture**: Circuit breakers, exponential backoff, and retry mechanisms
- ✅ **Modular Design**: Clean separation of concerns with focused components
- ✅ **Comprehensive Monitoring**: System metrics, health checks, and performance tracking
- ✅ **Production Ready**: Docker support, configuration management, and graceful shutdown
- ✅ **SQLite Connection Pooling**: Per-entity database management with automatic cleanup
- ✅ **Entity Lifecycle Management**: Complete CRUD operations with status tracking
- ✅ **Background Processing**: Automatic oplog processing and cache synchronization

## Quick Start with Docker

1. **Clone and Build**:
   ```bash
   git clone <repository>
   cd emcachers
   cp config.toml.template config.toml
   # Edit config.toml with your settings
   ```

2. **Start with Docker Compose**:
   ```bash
   docker-compose up -d
   ```

3. **Check Health**:
   ```bash
   curl http://localhost:8080/health
   ```

## Configuration

Copy `config.toml.template` to `config.toml` and customize:

```toml
[server]
host = "0.0.0.0"
port = 8080

[database]
postgres_url = "postgresql://user:pass@localhost:5432/emcachers"
mongodb_url = "mongodb://localhost:27017/myapp"

[batch]
oplog_batch_size = 1000
postgres_batch_size = 500

[retry]
max_attempts = 3
initial_delay_ms = 1000
backoff_multiplier = 2.0
```

## API Endpoints

### Health Checks
- `GET /health` - Basic health check
- `GET /health/detailed` - Comprehensive system status

### Entity Management
- `GET /api/entities` - List all entities
- `POST /api/entities` - Create new entity
- `GET /api/entities/{name}` - Get specific entity
- `PUT /api/entities/{name}` - Update entity
- `DELETE /api/entities/{name}` - Delete entity

### Oplog Operations
- `GET /api/oplogs/{entity}` - Get oplogs for entity
- `POST /api/oplogs/batch` - Get oplogs for multiple entities

### Replication Status
- `GET /api/replication/stats` - Replication statistics
- `POST /api/replication/recreate/{entity}` - Recreate entity cache

### System Information
- `GET /api/system/stats` - System metrics

## Entity Configuration

Entities define how MongoDB collections are mapped to cache databases:

```json
{
  "name": "users",
  "shape": {
    "entity_name": "User",
    "source_name": "users",
    "columns": [
      {"name": "id", "type": "Text", "path": "_id"},
      {"name": "email", "type": "Text", "path": "email"},
      {"name": "name", "type": "Text", "path": "name"}
    ]
  }
}
```

## Development

### Prerequisites
- Rust 1.75+
- PostgreSQL 13+
- MongoDB 4.4+ (for change streams)

### Build from Source
```bash
cargo build --release
```

### Run Tests
```bash
cargo test
```

### Development Environment
```bash
# Start PostgreSQL
docker run -d --name postgres \
  -e POSTGRES_DB=emcachers \
  -e POSTGRES_USER=emcachers \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 postgres:15

# Run application
cargo run
```

## Architecture Details

### Data Flow
1. **MongoDB** → Change streams capture document changes
2. **Oplog Manager** → Processes changes and stores in PostgreSQL
3. **Cache Replicator** → Reads oplogs and applies to SQLite caches
4. **SQLite Databases** → Per-entity cache databases for fast queries

### Fault Tolerance
- **Circuit Breakers**: Prevent cascade failures
- **Exponential Backoff**: Handles temporary failures gracefully
- **Health Monitoring**: Automatic detection of component issues
- **Graceful Degradation**: System continues operating during partial failures

### Performance Features
- **Batch Processing**: Efficient handling of high-volume changes
- **Connection Pooling**: Optimized database connections
- **Async Processing**: Non-blocking I/O throughout the system
- **Indexing**: Optimized database schemas for query performance

## Monitoring

### System Metrics
- Memory usage and thresholds
- CPU utilization
- Database connection health
- Processing throughput

### Alert Thresholds
- CPU > 80%
- Memory > 85%
- Error rate > 5%
- High pending oplog count

### Performance Tracking
- Operation duration metrics
- Success/failure rates
- Component response times

## Deployment

### Production Checklist
- [ ] Configure connection strings for all databases
- [ ] Set appropriate batch sizes for your workload
- [ ] Configure retry parameters for your network conditions
- [ ] Set up monitoring and alerting
- [ ] Configure log levels and rotation
- [ ] Set resource limits in Docker/Kubernetes

### Scaling Considerations
- **Horizontal Scaling**: Multiple cache replicator instances
- **Database Sharding**: Partition oplogs by entity
- **Load Balancing**: Distribute API requests
- **Resource Allocation**: Memory for connection pools and batching

## Troubleshooting

### Common Issues
1. **High Memory Usage**: Reduce batch sizes or increase connection cleanup frequency
2. **Slow Replication**: Check PostgreSQL performance and oplog query efficiency
3. **Connection Failures**: Verify database connectivity and adjust retry settings
4. **Cache Corruption**: Use recreation endpoints to rebuild from oplogs

### Debugging
- Enable debug logging: `RUST_LOG=debug`
- Check health endpoints for component status
- Monitor system metrics for bottlenecks
- Use PostgreSQL query analysis for performance issues

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Ensure `cargo fmt` and `cargo clippy` pass
6. Submit a pull request

## License

[Add your license information here]

## Status

✅ **Production Ready**: Core functionality complete with comprehensive error handling, monitoring, and deployment support.

Major components implemented:
- Entity Management with full CRUD operations
- Cache Replicator with background processing
- REST API with comprehensive endpoints
- Monitoring and alerting system
- Docker deployment configuration
- Database schema and migrations
