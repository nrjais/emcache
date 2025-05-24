package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/shape"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// PostgresPool represents a PostgreSQL connection pool interface
type PostgresPool interface {
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Exec(ctx context.Context, sql string, args ...any) (CommandTag, error)
	Close()
}

// Rows represents query result rows interface
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

// Row represents a single query result row interface
type Row interface {
	Scan(dest ...any) error
}

// CommandTag represents the result of an Exec operation
type CommandTag interface {
	RowsAffected() int64
}

// OplogRepository defines operations for oplog entries
type OplogRepository interface {
	GetOplogEntriesMultipleCollections(ctx context.Context, collections []string, afterID int64, limit int) ([]OplogEntry, error)
	GetOplogEntriesGlobal(ctx context.Context, afterID int64, limit int) ([]OplogEntry, error)
	InsertOplogEntry(ctx context.Context, entry OplogEntry) (int64, error)
}

// CollectionRepository defines operations for collection management
type CollectionRepository interface {
	AddReplicatedCollection(ctx context.Context, name string, collShape shape.Shape) error
	RemoveReplicatedCollection(ctx context.Context, name string) error
	GetReplicatedCollection(ctx context.Context, name string) (ReplicatedCollection, bool, error)
	ListReplicatedCollections(ctx context.Context) ([]string, error)
	GetAllCurrentCollectionVersions(ctx context.Context) ([]CollectionVersion, error)
	UpdateCollectionShape(ctx context.Context, name string, newShape shape.Shape, newVersion int) error
}

// ResumeTokenRepository defines operations for resume tokens
type ResumeTokenRepository interface {
	GetResumeToken(ctx context.Context, collection string) (string, bool, error)
	UpsertResumeToken(ctx context.Context, collection string, token string) error
}

// Database aggregates all database operations
type Database interface {
	OplogRepository
	CollectionRepository
	ResumeTokenRepository
}

// PostgresDatabase implements Database using PostgreSQL
type PostgresDatabase struct {
	pool PostgresPool
}

// NewPostgresDatabase creates a new PostgresDatabase instance
func NewPostgresDatabase(pool *pgxpool.Pool) *PostgresDatabase {
	return &PostgresDatabase{pool: NewPostgresPool(pool)}
}

// pgxPoolWrapper wraps pgxpool.Pool to implement PostgresPool interface
type pgxPoolWrapper struct {
	*pgxpool.Pool
}

func (p *pgxPoolWrapper) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return p.Pool.Query(ctx, sql, args...)
}

func (p *pgxPoolWrapper) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return p.Pool.QueryRow(ctx, sql, args...)
}

func (p *pgxPoolWrapper) Exec(ctx context.Context, sql string, args ...any) (CommandTag, error) {
	cmdTag, err := p.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return cmdTag, nil
}

// NewPostgresPool creates a PostgresPool wrapper from a pgxpool.Pool
func NewPostgresPool(pool *pgxpool.Pool) PostgresPool {
	return &pgxPoolWrapper{Pool: pool}
}
