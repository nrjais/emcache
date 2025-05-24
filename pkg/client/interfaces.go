package client

import (
	"context"
	"database/sql"
	"io"

	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/grpc"
)

// ClientInterface defines the interface for the EmCache client
type ClientInterface interface {
	Query(ctx context.Context, collectionName string, query string, args ...any) (*sql.Rows, error)
	SyncToLatest(ctx context.Context, limit int) error
	AddCollection(ctx context.Context, collectionName string, shape *pb.Shape) (*pb.AddCollectionResponse, error)
	RemoveCollection(ctx context.Context, collectionName string) (*pb.RemoveCollectionResponse, error)
	StartDbUpdates() error
	StopUpdates()
	Close() error
}

// SQLiteDBInterface wraps the sql.DB operations
type SQLiteDBInterface interface {
	Close() error
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (TxInterface, error)
	Exec(query string, args ...any) (sql.Result, error)
}

// TxInterface wraps the sql.Tx operations
type TxInterface interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// CollectionInterface manages individual collection state and operations
type CollectionInterface interface {
	GetLastAppliedOplogIndex(ctx context.Context) (int64, error)
	ApplyOplogEntries(ctx context.Context, entries []*pb.OplogEntry) error
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Close() error
}

// DecompressionInterface handles stream decompression
type DecompressionInterface interface {
	DecompressStream(stream grpc.ClientStream, firstChunk []byte, compression pb.Compression, writer io.Writer) error
}
