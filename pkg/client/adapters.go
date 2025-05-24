package client

import (
	"context"
	"database/sql"
	"io"

	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/grpc"
)

// SQLiteDBAdapter wraps sql.DB to implement SQLiteDBInterface
type SQLiteDBAdapter struct {
	db *sql.DB
}

func (s *SQLiteDBAdapter) Close() error {
	return s.db.Close()
}

func (s *SQLiteDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

func (s *SQLiteDBAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, query, args...)
}

func (s *SQLiteDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

func (s *SQLiteDBAdapter) BeginTx(ctx context.Context, opts *sql.TxOptions) (TxInterface, error) {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &TxAdapter{tx: tx}, nil
}

func (s *SQLiteDBAdapter) Exec(query string, args ...any) (sql.Result, error) {
	return s.db.Exec(query, args...)
}

// TxAdapter wraps sql.Tx to implement TxInterface
type TxAdapter struct {
	tx *sql.Tx
}

func (t *TxAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *TxAdapter) Commit() error {
	return t.tx.Commit()
}

func (t *TxAdapter) Rollback() error {
	return t.tx.Rollback()
}

// CollectionAdapter adapts sqliteCollection to CollectionInterface
type CollectionAdapter struct {
	collection *sqliteCollection
}

func NewCollectionAdapter(db SQLiteDBInterface, version int32, details *pb.Collection) *CollectionAdapter {
	return &CollectionAdapter{
		collection: &sqliteCollection{
			db:      db,
			version: version,
			details: details,
		},
	}
}

func (c *CollectionAdapter) GetLastAppliedOplogIndex(ctx context.Context) (int64, error) {
	return c.collection.getLastAppliedOplogIndex(ctx)
}

func (c *CollectionAdapter) ApplyOplogEntries(ctx context.Context, entries []*pb.OplogEntry) error {
	return c.collection.applyOplogEntries(ctx, entries)
}

func (c *CollectionAdapter) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.collection.query(ctx, query, args...)
}

func (c *CollectionAdapter) Close() error {
	return c.collection.close()
}

// DecompressionAdapter implements DecompressionInterface
type DecompressionAdapter struct{}

func NewDecompressionAdapter() *DecompressionAdapter {
	return &DecompressionAdapter{}
}

func (d *DecompressionAdapter) DecompressStream(stream grpc.ClientStream, firstChunk []byte, compression pb.Compression, writer io.Writer) error {
	return decompressStream(stream, firstChunk, compression, writer)
}
