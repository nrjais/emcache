package emcache

import (
	"context"
	"database/sql"
	"io"
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

// EntityAdapter adapts sqliteEntity to EntityInterface
type EntityAdapter struct {
	entity *sqliteEntity
}

func NewEntityAdapter(db SQLiteDBInterface, version int32, details *Entity) *EntityAdapter {
	return &EntityAdapter{
		entity: &sqliteEntity{
			db:      db,
			version: version,
			details: details,
		},
	}
}

func (c *EntityAdapter) GetLastAppliedOplogIndex(ctx context.Context) (int64, error) {
	return c.entity.getLastAppliedOplogIndex(ctx)
}

func (c *EntityAdapter) ApplyOplogEntries(ctx context.Context, entries []Oplog) error {
	return c.entity.applyOplogEntries(ctx, entries)
}

func (c *EntityAdapter) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.entity.query(ctx, query, args...)
}

func (c *EntityAdapter) Close() error {
	return c.entity.close()
}

// DecompressionAdapter implements DecompressionInterface
type DecompressionAdapter struct{}

func NewDecompressionAdapter() *DecompressionAdapter {
	return &DecompressionAdapter{}
}

func (d *DecompressionAdapter) DecompressStream(reader io.Reader, firstChunk []byte, compression CompressionType, writer io.Writer) error {
	return decompressStream(reader, firstChunk, compression, writer)
}
