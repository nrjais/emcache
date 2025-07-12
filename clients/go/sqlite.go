package emcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type LocalCache interface {
	DB() *sql.DB
	GetMaxOplogId(ctx context.Context) (int64, error)
	ApplyOplogs(ctx context.Context, oplogs []Oplog) error
	Close() error
}

const (
	metadataTableName = "metadata"
	maxOplogId        = "max_oplog_id"
)
const (
	dataTableName = "data"
	pkColumn      = "id"
)

type localCache struct {
	db      *sql.DB
	details *Entity
}

func openSQLiteDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db (%s): %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)

	return db, nil
}

// Close closes the SQLite database connection
func (e *localCache) Close() error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

func (e *localCache) DB() *sql.DB {
	return e.db
}

// GetMaxOplogId retrieves the last processed oplog ID from metadata
func (e *localCache) GetMaxOplogId(ctx context.Context) (int64, error) {
	if e.db == nil {
		return 0, fmt.Errorf("database connection is not available")
	}

	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	row := e.db.QueryRowContext(ctx, query, maxOplogId)

	var value string
	if err := row.Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // Default to 0 if no record exists (consistent with server)
		}
		return 0, fmt.Errorf("failed to get last processed ID: %w", err)
	}

	idx, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse last processed ID: %w", err)
	}

	return idx, nil
}

// ApplyOplogs applies a batch of oplogs to the SQLite database
func (e *localCache) ApplyOplogs(ctx context.Context, oplogs []Oplog) error {
	if e.db == nil {
		return fmt.Errorf("database connection is not available")
	}

	if len(oplogs) == 0 {
		return nil
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var maxProcessedID int64

	for _, entry := range oplogs {
		if entry.ID > maxProcessedID {
			maxProcessedID = entry.ID
		}

		switch entry.Operation {
		case OperationUpsert:
			err = e.applyUpsert(ctx, tx, entry, entry.DocID)
			if err != nil {
				return fmt.Errorf("failed to apply upsert for doc_id %s: %w", entry.DocID, err)
			}

		case OperationDelete:
			err = e.applyDelete(ctx, tx, entry.DocID)
			if err != nil {
				return fmt.Errorf("failed to apply delete for doc_id %s: %w", entry.DocID, err)
			}
		default:
		}
	}

	err = e.setLastProcessedID(ctx, tx, maxProcessedID)
	if err != nil {
		return fmt.Errorf("failed to update last processed ID: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// applyUpsert handles upsert operations consistent with server logic
func (e *localCache) applyUpsert(ctx context.Context, tx *sql.Tx, entry Oplog, pkValue string) error {
	if e.details == nil {
		return fmt.Errorf("entity details are not available")
	}

	if entry.Data == nil {
		return fmt.Errorf("missing data for oplog entry")
	}

	// Build column names and placeholders for INSERT OR REPLACE
	columns := []string{pkColumn}
	placeholders := []string{"?"}
	values := []any{pkValue}

	// Process data array according to shape columns
	for i, column := range e.details.Shape.Columns {
		if i < len(entry.Data) {
			columns = append(columns, quoteIdentifier(column.Name))
			placeholders = append(placeholders, "?")
			encodedValue, err := encodeValue(entry.Data[i])
			if err != nil {
				return fmt.Errorf("failed to encode value for column %s: %w", column.Name, err)
			}
			values = append(values, encodedValue)
		}
	}

	// Build and execute INSERT OR REPLACE query
	query := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		dataTableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := tx.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to execute upsert query: %w", err)
	}

	return nil
}

// applyDelete handles delete operations consistent with server logic
func (e *localCache) applyDelete(ctx context.Context, tx *sql.Tx, pkValue string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", dataTableName, pkColumn)
	_, err := tx.ExecContext(ctx, query, pkValue)
	if err != nil {
		return fmt.Errorf("failed to execute delete query: %w", err)
	}
	return nil
}

// setLastProcessedID updates the last processed ID in metadata table
func (e *localCache) setLastProcessedID(ctx context.Context, tx *sql.Tx, lastProcessedID int64) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
		metadataTableName,
	)
	_, err := tx.ExecContext(ctx, query, maxOplogId, strconv.FormatInt(lastProcessedID, 10))
	return err
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func encodeValue(value any) (any, error) {
	switch value := value.(type) {
	case int, int8, int16, int32, int64, float32, float64, string:
		return value, nil
	case bool:
		if value {
			return 1, nil
		}
		return 0, nil
	default:
		return json.Marshal(value)
	}
}
