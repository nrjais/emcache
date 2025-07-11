package emcache

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

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

// EntityInterface manages individual entity state and operations
type EntityInterface interface {
	GetLastAppliedOplogIndex(ctx context.Context) (int64, error)
	ApplyOplogEntries(ctx context.Context, entries []Oplog) error
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Close() error
}

const (
	metadataTableName = "metadata"
	lastAppliedIdxKey = "last_processed_id" // Consistent with server metadata key
	dbVersionKey      = "db_version"
)

type sqliteEntity struct {
	db      SQLiteDBInterface
	version int32
	details *Entity
}

func openSQLiteDB(dbPath string) (SQLiteDBInterface, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db (%s): %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)

	// Wrap the *sql.DB in an adapter
	adapter := &SQLiteDBAdapter{db: db}

	// Initialize metadata table if it doesn't exist
	if err := initMetadataTable(adapter); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize metadata table: %w", err)
	}

	return adapter, nil
}

// initMetadataTable creates the metadata table for storing entity state
func initMetadataTable(db SQLiteDBInterface) error {
	// Use STRICT table consistent with server metadata table
	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, value ANY NOT NULL) STRICT`, metadataTableName)
	_, err := db.Exec(createTableSQL)
	return err
}

// close closes the SQLite database connection
func (sc *sqliteEntity) close() error {
	if sc.db != nil {
		return sc.db.Close()
	}
	return nil
}

// getLastAppliedOplogIndex retrieves the last processed oplog ID from metadata
func (sc *sqliteEntity) getLastAppliedOplogIndex(ctx context.Context) (int64, error) {
	if sc.db == nil {
		return 0, fmt.Errorf("database connection is not available")
	}

	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	row := sc.db.QueryRowContext(ctx, query, lastAppliedIdxKey)

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

// getDbVersion retrieves the stored database version from metadata table
func getDbVersion(ctx context.Context, db SQLiteDBInterface) (int32, error) {
	if db == nil {
		return 0, fmt.Errorf("database connection is not available")
	}

	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	row := db.QueryRowContext(ctx, query, dbVersionKey)

	var value string
	if err := row.Scan(&value); err != nil {
		return 0, fmt.Errorf("failed to get database version: %w", err)
	}

	version, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse database version: %w", err)
	}

	return int32(version), nil
}

// applyOplogEntries applies a batch of oplog entries to the SQLite database
// consistent with server LocalCache logic
func (sc *sqliteEntity) applyOplogEntries(ctx context.Context, entries []Oplog) error {
	if sc.db == nil {
		return fmt.Errorf("database connection is not available")
	}

	if len(entries) == 0 {
		return nil
	}

	tx, err := sc.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	const (
		dataTableName = "data"
		pkColumn      = "id"
	)
	var maxProcessedID int64

	// Process oplogs in batch, consistent with server LocalCache logic
	for _, entry := range entries {
		if entry.ID > maxProcessedID {
			maxProcessedID = entry.ID
		}

		switch entry.Operation {
		case OperationUpsert:
			err = sc.applyUpsert(ctx, tx, entry, dataTableName, pkColumn, entry.DocID)
			if err != nil {
				return fmt.Errorf("failed to apply upsert for doc_id %s: %w", entry.DocID, err)
			}

		case OperationDelete:
			err = sc.applyDelete(ctx, tx, entry, dataTableName, pkColumn, entry.DocID)
			if err != nil {
				return fmt.Errorf("failed to apply delete for doc_id %s: %w", entry.DocID, err)
			}
		default:
			// Ignore unknown operation types
		}
	}

	// Update the last processed ID (consistent with server metadata management)
	err = sc.setLastProcessedID(ctx, tx, maxProcessedID)
	if err != nil {
		return fmt.Errorf("failed to update last processed ID: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// applyUpsert handles upsert operations consistent with server logic
func (sc *sqliteEntity) applyUpsert(ctx context.Context, tx TxInterface, entry Oplog, dataTableName, pkColumn, pkValue string) error {
	if sc.details == nil {
		return fmt.Errorf("entity details are not available")
	}

	if entry.Data == nil {
		return fmt.Errorf("missing data for oplog entry")
	}

	// Build query consistent with server mapper logic
	columns := []string{quoteIdentifier(pkColumn)}
	values := []any{pkValue}
	placeholders := []string{"?"}

	// Use array indexing consistent with server logic (data is array indexed by column position)
	for i, column := range sc.details.Shape.Columns {
		colName := column.Name

		// Get value from array at index i
		var val interface{}
		if i < len(entry.Data) {
			val = entry.Data[i]
		} else {
			val = nil // Use nil if array is shorter than expected
		}

		columns = append(columns, quoteIdentifier(colName))
		values = append(values, val)
		placeholders = append(placeholders, "?")
	}

	// Use INSERT OR REPLACE consistent with server mapper
	sql := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		quoteIdentifier(dataTableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tx.ExecContext(ctx, sql, values...)
	if err != nil {
		return fmt.Errorf("failed to execute upsert query: %s, error: %w", sql, err)
	}

	return nil
}

// applyDelete handles delete operations consistent with server logic
func (sc *sqliteEntity) applyDelete(ctx context.Context, tx TxInterface, entry Oplog, dataTableName, pkColumn, pkValue string) error {
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdentifier(dataTableName), quoteIdentifier(pkColumn))

	_, err := tx.ExecContext(ctx, sql, pkValue)
	if err != nil {
		return fmt.Errorf("failed to execute delete query: %s, error: %w", sql, err)
	}

	return nil
}

// setLastProcessedID updates the last processed ID, consistent with server metadata management
func (sc *sqliteEntity) setLastProcessedID(ctx context.Context, tx TxInterface, lastProcessedID int64) error {
	updateSQL := fmt.Sprintf(`INSERT OR REPLACE INTO %s (key, value) VALUES (?, ?)`, metadataTableName)
	_, err := tx.ExecContext(ctx, updateSQL, lastAppliedIdxKey, fmt.Sprintf("%d", lastProcessedID))
	if err != nil {
		return fmt.Errorf("failed to update last processed ID to %d: %w", lastProcessedID, err)
	}
	return nil
}

// query executes a SQL query against the entity's database
func (sc *sqliteEntity) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if sc.db == nil {
		return nil, fmt.Errorf("database connection is not available")
	}

	rows, err := sc.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

func quoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", name)
}
