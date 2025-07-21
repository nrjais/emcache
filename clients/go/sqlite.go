package emcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

// Mode represents the current synchronization mode
type Mode int

const (
	ModeLive Mode = iota
	ModeSync
)

func (m Mode) String() string {
	switch m {
	case ModeLive:
		return "live"
	case ModeSync:
		return "sync"
	default:
		return "live"
	}
}

func (m Mode) TableName() string {
	switch m {
	case ModeLive:
		return dataTableName
	case ModeSync:
		return dataSyncTableName
	default:
		return dataTableName
	}
}

func ModeFromString(s string) Mode {
	switch s {
	case "live":
		return ModeLive
	case "sync":
		return ModeSync
	default:
		return ModeLive
	}
}

type LocalCache interface {
	DB() *sql.DB
	GetMaxOplogId(ctx context.Context) (int64, error)
	ApplyOplogs(ctx context.Context, oplogs []Oplog) error
	Close() error
	CurrentMode() Mode
}

const (
	metadataTableName = "metadata"
	maxOplogId        = "max_oplog_id"
	modeKey           = "mode"
)
const (
	dataTableName     = "data"
	dataSyncTableName = "data_sync"
	pkColumn          = "id"
)

type localCache struct {
	db      *sql.DB
	details *Entity
	mode    Mode
	modeMu  sync.RWMutex
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
	currentMode := e.CurrentMode()

	for _, entry := range oplogs {
		if entry.ID > maxProcessedID {
			maxProcessedID = entry.ID
		}

		switch entry.Operation {
		case OperationUpsert:
			err = e.applyUpsert(ctx, tx, entry, entry.DocID, currentMode)
			if err != nil {
				return fmt.Errorf("failed to apply upsert for doc_id %s: %w", entry.DocID, err)
			}

		case OperationDelete:
			err = e.applyDelete(ctx, tx, entry.DocID, currentMode)
			if err != nil {
				return fmt.Errorf("failed to apply delete for doc_id %s: %w", entry.DocID, err)
			}

		case OperationSyncStart:
			err = e.applySyncStart(ctx, tx)
			if err != nil {
				return fmt.Errorf("failed to apply sync start: %w", err)
			}
			currentMode = ModeSync

		case OperationSyncEnd:
			err = e.applySyncEnd(ctx, tx)
			if err != nil {
				return fmt.Errorf("failed to apply sync end: %w", err)
			}
			currentMode = ModeLive

		default:
			// Unknown operation, skip
		}
	}

	err = e.setLastProcessedID(ctx, tx, maxProcessedID)
	if err != nil {
		return fmt.Errorf("failed to update last processed ID: %w", err)
	}

	err = e.setMode(ctx, tx, currentMode)
	if err != nil {
		return fmt.Errorf("failed to update mode: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// applyUpsert handles upsert operations consistent with server logic
func (e *localCache) applyUpsert(ctx context.Context, tx *sql.Tx, entry Oplog, pkValue string, mode Mode) error {
	if e.details == nil {
		return fmt.Errorf("entity details are not available")
	}

	if entry.Data == nil {
		return fmt.Errorf("missing data for oplog entry")
	}

	tableName := mode.TableName()

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
		tableName,
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
func (e *localCache) applyDelete(ctx context.Context, tx *sql.Tx, pkValue string, mode Mode) error {
	tableName := mode.TableName()
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", tableName, pkColumn)
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

// CurrentMode returns the current cached mode
func (e *localCache) CurrentMode() Mode {
	e.modeMu.RLock()
	defer e.modeMu.RUnlock()
	return e.mode
}

// loadMode loads the current mode from the metadata table
func (e *localCache) loadMode(ctx context.Context) error {
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	row := e.db.QueryRowContext(ctx, query, modeKey)

	var value string
	if err := row.Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			// Default to live mode if no record exists
			e.modeMu.Lock()
			e.mode = ModeLive
			e.modeMu.Unlock()
			return nil
		}
		return fmt.Errorf("failed to load mode: %w", err)
	}

	mode := ModeFromString(value)
	e.modeMu.Lock()
	e.mode = mode
	e.modeMu.Unlock()
	return nil
}

// setMode updates the mode in both memory and database
func (e *localCache) setMode(ctx context.Context, tx *sql.Tx, mode Mode) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
		metadataTableName,
	)
	_, err := tx.ExecContext(ctx, query, modeKey, mode.String())
	if err != nil {
		return fmt.Errorf("failed to set mode: %w", err)
	}

	e.modeMu.Lock()
	e.mode = mode
	e.modeMu.Unlock()
	return nil
}

// applySyncStart handles sync start operation
func (e *localCache) applySyncStart(ctx context.Context, tx *sql.Tx) error {
	// Create data_sync table as a copy of data table
	createSyncTableQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s",
		dataSyncTableName, dataTableName,
	)
	_, err := tx.ExecContext(ctx, createSyncTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create sync table: %w", err)
	}

	return nil
}

// applySyncEnd handles sync end operation
func (e *localCache) applySyncEnd(ctx context.Context, tx *sql.Tx) error {
	// Drop the data table
	dropDataQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", dataTableName)
	_, err := tx.ExecContext(ctx, dropDataQuery)
	if err != nil {
		return fmt.Errorf("failed to drop data table: %w", err)
	}

	// Rename data_sync to data
	renameQuery := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", dataSyncTableName, dataTableName)
	_, err = tx.ExecContext(ctx, renameQuery)
	if err != nil {
		return fmt.Errorf("failed to rename sync table: %w", err)
	}

	return nil
}
