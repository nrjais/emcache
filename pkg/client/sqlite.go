package client

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/protobuf/types/known/structpb"
)

const metadataTableName = "metadata"
const lastAppliedIdxKey = "last_applied_oplog_idx"
const dbVersionKey = "db_version"

type sqliteCollection struct {
	db      SQLiteDBInterface
	version int32
	details *pb.Collection
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

func initMetadataTable(db SQLiteDBInterface) error {
	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, value TEXT)`, metadataTableName)
	_, err := db.Exec(createTableSQL)
	return err
}

func (sc *sqliteCollection) close() error {
	if sc.db != nil {
		return sc.db.Close()
	}
	return nil
}

func (sc *sqliteCollection) getLastAppliedOplogIndex(ctx context.Context) (int64, error) {
	if sc.db == nil {
		return 0, fmt.Errorf("database connection is not available")
	}

	var idx int64
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	row := sc.db.QueryRowContext(ctx, query, lastAppliedIdxKey)

	var value string
	if err := row.Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // Default to 0 if no record exists
		}
		return 0, fmt.Errorf("failed to get last applied oplog index: %w", err)
	}

	if _, err := fmt.Sscanf(value, "%d", &idx); err != nil {
		return 0, fmt.Errorf("failed to parse last applied oplog index: %w", err)
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

	version, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("failed to parse database version: %w", err)
	}

	return int32(version), nil
}

func (sc *sqliteCollection) applyOplogEntries(ctx context.Context, entries []*pb.OplogEntry) error {
	if sc.db == nil {
		return fmt.Errorf("database connection is not available")
	}

	tx, err := sc.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	const dataTableName = "data"
	var lastIndex int64

	for _, entry := range entries {
		const pkColumn = "id"
		pkValue := entry.Id
		lastIndex = entry.Index

		if sc.version != entry.Version {
			// Skip entries with a version that is different from the current collection's version
			// TODO: Add realtime swap of the database file here to latest version
			continue
		}

		switch entry.Operation {
		case pb.OplogEntry_UPSERT:
			columns := []string{quoteIdentifier(pkColumn)}
			values := []any{pkValue}
			placeholders := []string{"?"}

			if sc.details == nil || sc.details.Shape == nil {
				return fmt.Errorf("collection details are not available")
			}

			for _, column := range sc.details.Shape.Columns {
				colName := column.Name
				if entry.Data == nil || entry.Data.Fields == nil {
					return fmt.Errorf("missing data for column %s", colName)
				}
				valProto := entry.Data.Fields[colName]
				columns = append(columns, quoteIdentifier(colName))
				values = append(values, valueFromProto(valProto))
				placeholders = append(placeholders, "?")
			}

			sql := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
				quoteIdentifier(dataTableName),
				strings.Join(columns, ", "),
				strings.Join(placeholders, ", "))

			_, err = tx.ExecContext(ctx, sql, values...)
			if err != nil {
				return fmt.Errorf("failed to execute UPSERT oplog entry index %d for ID %s (%s): %w", entry.Index, pkValue, sql, err)
			}

		case pb.OplogEntry_DELETE:
			sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdentifier(dataTableName), quoteIdentifier(pkColumn))

			_, err = tx.ExecContext(ctx, sql, pkValue)
			if err != nil {
				return fmt.Errorf("failed to execute DELETE oplog entry index %d for ID %s (%s): %w", entry.Index, pkValue, sql, err)
			}
		default:
			// Ignore unknown operation types
		}
	}

	// Update the last applied oplog index
	if len(entries) > 0 {
		updateSQL := fmt.Sprintf(`INSERT OR REPLACE INTO %s (key, value) VALUES (?, ?)`, metadataTableName)
		_, err = tx.ExecContext(ctx, updateSQL, lastAppliedIdxKey, fmt.Sprintf("%d", lastIndex))
		if err != nil {
			return fmt.Errorf("failed to update last applied oplog index to %d: %w", lastIndex, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction after applying oplog entries: %w", err)
	}

	return nil
}

func (sc *sqliteCollection) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if sc.db == nil {
		return nil, fmt.Errorf("database connection is not available")
	}

	rows, err := sc.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return rows, nil
}

func valueFromProto(v *structpb.Value) any {
	switch k := v.Kind.(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_NumberValue:
		return k.NumberValue
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_BoolValue:
		if k.BoolValue {
			return 1
		}
		return 0
	case *structpb.Value_StructValue:
		jsonBytes, err := k.StructValue.MarshalJSON()
		if err != nil {
			// TODO: Log or handle error
			return nil
		}
		return jsonBytes
	case *structpb.Value_ListValue:
		jsonBytes, err := k.ListValue.MarshalJSON()
		if err != nil {
			// TODO: Log or handle error
			return nil
		}
		return jsonBytes
	default:
		return nil
	}
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
