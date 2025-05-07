package follower

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/shape"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const metadataTableName = "metadata"
const lastAppliedIdxKey = "last_applied_oplog_idx"
const dbVersionKey = "db_version"
const dataTableName = "data"

var ErrNotFound = errors.New("record not found")

func GetCollectionDBPath(collectionName string, sqliteBaseDir string, version int) string {
	collDir := filepath.Join(sqliteBaseDir, "replicas")
	dirName := fmt.Sprintf("%s_v%d", collectionName, version)
	return filepath.Join(collDir, dirName, "db.sqlite")
}

func openCollectionDB(collectionName, sqliteBaseDir, dbPath string, version int, collShape shape.Shape) (*sqlite.Conn, error) {
	if sqliteBaseDir == "" {
		return nil, fmt.Errorf("sqlite base directory cannot be empty")
	}
	if version <= 0 {
		return nil, fmt.Errorf("database version must be positive")
	}

	collDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(collDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for collection %s at %s: %w", collectionName, collDir, err)
	}

	conn, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenCreate, sqlite.OpenWAL)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite conn for collection %s at %s: %w", collectionName, dbPath, err)
	}

	if err := initMetaTable(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize metadata table in %s: %w", dbPath, err)
	}

	if err := ensureCollectionTableAndIndexes(conn, collShape); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ensure data table/indexes in %s: %w", dbPath, err)
	}

	slog.Info("SQLite connection opened",
		"collection", collectionName,
		"path", dbPath,
		"version", version)
	return conn, nil
}

func GetOrResetLocalDBVersion(conn *sqlite.Conn, version int) (bool, error) {
	var storedVersion int
	var found bool
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)

	err := sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		Args: []any{dbVersionKey},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			value := stmt.ColumnText(0)
			if value == "" {
				return nil
			}
			var scanErr error
			storedVersion, scanErr = strconv.Atoi(value)
			if scanErr != nil {
				slog.Error("Failed to parse stored local DB version",
					"value", value,
					"error", scanErr)
				return nil
			}
			found = true
			return nil
		},
	})

	if err != nil {
		slog.Error("Failed to query local DB version", "error", err)
		return false, fmt.Errorf("error querying local DB version: %w", err)
	}

	if !found {
		return false, setLocalDBVersion(conn, version)
	}

	if version != storedVersion {
		slog.Warn("Version mismatch in existing file",
			"found_version", storedVersion,
			"expected_version", version)
		if err := sqlitex.Execute(conn, fmt.Sprintf("DELETE FROM %s WHERE key = ?", metadataTableName), &sqlitex.ExecOptions{Args: []any{lastAppliedIdxKey}}); err != nil {
			slog.Error("Failed to clear last applied index during version reset", "error", err)
		}
		return true, setLocalDBVersion(conn, version)
	}

	return false, nil
}

func setLocalDBVersion(conn *sqlite.Conn, version int) error {
	sql := fmt.Sprintf(`
        INSERT INTO %s (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value`, metadataTableName)
	args := []any{
		dbVersionKey,
		fmt.Sprintf("%d", version),
	}
	err := sqlitex.Execute(conn, sql, &sqlitex.ExecOptions{Args: args})
	if err != nil {
		return fmt.Errorf("failed to set local DB version to %d: %w", version, err)
	}
	return nil
}

func initMetaTable(conn *sqlite.Conn) error {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, value ANY) STRICT", metadataTableName)
	return sqlitex.Execute(conn, sql, nil)
}

func getLastAppliedOplogIndex(conn *sqlite.Conn) (int64, error) {
	var idx int64
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	err := sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		Args: []any{lastAppliedIdxKey},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			idx = stmt.ColumnInt64(0)
			return nil
		},
	})

	if err != nil {
		return 0, fmt.Errorf("failed to query last applied oplog index: %w", err)
	}
	return idx, nil
}

func setLastAppliedOplogIndex(conn *sqlite.Conn, index int64) error {
	sql := fmt.Sprintf(`
        INSERT INTO %s (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value`, metadataTableName)
	args := []any{
		lastAppliedIdxKey,
		fmt.Sprintf("%d", index),
	}
	err := sqlitex.Execute(conn, sql, &sqlitex.ExecOptions{Args: args})
	if err != nil {
		return fmt.Errorf("failed to set last applied oplog index to %d: %w", index, err)
	}
	return nil
}

func mapShapeTypeToSQLite(dt shape.DataType) string {
	switch dt {
	case shape.Integer:
		return "INTEGER"
	case shape.Number:
		return "REAL"
	case shape.Bool:
		return "INTEGER"
	case shape.Text:
		return "TEXT"
	case shape.JSONB:
		return "JSONB"
	case shape.Any:
		return "ANY"
	default:
		return "ANY"
	}
}

func quoteIdentifier(ident string) string {
	return `"` + ident + `"`
}

func ensureCollectionTableAndIndexes(conn *sqlite.Conn, collShape shape.Shape) error {
	var colDefs []string
	idColDef := quoteIdentifier("id") + " TEXT PRIMARY KEY"
	colDefs = append(colDefs, idColDef)

	colNames := make(map[string]struct{})
	colNames["id"] = struct{}{}

	for _, col := range collShape.Columns {
		if col.Name == "id" {
			return fmt.Errorf("column name 'id' is reserved and implicitly created from source '_id'")
		}
		if _, exists := colNames[col.Name]; exists {
			return fmt.Errorf("duplicate column name defined in shape: %s", col.Name)
		}
		colDef := fmt.Sprintf("%s %s", quoteIdentifier(col.Name), mapShapeTypeToSQLite(col.Type))
		colDefs = append(colDefs, colDef)
		colNames[col.Name] = struct{}{}
	}

	tableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s) STRICT;`,
		quoteIdentifier(dataTableName),
		strings.Join(colDefs, ", "))
	if err := sqlitex.Execute(conn, tableSQL, nil); err != nil {
		return fmt.Errorf("failed to execute create table statement: %w\nSQL: %s", err, tableSQL)
	}

	for i, indexDef := range collShape.Indexes {
		var quotedIndexCols []string
		for _, colName := range indexDef.Columns {
			if _, exists := colNames[colName]; !exists {
				return fmt.Errorf("index %d references non-existent column: %s", i, colName)
			}
			quotedIndexCols = append(quotedIndexCols, quoteIdentifier(colName))
		}
		if len(quotedIndexCols) == 0 {
			continue
		}

		indexName := fmt.Sprintf("idx_%s_%d", dataTableName, i)
		indexSQL := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s);",
			quoteIdentifier(indexName),
			quoteIdentifier(dataTableName),
			strings.Join(quotedIndexCols, ", "))

		if err := sqlitex.Execute(conn, indexSQL, nil); err != nil {
			return fmt.Errorf("failed to execute create index statement for index %d: %w\nSQL: %s", i, err, indexSQL)
		}
	}

	return nil
}

func applyOplogEntry(conn *sqlite.Conn, entry db.OplogEntry, collShape shape.Shape) error {
	idColNameQuoted := quoteIdentifier("id")

	if entry.Operation == "UPSERT" {
		if entry.Doc == nil {
			slog.Warn("UPSERT operation missing document data",
				"doc_id", entry.DocID,
				"collection", entry.Collection)
			return nil
		}

		var data map[string]any
		if err := json.Unmarshal(entry.Doc, &data); err != nil {
			return fmt.Errorf("failed to unmarshal transformed JSON for doc %s, collection %s: %w", entry.DocID, entry.Collection, err)
		}

		var cols []string
		var placeholders []string
		var args []any

		cols = append(cols, idColNameQuoted)
		placeholders = append(placeholders, "?")
		args = append(args, entry.DocID)

		for _, shapeCol := range collShape.Columns {
			cols = append(cols, quoteIdentifier(shapeCol.Name))
			placeholders = append(placeholders, "?")
			args = append(args, data[shapeCol.Name])
		}

		sql := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
			quoteIdentifier(dataTableName),
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "))

		err := sqlitex.Execute(conn, sql, &sqlitex.ExecOptions{Args: args})
		if err != nil {
			return fmt.Errorf("failed to execute shaped upsert for doc %s, collection %s: %w", entry.DocID, entry.Collection, err)
		}
	} else if entry.Operation == "DELETE" {
		sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ?",
			quoteIdentifier(dataTableName),
			idColNameQuoted)
		args := []any{entry.DocID}
		err := sqlitex.Execute(conn, sql, &sqlitex.ExecOptions{Args: args})
		if err != nil {
			return fmt.Errorf("failed to execute shaped delete for doc %s, collection %s: %w", entry.DocID, entry.Collection, err)
		}
	} else {
		return fmt.Errorf("unknown oplog operation '%s' for doc %s, collection %s", entry.Operation, entry.DocID, entry.Collection)
	}
	return nil
}
