package follower

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/nrjais/emcache/internal/db"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const metadataTableName = "_emcache_metadata"
const lastAppliedIdxKey = "last_applied_oplog_idx"
const dbVersionKey = "db_version"
const dataTableName = "_emcache_data"

var ErrNotFound = errors.New("record not found")

func GetCollectionDBPath(collectionName string, sqliteBaseDir string, version int) string {
	fileName := fmt.Sprintf("%s_v%d.sqlite", collectionName, version)
	return filepath.Join(sqliteBaseDir, fileName)
}

func openCollectionDB(collectionName string, sqliteBaseDir string, version int) (*sqlite.Conn, error) {
	if sqliteBaseDir == "" {
		return nil, fmt.Errorf("sqlite base directory cannot be empty")
	}
	if version <= 0 {
		return nil, fmt.Errorf("database version must be positive")
	}
	if err := os.MkdirAll(sqliteBaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for SQLite DBs at %s: %w", sqliteBaseDir, err)
	}
	dbPath := GetCollectionDBPath(collectionName, sqliteBaseDir, version)

	conn, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenCreate, sqlite.OpenWAL, sqlite.OpenURI)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite conn for collection %s at %s: %w", collectionName, dbPath, err)
	}

	schemaSQL := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    `, metadataTableName)

	err = sqlitex.ExecuteTransient(conn, schemaSQL, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create metadata table in %s: %w", dbPath, err)
	}

	if err := ensureCollectionTable(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ensure data table in %s: %w", dbPath, err)
	}

	log.Printf("[%s] Opened SQLite Conn: %s (Version: %d)", collectionName, dbPath, version)
	return conn, nil
}

func GetOrResetLocalDBVersion(conn *sqlite.Conn, version int) (bool, error) {
	var storedVersion int
	var found bool
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)

	err := sqlitex.ExecuteTransient(conn, query, &sqlitex.ExecOptions{
		Args: []any{dbVersionKey},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			value := stmt.ColumnText(0)
			if value == "" {
				return nil
			}
			var scanErr error
			storedVersion, scanErr = strconv.Atoi(value)
			if scanErr != nil {
				log.Printf("Error parsing stored local DB version '%s': %v", value, scanErr)
				return nil
			}
			found = true
			return nil
		},
	})

	if err != nil {
		log.Printf("Error querying local DB version: %v", err)
		return false, fmt.Errorf("error querying local DB version: %w", err)
	}

	if !found {
		return false, setLocalDBVersion(conn, version)
	}

	if version != storedVersion {
		log.Printf("Version mismatch in existing file (found %d, expected %d), resetting to %d", storedVersion, version, version)
		if err := sqlitex.ExecuteTransient(conn, fmt.Sprintf("DELETE FROM %s WHERE key = ?", metadataTableName), &sqlitex.ExecOptions{Args: []any{lastAppliedIdxKey}}); err != nil {
			log.Printf("Failed to clear last applied index during version reset: %v", err)
		}
		return true, setLocalDBVersion(conn, version)
	}

	return false, nil
}

func setLocalDBVersion(conn *sqlite.Conn, version int) error {
	sql := fmt.Sprintf(`
        INSERT INTO %s (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value;
    `, metadataTableName)
	args := []any{
		dbVersionKey,
		fmt.Sprintf("%d", version),
	}
	err := sqlitex.ExecuteTransient(conn, sql, &sqlitex.ExecOptions{Args: args})
	if err != nil {
		return fmt.Errorf("failed to set local DB version to %d: %w", version, err)
	}
	return nil
}

func getLastAppliedOplogIndex(conn *sqlite.Conn) (int64, error) {
	var idx int64
	var found bool
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)

	err := sqlitex.ExecuteTransient(conn, query, &sqlitex.ExecOptions{
		Args: []any{lastAppliedIdxKey},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			value := stmt.ColumnText(0)
			if value == "" {
				return nil
			}
			var scanErr error
			idx, scanErr = strconv.ParseInt(value, 10, 64)
			if scanErr != nil {
				log.Printf("Failed to parse stored oplog index '%s': %v. Treating as 0.", value, scanErr)
				return nil
			}
			found = true
			return nil
		},
	})

	if err != nil {
		return 0, fmt.Errorf("failed to query last applied oplog index: %w", err)
	}
	if !found {
		return 0, nil
	}
	return idx, nil
}

func setLastAppliedOplogIndex(conn *sqlite.Conn, index int64) error {
	sql := fmt.Sprintf(`
        INSERT INTO %s (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value;
    `, metadataTableName)
	args := []any{
		lastAppliedIdxKey,
		fmt.Sprintf("%d", index),
	}
	err := sqlitex.ExecuteTransient(conn, sql, &sqlitex.ExecOptions{Args: args})
	if err != nil {
		return fmt.Errorf("failed to set last applied oplog index to %d: %w", index, err)
	}
	return nil
}

func applyOplogEntry(conn *sqlite.Conn, entry db.OplogEntry) error {
	if entry.Operation == "UPSERT" {
		sql := fmt.Sprintf(`
            INSERT INTO %s (_id, source)
            VALUES (?, ?)
            ON CONFLICT(_id) DO UPDATE SET source = excluded.source;
        `, dataTableName)
		args := []any{
			entry.DocID,
			entry.Doc,
		}
		err := sqlitex.ExecuteTransient(conn, sql, &sqlitex.ExecOptions{Args: args})
		if err != nil {
			return fmt.Errorf("failed to execute upsert for doc %s: %w", entry.DocID, err)
		}
	} else if entry.Operation == "DELETE" {
		sql := fmt.Sprintf("DELETE FROM %s WHERE _id = ?", dataTableName)
		args := []any{entry.DocID}
		err := sqlitex.ExecuteTransient(conn, sql, &sqlitex.ExecOptions{Args: args})
		if err != nil {
			return fmt.Errorf("failed to execute delete for doc %s: %w", entry.DocID, err)
		}
	} else {
		return fmt.Errorf("unknown oplog operation: %s", entry.Operation)
	}
	return nil
}

func ensureCollectionTable(conn *sqlite.Conn) error {
	tableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			_id TEXT PRIMARY KEY,
			source BLOB
		);
	`, dataTableName)
	return sqlitex.ExecuteTransient(conn, tableSQL, nil)
}
