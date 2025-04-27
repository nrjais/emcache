package follower

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nrjais/emcache/internal/db"
)

const metadataTableName = "_emcache_metadata"
const lastAppliedIdxKey = "last_applied_oplog_idx"
const dbVersionKey = "db_version"
const dataTableName = "_emcache_data"

func GetCollectionDBPath(collectionName string, sqliteBaseDir string, version int) string {
	fileName := fmt.Sprintf("%s_v%d.sqlite", collectionName, version)
	return filepath.Join(sqliteBaseDir, fileName)
}

func openCollectionDB(collectionName string, sqliteBaseDir string, version int) (*sql.DB, string, error) {
	if sqliteBaseDir == "" {
		return nil, "", fmt.Errorf("sqlite base directory cannot be empty")
	}
	if version <= 0 {
		return nil, "", fmt.Errorf("database version must be positive")
	}
	if err := os.MkdirAll(sqliteBaseDir, 0755); err != nil {
		return nil, "", fmt.Errorf("failed to create directory for SQLite DBs at %s: %w", sqliteBaseDir, err)
	}
	dbPath := GetCollectionDBPath(collectionName, sqliteBaseDir, version)

	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_foreign_keys=on", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, dbPath, fmt.Errorf("failed to open sqlite db for collection %s at %s: %w", collectionName, dbPath, err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	schemaSQL := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    `, metadataTableName)
	_, err = db.Exec(schemaSQL)
	if err != nil {
		db.Close()
		return nil, dbPath, fmt.Errorf("failed to create metadata table in %s: %w", dbPath, err)
	}

	log.Printf("[%s] Opened SQLite DB: %s (Version: %d)", collectionName, dbPath, version)
	return db, dbPath, nil
}

func GetLocalDBVersion(dbConn *sql.DB) (int, bool) {
	var value string
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	err := dbConn.QueryRow(query, dbVersionKey).Scan(&value)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("Error querying local DB version: %v", err)
		}
		return 0, false
	}

	var version int
	_, err = fmt.Sscan(value, &version)
	if err != nil {
		log.Printf("Error parsing stored local DB version '%s': %v", value, err)
		return 0, false
	}
	return version, true
}

func setLocalDBVersion(dbConn *sql.DB, version int) error {
	sql := fmt.Sprintf(`
        INSERT INTO %s (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value;
    `, metadataTableName)
	_, err := dbConn.Exec(sql, dbVersionKey, fmt.Sprintf("%d", version))
	if err != nil {
		return fmt.Errorf("failed to set local DB version to %d: %w", version, err)
	}
	log.Printf("Set local DB version to %d", version)
	return nil
}

func getLastAppliedOplogIndex(db *sql.DB) (int64, error) {
	var value string
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	err := db.QueryRow(query, lastAppliedIdxKey).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to query last applied oplog index: %w", err)
	}

	var idx int64
	_, err = fmt.Sscan(value, &idx)
	if err != nil {
		return 0, fmt.Errorf("failed to parse stored oplog index '%s': %w", value, err)
	}
	return idx, nil
}

func setLastAppliedOplogIndex(tx *sql.Tx, index int64) error {
	sql := fmt.Sprintf(`
        INSERT INTO %s (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value;
    `, metadataTableName)
	_, err := tx.Exec(sql, lastAppliedIdxKey, fmt.Sprintf("%d", index))
	if err != nil {
		return fmt.Errorf("failed to set last applied oplog index to %d: %w", index, err)
	}
	return nil
}

func applyOplogEntry(tx *sql.Tx, collectionName string, entry db.OplogEntry) error {
	if err := ensureCollectionTable(tx); err != nil {
		return fmt.Errorf("failed to ensure collection table %s: %w", collectionName, err)
	}

	if entry.Operation == "UPSERT" {
		sql := fmt.Sprintf(`
            INSERT INTO %s (_id, source)
            VALUES (?, ?)
            ON CONFLICT(_id) DO UPDATE SET source = excluded.source;
        `, dataTableName)

		_, err := tx.Exec(sql, entry.DocID, entry.Doc)
		if err != nil {
			return fmt.Errorf("failed to execute upsert for doc %s: %w", entry.DocID, err)
		}

	} else if entry.Operation == "DELETE" {
		sql := fmt.Sprintf("DELETE FROM %s WHERE _id = ?", dataTableName)
		_, err := tx.Exec(sql, entry.DocID)
		if err != nil {
			return fmt.Errorf("failed to execute delete for doc %s: %w", entry.DocID, err)
		}
	} else {
		return fmt.Errorf("unknown oplog operation: %s", entry.Operation)
	}

	return nil
}

func VerifyCollectionTableSchema(db *sql.DB, collectionName string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s);", dataTableName))
	if err != nil {
		log.Printf("Error querying table info for %s (table might be missing): %v", dataTableName, err)
		return false, nil
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name string
		var typeName string
		var notnull bool
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typeName, &notnull, &dfltValue, &pk); err != nil {
			return false, fmt.Errorf("failed to scan table info row for %s: %w", dataTableName, err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("row error during table info query for %s: %w", dataTableName, err)
	}

	if !columns["_id"] || !columns["source"] {
		log.Printf("Table %s exists but is missing required columns (_id or source). Columns found: %v", dataTableName, columns)
		return false, nil
	}

	return true, nil
}

func ensureCollectionTable(tx *sql.Tx) error {
	tableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			_id TEXT PRIMARY KEY,
			source TEXT
		);
	`, dataTableName)
	_, err := tx.Exec(tableSQL)
	return err
}

func getLastAppliedOplogIndexTx(tx *sql.Tx) (int64, error) {
	var value string
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = ?", metadataTableName)
	err := tx.QueryRow(query, lastAppliedIdxKey).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to query last applied oplog index in transaction: %w", err)
	}

	var idx int64
	_, err = fmt.Sscan(value, &idx)
	if err != nil {
		return 0, fmt.Errorf("failed to parse stored oplog index '%s' in transaction: %w", value, err)
	}
	return idx, nil
}
