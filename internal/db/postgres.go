package db

import (
	"context"
	"fmt"
)

// GetOplogEntriesMultipleCollections implements OplogRepository
func (db *PostgresDatabase) GetOplogEntriesMultipleCollections(ctx context.Context, collections []string, afterID int64, limit int) ([]OplogEntry, error) {
	return GetOplogEntriesMultipleCollections(ctx, db.pool, collections, afterID, limit)
}

func GetOplogEntriesMultipleCollections(ctx context.Context, pool PostgresPool, collections []string, afterID int64, limit int) ([]OplogEntry, error) {
	if len(collections) == 0 {
		return []OplogEntry{}, nil
	}

	sql := `
        SELECT id, operation, doc_id, created_at, collection, doc, version
        FROM oplog
        WHERE collection = ANY($1) AND id > $2
        ORDER BY id ASC
        LIMIT $3`

	rows, err := pool.Query(ctx, sql, collections, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query oplog entries for collections %v after id %d: %w",
			collections, afterID, err)
	}
	defer rows.Close()

	var entries []OplogEntry
	for rows.Next() {
		var entry OplogEntry
		var rawDocData []byte

		if err := rows.Scan(
			&entry.ID, &entry.Operation, &entry.DocID, &entry.CreatedAt, &entry.Collection, &rawDocData, &entry.Version,
		); err != nil {
			return nil, fmt.Errorf("failed to scan oplog entry row for multiple collections: %w", err)
		}

		if entry.Operation == "UPSERT" {
			entry.Doc = rawDocData
		} else {
			entry.Doc = nil
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating oplog entry rows for multiple collections: %w", err)
	}
	return entries, nil
}
