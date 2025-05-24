package db

import (
	"context"
	"encoding/json"

	"github.com/nrjais/emcache/internal/shape"
)

// GetOplogEntriesGlobal implements OplogRepository
func (db *PostgresDatabase) GetOplogEntriesGlobal(ctx context.Context, afterID int64, limit int) ([]OplogEntry, error) {
	return GetOplogEntriesGlobal(ctx, db.pool, afterID, limit)
}

// InsertOplogEntry implements OplogRepository
func (db *PostgresDatabase) InsertOplogEntry(ctx context.Context, entry OplogEntry) (int64, error) {
	return InsertOplogEntry(ctx, db.pool, entry)
}

// AddReplicatedCollection implements CollectionRepository
func (db *PostgresDatabase) AddReplicatedCollection(ctx context.Context, name string, collShape shape.Shape) error {
	shapeJSON, err := json.Marshal(collShape)
	if err != nil {
		return err
	}
	return AddReplicatedCollection(ctx, db.pool, name, shapeJSON)
}

// RemoveReplicatedCollection implements CollectionRepository
func (db *PostgresDatabase) RemoveReplicatedCollection(ctx context.Context, name string) error {
	return RemoveReplicatedCollection(ctx, db.pool, name)
}

// GetReplicatedCollection implements CollectionRepository
func (db *PostgresDatabase) GetReplicatedCollection(ctx context.Context, name string) (ReplicatedCollection, bool, error) {
	collections, err := GetAllReplicatedCollectionsWithShapes(ctx, db.pool)
	if err != nil {
		return ReplicatedCollection{}, false, err
	}
	for _, coll := range collections {
		if coll.CollectionName == name {
			return coll, true, nil
		}
	}
	return ReplicatedCollection{}, false, nil
}

// ListReplicatedCollections implements CollectionRepository
func (db *PostgresDatabase) ListReplicatedCollections(ctx context.Context) ([]string, error) {
	return ListReplicatedCollections(ctx, db.pool)
}

// GetAllCurrentCollectionVersions implements CollectionRepository
func (db *PostgresDatabase) GetAllCurrentCollectionVersions(ctx context.Context) ([]CollectionVersion, error) {
	return GetAllCurrentCollectionVersions(ctx, db.pool)
}

// UpdateCollectionShape implements CollectionRepository
func (db *PostgresDatabase) UpdateCollectionShape(ctx context.Context, name string, newShape shape.Shape, newVersion int) error {
	shapeJSON, err := json.Marshal(newShape)
	if err != nil {
		return err
	}
	sql := `UPDATE replicated_collections SET shape = $1, current_version = $2 WHERE collection_name = $3`
	_, err = db.pool.Exec(ctx, sql, shapeJSON, newVersion, name)
	return err
}

// GetResumeToken implements ResumeTokenRepository
func (db *PostgresDatabase) GetResumeToken(ctx context.Context, collection string) (string, bool, error) {
	return GetResumeToken(ctx, db.pool, collection)
}

// UpsertResumeToken implements ResumeTokenRepository
func (db *PostgresDatabase) UpsertResumeToken(ctx context.Context, collection string, token string) error {
	return UpsertResumeToken(ctx, db.pool, collection, token)
}
