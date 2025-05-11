package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/shape"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var ErrCollectionNotFound = errors.New("not found in replicated_collections")
var ErrCollectionAlreadyExists = errors.New("collection already configured for replication")

func ConnectPostgres(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	slog.Info("Database connection established", "db", "PostgreSQL")
	return pool, nil
}

func ConnectMongo(ctx context.Context, mongoURL string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(mongoURL)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongo: %w", err)
	}

	ctxPing, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(ctxPing, readpref.Primary()); err != nil {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer disconnectCancel()
		if disconnectErr := client.Disconnect(disconnectCtx); disconnectErr != nil {
			slog.Error("Failed to disconnect from MongoDB after ping failure", "error", disconnectErr)
		}
		return nil, fmt.Errorf("failed to ping mongo: %w", err)
	}

	slog.Info("Database connection established", "db", "MongoDB")
	return client, nil
}

func GetResumeToken(ctx context.Context, pool *pgxpool.Pool, collection string) (string, bool, error) {
	var token string
	err := pool.QueryRow(ctx, "SELECT token FROM resume_tokens WHERE collection = $1", collection).Scan(&token)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return "", false, fmt.Errorf("failed to query resume token for collection %s: %w", collection, err)
	}
	return token, true, nil
}

func UpsertResumeToken(ctx context.Context, pool *pgxpool.Pool, collection string, token string) error {
	sql := `
        INSERT INTO resume_tokens (collection, token, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (collection)
        DO UPDATE SET token = EXCLUDED.token, updated_at = NOW()`
	_, err := pool.Exec(ctx, sql, collection, token)
	if err != nil {
		return fmt.Errorf("failed to upsert resume token for collection %s: %w", collection, err)
	}
	return nil
}

type OplogEntry struct {
	ID         int64
	Operation  string // "UPSERT" or "DELETE"
	DocID      string
	CreatedAt  time.Time
	Collection string
	Doc        []byte // Marshaled JSON data
	Version    int
}

func InsertOplogEntry(ctx context.Context, pool *pgxpool.Pool, entry OplogEntry) (int64, error) {
	sql := `
        INSERT INTO oplog (operation, doc_id, created_at, collection, doc, version)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id`
	var id int64
	err := pool.QueryRow(ctx, sql,
		entry.Operation, entry.DocID, entry.CreatedAt, entry.Collection, entry.Doc, entry.Version,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert oplog entry v%d for collection %s, doc %s: %w",
			entry.Version, entry.Collection, entry.DocID, err)
	}
	return id, nil
}

func GetOplogEntriesGlobal(ctx context.Context, pool *pgxpool.Pool, afterID int64, limit int) ([]OplogEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	sql := `
        SELECT id, operation, doc_id, created_at, collection, doc, version
        FROM oplog
        WHERE id > $1
        ORDER BY id ASC
        LIMIT $2`
	rows, err := pool.Query(ctx, sql, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query global oplog entries after id %d: %w", afterID, err)
	}
	defer rows.Close()

	var entries []OplogEntry
	for rows.Next() {
		var entry OplogEntry
		var rawDocData []byte

		if err := rows.Scan(
			&entry.ID, &entry.Operation, &entry.DocID, &entry.CreatedAt, &entry.Collection, &rawDocData, &entry.Version,
		); err != nil {
			return nil, fmt.Errorf("failed to scan global oplog entry row: %w", err)
		}

		if entry.Operation == "UPSERT" {
			entry.Doc = rawDocData
		} else {
			entry.Doc = nil
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating global oplog entry rows: %w", err)
	}
	return entries, nil
}

func IncrementCollectionVersion(ctx context.Context, pool *pgxpool.Pool, collectionName string) (int, error) {
	var newVersion int
	sql := `
        UPDATE replicated_collections
        SET current_version = current_version + 1
        WHERE collection_name = $1
        RETURNING current_version`
	err := pool.QueryRow(ctx, sql, collectionName).Scan(&newVersion)
	if err != nil {
		return 0, fmt.Errorf("failed to increment version for collection '%s': %w", collectionName, err)
	}
	slog.Info("Collection version incremented", "collection", collectionName, "new_version", newVersion)
	return newVersion, nil
}

func ListReplicatedCollections(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	sql := `SELECT collection_name FROM replicated_collections ORDER BY collection_name`
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to query replicated collections: %w", err)
	}
	defer rows.Close()

	var collections []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan replicated collection name: %w", err)
		}
		collections = append(collections, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating replicated collections rows: %w", err)
	}
	return collections, nil
}

type CollectionVersion struct {
	CollectionName string
	Version        int
}

func GetAllCurrentCollectionVersions(ctx context.Context, pool *pgxpool.Pool) ([]CollectionVersion, error) {
	sql := `SELECT collection_name, current_version FROM replicated_collections`
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to query all current collection versions: %w", err)
	}
	defer rows.Close()

	var versions []CollectionVersion
	for rows.Next() {
		var cv CollectionVersion
		if err := rows.Scan(&cv.CollectionName, &cv.Version); err != nil {
			return nil, fmt.Errorf("failed to scan collection version row: %w", err)
		}
		versions = append(versions, cv)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating collection version rows: %w", err)
	}
	return versions, nil
}

type ReplicatedCollection struct {
	CollectionName string
	CurrentVersion int
	Shape          shape.Shape
}

func AddReplicatedCollection(ctx context.Context, pool *pgxpool.Pool, collectionName string, shapeJSON []byte) error {
	sql := `INSERT INTO replicated_collections (collection_name, shape) VALUES ($1, $2)`
	_, err := pool.Exec(ctx, sql, collectionName, shapeJSON)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // Unique violation
			slog.Warn("Collection already exists", "collection", collectionName)
			return ErrCollectionAlreadyExists
		}
		return fmt.Errorf("failed to add replicated collection '%s': %w", collectionName, err)
	}
	slog.Info("Collection added to replication", "collection", collectionName)
	return nil
}

func RemoveReplicatedCollection(ctx context.Context, pool *pgxpool.Pool, collectionName string) error {
	sql := `DELETE FROM replicated_collections WHERE collection_name = $1`
	cmdTag, err := pool.Exec(ctx, sql, collectionName)
	if err != nil {
		return fmt.Errorf("failed to remove replicated collection '%s': %w", collectionName, err)
	}
	if cmdTag.RowsAffected() == 0 {
		slog.Warn("Collection not found for removal", "collection", collectionName)
		return fmt.Errorf("collection '%s' %w", collectionName, ErrCollectionNotFound)
	}
	slog.Info("Collection removed from replication", "collection", collectionName)
	return nil
}

func GetAllReplicatedCollectionsWithShapes(ctx context.Context, pool *pgxpool.Pool) ([]ReplicatedCollection, error) {
	sql := `
        SELECT collection_name, current_version, shape
        FROM replicated_collections
        ORDER BY collection_name`
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to query replicated collections with shapes: %w", err)
	}
	defer rows.Close()

	var collections []ReplicatedCollection
	for rows.Next() {
		var rc ReplicatedCollection
		var shapeJSON []byte
		if err := rows.Scan(&rc.CollectionName, &rc.CurrentVersion, &shapeJSON); err != nil {
			return nil, fmt.Errorf("failed to scan replicated collection with shape: %w", err)
		}

		// Skip collections with NULL shapes or corrupted JSON shapes
		if shapeJSON != nil {
			if err := json.Unmarshal(shapeJSON, &rc.Shape); err != nil {
				slog.Error("Failed to unmarshal shape JSON",
					"collection", rc.CollectionName,
					"error", err)
				continue
			}
		} else {
			slog.Warn("Shape is NULL in database", "collection", rc.CollectionName)
			continue
		}

		collections = append(collections, rc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating replicated collections with shapes: %w", err)
	}
	return collections, nil
}
