package leader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/shape"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/nrjais/emcache/internal/config"
)

func getValueByDotPath(data map[string]any, path string) any {
	if path == "." {
		return data
	}
	parts := strings.Split(path, ".")
	current := any(data)
	for _, part := range parts {
		mapCurrent, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		value, exists := mapCurrent[part]
		if !exists {
			return nil
		}
		current = value
	}
	return current
}

func transformDocument(sourceDoc bson.M, collShape shape.Shape) (transformedDocJSON []byte, err error) {
	transformedData := make(map[string]any)
	for _, col := range collShape.Columns {
		value := getValueByDotPath(sourceDoc, col.Path)
		transformedData[col.Name] = value
	}

	jsonData, jsonErr := json.Marshal(transformedData)
	if jsonErr != nil {
		return nil, fmt.Errorf("failed to marshal transformed document: %w", jsonErr)
	}

	return jsonData, nil
}

func StartChangeStreamListener(
	ctx context.Context,
	pool *pgxpool.Pool,
	mongoClient *mongo.Client,
	dbName string,
	replicatedColl db.ReplicatedCollection,
	cfg *config.LeaderConfig,
) {
	collectionName := replicatedColl.CollectionName
	slog.Info("Starting change stream listener", "role", "Leader", "collection", collectionName)

	resumeTokenUpdateInterval := time.Duration(cfg.ResumeTokenUpdateIntervalSecs) * time.Second
	initialRetryDelay := 1 * time.Second
	maxRetryDelay := 30 * time.Second
	currentRetryDelay := initialRetryDelay

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, stopping change stream listener",
				"role", "Leader",
				"collection", collectionName)
			return
		default:
		}

		err := processStream(ctx, pool, mongoClient, dbName, replicatedColl, resumeTokenUpdateInterval, cfg.InitialScanBatchSize)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("Context cancelled during stream processing",
					"role", "Leader",
					"collection", collectionName)
				return
			}
			slog.Error("Error processing change stream",
				"role", "Leader",
				"collection", collectionName,
				"error", err,
				"retry_delay", currentRetryDelay)
			time.Sleep(currentRetryDelay)
			currentRetryDelay *= 2
			if currentRetryDelay > maxRetryDelay {
				currentRetryDelay = maxRetryDelay
			}
		} else {
			slog.Info("Change stream processing stopped gracefully or encountered an unrecoverable state",
				"role", "Leader",
				"collection", collectionName)
			return
		}
	}
}

func processStream(
	ctx context.Context,
	pool *pgxpool.Pool,
	mongoClient *mongo.Client,
	dbName string,
	replicatedColl db.ReplicatedCollection,
	resumeTokenUpdateInterval time.Duration,
	initialScanBatchSize int,
) error {
	collectionName := replicatedColl.CollectionName
	coll := mongoClient.Database(dbName).Collection(collectionName)
	var stream *mongo.ChangeStream
	var err error
	var currentToken bson.Raw
	currentVersion := replicatedColl.CurrentVersion

	tokenStr, found, err := db.GetResumeToken(ctx, pool, collectionName)
	if err != nil {
		return fmt.Errorf("failed to get resume token: %w", err)
	}

	csOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	initialScanRequired := false
	var resumeToken bson.Raw

	if found && tokenStr != "" {
		if err := json.Unmarshal([]byte(tokenStr), &resumeToken); err != nil {
			slog.Warn("Failed to unmarshal stored resume token",
				"role", "Leader",
				"collection", collectionName,
				"token", tokenStr,
				"error", err)
			initialScanRequired = true
		} else {
			slog.Info("Found resume token, attempting to resume stream",
				"role", "Leader",
				"collection", collectionName)
			csOpts.SetResumeAfter(resumeToken)
			initialScanRequired = false
		}
	} else {
		slog.Info("No resume token found in Postgres, performing initial scan",
			"role", "Leader",
			"collection", collectionName)
		initialScanRequired = true
	}

	if initialScanRequired {
		newVersion, err := db.IncrementCollectionVersion(ctx, pool, collectionName)
		if err != nil {
			return fmt.Errorf("failed to increment collection version before initial scan: %w", err)
		}
		currentVersion = newVersion
		slog.Info("Initial scan required, incremented version",
			"role", "Leader",
			"collection", collectionName,
			"version", currentVersion)
	} else {
		slog.Info("Resuming stream",
			"role", "Leader",
			"collection", collectionName,
			"version", currentVersion)
	}

	stream, err = coll.Watch(ctx, mongo.Pipeline{}, csOpts)
	if err != nil {
		// Check for specific resume token errors (e.g., InvalidResumeToken, ChangeStreamHistoryLost)
		if cmdErr, ok := err.(mongo.CommandError); ok && (cmdErr.Code == 286 || cmdErr.Code == 280) {
			slog.Warn("Resume token invalid or history lost",
				"role", "Leader",
				"collection", collectionName,
				"code", cmdErr.Code,
				"error", err)
			initialScanRequired = true

			csOpts.SetResumeAfter(nil)

			stream, err = coll.Watch(ctx, mongo.Pipeline{}, csOpts)
			if err != nil {
				return fmt.Errorf("failed to start change stream even after invalid token: %w", err)
			}

			if initialScanRequired {
				slog.Info("Invalid resume token detected, forcing initial scan and version increment",
					"role", "Leader",
					"collection", collectionName)
				newVersion, err := db.IncrementCollectionVersion(ctx, pool, collectionName)
				if err != nil {
					return fmt.Errorf("failed to increment collection version after invalid token: %w", err)
				}
				currentVersion = newVersion
				slog.Info("Incremented version due to invalid token",
					"role", "Leader",
					"collection", collectionName,
					"version", currentVersion)
			}
		} else {
			return fmt.Errorf("failed to start change stream: %w", err)
		}
	}
	defer stream.Close(ctx)

	slog.Info("Change stream started successfully",
		"role", "Leader",
		"collection", collectionName)

	if initialScanRequired {
		slog.Info("Performing initial collection scan",
			"role", "Leader",
			"collection", collectionName)
		initialResumeToken := stream.ResumeToken()
		if initialResumeToken == nil {
			slog.Warn("Could not get initial resume token before scan, risk of missing events",
				"role", "Leader",
				"collection", collectionName)
		}

		if err := performInitialScan(ctx, pool, coll, collectionName, currentVersion, replicatedColl.Shape, initialScanBatchSize); err != nil {
			return fmt.Errorf("initial collection scan failed: %w", err)
		}
		slog.Info("Initial collection scan completed",
			"role", "Leader",
			"collection", collectionName,
			"version", currentVersion)

		if initialResumeToken != nil {
			currentToken = initialResumeToken
			if err := saveResumeToken(ctx, pool, collectionName, currentToken); err != nil {
				slog.Error("Error saving initial resume token after scan",
					"role", "Leader",
					"collection", collectionName,
					"error", err)
			}
		}
	}

	lastTokenUpdateTime := time.Now()

	for stream.Next(ctx) {
		var event bson.M
		if err := stream.Decode(&event); err != nil {
			return fmt.Errorf("failed to decode change stream event: %w", err)
		}

		currentToken = stream.ResumeToken()

		if err := processChangeEvent(ctx, pool, event, collectionName, currentVersion, replicatedColl.Shape); err != nil {
			slog.Error("Failed to process change event",
				"role", "Leader",
				"collection", collectionName,
				"error", err,
				"event", fmt.Sprintf("%v", event))
			return fmt.Errorf("failed to process change event: %w", err)
		}

		if time.Since(lastTokenUpdateTime) > resumeTokenUpdateInterval {
			if err := saveResumeToken(ctx, pool, collectionName, currentToken); err != nil {
				slog.Error("Error saving resume token periodically",
					"role", "Leader",
					"collection", collectionName,
					"error", err)
			}
			lastTokenUpdateTime = time.Now()
		}
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("change stream iteration error: %w", err)
	}

	slog.Info("Change stream finished without error",
		"role", "Leader",
		"collection", collectionName)
	return nil
}

func saveResumeToken(ctx context.Context, pool *pgxpool.Pool, collection string, token bson.Raw) error {
	if token == nil {
		slog.Warn("Attempted to save a nil resume token, skipping",
			"role", "Leader",
			"collection", collection)
		return nil
	}
	tokenBytes, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("failed to marshal resume token: %w", err)
	}
	if err := db.UpsertResumeToken(ctx, pool, collection, string(tokenBytes)); err != nil {
		return fmt.Errorf("failed to upsert resume token in db: %w", err)
	}
	return nil
}

func processChangeEvent(ctx context.Context, pool *pgxpool.Pool, event bson.M, collectionName string, version int, collShape shape.Shape) error {
	opTypeStr, _ := event["operationType"].(string)

	docKey, ok := event["documentKey"].(bson.M)
	if !ok || docKey == nil {
		slog.Warn("Skipping event with missing or invalid documentKey",
			"role", "Leader",
			"collection", collectionName,
			"event", fmt.Sprintf("%v", event))
		return nil
	}
	var docID string
	idValue := docKey["_id"]
	switch v := idValue.(type) {
	case primitive.ObjectID:
		docID = v.Hex()
	default:
		docID = fmt.Sprintf("%v", v)
	}

	clusterTime, ok := event["clusterTime"].(primitive.Timestamp)
	if !ok {
		slog.Warn("Event missing clusterTime, using wall clock time",
			"role", "Leader",
			"collection", collectionName,
			"event", fmt.Sprintf("%v", event))
		clusterTime = primitive.Timestamp{T: uint32(time.Now().Unix())}
	}
	ts := time.Unix(int64(clusterTime.T), 0)

	var operation string
	var data []byte
	var err error

	switch opTypeStr {
	case "insert", "update", "replace":
		operation = "UPSERT"
		fullDoc, ok := event["fullDocument"].(bson.M)
		if !ok {
			slog.Warn("UPSERT event missing fullDocument ID",
				"role", "Leader",
				"collection", collectionName,
				"doc_id", docID,
				"event", fmt.Sprintf("%v", event))
			return nil
		}

		data, err = transformDocument(fullDoc, collShape)
		if err != nil {
			slog.Warn("Failed to transform document for ID",
				"role", "Leader",
				"collection", collectionName,
				"doc_id", docID,
				"error", err)
			return nil
		}
	case "delete":
		operation = "DELETE"

	default:
		slog.Warn("Skipping unhandled operation type",
			"role", "Leader",
			"collection", collectionName,
			"op_type", opTypeStr,
			"doc_id", docID)
		return nil
	}

	oplogEntry := db.OplogEntry{
		Operation:  operation,
		DocID:      docID,
		CreatedAt:  ts,
		Collection: collectionName,
		Doc:        data,
		Version:    version,
	}

	idx, err := db.InsertOplogEntry(ctx, pool, oplogEntry)
	if err != nil {
		return err
	}

	slog.Info("Processed",
		"role", "Leader",
		"collection", collectionName,
		"operation", operation,
		"doc_id", docID,
		"oplog_index", idx)
	return nil
}

func performInitialScan(ctx context.Context, pool *pgxpool.Pool, coll *mongo.Collection, collectionName string, version int, collShape shape.Shape, batchSize int) error {
	cursor, err := coll.Find(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to start Find cursor for initial scan: %w", err)
	}
	defer cursor.Close(ctx)

	scanCount := 0
	startTime := time.Now()

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("failed to decode document during initial scan: %w", err)
		}

		docID, err := createAndInsertEntry(ctx, pool, doc, collectionName, version, collShape)
		if err != nil {
			return fmt.Errorf("failed to create and insert entry for doc %s: %w", docID, err)
		}

		scanCount++
		if scanCount%batchSize == 0 {
			slog.Info("Scanned documents",
				"role", "Leader",
				"collection", collectionName,
				"count", scanCount)
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error during initial scan: %w", err)
	}

	slog.Info("Finished initial scan",
		"role", "Leader",
		"collection", collectionName,
		"count", scanCount,
		"duration", time.Since(startTime))
	return nil
}

func createAndInsertEntry(ctx context.Context, pool *pgxpool.Pool, doc bson.M, collectionName string, version int, collShape shape.Shape) (string, error) {
	docIDRaw, ok := doc["_id"]
	if !ok {
		slog.Warn("Skipping document with missing _id during initial scan",
			"role", "Leader",
			"collection", collectionName,
			"doc", fmt.Sprintf("%v", doc))
		return "", fmt.Errorf("document with missing _id during initial scan: %v", doc)
	}
	var docID string
	switch v := docIDRaw.(type) {
	case primitive.ObjectID:
		docID = v.Hex()
	default:
		docID = fmt.Sprintf("%v", v)
	}

	data, err := transformDocument(doc, collShape)
	if err != nil {
		slog.Warn("Failed to transform document for ID",
			"role", "Leader",
			"collection", collectionName,
			"doc_id", docID,
			"error", err)
		return docID, fmt.Errorf("failed to transform document for ID %s: %w", docID, err)
	}
	oplogEntry := db.OplogEntry{
		Operation:  "UPSERT",
		DocID:      docID,
		CreatedAt:  time.Now(),
		Collection: collectionName,
		Doc:        data,
		Version:    version,
	}
	_, err = db.InsertOplogEntry(ctx, pool, oplogEntry)
	if err != nil {
		return docID, fmt.Errorf("failed to insert initial scan oplog entry for doc %s: %w", docID, err)
	}

	return docID, nil
}
