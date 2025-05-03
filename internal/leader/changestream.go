package leader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	collectionName string,
	collShape shape.Shape,
	cfg *config.LeaderConfig,
) {
	log.Printf("[Leader:%s] Starting change stream listener...", collectionName)

	resumeTokenUpdateInterval := time.Duration(cfg.ResumeTokenUpdateIntervalSecs) * time.Second
	initialRetryDelay := 1 * time.Second
	maxRetryDelay := 30 * time.Second
	currentRetryDelay := initialRetryDelay

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Leader:%s] Context cancelled, stopping change stream listener.", collectionName)
			return
		default:
		}

		err := processStream(ctx, pool, mongoClient, dbName, collectionName, collShape, resumeTokenUpdateInterval, cfg.InitialScanBatchSize)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("[Leader:%s] Context cancelled during stream processing.", collectionName)
				return
			}
			log.Printf("[Leader:%s] Error processing change stream: %v. Retrying in %v...", collectionName, err, currentRetryDelay)
			time.Sleep(currentRetryDelay)
			currentRetryDelay *= 2
			if currentRetryDelay > maxRetryDelay {
				currentRetryDelay = maxRetryDelay
			}
		} else {
			log.Printf("[Leader:%s] Change stream processing stopped gracefully or encountered an unrecoverable state.", collectionName)
			return
		}
	}
}

func processStream(
	ctx context.Context,
	pool *pgxpool.Pool,
	mongoClient *mongo.Client,
	dbName string,
	collectionName string,
	collShape shape.Shape,
	resumeTokenUpdateInterval time.Duration,
	initialScanBatchSize int,
) error {
	coll := mongoClient.Database(dbName).Collection(collectionName)
	var stream *mongo.ChangeStream
	var err error
	var currentToken bson.Raw
	var currentVersion int

	tokenStr, found, err := db.GetResumeToken(ctx, pool, collectionName)
	if err != nil {
		return fmt.Errorf("failed to get resume token: %w", err)
	}

	csOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	initialScanRequired := false
	var resumeToken bson.Raw

	if found && tokenStr != "" {
		if err := json.Unmarshal([]byte(tokenStr), &resumeToken); err != nil {
			log.Printf("[Leader:%s] Failed to unmarshal stored resume token '%s': %v. Performing initial scan.", collectionName, tokenStr, err)
			initialScanRequired = true
		} else {
			log.Printf("[Leader:%s] Found resume token. Attempting to resume stream...", collectionName)
			csOpts.SetResumeAfter(resumeToken)
			initialScanRequired = false
		}
	} else {
		log.Printf("[Leader:%s] No resume token found in Postgres. Performing initial scan.", collectionName)
		initialScanRequired = true
	}

	if initialScanRequired {
		newVersion, err := db.IncrementCollectionVersion(ctx, pool, collectionName)
		if err != nil {
			return fmt.Errorf("failed to increment collection version before initial scan: %w", err)
		}
		currentVersion = newVersion
		log.Printf("[Leader:%s] Initial scan required. Incremented version to %d.", collectionName, currentVersion)
	} else {
		existingVersion, err := db.GetCurrentCollectionVersion(ctx, pool, collectionName)
		if err != nil {
			return fmt.Errorf("failed to get current collection version for resuming stream: %w", err)
		}
		currentVersion = existingVersion
		log.Printf("[Leader:%s] Resuming stream for version %d.", collectionName, currentVersion)
	}

	stream, err = coll.Watch(ctx, mongo.Pipeline{}, csOpts)
	if err != nil {
		// Check for specific resume token errors (e.g., InvalidResumeToken, ChangeStreamHistoryLost)
		if cmdErr, ok := err.(mongo.CommandError); ok && (cmdErr.Code == 286 || cmdErr.Code == 280) {
			log.Printf("[Leader:%s] Resume token invalid or history lost (Code: %d): %v. Starting stream from beginning and performing initial scan.", collectionName, cmdErr.Code, err)
			initialScanRequired = true

			csOpts.SetResumeAfter(nil)

			stream, err = coll.Watch(ctx, mongo.Pipeline{}, csOpts)
			if err != nil {
				return fmt.Errorf("failed to start change stream even after invalid token: %w", err)
			}

			if initialScanRequired {
				log.Printf("[Leader:%s] Invalid resume token detected, forcing initial scan and version increment.", collectionName)
				newVersion, err := db.IncrementCollectionVersion(ctx, pool, collectionName)
				if err != nil {
					return fmt.Errorf("failed to increment collection version after invalid token: %w", err)
				}
				currentVersion = newVersion
				log.Printf("[Leader:%s] Incremented version to %d due to invalid token.", collectionName, currentVersion)
			}
		} else {
			return fmt.Errorf("failed to start change stream: %w", err)
		}
	}
	defer stream.Close(ctx)

	log.Printf("[Leader:%s] Change stream started successfully.", collectionName)

	if initialScanRequired {
		log.Printf("[Leader:%s] Performing initial collection scan...", collectionName)
		initialResumeToken := stream.ResumeToken()
		if initialResumeToken == nil {
			log.Printf("[Leader:%s] Warning: Could not get initial resume token before scan. There's a small chance of missing events.", collectionName)
		}

		if err := performInitialScan(ctx, pool, coll, collectionName, currentVersion, collShape, initialScanBatchSize); err != nil {
			return fmt.Errorf("initial collection scan failed: %w", err)
		}
		log.Printf("[Leader:%s] Initial collection scan completed for version %d.", collectionName, currentVersion)

		if initialResumeToken != nil {
			currentToken = initialResumeToken
			if err := saveResumeToken(ctx, pool, collectionName, currentToken); err != nil {
				log.Printf("[Leader:%s] Error saving initial resume token after scan: %v", collectionName, err)
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

		if err := processChangeEvent(ctx, pool, event, collectionName, currentVersion, collShape); err != nil {
			log.Printf("[Leader:%s] Failed to process change event: %v. Event: %v", collectionName, err, event)
			return fmt.Errorf("failed to process change event: %w", err)
		}

		if time.Since(lastTokenUpdateTime) > resumeTokenUpdateInterval {
			if err := saveResumeToken(ctx, pool, collectionName, currentToken); err != nil {
				log.Printf("[Leader:%s] Error saving resume token periodically: %v", collectionName, err)
			}
			lastTokenUpdateTime = time.Now()
		}
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("change stream iteration error: %w", err)
	}

	log.Printf("[Leader:%s] Change stream finished without error.", collectionName)
	return nil
}

func saveResumeToken(ctx context.Context, pool *pgxpool.Pool, collection string, token bson.Raw) error {
	if token == nil {
		log.Printf("[Leader:%s] Attempted to save a nil resume token. Skipping.", collection)
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
		log.Printf("[Leader:%s] Warning: Skipping event with missing or invalid documentKey: %v", collectionName, event)
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
		log.Printf("[Leader:%s] Warning: Event missing clusterTime, using wall clock time. Event: %v", collectionName, event)
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
			log.Printf("[Leader:%s] Warning: UPSERT event missing fullDocument ID %s: %v. Skipping.", collectionName, docID, event)
			return nil
		}

		data, err = transformDocument(fullDoc, collShape)
		if err != nil {
			log.Printf("[Leader:%s] Failed to transform document for ID %s: %v. Skipping event.", collectionName, docID, err)
			return nil
		}
	case "delete":
		operation = "DELETE"

	default:
		log.Printf("[Leader:%s] Skipping unhandled operation type '%s' for doc %s", collectionName, opTypeStr, docID)
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

	log.Printf("[Leader:%s] Processed %s for doc %s -> Oplog Index %d", collectionName, operation, docID, idx)
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
			log.Printf("[Leader:%s-Scan] Scanned %d documents...", collectionName, scanCount)
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error during initial scan: %w", err)
	}

	log.Printf("[Leader:%s-Scan] Finished initial scan of %d documents in %v.", collectionName, scanCount, time.Since(startTime))
	return nil
}

func createAndInsertEntry(ctx context.Context, pool *pgxpool.Pool, doc bson.M, collectionName string, version int, collShape shape.Shape) (string, error) {
	docIDRaw, ok := doc["_id"]
	if !ok {
		log.Printf("[Leader:%s-Scan] Warning: Skipping document with missing _id during initial scan: %v", collectionName, doc)
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
		log.Printf("[Leader:%s-Scan] Failed to transform document for ID %s: %v. Skipping doc.", collectionName, docID, err)
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
