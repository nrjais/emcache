package e2e_tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	emclient "github.com/nrjais/emcache/client"
	pb "github.com/nrjais/emcache/pkg/protos"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoClient   *mongo.Client
	emcacheClient *emclient.Client
)

const (
	mongoURI          = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
	emcacheAddr       = "localhost:50051"
	dockerComposeFile = "./docker-compose.yml"
)

var uniqueId = primitive.NewObjectID().Hex()

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error

	log.Printf("Connecting to MongoDB at %s...", mongoURI)
	clientOptions := options.Client().ApplyURI(mongoURI)
	mongoClient, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Could not connect to mongo: %v", err)
	}
	defer func() {
		if err = mongoClient.Disconnect(ctx); err != nil {
			log.Printf("Could not disconnect mongo client: %v", err)
		}
	}()
	if err = mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("Could not ping mongo: %v", err)
	}
	log.Println("Successfully connected to MongoDB")

	log.Printf("Connecting to Emcache gRPC server at %s...", emcacheAddr)
	clientCtx, clientCancel := context.WithTimeout(ctx, 15*time.Second)
	emcacheClient, err = emclient.NewClient(clientCtx, emcacheAddr)
	clientCancel()
	if err != nil {
		log.Fatalf("Could not create emcache client: %v", err)
	}
	defer func() {
		if err := emcacheClient.Close(); err != nil {
			log.Printf("Could not close emcache client connection: %v", err)
		}
	}()
	log.Println("Successfully connected to Emcache gRPC server")

	log.Println("Starting E2E tests...")
	code := m.Run()
	log.Println("E2E tests finished.")

	os.Exit(code)
}

type TestDoc struct {
	ID   string `bson:"_id" json:"_id"`
	Name string `bson:"name" json:"name"`
	Age  int    `bson:"age" json:"age"`
}

func setupSyncedCollection(t *testing.T, ctx context.Context, numInitialDocs int, collectionName string) ([]any, string, int32) {
	t.Helper()

	dbName := "test"
	log.Printf("[%s] Setting up collection: %s", t.Name(), collectionName)

	initialDocs := make([]any, 0, numInitialDocs)
	for i := range numInitialDocs {
		initialDocs = append(initialDocs, TestDoc{
			ID:   uuid.NewString(),
			Name: fmt.Sprintf("InitialDoc_%d_%s", i, uuid.NewString()[:4]),
			Age:  20 + i,
		})
	}

	collection := mongoClient.Database(dbName).Collection(collectionName)
	t.Cleanup(func() {
		_, err := emcacheClient.RemoveCollection(context.Background(), collectionName)
		require.NoError(t, err, "Failed to remove collection from Emcache")
		log.Printf("[%s] Dropping collection: %s", t.Name(), collectionName)
		if err := collection.Drop(context.Background()); err != nil {
			log.Printf("[%s] Failed to drop collection %s: %v", t.Name(), collectionName, err)
		}
	})

	if len(initialDocs) > 0 {
		log.Printf("[%s] Inserting %d initial documents...", t.Name(), len(initialDocs))
		_, err := collection.InsertMany(ctx, initialDocs)
		require.NoError(t, err, "Failed to insert initial documents into MongoDB")
	}

	log.Printf("[%s] Calling AddCollection...", t.Name())
	_, err := emcacheClient.AddCollection(ctx, collectionName)
	require.NoError(t, err, "Failed to call AddCollection")
	time.Sleep(10 * time.Second)

	log.Printf("[%s] Calling DownloadDb...", t.Name())
	var dbBuf bytes.Buffer
	dbVersion, err := emcacheClient.DownloadDb(ctx, collectionName, &dbBuf)
	require.NoError(t, err, "Failed to download DB")
	require.NotEqual(t, -1, dbVersion, "Expected a valid DB version")
	log.Printf("[%s] Downloaded initial DB version: %d, size: %d bytes", t.Name(), dbVersion, dbBuf.Len())

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, collectionName+".sqlite")
	err = os.WriteFile(dbPath, dbBuf.Bytes(), 0644)
	require.NoError(t, err, "Failed to write downloaded DB to temp file")
	log.Printf("[%s] Saved initial DB to: %s", t.Name(), dbPath)

	return initialDocs, dbPath, dbVersion
}

func applyOplogEvents(t *testing.T, conn *sqlite.Conn, collectionName string, entries []*pb.OplogEntry) (lastAppliedIndex int64) {
	t.Helper()
	lastAppliedIndex = -1

	if len(entries) == 0 {
		return lastAppliedIndex
	}

	log.Printf("[%s] Applying %d oplog events to collection %s...", t.Name(), len(entries), collectionName)

	var err error
	defer sqlitex.Save(conn)(&err)

	for _, entry := range entries {
		log.Printf("[%s] Applying event: Index=%d, Op=%s, ID=%s", t.Name(), entry.Index, entry.Operation, entry.Id)
		switch entry.Operation {
		case pb.OplogEntry_UPSERT:
			if entry.Data == nil {
				log.Printf("[%s] WARN: UPSERT event for ID %s has nil data, skipping.", t.Name(), entry.Id)
				continue
			}

			stmt := "INSERT INTO _emcache_data (id, source) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET source = excluded.source"

			jsonData, jsonErr := entry.Data.MarshalJSON()
			if jsonErr != nil {
				err = fmt.Errorf("failed to marshal entry data to JSON for ID %s: %w", entry.Id, jsonErr)
				log.Printf("[%s] ERROR: %v", t.Name(), err)
				require.FailNow(t, "Failed to process UPSERT event", err.Error())
			}

			err = sqlitex.Execute(conn, stmt, &sqlitex.ExecOptions{
				Args: []any{entry.Id, jsonData},
			})
			if err != nil {
				err = fmt.Errorf("failed to apply UPSERT for ID %s: %w", entry.Id, err)
				log.Printf("[%s] ERROR: %v", t.Name(), err)
				require.FailNow(t, "Failed to apply UPSERT event", err.Error())
			}

		case pb.OplogEntry_DELETE:
			stmt := "DELETE FROM _emcache_data WHERE id = ?"
			err = sqlitex.Execute(conn, stmt, &sqlitex.ExecOptions{
				Args: []any{entry.Id},
			})
			if err != nil {
				err = fmt.Errorf("failed to apply DELETE for ID %s: %w", entry.Id, err)
				log.Printf("[%s] ERROR: %v", t.Name(), err)
				require.FailNow(t, "Failed to apply DELETE event", err.Error())
			}
		default:
			log.Printf("[%s] WARN: Unknown oplog operation type %s for ID %s", t.Name(), entry.Operation, entry.Id)
		}
		lastAppliedIndex = entry.Index
	}

	if err == nil {
		log.Printf("[%s] Successfully applied %d events up to index %d", t.Name(), len(entries), lastAppliedIndex)
	}

	return lastAppliedIndex
}

func verifyDocsInSQLite(t *testing.T, conn *sqlite.Conn, collectionName string, expectedDocs map[string]TestDoc) {
	t.Helper()

	query := "SELECT id, source FROM _emcache_data"
	foundDocs := make(map[string]TestDoc)

	err := sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			id := stmt.ColumnText(0)

			length := stmt.ColumnLen(1)
			if length == 0 {
				log.Printf("[%s] WARN: Found document with ID %s but empty source data.", t.Name(), id)
				return nil
			}

			sourceBytes := make([]byte, length)
			bytesRead := stmt.ColumnBytes(1, sourceBytes)
			if bytesRead != length {
				log.Printf("[%s] WARN: Mismatch read length for doc ID %s (expected %d, got %d)", t.Name(), id, length, bytesRead)

			}

			var doc TestDoc
			if err := json.Unmarshal(sourceBytes, &doc); err != nil {

				log.Printf("[%s] ERROR: Failed to unmarshal source JSON for doc ID %s: %v\nJSON: %s", t.Name(), id, err, string(sourceBytes))
				return nil
			}

			if doc.ID != id {
				log.Printf("[%s] WARN: Mismatch between id column ('%s') and JSON id field ('%s')", t.Name(), id, doc.ID)

			}
			foundDocs[id] = doc
			return nil
		},
	})
	require.NoError(t, err, "Failed to execute query for verification")

	require.Equal(t, len(expectedDocs), len(foundDocs), "Number of documents in SQLite DB (%d) does not match expected count (%d)", len(foundDocs), len(expectedDocs))

	for id, expected := range expectedDocs {
		found, ok := foundDocs[id]
		require.True(t, ok, "Expected document with ID %s not found in SQLite DB", id)
		require.Equal(t, expected.Name, found.Name, "Name mismatch for doc ID %s. Expected: %s, Found: %s", id, expected.Name, found.Name)
		require.Equal(t, expected.Age, found.Age, "Age mismatch for doc ID %s. Expected: %d, Found: %d", id, expected.Age, found.Age)
	}

	if len(expectedDocs) > 0 {
		log.Printf("[%s] Successfully verified %d documents in SQLite DB %s", t.Name(), len(expectedDocs), collectionName)
	} else {
		log.Printf("[%s] Successfully verified SQLite DB %s is empty as expected", t.Name(), collectionName)
	}
}

func TestInitialSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := "test_initial_sync_" + uniqueId
	numInitial := 2
	initialDocsSlice, dbPath, _ := setupSyncedCollection(t, ctx, numInitial, collectionName)

	initialDocsMap := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		initialDocsMap[d.ID] = d
	}

	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL)
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	verifyDocsInSQLite(t, sqlDB, collectionName, initialDocsMap)

	log.Printf("[%s] Initial sync verification successful!", t.Name())
}

func TestInsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	numInitial := 1
	collectionName := "test_insert_sync_" + uniqueId
	initialDocsSlice, dbPath, initialDbVersion := setupSyncedCollection(t, ctx, numInitial, collectionName)
	require.GreaterOrEqual(t, initialDbVersion, int32(0), "Initial DB version should be valid")

	expectedDocs := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
	}

	newDocs := []any{
		TestDoc{ID: uuid.NewString(), Name: "Charlie", Age: 35},
		TestDoc{ID: uuid.NewString(), Name: "David", Age: 40},
	}

	log.Printf("[%s] Inserting %d new documents into MongoDB...", t.Name(), len(newDocs))
	collection := mongoClient.Database("test").Collection(collectionName)
	_, err := collection.InsertMany(ctx, newDocs)
	require.NoError(t, err, "Failed to insert new documents into MongoDB")

	for _, doc := range newDocs {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
	}

	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	log.Printf("[%s] Getting oplog entries after index %d...", t.Name(), initialDbVersion)
	limit := int32(100)
	oplogResp, err := emcacheClient.GetOplogEntries(ctx, []string{collectionName}, int64(initialDbVersion), limit)
	require.NoError(t, err, "Failed to get oplog entries")
	log.Printf("[%s] Received %d oplog entries.", t.Name(), len(oplogResp.Entries))

	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL)
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	applyOplogEvents(t, sqlDB, collectionName, oplogResp.Entries)

	log.Printf("[%s] Verifying all documents in SQLite DB...", t.Name())
	verifyDocsInSQLite(t, sqlDB, collectionName, expectedDocs)

	log.Printf("[%s] Insert sync verification successful!", t.Name())
}

func TestUpdateSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	numInitial := 2
	collectionName := "test_update_sync_" + uniqueId
	initialDocsSlice, dbPath, initialDbVersion := setupSyncedCollection(t, ctx, numInitial, collectionName)
	require.GreaterOrEqual(t, initialDbVersion, int32(0), "Initial DB version should be valid")

	expectedDocs := make(map[string]TestDoc)
	var docToUpdate TestDoc
	for i, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
		if i == 0 {
			docToUpdate = d
		}
	}
	require.NotEmpty(t, docToUpdate.ID, "Should have selected a document to update")

	updatedName := "Updated " + docToUpdate.Name
	updatedAge := docToUpdate.Age + 10

	log.Printf("[%s] Updating document ID %s in MongoDB...", t.Name(), docToUpdate.ID)
	collection := mongoClient.Database("test").Collection(collectionName)
	filter := bson.M{"_id": docToUpdate.ID}
	update := bson.M{"$set": bson.M{"name": updatedName, "age": updatedAge}}
	_, err := collection.UpdateOne(ctx, filter, update)
	require.NoError(t, err, "Failed to update document in MongoDB")

	expectedDocs[docToUpdate.ID] = TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}

	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	log.Printf("[%s] Getting oplog entries after index %d...", t.Name(), initialDbVersion)
	limit := int32(100)
	oplogResp, err := emcacheClient.GetOplogEntries(ctx, []string{collectionName}, int64(initialDbVersion), limit)
	require.NoError(t, err, "Failed to get oplog entries")
	log.Printf("[%s] Received %d oplog entries.", t.Name(), len(oplogResp.Entries))

	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL)
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	applyOplogEvents(t, sqlDB, collectionName, oplogResp.Entries)

	log.Printf("[%s] Verifying updated documents in SQLite DB...", t.Name())
	verifyDocsInSQLite(t, sqlDB, collectionName, expectedDocs)

	log.Printf("[%s] Update sync verification successful!", t.Name())
}

func TestDeleteSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	numInitial := 3
	collectionName := "test_delete_sync_" + uniqueId
	initialDocsSlice, dbPath, initialDbVersion := setupSyncedCollection(t, ctx, numInitial, collectionName)
	require.GreaterOrEqual(t, initialDbVersion, int32(0), "Initial DB version should be valid")

	expectedDocs := make(map[string]TestDoc)
	var docsToDelete []TestDoc
	for i, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
		if i < 2 {
			docsToDelete = append(docsToDelete, d)
		}
	}
	require.Len(t, docsToDelete, 2, "Should have selected documents to delete")

	collection := mongoClient.Database("test").Collection(collectionName)
	for _, doc := range docsToDelete {
		log.Printf("[%s] Deleting document ID %s from MongoDB...", t.Name(), doc.ID)
		filter := bson.M{"_id": doc.ID}
		_, err := collection.DeleteOne(ctx, filter)
		require.NoError(t, err, "Failed to delete document ID %s from MongoDB", doc.ID)

		delete(expectedDocs, doc.ID)
	}

	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	log.Printf("[%s] Getting oplog entries after index %d...", t.Name(), initialDbVersion)
	limit := int32(100)
	oplogResp, err := emcacheClient.GetOplogEntries(ctx, []string{collectionName}, int64(initialDbVersion), limit)
	require.NoError(t, err, "Failed to get oplog entries")
	log.Printf("[%s] Received %d oplog entries.", t.Name(), len(oplogResp.Entries))

	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL)
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	applyOplogEvents(t, sqlDB, collectionName, oplogResp.Entries)

	log.Printf("[%s] Verifying documents after deletion in SQLite DB...", t.Name())
	verifyDocsInSQLite(t, sqlDB, collectionName, expectedDocs)

	log.Printf("[%s] Delete sync verification successful!", t.Name())
}

func TestUpsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	numInitial := 2
	collectionName := "test_upsert_sync_" + uniqueId
	initialDocsSlice, dbPath, initialDbVersion := setupSyncedCollection(t, ctx, numInitial, collectionName)
	require.GreaterOrEqual(t, initialDbVersion, int32(0), "Initial DB version should be valid")

	expectedDocs := make(map[string]TestDoc)
	var docToUpdate TestDoc
	for i, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
		if i == 0 {
			docToUpdate = d
		}
	}

	collection := mongoClient.Database("test").Collection(collectionName)
	upsertOpts := options.Replace().SetUpsert(true)

	updatedName := "Upserted " + docToUpdate.Name
	updatedAge := docToUpdate.Age + 5
	updatedDoc := TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}
	filterUpdate := bson.M{"_id": docToUpdate.ID}
	log.Printf("[%s] Upserting (update) document ID %s in MongoDB...", t.Name(), docToUpdate.ID)
	_, err := collection.ReplaceOne(ctx, filterUpdate, updatedDoc, upsertOpts)
	require.NoError(t, err, "Failed to upsert (update) document ID %s", docToUpdate.ID)
	expectedDocs[docToUpdate.ID] = updatedDoc

	newDocID := uuid.NewString()
	newDoc := TestDoc{ID: newDocID, Name: "New Upserted Doc", Age: 55}
	filterInsert := bson.M{"_id": newDocID}
	log.Printf("[%s] Upserting (insert) document ID %s in MongoDB...", t.Name(), newDocID)
	_, err = collection.ReplaceOne(ctx, filterInsert, newDoc, upsertOpts)
	require.NoError(t, err, "Failed to upsert (insert) document ID %s", newDocID)
	expectedDocs[newDocID] = newDoc

	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	log.Printf("[%s] Getting oplog entries after index %d...", t.Name(), initialDbVersion)
	limit := int32(100)
	oplogResp, err := emcacheClient.GetOplogEntries(ctx, []string{collectionName}, int64(initialDbVersion), limit)
	require.NoError(t, err, "Failed to get oplog entries")
	log.Printf("[%s] Received %d oplog entries.", t.Name(), len(oplogResp.Entries))

	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL)
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	applyOplogEvents(t, sqlDB, collectionName, oplogResp.Entries)

	log.Printf("[%s] Verifying documents after upserts in SQLite DB...", t.Name())
	verifyDocsInSQLite(t, sqlDB, collectionName, expectedDocs)

	log.Printf("[%s] Upsert sync verification successful!", t.Name())
}
