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

// TestDoc is a sample document structure used in tests.
type TestDoc struct {
	ID   string `bson:"_id" json:"_id"`
	Name string `bson:"name" json:"name"`
	Age  int    `bson:"age" json:"age"`
}

// setupSyncedCollection creates a unique collection, adds initial data,
// calls AddCollection, downloads the initial DB, and returns the collection name,
// the initial docs, the path to the local SQLite DB, and the initial DB version.
func setupSyncedCollection(t *testing.T, ctx context.Context, numInitialDocs int, collectionName string) ([]any, string, int32) {
	t.Helper()

	dbName := "test"
	log.Printf("[%s] Setting up collection: %s", t.Name(), collectionName)

	// Generate initial docs
	initialDocs := make([]any, 0, numInitialDocs)
	for i := range numInitialDocs {
		initialDocs = append(initialDocs, TestDoc{
			ID:   uuid.NewString(),
			Name: fmt.Sprintf("InitialDoc_%d_%s", i, uuid.NewString()[:4]),
			Age:  20 + i,
		})
	}

	// Get mongo collection handle
	collection := mongoClient.Database(dbName).Collection(collectionName)
	t.Cleanup(func() {
		_, err := emcacheClient.RemoveCollection(context.Background(), collectionName)
		require.NoError(t, err, "Failed to remove collection from Emcache")
		log.Printf("[%s] Dropping collection: %s", t.Name(), collectionName)
		if err := collection.Drop(context.Background()); err != nil {
			log.Printf("[%s] Failed to drop collection %s: %v", t.Name(), collectionName, err)
		}
	})

	// Insert initial documents if any
	if len(initialDocs) > 0 {
		log.Printf("[%s] Inserting %d initial documents...", t.Name(), len(initialDocs))
		_, err := collection.InsertMany(ctx, initialDocs)
		require.NoError(t, err, "Failed to insert initial documents into MongoDB")
	}

	// Call AddCollection
	log.Printf("[%s] Calling AddCollection...", t.Name())
	_, err := emcacheClient.AddCollection(ctx, collectionName)
	require.NoError(t, err, "Failed to call AddCollection")
	time.Sleep(10 * time.Second) // Allow time for processing

	// Download the initial DB
	log.Printf("[%s] Calling DownloadDb...", t.Name())
	var dbBuf bytes.Buffer
	dbVersion, err := emcacheClient.DownloadDb(ctx, collectionName, &dbBuf)
	require.NoError(t, err, "Failed to download DB")
	require.NotEqual(t, -1, dbVersion, "Expected a valid DB version")
	log.Printf("[%s] Downloaded initial DB version: %d, size: %d bytes", t.Name(), dbVersion, dbBuf.Len())

	// Save DB to a temporary file
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, collectionName+".sqlite")
	err = os.WriteFile(dbPath, dbBuf.Bytes(), 0644)
	require.NoError(t, err, "Failed to write downloaded DB to temp file")
	log.Printf("[%s] Saved initial DB to: %s", t.Name(), dbPath)

	return initialDocs, dbPath, dbVersion
}

// applyOplogEvents applies received oplog events to the local SQLite database.
// It returns the index of the last successfully applied event.
func applyOplogEvents(t *testing.T, conn *sqlite.Conn, collectionName string, entries []*pb.OplogEntry) (lastAppliedIndex int64) {
	t.Helper()
	lastAppliedIndex = -1 // Initialize

	if len(entries) == 0 {
		return lastAppliedIndex
	}

	log.Printf("[%s] Applying %d oplog events to collection %s...", t.Name(), len(entries), collectionName)

	// Use sqlitex.Save for transaction handling
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

			// Assuming _emcache_data table structure (id TEXT PK, source BLOB)
			stmt := "INSERT INTO _emcache_data (id, source) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET source = excluded.source" // Use constant table name

			// Convert structpb to JSON bytes for the source BLOB column
			jsonData, jsonErr := entry.Data.MarshalJSON()
			if jsonErr != nil {
				err = fmt.Errorf("failed to marshal entry data to JSON for ID %s: %w", entry.Id, jsonErr)
				log.Printf("[%s] ERROR: %v", t.Name(), err)
				require.FailNow(t, "Failed to process UPSERT event", err.Error()) // Let defer handle rollback
			}

			err = sqlitex.Execute(conn, stmt, &sqlitex.ExecOptions{
				Args: []any{entry.Id, jsonData},
			})
			if err != nil {
				err = fmt.Errorf("failed to apply UPSERT for ID %s: %w", entry.Id, err) // Assign error for defer
				log.Printf("[%s] ERROR: %v", t.Name(), err)
				require.FailNow(t, "Failed to apply UPSERT event", err.Error()) // Let defer handle rollback
			}

		case pb.OplogEntry_DELETE:
			stmt := "DELETE FROM _emcache_data WHERE id = ?"
			err = sqlitex.Execute(conn, stmt, &sqlitex.ExecOptions{
				Args: []any{entry.Id},
			})
			if err != nil {
				err = fmt.Errorf("failed to apply DELETE for ID %s: %w", entry.Id, err) // Assign error for defer
				log.Printf("[%s] ERROR: %v", t.Name(), err)
				require.FailNow(t, "Failed to apply DELETE event", err.Error()) // Let defer handle rollback
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

// verifyDocsInSQLite queries the SQLite DB and verifies the presence and content of expected documents.
func verifyDocsInSQLite(t *testing.T, conn *sqlite.Conn, collectionName string, expectedDocs map[string]TestDoc) {
	t.Helper()

	// Use the actual collection name in the query now
	query := "SELECT id, source FROM _emcache_data"
	foundDocs := make(map[string]TestDoc)

	err := sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			id := stmt.ColumnText(0)
			// Get length of the blob first
			length := stmt.ColumnLen(1)
			if length == 0 {
				log.Printf("[%s] WARN: Found document with ID %s but empty source data.", t.Name(), id)
				return nil // Skip this row
			}

			// Allocate buffer and read bytes
			sourceBytes := make([]byte, length)
			bytesRead := stmt.ColumnBytes(1, sourceBytes) // Read into the buffer
			if bytesRead != length {
				log.Printf("[%s] WARN: Mismatch read length for doc ID %s (expected %d, got %d)", t.Name(), id, length, bytesRead)
				// Continue anyway, maybe partial data is useful?
			}

			var doc TestDoc
			if err := json.Unmarshal(sourceBytes, &doc); err != nil {
				// Log the error but continue verification if possible
				log.Printf("[%s] ERROR: Failed to unmarshal source JSON for doc ID %s: %v\nJSON: %s", t.Name(), id, err, string(sourceBytes))
				return nil // Treat unmarshal error as a verification failure later
			}

			// Ensure the id from the JSON matches the key column
			if doc.ID != id {
				log.Printf("[%s] WARN: Mismatch between id column ('%s') and JSON id field ('%s')", t.Name(), id, doc.ID)
				// Use the column id as the key for the map
			}
			foundDocs[id] = doc
			return nil
		},
	})
	require.NoError(t, err, "Failed to execute query for verification")

	// Detailed comparison
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

// --- Test Cases ---

func TestInitialSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Setup collection and initial data using helper
	collectionName := "test_initial_sync_" + uniqueId
	numInitial := 2
	initialDocsSlice, dbPath, _ := setupSyncedCollection(t, ctx, numInitial, collectionName)

	// Convert initial docs slice to map for easier verification
	initialDocsMap := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		initialDocsMap[d.ID] = d
	}

	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL) // Open for writing
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	// 2. Verify initial data in the downloaded SQLite DB
	verifyDocsInSQLite(t, sqlDB, collectionName, initialDocsMap)

	log.Printf("[%s] Initial sync verification successful!", t.Name())
}

func TestInsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second) // Increased timeout
	defer cancel()

	// 1. Setup: Create collection, add initial doc, get local DB path and initial version
	numInitial := 1
	collectionName := "test_insert_sync_" + uniqueId
	initialDocsSlice, dbPath, initialDbVersion := setupSyncedCollection(t, ctx, numInitial, collectionName)
	require.GreaterOrEqual(t, initialDbVersion, int32(0), "Initial DB version should be valid")

	// Keep track of expected docs in the local DB (start with initial docs)
	expectedDocs := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
	}

	// 2. Actions: Insert new documents into MongoDB
	newDocs := []any{
		TestDoc{ID: uuid.NewString(), Name: "Charlie", Age: 35},
		TestDoc{ID: uuid.NewString(), Name: "David", Age: 40},
	}

	log.Printf("[%s] Inserting %d new documents into MongoDB...", t.Name(), len(newDocs))
	collection := mongoClient.Database("test").Collection(collectionName) // Use correct DB name
	_, err := collection.InsertMany(ctx, newDocs)
	require.NoError(t, err, "Failed to insert new documents into MongoDB")

	// Add new docs to expected state *before* fetching oplogs
	for _, doc := range newDocs {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
	}

	// 3. Wait for oplog events to propagate
	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second) // Adjust delay as needed

	// 4. Fetch Oplog Events SINCE the initial download
	log.Printf("[%s] Getting oplog entries after index %d...", t.Name(), initialDbVersion)
	limit := int32(100) // Fetch a reasonable limit
	oplogResp, err := emcacheClient.GetOplogEntries(ctx, []string{collectionName}, int64(initialDbVersion), limit)
	require.NoError(t, err, "Failed to get oplog entries")
	log.Printf("[%s] Received %d oplog entries.", t.Name(), len(oplogResp.Entries))

	// 5. Apply Oplog events to local DB
	// Open the local SQLite DB for writing
	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL) // Remove OpenReadOnly flag
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	applyOplogEvents(t, sqlDB, collectionName, oplogResp.Entries)
	// We don't strictly need the lastAppliedIndex here if we verify the final state

	// 6. Verify final state of local DB
	log.Printf("[%s] Verifying all documents in SQLite DB...", t.Name())
	verifyDocsInSQLite(t, sqlDB, collectionName, expectedDocs)

	log.Printf("[%s] Insert sync verification successful!", t.Name())
}

func TestUpdateSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// 1. Setup: Create collection, add initial docs, get local DB path and initial version
	numInitial := 2
	collectionName := "test_update_sync_" + uniqueId
	initialDocsSlice, dbPath, initialDbVersion := setupSyncedCollection(t, ctx, numInitial, collectionName)
	require.GreaterOrEqual(t, initialDbVersion, int32(0), "Initial DB version should be valid")

	// Keep track of expected docs in the local DB
	expectedDocs := make(map[string]TestDoc)
	var docToUpdate TestDoc // Keep track of one doc to update
	for i, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
		if i == 0 { // Pick the first doc for updating
			docToUpdate = d
		}
	}
	require.NotEmpty(t, docToUpdate.ID, "Should have selected a document to update")

	// 2. Action: Update an existing document in MongoDB
	updatedName := "Updated " + docToUpdate.Name
	updatedAge := docToUpdate.Age + 10

	log.Printf("[%s] Updating document ID %s in MongoDB...", t.Name(), docToUpdate.ID)
	collection := mongoClient.Database("test").Collection(collectionName) // Use correct DB name
	filter := bson.M{"_id": docToUpdate.ID}
	update := bson.M{"$set": bson.M{"name": updatedName, "age": updatedAge}}
	_, err := collection.UpdateOne(ctx, filter, update)
	require.NoError(t, err, "Failed to update document in MongoDB")

	// Update the expected state *before* fetching oplogs
	expectedDocs[docToUpdate.ID] = TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}

	// 3. Wait for oplog events to propagate
	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second) // Adjust delay as needed

	// 4. Fetch Oplog Events SINCE the initial download
	log.Printf("[%s] Getting oplog entries after index %d...", t.Name(), initialDbVersion)
	limit := int32(100)
	oplogResp, err := emcacheClient.GetOplogEntries(ctx, []string{collectionName}, int64(initialDbVersion), limit)
	require.NoError(t, err, "Failed to get oplog entries")
	log.Printf("[%s] Received %d oplog entries.", t.Name(), len(oplogResp.Entries))

	// 5. Apply Oplog events to local DB
	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL) // Open for writing
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	applyOplogEvents(t, sqlDB, collectionName, oplogResp.Entries)

	// 6. Verify final state of local DB
	log.Printf("[%s] Verifying updated documents in SQLite DB...", t.Name())
	verifyDocsInSQLite(t, sqlDB, collectionName, expectedDocs)

	log.Printf("[%s] Update sync verification successful!", t.Name())
}

func TestDeleteSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// 1. Setup: Create collection with initial data, get local DB path and version
	numInitial := 3
	collectionName := "test_delete_sync_" + uniqueId
	initialDocsSlice, dbPath, initialDbVersion := setupSyncedCollection(t, ctx, numInitial, collectionName)
	require.GreaterOrEqual(t, initialDbVersion, int32(0), "Initial DB version should be valid")

	// Keep track of expected docs (start with all initial docs)
	expectedDocs := make(map[string]TestDoc)
	var docsToDelete []TestDoc // Track docs to delete
	for i, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
		if i < 2 { // Select first 2 docs for deletion
			docsToDelete = append(docsToDelete, d)
		}
	}
	require.Len(t, docsToDelete, 2, "Should have selected documents to delete")

	// 2. Actions: Delete documents from MongoDB
	collection := mongoClient.Database("test").Collection(collectionName)
	for _, doc := range docsToDelete {
		log.Printf("[%s] Deleting document ID %s from MongoDB...", t.Name(), doc.ID)
		filter := bson.M{"_id": doc.ID}
		_, err := collection.DeleteOne(ctx, filter)
		require.NoError(t, err, "Failed to delete document ID %s from MongoDB", doc.ID)

		// Update expected state: remove deleted doc
		delete(expectedDocs, doc.ID)
	}

	// 3. Wait for oplog propagation
	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	// 4. Fetch Oplog Events SINCE initial download
	log.Printf("[%s] Getting oplog entries after index %d...", t.Name(), initialDbVersion)
	limit := int32(100)
	oplogResp, err := emcacheClient.GetOplogEntries(ctx, []string{collectionName}, int64(initialDbVersion), limit)
	require.NoError(t, err, "Failed to get oplog entries")
	log.Printf("[%s] Received %d oplog entries.", t.Name(), len(oplogResp.Entries))

	// 5. Apply Oplog events to local DB
	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL) // Open for writing
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	applyOplogEvents(t, sqlDB, collectionName, oplogResp.Entries)

	// 6. Verify final state of local DB (deleted docs should be gone)
	log.Printf("[%s] Verifying documents after deletion in SQLite DB...", t.Name())
	verifyDocsInSQLite(t, sqlDB, collectionName, expectedDocs) // expectedDocs now only contains the non-deleted ones

	log.Printf("[%s] Delete sync verification successful!", t.Name())
}

func TestUpsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// 1. Setup: Create collection with initial data, get local DB path and version
	numInitial := 2
	collectionName := "test_upsert_sync_" + uniqueId
	initialDocsSlice, dbPath, initialDbVersion := setupSyncedCollection(t, ctx, numInitial, collectionName)
	require.GreaterOrEqual(t, initialDbVersion, int32(0), "Initial DB version should be valid")

	// Keep track of expected docs
	expectedDocs := make(map[string]TestDoc)
	var docToUpdate TestDoc
	for i, doc := range initialDocsSlice {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
		if i == 0 {
			docToUpdate = d // Select the first doc for an update via upsert
		}
	}

	// 2. Actions: Perform upsert operations (one update, one insert)
	collection := mongoClient.Database("test").Collection(collectionName)
	upsertOpts := options.Replace().SetUpsert(true)

	// Upsert 1: Update existing document
	updatedName := "Upserted " + docToUpdate.Name
	updatedAge := docToUpdate.Age + 5
	updatedDoc := TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}
	filterUpdate := bson.M{"_id": docToUpdate.ID}
	log.Printf("[%s] Upserting (update) document ID %s in MongoDB...", t.Name(), docToUpdate.ID)
	_, err := collection.ReplaceOne(ctx, filterUpdate, updatedDoc, upsertOpts)
	require.NoError(t, err, "Failed to upsert (update) document ID %s", docToUpdate.ID)
	expectedDocs[docToUpdate.ID] = updatedDoc // Update expected state

	// Upsert 2: Insert new document
	newDocID := uuid.NewString()
	newDoc := TestDoc{ID: newDocID, Name: "New Upserted Doc", Age: 55}
	filterInsert := bson.M{"_id": newDocID}
	log.Printf("[%s] Upserting (insert) document ID %s in MongoDB...", t.Name(), newDocID)
	_, err = collection.ReplaceOne(ctx, filterInsert, newDoc, upsertOpts)
	require.NoError(t, err, "Failed to upsert (insert) document ID %s", newDocID)
	expectedDocs[newDocID] = newDoc // Add new doc to expected state

	// 3. Wait for oplog propagation
	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	// 4. Fetch Oplog Events SINCE initial download
	log.Printf("[%s] Getting oplog entries after index %d...", t.Name(), initialDbVersion)
	limit := int32(100)
	oplogResp, err := emcacheClient.GetOplogEntries(ctx, []string{collectionName}, int64(initialDbVersion), limit)
	require.NoError(t, err, "Failed to get oplog entries")
	log.Printf("[%s] Received %d oplog entries.", t.Name(), len(oplogResp.Entries))

	// 5. Apply Oplog events to local DB
	sqlDB, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenWAL) // Open for writing
	require.NoError(t, err, "Failed to open SQLite DB for oplog application")
	defer sqlDB.Close()

	applyOplogEvents(t, sqlDB, collectionName, oplogResp.Entries)

	// 6. Verify final state of local DB
	log.Printf("[%s] Verifying documents after upserts in SQLite DB...", t.Name())
	verifyDocsInSQLite(t, sqlDB, collectionName, expectedDocs)

	log.Printf("[%s] Upsert sync verification successful!", t.Name())
}
