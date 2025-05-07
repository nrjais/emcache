package tests

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoURI = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
)

var uniqueId = primitive.NewObjectID().Hex()

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error

	slog.Info("Connecting to MongoDB", "uri", mongoURI)
	clientOptions := options.Client().ApplyURI(mongoURI)
	mongoClient, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		slog.Error("Could not connect to mongo", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err = mongoClient.Disconnect(ctx); err != nil {
			slog.Error("Could not disconnect mongo client", "error", err)
		}
	}()
	if err = mongoClient.Ping(ctx, nil); err != nil {
		slog.Error("Could not ping mongo", "error", err)
		os.Exit(1)
	}
	slog.Info("Successfully connected to MongoDB")

	slog.Info("Starting E2E tests")
	code := m.Run()
	slog.Info("E2E tests finished")

	os.Exit(code)
}

func TestInitialSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	collectionName := "test_initial_sync_" + uniqueId
	numInitial := 2
	initialDocsSlice, emcacheClient := setupSyncedCollection(t, ctx, numInitial, collectionName)

	initialDocsMap := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		initialDocsMap[doc.ID] = doc
	}

	verifyDocsInSQLite(t, emcacheClient, collectionName, initialDocsMap)

	slog.Info("Initial sync verification successful",
		"test", t.Name())
}

func TestInsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	numInitial := 1
	collectionName := "test_insert_sync_" + uniqueId
	initialDocsSlice, emcacheClient := setupSyncedCollection(t, ctx, numInitial, collectionName)

	var err error
	expectedDocs := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		expectedDocs[doc.ID] = doc
	}

	newDocs := []any{
		TestDoc{ID: uuid.NewString(), Name: "Charlie", Age: 35},
		TestDoc{ID: uuid.NewString(), Name: "David", Age: 40},
	}

	slog.Info("Inserting new documents into MongoDB",
		"test", t.Name(),
		"count", len(newDocs))
	collection := mongoClient.Database("test").Collection(collectionName)
	_, err = collection.InsertMany(ctx, newDocs)
	require.NoError(t, err, "Failed to insert new documents into MongoDB")

	for _, doc := range newDocs {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
	}

	slog.Info("Waiting for oplog propagation",
		"test", t.Name())
	time.Sleep(5 * time.Second)

	slog.Info("Calling SyncToLatest",
		"test", t.Name())
	err = emcacheClient.SyncToLatest(ctx, 10)
	require.NoError(t, err, "Failed to sync client to latest")

	slog.Info("Verifying all documents in Client DB",
		"test", t.Name())
	verifyDocsInSQLite(t, emcacheClient, collectionName, expectedDocs)

	slog.Info("Insert sync verification successful",
		"test", t.Name())
}

func TestUpdateSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	numInitial := 2
	collectionName := "test_update_sync_" + uniqueId
	initialDocsSlice, emcacheClient := setupSyncedCollection(t, ctx, numInitial, collectionName)

	var err error
	expectedDocs := make(map[string]TestDoc)
	var docToUpdate TestDoc
	for i, doc := range initialDocsSlice {
		expectedDocs[doc.ID] = doc
		if i == 0 {
			docToUpdate = doc
		}
	}
	require.NotEmpty(t, docToUpdate.ID, "Should have selected a document to update")

	updatedName := "Updated " + docToUpdate.Name
	updatedAge := docToUpdate.Age + 10

	slog.Info("Updating document in MongoDB",
		"test", t.Name(),
		"doc_id", docToUpdate.ID)
	collection := mongoClient.Database("test").Collection(collectionName)
	filter := bson.M{"_id": docToUpdate.ID}
	update := bson.M{"$set": bson.M{"name": updatedName, "age": updatedAge}}
	_, err = collection.UpdateOne(ctx, filter, update)
	require.NoError(t, err, "Failed to update document in MongoDB")

	expectedDocs[docToUpdate.ID] = TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}

	slog.Info("Waiting for oplog propagation",
		"test", t.Name())
	time.Sleep(5 * time.Second)

	slog.Info("Calling SyncToLatest",
		"test", t.Name())
	err = emcacheClient.SyncToLatest(ctx, 10)
	require.NoError(t, err, "Failed to sync client to latest")

	slog.Info("Verifying updated documents in Client DB",
		"test", t.Name())
	verifyDocsInSQLite(t, emcacheClient, collectionName, expectedDocs)

	slog.Info("Update sync verification successful",
		"test", t.Name())
}

func TestDeleteSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	numInitial := 3
	collectionName := "test_delete_sync_" + uniqueId
	initialDocsSlice, emcacheClient := setupSyncedCollection(t, ctx, numInitial, collectionName)

	var err error // Declare err for the function scope
	expectedDocs := make(map[string]TestDoc)
	var docsToDelete []TestDoc
	for i, doc := range initialDocsSlice {
		expectedDocs[doc.ID] = doc
		if i < 2 {
			docsToDelete = append(docsToDelete, doc)
		}
	}
	require.Len(t, docsToDelete, 2, "Should have selected documents to delete")

	collection := mongoClient.Database("test").Collection(collectionName)
	for _, doc := range docsToDelete {
		slog.Info("Deleting document from MongoDB",
			"test", t.Name(),
			"doc_id", doc.ID)
		filter := bson.M{"_id": doc.ID}
		_, err = collection.DeleteOne(ctx, filter)
		require.NoError(t, err, "Failed to delete document ID %s from MongoDB", doc.ID)

		delete(expectedDocs, doc.ID)
	}

	slog.Info("Waiting for oplog propagation",
		"test", t.Name())
	time.Sleep(5 * time.Second)

	slog.Info("Calling SyncToLatest",
		"test", t.Name())
	err = emcacheClient.SyncToLatest(ctx, 10)
	require.NoError(t, err, "Failed to sync client to latest")

	slog.Info("Verifying documents after deletion in Client DB",
		"test", t.Name())
	verifyDocsInSQLite(t, emcacheClient, collectionName, expectedDocs)

	slog.Info("Delete sync verification successful",
		"test", t.Name())
}

func TestUpsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	numInitial := 2
	collectionName := "test_upsert_sync_" + uniqueId
	initialDocsSlice, emcacheClient := setupSyncedCollection(t, ctx, numInitial, collectionName)

	var err error
	expectedDocs := make(map[string]TestDoc)
	var docToUpdate TestDoc
	for i, doc := range initialDocsSlice {
		expectedDocs[doc.ID] = doc
		if i == 0 {
			docToUpdate = doc
		}
	}

	collection := mongoClient.Database("test").Collection(collectionName)
	upsertOpts := options.Replace().SetUpsert(true)

	updatedName := "Upserted " + docToUpdate.Name
	updatedAge := docToUpdate.Age + 5
	updatedDoc := TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}
	filterUpdate := bson.M{"_id": docToUpdate.ID}
	slog.Info("Upserting (update) document in MongoDB",
		"test", t.Name(),
		"doc_id", docToUpdate.ID)
	_, err = collection.ReplaceOne(ctx, filterUpdate, updatedDoc, upsertOpts)
	require.NoError(t, err, "Failed to upsert (update) document ID %s", docToUpdate.ID)
	expectedDocs[docToUpdate.ID] = updatedDoc

	newDocID := uuid.NewString()
	newDoc := TestDoc{ID: newDocID, Name: "New Upserted Doc", Age: 55}
	filterInsert := bson.M{"_id": newDocID}
	slog.Info("Upserting (insert) document in MongoDB",
		"test", t.Name(),
		"doc_id", newDocID)
	_, err = collection.ReplaceOne(ctx, filterInsert, newDoc, upsertOpts)
	require.NoError(t, err, "Failed to upsert (insert) document ID %s", newDocID)
	expectedDocs[newDocID] = newDoc

	slog.Info("Waiting for oplog propagation",
		"test", t.Name())
	time.Sleep(5 * time.Second)

	slog.Info("Calling SyncToLatest",
		"test", t.Name())
	err = emcacheClient.SyncToLatest(ctx, 10)
	require.NoError(t, err, "Failed to sync client to latest")

	slog.Info("Verifying documents after upserts in Client DB",
		"test", t.Name())
	verifyDocsInSQLite(t, emcacheClient, collectionName, expectedDocs)
}
