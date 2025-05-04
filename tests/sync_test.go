package tests

import (
	"context"
	"log"
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

	log.Println("Starting E2E tests...")
	code := m.Run()
	log.Println("E2E tests finished.")

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

	log.Printf("[%s] Initial sync verification successful!", t.Name())
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

	log.Printf("[%s] Inserting %d new documents into MongoDB...", t.Name(), len(newDocs))
	collection := mongoClient.Database("test").Collection(collectionName)
	_, err = collection.InsertMany(ctx, newDocs)
	require.NoError(t, err, "Failed to insert new documents into MongoDB")

	for _, doc := range newDocs {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
	}

	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	log.Printf("[%s] Calling SyncToLatest...", t.Name())
	err = emcacheClient.SyncToLatest(ctx, 10)
	require.NoError(t, err, "Failed to sync client to latest")

	log.Printf("[%s] Verifying all documents in Client DB...", t.Name())
	verifyDocsInSQLite(t, emcacheClient, collectionName, expectedDocs)

	log.Printf("[%s] Insert sync verification successful!", t.Name())
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

	log.Printf("[%s] Updating document ID %s in MongoDB...", t.Name(), docToUpdate.ID)
	collection := mongoClient.Database("test").Collection(collectionName)
	filter := bson.M{"_id": docToUpdate.ID}
	update := bson.M{"$set": bson.M{"name": updatedName, "age": updatedAge}}
	_, err = collection.UpdateOne(ctx, filter, update)
	require.NoError(t, err, "Failed to update document in MongoDB")

	expectedDocs[docToUpdate.ID] = TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}

	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	log.Printf("[%s] Calling SyncToLatest...", t.Name())
	err = emcacheClient.SyncToLatest(ctx, 10)
	require.NoError(t, err, "Failed to sync client to latest")

	log.Printf("[%s] Verifying updated documents in Client DB...", t.Name())
	verifyDocsInSQLite(t, emcacheClient, collectionName, expectedDocs)

	log.Printf("[%s] Update sync verification successful!", t.Name())
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
		log.Printf("[%s] Deleting document ID %s from MongoDB...", t.Name(), doc.ID)
		filter := bson.M{"_id": doc.ID}
		_, err = collection.DeleteOne(ctx, filter)
		require.NoError(t, err, "Failed to delete document ID %s from MongoDB", doc.ID)

		delete(expectedDocs, doc.ID)
	}

	log.Printf("[%s] Waiting for oplog propagation...", t.Name())
	time.Sleep(5 * time.Second)

	log.Printf("[%s] Calling SyncToLatest...", t.Name())
	err = emcacheClient.SyncToLatest(ctx, 10)
	require.NoError(t, err, "Failed to sync client to latest")

	log.Printf("[%s] Verifying documents after deletion in Client DB...", t.Name())
	verifyDocsInSQLite(t, emcacheClient, collectionName, expectedDocs)

	log.Printf("[%s] Delete sync verification successful!", t.Name())
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
	log.Printf("[%s] Upserting (update) document ID %s in MongoDB...", t.Name(), docToUpdate.ID)
	_, err = collection.ReplaceOne(ctx, filterUpdate, updatedDoc, upsertOpts)
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

	log.Printf("[%s] Calling SyncToLatest...", t.Name())
	err = emcacheClient.SyncToLatest(ctx, 10)
	require.NoError(t, err, "Failed to sync client to latest")

	log.Printf("[%s] Verifying documents after upserts in Client DB...", t.Name())
	verifyDocsInSQLite(t, emcacheClient, collectionName, expectedDocs)

	log.Printf("[%s] Upsert sync verification successful!", t.Name())
}
