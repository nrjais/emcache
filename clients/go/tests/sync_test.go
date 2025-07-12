package tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

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

	clientOptions := options.Client().ApplyURI(mongoURI)
	mongoClient, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Printf("Could not connect to mongo: %v", err)
		os.Exit(1)
	}
	defer func() {
		if err = mongoClient.Disconnect(ctx); err != nil {
			log.Printf("Could not disconnect mongo client: %v", err)
		}
	}()
	if err = mongoClient.Ping(ctx, nil); err != nil {
		log.Printf("Could not ping mongo: %v", err)
		os.Exit(1)
	}

	code := m.Run()

	os.Exit(code)
}

func setupTestEntity(t *testing.T, numDocs int) (string, []TestDoc, *mongo.Collection, map[string]TestDoc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName := fmt.Sprintf("test_%s_%s", t.Name(), uniqueId)
	initialDocsSlice, _ := setupSyncedEntity(t, ctx, numDocs, entityName)

	expectedDocs := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		expectedDocs[doc.ID] = doc
	}

	collection := mongoClient.Database(mongoDBName).Collection(entityName)

	return entityName, initialDocsSlice, collection, expectedDocs
}

func TestInitialSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName := "test_initial_sync_" + uniqueId
	numInitial := 2
	initialDocsSlice, emcacheClient := setupSyncedEntity(t, ctx, numInitial, entityName)

	initialDocsMap := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		initialDocsMap[doc.ID] = doc
	}

	verifyDocsInSQLite(t, emcacheClient, entityName, initialDocsMap)
}

func TestEmptyEntitySync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName := "test_empty_entity_sync_" + uniqueId
	numInitial := 0
	initialDocsSlice, emcacheClient := setupSyncedEntity(t, ctx, numInitial, entityName)

	initialDocsMap := make(map[string]TestDoc)
	for _, doc := range initialDocsSlice {
		initialDocsMap[doc.ID] = doc
	}

	verifyDocsInSQLite(t, emcacheClient, entityName, nil)
}

func TestInsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName, _, collection, expectedDocs := setupTestEntity(t, 1)
	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	newDocs := []any{
		TestDoc{ID: uuid.NewString(), Name: "Charlie", Age: 35},
		TestDoc{ID: uuid.NewString(), Name: "David", Age: 40},
	}

	_, err = collection.InsertMany(ctx, newDocs)
	require.NoError(t, err, "Failed to insert new documents into MongoDB")

	for _, doc := range newDocs {
		d := doc.(TestDoc)
		expectedDocs[d.ID] = d
	}

	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)
}

func TestUpdateSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName, initialDocs, collection, expectedDocs := setupTestEntity(t, 2)
	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	docToUpdate := initialDocs[0]
	updatedName := "Updated " + docToUpdate.Name
	updatedAge := docToUpdate.Age + 10

	filter := bson.M{"_id": docToUpdate.ID}
	update := bson.M{"$set": bson.M{"name": updatedName, "age": updatedAge}}
	_, err = collection.UpdateOne(ctx, filter, update)
	require.NoError(t, err, "Failed to update document in MongoDB")

	expectedDocs[docToUpdate.ID] = TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}

	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)
}

func TestDeleteSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName, initialDocs, collection, expectedDocs := setupTestEntity(t, 3)
	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	docsToDelete := initialDocs[:2]
	require.Len(t, docsToDelete, 2, "Should have selected documents to delete")

	for _, doc := range docsToDelete {
		filter := bson.M{"_id": doc.ID}
		_, err = collection.DeleteOne(ctx, filter)
		require.NoError(t, err, "Failed to delete document from MongoDB")

		delete(expectedDocs, doc.ID)
	}

	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)
}

func TestUpsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName, initialDocs, collection, expectedDocs := setupTestEntity(t, 2)
	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	docToUpdate := initialDocs[0]
	updatedName := "Upserted " + docToUpdate.Name
	updatedAge := docToUpdate.Age + 5
	updatedDoc := TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}

	upsertOpts := options.Replace().SetUpsert(true)
	filterUpdate := bson.M{"_id": docToUpdate.ID}

	_, err = collection.ReplaceOne(ctx, filterUpdate, updatedDoc, upsertOpts)
	require.NoError(t, err, "Failed to upsert (update) document")
	expectedDocs[docToUpdate.ID] = updatedDoc

	newDocID := uuid.NewString()
	newDoc := TestDoc{ID: newDocID, Name: "New Upserted Doc", Age: 55}
	filterInsert := bson.M{"_id": newDocID}

	_, err = collection.ReplaceOne(ctx, filterInsert, newDoc, upsertOpts)
	require.NoError(t, err, "Failed to upsert (insert) document")
	expectedDocs[newDocID] = newDoc

	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)
}

func TestPartialUpdateSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName, initialDocs, collection, expectedDocs := setupTestEntity(t, 3)
	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	docToUpdate := initialDocs[0]
	filter := bson.M{"_id": docToUpdate.ID}

	updatedName := "Partially Updated " + docToUpdate.Name
	update := bson.M{"$set": bson.M{"name": updatedName}}
	_, err = collection.UpdateOne(ctx, filter, update)
	require.NoError(t, err, "Failed to partially update document")

	expectedDocs[docToUpdate.ID] = TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: docToUpdate.Age}
	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)

	updatedAge := docToUpdate.Age + 15
	update = bson.M{"$set": bson.M{"age": updatedAge}}
	_, err = collection.UpdateOne(ctx, filter, update)
	require.NoError(t, err, "Failed to partially update document")

	expectedDocs[docToUpdate.ID] = TestDoc{ID: docToUpdate.ID, Name: updatedName, Age: updatedAge}
	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)

}

func TestDeleteAllSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName, _, collection, _ := setupTestEntity(t, 5)
	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	filter := bson.M{}
	_, err = collection.DeleteMany(ctx, filter)
	require.NoError(t, err, "Failed to delete all documents from MongoDB")

	expectedDocs := make(map[string]TestDoc)

	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)
}

func TestBulkInsertSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName, _, collection, expectedDocs := setupTestEntity(t, 2)
	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	bulkSize := 100
	newDocs := make([]any, bulkSize)
	for i := 0; i < bulkSize; i++ {
		doc := TestDoc{ID: uuid.NewString(), Name: fmt.Sprintf("BulkDoc_%d", i), Age: 30 + i}
		newDocs[i] = doc
		expectedDocs[doc.ID] = doc
	}

	_, err = collection.InsertMany(ctx, newDocs)
	require.NoError(t, err, "Failed to insert bulk documents into MongoDB")

	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)
}

func TestReplaceDocSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName, initialDocs, collection, expectedDocs := setupTestEntity(t, 3)
	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	docToReplace := initialDocs[0]
	replacementDoc := TestDoc{
		ID:   docToReplace.ID,
		Name: "Replaced " + docToReplace.Name,
		Age:  docToReplace.Age * 2,
	}

	filter := bson.M{"_id": docToReplace.ID}
	_, err = collection.ReplaceOne(ctx, filter, replacementDoc)
	require.NoError(t, err, "Failed to replace document in MongoDB")

	expectedDocs[docToReplace.ID] = replacementDoc

	syncToLatest(t, emcacheClient)
	verifyDocsInSQLite(t, emcacheClient, entityName, expectedDocs)
}

func TestNestedFieldsSync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	entityName := "test_nested_fields_sync_" + uniqueId

	type Address struct {
		Street string `bson:"street" json:"street"`
		City   string `bson:"city" json:"city"`
		Zip    string `bson:"zip" json:"zip"`
	}

	type ContactInfo struct {
		Email   string  `bson:"email" json:"email"`
		Phone   string  `bson:"phone" json:"phone"`
		Address Address `bson:"address" json:"address"`
	}

	type NestedDoc struct {
		ID          string      `bson:"_id" json:"_id"`
		Name        string      `bson:"name" json:"name"`
		Age         int         `bson:"age" json:"age"`
		ContactInfo ContactInfo `bson:"contact_info" json:"contact_info"`
		Tags        []string    `bson:"tags" json:"tags"`
	}

	docID := uuid.NewString()
	nestedDoc := NestedDoc{
		ID:   docID,
		Name: "Nested Fields Test",
		Age:  30,
		ContactInfo: ContactInfo{
			Email: "test@example.com",
			Phone: "555-1234",
			Address: Address{
				Street: "123 Main St",
				City:   "Testville",
				Zip:    "12345",
			},
		},
		Tags: []string{"test", "nested", "document"},
	}

	emcacheClient := setupEntityWithShapeAndDocs(t, ctx, entityName, nestedFieldsDocShape(), []NestedDoc{nestedDoc})

	collection := mongoClient.Database(mongoDBName).Collection(entityName)

	query := `SELECT id, name, age, email, phone, street, city, zip, tags FROM data WHERE id = ?`
	db, err := emcacheClient.DB(ctx, entityName)
	require.NoError(t, err, "Failed to get database for entity")

	rows, err := db.QueryContext(ctx, query, docID)
	require.NoError(t, err, "Failed to execute query via client")
	defer rows.Close()

	found := false
	for rows.Next() {
		var id, name, email, phone, street, city, zip, tags string
		var age int

		err := rows.Scan(&id, &name, &age, &email, &phone, &street, &city, &zip, &tags)
		require.NoError(t, err, "Failed to scan row")

		found = true
		require.Equal(t, nestedDoc.Name, name, "Name field mismatch")
		require.Equal(t, nestedDoc.Age, age, "Age field mismatch")
		require.Equal(t, nestedDoc.ContactInfo.Email, email, "Email field mismatch")
		require.Equal(t, nestedDoc.ContactInfo.Phone, phone, "Phone field mismatch")
		require.Equal(t, nestedDoc.ContactInfo.Address.Street, street, "Street field mismatch")
		require.Equal(t, nestedDoc.ContactInfo.Address.City, city, "City field mismatch")
		require.Equal(t, nestedDoc.ContactInfo.Address.Zip, zip, "Zip field mismatch")

		for _, tag := range nestedDoc.Tags {
			require.Contains(t, tags, tag, "Tags field should contain tag %s", tag)
		}
	}
	require.NoError(t, rows.Err(), "Error iterating rows")
	require.True(t, found, "Document with nested fields not found in Client DB")

	filter := bson.M{"_id": docID}
	update := bson.M{"$set": bson.M{"contact_info.address.city": "New City"}}
	_, err = collection.UpdateOne(ctx, filter, update)
	require.NoError(t, err, "Failed to update nested field")

	syncToLatest(t, emcacheClient)

	db, err = emcacheClient.DB(ctx, entityName)
	require.NoError(t, err, "Failed to get database for entity")

	rows, err = db.QueryContext(ctx, query, docID)
	require.NoError(t, err, "Failed to execute query via client")
	defer rows.Close()

	found = false
	for rows.Next() {
		var id, name, email, phone, street, city, zip, tags string
		var age int

		err := rows.Scan(&id, &name, &age, &email, &phone, &street, &city, &zip, &tags)
		require.NoError(t, err, "Failed to scan row")

		found = true
		require.Equal(t, "New City", city, "Updated city field mismatch")
	}
	require.NoError(t, rows.Err(), "Error iterating rows")
	require.True(t, found, "Document with updated nested field not found in Client DB")

}
