package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"

	"emcache"
)

const (
	emcacheServerURL = "http://localhost:8080"
	testTimeout      = 45 * time.Second
	syncTimeout      = 10 * time.Second
	mongoDBName      = "test"
)

var (
	mongoClient *mongo.Client
)

type TestDoc struct {
	ID   string `bson:"_id" json:"_id"`
	Name string `bson:"name" json:"name"`
	Age  int    `bson:"age" json:"age"`
}

func defaultTestDocShape() emcache.Shape {
	return emcache.Shape{
		IdColumn: emcache.IdColumn{
			Path: "$._id",
			Type: emcache.IdTypeString,
		},
		Columns: []emcache.Column{
			{Name: "name", Type: emcache.DataTypeString, Path: "$.name"},
			{Name: "age", Type: emcache.DataTypeInteger, Path: "$.age"},
		},
	}
}

func syncToLatest(t *testing.T, client *emcache.Client) {
	t.Helper()
	ctx := context.Background()

	time.Sleep(3 * time.Second)
	err := client.SyncOnce(ctx)
	require.NoError(t, err, "Failed to sync to latest")
}

func createClient(t *testing.T, ctx context.Context, entities ...string) (*emcache.Client, error) {
	t.Helper()

	clientCtx, clientCancel := context.WithTimeout(ctx, 15*time.Second)
	defer clientCancel()

	client := emcache.NewClient(clientCtx, emcache.Config{
		ServerURL:    emcacheServerURL,
		Directory:    t.TempDir(),
		Entities:     entities,
		SyncInterval: 1 * time.Second,
		BatchSize:    100,
	})

	if len(entities) > 0 {
		err := client.Initialize(clientCtx)
		require.NoError(t, err, "Failed to initialize emcache client")
		err = client.SyncOnce(ctx)
		require.NoError(t, err, "Failed to sync to latest")
		err = client.StartSync()
		require.NoError(t, err, "Failed to start sync")
	}

	t.Cleanup(func() {
		client.StopSync()
		client.Close()
	})

	return client, nil
}

// setupSyncedEntity creates an entity with default shape and initial docs
func setupSyncedEntity(t *testing.T, ctx context.Context, numInitialDocs int, entityName string) ([]TestDoc, *emcache.Client) {
	t.Helper()
	initialDocs, emcacheClient := setupEntityWithShape(t, ctx, numInitialDocs, entityName, defaultTestDocShape())
	return initialDocs, emcacheClient
}

func verifyDocsInSQLite(t *testing.T, client *emcache.Client, entityName string, expectedDocs map[string]TestDoc) {
	t.Helper()
	ctx := context.Background()
	query := "SELECT id, name, age FROM data"
	foundDocs := make(map[string]TestDoc)

	db, err := client.DB(ctx, entityName)
	require.NoError(t, err, "Failed to get database for entity")

	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err, "Failed to execute query via client")
	defer rows.Close()

	for rows.Next() {
		var id, name string
		var age int
		err := rows.Scan(&id, &name, &age)
		require.NoError(t, err, "Failed to scan row")

		foundDocs[id] = TestDoc{ID: id, Name: name, Age: age}
	}
	require.NoError(t, rows.Err(), "Error iterating rows")

	require.Equal(t, len(expectedDocs), len(foundDocs),
		"Number of documents in Client DB (%d) does not match expected count (%d)", len(foundDocs), len(expectedDocs))

	for id, expected := range expectedDocs {
		found, ok := foundDocs[id]
		require.True(t, ok, "Expected document with ID %s not found in Client DB", id)
		require.Equal(t, expected.Name, found.Name,
			"Name mismatch for doc ID %s. Expected: %s, Found: %s", id, expected.Name, found.Name)
		require.Equal(t, expected.Age, found.Age,
			"Age mismatch for doc ID %s. Expected: %d, Found: %d", id, expected.Age, found.Age)
	}
}

func nestedFieldsDocShape() emcache.Shape {
	return emcache.Shape{
		IdColumn: emcache.IdColumn{
			Path: "$._id",
			Type: emcache.IdTypeString,
		},
		Columns: []emcache.Column{
			{Name: "name", Type: emcache.DataTypeString, Path: "$.name"},
			{Name: "age", Type: emcache.DataTypeInteger, Path: "$.age"},
			{Name: "email", Type: emcache.DataTypeString, Path: "$.contact_info.email"},
			{Name: "phone", Type: emcache.DataTypeString, Path: "$.contact_info.phone"},
			{Name: "street", Type: emcache.DataTypeString, Path: "$.contact_info.address.street"},
			{Name: "city", Type: emcache.DataTypeString, Path: "$.contact_info.address.city"},
			{Name: "zip", Type: emcache.DataTypeString, Path: "$.contact_info.address.zip"},
			{Name: "tags", Type: emcache.DataTypeJsonb, Path: "$.tags"},
		},
	}
}

func setupEntityWithShape(t *testing.T, ctx context.Context, numInitialDocs int, entityName string, shape emcache.Shape) ([]TestDoc, *emcache.Client) {
	t.Helper()

	initialDocs := make([]TestDoc, 0, numInitialDocs)
	for i := 0; i < numInitialDocs; i++ {
		initialDocs = append(initialDocs, TestDoc{
			ID:   uuid.NewString(),
			Name: fmt.Sprintf("InitialDoc_%d_%s", i, uuid.NewString()[:4]),
			Age:  20 + i,
		})
	}

	client := setupEntityWithShapeAndDocs(t, ctx, entityName, shape, initialDocs)
	return initialDocs, client
}

func setupEntityWithShapeAndDocs[T any](t *testing.T, ctx context.Context, entityName string, shape emcache.Shape, initialDocs []T) *emcache.Client {
	collection := mongoClient.Database(mongoDBName).Collection(entityName)
	emcacheAdminClient, err := createClient(t, ctx)
	require.NoError(t, err, "Failed to create emcache client")

	_, err = emcacheAdminClient.CreateEntity(ctx, emcache.CreateEntityRequest{
		Name:   entityName,
		Client: "main",
		Source: entityName,
		Shape:  shape,
	})
	require.NoError(t, err, "Failed to call AddEntity with custom shape before client init")

	t.Cleanup(func() {
		err := emcacheAdminClient.RemoveEntity(context.Background(), entityName)
		require.NoError(t, err, "Failed to remove entity from Emcache")

		err = collection.Drop(context.Background())
		require.NoError(t, err, "Failed to drop collection from MongoDB")
	})

	if len(initialDocs) > 0 {
		docs := make([]any, len(initialDocs))
		for i, doc := range initialDocs {
			docs[i] = doc
		}
		_, err := collection.InsertMany(ctx, docs)
		require.NoError(t, err, "Failed to insert initial documents into MongoDB")
		time.Sleep(5 * time.Second)
	}

	emcacheClient, err := createClient(t, ctx, entityName)
	require.NoError(t, err, "Failed to create emcache client")

	return emcacheClient
}
