package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"

	// Use the local emcache package instead of the old gRPC client
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

func createClient(t *testing.T, ctx context.Context, collections ...string) (*emcache.Client, error) {
	t.Helper()

	clientCtx, clientCancel := context.WithTimeout(ctx, 15*time.Second)
	defer clientCancel()

	client, err := emcache.NewClient(clientCtx, emcache.Config{
		ServerURL:    emcacheServerURL,
		Directory:    t.TempDir(),
		Collections:  collections,
		SyncInterval: 1 * time.Second,
		BatchSize:    100,
	})

	if err != nil {
		return nil, err
	}

	err = client.StartSync()
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		client.StopSync()
		client.Close()
	})

	return client, nil
}

// setupSyncedCollection creates a collection with default shape and initial docs
func setupSyncedCollection(t *testing.T, ctx context.Context, numInitialDocs int, collectionName string) ([]TestDoc, *emcache.Client) {
	t.Helper()
	initialDocs, emcacheClient := setupCollectionWithShape(t, ctx, numInitialDocs, collectionName, defaultTestDocShape())
	return initialDocs, emcacheClient
}

func verifyDocsInSQLite(t *testing.T, client *emcache.Client, collectionName string, expectedDocs map[string]TestDoc) {
	t.Helper()
	ctx := context.Background()
	query := "SELECT id, name, age FROM data"
	foundDocs := make(map[string]TestDoc)

	db, err := client.DB(ctx, collectionName)
	require.NoError(t, err, "Failed to get database for collection")

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

func setupCollectionWithShape(t *testing.T, ctx context.Context, numInitialDocs int, collectionName string, shape emcache.Shape) ([]TestDoc, *emcache.Client) {
	t.Helper()

	initialDocs := make([]TestDoc, 0, numInitialDocs)
	for i := 0; i < numInitialDocs; i++ {
		initialDocs = append(initialDocs, TestDoc{
			ID:   uuid.NewString(),
			Name: fmt.Sprintf("InitialDoc_%d_%s", i, uuid.NewString()[:4]),
			Age:  20 + i,
		})
	}

	client := setupCollectionWithShapeAndDocs(t, ctx, collectionName, shape, initialDocs)
	return initialDocs, client
}

func setupCollectionWithShapeAndDocs[T any](t *testing.T, ctx context.Context, collectionName string, shape emcache.Shape, initialDocs []T) *emcache.Client {
	collection := mongoClient.Database(mongoDBName).Collection(collectionName)
	emcacheAdminClient, err := createClient(t, ctx)
	require.NoError(t, err, "Failed to create emcache client")

	_, err = emcacheAdminClient.CreateEntity(ctx, emcache.CreateEntityRequest{
		Name:   collectionName,
		Client: "main",
		Source: collectionName,
		Shape:  shape,
	})
	require.NoError(t, err, "Failed to call AddEntity with custom shape before client init")

	t.Cleanup(func() {
		err := emcacheAdminClient.RemoveEntity(context.Background(), collectionName)
		require.NoError(t, err, "Failed to remove collection from Emcache")

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

	var emcacheClient *emcache.Client
	waitForSync(t, func() error {
		emcacheClient, err = createClient(t, ctx, collectionName)
		return err
	})
	require.NoError(t, err, "Failed to create emcache client")

	return emcacheClient
}

func waitForSync(t *testing.T, operation func() error) {
	t.Helper()

	maxWaitTime := 20 * time.Second
	deadline := time.Now().Add(maxWaitTime)
	backoff := 200 * time.Millisecond
	var lastErr error

	for time.Now().Before(deadline) {
		time.Sleep(backoff)

		err := operation()
		if err == nil {
			return // Success
		}

		lastErr = err
		newBackoff := time.Duration(float64(backoff) * 1.5)
		if newBackoff > 3*time.Second {
			backoff = 3 * time.Second
		} else {
			backoff = newBackoff
		}
	}

	require.NoError(t, lastErr, "Failed to perform operation within timeout")
}
