package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	emclient "github.com/nrjais/emcache/pkg/client"
	pb "github.com/nrjais/emcache/pkg/protos"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	emcacheAddr = "localhost:50051"
	testTimeout = 45 * time.Second
	syncTimeout = 10 * time.Second
	mongoDBName = "test"
)

var (
	mongoClient *mongo.Client
)

type TestDoc struct {
	ID   string `bson:"_id" json:"_id"`
	Name string `bson:"name" json:"name"`
	Age  int    `bson:"age" json:"age"`
}

func defaultTestDocShape() *pb.Shape {
	return &pb.Shape{
		Columns: []*pb.Column{
			{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
			{Name: "age", Type: pb.DataType_INTEGER, Path: "age"},
		},
	}
}

func syncToLatest(t *testing.T, client *emclient.Client) {
	t.Helper()
	ctx := context.Background()

	time.Sleep(3 * time.Second)
	err := client.SyncToLatest(ctx, 1000)
	require.NoError(t, err, "Failed to sync to latest")
}

func createClient(t *testing.T, ctx context.Context, collections ...string) (*emclient.Client, error) {
	t.Helper()

	clientCtx, clientCancel := context.WithTimeout(ctx, 15*time.Second)
	defer clientCancel()

	client, err := emclient.NewClient(clientCtx, emclient.ClientConfig{
		ServerAddr: emcacheAddr,
		Directory:  t.TempDir(),
		Collections: lo.Map(collections, func(collection string, _ int) emclient.CollectionConfig {
			return emclient.CollectionConfig{Name: collection}
		}),
		UpdateInterval: 1 * time.Second,
		BatchSize:      100,
	})

	if err != nil {
		return nil, err
	}

	client.StartDbUpdates()
	t.Cleanup(func() {
		client.StopUpdates()
	})

	return client, nil
}

// setupSyncedCollection creates a collection with default shape and initial docs
func setupSyncedCollection(t *testing.T, ctx context.Context, numInitialDocs int, collectionName string) ([]TestDoc, *emclient.Client) {
	t.Helper()
	initialDocs, emcacheClient := setupCollectionWithShape(t, ctx, numInitialDocs, collectionName, defaultTestDocShape())
	return initialDocs, emcacheClient
}

func verifyDocsInSQLite(t *testing.T, client *emclient.Client, collectionName string, expectedDocs map[string]TestDoc) {
	t.Helper()
	ctx := context.Background()
	query := "SELECT id, name, age FROM data"
	foundDocs := make(map[string]TestDoc)

	rows, err := client.Query(ctx, collectionName, query)
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

	count := len(expectedDocs)
	if count > 0 {
	} else {
	}
}

func nestedFieldsDocShape() *pb.Shape {
	return &pb.Shape{
		Columns: []*pb.Column{
			{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
			{Name: "age", Type: pb.DataType_INTEGER, Path: "age"},
			{Name: "email", Type: pb.DataType_TEXT, Path: "contact_info.email"},
			{Name: "phone", Type: pb.DataType_TEXT, Path: "contact_info.phone"},
			{Name: "street", Type: pb.DataType_TEXT, Path: "contact_info.address.street"},
			{Name: "city", Type: pb.DataType_TEXT, Path: "contact_info.address.city"},
			{Name: "zip", Type: pb.DataType_TEXT, Path: "contact_info.address.zip"},
			{Name: "tags", Type: pb.DataType_JSONB, Path: "tags"},
		},
	}
}

func setupCollectionWithShape(t *testing.T, ctx context.Context, numInitialDocs int, collectionName string, shape *pb.Shape) ([]TestDoc, *emclient.Client) {
	t.Helper()

	initialDocs := make([]TestDoc, 0, numInitialDocs)
	for i := range numInitialDocs {
		initialDocs = append(initialDocs, TestDoc{
			ID:   uuid.NewString(),
			Name: fmt.Sprintf("InitialDoc_%d_%s", i, uuid.NewString()[:4]),
			Age:  20 + i,
		})
	}

	client := setupCollectionWithShapeAndDocs(t, ctx, collectionName, shape, initialDocs)
	return initialDocs, client
}

func setupCollectionWithShapeAndDocs[T any](t *testing.T, ctx context.Context, collectionName string, shape *pb.Shape, initialDocs []T) *emclient.Client {
	collection := mongoClient.Database(mongoDBName).Collection(collectionName)
	emcacheAdminClient, err := createClient(t, ctx)
	require.NoError(t, err, "Failed to create emcache client")

	_, err = emcacheAdminClient.AddCollection(ctx, collectionName, shape)
	require.NoError(t, err, "Failed to call AddCollection with custom shape before client init")

	t.Cleanup(func() {
		_, err := emcacheAdminClient.RemoveCollection(context.Background(), collectionName)
		require.NoError(t, err, "Failed to remove collection from Emcache")

		err = collection.Drop(context.Background())
		require.NoError(t, err, "Failed to drop collection from MongoDB")
	})

	if len(initialDocs) > 0 {
		docs := lo.Map(initialDocs, func(doc T, _ int) any { return doc })
		_, err := collection.InsertMany(ctx, docs)
		require.NoError(t, err, "Failed to insert initial documents into MongoDB")
		time.Sleep(5 * time.Second)
	}

	var emcacheClient *emclient.Client
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
		backoff = min(time.Duration(float64(backoff)*1.5), 3*time.Second)
	}

	require.NoError(t, lastErr, "Failed to perform operation within timeout")
}
