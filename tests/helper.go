package tests

import (
	"context"
	"fmt"
	"log"
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
)

var (
	mongoClient        *mongo.Client
	emcacheAdminClient *emclient.Client
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

func createClient(t *testing.T, ctx context.Context, collections ...string) *emclient.Client {
	t.Helper()

	clientCtx, clientCancel := context.WithTimeout(ctx, 15*time.Second)
	defer clientCancel()
	client, err := emclient.NewClient(clientCtx, emclient.ClientConfig{
		ServerAddr: emcacheAddr,
		Directory:  t.TempDir(),
		Collections: lo.Map(collections, func(collection string, _ int) emclient.CollectionConfig {
			return emclient.CollectionConfig{Name: collection}
		}),
		UpdateInterval: 1 * time.Minute,
		BatchSize:      100,
	})
	require.NoError(t, err, "Failed to create emcache client")
	return client
}

func setupSyncedCollection(t *testing.T, ctx context.Context, numInitialDocs int, collectionName string) ([]TestDoc, *emclient.Client) {
	t.Helper()

	dbName := "test"
	log.Printf("[%s] Setting up collection: %s", t.Name(), collectionName)

	initialDocs := make([]TestDoc, 0, numInitialDocs)
	for i := range numInitialDocs {
		initialDocs = append(initialDocs, TestDoc{
			ID:   uuid.NewString(),
			Name: fmt.Sprintf("InitialDoc_%d_%s", i, uuid.NewString()[:4]),
			Age:  20 + i,
		})
	}

	collection := mongoClient.Database(dbName).Collection(collectionName)
	emcacheAdminClient = createClient(t, ctx)

	_, err := emcacheAdminClient.AddCollection(ctx, collectionName, defaultTestDocShape())
	require.NoError(t, err, "Failed to call AddCollection before client init")

	t.Cleanup(func() {
		_, err := emcacheAdminClient.RemoveCollection(context.Background(), collectionName)
		require.NoError(t, err, "Failed to remove collection from Emcache")
		log.Printf("[%s] Dropping collection: %s", t.Name(), collectionName)
		if err := collection.Drop(context.Background()); err != nil {
			log.Printf("[%s] Failed to drop collection %s: %v", t.Name(), collectionName, err)
		}
	})

	if len(initialDocs) > 0 {
		log.Printf("[%s] Inserting %d initial documents into MongoDB...", t.Name(), len(initialDocs))
		_, err := collection.InsertMany(ctx, lo.Map(initialDocs, func(doc TestDoc, _ int) any {
			return doc
		}))
		require.NoError(t, err, "Failed to insert initial documents into MongoDB")
	}

	log.Printf("[%s] Waiting for potential initial sync / propagation...", t.Name())
	time.Sleep(10 * time.Second)
	emcacheClient := createClient(t, ctx, collectionName)

	return initialDocs, emcacheClient
}

func verifyDocsInSQLite(t *testing.T, client *emclient.Client, collectionName string, expectedDocs map[string]TestDoc) {
	t.Helper()

	query := "SELECT id, name, age FROM data"
	foundDocs := make(map[string]TestDoc)

	rows, err := client.Query(context.Background(), collectionName, query)
	require.NoError(t, err, "Failed to execute query via client")
	defer rows.Close()

	for rows.Next() {
		var id, name string
		var age int
		err := rows.Scan(&id, &name, &age)
		require.NoError(t, err, "Failed to scan row")

		doc := TestDoc{
			ID:   id,
			Name: name,
			Age:  age,
		}
		foundDocs[id] = doc
	}
	require.NoError(t, rows.Err(), "Error iterating rows")

	require.Equal(t, len(expectedDocs), len(foundDocs), "Number of documents in Client DB (%d) does not match expected count (%d)", len(foundDocs), len(expectedDocs))

	for id, expected := range expectedDocs {
		found, ok := foundDocs[id]
		require.True(t, ok, "Expected document with ID %s not found in Client DB", id)
		require.Equal(t, expected.Name, found.Name, "Name mismatch for doc ID %s. Expected: %s, Found: %s", id, expected.Name, found.Name)
		require.Equal(t, expected.Age, found.Age, "Age mismatch for doc ID %s. Expected: %d, Found: %d", id, expected.Age, found.Age)
	}

	if len(expectedDocs) > 0 {
		log.Printf("[%s] Successfully verified %d documents in Client DB %s", t.Name(), len(expectedDocs), collectionName)
	} else {
		log.Printf("[%s] Successfully verified Client DB %s is empty as expected", t.Name(), collectionName)
	}
}
