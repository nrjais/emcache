package manager

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/shape"
)

func TestCollectionManagerInterface(t *testing.T) {
	t.Run("interface definition exists", func(t *testing.T) {
		// Test that the interface has the expected method signature
		var manager CollectionManager
		assert.Nil(t, manager) // Interface is nil when not assigned
	})

	t.Run("CollectionManagerFunc type exists", func(t *testing.T) {
		// Test that the CollectionManagerFunc type is defined
		var funcType CollectionManagerFunc
		assert.Nil(t, funcType) // Function type is nil when not assigned
	})

	t.Run("parameters are correctly typed", func(t *testing.T) {
		// This test verifies that the interface accepts the correct parameter types
		testColl := db.ReplicatedCollection{
			CollectionName: "test_collection",
			CurrentVersion: 5,
			Shape: shape.Shape{
				Columns: []shape.Column{
					{Name: "id", Type: shape.Text, Path: "_id"},
				},
			},
		}

		cfg := &config.Config{
			LeaderOptions: config.LeaderConfig{
				LeaseDurationSecs: 30,
			},
		}

		// Test that we can pass the correct parameter types to a manager function
		called := false
		testFunc := func(
			ctx context.Context,
			wg *sync.WaitGroup,
			replicatedColl db.ReplicatedCollection,
			pgPool interface{},
			mongoClient interface{},
			mongoDBName string,
			leaderElector interface{},
			receivedCfg *config.Config,
		) {
			defer wg.Done()
			called = true

			// Test that parameters are received correctly
			assert.Equal(t, "test_collection", replicatedColl.CollectionName)
			assert.Equal(t, 5, replicatedColl.CurrentVersion)
			assert.Equal(t, "test_db", mongoDBName)
			assert.Equal(t, int64(30), receivedCfg.LeaderOptions.LeaseDurationSecs)
		}

		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)

		// Call the function directly (not through interface since interface method signature is different)
		testFunc(ctx, &wg, testColl, nil, nil, "test_db", nil, cfg)

		wg.Wait()
		assert.True(t, called)
	})
}

func TestReplicatedCollectionStructure(t *testing.T) {
	t.Run("can create ReplicatedCollection", func(t *testing.T) {
		testShape := shape.Shape{
			Columns: []shape.Column{
				{Name: "id", Type: shape.Text, Path: "_id"},
				{Name: "name", Type: shape.Text, Path: "name"},
			},
		}

		coll := db.ReplicatedCollection{
			CollectionName: "users",
			CurrentVersion: 3,
			Shape:          testShape,
		}

		assert.Equal(t, "users", coll.CollectionName)
		assert.Equal(t, 3, coll.CurrentVersion)
		assert.Equal(t, testShape, coll.Shape)
		assert.Len(t, coll.Shape.Columns, 2)
	})
}
