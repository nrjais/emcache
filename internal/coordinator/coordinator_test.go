package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/mock/gomock"

	"github.com/nrjais/emcache/internal/collectioncache/mocks"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	dbmocks "github.com/nrjais/emcache/internal/db/mocks"
	leadermocks "github.com/nrjais/emcache/internal/leader/mocks"
	"github.com/nrjais/emcache/internal/manager"
	"github.com/nrjais/emcache/internal/shape"
)

func TestNewCoordinator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("creates coordinator with all dependencies", func(t *testing.T) {
		mockPgPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		mockLeaderElector := leadermocks.NewMockLeaderElectorInterface(ctrl)
		mockClient := &mongo.Client{}
		cfg := &config.Config{}
		wg := &sync.WaitGroup{}

		managerFunc := func(context.Context, *sync.WaitGroup, db.ReplicatedCollection, interface{}, *mongo.Client, string, interface{}, *config.Config) {
		}

		opts := CoordinatorOptions{
			PgPool:        mockPgPool,
			MongoClient:   mockClient,
			MongoDBName:   "test_db",
			LeaderElector: mockLeaderElector,
			CollCache:     mockCollCache,
			Config:        cfg,
			WaitGroup:     wg,
			ManagerFunc:   managerFunc,
		}

		coordinator := NewCoordinator(opts)

		assert.NotNil(t, coordinator)
		assert.Equal(t, mockPgPool, coordinator.pgPool)
		assert.Equal(t, mockClient, coordinator.mongoClient)
		assert.Equal(t, "test_db", coordinator.mongoDBName)
		assert.Equal(t, mockLeaderElector, coordinator.leaderElector)
		assert.Equal(t, mockCollCache, coordinator.collCache)
		assert.Equal(t, cfg, coordinator.cfg)
		assert.Equal(t, wg, coordinator.wg)
		assert.NotNil(t, coordinator.managedCollections)
		assert.Len(t, coordinator.managedCollections, 0)
	})
}

func TestCoordinator_SyncCollections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("adds new collections from cache", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		testCollections := []db.ReplicatedCollection{
			{CollectionName: "users", Shape: shape.Shape{}},
			{CollectionName: "orders", Shape: shape.Shape{}},
		}

		mockCollCache.EXPECT().GetAllCollections().Return(testCollections)

		ctx := context.Background()
		err := coordinator.syncCollections(ctx)

		assert.NoError(t, err)
		assert.Len(t, coordinator.managedCollections, 2)
		assert.Contains(t, coordinator.managedCollections, "users")
		assert.Contains(t, coordinator.managedCollections, "orders")
	})

	t.Run("removes collections no longer in cache", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		// Pre-populate managed collections
		coordinator.managedCollections["users"] = func() {}
		coordinator.managedCollections["orders"] = func() {}

		// Cache now only has users
		testCollections := []db.ReplicatedCollection{
			{CollectionName: "users", Shape: shape.Shape{}},
		}

		mockCollCache.EXPECT().GetAllCollections().Return(testCollections)

		ctx := context.Background()
		err := coordinator.syncCollections(ctx)

		assert.NoError(t, err)
		assert.Len(t, coordinator.managedCollections, 1)
		assert.Contains(t, coordinator.managedCollections, "users")
		assert.NotContains(t, coordinator.managedCollections, "orders")
	})

	t.Run("handles empty cache", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		// Pre-populate managed collections
		coordinator.managedCollections["users"] = func() {}

		mockCollCache.EXPECT().GetAllCollections().Return([]db.ReplicatedCollection{})

		ctx := context.Background()
		err := coordinator.syncCollections(ctx)

		assert.NoError(t, err)
		assert.Len(t, coordinator.managedCollections, 0)
	})

	t.Run("ignores already managed collections", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		// Pre-populate managed collections
		coordinator.managedCollections["users"] = func() {}

		testCollections := []db.ReplicatedCollection{
			{CollectionName: "users", Shape: shape.Shape{}},
		}

		mockCollCache.EXPECT().GetAllCollections().Return(testCollections)

		ctx := context.Background()
		err := coordinator.syncCollections(ctx)

		assert.NoError(t, err)
		assert.Len(t, coordinator.managedCollections, 1)
		assert.Contains(t, coordinator.managedCollections, "users")
	})
}

func TestCoordinator_StartManaging(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("starts managing new collection", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		managerCallCount := 0
		var mu sync.Mutex
		var wg sync.WaitGroup

		managerFunc := func(context.Context, *sync.WaitGroup, db.ReplicatedCollection, interface{}, *mongo.Client, string, interface{}, *config.Config) {
			mu.Lock()
			managerCallCount++
			mu.Unlock()
			wg.Done()
		}

		coordinator := createTestCoordinatorWithManager(ctrl, mockCollCache, managerFunc)

		testColl := db.ReplicatedCollection{
			CollectionName: "users",
			Shape:          shape.Shape{},
		}

		ctx := context.Background()
		wg.Add(1)
		coordinator.startManaging(ctx, testColl)

		assert.Len(t, coordinator.managedCollections, 1)
		assert.Contains(t, coordinator.managedCollections, "users")

		// Wait for the goroutine to complete
		wg.Wait()

		mu.Lock()
		assert.Equal(t, 1, managerCallCount)
		mu.Unlock()
	})

	t.Run("ignores already managed collection", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		managerCallCount := 0
		var mu sync.Mutex

		managerFunc := func(context.Context, *sync.WaitGroup, db.ReplicatedCollection, interface{}, *mongo.Client, string, interface{}, *config.Config) {
			mu.Lock()
			managerCallCount++
			mu.Unlock()
		}

		coordinator := createTestCoordinatorWithManager(ctrl, mockCollCache, managerFunc)

		// Pre-populate managed collections
		coordinator.managedCollections["users"] = func() {}

		testColl := db.ReplicatedCollection{
			CollectionName: "users",
			Shape:          shape.Shape{},
		}

		ctx := context.Background()
		coordinator.startManaging(ctx, testColl)

		assert.Len(t, coordinator.managedCollections, 1)

		// Give a small amount of time to ensure no goroutine was started
		time.Sleep(10 * time.Millisecond)

		mu.Lock()
		assert.Equal(t, 0, managerCallCount) // Should not be called
		mu.Unlock()
	})
}

func TestCoordinator_StopManaging(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("stops managing existing collection", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		// Pre-populate managed collections
		called := false
		coordinator.managedCollections["users"] = func() { called = true }

		coordinator.stopManaging("users")

		assert.Len(t, coordinator.managedCollections, 0)
		assert.True(t, called)
	})

	t.Run("ignores non-managed collection", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		// No collections are managed
		coordinator.stopManaging("users")

		assert.Len(t, coordinator.managedCollections, 0)
	})
}

func TestCoordinator_StopAllManaging(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("stops all managed collections", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		// Pre-populate managed collections
		called1, called2 := false, false
		coordinator.managedCollections["users"] = func() { called1 = true }
		coordinator.managedCollections["orders"] = func() { called2 = true }

		coordinator.stopAllManaging()

		assert.Len(t, coordinator.managedCollections, 0)
		assert.True(t, called1)
		assert.True(t, called2)
	})

	t.Run("handles empty managed collections", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		coordinator.stopAllManaging()

		assert.Len(t, coordinator.managedCollections, 0)
	})
}

func TestCoordinator_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("processes refresh signals", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		refreshCh := make(chan struct{}, 1)
		mockCollCache.EXPECT().RefreshChannel().Return(refreshCh).AnyTimes()
		mockCollCache.EXPECT().GetAllCollections().Return([]db.ReplicatedCollection{}).Times(1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan bool)
		go func() {
			coordinator.Start(ctx)
			done <- true
		}()

		// Send refresh signal
		refreshCh <- struct{}{}

		// Wait a bit for processing
		time.Sleep(50 * time.Millisecond)

		// Cancel context to stop coordinator
		cancel()

		// Wait for coordinator to stop
		select {
		case <-done:
			// Test passed
		case <-time.After(2 * time.Second):
			t.Fatal("Coordinator did not stop within timeout")
		}
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		refreshCh := make(chan struct{})
		mockCollCache.EXPECT().RefreshChannel().Return(refreshCh).AnyTimes()

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan bool)
		go func() {
			coordinator.Start(ctx)
			done <- true
		}()

		// Cancel context immediately
		cancel()

		// Wait for coordinator to stop
		select {
		case <-done:
			// Test passed
		case <-time.After(2 * time.Second):
			t.Fatal("Coordinator did not stop within timeout")
		}
	})

	t.Run("recovers from panic during sync", func(t *testing.T) {
		mockCollCache := mocks.NewMockCollectionCacheManager(ctrl)
		coordinator := createTestCoordinator(ctrl, mockCollCache)

		refreshCh := make(chan struct{}, 1)
		mockCollCache.EXPECT().RefreshChannel().Return(refreshCh).AnyTimes()
		mockCollCache.EXPECT().GetAllCollections().Return([]db.ReplicatedCollection{}).Times(1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan bool)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Coordinator should have recovered from panic: %v", r)
				}
			}()
			coordinator.Start(ctx)
			done <- true
		}()

		// Send refresh signal
		refreshCh <- struct{}{}

		// Wait a bit for processing
		time.Sleep(50 * time.Millisecond)

		// Cancel context to stop coordinator
		cancel()

		// Wait for coordinator to stop
		select {
		case <-done:
			// Test passed
		case <-time.After(2 * time.Second):
			t.Fatal("Coordinator did not stop within timeout")
		}
	})
}

// Helper functions for testing

func createTestCoordinator(ctrl *gomock.Controller, mockCollCache *mocks.MockCollectionCacheManager) *Coordinator {
	mockPgPool := dbmocks.NewMockPostgresPool(ctrl)
	mockLeaderElector := leadermocks.NewMockLeaderElectorInterface(ctrl)
	mockClient := &mongo.Client{}
	cfg := &config.Config{}
	wg := &sync.WaitGroup{}

	managerFunc := func(context.Context, *sync.WaitGroup, db.ReplicatedCollection, interface{}, *mongo.Client, string, interface{}, *config.Config) {
	}

	opts := CoordinatorOptions{
		PgPool:        mockPgPool,
		MongoClient:   mockClient,
		MongoDBName:   "test_db",
		LeaderElector: mockLeaderElector,
		CollCache:     mockCollCache,
		Config:        cfg,
		WaitGroup:     wg,
		ManagerFunc:   managerFunc,
	}

	return NewCoordinator(opts)
}

func createTestCoordinatorWithManager(ctrl *gomock.Controller, mockCollCache *mocks.MockCollectionCacheManager, managerFunc manager.CollectionManagerFunc) *Coordinator {
	mockPgPool := dbmocks.NewMockPostgresPool(ctrl)
	mockLeaderElector := leadermocks.NewMockLeaderElectorInterface(ctrl)
	mockClient := &mongo.Client{}
	cfg := &config.Config{}
	wg := &sync.WaitGroup{}

	opts := CoordinatorOptions{
		PgPool:        mockPgPool,
		MongoClient:   mockClient,
		MongoDBName:   "test_db",
		LeaderElector: mockLeaderElector,
		CollCache:     mockCollCache,
		Config:        cfg,
		WaitGroup:     wg,
		ManagerFunc:   managerFunc,
	}

	return NewCoordinator(opts)
}
