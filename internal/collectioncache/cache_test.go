package collectioncache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/shape"
)

func TestNewManager(t *testing.T) {
	t.Run("creates manager with valid config", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}

		manager := NewManager(nil, cfg)

		assert.NotNil(t, manager)
		assert.Equal(t, 60*time.Second, manager.refreshInterval)
		assert.NotNil(t, manager.collections)
		assert.NotNil(t, manager.RefreshCh)
		assert.Equal(t, 0, len(manager.collections))
	})

	t.Run("uses default interval for invalid config", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 0, // Invalid
			},
		}

		manager := NewManager(nil, cfg)

		assert.NotNil(t, manager)
		assert.Equal(t, 5*time.Second, manager.refreshInterval) // Default
	})

	t.Run("uses default interval for negative config", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: -10, // Invalid
			},
		}

		manager := NewManager(nil, cfg)

		assert.NotNil(t, manager)
		assert.Equal(t, 5*time.Second, manager.refreshInterval) // Default
	})
}

func TestManager_GetCollection(t *testing.T) {
	t.Run("returns collection when exists", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		// Add test collection
		testCollection := db.ReplicatedCollection{
			CollectionName: "users",
			Shape: shape.Shape{
				Columns: []shape.Column{
					{Name: "id", Type: shape.Text, Path: "_id"},
				},
			},
		}
		manager.collections["users"] = testCollection

		result, found := manager.GetCollection("users")

		assert.True(t, found)
		assert.Equal(t, testCollection, result)
	})

	t.Run("returns not found when collection doesn't exist", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		result, found := manager.GetCollection("nonexistent")

		assert.False(t, found)
		assert.Equal(t, db.ReplicatedCollection{}, result)
	})
}

func TestManager_GetAllCollections(t *testing.T) {
	t.Run("returns all collections", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		// Add test collections
		testCollections := map[string]db.ReplicatedCollection{
			"users": {
				CollectionName: "users",
				Shape: shape.Shape{
					Columns: []shape.Column{
						{Name: "id", Type: shape.Text, Path: "_id"},
					},
				},
			},
			"orders": {
				CollectionName: "orders",
				Shape: shape.Shape{
					Columns: []shape.Column{
						{Name: "id", Type: shape.Text, Path: "_id"},
						{Name: "total", Type: shape.Number, Path: "total"},
					},
				},
			},
		}
		manager.collections = testCollections

		result := manager.GetAllCollections()

		assert.Len(t, result, 2)

		// Convert result to map for easier checking
		resultMap := make(map[string]db.ReplicatedCollection)
		for _, coll := range result {
			resultMap[coll.CollectionName] = coll
		}

		assert.Contains(t, resultMap, "users")
		assert.Contains(t, resultMap, "orders")
		assert.Equal(t, testCollections["users"], resultMap["users"])
		assert.Equal(t, testCollections["orders"], resultMap["orders"])
	})

	t.Run("returns empty slice when no collections", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		result := manager.GetAllCollections()

		assert.NotNil(t, result)
		assert.Len(t, result, 0)
	})
}

func TestManager_RefreshChannel(t *testing.T) {
	t.Run("returns refresh channel", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		ch := manager.RefreshChannel()

		assert.NotNil(t, ch)
		// Cannot directly compare channels of different types, just ensure it's the same underlying channel
		assert.Equal(t, cap(manager.RefreshCh), cap(ch))
	})

	t.Run("channel can receive messages", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		ch := manager.RefreshChannel()

		// Send a message to the channel
		select {
		case manager.RefreshCh <- struct{}{}:
			// Success
		default:
			t.Fatal("Should be able to send to refresh channel")
		}

		// Receive the message
		select {
		case <-ch:
			// Success
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Should have received message from refresh channel")
		}
	})
}

func TestManager_Stop(t *testing.T) {
	t.Run("calls stop without error", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		// Should not panic or error
		manager.Stop()
	})
}

func TestManager_Start(t *testing.T) {
	t.Run("start method exists and can be called", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Just verify the method can be called without panicking
		// We don't test the actual refresh behavior since it requires a database
		assert.NotPanics(t, func() {
			manager.Start(ctx)
		})

		// Cancel immediately to stop any goroutines
		cancel()

		// Give a small amount of time for cleanup
		time.Sleep(10 * time.Millisecond)
	})
}

func TestManager_ConcurrentAccess(t *testing.T) {
	t.Run("handles concurrent read/write access safely", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		// Add initial collection
		testCollection := db.ReplicatedCollection{
			CollectionName: "users",
			Shape: shape.Shape{
				Columns: []shape.Column{
					{Name: "id", Type: shape.Text, Path: "_id"},
				},
			},
		}
		manager.collections["users"] = testCollection

		done := make(chan bool, 10)

		// Start multiple goroutines doing concurrent operations
		for i := 0; i < 5; i++ {
			// Readers
			go func() {
				for j := 0; j < 100; j++ {
					_, _ = manager.GetCollection("users")
					_ = manager.GetAllCollections()
				}
				done <- true
			}()

			// Writers (simulating refresh)
			go func() {
				for j := 0; j < 100; j++ {
					manager.mu.Lock()
					manager.collections = map[string]db.ReplicatedCollection{
						"users": testCollection,
					}
					manager.mu.Unlock()
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			select {
			case <-done:
				// Continue
			case <-time.After(5 * time.Second):
				t.Fatal("Concurrent access test timed out - possible deadlock")
			}
		}
	})
}

func TestManager_EdgeCases(t *testing.T) {
	t.Run("handles nil collections map", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)
		manager.collections = nil

		// Should not panic
		result := manager.GetAllCollections()
		assert.NotNil(t, result)
		assert.Len(t, result, 0)

		_, found := manager.GetCollection("test")
		assert.False(t, found)
	})

	t.Run("handles very large collection names", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		longName := string(make([]byte, 1000)) // Very long collection name
		testCollection := db.ReplicatedCollection{
			CollectionName: longName,
			Shape:          shape.Shape{},
		}
		manager.collections[longName] = testCollection

		result, found := manager.GetCollection(longName)
		assert.True(t, found)
		assert.Equal(t, testCollection, result)
	})

	t.Run("refresh channel doesn't block when full", func(t *testing.T) {
		cfg := &config.Config{
			CoordinatorOptions: config.CoordinatorConfig{
				CollectionRefreshIntervalSecs: 60,
			},
		}
		manager := NewManager(nil, cfg)

		// Fill the channel
		manager.RefreshCh <- struct{}{}

		// This should not block since the channel has capacity 1
		// and the implementation uses a default case
		select {
		case manager.RefreshCh <- struct{}{}:
			// If this succeeds, that's fine too
		default:
			// This is the expected path when channel is full
		}

		// Clear the channel for cleanup
		select {
		case <-manager.RefreshCh:
		default:
		}
	})
}
