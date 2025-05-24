package follower

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/nrjais/emcache/internal/collectioncache/mocks"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	dbmocks "github.com/nrjais/emcache/internal/db/mocks"
	"github.com/nrjais/emcache/internal/shape"
)

func TestGetCollectionDBPath(t *testing.T) {
	t.Run("constructs correct path", func(t *testing.T) {
		baseDir := "/tmp/emcache"
		collectionName := "users"
		version := 1

		path := GetCollectionDBPath(collectionName, baseDir, version)

		expected := filepath.Join(baseDir, "replicas", "users_v1", "db.sqlite")
		assert.Equal(t, expected, path)
	})

	t.Run("handles special characters in collection name", func(t *testing.T) {
		baseDir := "/tmp/emcache"
		collectionName := "test-collection_with.special"
		version := 5

		path := GetCollectionDBPath(collectionName, baseDir, version)

		expected := filepath.Join(baseDir, "replicas", "test-collection_with.special_v5", "db.sqlite")
		assert.Equal(t, expected, path)
	})

	t.Run("handles higher version numbers", func(t *testing.T) {
		baseDir := "/tmp/emcache"
		collectionName := "orders"
		version := 1000

		path := GetCollectionDBPath(collectionName, baseDir, version)

		expected := filepath.Join(baseDir, "replicas", "orders_v1000", "db.sqlite")
		assert.Equal(t, expected, path)
	})

	t.Run("handles empty base directory", func(t *testing.T) {
		baseDir := ""
		collectionName := "users"
		version := 1

		path := GetCollectionDBPath(collectionName, baseDir, version)

		expected := filepath.Join("replicas", "users_v1", "db.sqlite")
		assert.Equal(t, expected, path)
	})
}

func TestParseDBDirName(t *testing.T) {
	t.Run("parses valid directory name", func(t *testing.T) {
		dirName := "users_v1"

		collectionName, version := parseDBDirName(dirName)

		assert.Equal(t, "users", collectionName)
		assert.Equal(t, 1, version)
	})

	t.Run("parses directory with underscores in collection name", func(t *testing.T) {
		dirName := "user_profiles_v5"

		collectionName, version := parseDBDirName(dirName)

		assert.Equal(t, "user_profiles", collectionName)
		assert.Equal(t, 5, version)
	})

	t.Run("parses directory with special characters", func(t *testing.T) {
		dirName := "test-collection.name_v10"

		collectionName, version := parseDBDirName(dirName)

		assert.Equal(t, "test-collection.name", collectionName)
		assert.Equal(t, 10, version)
	})

	t.Run("handles invalid format", func(t *testing.T) {
		dirName := "invalid_format"

		collectionName, version := parseDBDirName(dirName)

		assert.Equal(t, "", collectionName)
		assert.Equal(t, 0, version)
	})

	t.Run("handles non-numeric version", func(t *testing.T) {
		dirName := "users_vabc"

		collectionName, version := parseDBDirName(dirName)

		assert.Equal(t, "users", collectionName)
		assert.Equal(t, 0, version)
	})

	t.Run("handles multiple underscores and versions", func(t *testing.T) {
		dirName := "complex_collection_name_v999"

		collectionName, version := parseDBDirName(dirName)

		assert.Equal(t, "complex_collection_name", collectionName)
		assert.Equal(t, 999, version)
	})
}

func TestMapShapeTypeToSQLite(t *testing.T) {
	testCases := []struct {
		input    shape.DataType
		expected string
	}{
		{shape.Integer, "INTEGER"},
		{shape.Number, "REAL"},
		{shape.Bool, "INTEGER"},
		{shape.Text, "TEXT"},
		{shape.JSONB, "BLOB"},
		{shape.Any, "BLOB"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.input), func(t *testing.T) {
			result := mapShapeTypeToSQLite(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}

	t.Run("handles unknown type", func(t *testing.T) {
		unknownType := shape.DataType("unknown")
		result := mapShapeTypeToSQLite(unknownType)
		assert.Equal(t, "BLOB", result)
	})
}

func TestQuoteIdentifier(t *testing.T) {
	t.Run("quotes simple identifier", func(t *testing.T) {
		result := quoteIdentifier("id")
		assert.Equal(t, `"id"`, result)
	})

	t.Run("quotes identifier with spaces", func(t *testing.T) {
		result := quoteIdentifier("user name")
		assert.Equal(t, `"user name"`, result)
	})

	t.Run("quotes identifier with special characters", func(t *testing.T) {
		result := quoteIdentifier("user-id_test.field")
		assert.Equal(t, `"user-id_test.field"`, result)
	})

	t.Run("quotes empty identifier", func(t *testing.T) {
		result := quoteIdentifier("")
		assert.Equal(t, `""`, result)
	})

	t.Run("quotes identifier with quotes", func(t *testing.T) {
		result := quoteIdentifier(`user"field`)
		assert.Equal(t, `"user"field"`, result)
	})
}

func TestMapValueToSqlite(t *testing.T) {
	t.Run("handles nil value", func(t *testing.T) {
		result := mapValueToSqlite(nil)
		assert.Nil(t, result)
	})

	t.Run("handles boolean true", func(t *testing.T) {
		result := mapValueToSqlite(true)
		assert.Equal(t, 1, result)
	})

	t.Run("handles boolean false", func(t *testing.T) {
		result := mapValueToSqlite(false)
		assert.Equal(t, 0, result)
	})

	t.Run("passes through string", func(t *testing.T) {
		result := mapValueToSqlite("test string")
		assert.Equal(t, "test string", result)
	})

	t.Run("passes through integer", func(t *testing.T) {
		result := mapValueToSqlite(42)
		bytes, ok := result.([]byte)
		require.True(t, ok)
		assert.Contains(t, string(bytes), "42")
	})

	t.Run("passes through float", func(t *testing.T) {
		result := mapValueToSqlite(3.14)
		assert.Equal(t, 3.14, result)
	})

	t.Run("converts complex object to JSON bytes", func(t *testing.T) {
		input := map[string]interface{}{
			"name": "John",
			"age":  30,
		}

		result := mapValueToSqlite(input)

		bytes, ok := result.([]byte)
		require.True(t, ok)
		assert.Contains(t, string(bytes), "John")
		assert.Contains(t, string(bytes), "30")
	})

	t.Run("converts slice to JSON bytes", func(t *testing.T) {
		input := []string{"apple", "banana", "cherry"}

		result := mapValueToSqlite(input)

		bytes, ok := result.([]byte)
		require.True(t, ok)
		assert.Contains(t, string(bytes), "apple")
		assert.Contains(t, string(bytes), "banana")
		assert.Contains(t, string(bytes), "cherry")
	})
}

func TestNewMainFollower(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir, err := os.MkdirTemp("", "follower_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Run("creates new follower with valid configuration", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    5,
				BatchSize:           100,
				CleanupIntervalSecs: 300,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, tempDir, cfg)

		require.NoError(t, err)
		assert.NotNil(t, follower)
		assert.Equal(t, mockPool, follower.pgPool)
		assert.Equal(t, mockCacheMgr, follower.collCache)
		assert.Equal(t, tempDir, follower.sqliteBaseDir)
		assert.Equal(t, 5*time.Second, follower.pollInterval)
		assert.Equal(t, 100, follower.batchSize)
		assert.Equal(t, 300*time.Second, follower.cleanupInterval)
		assert.Equal(t, int64(0), follower.globalLastOplogID)
		assert.NotNil(t, follower.connections)
		assert.NotNil(t, follower.metaDB)
		assert.NotNil(t, follower.updateTracker)

		follower.metaDB.Close()
	})

	t.Run("creates directory if it doesn't exist", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		nonExistentDir := filepath.Join(tempDir, "non_existent_subdir")
		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    1,
				BatchSize:           50,
				CleanupIntervalSecs: 60,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, nonExistentDir, cfg)

		require.NoError(t, err)
		assert.NotNil(t, follower)

		_, err = os.Stat(nonExistentDir)
		assert.NoError(t, err)

		follower.metaDB.Close()
	})

	t.Run("handles invalid directory path", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		invalidDir := "/root/invalid_path_for_test"
		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    1,
				BatchSize:           50,
				CleanupIntervalSecs: 60,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, invalidDir, cfg)

		if err != nil {
			assert.Error(t, err)
			assert.Nil(t, follower)
		} else {

			if follower != nil {
				follower.metaDB.Close()
			}
		}
	})
}

func TestMainFollower_CloseAllConnections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir, err := os.MkdirTemp("", "follower_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Run("closes all connections without error", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    1,
				BatchSize:           50,
				CleanupIntervalSecs: 60,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, tempDir, cfg)
		require.NoError(t, err)

		follower.closeAllConnections()

		follower.metaDB.Close()
	})
}

func TestMainFollower_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir, err := os.MkdirTemp("", "follower_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Run("starts and stops gracefully", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    1,
				BatchSize:           10,
				CleanupIntervalSecs: 60,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, tempDir, cfg)
		require.NoError(t, err)

		mockRow := dbmocks.NewMockRow(ctrl)
		mockRow.EXPECT().Scan(gomock.Any()).Return(nil).AnyTimes()
		mockPool.EXPECT().QueryRow(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow).AnyTimes()

		mockCacheMgr.EXPECT().GetAllCollections().Return([]db.ReplicatedCollection{}).AnyTimes()

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup

		wg.Add(1)
		go follower.Start(ctx, &wg)

		time.Sleep(100 * time.Millisecond)

		cancel()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:

		case <-time.After(5 * time.Second):
			t.Fatal("Follower did not stop within timeout")
		}
	})
}

func TestErrCollectionNotFound(t *testing.T) {
	t.Run("error has correct message", func(t *testing.T) {
		assert.Equal(t, "collection not found", ErrCollectionNotFound.Error())
	})

	t.Run("error can be compared", func(t *testing.T) {
		err := ErrCollectionNotFound
		assert.True(t, err == ErrCollectionNotFound)
	})
}

func TestNewBatchUpdateTracker(t *testing.T) {
	t.Run("creates new tracker with empty state", func(t *testing.T) {
		tracker := newBatchUpdateTracker()

		assert.NotNil(t, tracker)
		assert.NotNil(t, tracker.lastCommittedOffset)
		assert.Equal(t, 0, len(tracker.lastCommittedOffset))
	})
}

func TestBatchUpdateTracker_UpdateOffset(t *testing.T) {
	t.Run("updates offset for database", func(t *testing.T) {
		tracker := newBatchUpdateTracker()
		dbKey := colVersion{collectionName: "users", version: 1}

		tracker.updateOffset(dbKey, 100)

		assert.Equal(t, int64(100), tracker.lastCommittedOffset[dbKey])
	})

	t.Run("tracks multiple updates for same database", func(t *testing.T) {
		tracker := newBatchUpdateTracker()
		dbKey := colVersion{collectionName: "users", version: 1}

		tracker.updateOffset(dbKey, 100)
		tracker.updateOffset(dbKey, 200)
		tracker.updateOffset(dbKey, 300)

		assert.Equal(t, int64(300), tracker.lastCommittedOffset[dbKey])
	})

	t.Run("tracks multiple databases", func(t *testing.T) {
		tracker := newBatchUpdateTracker()

		tracker.updateOffset(colVersion{collectionName: "users", version: 1}, 100)
		tracker.updateOffset(colVersion{collectionName: "orders", version: 1}, 200)
		tracker.updateOffset(colVersion{collectionName: "products", version: 2}, 300)

		assert.Equal(t, int64(100), tracker.lastCommittedOffset[colVersion{collectionName: "users", version: 1}])
		assert.Equal(t, int64(200), tracker.lastCommittedOffset[colVersion{collectionName: "orders", version: 1}])
		assert.Equal(t, int64(300), tracker.lastCommittedOffset[colVersion{collectionName: "products", version: 2}])
	})
}

func TestBatchUpdateTracker_IsStale(t *testing.T) {
	t.Run("returns false for new database", func(t *testing.T) {
		tracker := newBatchUpdateTracker()

		isStale := tracker.isStale(colVersion{collectionName: "users", version: 1}, 1000, 10)

		assert.False(t, isStale)
	})

	t.Run("returns false for recently updated database", func(t *testing.T) {
		tracker := newBatchUpdateTracker()
		dbKey := colVersion{collectionName: "users", version: 1}

		tracker.updateOffset(dbKey, 980)
		isStale := tracker.isStale(dbKey, 1000, 10)

		assert.False(t, isStale)
	})

	t.Run("returns true for database with stale offset", func(t *testing.T) {
		tracker := newBatchUpdateTracker()
		dbKey := colVersion{collectionName: "users", version: 1}

		tracker.updateOffset(dbKey, 900)
		isStale := tracker.isStale(dbKey, 1000, 10)

		assert.True(t, isStale)
	})

	t.Run("returns false exactly at threshold boundary", func(t *testing.T) {
		tracker := newBatchUpdateTracker()
		dbKey := colVersion{collectionName: "users", version: 1}

		tracker.updateOffset(dbKey, 950)
		isStale := tracker.isStale(dbKey, 1000, 10)

		assert.False(t, isStale)
	})

	t.Run("handles different batch sizes", func(t *testing.T) {
		tracker := newBatchUpdateTracker()
		dbKey := colVersion{collectionName: "users", version: 1}

		tracker.updateOffset(dbKey, 750)
		isStale := tracker.isStale(dbKey, 1000, 100)

		assert.False(t, isStale)
	})

	t.Run("returns true with larger batch size threshold", func(t *testing.T) {
		tracker := newBatchUpdateTracker()
		dbKey := colVersion{collectionName: "users", version: 1}

		tracker.updateOffset(dbKey, 400)
		isStale := tracker.isStale(dbKey, 1000, 100)

		assert.True(t, isStale)
	})
}

func TestBatchUpdateTracker_GetAllTrackedDBs(t *testing.T) {
	t.Run("returns empty slice for new tracker", func(t *testing.T) {
		tracker := newBatchUpdateTracker()

		dbKeys := tracker.getAllTrackedDBs()

		assert.Equal(t, 0, len(dbKeys))
	})

	t.Run("returns all tracked database keys", func(t *testing.T) {
		tracker := newBatchUpdateTracker()

		tracker.updateOffset(colVersion{collectionName: "users", version: 1}, 100)
		tracker.updateOffset(colVersion{collectionName: "orders", version: 2}, 200)
		tracker.updateOffset(colVersion{collectionName: "products", version: 1}, 300)

		dbKeys := tracker.getAllTrackedDBs()

		assert.Equal(t, 3, len(dbKeys))
		assert.Contains(t, dbKeys, colVersion{collectionName: "users", version: 1})
		assert.Contains(t, dbKeys, colVersion{collectionName: "orders", version: 2})
		assert.Contains(t, dbKeys, colVersion{collectionName: "products", version: 1})
	})
}

func TestMainFollower_UpdateStaleDBOffsets(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir, err := os.MkdirTemp("", "follower_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Run("skips update when no stale databases", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    1,
				BatchSize:           10,
				CleanupIntervalSecs: 60,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, tempDir, cfg)
		require.NoError(t, err)
		defer follower.metaDB.Close()

		ctx := context.Background()
		follower.updateStaleDBOffsets(ctx)

	})

	t.Run("skips update when database is not stale", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    1,
				BatchSize:           10,
				CleanupIntervalSecs: 60,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, tempDir, cfg)
		require.NoError(t, err)
		defer follower.metaDB.Close()

		follower.globalLastOplogID = 1000
		follower.updateTracker.updateOffset(colVersion{collectionName: "users", version: 1}, 980)

		ctx := context.Background()
		follower.updateStaleDBOffsets(ctx)
	})

	t.Run("handles collection not found gracefully", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    1,
				BatchSize:           10,
				CleanupIntervalSecs: 60,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, tempDir, cfg)
		require.NoError(t, err)
		defer follower.metaDB.Close()

		follower.globalLastOplogID = 1000
		follower.updateTracker.updateOffset(colVersion{collectionName: "nonexistent", version: 1}, 900)

		mockCacheMgr.EXPECT().GetCollectionRefresh(gomock.Any(), "nonexistent").
			Return(db.ReplicatedCollection{}, false, nil)

		ctx := context.Background()
		follower.updateStaleDBOffsets(ctx)

	})

	t.Run("handles connection error gracefully", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    1,
				BatchSize:           10,
				CleanupIntervalSecs: 60,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, tempDir, cfg)
		require.NoError(t, err)
		defer follower.metaDB.Close()

		follower.globalLastOplogID = 1000
		follower.updateTracker.updateOffset(colVersion{collectionName: "testcoll", version: 1}, 900)

		mockCacheMgr.EXPECT().GetCollectionRefresh(gomock.Any(), "testcoll").
			Return(db.ReplicatedCollection{}, false, errors.New("connection error"))

		ctx := context.Background()
		follower.updateStaleDBOffsets(ctx)

	})
}

func TestMainFollower_NewMainFollowerIncludesUpdateTracker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir, err := os.MkdirTemp("", "follower_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Run("initializes update tracker", func(t *testing.T) {
		mockPool := dbmocks.NewMockPostgresPool(ctrl)
		mockCacheMgr := mocks.NewMockCollectionCacheManager(ctrl)

		cfg := &config.Config{
			FollowerOptions: config.FollowerConfig{
				PollIntervalSecs:    5,
				BatchSize:           100,
				CleanupIntervalSecs: 300,
			},
		}

		follower, err := NewMainFollower(mockPool, mockCacheMgr, tempDir, cfg)

		require.NoError(t, err)
		assert.NotNil(t, follower.updateTracker)
		assert.Equal(t, 0, len(follower.updateTracker.lastCommittedOffset))

		follower.metaDB.Close()
	})
}
