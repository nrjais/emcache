package leader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/nrjais/emcache/internal/db/mocks"
)

func TestNewElector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("creates new elector with all parameters", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		instanceID := "test-instance-123"

		elector := NewElector(mockPool, instanceID)

		assert.NotNil(t, elector)
		assert.Equal(t, mockPool, elector.pool)
		assert.Equal(t, instanceID, elector.instanceID)
	})

	t.Run("creates elector with empty instance ID", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		instanceID := ""

		elector := NewElector(mockPool, instanceID)

		assert.NotNil(t, elector)
		assert.Equal(t, instanceID, elector.instanceID)
	})
}

func TestLeaderElector_TryAcquire(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("successfully acquires leadership", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(1))

		mockPool.EXPECT().Exec(
			gomock.Any(),      // context
			gomock.Any(),      // SQL query
			"test-collection", // collection name
			"test-instance",   // instance ID
			30,                // lease duration in seconds
		).Return(mockResult, nil)

		ctx := context.Background()
		acquired, err := elector.TryAcquire(ctx, "test-collection", 30*time.Second)

		assert.NoError(t, err)
		assert.True(t, acquired)
	})

	t.Run("fails to acquire leadership when no rows affected", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(0))

		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-collection",
			"test-instance",
			30,
		).Return(mockResult, nil)

		ctx := context.Background()
		acquired, err := elector.TryAcquire(ctx, "test-collection", 30*time.Second)

		assert.NoError(t, err)
		assert.False(t, acquired)
	})

	t.Run("handles database error", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		dbErr := assert.AnError
		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-collection",
			"test-instance",
			30,
		).Return(nil, dbErr)

		ctx := context.Background()
		acquired, err := elector.TryAcquire(ctx, "test-collection", 30*time.Second)

		assert.Error(t, err)
		assert.False(t, acquired)
		assert.Contains(t, err.Error(), "failed to acquire/renew leadership lock")
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-collection",
			"test-instance",
			30,
		).Return(nil, context.Canceled)

		acquired, err := elector.TryAcquire(ctx, "test-collection", 30*time.Second)

		assert.Error(t, err)
		assert.False(t, acquired)
	})

	t.Run("handles different lease durations", func(t *testing.T) {
		testCases := []struct {
			name          string
			leaseDuration time.Duration
			expectedSecs  int
		}{
			{"1 minute", 1 * time.Minute, 60},
			{"30 seconds", 30 * time.Second, 30},
			{"2 hours", 2 * time.Hour, 7200},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockPool := mocks.NewMockPostgresPool(ctrl)
				elector := NewElector(mockPool, "test-instance")

				mockResult := mocks.NewMockCommandTag(ctrl)
				mockResult.EXPECT().RowsAffected().Return(int64(1))

				mockPool.EXPECT().Exec(
					gomock.Any(),
					gomock.Any(),
					"test-collection",
					"test-instance",
					tc.expectedSecs,
				).Return(mockResult, nil)

				ctx := context.Background()
				acquired, err := elector.TryAcquire(ctx, "test-collection", tc.leaseDuration)

				assert.NoError(t, err)
				assert.True(t, acquired)
			})
		}
	})
}

func TestLeaderElector_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("successfully releases leadership", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(1))

		mockPool.EXPECT().Exec(
			gomock.Any(), // context with timeout
			gomock.Any(), // SQL query
			"test-collection",
			"test-instance",
		).Return(mockResult, nil)

		err := elector.Release("test-collection")

		assert.NoError(t, err)
	})

	t.Run("handles no rows affected (not leader)", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(0))

		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-collection",
			"test-instance",
		).Return(mockResult, nil)

		err := elector.Release("test-collection")

		assert.NoError(t, err) // Should not error even if not leader
	})

	t.Run("handles database error", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		dbErr := assert.AnError
		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-collection",
			"test-instance",
		).Return(nil, dbErr)

		err := elector.Release("test-collection")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete leadership record")
	})
}

func TestLeaderElector_ReleaseAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("successfully releases all leases", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(3))

		mockPool.EXPECT().Exec(
			gomock.Any(), // context with timeout
			gomock.Any(), // SQL query
			"test-instance",
		).Return(mockResult, nil)

		// Should not panic or error
		elector.ReleaseAll()
	})

	t.Run("handles database error gracefully", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		dbErr := assert.AnError
		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-instance",
		).Return(nil, dbErr)

		// Should not panic even on error
		elector.ReleaseAll()
	})

	t.Run("handles no leases to release", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(0))

		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-instance",
		).Return(mockResult, nil)

		// Should not panic or error
		elector.ReleaseAll()
	})
}

func TestLeaderElector_IsLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("returns true when lease is successfully extended", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(1))

		mockPool.EXPECT().Exec(
			gomock.Any(), // context with timeout
			gomock.Any(), // SQL query
			"test-collection",
			"test-instance",
			60, // lease duration in seconds
		).Return(mockResult, nil)

		isLeader := elector.IsLeader("test-collection", 60*time.Second)

		assert.True(t, isLeader)
	})

	t.Run("returns false when no rows affected (lease expired)", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(0))

		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-collection",
			"test-instance",
			60,
		).Return(mockResult, nil)

		isLeader := elector.IsLeader("test-collection", 60*time.Second)

		assert.False(t, isLeader)
	})

	t.Run("returns false on database error", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		dbErr := assert.AnError
		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-collection",
			"test-instance",
			60,
		).Return(nil, dbErr)

		isLeader := elector.IsLeader("test-collection", 60*time.Second)

		assert.False(t, isLeader)
	})

	t.Run("handles different lease durations correctly", func(t *testing.T) {
		testCases := []struct {
			name          string
			leaseDuration time.Duration
			expectedSecs  int
		}{
			{"5 minutes", 5 * time.Minute, 300},
			{"15 seconds", 15 * time.Second, 15},
			{"1 hour", 1 * time.Hour, 3600},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockPool := mocks.NewMockPostgresPool(ctrl)
				elector := NewElector(mockPool, "test-instance")

				mockResult := mocks.NewMockCommandTag(ctrl)
				mockResult.EXPECT().RowsAffected().Return(int64(1))

				mockPool.EXPECT().Exec(
					gomock.Any(),
					gomock.Any(),
					"test-collection",
					"test-instance",
					tc.expectedSecs,
				).Return(mockResult, nil)

				isLeader := elector.IsLeader("test-collection", tc.leaseDuration)

				assert.True(t, isLeader)
			})
		}
	})
}

func TestLeaderElector_EdgeCases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("handles very long collection names", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		longCollectionName := string(make([]byte, 1000))
		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(1))

		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			longCollectionName,
			"test-instance",
			30,
		).Return(mockResult, nil)

		ctx := context.Background()
		acquired, err := elector.TryAcquire(ctx, longCollectionName, 30*time.Second)

		assert.NoError(t, err)
		assert.True(t, acquired)
	})

	t.Run("handles special characters in collection names", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		specialCollectionName := "test-collection-with-special-chars!@#$%"
		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(1))

		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			specialCollectionName,
			"test-instance",
			30,
		).Return(mockResult, nil)

		ctx := context.Background()
		acquired, err := elector.TryAcquire(ctx, specialCollectionName, 30*time.Second)

		assert.NoError(t, err)
		assert.True(t, acquired)
	})

	t.Run("handles zero lease duration", func(t *testing.T) {
		mockPool := mocks.NewMockPostgresPool(ctrl)
		elector := NewElector(mockPool, "test-instance")

		mockResult := mocks.NewMockCommandTag(ctrl)
		mockResult.EXPECT().RowsAffected().Return(int64(1))

		mockPool.EXPECT().Exec(
			gomock.Any(),
			gomock.Any(),
			"test-collection",
			"test-instance",
			0, // zero seconds
		).Return(mockResult, nil)

		ctx := context.Background()
		acquired, err := elector.TryAcquire(ctx, "test-collection", 0*time.Second)

		assert.NoError(t, err)
		assert.True(t, acquired)
	})
}
