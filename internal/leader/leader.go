package leader

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
)

type LeaderElector struct {
	pool       *pgxpool.Pool
	instanceID string
}

func NewElector(pool *pgxpool.Pool, instanceID string) *LeaderElector {
	return &LeaderElector{
		pool:       pool,
		instanceID: instanceID,
	}
}

func (e *LeaderElector) TryAcquire(ctx context.Context, collectionName string, leaseDuration time.Duration) (bool, error) {
	sql := `
		INSERT INTO leader_locks (collection_name, leader_id, lease_expires_at)
		VALUES ($1, $2, NOW() + $3 * interval '1 second')
		ON CONFLICT (collection_name) DO UPDATE SET
			leader_id = EXCLUDED.leader_id,
			lease_expires_at = EXCLUDED.lease_expires_at
		WHERE leader_locks.lease_expires_at < NOW() OR leader_locks.leader_id = $2;
	`
	leaseSeconds := int(leaseDuration.Seconds())

	acquireCtx, acquireCancel := context.WithTimeout(ctx, 5*time.Second)
	defer acquireCancel()

	cmdTag, err := e.pool.Exec(acquireCtx, sql, collectionName, e.instanceID, leaseSeconds)
	if err != nil {
		return false, fmt.Errorf("[%s] failed to acquire/renew leadership lock: %w", collectionName, err)
	}

	acquired := cmdTag.RowsAffected() > 0
	if acquired {
		slog.Info("Leadership lease acquired/renewed",
			"collection", collectionName,
			"duration", leaseDuration)
	}

	return acquired, nil
}

func (e *LeaderElector) Release(collectionName string) error {
	sql := `
		DELETE FROM leader_locks
		WHERE collection_name = $1 AND leader_id = $2;
	`
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer releaseCancel()

	cmdTag, err := e.pool.Exec(releaseCtx, sql, collectionName, e.instanceID)
	if err != nil {
		slog.Error("Failed to delete leadership lease",
			"collection", collectionName,
			"error", err)
		return fmt.Errorf("failed to delete leadership record: %w", err)
	}

	if cmdTag.RowsAffected() > 0 {
		slog.Info("Leadership lease released", "collection", collectionName)
	} else {
		slog.Info("Leadership lease not found",
			"collection", collectionName,
			"note", "Not the leader or record did not exist")
	}

	return nil
}

func (e *LeaderElector) ReleaseAll() {
	slog.Info("Releasing all leadership leases", "instance_id", e.instanceID)
	sql := `
		DELETE FROM leader_locks
		WHERE leader_id = $1;
	`
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer releaseCancel()

	cmdTag, err := e.pool.Exec(releaseCtx, sql, e.instanceID)
	if err != nil {
		slog.Error("Failed to release all leadership leases",
			"instance_id", e.instanceID,
			"error", err)
	} else {
		slog.Info("Leadership leases released",
			"instance_id", e.instanceID,
			"count", cmdTag.RowsAffected())
	}
}

func (e *LeaderElector) IsLeader(collectionName string, leaseDuration time.Duration) bool {
	extendSql := `
		UPDATE leader_locks SET lease_expires_at = NOW() + $3 * interval '1 second'
		WHERE collection_name = $1 AND leader_id = $2;
	`
	leaseSeconds := int(leaseDuration.Seconds())
	extendCtx, extendCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer extendCancel()

	cmdTag, err := e.pool.Exec(extendCtx, extendSql, collectionName, e.instanceID, leaseSeconds)
	if err != nil {
		slog.Error("Failed to extend leadership lease",
			"collection", collectionName,
			"error", err)
		return false
	} else if cmdTag.RowsAffected() == 0 {
		slog.Warn("Leadership lease extension failed",
			"collection", collectionName,
			"reason", "Lease might have expired")
		return false
	}

	return true
}
