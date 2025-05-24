package snapshot

import (
	"context"
	"sync"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// SnapshotManager defines the interface for snapshot management
type SnapshotManager interface {
	GetOrGenerateSnapshot(ctx context.Context, dbPath, snapshotDir, snapshotFileName string) (string, func(), error)
	StartCleanupLoop(ctx context.Context, wg *sync.WaitGroup, ttl int64, snapshotDir string)
}
