package leader

import (
	"context"
	"time"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// LeaderElectorInterface defines the interface for leader election
type LeaderElectorInterface interface {
	TryAcquireLock(ctx context.Context, collectionName string, leaseDuration time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, collectionName string) error
	ReleaseAll()
}

// ChangeStreamHandler defines the interface for handling MongoDB change streams
type ChangeStreamHandler interface {
	StartLeaderWork(ctx context.Context, collectionName string, leaseDuration time.Duration) error
}
