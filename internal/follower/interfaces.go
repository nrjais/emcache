package follower

import (
	"context"
	"sync"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// FollowerInterface defines the interface for the main follower functionality
type FollowerInterface interface {
	Start(ctx context.Context, wg *sync.WaitGroup)
	EnsureOpenConnection(ctx context.Context, collectionName string, version int) error
}
