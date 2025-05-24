package collectioncache

import (
	"context"

	"github.com/nrjais/emcache/internal/db"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// CollectionCacheManager defines the interface for managing collection cache
type CollectionCacheManager interface {
	Start(ctx context.Context)
	Stop()
	GetCollection(name string) (db.ReplicatedCollection, bool)
	GetAllCollections() []db.ReplicatedCollection
	GetCollectionRefresh(ctx context.Context, name string) (db.ReplicatedCollection, bool, error)
	RefreshChannel() <-chan struct{}
}
