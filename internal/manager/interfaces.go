package manager

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/leader"
	"go.mongodb.org/mongo-driver/mongo"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// CollectionManager defines the interface for managing a single collection
type CollectionManager interface {
	ManageCollection(
		ctx context.Context,
		wg *sync.WaitGroup,
		replicatedColl db.ReplicatedCollection,
		pgPool *pgxpool.Pool,
		mongoClient *mongo.Client,
		mongoDBName string,
		leaderElector *leader.LeaderElector,
		cfg *config.Config,
	)
}

// CollectionManagerFunc is a function type that matches ManageCollection signature
type CollectionManagerFunc func(
	ctx context.Context,
	wg *sync.WaitGroup,
	replicatedColl db.ReplicatedCollection,
	pgPool interface{}, // Using interface{} to accept both *pgxpool.Pool and db.PostgresPool
	mongoClient *mongo.Client,
	mongoDBName string,
	leaderElector interface{}, // Using interface{} to accept both *leader.LeaderElector and leader.LeaderElectorInterface
	cfg *config.Config,
)
