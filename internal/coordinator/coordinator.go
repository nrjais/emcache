package coordinator

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/collectioncache"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/leader"
	"github.com/nrjais/emcache/internal/manager"
	"go.mongodb.org/mongo-driver/mongo"
)

type Coordinator struct {
	pgPool             *pgxpool.Pool
	mongoClient        *mongo.Client
	mongoDBName        string
	leaderElector      *leader.LeaderElector
	collCache          *collectioncache.Manager
	cfg                *config.Config
	managedCollections map[string]context.CancelFunc
	mu                 sync.Mutex
	wg                 *sync.WaitGroup
}

func NewCoordinator(pgPool *pgxpool.Pool, mongoClient *mongo.Client, mongoDBName string,
	leaderElector *leader.LeaderElector, cacheMgr *collectioncache.Manager, cfg *config.Config, wg *sync.WaitGroup) *Coordinator {
	return &Coordinator{
		pgPool:             pgPool,
		mongoClient:        mongoClient,
		mongoDBName:        mongoDBName,
		leaderElector:      leaderElector,
		collCache:          cacheMgr,
		cfg:                cfg,
		managedCollections: make(map[string]context.CancelFunc),
		wg:                 wg,
	}
}

func (c *Coordinator) Start(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Panic during periodic collection sync",
				"component", "Coordinator",
				"error", r)
		}
	}()
	slog.Info("Starting", "component", "Coordinator")
	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled. Stopping coordinator and managed collections",
				"component", "Coordinator")
			c.stopAllManaging()
			return
		case <-c.collCache.RefreshCh:
			slog.Info("Received cache refresh signal. Running periodic collection sync",
				"component", "Coordinator")
			if err := c.syncCollections(ctx); err != nil {
				slog.Error("Error during periodic collection sync",
					"component", "Coordinator",
					"error", err)
			}
			slog.Info("Periodic collection sync finished",
				"component", "Coordinator")
		}
	}
}

func (c *Coordinator) syncCollections(ctx context.Context) error {
	slog.Info("Starting syncCollections",
		"component", "Coordinator")
	cachedCollections := c.collCache.GetAllCollections()

	cachedCollectionsSet := make(map[string]db.ReplicatedCollection, len(cachedCollections))
	for _, coll := range cachedCollections {
		cachedCollectionsSet[coll.CollectionName] = coll
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	currentManagedKeys := make([]string, 0, len(c.managedCollections))
	for k := range c.managedCollections {
		currentManagedKeys = append(currentManagedKeys, k)
	}

	for collName, replicatedColl := range cachedCollectionsSet {
		if _, exists := c.managedCollections[collName]; !exists {
			slog.Info("Detected new collection to manage from cache",
				"component", "Coordinator",
				"collection", collName)
			c.startManaging(ctx, replicatedColl)
		}
	}

	for _, managedCollKey := range currentManagedKeys {
		if _, existsInCache := cachedCollectionsSet[managedCollKey]; !existsInCache {
			slog.Info("Detected removed collection to stop managing",
				"component", "Coordinator",
				"collection", managedCollKey,
				"reason", "no longer in cache")
			c.stopManaging(managedCollKey)
		}
	}

	return nil
}

func (c *Coordinator) startManaging(parentCtx context.Context, replicatedColl db.ReplicatedCollection) {
	collectionName := replicatedColl.CollectionName

	if _, exists := c.managedCollections[collectionName]; exists {
		slog.Info("Collection is already being managed",
			"component", "Coordinator",
			"collection", collectionName)
		return
	}

	collCtx, collCancel := context.WithCancel(parentCtx)

	c.managedCollections[collectionName] = collCancel

	c.wg.Add(1)
	go manager.ManageCollection(collCtx, c.wg, replicatedColl, c.pgPool, c.mongoClient, c.mongoDBName, c.leaderElector, c.cfg)
}

func (c *Coordinator) stopManaging(collectionName string) {
	cancelFunc, exists := c.managedCollections[collectionName]
	if !exists {
		slog.Info("Collection is not currently managed",
			"component", "Coordinator",
			"collection", collectionName)
		return
	}

	slog.Info("Stopping management",
		"component", "Coordinator",
		"collection", collectionName)
	cancelFunc()
	delete(c.managedCollections, collectionName)
}

func (c *Coordinator) stopAllManaging() {
	c.mu.Lock()
	defer c.mu.Unlock()

	slog.Info("Stopping management for collections",
		"component", "Coordinator",
		"count", len(c.managedCollections))
	for name, cancelFunc := range c.managedCollections {
		slog.Info("Signaling stop during shutdown",
			"component", "Coordinator",
			"collection", name)
		cancelFunc()
	}
	c.managedCollections = make(map[string]context.CancelFunc)
}
