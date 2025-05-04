package coordinator

import (
	"context"
	"log"
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
			log.Printf("[Coordinator] Panic during periodic collection sync: %v", r)
		}
	}()
	log.Println("[Coordinator] Starting...")
	for {
		select {
		case <-ctx.Done():
			log.Println("[Coordinator] Context cancelled. Stopping coordinator and managed collections...")
			c.stopAllManaging()
			return
		case <-c.collCache.RefreshCh:
			log.Println("[Coordinator] Received cache refresh signal. Running periodic collection sync...")
			if err := c.syncCollections(ctx); err != nil {
				log.Printf("[Coordinator] Error during periodic collection sync: %v", err)
			}
			log.Println("[Coordinator] Periodic collection sync finished.")
		}
	}
}

func (c *Coordinator) syncCollections(ctx context.Context) error {
	log.Println("[Coordinator] Starting syncCollections...")
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
			log.Printf("[Coordinator] Detected new collection to manage from cache: %s", collName)
			c.startManaging(ctx, replicatedColl)
		}
	}

	for _, managedCollKey := range currentManagedKeys {
		if _, existsInCache := cachedCollectionsSet[managedCollKey]; !existsInCache {
			log.Printf("[Coordinator] Detected removed collection (no longer in cache) to stop managing: %s", managedCollKey)
			c.stopManaging(managedCollKey)
		}
	}

	return nil
}

func (c *Coordinator) startManaging(parentCtx context.Context, replicatedColl db.ReplicatedCollection) {
	collectionName := replicatedColl.CollectionName

	if _, exists := c.managedCollections[collectionName]; exists {
		log.Printf("[Coordinator:%s] Collection is already being managed.", collectionName)
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
		log.Printf("[Coordinator:%s] Collection is not currently managed.", collectionName)
		return
	}

	log.Printf("[Coordinator:%s] Stopping management.", collectionName)
	cancelFunc()
	delete(c.managedCollections, collectionName)
}

func (c *Coordinator) stopAllManaging() {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("[Coordinator] Stopping management for %d collections.", len(c.managedCollections))
	for name, cancelFunc := range c.managedCollections {
		log.Printf("[Coordinator:%s] Signaling stop during shutdown.", name)
		cancelFunc()
	}
	c.managedCollections = make(map[string]context.CancelFunc)
}
