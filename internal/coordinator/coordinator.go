package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	cfg                *config.Config
	checkInterval      time.Duration
	managedCollections map[string]context.CancelFunc
	mu                 sync.Mutex
	wg                 *sync.WaitGroup
}

func NewCoordinator(pgPool *pgxpool.Pool, mongoClient *mongo.Client, mongoDBName string, leaderElector *leader.LeaderElector, cfg *config.Config, wg *sync.WaitGroup) *Coordinator {
	checkInterval := time.Duration(cfg.CoordinatorOptions.CollectionRefreshIntervalSecs) * time.Second

	return &Coordinator{
		pgPool:             pgPool,
		mongoClient:        mongoClient,
		mongoDBName:        mongoDBName,
		leaderElector:      leaderElector,
		cfg:                cfg,
		checkInterval:      checkInterval,
		managedCollections: make(map[string]context.CancelFunc),
		wg:                 wg,
	}
}

func (c *Coordinator) Start(ctx context.Context) {
	log.Println("[Coordinator] Starting...")

	if err := c.syncCollections(ctx); err != nil {
		log.Printf("[Coordinator] CRITICAL: Initial collection sync failed: %v. Coordinator stopping.", err)
		return
	}

	ticker := time.NewTicker(c.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Coordinator] Context cancelled. Stopping coordinator and managed collections...")
			c.stopAllManaging()
			return
		case <-ticker.C:
			log.Println("[Coordinator] Running periodic collection sync...")
			if err := c.syncCollections(ctx); err != nil {
				log.Printf("[Coordinator] Error during periodic collection sync: %v", err)
			}
		}
	}
}

func (c *Coordinator) syncCollections(ctx context.Context) error {
	dbCollections, err := db.ListReplicatedCollections(ctx, c.pgPool)
	if err != nil {
		return fmt.Errorf("failed to list replicated collections: %w", err)
	}

	dbCollectionsSet := make(map[string]struct{}, len(dbCollections))
	for _, coll := range dbCollections {
		dbCollectionsSet[coll] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	currentManagedKeys := make([]string, 0, len(c.managedCollections))
	for k := range c.managedCollections {
		currentManagedKeys = append(currentManagedKeys, k)
	}

	for _, dbColl := range dbCollections {
		if _, exists := c.managedCollections[dbColl]; !exists {
			log.Printf("[Coordinator] Detected new collection to manage: %s", dbColl)
			c.startManaging(ctx, dbColl)
		}
	}

	for _, managedCollKey := range currentManagedKeys {
		if _, existsInDB := dbCollectionsSet[managedCollKey]; !existsInDB {
			log.Printf("[Coordinator] Detected removed collection to stop managing: %s", managedCollKey)
			c.stopManaging(managedCollKey)
		}
	}

	return nil
}

func (c *Coordinator) startManaging(parentCtx context.Context, collectionName string) {
	if _, exists := c.managedCollections[collectionName]; exists {
		log.Printf("[Coordinator:%s] Collection is already being managed.", collectionName)
		return
	}

	log.Printf("[Coordinator:%s] Starting management.", collectionName)

	collCtx, collCancel := context.WithCancel(parentCtx)
	c.managedCollections[collectionName] = collCancel

	c.wg.Add(1)
	go manager.ManageCollection(collCtx, c.wg, collectionName, c.pgPool, c.mongoClient, c.mongoDBName, c.leaderElector, c.cfg)
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
