package collectioncache

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
)

type Manager struct {
	pgPool          *pgxpool.Pool
	refreshInterval time.Duration
	mu              sync.RWMutex
	collections     map[string]db.ReplicatedCollection
	stopCh          chan struct{}
	RefreshCh       chan struct{}
}

func NewManager(pgPool *pgxpool.Pool, cfg *config.Config) *Manager {
	interval := time.Duration(cfg.CoordinatorOptions.CollectionRefreshIntervalSecs) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
		log.Printf("[CollectionCache] Invalid refresh interval %d, using default %v", cfg.CoordinatorOptions.CollectionRefreshIntervalSecs, interval)
	}
	return &Manager{
		pgPool:          pgPool,
		refreshInterval: interval,
		collections:     make(map[string]db.ReplicatedCollection),
		stopCh:          make(chan struct{}),
		RefreshCh:       make(chan struct{}, 1),
	}
}

func (m *Manager) Start(ctx context.Context, wg *sync.WaitGroup) {
	log.Println("[CollectionCache] Starting...")
	wg.Add(1)

	if err := m.refresh(ctx); err != nil {
		log.Printf("[CollectionCache] CRITICAL: Initial fetch failed: %v. Cache might be empty.", err)
	} else {
		log.Printf("[CollectionCache] Initial fetch successful, %d collections loaded.", len(m.collections))
	}

	go func() {
		defer wg.Done()
		log.Printf("[CollectionCache] Refresh loop started with interval %v.", m.refreshInterval)
		for {
			timer := time.NewTimer(m.refreshInterval)
			select {
			case <-timer.C:
				if err := m.refresh(ctx); err != nil {
					log.Printf("[CollectionCache] Error during periodic refresh: %v", err)
				} else {
					m.mu.RLock()
					count := len(m.collections)
					m.mu.RUnlock()
					log.Printf("[CollectionCache] Periodic refresh successful, %d collections loaded.", count)
				}
			case <-m.stopCh:
				log.Println("[CollectionCache] Received stop signal. Refresh loop exiting.")
				return
			case <-ctx.Done():
				log.Println("[CollectionCache] Context cancelled. Refresh loop exiting.")
				return
			}
		}
	}()
}

func (m *Manager) Stop() {
	log.Println("[CollectionCache] Signaling stop...")
	close(m.stopCh)
}

func (m *Manager) refresh(ctx context.Context) error {
	ctxTimeout, cancel := context.WithTimeout(ctx, m.refreshInterval*time.Second)
	defer cancel()

	collectionsList, err := db.GetAllReplicatedCollectionsWithShapes(ctxTimeout, m.pgPool)
	if err != nil {
		return fmt.Errorf("failed to get all collections with shapes from DB: %w", err)
	}

	newCollectionMap := make(map[string]db.ReplicatedCollection, len(collectionsList))
	for _, coll := range collectionsList {
		newCollectionMap[coll.CollectionName] = coll
	}

	m.mu.Lock()
	m.collections = newCollectionMap
	m.mu.Unlock()

	select {
	case m.RefreshCh <- struct{}{}:
	default:
		log.Println("[CollectionCache] Refresh channel is blocked. Skipping refresh.")
	}
	return nil
}

func (m *Manager) GetCollection(name string) (db.ReplicatedCollection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	coll, found := m.collections[name]
	return coll, found
}

func (m *Manager) GetAllCollections() []db.ReplicatedCollection {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]db.ReplicatedCollection, 0, len(m.collections))
	for _, coll := range m.collections {
		list = append(list, coll)
	}
	return list
}

func (m *Manager) GetCollectionRefresh(ctx context.Context, name string) (db.ReplicatedCollection, bool) {
	coll, found := m.GetCollection(name)

	if !found {
		ctxTimeout, cancel := context.WithTimeout(ctx, m.refreshInterval*time.Second)
		defer cancel()
		m.refresh(ctxTimeout)
		coll, found = m.GetCollection(name)
	}

	return coll, found
}
