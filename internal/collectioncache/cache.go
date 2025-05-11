package collectioncache

import (
	"context"
	"fmt"
	"log/slog"
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
	RefreshCh       chan struct{}
}

func NewManager(pgPool *pgxpool.Pool, cfg *config.Config) *Manager {
	interval := time.Duration(cfg.CoordinatorOptions.CollectionRefreshIntervalSecs) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
		slog.Warn("Invalid refresh interval, using default",
			"configured_seconds", cfg.CoordinatorOptions.CollectionRefreshIntervalSecs,
			"default", interval)
	}
	return &Manager{
		pgPool:          pgPool,
		refreshInterval: interval,
		collections:     make(map[string]db.ReplicatedCollection),
		RefreshCh:       make(chan struct{}, 1),
	}
}

func (m *Manager) Start(ctx context.Context, wg *sync.WaitGroup) {
	slog.Info("Collection cache starting")
	wg.Add(1)

	if err := m.refresh(ctx); err != nil {
		slog.Error("Initial fetch failed, cache might be empty", "error", err)
	} else {
		slog.Info("Initial fetch successful", "collections_loaded", len(m.collections))
	}

	go func() {
		defer wg.Done()
		slog.Info("Refresh loop started", "interval", m.refreshInterval)
		for {
			select {
			case <-time.After(m.refreshInterval):
				if err := m.refresh(ctx); err != nil {
					slog.Error("Failed to refresh collections", "error", err)
				} else {
					m.mu.RLock()
					count := len(m.collections)
					m.mu.RUnlock()
					slog.Info("Collections refreshed", "count", count)
				}
			case <-ctx.Done():
				slog.Info("Context cancelled, refresh loop exiting")
				return
			}
		}
	}()
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
		slog.Warn("Refresh channel is blocked, skipping notification")
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

func (m *Manager) GetCollectionRefresh(ctx context.Context, name string) (db.ReplicatedCollection, bool, error) {
	col, found := m.GetCollection(name)
	if !found {
		err := m.refresh(ctx)
		if err != nil {
			return db.ReplicatedCollection{}, false, err
		}
		col, found = m.GetCollection(name)
	}
	return col, found, nil
}
