package follower

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrjais/emcache/internal/collectioncache"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/shape"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var ErrCollectionNotFound = errors.New("collection not found")

type conn struct {
	conn  *sqlite.Conn
	shape shape.Shape
}
type colVersion struct {
	collectionName string
	version        int
}

type batchUpdateTracker struct {
	lastCommittedOffset map[colVersion]int64
	mutex               sync.Mutex
}

func newBatchUpdateTracker() *batchUpdateTracker {
	return &batchUpdateTracker{
		lastCommittedOffset: make(map[colVersion]int64),
	}
}

func (t *batchUpdateTracker) updateOffset(dbKey colVersion, offset int64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.lastCommittedOffset[dbKey] = offset
}

func (t *batchUpdateTracker) isStale(dbKey colVersion, globalOffset int64, batchSize int) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	lastOffset, exists := t.lastCommittedOffset[dbKey]
	if !exists {
		return false
	}

	threshold := globalOffset - int64(5*batchSize)
	return lastOffset < threshold
}

func (t *batchUpdateTracker) getAllTrackedDBs() []colVersion {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	dbKeys := make([]colVersion, 0, len(t.lastCommittedOffset))
	for dbKey := range t.lastCommittedOffset {
		dbKeys = append(dbKeys, dbKey)
	}
	return dbKeys
}

type MainFollower struct {
	collCache         collectioncache.CollectionCacheManager
	pgPool            db.PostgresPool
	sqliteBaseDir     string
	pollInterval      time.Duration
	cleanupInterval   time.Duration
	batchSize         int
	globalLastOplogID int64
	connections       map[string]conn
	connMutex         sync.Mutex
	metaDB            *sqlite.Conn
	updateTracker     *batchUpdateTracker
}

func NewMainFollower(pgPool db.PostgresPool, cacheMgr collectioncache.CollectionCacheManager, sqliteBaseDir string, cfg *config.Config) (*MainFollower, error) {
	pollInterval := time.Duration(cfg.FollowerOptions.PollIntervalSecs) * time.Second
	batchSize := cfg.FollowerOptions.BatchSize

	cleanupInterval := time.Duration(cfg.FollowerOptions.CleanupIntervalSecs) * time.Second

	if err := os.MkdirAll(sqliteBaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base SQLite directory: %w", err)
	}

	metaDBPath := filepath.Join(sqliteBaseDir, "meta.sqlite")
	metaDB, err := sqlite.OpenConn(metaDBPath, sqlite.OpenReadWrite, sqlite.OpenWAL, sqlite.OpenCreate)
	if err != nil {
		slog.Error("Failed to open meta DB", "error", err)
		return nil, err
	}

	err = initMetaTable(metaDB)
	if err != nil {
		slog.Error("Failed to initialize meta DB", "error", err)
		return nil, err
	}

	cf := &MainFollower{
		collCache:         cacheMgr,
		pgPool:            pgPool,
		sqliteBaseDir:     sqliteBaseDir,
		pollInterval:      pollInterval,
		cleanupInterval:   cleanupInterval,
		batchSize:         batchSize,
		globalLastOplogID: 0,
		connections:       make(map[string]conn),
		metaDB:            metaDB,
		updateTracker:     newBatchUpdateTracker(),
	}
	err = cf.initializeGlobalLastOplogID()
	if err != nil {
		slog.Error("Failed to initialize global last processed oplog ID", "error", err)
		return nil, err
	}

	return cf, nil
}

func (cf *MainFollower) initializeGlobalLastOplogID() error {
	slog.Info("Initializing global last processed oplog ID")
	var lastOplogID int64

	lastOplogID, err := getLastAppliedOplogIndex(cf.metaDB)
	if err != nil {
		slog.Error("Failed to query meta DB", "error", err)
		return err
	}

	cf.globalLastOplogID = lastOplogID
	slog.Info("Global last processed oplog ID initialized", "id", lastOplogID)
	return nil
}

func (cf *MainFollower) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	slog.Info("MainFollower starting")

	var loopWg sync.WaitGroup
	loopWg.Add(2)

	go cf.runMainLoop(ctx, &loopWg)
	go cf.runCleanupLoop(ctx, &loopWg)

	loopWg.Wait()
	slog.Info("MainFollower stopped")
	cf.closeAllConnections()
}

func (cf *MainFollower) runMainLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Info("Main loop started")

	pollingInterval := cf.pollInterval
	for {
		timer := time.NewTimer(pollingInterval)
		pollingInterval = cf.pollInterval
		select {
		case <-ctx.Done():
			slog.Info("Main loop stopping due to context cancellation")
			timer.Stop()
			return
		case <-timer.C:
			batchMaxProcessedID := cf.globalLastOplogID
			slog.Info("Fetching oplog entries", "after_id", batchMaxProcessedID)
			entries, err := db.GetOplogEntriesGlobal(ctx, cf.pgPool, batchMaxProcessedID, cf.batchSize)
			if err != nil {
				slog.Error("Failed to fetch oplog entries", "after_id", batchMaxProcessedID, "error", err)
				continue
			}

			slog.Info("Fetched new oplog entries", "count", len(entries), "after_id", batchMaxProcessedID)

			processedCount := 0
			batchFailed := false

			entriesByCollection := make(map[colVersion][]db.OplogEntry)
			for _, entry := range entries {
				colVersion := colVersion{entry.Collection, entry.Version}
				entriesByCollection[colVersion] = append(entriesByCollection[colVersion], entry)
			}

			for colVersion, entries := range entriesByCollection {
				conn, err := cf.getOrCreateConnection(ctx, colVersion.collectionName, colVersion.version)
				if err != nil {
					if errors.Is(err, ErrCollectionNotFound) {
						slog.Error("Collection not found, skipping batch", "collection", colVersion.collectionName, "version", colVersion.version)
						continue
					}
					slog.Error("Failed to get/create SQLite connection", "collection", colVersion.collectionName, "version", colVersion.version, "error", err)
					batchFailed = true
					break
				}

				lastId, err := cf.applyBatchEntries(conn, entries)
				if err != nil {
					slog.Error("Failed to apply oplog entries", "collection", colVersion.collectionName, "version", colVersion.version, "error", err)
					batchFailed = true
					break
				}

				cf.updateTracker.updateOffset(colVersion, lastId)

				batchMaxProcessedID = max(batchMaxProcessedID, lastId)
				processedCount += len(entries)
			}

			if !batchFailed && batchMaxProcessedID > cf.globalLastOplogID {
				cf.globalLastOplogID = batchMaxProcessedID
				err = setLastAppliedOplogIndex(cf.metaDB, batchMaxProcessedID)
				if err != nil {
					slog.Error("Failed to update meta DB", "error", err)
				}
				slog.Info("Applied oplog entries", "count", processedCount, "new_last_id", batchMaxProcessedID)
			} else {
				slog.Info("Applied oplog entries with no ID update",
					"count", processedCount,
					"current_id", cf.globalLastOplogID,
					"batch_max_id", batchMaxProcessedID)
			}

			cf.updateStaleDBOffsets(ctx)

			if len(entries) == cf.batchSize {
				pollingInterval = 1 * time.Millisecond
			}
		}
	}
}

func (cf *MainFollower) runCleanupLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(cf.cleanupInterval)
	defer ticker.Stop()

	slog.Info("Cleanup loop started")

	slog.Info("Running initial cleanup of old SQLite files")
	if err := cf.cleanupOldFiles(); err != nil {
		slog.Error("Error during initial cleanup", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("Cleanup loop stopping due to context cancellation")
			return
		case <-ticker.C:
			slog.Info("Running cleanup of old SQLite files using cache")
			if err := cf.cleanupOldFiles(); err != nil {
				slog.Error("Error during cleanup", "error", err)
			}
		}
	}
}

func (cf *MainFollower) dbKey(collectionName string, version int) string {
	return fmt.Sprintf("%s_v%d", collectionName, version)
}

func (cf *MainFollower) getOrCreateConnection(ctx context.Context, collectionName string, version int) (conn, error) {
	cf.connMutex.Lock()
	defer cf.connMutex.Unlock()

	dbKey := cf.dbKey(collectionName, version)
	if conn, exists := cf.connections[dbKey]; exists {
		return conn, nil
	}

	dbPath := GetCollectionDBPath(collectionName, cf.sqliteBaseDir, version)
	replicatedColl, found, err := cf.collCache.GetCollectionRefresh(ctx, collectionName)
	if err != nil {
		slog.Error("Failed to get collection", "collection", collectionName, "error", err)
		return conn{}, err
	}
	if !found {
		slog.Error("Collection not found", "collection", collectionName)
		return conn{}, ErrCollectionNotFound
	}

	slog.Info("Opening connection for", "collection", collectionName, "at", dbPath)
	sqliteConn, err := openCollectionDB(collectionName, cf.sqliteBaseDir, dbPath, version, replicatedColl.Shape)
	if err != nil {
		return conn{}, err
	}

	sqliteConn.SetInterrupt(ctx.Done())

	reset, err := GetOrResetLocalDBVersion(sqliteConn, version)
	if err != nil {
		sqliteConn.Close()
		slog.Error("Error getting/resetting internal version", "path", dbPath, "error", err)
		return conn{}, fmt.Errorf("failed to get/reset internal version for %s: %w", dbPath, err)
	}
	if reset {
		slog.Info("Version mismatch in existing file, resetting to", "version", version, "for", dbPath)
	}

	conn := conn{conn: sqliteConn, shape: replicatedColl.Shape}
	cf.connections[dbKey] = conn
	return conn, nil
}

func (cf *MainFollower) applyBatchEntries(conn conn, entries []db.OplogEntry) (lastId int64, err error) {
	defer sqlitex.Save(conn.conn)(&err)

	lastId = entries[0].ID
	for _, entry := range entries {
		if err = applyOplogEntry(conn.conn, entry, conn.shape); err != nil {
			return 0, fmt.Errorf("failed to apply operation for entry ID %d: %w", entry.ID, err)
		}
		lastId = entry.ID
	}

	if err = setLastAppliedOplogIndex(conn.conn, lastId); err != nil {
		return 0, fmt.Errorf("failed to set last applied index to %d for entry ID %d: %w", lastId, lastId, err)
	}

	return lastId, nil
}

func (cf *MainFollower) cleanupOldFiles() error {
	cachedCollections := cf.collCache.GetAllCollections()

	currentVersionMap := make(map[string]int)
	for _, coll := range cachedCollections {
		currentVersionMap[coll.CollectionName] = coll.CurrentVersion
	}

	replicasDir := filepath.Join(cf.sqliteBaseDir, "replicas")

	if _, err := os.Stat(replicasDir); os.IsNotExist(err) {
		return nil
	}

	replicas, err := os.ReadDir(replicasDir)
	if err != nil {
		return fmt.Errorf("failed to read collections directory %s: %w", replicasDir, err)
	}

	deletedCount := 0

	for _, replica := range replicas {
		if !replica.IsDir() {
			continue
		}
		deletedCount := 0

		replicaName := replica.Name()
		replicaPath := filepath.Join(replicasDir, replicaName)
		collectionName, version := parseDBDirName(replicaName)

		currentVersion, collectionExistsInCache := currentVersionMap[collectionName]
		stale := !collectionExistsInCache || (collectionExistsInCache && version < currentVersion)

		if stale {
			slog.Info("Deleting stale file", "file", replicaPath, "collection", collectionName, "current_version_in_cache", currentVersion, "collection_exists_in_cache", collectionExistsInCache)

			cf.connMutex.Lock()
			if conn, exists := cf.connections[replicaName]; exists {
				slog.Info("Closing connection for before deleting file", "collection", collectionName)
				conn.conn.Close()
				delete(cf.connections, replicaName)
			}
			cf.connMutex.Unlock()

			if err := os.RemoveAll(replicaPath); err != nil {
				slog.Error("Failed to delete file", "file", replicaPath, "error", err)
			} else {
				deletedCount++
			}
		}
	}

	if deletedCount > 0 {
		slog.Info("Deleted stale SQLite files", "count", deletedCount)
	}

	return nil
}

func parseDBDirName(dirName string) (collectionName string, version int) {
	parts := strings.Split(dirName, "_v")
	if len(parts) != 2 {
		return
	}
	collectionName = parts[0]
	versionStr := parts[1]
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return
	}
	return
}

func (cf *MainFollower) closeAllConnections() {
	cf.connMutex.Lock()
	defer cf.connMutex.Unlock()

	if len(cf.connections) == 0 {
		return
	}

	slog.Info("Closing cached connections", "count", len(cf.connections))
	for key, conn := range cf.connections {
		if err := conn.conn.Close(); err != nil {
			slog.Error("Error closing connection", "key", key, "error", err)
		}
		delete(cf.connections, key)
	}
}

func (cf *MainFollower) updateStaleDBOffsets(ctx context.Context) {
	trackedDBs := cf.updateTracker.getAllTrackedDBs()

	for _, dbKey := range trackedDBs {
		if cf.updateTracker.isStale(dbKey, cf.globalLastOplogID, cf.batchSize) {
			slog.Info("Updating stale database offset", "dbKey", dbKey, "to_offset", cf.globalLastOplogID)

			collectionName := dbKey.collectionName
			version := dbKey.version

			conn, err := cf.getOrCreateConnection(ctx, collectionName, version)
			if err != nil {
				if errors.Is(err, ErrCollectionNotFound) {
					slog.Error("Collection not found, skipping update", "dbKey", dbKey)
					continue
				}
				slog.Error("Failed to get/create SQLite connection", "dbKey", dbKey, "error", err)
				continue
			}

			err = setLastAppliedOplogIndex(conn.conn, cf.globalLastOplogID)
			if err != nil {
				slog.Error("Failed to update stale database offset", "dbKey", dbKey, "error", err)
				continue
			}

			cf.updateTracker.updateOffset(dbKey, cf.globalLastOplogID)
			slog.Info("Updated stale database offset", "dbKey", dbKey, "offset", cf.globalLastOplogID)
		}
	}
}
