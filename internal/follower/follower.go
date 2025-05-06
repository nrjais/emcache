package follower

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/collectioncache"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/shape"
	"golang.org/x/exp/constraints"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type conn struct {
	conn  *sqlite.Conn
	shape shape.Shape
}

type MainFollower struct {
	collCache         *collectioncache.Manager
	pgPool            *pgxpool.Pool
	sqliteBaseDir     string
	pollInterval      time.Duration
	cleanupInterval   time.Duration
	batchSize         int
	globalLastOplogID int64
	connections       map[string]conn
	connMutex         sync.Mutex
	metaDB            *sqlite.Conn
}

func NewMainFollower(pgPool *pgxpool.Pool, cacheMgr *collectioncache.Manager, sqliteBaseDir string, cfg *config.Config) (*MainFollower, error) {
	pollInterval := time.Duration(cfg.FollowerOptions.PollIntervalSecs) * time.Second
	batchSize := cfg.FollowerOptions.BatchSize

	cleanupInterval := time.Duration(cfg.FollowerOptions.CleanupIntervalSecs) * time.Second
	metaDBPath := filepath.Join(sqliteBaseDir, "meta.sqlite")
	metaDB, err := sqlite.OpenConn(metaDBPath, sqlite.OpenReadWrite, sqlite.OpenWAL, sqlite.OpenCreate)
	if err != nil {
		log.Printf("[MainFollower] Error opening meta DB: %v", err)
		return nil, err
	}

	err = initMetaTable(metaDB)
	if err != nil {
		log.Printf("[MainFollower] Error initializing meta DB: %v", err)
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
	}
	err = cf.initializeGlobalLastOplogID()
	if err != nil {
		log.Printf("[MainFollower] Error initializing global last processed oplog ID: %v", err)
		return nil, err
	}

	return cf, nil
}

func (cf *MainFollower) initializeGlobalLastOplogID() error {
	log.Println("[MainFollower] Initializing global last processed oplog ID...")
	var lastOplogID int64

	lastOplogID, err := getLastAppliedOplogIndex(cf.metaDB)
	if err != nil {
		log.Printf("[MainFollower] Error querying meta DB: %v", err)
		return err
	}

	cf.globalLastOplogID = lastOplogID
	log.Printf("[MainFollower] Initialized global last processed oplog ID to %d", cf.globalLastOplogID)
	return nil
}

func (cf *MainFollower) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("[MainFollower] Starting...")

	var loopWg sync.WaitGroup
	loopWg.Add(2)

	go cf.runMainLoop(ctx, &loopWg)
	go cf.runCleanupLoop(ctx, &loopWg)

	loopWg.Wait()
	log.Println("[MainFollower] Stopped.")
	cf.closeAllConnections()
}

func (cf *MainFollower) runMainLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("[MainFollower] Main loop started.")

	pollingInterval := cf.pollInterval
	for {
		timer := time.NewTimer(pollingInterval)
		pollingInterval = cf.pollInterval
		select {
		case <-ctx.Done():
			log.Println("[MainFollower] Main loop stopping due to context cancellation.")
			timer.Stop()
			return
		case <-timer.C:
			batchMaxProcessedID := cf.globalLastOplogID
			log.Printf("[MainFollower] Fetching global oplog entries after ID %d", batchMaxProcessedID)
			entries, err := db.GetOplogEntriesGlobal(ctx, cf.pgPool, batchMaxProcessedID, cf.batchSize)
			if err != nil {
				log.Printf("[MainFollower] Error fetching global oplog entries after ID %d: %v", batchMaxProcessedID, err)
				continue
			}

			if len(entries) == 0 {
				continue
			}

			log.Printf("[MainFollower] Fetched %d new oplog entries (after ID %d).", len(entries), batchMaxProcessedID)

			processedCount := 0
			batchFailed := false

			type colVersion struct {
				collectionName string
				version        int
			}
			entriesByCollection := make(map[colVersion][]db.OplogEntry)
			for _, entry := range entries {
				colVersion := colVersion{entry.Collection, entry.Version}
				entriesByCollection[colVersion] = append(entriesByCollection[colVersion], entry)
			}

			for colVersion, entries := range entriesByCollection {
				conn, err := cf.getOrCreateConnection(ctx, colVersion.collectionName, colVersion.version)
				if err != nil {
					log.Printf("[MainFollower] CRITICAL: Failed to get/create SQLite Conn for %s v%d: %v. Halting batch processing for this cycle.", colVersion.collectionName, colVersion.version, err)
					batchFailed = true
					break
				}

				lastId, err := cf.applyBatchEntries(conn, entries)
				if err != nil {
					log.Printf("[MainFollower] CRITICAL: Failed to apply oplogs for %s v%d: %v. Halting batch processing for this cycle.", colVersion.collectionName, colVersion.version, err)
					batchFailed = true
					break
				}

				batchMaxProcessedID = Max(batchMaxProcessedID, lastId)
				processedCount += len(entries)
			}

			if !batchFailed && batchMaxProcessedID > cf.globalLastOplogID {
				cf.globalLastOplogID = batchMaxProcessedID
				err = setLastAppliedOplogIndex(cf.metaDB, batchMaxProcessedID)
				if err != nil {
					log.Printf("[MainFollower] Error updating meta DB: %v", err)
				}
				log.Printf("[MainFollower] Applied %d entries. New global last processed oplog ID: %d", processedCount, batchMaxProcessedID)
			} else {
				log.Printf("[MainFollower] Applied %d entries, but global ID was already %d. Current batch max was %d.", processedCount, cf.globalLastOplogID, batchMaxProcessedID)
			}

			if len(entries) == cf.batchSize {
				pollingInterval = 1 * time.Millisecond
			}
		}
	}
}

func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func (cf *MainFollower) runCleanupLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(cf.cleanupInterval)
	defer ticker.Stop()

	log.Println("[MainFollower] Cleanup loop started.")

	log.Println("[MainFollower] Running initial cleanup of old SQLite files...")
	if err := cf.cleanupOldFiles(); err != nil {
		log.Printf("[MainFollower] Error during initial cleanup: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("[MainFollower] Cleanup loop stopping due to context cancellation.")
			return
		case <-ticker.C:
			log.Println("[MainFollower] Running cleanup of old SQLite files using cache...")
			if err := cf.cleanupOldFiles(); err != nil {
				log.Printf("[MainFollower] Error during cleanup: %v", err)
			}
		}
	}
}

func (cf *MainFollower) getOrCreateConnection(ctx context.Context, collectionName string, version int) (conn, error) {
	cf.connMutex.Lock()
	defer cf.connMutex.Unlock()

	dbKey := fmt.Sprintf("%s_v%d", collectionName, version)
	dbPath := GetCollectionDBPath(collectionName, cf.sqliteBaseDir, version)

	if conn, exists := cf.connections[dbKey]; exists {
		return conn, nil
	}

	replicatedColl, found := cf.collCache.GetCollectionRefresh(ctx, collectionName)
	if !found {
		log.Printf("[MainFollower] CRITICAL: Collection '%s' not found. collection might have been removed.", collectionName)
		return conn{}, fmt.Errorf("collection '%s' not found. collection might have been removed", collectionName)
	}

	log.Printf("[MainFollower] Opening connection for %s at %s", dbKey, dbPath)
	sqliteConn, err := openCollectionDB(collectionName, cf.sqliteBaseDir, dbPath, version, replicatedColl.Shape)
	if err != nil {
		return conn{}, err
	}

	sqliteConn.SetInterrupt(ctx.Done())

	reset, err := GetOrResetLocalDBVersion(sqliteConn, version)
	if err != nil {
		sqliteConn.Close()
		log.Printf("[MainFollower] Error getting/resetting internal version for %s: %v", dbPath, err)
		return conn{}, fmt.Errorf("failed to get/reset internal version for %s: %w", dbPath, err)
	}
	if reset {
		log.Printf("[MainFollower] Version mismatch in existing file, resetting to %d for %s", version, dbPath)
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

	files, err := os.ReadDir(cf.sqliteBaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read sqlite directory %s: %w", cf.sqliteBaseDir, err)
	}

	deletedCount := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sqlite") {
			continue
		}
		fileName := file.Name()
		dbKey, collNameFromFile, versionFromFile := parseDBFileName(fileName)
		if dbKey == "" {
			continue
		}

		currentVersion, collectionExistsInCache := currentVersionMap[collNameFromFile]
		stale := !collectionExistsInCache || (collectionExistsInCache && versionFromFile != currentVersion)

		if stale {
			log.Printf("[MainFollower:Cleanup] Deleting stale file: %s (Current version for '%s' in cache is %d, collection exists in cache=%t)", fileName, collNameFromFile, currentVersion, collectionExistsInCache)

			cf.connMutex.Lock()
			if conn, exists := cf.connections[dbKey]; exists {
				log.Printf("[MainFollower:Cleanup] Closing connection for %s before deleting file.", dbKey)
				conn.conn.Close()
				delete(cf.connections, dbKey)
			}
			cf.connMutex.Unlock()

			fullPath := filepath.Join(cf.sqliteBaseDir, fileName)
			if err := os.Remove(fullPath); err != nil {
				log.Printf("[MainFollower:Cleanup] Failed to delete file %s: %v", fullPath, err)
			} else {
				deletedCount++
			}
		}
	}

	if deletedCount > 0 {
		log.Printf("[MainFollower:Cleanup] Deleted %d stale SQLite files.", deletedCount)
	}

	return nil
}

func parseDBFileName(fileName string) (dbKey, collectionName string, version int) {
	if !strings.HasSuffix(fileName, ".sqlite") {
		return
	}
	baseName := strings.TrimSuffix(fileName, ".sqlite")
	parts := strings.Split(baseName, "_v")
	if len(parts) != 2 {
		return
	}
	collectionName = parts[0]
	versionStr := parts[1]
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return
	}
	dbKey = baseName
	return
}

func (cf *MainFollower) closeAllConnections() {
	cf.connMutex.Lock()
	defer cf.connMutex.Unlock()

	if len(cf.connections) == 0 {
		return
	}

	log.Printf("[MainFollower] Closing %d cached connections...", len(cf.connections))
	for key, conn := range cf.connections {
		if err := conn.conn.Close(); err != nil {
			log.Printf("[MainFollower] Error closing connection %s: %v", key, err)
		}
		delete(cf.connections, key)
	}
}
