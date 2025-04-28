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
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type CentralFollower struct {
	pgPool            *pgxpool.Pool
	sqliteBaseDir     string
	pollInterval      time.Duration
	cleanupInterval   time.Duration
	batchSize         int
	globalLastOplogID int64
	connections       map[string]*sqlite.Conn
	connMutex         sync.Mutex
}

func NewCentralFollower(pgPool *pgxpool.Pool, sqliteBaseDir string, cfg *config.Config) *CentralFollower {
	pollInterval := time.Duration(cfg.FollowerOptions.PollIntervalSecs) * time.Second
	batchSize := cfg.FollowerOptions.BatchSize

	cleanupInterval := time.Duration(cfg.FollowerOptions.CleanupIntervalSecs) * time.Second

	cf := &CentralFollower{
		pgPool:            pgPool,
		sqliteBaseDir:     sqliteBaseDir,
		pollInterval:      pollInterval,
		cleanupInterval:   cleanupInterval,
		batchSize:         batchSize,
		globalLastOplogID: 0,
		connections:       make(map[string]*sqlite.Conn),
	}
	cf.initializeGlobalLastOplogID()

	return cf
}

func (cf *CentralFollower) initializeGlobalLastOplogID() {
	log.Println("[CentralFollower] Initializing global last processed oplog ID...")
	files, err := os.ReadDir(cf.sqliteBaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[CentralFollower] SQLite directory %s does not exist. Starting from oplog ID 0.", cf.sqliteBaseDir)
			cf.globalLastOplogID = 0
			return
		}
		log.Printf("[CentralFollower] Warning: Failed to read sqlite directory %s during init: %v. Starting from oplog ID 0.", cf.sqliteBaseDir, err)
		cf.globalLastOplogID = 0
		return
	}

	var maxID int64 = 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sqlite") {
			continue
		}
		baseName := strings.TrimSuffix(file.Name(), ".sqlite")
		parts := strings.Split(baseName, "_v")
		if len(parts) != 2 {
			continue
		}
		collectionName := parts[0]
		version, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		tempConn, err := openCollectionDB(collectionName, cf.sqliteBaseDir, version)
		if err != nil {
			log.Printf("[CentralFollower:Init] Failed to open Conn %s to read metadata: %v", collectionName, err)
			continue
		}

		lastIdx, err := getLastAppliedOplogIndex(tempConn)
		tempConn.Close()

		if err != nil {
			log.Printf("[CentralFollower:Init] Failed to read last applied index from %s: %v", collectionName, err)
			continue
		}

		if lastIdx > maxID {
			maxID = lastIdx
		}
	}

	cf.globalLastOplogID = maxID
	log.Printf("[CentralFollower] Initialized global last processed oplog ID to %d", cf.globalLastOplogID)
}

func (cf *CentralFollower) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("[CentralFollower] Starting...")

	var loopWg sync.WaitGroup
	loopWg.Add(2)

	go cf.runMainLoop(ctx, &loopWg)
	go cf.runCleanupLoop(ctx, &loopWg)

	loopWg.Wait()
	log.Println("[CentralFollower] Stopped.")
	cf.closeAllConnections()
}

func (cf *CentralFollower) runMainLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("[CentralFollower] Main loop started.")

	pollingInterval := cf.pollInterval
	for {
		timer := time.NewTimer(pollingInterval)
		pollingInterval = cf.pollInterval
		select {
		case <-ctx.Done():
			log.Println("[CentralFollower] Main loop stopping due to context cancellation.")
			timer.Stop()
			return
		case <-timer.C:
			currentLastID := cf.globalLastOplogID

			pgCtx, pgCancel := context.WithTimeout(ctx, 15*time.Second)
			entries, err := db.GetOplogEntriesGlobal(pgCtx, cf.pgPool, currentLastID, cf.batchSize)
			pgCancel()

			if err != nil {
				log.Printf("[CentralFollower] Error fetching global oplog entries after ID %d: %v", currentLastID, err)
				continue
			}

			if len(entries) == 0 {
				continue
			}

			log.Printf("[CentralFollower] Fetched %d new oplog entries (after ID %d).", len(entries), currentLastID)

			batchMaxProcessedID := currentLastID
			processedCount := 0
			batchFailed := false

			for _, entry := range entries {
				if entry.ID <= currentLastID {
					log.Printf("[CentralFollower] Warning: Encountered out-of-order or duplicate global oplog ID %d (expected > %d). Skipping.", entry.ID, currentLastID)
					continue
				}

				sqliteConn, err := cf.getOrCreateConnection(ctx, entry.Collection, entry.Version)
				if err != nil {
					log.Printf("[CentralFollower] CRITICAL: Failed to get/create SQLite Conn for %s v%d: %v. Halting batch processing for this cycle.", entry.Collection, entry.Version, err)
					batchFailed = true
					break
				}

				err = cf.applySingleEntry(sqliteConn, entry)
				if err != nil {
					log.Printf("[CentralFollower] CRITICAL: Failed to apply oplog entry ID %d to %s: %v. Halting batch processing for this cycle.", entry.ID, entry.Collection, err)
					batchFailed = true
					break
				}

				batchMaxProcessedID = entry.ID
				processedCount++
			}

			if !batchFailed && batchMaxProcessedID > currentLastID {
				if batchMaxProcessedID > cf.globalLastOplogID {
					cf.globalLastOplogID = batchMaxProcessedID
					log.Printf("[CentralFollower] Applied %d entries. New global last processed oplog ID: %d", processedCount, batchMaxProcessedID)
				} else {
					log.Printf("[CentralFollower] Applied %d entries, but global ID was already %d. Current batch max was %d.", processedCount, cf.globalLastOplogID, batchMaxProcessedID)
				}
			}

			if len(entries) == cf.batchSize {
				pollingInterval = 1 * time.Millisecond
			}
		}
	}
}

func (cf *CentralFollower) runCleanupLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(cf.cleanupInterval)
	defer ticker.Stop()

	log.Println("[CentralFollower] Cleanup loop started.")

	log.Println("[CentralFollower] Running initial cleanup of old SQLite files...")
	if err := cf.cleanupOldFiles(ctx); err != nil {
		log.Printf("[CentralFollower] Error during initial cleanup: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("[CentralFollower] Cleanup loop stopping due to context cancellation.")
			return
		case <-ticker.C:
			log.Println("[CentralFollower] Running cleanup of old SQLite files...")
			if err := cf.cleanupOldFiles(ctx); err != nil {
				log.Printf("[CentralFollower] Error during cleanup: %v", err)
			}
		}
	}
}

func (cf *CentralFollower) getOrCreateConnection(ctx context.Context, collectionName string, version int) (*sqlite.Conn, error) {
	cf.connMutex.Lock()
	defer cf.connMutex.Unlock()

	dbKey := fmt.Sprintf("%s_v%d", collectionName, version)
	dbPath := GetCollectionDBPath(collectionName, cf.sqliteBaseDir, version)

	if conn, exists := cf.connections[dbKey]; exists {
		err := sqlitex.Execute(conn, "SELECT 1;", nil)
		if err == nil {
			return conn, nil
		}
		log.Printf("[CentralFollower] Stale connection detected for %s (err: %v). Closing and reopening.", dbKey, err)
		conn.Close()
		delete(cf.connections, dbKey)
	}

	log.Printf("[CentralFollower] Opening connection for %s at %s", dbKey, dbPath)
	sqliteConn, err := openCollectionDB(collectionName, cf.sqliteBaseDir, version)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create conn %s: %w", dbPath, err)
	}

	sqliteConn.SetInterrupt(ctx.Done())

	reset, err := GetOrResetLocalDBVersion(sqliteConn, version)
	if err != nil {
		sqliteConn.Close()
		log.Printf("[CentralFollower] Error getting/resetting internal version for %s: %v", dbPath, err)
		return nil, fmt.Errorf("failed to get/reset internal version for %s: %w", dbPath, err)
	}
	if reset {
		log.Printf("[CentralFollower] Version mismatch in existing file, resetting to %d for %s", version, dbPath)
	}

	cf.connections[dbKey] = sqliteConn
	return sqliteConn, nil
}

func (cf *CentralFollower) applySingleEntry(conn *sqlite.Conn, entry db.OplogEntry) (err error) {
	defer sqlitex.Save(conn)(&err)

	if err = applyOplogEntry(conn, entry); err != nil {
		return fmt.Errorf("failed to apply operation for entry ID %d: %w", entry.ID, err)
	}

	if err = setLastAppliedOplogIndex(conn, entry.ID); err != nil {
		return fmt.Errorf("failed to set last applied index to %d for entry ID %d: %w", entry.ID, entry.ID, err)
	}

	return nil
}

func (cf *CentralFollower) cleanupOldFiles(ctx context.Context) error {
	pgCtx, pgCancel := context.WithTimeout(ctx, 30*time.Second)
	currentVersions, err := db.GetAllCurrentCollectionVersions(pgCtx, cf.pgPool)
	pgCancel()
	if err != nil {
		return fmt.Errorf("failed to get current collection versions from postgres: %w", err)
	}

	currentVersionMap := make(map[string]int)
	for _, cv := range currentVersions {
		currentVersionMap[cv.CollectionName] = cv.Version
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
		dbKey, collectionName, version := parseDBFileName(fileName)
		if dbKey == "" {
			continue
		}

		currentVersion, collectionExists := currentVersionMap[collectionName]
		stale := !collectionExists || (collectionExists && version != currentVersion)

		if stale {
			log.Printf("[CentralFollower:Cleanup] Deleting stale file: %s (Current version for '%s' is %d, collection exists=%t)", fileName, collectionName, currentVersion, collectionExists)

			cf.connMutex.Lock()
			if conn, exists := cf.connections[dbKey]; exists {
				log.Printf("[CentralFollower:Cleanup] Closing connection for %s before deleting file.", dbKey)
				conn.Close()
				delete(cf.connections, dbKey)
			}
			cf.connMutex.Unlock()

			fullPath := filepath.Join(cf.sqliteBaseDir, fileName)
			if err := os.Remove(fullPath); err != nil {
				log.Printf("[CentralFollower:Cleanup] Failed to delete file %s: %v", fullPath, err)
			} else {
				deletedCount++
			}
		}
	}

	if deletedCount > 0 {
		log.Printf("[CentralFollower:Cleanup] Deleted %d stale SQLite files.", deletedCount)
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

func (cf *CentralFollower) closeAllConnections() {
	cf.connMutex.Lock()
	defer cf.connMutex.Unlock()

	if len(cf.connections) == 0 {
		return
	}

	log.Printf("[CentralFollower] Closing %d cached connections...", len(cf.connections))
	for key, conn := range cf.connections {
		if err := conn.Close(); err != nil {
			log.Printf("[CentralFollower] Error closing connection %s: %v", key, err)
		}
		delete(cf.connections, key)
	}
}
