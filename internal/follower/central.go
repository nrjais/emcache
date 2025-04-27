package follower

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
)

type CentralFollower struct {
	pgPool            *pgxpool.Pool
	sqliteBaseDir     string
	pollInterval      time.Duration
	cleanupInterval   time.Duration
	batchSize         int
	globalLastOplogID int64
	connections       map[string]*sql.DB
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
		connections:       make(map[string]*sql.DB),
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
	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, file := range files {
		if initCtx.Err() != nil {
			log.Println("[CentralFollower:Init] Initialization scan timed out.")
			break
		}

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

		tempDB, dbPath, err := openCollectionDB(collectionName, cf.sqliteBaseDir, version)
		if err != nil {
			log.Printf("[CentralFollower:Init] Failed to open DB %s to read metadata: %v", dbPath, err)
			continue
		}

		lastIdx, err := getLastAppliedOplogIndex(tempDB)
		tempDB.Close()

		if err != nil {
			log.Printf("[CentralFollower:Init] Failed to read last applied index from %s: %v", dbPath, err)
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

			entries, err := db.GetOplogEntriesGlobal(ctx, cf.pgPool, currentLastID, cf.batchSize)
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

				dbConn, dbPath, err := cf.getOrCreateConnection(ctx, entry.Collection, entry.Version)
				if err != nil {
					log.Printf("[CentralFollower] CRITICAL: Failed to get/create SQLite DB for %s v%d (%s): %v. Halting batch processing for this cycle.", entry.Collection, entry.Version, dbPath, err)
					batchFailed = true
					break
				}

				err = cf.applySingleEntry(ctx, dbConn, entry, dbPath)
				if err != nil {
					log.Printf("[CentralFollower] CRITICAL: Failed to apply oplog entry ID %d (%s v%d, Op: %s, DocID: %s) to %s: %v. Halting batch processing for this cycle.", entry.ID, entry.Collection, entry.Version, entry.Operation, entry.DocID, dbPath, err)
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

func (cf *CentralFollower) getOrCreateConnection(ctx context.Context, collectionName string, version int) (*sql.DB, string, error) {
	cf.connMutex.Lock()
	defer cf.connMutex.Unlock()

	dbKey := fmt.Sprintf("%s_v%d", collectionName, version)
	dbPath := GetCollectionDBPath(collectionName, cf.sqliteBaseDir, version)

	if conn, exists := cf.connections[dbKey]; exists {
		if err := conn.PingContext(ctx); err == nil {
			return conn, dbPath, nil
		}
		log.Printf("[CentralFollower] Stale connection detected for %s. Closing and reopening.", dbKey)
		conn.Close()
		delete(cf.connections, dbKey)
	}

	log.Printf("[CentralFollower] Opening connection for %s at %s", dbKey, dbPath)
	sqliteDB, actualPath, err := openCollectionDB(collectionName, cf.sqliteBaseDir, version)
	if err != nil {
		return nil, actualPath, fmt.Errorf("failed to open/create db %s: %w", actualPath, err)
	}
	dbPath = actualPath

	fileExists := false
	if _, statErr := os.Stat(dbPath); statErr == nil {
		fileExists = true
	} else if !os.IsNotExist(statErr) {
		sqliteDB.Close()
		return nil, dbPath, fmt.Errorf("error stating new db file %s: %w", dbPath, statErr)
	}

	if !fileExists {
		if err := setLocalDBVersion(sqliteDB, version); err != nil {
			sqliteDB.Close()
			return nil, dbPath, fmt.Errorf("failed to set version %d in new db %s: %w", version, dbPath, err)
		}
		log.Printf("[CentralFollower] Set internal version for new DB %s to %d", dbPath, version)
	} else {
		localVersion, found := GetLocalDBVersion(sqliteDB)
		if !found || localVersion != version {
			sqliteDB.Close()
			return nil, dbPath, fmt.Errorf("internal version mismatch in existing file %s! Expected: %d, Found: %d (found=%t)", dbPath, version, localVersion, found)
		}
	}

	cf.connections[dbKey] = sqliteDB
	return sqliteDB, dbPath, nil
}

func (cf *CentralFollower) applySingleEntry(ctx context.Context, dbConn *sql.DB, entry db.OplogEntry, dbPath string) error {
	tx, err := dbConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin sqlite transaction for entry ID %d: %w", entry.ID, err)
	}
	var commitErr error
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		} else if commitErr != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Printf("[CentralFollower] Error rolling back transaction for %s after commit error: %v (Original error: %v)", dbPath, rbErr, commitErr)
			}
		}
	}()

	lastAppliedHere, err := getLastAppliedOplogIndexTx(tx)
	if err != nil {
		commitErr = fmt.Errorf("failed to get last applied index within transaction for entry ID %d: %w", entry.ID, err)
		return commitErr
	}

	if entry.ID <= lastAppliedHere {
		return nil
	}

	if err := applyOplogEntry(tx, entry.Collection, entry); err != nil {
		commitErr = fmt.Errorf("failed to apply operation for entry ID %d: %w", entry.ID, err)
		return commitErr
	}

	if err := setLastAppliedOplogIndex(tx, entry.ID); err != nil {
		commitErr = fmt.Errorf("failed to set last applied index to %d for entry ID %d: %w", entry.ID, entry.ID, err)
		return commitErr
	}

	commitErr = tx.Commit()
	if commitErr != nil {
		return fmt.Errorf("failed to commit sqlite transaction for entry ID %d: %w", entry.ID, commitErr)
	}

	return nil
}

func (cf *CentralFollower) cleanupOldFiles(ctx context.Context) error {
	currentVersions, err := db.GetAllCurrentCollectionVersions(ctx, cf.pgPool)
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
