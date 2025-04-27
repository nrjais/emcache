package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

type snapshotInfo struct {
	refCount     int
	creationTime time.Time
}

var snapshotManager = struct {
	mu        sync.Mutex
	snapshots map[string]*snapshotInfo
}{
	snapshots: make(map[string]*snapshotInfo),
}

func GetOrGenerateSnapshot(ctx context.Context, dbPath string) (snapshotPath string, cleanup func(), err error) {
	snapshotPath = dbPath + ".snapshot"

	snapshotManager.mu.Lock()

	cleanupFunc := func(info *snapshotInfo) func() {
		return func() {
			snapshotManager.mu.Lock()
			defer snapshotManager.mu.Unlock()
			if currentInfo, ok := snapshotManager.snapshots[snapshotPath]; ok && currentInfo == info {
				info.refCount--
				log.Printf("[Snapshot] Decremented ref count for %s upon download completion (Current refCount: %d)", snapshotPath, info.refCount)
			} else {
				log.Printf("[Snapshot] Snapshot %s was already cleaned up or replaced before download completion ref count decrement.", snapshotPath)
			}
		}
	}

	if info, exists := snapshotManager.snapshots[snapshotPath]; exists {
		info.refCount++
		log.Printf("[Snapshot] Reusing existing snapshot %s (refCount: %d)", snapshotPath, info.refCount)
		snapshotManager.mu.Unlock()
		return snapshotPath, cleanupFunc(info), nil
	}

	defer snapshotManager.mu.Unlock()
	log.Printf("[Snapshot] Creating new snapshot %s for %s", snapshotPath, dbPath)

	if _, statErr := os.Stat(dbPath); statErr != nil {
		if os.IsNotExist(statErr) {
			return "", nil, fmt.Errorf("source database file %s does not exist", dbPath)
		}
		return "", nil, fmt.Errorf("failed to stat source database file %s: %w", dbPath, statErr)
	}

	creationTime := time.Now()
	log.Printf("[Snapshot] Using SQLite backup API for %s", snapshotPath)

	_ = os.Remove(snapshotPath)

	srcDSN := fmt.Sprintf("file:%s?mode=ro&_journal_mode=WAL", dbPath)
	srcDB, err := sql.Open("sqlite3", srcDSN)
	if err != nil {
		return "", nil, fmt.Errorf("backup: failed to open source db %s: %w", dbPath, err)
	}
	defer srcDB.Close()
	srcDB.SetMaxOpenConns(1)

	dstDSN := fmt.Sprintf("file:%s?_journal_mode=DELETE", snapshotPath)
	dstDB, err := sql.Open("sqlite3", dstDSN)
	if err != nil {
		_ = os.Remove(snapshotPath)
		return "", nil, fmt.Errorf("backup: failed to create destination db %s: %w", snapshotPath, err)
	}
	defer dstDB.Close()
	dstDB.SetMaxOpenConns(1)

	srcConn, err := srcDB.Conn(ctx)
	if err != nil {
		_ = os.Remove(snapshotPath)
		return "", nil, fmt.Errorf("backup: failed to get source connection: %w", err)
	}
	defer srcConn.Close()

	dstConn, err := dstDB.Conn(ctx)
	if err != nil {
		_ = os.Remove(snapshotPath)
		return "", nil, fmt.Errorf("backup: failed to get destination connection: %w", err)
	}
	defer dstConn.Close()

	backupStart := time.Now()
	err = dstConn.Raw(func(dstDriverConn interface{}) error {
		dstSQLiteConn, ok := dstDriverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("destination connection is not *sqlite3.SQLiteConn (%T)", dstDriverConn)
		}

		return srcConn.Raw(func(srcDriverConn interface{}) error {
			srcSQLiteConn, ok := srcDriverConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("source connection is not *sqlite3.SQLiteConn (%T)", srcDriverConn)
			}

			log.Printf("[Snapshot] Starting backup process from %s to %s", dbPath, snapshotPath)
			backup, err := dstSQLiteConn.Backup("main", srcSQLiteConn, "main")
			if err != nil {
				return fmt.Errorf("failed to initialize backup: %w", err)
			}

			done, err := backup.Step(-1)
			if err != nil {
				_ = backup.Finish()
				return fmt.Errorf("backup step failed: %w", err)
			}
			if !done {
				_ = backup.Finish()
				return fmt.Errorf("backup step completed without finishing (done=%t)", done)
			}

			err = backup.Finish()
			if err != nil {
				return fmt.Errorf("backup finish failed: %w", err)
			}
			log.Printf("[Snapshot] Backup process completed successfully.")
			return nil
		})
	})

	if err != nil {
		_ = os.Remove(snapshotPath)
		return "", nil, fmt.Errorf("sqlite backup process failed: %w", err)
	}
	log.Printf("[Snapshot] SQLite backup created successfully in %v", time.Since(backupStart))

	info := &snapshotInfo{
		refCount:     1,
		creationTime: creationTime,
	}
	snapshotManager.snapshots[snapshotPath] = info
	log.Printf("[Snapshot] Registered snapshot %s (refCount: 1)", snapshotPath)

	return snapshotPath, cleanupFunc(info), nil
}

func StartCleanupLoop(ctx context.Context, wg *sync.WaitGroup, ttl time.Duration) {
	defer wg.Done()
	log.Printf("[Snapshot] Starting cleanup loop with TTL: %v", ttl)

	checkInterval := max(ttl/2, 1*time.Minute)
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Snapshot] Cleanup loop stopping due to context cancellation.")
			cleanupStaleSnapshots(ttl)
			return
		case <-ticker.C:
			cleanupStaleSnapshots(ttl)
		}
	}
}

func cleanupStaleSnapshots(ttl time.Duration) {
	log.Println("[Snapshot] Running periodic cleanup check...")
	now := time.Now()
	deletedCount := 0

	snapshotManager.mu.Lock()
	defer snapshotManager.mu.Unlock()

	for path, info := range snapshotManager.snapshots {
		if info.refCount <= 0 && now.Sub(info.creationTime) > ttl {
			log.Printf("[Snapshot] Deleting stale snapshot %s (created: %v, refCount: %d)", path, info.creationTime, info.refCount)
			delete(snapshotManager.snapshots, path)
			if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
				log.Printf("[Snapshot] Error deleting snapshot file %s: %v", path, rmErr)
			} else {
				deletedCount++
			}
		}
	}
	if deletedCount > 0 {
		log.Printf("[Snapshot] Deleted %d stale snapshot(s).", deletedCount)
	}
}
