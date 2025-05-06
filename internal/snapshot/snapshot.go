package snapshot

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"zombiezen.com/go/sqlite"
)

const snapshotDirName = "snapshots"

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

func GetOrGenerateSnapshot(ctx context.Context, dbPath string, baseDir string, snapshotFileName string) (snapshotPath string, cleanup func(), err error) {
	snapshotPath = filepath.Join(baseDir, snapshotDirName, snapshotFileName)

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

	srcConn, err := sqlite.OpenConn(dbPath, sqlite.OpenReadOnly, sqlite.OpenWAL)
	if err != nil {
		return "", nil, fmt.Errorf("backup: failed to open source conn %s: %w", dbPath, err)
	}
	defer srcConn.Close()

	dstConn, err := sqlite.OpenConn(snapshotPath, sqlite.OpenReadWrite, sqlite.OpenCreate)
	if err != nil {
		_ = os.Remove(snapshotPath)
		return "", nil, fmt.Errorf("backup: failed to create destination conn %s: %w", snapshotPath, err)
	}
	defer dstConn.Close()

	srcConn.SetInterrupt(ctx.Done())
	dstConn.SetInterrupt(ctx.Done())

	backupStart := time.Now()

	backup, err := sqlite.NewBackup(dstConn, "main", srcConn, "main")
	if err != nil {
		_ = os.Remove(snapshotPath)
		return "", nil, fmt.Errorf("backup: failed to initialize: %w", err)
	}

	more := true
	for more {
		select {
		case <-ctx.Done():
			backup.Close()
			_ = os.Remove(snapshotPath)
			return "", nil, fmt.Errorf("backup context cancelled: %w", ctx.Err())
		default:
		}

		more, err = backup.Step(-1)
		if err != nil {
			backup.Close()
			_ = os.Remove(snapshotPath)
			return "", nil, fmt.Errorf("backup step failed: %w", err)
		}
	}

	err = backup.Close()
	if err != nil {
		_ = os.Remove(snapshotPath)
		return "", nil, fmt.Errorf("backup finish failed: %w", err)
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

func StartCleanupLoop(ctx context.Context, wg *sync.WaitGroup, ttl time.Duration, sqliteBaseDir string) {
	defer wg.Done()
	log.Printf("[Snapshot] Starting cleanup loop with TTL: %v", ttl)

	snapshotsDir := filepath.Join(sqliteBaseDir, snapshotDirName)
	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		log.Printf("[Snapshot] Warning: Failed to create snapshots directory: %v", err)
	}

	checkInterval := ttl / 2
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Snapshot] Cleanup loop stopping due to context cancellation.")
			cleanupStaleSnapshots(ttl, snapshotsDir)
			return
		case <-ticker.C:
			cleanupStaleSnapshots(ttl, snapshotsDir)
		}
	}
}

func cleanupStaleSnapshots(ttl time.Duration, snapshotsDir string) {
	log.Println("[Snapshot] Running periodic cleanup check...")
	now := time.Now()
	deletedCount := 0

	snapshotManager.mu.Lock()
	defer snapshotManager.mu.Unlock()

	toKeep := make(map[string]struct{}, len(snapshotManager.snapshots))

	for path, info := range snapshotManager.snapshots {
		if info.refCount > 0 && now.Sub(info.creationTime) < ttl {
			toKeep[path] = struct{}{}
		}
	}

	snapshotsToDelete := make([]string, 0, len(snapshotManager.snapshots))
	files, err := os.ReadDir(snapshotsDir)
	if err != nil {
		log.Printf("[Snapshot] Error reading snapshot directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".snapshot") {
			continue
		}

		fullPath := filepath.Join(snapshotsDir, file.Name())
		if _, ok := toKeep[fullPath]; ok {
			continue
		}
		snapshotsToDelete = append(snapshotsToDelete, fullPath)
	}

	for _, path := range snapshotsToDelete {
		if rmErr := os.Remove(path); rmErr != nil {
			log.Printf("[Snapshot] Error deleting snapshot file %s: %v", path, rmErr)
		} else {
			log.Printf("[Snapshot] Deleted stale snapshot file %s", path)
			deletedCount++
		}
	}

	if deletedCount > 0 {
		log.Printf("[Snapshot] Deleted %d stale snapshot(s).", deletedCount)
	}
}
