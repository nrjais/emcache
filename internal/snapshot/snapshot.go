package snapshot

import (
	"context"
	"fmt"
	"log/slog"
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
				slog.Info("Decremented ref count upon download completion",
					"path", snapshotPath,
					"refCount", info.refCount)
			} else {
				slog.Info("Snapshot was already cleaned up or replaced before download completion",
					"path", snapshotPath)
			}
		}
	}

	if info, exists := snapshotManager.snapshots[snapshotPath]; exists {
		info.refCount++
		slog.Info("Reusing existing snapshot",
			"path", snapshotPath,
			"refCount", info.refCount)
		snapshotManager.mu.Unlock()
		return snapshotPath, cleanupFunc(info), nil
	}

	defer snapshotManager.mu.Unlock()
	slog.Info("Creating new snapshot", "path", snapshotPath, "source", dbPath)

	if _, statErr := os.Stat(dbPath); statErr != nil {
		if os.IsNotExist(statErr) {
			return "", nil, fmt.Errorf("source database file %s does not exist", dbPath)
		}
		return "", nil, fmt.Errorf("failed to stat source database file %s: %w", dbPath, statErr)
	}

	creationTime := time.Now()
	slog.Info("Using SQLite backup API", "path", snapshotPath)

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

	slog.Info("SQLite backup created successfully",
		"path", snapshotPath,
		"duration", time.Since(backupStart))

	info := &snapshotInfo{
		refCount:     1,
		creationTime: creationTime,
	}
	snapshotManager.snapshots[snapshotPath] = info
	slog.Info("Registered snapshot",
		"path", snapshotPath,
		"refCount", 1)

	return snapshotPath, cleanupFunc(info), nil
}

func StartCleanupLoop(ctx context.Context, wg *sync.WaitGroup, ttl time.Duration, sqliteBaseDir string) {
	defer wg.Done()
	slog.Info("Starting cleanup loop",
		"ttl", ttl)

	snapshotsDir := filepath.Join(sqliteBaseDir, snapshotDirName)
	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		slog.Warn("Failed to create snapshots directory",
			"path", snapshotsDir,
			"error", err)
	}

	checkInterval := ttl / 2
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Cleanup loop stopping due to context cancellation")
			cleanupStaleSnapshots(ttl, snapshotsDir)
			return
		case <-ticker.C:
			cleanupStaleSnapshots(ttl, snapshotsDir)
		}
	}
}

func cleanupStaleSnapshots(ttl time.Duration, snapshotsDir string) {
	slog.Info("Running periodic cleanup check")
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
		slog.Error("Error reading snapshot directory",
			"path", snapshotsDir,
			"error", err)
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
			slog.Error("Error deleting snapshot file",
				"path", path,
				"error", rmErr)
		} else {
			slog.Info("Deleted stale snapshot file",
				"path", path)
			deletedCount++
		}
	}

	if deletedCount > 0 {
		slog.Info("Deleted stale snapshots",
			"count", deletedCount)
	}
}
