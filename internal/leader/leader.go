package leader

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
)

type LeaderElector struct {
	pool       *pgxpool.Pool
	heldLocks  map[string]*pgxpool.Conn
	mu         sync.Mutex
	instanceID string
}

func NewElector(pool *pgxpool.Pool, instanceID string) *LeaderElector {
	return &LeaderElector{
		pool:       pool,
		heldLocks:  make(map[string]*pgxpool.Conn),
		instanceID: instanceID,
	}
}

func collectionLockID(collectionName string) int64 {
	h := sha1.New()
	h.Write([]byte(collectionName))
	bs := h.Sum(nil)
	return int64(binary.BigEndian.Uint64(bs[len(bs)-8:]))
}

func (e *LeaderElector) TryAcquire(ctx context.Context, collectionName string) (bool, error) {
	lockID := collectionLockID(collectionName)

	e.mu.Lock()
	conn, exists := e.heldLocks[collectionName]
	if exists {
		if err := conn.Ping(ctx); err == nil {
			e.mu.Unlock()
			return true, nil
		}
		log.Printf("[%s] Connection holding lock lost. Releasing locally.", collectionName)
		delete(e.heldLocks, collectionName)
		conn.Release()
	}
	e.mu.Unlock()

	acquireCtx, acquireCancel := context.WithTimeout(ctx, 5*time.Second)
	defer acquireCancel()

	newConn, err := e.pool.Acquire(acquireCtx)
	if err != nil {
		return false, fmt.Errorf("failed to acquire connection for lock attempt: %w", err)
	}

	var acquired bool
	err = newConn.QueryRow(acquireCtx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
	if err != nil {
		newConn.Release()
		return false, fmt.Errorf("failed to attempt advisory lock %d: %w", lockID, err)
	}

	if !acquired {
		newConn.Release()
		return false, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.heldLocks[collectionName]; exists {
		log.Printf("[%s] Lock acquired concurrently? Releasing duplicate attempt.", collectionName)
		go func() {
			_, releaseErr := newConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockID)
			newConn.Release()
			if releaseErr != nil {
				log.Printf("[%s] Error releasing duplicate lock %d: %v", collectionName, lockID, releaseErr)
			}
		}()
		return true, nil
	}
	e.heldLocks[collectionName] = newConn
	log.Printf("[%s] Acquired leadership lock (ID: %d)", collectionName, lockID)

	return true, nil
}

func (e *LeaderElector) Release(collectionName string) error {
	lockID := collectionLockID(collectionName)

	e.mu.Lock()
	conn, exists := e.heldLocks[collectionName]
	if !exists {
		e.mu.Unlock()
		return nil
	}
	delete(e.heldLocks, collectionName)
	e.mu.Unlock()

	_, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockID)
	if err != nil {
		log.Printf("[%s] Error releasing advisory lock %d: %v", collectionName, lockID, err)
	}

	conn.Release()
	log.Printf("[%s] Released leadership lock (ID: %d)", collectionName, lockID)
	return err
}

func (e *LeaderElector) ReleaseAll() {
	e.mu.Lock()
	collectionsToRelease := make([]string, 0, len(e.heldLocks))
	for k := range e.heldLocks {
		collectionsToRelease = append(collectionsToRelease, k)
	}
	e.mu.Unlock()

	log.Printf("Releasing all held locks (%d)...", len(collectionsToRelease))
	for _, coll := range collectionsToRelease {
		if err := e.Release(coll); err != nil {
			log.Printf("Error during ReleaseAll for collection %s: %v", coll, err)
		}
	}
	log.Println("Finished releasing all locks.")
}

func (e *LeaderElector) IsLeader(collectionName string) bool {
	e.mu.Lock()
	conn, exists := e.heldLocks[collectionName]
	e.mu.Unlock()

	if !exists {
		return false
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pingCancel()
	if err := conn.Ping(pingCtx); err == nil {
		return true
	}

	log.Printf("[%s] IsLeader check failed: Connection ping failed. Releasing lock locally.", collectionName)
	e.mu.Lock()
	if storedConn, ok := e.heldLocks[collectionName]; ok && storedConn == conn {
		delete(e.heldLocks, collectionName)
		conn.Release()
	}
	e.mu.Unlock()
	return false
}
