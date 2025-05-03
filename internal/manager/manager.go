package manager

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/leader"
	"github.com/nrjais/emcache/internal/shape"
	"go.mongodb.org/mongo-driver/mongo"
)

func ManageCollection(
	ctx context.Context,
	wg *sync.WaitGroup,
	collectionName string,
	collShape shape.Shape,
	pgPool *pgxpool.Pool,
	mongoClient *mongo.Client,
	mongoDBName string,
	leaderElector *leader.LeaderElector,
	cfg *config.Config,
) {
	defer wg.Done()

	var roleCtx context.Context
	var roleCancel context.CancelFunc = func() {}
	var currentRole string

	defer roleCancel()

	log.Printf("[%s] Starting management routine", collectionName)

	const checkInterval = 5 * time.Second
	timer := time.NewTimer(checkInterval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Management context cancelled. Stopping.", collectionName)
			timer.Stop()
			return

		case <-timer.C:
			isLeaderNow := false
			var acquireErr error
			if leaderElector.IsLeader(collectionName) {
				isLeaderNow = true
			} else {
				var acquired bool
				acquired, acquireErr = leaderElector.TryAcquire(ctx, collectionName)
				if acquireErr != nil {
					log.Printf("[%s] Error trying to acquire leadership: %v", collectionName, acquireErr)
				} else if acquired {
					isLeaderNow = true
				}
			}

			desiredRole := "follower"
			if isLeaderNow {
				desiredRole = "leader"
			}

			if currentRole == "leader" && roleCtx != nil && roleCtx.Err() != nil {
				log.Printf("[%s] Detected ended context for leader role (Error: %v). Forcing role re-evaluation.", collectionName, roleCtx.Err())
				roleCancel()
				roleCancel = func() {}
				currentRole = ""
				roleCtx = nil
			}

			if desiredRole != currentRole {
				log.Printf("[%s] Transitioning role from '%s' to '%s'", collectionName, currentRole, desiredRole)

				roleCancel()

				currentRole = desiredRole

				if currentRole == "leader" {
					log.Printf("[%s] Starting leader change stream listener (Mongo->Postgres).", collectionName)
					roleCtx, roleCancel = context.WithCancel(ctx)
					go leader.StartChangeStreamListener(roleCtx, pgPool, mongoClient, mongoDBName, collectionName, collShape, &cfg.LeaderOptions)
				} else {
					log.Printf("[%s] Instance is a follower.", collectionName)
					roleCancel = func() {}
					roleCtx = nil
				}
			}

			timer.Reset(checkInterval)
		}
	}
}
