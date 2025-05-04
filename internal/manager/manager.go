package manager

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/leader"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/mongo"
)

func ManageCollection(
	ctx context.Context,
	wg *sync.WaitGroup,
	replicatedColl db.ReplicatedCollection,
	pgPool *pgxpool.Pool,
	mongoClient *mongo.Client,
	mongoDBName string,
	leaderElector *leader.LeaderElector,
	cfg *config.Config,
) {
	collectionName := replicatedColl.CollectionName
	defer wg.Done()

	var roleCtx context.Context
	var roleCancel context.CancelFunc = func() {}
	var currentRole string

	defer roleCancel()

	log.Printf("[%s] Starting management routine", collectionName)

	leaseDuration := time.Duration(cfg.LeaderOptions.LeaseDurationSecs) * time.Second

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Management context cancelled. Stopping.", collectionName)
			leaderElector.Release(collectionName)
			roleCancel()
			return

		case <-time.After(leaseDuration / 2):
			isLeaderNow := false
			var acquireErr error
			if leaderElector.IsLeader(collectionName, leaseDuration) {
				isLeaderNow = true
			} else {
				var acquired bool
				acquired, acquireErr = leaderElector.TryAcquire(ctx, collectionName, leaseDuration)
				if acquireErr != nil {
					log.Printf("[%s] Error trying to acquire leadership: %v", collectionName, acquireErr)
				} else if acquired {
					isLeaderNow = true
				}
			}

			desiredRole := lo.If(isLeaderNow, "leader").Else("follower")

			if desiredRole != currentRole {
				log.Printf("[%s] Transitioning role from '%s' to '%s'", collectionName, currentRole, desiredRole)

				roleCancel()

				currentRole = desiredRole

				if currentRole == "leader" {
					log.Printf("[%s] Starting leader change stream listener (Mongo->Postgres).", collectionName)
					roleCtx, roleCancel = context.WithCancel(ctx)
					go leader.StartChangeStreamListener(roleCtx, pgPool, mongoClient, mongoDBName, replicatedColl, &cfg.LeaderOptions)
				} else {
					log.Printf("[%s] Instance is a follower.", collectionName)
					roleCancel = func() {}
					roleCtx = nil
				}
			}
		}
	}
}
