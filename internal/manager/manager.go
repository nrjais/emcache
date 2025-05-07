package manager

import (
	"context"
	"log/slog"
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

	slog.Info("Starting management routine", "collection", collectionName)

	leaseDuration := time.Duration(cfg.LeaderOptions.LeaseDurationSecs) * time.Second

	for {
		select {
		case <-ctx.Done():
			slog.Info("Management context cancelled, stopping", "collection", collectionName)
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
					slog.Error("Failed to acquire leadership",
						"collection", collectionName,
						"error", acquireErr)
				} else if acquired {
					isLeaderNow = true
				}
			}

			desiredRole := lo.If(isLeaderNow, "leader").Else("follower")

			if desiredRole != currentRole {
				slog.Info("Transitioning role",
					"collection", collectionName,
					"from", currentRole,
					"to", desiredRole)

				roleCancel()

				currentRole = desiredRole

				if currentRole == "leader" {
					slog.Info("Starting change stream listener",
						"collection", collectionName,
						"direction", "Mongo->Postgres")
					roleCtx, roleCancel = context.WithCancel(ctx)
					go leader.StartChangeStreamListener(roleCtx, pgPool, mongoClient, mongoDBName, replicatedColl, &cfg.LeaderOptions)
				} else {
					slog.Info("Instance is now a follower", "collection", collectionName)
					roleCancel = func() {}
					roleCtx = nil
				}
			}
		}
	}
}
