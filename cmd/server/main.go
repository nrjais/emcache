package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/nrjais/emcache/internal/collectioncache"
	"github.com/nrjais/emcache/internal/config"
	"github.com/nrjais/emcache/internal/coordinator"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/follower"
	"github.com/nrjais/emcache/internal/grpcapi"
	"github.com/nrjais/emcache/internal/leader"
	"github.com/nrjais/emcache/internal/migrations"
	"github.com/nrjais/emcache/internal/snapshot"
	pb "github.com/nrjais/emcache/pkg/protos"
)

func main() {
	// Parse command line arguments
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to configuration file (overrides EMCACHE_CONFIG_PATH)")
	flag.Parse()

	// Set config path if provided via command line
	if configPath != "" {
		os.Setenv("EMCACHE_CONFIG_PATH", configPath)
		slog.Info("Using config file from command line", "path", configPath)
	}

	slog.Info("Starting emcache server")

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgPool, mongoClient, mongoDBName, err := setupDatabases(ctx, cfg)
	if err != nil {
		slog.Error("Database setup failed", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	if err := migrations.RunMigrations(pgPool); err != nil {
		slog.Error("Database migration failed", "error", err)
		os.Exit(1)
	}

	defer func() {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer disconnectCancel()
		if err := mongoClient.Disconnect(disconnectCtx); err != nil {
			slog.Error("Error disconnecting from MongoDB", "error", err)
		}
	}()

	leaderElector, instanceID := setupLeaderElector(pgPool)
	defer leaderElector.ReleaseAll()
	slog.Info("Starting instance", "instanceID", instanceID)

	var wg sync.WaitGroup
	bgTaskCtx, bgTaskCancel := context.WithCancel(ctx)

	collectionCacheManager := collectioncache.NewManager(pgPool, cfg)
	collectionCacheManager.Start(bgTaskCtx)

	err = startCentralFollower(bgTaskCtx, &wg, pgPool, collectionCacheManager, cfg)
	if err != nil {
		slog.Error("Failed to start central follower", "error", err)
		os.Exit(1)
	}

	startCollectionCoordinator(bgTaskCtx, &wg, pgPool, mongoClient, mongoDBName, leaderElector, collectionCacheManager, cfg)

	startSnapshotCleanup(bgTaskCtx, &wg, cfg)

	grpcServer := startGRPCServer(&wg, pgPool, cfg, collectionCacheManager)

	waitForShutdownSignal()
	slog.Info("Shutting down server")

	if grpcServer != nil {
		grpcServer.GracefulStop()
	}
	slog.Info("Signalling background tasks to stop")
	bgTaskCancel()

	slog.Info("Waiting for background tasks to stop")
	wg.Wait()
	slog.Info("Background tasks stopped")

	slog.Info("Server stopped gracefully")
}

func setupDatabases(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, *mongo.Client, string, error) {
	pgPool, err := db.ConnectPostgres(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to connect to Postgres: %w", err)
	}

	mongoClient, err := db.ConnectMongo(ctx, cfg.MongoURL)
	if err != nil {
		pgPool.Close()
		return nil, nil, "", fmt.Errorf("failed to connect to Mongo: %w", err)
	}

	cs, err := connstring.Parse(cfg.MongoURL)
	if err != nil {
		mongoClient.Disconnect(ctx)
		pgPool.Close()
		return nil, nil, "", fmt.Errorf("failed to parse Mongo URL '%s': %w", cfg.MongoURL, err)
	}
	if cs.Database == "" {
		mongoClient.Disconnect(ctx)
		pgPool.Close()
		return nil, nil, "", fmt.Errorf("mongo URL must include a database name")
	}
	mongoDBName := cs.Database
	slog.Info("Target MongoDB database", "database", mongoDBName)

	return pgPool, mongoClient, mongoDBName, nil
}

func setupLeaderElector(pgPool *pgxpool.Pool) (*leader.LeaderElector, string) {
	hostname, err := os.Hostname()
	if err != nil {
		slog.Warn("Failed to get hostname, using default instance ID", "error", err)
		hostname = "unknown-instance"
	}
	instanceID := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	leaderElector := leader.NewElector(db.NewPostgresPool(pgPool), instanceID)
	return leaderElector, instanceID
}

func startCentralFollower(ctx context.Context, wg *sync.WaitGroup, pgPool *pgxpool.Pool, cacheMgr *collectioncache.Manager, cfg *config.Config) error {
	centralFollower, err := follower.NewMainFollower(db.NewPostgresPool(pgPool), cacheMgr, cfg.SQLiteDir, cfg)
	if err != nil {
		return fmt.Errorf("failed to create central follower: %w", err)
	}
	wg.Add(1)
	go centralFollower.Start(ctx, wg)
	return nil
}

func startCollectionCoordinator(ctx context.Context, wg *sync.WaitGroup, pgPool *pgxpool.Pool, mongoClient *mongo.Client, mongoDBName string, leaderElector *leader.LeaderElector, cacheMgr *collectioncache.Manager, cfg *config.Config) {
	slog.Info("Initializing Collection Coordinator")

	// Create wrapper for leader elector to match interface
	leaderElectorWrapper := &leaderElectorWrapper{elector: leaderElector}

	// Create wrapper for manager function to match expected signature
	managerWrapper := func(
		ctx context.Context,
		wg *sync.WaitGroup,
		replicatedColl db.ReplicatedCollection,
		pgPool interface{},
		mongoClient *mongo.Client,
		mongoDBName string,
		leaderElector interface{},
		cfg *config.Config,
	) {
		// The coordinator passes db.PostgresPool and leader.LeaderElectorInterface
		// We need to convert them to concrete types for the manager function
		actualPgPool := pgPool.(db.PostgresPool)
		actualLeaderElector := leaderElector.(leader.LeaderElectorInterface)

		// Since manager.ManageCollection expects concrete types, we need to extract them
		// This is a design issue - for now, let's call the leader function directly
		// TODO: This needs to be redesigned to work with interfaces properly

		// For now, let's create a simple management routine similar to manager.ManageCollection
		// but using interfaces
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
				actualLeaderElector.ReleaseLock(ctx, collectionName)
				roleCancel()
				return

			case <-time.After(leaseDuration / 2):
				isLeaderNow := false
				var acquireErr error
				// Try to acquire leadership
				acquired, acquireErr := actualLeaderElector.TryAcquireLock(ctx, collectionName, leaseDuration)
				if acquireErr != nil {
					slog.Error("Failed to acquire leadership",
						"collection", collectionName,
						"error", acquireErr)
				} else if acquired {
					isLeaderNow = true
				}

				desiredRole := ""
				if isLeaderNow {
					desiredRole = "leader"
				} else {
					desiredRole = "follower"
				}

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
						go leader.StartChangeStreamListener(roleCtx, actualPgPool, mongoClient, mongoDBName, replicatedColl, &cfg.LeaderOptions)
					} else {
						slog.Info("Instance is now a follower", "collection", collectionName)
						roleCancel = func() {}
						roleCtx = nil
					}
				}
			}
		}
	}

	opts := coordinator.CoordinatorOptions{
		PgPool:        db.NewPostgresPool(pgPool),
		MongoClient:   mongoClient,
		MongoDBName:   mongoDBName,
		LeaderElector: leaderElectorWrapper,
		CollCache:     cacheMgr,
		Config:        cfg,
		WaitGroup:     wg,
		ManagerFunc:   managerWrapper,
	}

	coord := coordinator.NewCoordinator(opts)
	wg.Add(1)
	go func() {
		defer wg.Done()
		coord.Start(ctx)
	}()
}

// leaderElectorWrapper adapts LeaderElector to LeaderElectorInterface
type leaderElectorWrapper struct {
	elector *leader.LeaderElector
}

func (w *leaderElectorWrapper) TryAcquireLock(ctx context.Context, collectionName string, leaseDuration time.Duration) (bool, error) {
	return w.elector.TryAcquire(ctx, collectionName, leaseDuration)
}

func (w *leaderElectorWrapper) ReleaseLock(ctx context.Context, collectionName string) error {
	return w.elector.Release(collectionName)
}

func (w *leaderElectorWrapper) ReleaseAll() {
	w.elector.ReleaseAll()
}

func startSnapshotCleanup(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config) {
	snapshotTTL := time.Duration(cfg.SnapshotOptions.TTLSecs) * time.Second
	wg.Add(1)
	go snapshot.StartCleanupLoop(ctx, wg, snapshotTTL, cfg.SQLiteDir)
}

func startGRPCServer(wg *sync.WaitGroup, pgPool *pgxpool.Pool, cfg *config.Config, collectionCacheManager *collectioncache.Manager) *grpc.Server {
	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		slog.Error("Failed to listen on port, gRPC server not started",
			"port", cfg.GRPCPort,
			"error", err,
			"severity", "CRITICAL")
		return nil
	}

	grpcServerImpl := grpcapi.NewEmcacheServer(pgPool, cfg.SQLiteDir, collectionCacheManager)
	s := grpc.NewServer()
	pb.RegisterEmcacheServiceServer(s, grpcServerImpl)
	reflection.Register(s)

	slog.Info("gRPC server listening", "port", cfg.GRPCPort)
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("gRPC server starting to serve")
		if err := s.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				slog.Error("Failed to serve gRPC", "error", err, "severity", "CRITICAL")
			} else {
				slog.Info("gRPC server stopped gracefully")
			}
		}
	}()

	return s
}

func waitForShutdownSignal() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}
