package main

import (
	"context"
	"errors"
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
	collectionCacheManager.Start(bgTaskCtx, &wg)

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
	leaderElector := leader.NewElector(pgPool, instanceID)
	return leaderElector, instanceID
}

func startCentralFollower(ctx context.Context, wg *sync.WaitGroup, pgPool *pgxpool.Pool, cacheMgr *collectioncache.Manager, cfg *config.Config) error {
	centralFollower, err := follower.NewMainFollower(pgPool, cacheMgr, cfg.SQLiteDir, cfg)
	if err != nil {
		return fmt.Errorf("failed to create central follower: %w", err)
	}
	wg.Add(1)
	go centralFollower.Start(ctx, wg)
	return nil
}

func startCollectionCoordinator(ctx context.Context, wg *sync.WaitGroup, pgPool *pgxpool.Pool, mongoClient *mongo.Client, mongoDBName string, leaderElector *leader.LeaderElector, cacheMgr *collectioncache.Manager, cfg *config.Config) {
	slog.Info("Initializing Collection Coordinator")
	coord := coordinator.NewCoordinator(pgPool, mongoClient, mongoDBName, leaderElector, cacheMgr, cfg, wg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		coord.Start(ctx)
	}()
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
