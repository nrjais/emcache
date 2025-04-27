package main

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	log.Println("Starting emcache server...")

	cfg := config.Load()

	if err := migrations.RunMigrations(cfg.PostgresURL); err != nil {
		log.Fatalf("Database migration failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgPool, mongoClient, mongoDBName, err := setupDatabases(ctx, cfg)
	if err != nil {
		log.Fatalf("Database setup failed: %v", err)
	}
	defer pgPool.Close()

	defer func() {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer disconnectCancel()
		if err := mongoClient.Disconnect(disconnectCtx); err != nil {
			log.Printf("Error disconnecting from MongoDB: %v", err)
		}
	}()

	leaderElector, instanceID := setupLeaderElector(pgPool)
	defer leaderElector.ReleaseAll()
	log.Printf("Starting instance with ID: %s", instanceID)

	var wg sync.WaitGroup
	bgTaskCtx, bgTaskCancel := context.WithCancel(ctx)

	startCentralFollower(bgTaskCtx, &wg, pgPool, cfg)

	startCollectionCoordinator(bgTaskCtx, &wg, pgPool, mongoClient, mongoDBName, leaderElector, cfg)

	startSnapshotCleanup(bgTaskCtx, &wg, cfg)

	grpcServer := startGRPCServer(&wg, pgPool, cfg)

	waitForShutdownSignal()
	log.Println("Shutting down server...")

	if grpcServer != nil {
		grpcServer.GracefulStop()
	}
	log.Println("Signalling background tasks to stop...")
	bgTaskCancel()

	log.Println("Waiting for background tasks to stop...")
	wg.Wait()
	log.Println("Background tasks stopped.")

	log.Println("Server stopped gracefully.")
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
	log.Printf("Target MongoDB database: %s", mongoDBName)

	return pgPool, mongoClient, mongoDBName, nil
}

func setupLeaderElector(pgPool *pgxpool.Pool) (*leader.LeaderElector, string) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Warning: Failed to get hostname, using default instance ID: %v", err)
		hostname = "unknown-instance"
	}
	instanceID := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	leaderElector := leader.NewElector(pgPool, instanceID)
	return leaderElector, instanceID
}

func startCentralFollower(ctx context.Context, wg *sync.WaitGroup, pgPool *pgxpool.Pool, cfg *config.Config) {
	centralFollower := follower.NewCentralFollower(pgPool, cfg.SQLiteDir, cfg)
	wg.Add(1)
	go centralFollower.Start(ctx, wg)
}

func startCollectionCoordinator(ctx context.Context, wg *sync.WaitGroup, pgPool *pgxpool.Pool, mongoClient *mongo.Client, mongoDBName string, leaderElector *leader.LeaderElector, cfg *config.Config) {
	log.Println("Initializing Collection Coordinator...")
	coord := coordinator.NewCoordinator(pgPool, mongoClient, mongoDBName, leaderElector, cfg, wg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		coord.Start(ctx)
	}()
}

func startSnapshotCleanup(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config) {
	snapshotTTL := time.Duration(cfg.SnapshotOptions.TTLSecs) * time.Second
	wg.Add(1)
	go snapshot.StartCleanupLoop(ctx, wg, snapshotTTL)
}

func startGRPCServer(wg *sync.WaitGroup, pgPool *pgxpool.Pool, cfg *config.Config) *grpc.Server {
	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Printf("CRITICAL: Failed to listen on port %s: %v. gRPC server not started.", cfg.GRPCPort, err)
		return nil
	}

	grpcServerImpl := grpcapi.NewEmcacheServer(pgPool, cfg.SQLiteDir)
	s := grpc.NewServer()
	pb.RegisterEmcacheServiceServer(s, grpcServerImpl)
	reflection.Register(s)

	log.Printf("gRPC server listening on %s", cfg.GRPCPort)
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("gRPC server starting to serve...")
		if err := s.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				log.Printf("CRITICAL: Failed to serve gRPC: %v", err)
			} else {
				log.Println("gRPC server stopped gracefully.")
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
