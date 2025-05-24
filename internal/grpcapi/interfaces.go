package grpcapi

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/collectioncache"
	pb "github.com/nrjais/emcache/pkg/protos"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// EmcacheServerInterface defines the interface for the EmCache gRPC server
type EmcacheServerInterface interface {
	pb.EmcacheServiceServer
}

// ServerFactory creates EmCache server instances
type ServerFactory interface {
	NewEmcacheServer(pgPool *pgxpool.Pool, sqliteBaseDir string, collCache *collectioncache.Manager) pb.EmcacheServiceServer
}
