package client

import (
	"context"
	"database/sql"

	pb "github.com/nrjais/emcache/pkg/protos"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mock_interfaces.go -package=mocks

// ClientInterface defines the interface for the EmCache client
type ClientInterface interface {
	Query(ctx context.Context, collectionName string, query string, args ...any) (*sql.Rows, error)
	SyncToLatest(ctx context.Context, limit int) error
	AddCollection(ctx context.Context, collectionName string, shape *pb.Shape) (*pb.AddCollectionResponse, error)
	RemoveCollection(ctx context.Context, collectionName string) (*pb.RemoveCollectionResponse, error)
	StartDbUpdates() error
	StopUpdates()
	Close() error
}
