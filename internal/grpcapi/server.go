package grpcapi

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/follower"
	"github.com/nrjais/emcache/internal/snapshot"
	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type server struct {
	pb.UnimplementedEmcacheServiceServer
	pgPool    *pgxpool.Pool
	sqliteDir string
}

func NewEmcacheServer(pgPool *pgxpool.Pool, sqliteBaseDir string) pb.EmcacheServiceServer {
	return &server{
		pgPool:    pgPool,
		sqliteDir: sqliteBaseDir,
	}
}
func (s *server) DownloadDb(req *pb.DownloadDbRequest, stream pb.EmcacheService_DownloadDbServer) error {
	collectionName := req.GetCollectionName()
	if collectionName == "" {
		return status.Error(codes.InvalidArgument, "Collection name cannot be empty")
	}
	log.Printf("gRPC DownloadDb request for collection: %s", collectionName)

	ctx := stream.Context()

	currentVersion, err := db.GetCurrentCollectionVersion(ctx, s.pgPool, collectionName)
	if err != nil {
		log.Printf("Error getting current version for %s: %v", collectionName, err)
		if err.Error() == fmt.Sprintf("collection '%s' not found in replicated_collections", collectionName) {
			return status.Errorf(codes.NotFound, "Collection %s not configured for replication", collectionName)
		}
		return status.Error(codes.Internal, "Failed to get current collection version")
	}

	dbPath := follower.GetCollectionDBPath(collectionName, s.sqliteDir, currentVersion)

	snapshotPath, cleanupSnapshot, err := snapshot.GetOrGenerateSnapshot(ctx, dbPath)
	if err != nil {
		log.Printf("Error preparing snapshot for %s (v%d): %v", collectionName, currentVersion, err)
		return status.Errorf(codes.Internal, "Failed to prepare database snapshot for download")
	}
	defer cleanupSnapshot()

	log.Printf("Streaming snapshot version %d for collection '%s' from %s", currentVersion, collectionName, snapshotPath)
	file, err := os.Open(snapshotPath)
	if err != nil {
		log.Printf("Error opening snapshot file %s for streaming: %v", snapshotPath, err)
		return status.Errorf(codes.Internal, "Failed to open database snapshot for streaming")
	}
	defer file.Close()

	if err := stream.Send(&pb.DownloadDbResponse{Version: int32(currentVersion)}); err != nil {
		log.Printf("Error sending DB version for collection %s: %v", collectionName, err)
		return status.Errorf(codes.Internal, "Failed to send database version")
	}

	buffer := make([]byte, 1024*1024)
	for {
		n, err := file.Read(buffer)
		if n > 0 {
			if err := stream.Send(&pb.DownloadDbResponse{Chunk: buffer[:n]}); err != nil {
				log.Printf("Error sending DB chunk for collection %s: %v", collectionName, err)
				return status.Errorf(codes.Internal, "Failed to stream database chunk")
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading from snapshot file %s: %v", snapshotPath, err)
			return status.Errorf(codes.Internal, "Failed to read database snapshot")
		}
	}

	log.Printf("Finished streaming DB for collection: %s", collectionName)
	return nil
}

func (s *server) GetOplogEntries(ctx context.Context, req *pb.GetOplogEntriesRequest) (*pb.GetOplogEntriesResponse, error) {
	collectionNames := req.GetCollectionNames()
	afterIndex := req.GetAfterIndex()
	limit := req.GetLimit()

	if len(collectionNames) == 0 {
		return nil, status.Error(codes.InvalidArgument, "At least one collection name must be provided")
	}
	if afterIndex < 0 {
		return nil, status.Error(codes.InvalidArgument, "after_index cannot be negative")
	}
	if limit <= 0 {
		limit = 100
		log.Printf("gRPC GetOplogEntries: Invalid or missing limit, defaulting to %d", limit)
	} else if limit > 1000 {
		limit = 1000
		log.Printf("gRPC GetOplogEntries: Requested limit %d exceeds max limit, capping at %d", req.GetLimit(), limit)
	}

	log.Printf("gRPC GetOplogEntries request for collections %v after index %d with limit %d", collectionNames, afterIndex, limit)

	entries, err := db.GetOplogEntriesMultipleCollections(ctx, s.pgPool, collectionNames, afterIndex, int(limit))
	if err != nil {
		log.Printf("Error fetching oplog entries for collections %v after index %d: %v", collectionNames, afterIndex, err)
		return nil, status.Error(codes.Internal, "Failed to fetch oplog entries")
	}

	pbEntries := make([]*pb.OplogEntry, 0, len(entries))
	for _, entry := range entries {
		pbEntry, err := convertDbOplogToProto(entry)
		if err != nil {
			log.Printf("Error converting oplog entry %d (v%d) for %s to proto: %v", entry.ID, entry.Version, entry.Collection, err)
			return nil, status.Error(codes.Internal, "Failed to convert oplog entry")
		}
		pbEntries = append(pbEntries, pbEntry)
	}

	log.Printf("Returning %d oplog entries for collections %v", len(pbEntries), collectionNames)

	return &pb.GetOplogEntriesResponse{Entries: pbEntries}, nil
}

func (s *server) AddCollection(ctx context.Context, req *pb.AddCollectionRequest) (*pb.AddCollectionResponse, error) {
	collectionName := req.GetCollectionName()
	if collectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "Collection name cannot be empty")
	}

	log.Printf("gRPC AddCollection request for collection: %s", collectionName)

	if err := db.AddReplicatedCollection(ctx, s.pgPool, collectionName); err != nil {
		log.Printf("Error adding replicated collection '%s' to database: %v", collectionName, err)
		return nil, status.Error(codes.Internal, "Failed to add collection to replication list")
	}

	return &pb.AddCollectionResponse{}, nil
}

func convertDbOplogToProto(entry db.OplogEntry) (*pb.OplogEntry, error) {
	pbOpType := pb.OplogEntry_OPERATION_TYPE_UNSPECIFIED
	switch entry.Operation {
	case "UPSERT":
		pbOpType = pb.OplogEntry_UPSERT
	case "DELETE":
		pbOpType = pb.OplogEntry_DELETE
	}

	pbTimestamp := timestamppb.New(entry.CreatedAt)

	pbData := &structpb.Struct{}
	if len(entry.Doc) > 0 {
		if err := pbData.UnmarshalJSON(entry.Doc); err != nil {
			return nil, fmt.Errorf("failed to unmarshal oplog JSON data to Struct: %w", err)
		}
	} else {
		pbData = nil
	}

	return &pb.OplogEntry{
		Index:      entry.ID,
		Operation:  pbOpType,
		Id:         entry.DocID,
		Timestamp:  pbTimestamp,
		Collection: entry.Collection,
		Data:       pbData,
		Version:    int32(entry.Version),
	}, nil
}
