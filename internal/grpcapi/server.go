package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nrjais/emcache/internal/db"
	"github.com/nrjais/emcache/internal/follower"
	"github.com/nrjais/emcache/internal/shape"
	"github.com/nrjais/emcache/internal/snapshot"
	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var validate *validator.Validate

func init() {
	validate = validator.New(validator.WithRequiredStructEnabled())
}

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

	if err := stream.Send(&pb.DownloadDbResponse{Version: int32(currentVersion), Compression: pb.Compression_NONE}); err != nil {
		log.Printf("Error sending DB version for collection %s: %v", collectionName, err)
		return status.Errorf(codes.Internal, "Failed to send database version")
	}

	buffer := make([]byte, 1024*1024*5)
	for {
		n, err := file.Read(buffer)
		if n > 0 {
			data := &pb.DownloadDbResponse{Chunk: buffer[:n]}
			if err := stream.Send(data); err != nil {
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

// GetCollections returns a list of all collections currently configured for replication.
func (s *server) GetCollections(ctx context.Context, req *pb.GetCollectionsRequest) (*pb.GetCollectionsResponse, error) {
	log.Printf("gRPC GetCollections request")

	internalCollections, err := db.GetAllReplicatedCollectionsWithShapes(ctx, s.pgPool)
	if err != nil {
		log.Printf("Error fetching all replicated collections with shapes: %v", err)
		return nil, status.Error(codes.Internal, "Failed to fetch collection list")
	}

	pbCollections := make([]*pb.Collection, 0, len(internalCollections))
	for _, coll := range internalCollections {
		pbColl := &pb.Collection{
			Name:    coll.CollectionName,
			Version: int32(coll.CurrentVersion),
			Shape:   convertInternalShapeToProto(coll.Shape),
		}
		pbCollections = append(pbCollections, pbColl)
	}

	log.Printf("Returning %d collections", len(pbCollections))

	return &pb.GetCollectionsResponse{Collections: pbCollections}, nil
}

func (s *server) AddCollection(ctx context.Context, req *pb.AddCollectionRequest) (*pb.AddCollectionResponse, error) {
	collectionName := req.GetCollectionName()
	if collectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "Collection name cannot be empty")
	}
	protoShape := req.GetShape()
	if protoShape == nil {
		return nil, status.Error(codes.InvalidArgument, "Shape definition cannot be empty")
	}

	log.Printf("gRPC AddCollection request for collection: %s", collectionName)

	collShape, err := convertProtoShapeToInternal(protoShape)
	if err != nil {
		log.Printf("Error converting proto shape for collection '%s': %v", collectionName, err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid shape structure: %v", err)
	}

	if err := validate.Struct(collShape); err != nil {
		log.Printf("Internal shape validation failed for collection '%s': %v", collectionName, err)
		var validationErrors validator.ValidationErrors
		if errors.As(err, &validationErrors) {
			var errMsgs []string
			for _, fe := range validationErrors {
				errMsgs = append(errMsgs, fmt.Sprintf("field '%s' failed validation '%s'", fe.Namespace(), fe.Tag()))
			}
			return nil, status.Errorf(codes.InvalidArgument, "Invalid shape definition: %s", strings.Join(errMsgs, "; "))
		}
		return nil, status.Errorf(codes.InvalidArgument, "Invalid shape definition: %v", err)
	}

	for _, col := range collShape.Columns {
		if strings.ToLower(col.Name) == "id" {
			return nil, status.Errorf(codes.InvalidArgument, "Column name 'id' is reserved and cannot be explicitly defined in the shape")
		}
	}

	shapeBytes, err := json.Marshal(collShape)
	if err != nil {
		log.Printf("CRITICAL: Failed to marshal internal shape to JSON for collection '%s': %v", collectionName, err)
		return nil, status.Error(codes.Internal, "Failed to process shape definition")
	}

	if err := db.AddReplicatedCollection(ctx, s.pgPool, collectionName, shapeBytes); err != nil {
		if errors.Is(err, db.ErrCollectionAlreadyExists) {
			log.Printf("Attempt to add collection '%s' failed: already exists.", collectionName)
			return nil, status.Errorf(codes.AlreadyExists, "Collection '%s' is already configured for replication", collectionName)
		}

		log.Printf("Error adding replicated collection '%s' to database: %v", collectionName, err)
		return nil, status.Error(codes.Internal, "Failed to add collection to replication list")
	}

	log.Printf("Successfully added collection '%s' for replication.", collectionName)
	return &pb.AddCollectionResponse{}, nil
}

func (s *server) RemoveCollection(ctx context.Context, req *pb.RemoveCollectionRequest) (*pb.RemoveCollectionResponse, error) {
	collectionName := req.GetCollectionName()
	if collectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "Collection name cannot be empty")
	}

	log.Printf("gRPC RemoveCollection request for collection: %s", collectionName)

	if err := db.RemoveReplicatedCollection(ctx, s.pgPool, collectionName); err != nil {
		if strings.Contains(err.Error(), "not found") {
			log.Printf("Collection '%s' not found for removal.", collectionName)
			return nil, status.Errorf(codes.NotFound, "Collection %s not found for removal", collectionName)
		}
		log.Printf("Error removing replicated collection '%s' from database: %v", collectionName, err)
		return nil, status.Error(codes.Internal, "Failed to remove collection from replication list")
	}

	log.Printf("Successfully removed collection '%s' from replication.", collectionName)
	return &pb.RemoveCollectionResponse{}, nil
}

func convertDbOplogToProto(entry db.OplogEntry) (*pb.OplogEntry, error) {
	pbOpType := pb.OplogEntry_UPSERT
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

func convertProtoDataType(dt pb.DataType) shape.DataType {
	switch dt {
	case pb.DataType_JSONB:
		return shape.JSONB
	case pb.DataType_ANY:
		return shape.Any
	case pb.DataType_BOOL:
		return shape.Bool
	case pb.DataType_NUMBER:
		return shape.Number
	case pb.DataType_INTEGER:
		return shape.Integer
	case pb.DataType_TEXT:
		return shape.Text
	default:
		return shape.Any
	}
}

func convertProtoColumn(pc *pb.Column) shape.Column {
	if pc == nil {
		return shape.Column{}
	}
	return shape.Column{
		Name: pc.GetName(),
		Type: convertProtoDataType(pc.GetType()),
		Path: pc.GetPath(),
	}
}

func convertProtoIndex(pi *pb.Index) shape.Index {
	if pi == nil {
		return shape.Index{}
	}
	cols := make([]string, len(pi.GetColumns()))
	copy(cols, pi.GetColumns())
	return shape.Index{
		Columns: cols,
	}
}

func convertProtoShapeToInternal(ps *pb.Shape) (shape.Shape, error) {
	if ps == nil {
		return shape.Shape{}, fmt.Errorf("protobuf shape cannot be nil")
	}

	internalShape := shape.Shape{
		Columns: make([]shape.Column, 0, len(ps.GetColumns())),
		Indexes: make([]shape.Index, 0, len(ps.GetIndexes())),
	}

	for _, pc := range ps.GetColumns() {
		if pc == nil {
			continue
		}
		internalShape.Columns = append(internalShape.Columns, convertProtoColumn(pc))
	}

	for _, pi := range ps.GetIndexes() {
		if pi == nil {
			continue
		}
		internalShape.Indexes = append(internalShape.Indexes, convertProtoIndex(pi))
	}

	if len(internalShape.Columns) == 0 {
		return shape.Shape{}, fmt.Errorf("shape must have at least one data column defined")
	}

	return internalShape, nil
}

func convertInternalDataTypeToProto(dt shape.DataType) pb.DataType {
	switch dt {
	case shape.JSONB:
		return pb.DataType_JSONB
	case shape.Bool:
		return pb.DataType_BOOL
	case shape.Number:
		return pb.DataType_NUMBER
	case shape.Integer:
		return pb.DataType_INTEGER
	case shape.Text:
		return pb.DataType_TEXT
	case shape.Any:
		fallthrough // Treat Any as Any
	default:
		return pb.DataType_ANY
	}
}

func convertInternalColumnToProto(ic shape.Column) *pb.Column {
	return &pb.Column{
		Name: ic.Name,
		Type: convertInternalDataTypeToProto(ic.Type),
		Path: ic.Path,
	}
}

func convertInternalIndexToProto(ii shape.Index) *pb.Index {
	cols := make([]string, len(ii.Columns))
	copy(cols, ii.Columns)
	return &pb.Index{
		Columns: cols,
	}
}

func convertInternalShapeToProto(is shape.Shape) *pb.Shape {
	protoShape := &pb.Shape{
		Columns: make([]*pb.Column, 0, len(is.Columns)),
		Indexes: make([]*pb.Index, 0, len(is.Indexes)),
	}

	for _, ic := range is.Columns {
		protoShape.Columns = append(protoShape.Columns, convertInternalColumnToProto(ic))
	}

	for _, ii := range is.Indexes {
		protoShape.Indexes = append(protoShape.Indexes, convertInternalIndexToProto(ii))
	}

	return protoShape
}
