package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	collectioncachemocks "github.com/nrjais/emcache/internal/collectioncache/mocks"
	"github.com/nrjais/emcache/internal/db"
	dbmocks "github.com/nrjais/emcache/internal/db/mocks"
	followermocks "github.com/nrjais/emcache/internal/follower/mocks"
	"github.com/nrjais/emcache/internal/grpcapi/mocks"
	"github.com/nrjais/emcache/internal/shape"
	pb "github.com/nrjais/emcache/pkg/protos"
)

func TestNewEmcacheServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCollCache := collectioncachemocks.NewMockCollectionCacheManager(ctrl)
	mockFollower := followermocks.NewMockFollowerInterface(ctrl)

	t.Run("creates server with valid parameters", func(t *testing.T) {
		server := NewEmcacheServer(nil, "/tmp/sqlite", mockCollCache, mockFollower)

		assert.NotNil(t, server)
		assert.Implements(t, (*pb.EmcacheServiceServer)(nil), server)
	})
}

func TestServer_GetOplogEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPgPool := dbmocks.NewMockPostgresPool(ctrl)
	mockCollCache := collectioncachemocks.NewMockCollectionCacheManager(ctrl)
	mockFollower := followermocks.NewMockFollowerInterface(ctrl)
	mockDbOps := mocks.NewMockDatabaseOperations(ctrl)

	server := NewEmcacheServerWithDeps(mockPgPool, "/tmp/sqlite", mockCollCache, mockFollower, mockDbOps).(*server)

	t.Run("successful request with valid parameters", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetOplogEntriesRequest{
			CollectionNames: []string{"users", "orders"},
			AfterIndex:      100,
			Limit:           50,
		}

		expectedEntries := []db.OplogEntry{
			{
				ID:         101,
				Operation:  "UPSERT",
				DocID:      "user1",
				CreatedAt:  time.Now(),
				Collection: "users",
				Doc:        []byte(`{"name": "John"}`),
				Version:    1,
			},
			{
				ID:         102,
				Operation:  "DELETE",
				DocID:      "user2",
				CreatedAt:  time.Now(),
				Collection: "users",
				Version:    1,
			},
		}

		mockDbOps.EXPECT().
			GetOplogEntriesMultipleCollections(ctx, mockPgPool, []string{"users", "orders"}, int64(100), 50).
			Return(expectedEntries, nil)

		resp, err := server.GetOplogEntries(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, resp.Entries, 2)

		// Check first entry
		assert.Equal(t, int64(101), resp.Entries[0].Index)
		assert.Equal(t, pb.OplogEntry_UPSERT, resp.Entries[0].Operation)
		assert.Equal(t, "user1", resp.Entries[0].Id)
		assert.Equal(t, "users", resp.Entries[0].Collection)
		assert.Equal(t, int32(1), resp.Entries[0].Version)

		// Check second entry
		assert.Equal(t, int64(102), resp.Entries[1].Index)
		assert.Equal(t, pb.OplogEntry_DELETE, resp.Entries[1].Operation)
		assert.Equal(t, "user2", resp.Entries[1].Id)
		assert.Equal(t, "users", resp.Entries[1].Collection)
		assert.Equal(t, int32(1), resp.Entries[1].Version)
	})

	t.Run("returns error for empty collection names", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetOplogEntriesRequest{
			CollectionNames: []string{},
			AfterIndex:      100,
			Limit:           50,
		}

		resp, err := server.GetOplogEntries(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "At least one collection name must be provided")
	})

	t.Run("uses default limit when limit is zero", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetOplogEntriesRequest{
			CollectionNames: []string{"users"},
			AfterIndex:      100,
			Limit:           0,
		}

		mockDbOps.EXPECT().
			GetOplogEntriesMultipleCollections(ctx, mockPgPool, []string{"users"}, int64(100), 100).
			Return([]db.OplogEntry{}, nil)

		resp, err := server.GetOplogEntries(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("caps limit when too high", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetOplogEntriesRequest{
			CollectionNames: []string{"users"},
			AfterIndex:      100,
			Limit:           2000, // Above max
		}

		mockDbOps.EXPECT().
			GetOplogEntriesMultipleCollections(ctx, mockPgPool, []string{"users"}, int64(100), 1000).
			Return([]db.OplogEntry{}, nil)

		resp, err := server.GetOplogEntries(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("handles database error", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetOplogEntriesRequest{
			CollectionNames: []string{"users"},
			AfterIndex:      100,
			Limit:           50,
		}

		mockDbOps.EXPECT().
			GetOplogEntriesMultipleCollections(ctx, mockPgPool, []string{"users"}, int64(100), 50).
			Return(nil, errors.New("database connection failed"))

		resp, err := server.GetOplogEntries(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "Failed to fetch oplog entries")
	})

	t.Run("handles invalid JSON in oplog entry", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetOplogEntriesRequest{
			CollectionNames: []string{"users"},
			AfterIndex:      100,
			Limit:           50,
		}

		invalidEntry := db.OplogEntry{
			ID:         101,
			Operation:  "UPSERT",
			DocID:      "user1",
			CreatedAt:  time.Now(),
			Collection: "users",
			Doc:        []byte(`{invalid json`), // Invalid JSON
			Version:    1,
		}

		mockDbOps.EXPECT().
			GetOplogEntriesMultipleCollections(ctx, mockPgPool, []string{"users"}, int64(100), 50).
			Return([]db.OplogEntry{invalidEntry}, nil)

		resp, err := server.GetOplogEntries(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "Failed to convert oplog entry")
	})
}

func TestServer_GetCollections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPgPool := dbmocks.NewMockPostgresPool(ctrl)
	mockCollCache := collectioncachemocks.NewMockCollectionCacheManager(ctrl)
	mockFollower := followermocks.NewMockFollowerInterface(ctrl)
	mockDbOps := mocks.NewMockDatabaseOperations(ctrl)

	server := NewEmcacheServerWithDeps(mockPgPool, "/tmp/sqlite", mockCollCache, mockFollower, mockDbOps).(*server)

	t.Run("successful request returns collections", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetCollectionsRequest{}

		expectedCollections := []db.ReplicatedCollection{
			{
				CollectionName: "users",
				CurrentVersion: 1,
				Shape: shape.Shape{
					Columns: []shape.Column{
						{Name: "name", Type: shape.Text, Path: "name"},
						{Name: "age", Type: shape.Integer, Path: "age"},
					},
					Indexes: []shape.Index{
						{Columns: []string{"name"}},
					},
				},
			},
			{
				CollectionName: "orders",
				CurrentVersion: 2,
				Shape: shape.Shape{
					Columns: []shape.Column{
						{Name: "total", Type: shape.Number, Path: "total"},
					},
				},
			},
		}

		mockDbOps.EXPECT().
			GetAllReplicatedCollectionsWithShapes(ctx, mockPgPool).
			Return(expectedCollections, nil)

		resp, err := server.GetCollections(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, resp.Collections, 2)

		// Check first collection
		assert.Equal(t, "users", resp.Collections[0].Name)
		assert.Equal(t, int32(1), resp.Collections[0].Version)
		assert.Len(t, resp.Collections[0].Shape.Columns, 2)
		assert.Len(t, resp.Collections[0].Shape.Indexes, 1)

		// Check second collection
		assert.Equal(t, "orders", resp.Collections[1].Name)
		assert.Equal(t, int32(2), resp.Collections[1].Version)
		assert.Len(t, resp.Collections[1].Shape.Columns, 1)
	})

	t.Run("handles database error", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetCollectionsRequest{}

		mockDbOps.EXPECT().
			GetAllReplicatedCollectionsWithShapes(ctx, mockPgPool).
			Return(nil, errors.New("database connection failed"))

		resp, err := server.GetCollections(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "Failed to fetch collection list")
	})

	t.Run("returns empty list when no collections", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.GetCollectionsRequest{}

		mockDbOps.EXPECT().
			GetAllReplicatedCollectionsWithShapes(ctx, mockPgPool).
			Return([]db.ReplicatedCollection{}, nil)

		resp, err := server.GetCollections(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, resp.Collections, 0)
	})
}

func TestServer_AddCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPgPool := dbmocks.NewMockPostgresPool(ctrl)
	mockCollCache := collectioncachemocks.NewMockCollectionCacheManager(ctrl)
	mockFollower := followermocks.NewMockFollowerInterface(ctrl)
	mockDbOps := mocks.NewMockDatabaseOperations(ctrl)

	server := NewEmcacheServerWithDeps(mockPgPool, "/tmp/sqlite", mockCollCache, mockFollower, mockDbOps).(*server)

	t.Run("successful request adds collection", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.AddCollectionRequest{
			CollectionName: "users",
			Shape: &pb.Shape{
				Columns: []*pb.Column{
					{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
					{Name: "age", Type: pb.DataType_INTEGER, Path: "age"},
				},
				Indexes: []*pb.Index{
					{Columns: []string{"name"}},
				},
			},
		}

		mockDbOps.EXPECT().
			AddReplicatedCollection(ctx, mockPgPool, "users", gomock.Any()).
			DoAndReturn(func(ctx context.Context, pool db.PostgresPool, collectionName string, shapeJSON []byte) error {
				assert.Equal(t, "users", collectionName)

				var shapeObj shape.Shape
				err := json.Unmarshal(shapeJSON, &shapeObj)
				require.NoError(t, err)
				assert.Len(t, shapeObj.Columns, 2)
				assert.Equal(t, "name", shapeObj.Columns[0].Name)
				assert.Equal(t, shape.Text, shapeObj.Columns[0].Type)

				return nil
			})

		resp, err := server.AddCollection(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("returns error for empty collection name", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.AddCollectionRequest{
			CollectionName: "",
			Shape: &pb.Shape{
				Columns: []*pb.Column{
					{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
				},
			},
		}

		resp, err := server.AddCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "Collection name cannot be empty")
	})

	t.Run("returns error for nil shape", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.AddCollectionRequest{
			CollectionName: "users",
			Shape:          nil,
		}

		resp, err := server.AddCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "Shape definition cannot be empty")
	})

	t.Run("returns error for shape with no columns", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.AddCollectionRequest{
			CollectionName: "users",
			Shape: &pb.Shape{
				Columns: []*pb.Column{},
			},
		}

		resp, err := server.AddCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "shape must have at least one data column defined")
	})

	t.Run("returns error for reserved column name 'id'", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.AddCollectionRequest{
			CollectionName: "users",
			Shape: &pb.Shape{
				Columns: []*pb.Column{
					{Name: "id", Type: pb.DataType_TEXT, Path: "_id"},
					{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
				},
			},
		}

		resp, err := server.AddCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "Column name 'id' is reserved")
	})

	t.Run("returns error when collection already exists", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.AddCollectionRequest{
			CollectionName: "users",
			Shape: &pb.Shape{
				Columns: []*pb.Column{
					{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
				},
			},
		}

		mockDbOps.EXPECT().
			AddReplicatedCollection(ctx, mockPgPool, "users", gomock.Any()).
			Return(db.ErrCollectionAlreadyExists)

		resp, err := server.AddCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.AlreadyExists, st.Code())
		assert.Contains(t, st.Message(), "already configured for replication")
	})

	t.Run("handles database error", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.AddCollectionRequest{
			CollectionName: "users",
			Shape: &pb.Shape{
				Columns: []*pb.Column{
					{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
				},
			},
		}

		mockDbOps.EXPECT().
			AddReplicatedCollection(ctx, mockPgPool, "users", gomock.Any()).
			Return(errors.New("database connection failed"))

		resp, err := server.AddCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "Failed to add collection to replication list")
	})
}

func TestServer_RemoveCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPgPool := dbmocks.NewMockPostgresPool(ctrl)
	mockCollCache := collectioncachemocks.NewMockCollectionCacheManager(ctrl)
	mockFollower := followermocks.NewMockFollowerInterface(ctrl)
	mockDbOps := mocks.NewMockDatabaseOperations(ctrl)

	server := NewEmcacheServerWithDeps(mockPgPool, "/tmp/sqlite", mockCollCache, mockFollower, mockDbOps).(*server)

	t.Run("successful request removes collection", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.RemoveCollectionRequest{
			CollectionName: "users",
		}

		mockDbOps.EXPECT().
			RemoveReplicatedCollection(ctx, mockPgPool, "users").
			Return(nil)

		resp, err := server.RemoveCollection(ctx, req)

		require.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("returns error for empty collection name", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.RemoveCollectionRequest{
			CollectionName: "",
		}

		resp, err := server.RemoveCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "Collection name cannot be empty")
	})

	t.Run("returns error when collection not found", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.RemoveCollectionRequest{
			CollectionName: "nonexistent",
		}

		mockDbOps.EXPECT().
			RemoveReplicatedCollection(ctx, mockPgPool, "nonexistent").
			Return(errors.New("collection 'nonexistent' not found"))

		resp, err := server.RemoveCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.NotFound, st.Code())
		assert.Contains(t, st.Message(), "not found for removal")
	})

	t.Run("handles database error", func(t *testing.T) {
		ctx := context.Background()
		req := &pb.RemoveCollectionRequest{
			CollectionName: "users",
		}

		mockDbOps.EXPECT().
			RemoveReplicatedCollection(ctx, mockPgPool, "users").
			Return(errors.New("database connection failed"))

		resp, err := server.RemoveCollection(ctx, req)

		assert.Nil(t, resp)
		assert.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "Failed to remove collection from replication list")
	})
}

func TestConvertDbOplogToProto(t *testing.T) {
	t.Run("converts UPSERT operation correctly", func(t *testing.T) {
		now := time.Now()
		entry := db.OplogEntry{
			ID:         123,
			Operation:  "UPSERT",
			DocID:      "user1",
			CreatedAt:  now,
			Collection: "users",
			Doc:        []byte(`{"name": "John", "age": 30}`),
			Version:    1,
		}

		pbEntry, err := convertDbOplogToProto(entry)

		require.NoError(t, err)
		assert.Equal(t, int64(123), pbEntry.Index)
		assert.Equal(t, pb.OplogEntry_UPSERT, pbEntry.Operation)
		assert.Equal(t, "user1", pbEntry.Id)
		assert.Equal(t, "users", pbEntry.Collection)
		assert.Equal(t, int32(1), pbEntry.Version)
		assert.NotNil(t, pbEntry.Data)
		assert.NotNil(t, pbEntry.Timestamp)

		// Check timestamp conversion
		assert.Equal(t, now.Unix(), pbEntry.Timestamp.Seconds)
	})

	t.Run("converts DELETE operation correctly", func(t *testing.T) {
		now := time.Now()
		entry := db.OplogEntry{
			ID:         124,
			Operation:  "DELETE",
			DocID:      "user2",
			CreatedAt:  now,
			Collection: "users",
			Doc:        nil,
			Version:    1,
		}

		pbEntry, err := convertDbOplogToProto(entry)

		require.NoError(t, err)
		assert.Equal(t, int64(124), pbEntry.Index)
		assert.Equal(t, pb.OplogEntry_DELETE, pbEntry.Operation)
		assert.Equal(t, "user2", pbEntry.Id)
		assert.Equal(t, "users", pbEntry.Collection)
		assert.Equal(t, int32(1), pbEntry.Version)
		assert.Nil(t, pbEntry.Data)
		assert.NotNil(t, pbEntry.Timestamp)
	})

	t.Run("handles invalid JSON in doc", func(t *testing.T) {
		entry := db.OplogEntry{
			ID:         125,
			Operation:  "UPSERT",
			DocID:      "user3",
			CreatedAt:  time.Now(),
			Collection: "users",
			Doc:        []byte(`{invalid json`),
			Version:    1,
		}

		pbEntry, err := convertDbOplogToProto(entry)

		assert.Nil(t, pbEntry)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal oplog JSON data")
	})

	t.Run("handles empty doc for UPSERT", func(t *testing.T) {
		entry := db.OplogEntry{
			ID:         126,
			Operation:  "UPSERT",
			DocID:      "user4",
			CreatedAt:  time.Now(),
			Collection: "users",
			Doc:        []byte{},
			Version:    1,
		}

		pbEntry, err := convertDbOplogToProto(entry)

		require.NoError(t, err)
		assert.Nil(t, pbEntry.Data)
	})
}

func TestConvertProtoShapeToInternal(t *testing.T) {
	t.Run("converts valid shape correctly", func(t *testing.T) {
		protoShape := &pb.Shape{
			Columns: []*pb.Column{
				{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
				{Name: "age", Type: pb.DataType_INTEGER, Path: "age"},
				{Name: "active", Type: pb.DataType_BOOL, Path: "active"},
				{Name: "score", Type: pb.DataType_NUMBER, Path: "score"},
				{Name: "metadata", Type: pb.DataType_JSONB, Path: "metadata"},
				{Name: "any_field", Type: pb.DataType_ANY, Path: "any_field"},
			},
			Indexes: []*pb.Index{
				{Columns: []string{"name"}},
				{Columns: []string{"name", "age"}},
			},
		}

		internalShape, err := convertProtoShapeToInternal(protoShape)

		require.NoError(t, err)
		assert.Len(t, internalShape.Columns, 6)
		assert.Len(t, internalShape.Indexes, 2)

		// Check column conversions
		assert.Equal(t, "name", internalShape.Columns[0].Name)
		assert.Equal(t, shape.Text, internalShape.Columns[0].Type)
		assert.Equal(t, "name", internalShape.Columns[0].Path)

		assert.Equal(t, "age", internalShape.Columns[1].Name)
		assert.Equal(t, shape.Integer, internalShape.Columns[1].Type)

		assert.Equal(t, "active", internalShape.Columns[2].Name)
		assert.Equal(t, shape.Bool, internalShape.Columns[2].Type)

		assert.Equal(t, "score", internalShape.Columns[3].Name)
		assert.Equal(t, shape.Number, internalShape.Columns[3].Type)

		assert.Equal(t, "metadata", internalShape.Columns[4].Name)
		assert.Equal(t, shape.JSONB, internalShape.Columns[4].Type)

		assert.Equal(t, "any_field", internalShape.Columns[5].Name)
		assert.Equal(t, shape.Any, internalShape.Columns[5].Type)

		// Check index conversions
		assert.Equal(t, []string{"name"}, internalShape.Indexes[0].Columns)
		assert.Equal(t, []string{"name", "age"}, internalShape.Indexes[1].Columns)
	})

	t.Run("returns error for nil shape", func(t *testing.T) {
		internalShape, err := convertProtoShapeToInternal(nil)

		assert.Equal(t, shape.Shape{}, internalShape)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "protobuf shape cannot be nil")
	})

	t.Run("returns error for shape with no columns", func(t *testing.T) {
		protoShape := &pb.Shape{
			Columns: []*pb.Column{},
		}

		internalShape, err := convertProtoShapeToInternal(protoShape)

		assert.Equal(t, shape.Shape{}, internalShape)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "shape must have at least one data column defined")
	})

	t.Run("handles nil columns and indexes gracefully", func(t *testing.T) {
		protoShape := &pb.Shape{
			Columns: []*pb.Column{
				nil,
				{Name: "name", Type: pb.DataType_TEXT, Path: "name"},
				nil,
			},
			Indexes: []*pb.Index{
				nil,
				{Columns: []string{"name"}},
				nil,
			},
		}

		internalShape, err := convertProtoShapeToInternal(protoShape)

		require.NoError(t, err)
		assert.Len(t, internalShape.Columns, 1) // Only non-nil column
		assert.Len(t, internalShape.Indexes, 1) // Only non-nil index
		assert.Equal(t, "name", internalShape.Columns[0].Name)
		assert.Equal(t, []string{"name"}, internalShape.Indexes[0].Columns)
	})
}

func TestConvertInternalShapeToProto(t *testing.T) {
	t.Run("converts internal shape to proto correctly", func(t *testing.T) {
		internalShape := shape.Shape{
			Columns: []shape.Column{
				{Name: "name", Type: shape.Text, Path: "name"},
				{Name: "age", Type: shape.Integer, Path: "age"},
				{Name: "active", Type: shape.Bool, Path: "active"},
				{Name: "score", Type: shape.Number, Path: "score"},
				{Name: "metadata", Type: shape.JSONB, Path: "metadata"},
				{Name: "any_field", Type: shape.Any, Path: "any_field"},
			},
			Indexes: []shape.Index{
				{Columns: []string{"name"}},
				{Columns: []string{"name", "age"}},
			},
		}

		protoShape := convertInternalShapeToProto(internalShape)

		assert.Len(t, protoShape.Columns, 6)
		assert.Len(t, protoShape.Indexes, 2)

		// Check column conversions
		assert.Equal(t, "name", protoShape.Columns[0].Name)
		assert.Equal(t, pb.DataType_TEXT, protoShape.Columns[0].Type)
		assert.Equal(t, "name", protoShape.Columns[0].Path)

		assert.Equal(t, "age", protoShape.Columns[1].Name)
		assert.Equal(t, pb.DataType_INTEGER, protoShape.Columns[1].Type)

		assert.Equal(t, "active", protoShape.Columns[2].Name)
		assert.Equal(t, pb.DataType_BOOL, protoShape.Columns[2].Type)

		assert.Equal(t, "score", protoShape.Columns[3].Name)
		assert.Equal(t, pb.DataType_NUMBER, protoShape.Columns[3].Type)

		assert.Equal(t, "metadata", protoShape.Columns[4].Name)
		assert.Equal(t, pb.DataType_JSONB, protoShape.Columns[4].Type)

		assert.Equal(t, "any_field", protoShape.Columns[5].Name)
		assert.Equal(t, pb.DataType_ANY, protoShape.Columns[5].Type)

		// Check index conversions
		assert.Equal(t, []string{"name"}, protoShape.Indexes[0].Columns)
		assert.Equal(t, []string{"name", "age"}, protoShape.Indexes[1].Columns)
	})

	t.Run("handles empty shape", func(t *testing.T) {
		internalShape := shape.Shape{
			Columns: []shape.Column{},
			Indexes: []shape.Index{},
		}

		protoShape := convertInternalShapeToProto(internalShape)

		assert.Len(t, protoShape.Columns, 0)
		assert.Len(t, protoShape.Indexes, 0)
	})
}

func TestDataTypeConversions(t *testing.T) {
	t.Run("convertProtoDataType handles all types", func(t *testing.T) {
		tests := []struct {
			proto    pb.DataType
			expected shape.DataType
		}{
			{pb.DataType_TEXT, shape.Text},
			{pb.DataType_INTEGER, shape.Integer},
			{pb.DataType_NUMBER, shape.Number},
			{pb.DataType_BOOL, shape.Bool},
			{pb.DataType_JSONB, shape.JSONB},
			{pb.DataType_ANY, shape.Any},
		}

		for _, test := range tests {
			result := convertProtoDataType(test.proto)
			assert.Equal(t, test.expected, result, "Failed for %v", test.proto)
		}
	})

	t.Run("convertInternalDataTypeToProto handles all types", func(t *testing.T) {
		tests := []struct {
			internal shape.DataType
			expected pb.DataType
		}{
			{shape.Text, pb.DataType_TEXT},
			{shape.Integer, pb.DataType_INTEGER},
			{shape.Number, pb.DataType_NUMBER},
			{shape.Bool, pb.DataType_BOOL},
			{shape.JSONB, pb.DataType_JSONB},
			{shape.Any, pb.DataType_ANY},
		}

		for _, test := range tests {
			result := convertInternalDataTypeToProto(test.internal)
			assert.Equal(t, test.expected, result, "Failed for %v", test.internal)
		}
	})
}

func TestValidatorInitialization(t *testing.T) {
	t.Run("validator is initialized", func(t *testing.T) {
		assert.NotNil(t, validate)
		assert.IsType(t, &validator.Validate{}, validate)
	})
}

func TestDatabaseOperations(t *testing.T) {
	t.Run("DefaultDatabaseOperations implements interface", func(t *testing.T) {
		var ops DatabaseOperations = &DefaultDatabaseOperations{}
		assert.NotNil(t, ops)
	})
}
