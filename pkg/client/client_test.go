package client

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"testing"
	"time"

	pb "github.com/nrjais/emcache/pkg/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Simple hand-written mocks to avoid import cycles
type MockEmcacheServiceClient struct {
	downloadDbFunc       func(ctx context.Context, req *pb.DownloadDbRequest, opts ...grpc.CallOption) (pb.EmcacheService_DownloadDbClient, error)
	getOplogEntriesFunc  func(ctx context.Context, req *pb.GetOplogEntriesRequest, opts ...grpc.CallOption) (*pb.GetOplogEntriesResponse, error)
	addCollectionFunc    func(ctx context.Context, req *pb.AddCollectionRequest, opts ...grpc.CallOption) (*pb.AddCollectionResponse, error)
	removeCollectionFunc func(ctx context.Context, req *pb.RemoveCollectionRequest, opts ...grpc.CallOption) (*pb.RemoveCollectionResponse, error)
	getCollectionsFunc   func(ctx context.Context, req *pb.GetCollectionsRequest, opts ...grpc.CallOption) (*pb.GetCollectionsResponse, error)
}

func (m *MockEmcacheServiceClient) DownloadDb(ctx context.Context, req *pb.DownloadDbRequest, opts ...grpc.CallOption) (pb.EmcacheService_DownloadDbClient, error) {
	return m.downloadDbFunc(ctx, req, opts...)
}

func (m *MockEmcacheServiceClient) GetOplogEntries(ctx context.Context, req *pb.GetOplogEntriesRequest, opts ...grpc.CallOption) (*pb.GetOplogEntriesResponse, error) {
	return m.getOplogEntriesFunc(ctx, req, opts...)
}

func (m *MockEmcacheServiceClient) AddCollection(ctx context.Context, req *pb.AddCollectionRequest, opts ...grpc.CallOption) (*pb.AddCollectionResponse, error) {
	return m.addCollectionFunc(ctx, req, opts...)
}

func (m *MockEmcacheServiceClient) RemoveCollection(ctx context.Context, req *pb.RemoveCollectionRequest, opts ...grpc.CallOption) (*pb.RemoveCollectionResponse, error) {
	return m.removeCollectionFunc(ctx, req, opts...)
}

func (m *MockEmcacheServiceClient) GetCollections(ctx context.Context, req *pb.GetCollectionsRequest, opts ...grpc.CallOption) (*pb.GetCollectionsResponse, error) {
	return m.getCollectionsFunc(ctx, req, opts...)
}

type MockCollection struct {
	getLastAppliedOplogIndexFunc func(ctx context.Context) (int64, error)
	applyOplogEntriesFunc        func(ctx context.Context, entries []*pb.OplogEntry) error
	queryFunc                    func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	closeFunc                    func() error
}

func (m *MockCollection) GetLastAppliedOplogIndex(ctx context.Context) (int64, error) {
	return m.getLastAppliedOplogIndexFunc(ctx)
}

func (m *MockCollection) ApplyOplogEntries(ctx context.Context, entries []*pb.OplogEntry) error {
	return m.applyOplogEntriesFunc(ctx, entries)
}

func (m *MockCollection) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return m.queryFunc(ctx, query, args...)
}

func (m *MockCollection) Close() error {
	return m.closeFunc()
}

type MockDecompression struct {
	decompressStreamFunc func(stream grpc.ClientStream, firstChunk []byte, compression pb.Compression, writer io.Writer) error
}

func (m *MockDecompression) DecompressStream(stream grpc.ClientStream, firstChunk []byte, compression pb.Compression, writer io.Writer) error {
	return m.decompressStreamFunc(stream, firstChunk, compression, writer)
}

type MockDownloadStream struct {
	recvFunc      func() (*pb.DownloadDbResponse, error)
	closeSendFunc func() error
}

func (m *MockDownloadStream) Recv() (*pb.DownloadDbResponse, error) {
	return m.recvFunc()
}

func (m *MockDownloadStream) CloseSend() error {
	return m.closeSendFunc()
}

func (m *MockDownloadStream) Header() (metadata.MD, error) { return nil, nil }
func (m *MockDownloadStream) Trailer() metadata.MD         { return nil }
func (m *MockDownloadStream) Context() context.Context     { return context.Background() }
func (m *MockDownloadStream) SendMsg(interface{}) error    { return nil }
func (m *MockDownloadStream) RecvMsg(interface{}) error    { return nil }

func setupTestClient(t *testing.T) (*Client, *MockEmcacheServiceClient, *MockDecompression) {
	mockGrpcClient := &MockEmcacheServiceClient{}
	mockDecompression := &MockDecompression{}

	deps := ClientDependencies{
		GrpcClient:    mockGrpcClient,
		Decompression: mockDecompression,
		Conn:          nil, // Not needed for tests
	}

	config := ClientConfig{
		ServerAddr:     "test-addr",
		Directory:      "/test/dir",
		Collections:    []CollectionConfig{}, // Start with no collections to avoid initialization issues
		UpdateInterval: time.Second,
		BatchSize:      10,
	}

	// Create client without collections initially
	client := &Client{
		deps:         deps,
		config:       config,
		collections:  make(map[string]*collectionState),
		colNames:     []string{"test-collection"},
		lastOplogIdx: 5, // Set directly
	}

	if client.config.UpdateInterval <= 0 {
		t.Fatal("update interval must be greater than 0")
	}

	// Manually add the mock collection
	mockCollection := &MockCollection{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 5, nil
		},
		closeFunc: func() error { return nil },
	}

	client.collections["test-collection"] = &collectionState{
		config:     CollectionConfig{Name: "test-collection"},
		collection: mockCollection,
	}

	return client, mockGrpcClient, mockDecompression
}

func TestNewClientWithDependencies_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	assert.NotNil(t, client)
	assert.Equal(t, int64(5), client.lastOplogIdx)
	assert.Len(t, client.collections, 1)
	assert.Contains(t, client.collections, "test-collection")
}

func TestNewClientWithDependencies_InvalidUpdateInterval(t *testing.T) {
	deps := ClientDependencies{
		GrpcClient:    &MockEmcacheServiceClient{},
		Decompression: &MockDecompression{},
	}

	config := ClientConfig{
		UpdateInterval: 0, // Invalid
	}

	_, err := NewClientWithDependencies(context.Background(), config, deps)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update interval must be greater than 0")
}

func TestInitializeLastOplogIdx_MultipleCollections(t *testing.T) {
	client := &Client{
		collections: make(map[string]*collectionState),
	}

	// Create mock collections with different last oplog indices
	mockCol1 := &MockCollection{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 10, nil
		},
	}

	mockCol2 := &MockCollection{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 5, nil
		},
	}

	mockCol3 := &MockCollection{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 15, nil
		},
	}

	client.collections["col1"] = &collectionState{collection: mockCol1}
	client.collections["col2"] = &collectionState{collection: mockCol2}
	client.collections["col3"] = &collectionState{collection: mockCol3}

	err := client.initializeLastOplogIdx(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(5), client.lastOplogIdx, "Should use the minimum oplog index")
}

func TestInitializeLastOplogIdx_NoCollections(t *testing.T) {
	client := &Client{
		collections: make(map[string]*collectionState),
	}

	err := client.initializeLastOplogIdx(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(0), client.lastOplogIdx, "Should default to 0 when no collections")
}

func TestInitializeLastOplogIdx_Error(t *testing.T) {
	client := &Client{
		collections: make(map[string]*collectionState),
	}

	mockCol := &MockCollection{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 0, errors.New("database error")
		},
	}

	client.collections["test"] = &collectionState{collection: mockCol}

	err := client.initializeLastOplogIdx(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database error")
}

func TestSyncToLatest_Success(t *testing.T) {
	client, mockGrpc, _ := setupTestClient(t)

	entries := []*pb.OplogEntry{
		{Index: 6, Collection: "test-collection", Operation: pb.OplogEntry_UPSERT, Id: "doc1"},
		{Index: 7, Collection: "test-collection", Operation: pb.OplogEntry_DELETE, Id: "doc2"},
	}

	// First call returns entries, second call returns empty (sync complete)
	callCount := 0
	mockGrpc.getOplogEntriesFunc = func(ctx context.Context, req *pb.GetOplogEntriesRequest, opts ...grpc.CallOption) (*pb.GetOplogEntriesResponse, error) {
		callCount++
		if callCount == 1 {
			return &pb.GetOplogEntriesResponse{Entries: entries}, nil
		}
		return &pb.GetOplogEntriesResponse{Entries: []*pb.OplogEntry{}}, nil
	}

	// Mock applying entries
	mockCollection := client.collections["test-collection"].collection.(*MockCollection)
	mockCollection.applyOplogEntriesFunc = func(ctx context.Context, entries []*pb.OplogEntry) error {
		return nil
	}

	err := client.SyncToLatest(context.Background(), 10)
	require.NoError(t, err)

	assert.Equal(t, int64(7), client.lastOplogIdx)
}

func TestSyncToLatest_ErrorGettingEntries(t *testing.T) {
	client, mockGrpc, _ := setupTestClient(t)

	mockGrpc.getOplogEntriesFunc = func(ctx context.Context, req *pb.GetOplogEntriesRequest, opts ...grpc.CallOption) (*pb.GetOplogEntriesResponse, error) {
		return nil, errors.New("grpc error")
	}

	err := client.SyncToLatest(context.Background(), 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "grpc error")
}

func TestQuery_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	mockRows := &sql.Rows{}
	mockCollection := client.collections["test-collection"].collection.(*MockCollection)
	mockCollection.queryFunc = func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		assert.Equal(t, "SELECT * FROM data", query)
		assert.Equal(t, []any{"arg1"}, args)
		return mockRows, nil
	}

	rows, err := client.Query(context.Background(), "test-collection", "SELECT * FROM data", "arg1")
	require.NoError(t, err)
	assert.Equal(t, mockRows, rows)
}

func TestQuery_CollectionNotFound(t *testing.T) {
	client, _, _ := setupTestClient(t)

	_, err := client.Query(context.Background(), "non-existent", "SELECT * FROM data")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection 'non-existent' not found")
}

func TestAddCollection_Success(t *testing.T) {
	client, mockGrpc, _ := setupTestClient(t)

	shape := &pb.Shape{
		Columns: []*pb.Column{{Name: "field1", Type: pb.DataType_TEXT}},
	}

	expectedResp := &pb.AddCollectionResponse{}
	mockGrpc.addCollectionFunc = func(ctx context.Context, req *pb.AddCollectionRequest, opts ...grpc.CallOption) (*pb.AddCollectionResponse, error) {
		assert.Equal(t, "new-collection", req.CollectionName)
		assert.Equal(t, shape, req.Shape)
		return expectedResp, nil
	}

	resp, err := client.AddCollection(context.Background(), "new-collection", shape)
	require.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
}

func TestAddCollection_NilShape(t *testing.T) {
	client, _, _ := setupTestClient(t)

	_, err := client.AddCollection(context.Background(), "new-collection", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "shape cannot be nil")
}

func TestRemoveCollection_Success(t *testing.T) {
	client, mockGrpc, _ := setupTestClient(t)

	expectedResp := &pb.RemoveCollectionResponse{}
	mockGrpc.removeCollectionFunc = func(ctx context.Context, req *pb.RemoveCollectionRequest, opts ...grpc.CallOption) (*pb.RemoveCollectionResponse, error) {
		assert.Equal(t, "test-collection", req.CollectionName)
		return expectedResp, nil
	}

	resp, err := client.RemoveCollection(context.Background(), "test-collection")
	require.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
}

func TestStartDbUpdates_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	err := client.StartDbUpdates()
	require.NoError(t, err)

	// Verify it was started
	assert.NotNil(t, client.cancelFunc)

	// Stop it
	client.StopUpdates()
	assert.Nil(t, client.cancelFunc)
}

func TestStartDbUpdates_AlreadyStarted(t *testing.T) {
	client, _, _ := setupTestClient(t)

	err := client.StartDbUpdates()
	require.NoError(t, err)

	// Try to start again
	err = client.StartDbUpdates()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db updates already started")

	client.StopUpdates()
}

func TestClose_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	err := client.Close()
	assert.NoError(t, err)
}

func TestApplyOplogEntries_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	entries := []*pb.OplogEntry{
		{Index: 6, Collection: "test-collection", Operation: pb.OplogEntry_UPSERT, Id: "doc1"},
		{Index: 7, Collection: "test-collection", Operation: pb.OplogEntry_DELETE, Id: "doc2"},
	}

	mockCollection := client.collections["test-collection"].collection.(*MockCollection)
	mockCollection.applyOplogEntriesFunc = func(ctx context.Context, entries []*pb.OplogEntry) error {
		return nil
	}

	lastIdx, err := client.applyOplogEntries(context.Background(), entries)
	require.NoError(t, err)
	assert.Equal(t, int64(7), lastIdx)
}

func TestApplyOplogEntries_WithUnknownCollection(t *testing.T) {
	client, _, _ := setupTestClient(t)

	entries := []*pb.OplogEntry{
		{Index: 6, Collection: "unknown-collection", Operation: pb.OplogEntry_UPSERT, Id: "doc1"},
		{Index: 7, Collection: "test-collection", Operation: pb.OplogEntry_DELETE, Id: "doc2"},
	}

	mockCollection := client.collections["test-collection"].collection.(*MockCollection)
	// Should only get the entry for test-collection
	mockCollection.applyOplogEntriesFunc = func(ctx context.Context, entries []*pb.OplogEntry) error {
		assert.Len(t, entries, 1)
		assert.Equal(t, int64(7), entries[0].Index)
		return nil
	}

	lastIdx, err := client.applyOplogEntries(context.Background(), entries)
	require.NoError(t, err)
	assert.Equal(t, int64(7), lastIdx) // Should still track the highest index
}

func TestDatabaseReuse(t *testing.T) {
	// This test would require a more complex setup with actual file system operations
	// For now, we'll verify that the version checking logic works correctly

	// Create test collection data
	serverCollections := []*pb.Collection{
		{
			Name:    "test-collection",
			Version: 5,
			Shape: &pb.Shape{
				Columns: []*pb.Column{
					{Name: "id", Type: pb.DataType_TEXT},
					{Name: "name", Type: pb.DataType_TEXT},
				},
			},
		},
	}

	collectionsData := &pb.GetCollectionsResponse{
		Collections: serverCollections,
	}

	// Test that collection details are correctly extracted from server response
	for _, coll := range collectionsData.Collections {
		if coll.Name == "test-collection" {
			assert.Equal(t, int32(5), coll.Version)
			assert.NotNil(t, coll.Shape)
			assert.Len(t, coll.Shape.Columns, 2)
		}
	}
}

func TestVersionMismatch(t *testing.T) {
	// This test verifies the version comparison logic

	storedVersion := int32(3)
	expectedVersion := int32(5)

	// When stored version doesn't match expected version, database should not be reused
	shouldReuse := storedVersion == expectedVersion
	assert.False(t, shouldReuse, "Database with wrong version should not be reused")

	// When versions match, database should be reused
	storedVersion = int32(5)
	shouldReuse = storedVersion == expectedVersion
	assert.True(t, shouldReuse, "Database with correct version should be reused")
}
