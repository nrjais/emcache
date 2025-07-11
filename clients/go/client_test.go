package emcache

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Simple hand-written mocks to avoid import cycles
type MockHTTPClient struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

type MockEntity struct {
	getLastAppliedOplogIndexFunc func(ctx context.Context) (int64, error)
	applyOplogEntriesFunc        func(ctx context.Context, entries []Oplog) error
	queryFunc                    func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	closeFunc                    func() error
}

func (m *MockEntity) GetLastAppliedOplogIndex(ctx context.Context) (int64, error) {
	return m.getLastAppliedOplogIndexFunc(ctx)
}

func (m *MockEntity) ApplyOplogEntries(ctx context.Context, entries []Oplog) error {
	return m.applyOplogEntriesFunc(ctx, entries)
}

func (m *MockEntity) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return m.queryFunc(ctx, query, args...)
}

func (m *MockEntity) Close() error {
	return m.closeFunc()
}

type MockDecompression struct {
	decompressStreamFunc func(reader io.Reader, firstChunk []byte, compression CompressionType, writer io.Writer) error
}

func (m *MockDecompression) DecompressStream(reader io.Reader, firstChunk []byte, compression CompressionType, writer io.Writer) error {
	return m.decompressStreamFunc(reader, firstChunk, compression, writer)
}

func setupTestClient(t *testing.T) (*Client, *MockHTTPClient, *MockDecompression) {
	mockHTTPClient := &MockHTTPClient{}
	mockDecompression := &MockDecompression{}

	config := Config{
		ServerURL:    "http://test-server",
		Directory:    "/test/dir",
		Collections:  []string{}, // Start with no entities to avoid initialization issues
		SyncInterval: time.Second,
		BatchSize:    10,
	}

	// Create client without entities initially
	client := &Client{
		config:        config,
		entities:      make(map[string]*entityState),
		colNames:      []string{"test-entity"},
		lastOplogIdx:  0, // Set to 0 by default
		httpClient:    mockHTTPClient,
		decompression: mockDecompression,
	}

	// Manually add the mock entity
	mockEntity := &MockEntity{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 0, nil
		},
		closeFunc: func() error { return nil },
	}

	client.entities["test-entity"] = &entityState{
		config: EntityConfig{Name: "test-entity"},
		entity: mockEntity,
	}

	return client, mockHTTPClient, mockDecompression
}

func TestNewClientWithDependencies_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	assert.NotNil(t, client)
	assert.Equal(t, int64(0), client.lastOplogIdx)
	assert.Len(t, client.entities, 1)
	assert.Contains(t, client.entities, "test-entity")
}

func TestNewClientWithDependencies_InvalidUpdateInterval(t *testing.T) {
	config := Config{
		ServerURL:    "",
		Directory:    "",
		Collections:  []string{},
		SyncInterval: 0, // Invalid - will be set to default
		BatchSize:    0, // Invalid - will be set to default
	}

	_, err := NewClient(context.Background(), config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ServerURL is required")
}

func TestInitializeLastOplogIdx_MultipleEntities(t *testing.T) {
	client := &Client{
		entities: make(map[string]*entityState),
	}

	// Create mock entities with different last oplog indices
	mockEnt1 := &MockEntity{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 10, nil
		},
	}

	mockEnt2 := &MockEntity{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 5, nil
		},
	}

	mockEnt3 := &MockEntity{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 15, nil
		},
	}

	client.entities["ent1"] = &entityState{entity: mockEnt1}
	client.entities["ent2"] = &entityState{entity: mockEnt2}
	client.entities["ent3"] = &entityState{entity: mockEnt3}

	err := client.initializeLastOplogIdx(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(5), client.lastOplogIdx, "Should use the minimum oplog index")
}

func TestInitializeLastOplogIdx_NoEntities(t *testing.T) {
	client := &Client{
		entities: make(map[string]*entityState),
	}

	err := client.initializeLastOplogIdx(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(0), client.lastOplogIdx, "Should default to 0 when no entities")
}

func TestInitializeLastOplogIdx_Error(t *testing.T) {
	client := &Client{
		entities: make(map[string]*entityState),
	}

	mockEnt := &MockEntity{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 0, errors.New("test error")
		},
	}

	client.entities["ent1"] = &entityState{entity: mockEnt}

	err := client.initializeLastOplogIdx(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get last applied oplog index")
}

func TestSyncToLatest_Success(t *testing.T) {
	client, mockHTTPClient, _ := setupTestClient(t)

	// Mock the HTTP response for oplogs
	mockHTTPClient.doFunc = func(req *http.Request) (*http.Response, error) {
		// Return empty oplog response to simulate no new entries
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
		}, nil
	}

	err := client.SyncOnce(context.Background())
	assert.NoError(t, err)
}

func TestSyncToLatest_ErrorGettingEntries(t *testing.T) {
	client, mockHTTPClient, _ := setupTestClient(t)

	// Mock HTTP error
	mockHTTPClient.doFunc = func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("network error")
	}

	err := client.SyncOnce(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get oplog entries")
}

func TestQuery_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	// Mock successful query
	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.queryFunc = func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return nil, nil // Return nil rows for simplicity
	}

	_, err := client.Query(context.Background(), "test-entity", "SELECT * FROM data")
	assert.NoError(t, err)
}

func TestQuery_EntityNotFound(t *testing.T) {
	client, _, _ := setupTestClient(t)

	_, err := client.Query(context.Background(), "non-existent", "SELECT * FROM data")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "entity 'non-existent' not found")
}

func TestAddEntity_Success(t *testing.T) {
	client, mockHTTPClient, _ := setupTestClient(t)

	shape := &Shape{
		IdColumn: IdColumn{Path: "id", Type: IdTypeString},
		Columns:  []Column{{Name: "test-entity", Type: DataTypeString, Path: "name"}},
	}

	// Mock successful HTTP response
	mockHTTPClient.doFunc = func(req *http.Request) (*http.Response, error) {
		entity := Entity{ID: 1, Name: "test-entity"}
		body, _ := json.Marshal(entity)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	}

	_, err := client.AddEntity(context.Background(), "test-entity", shape)
	assert.NoError(t, err)
}

func TestAddEntity_NilShape(t *testing.T) {
	client, _, _ := setupTestClient(t)

	_, err := client.AddEntity(context.Background(), "test-entity", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "shape cannot be nil")
}

func TestRemoveEntity_Success(t *testing.T) {
	client, mockHTTPClient, _ := setupTestClient(t)

	// Mock successful HTTP response
	mockHTTPClient.doFunc = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(""))),
		}, nil
	}

	err := client.RemoveEntity(context.Background(), "test-entity")
	assert.NoError(t, err)
}

func TestStartSync_Success(t *testing.T) {
	client, mockHTTPClient, _ := setupTestClient(t)

	// Mock HTTP response for sync
	mockHTTPClient.doFunc = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
		}, nil
	}

	err := client.StartSync()
	assert.NoError(t, err)

	// Clean up
	client.StopSync()
}

func TestStartSync_AlreadyStarted(t *testing.T) {
	client, mockHTTPClient, _ := setupTestClient(t)

	// Mock HTTP response
	mockHTTPClient.doFunc = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
		}, nil
	}

	err := client.StartSync()
	assert.NoError(t, err)

	// Try to start again
	err = client.StartSync()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sync is already started")

	// Clean up
	client.StopSync()
}

func TestClose_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	err := client.Close()
	assert.NoError(t, err)
}

func TestApplyOplogEntries_Success(t *testing.T) {
	client, _, _ := setupTestClient(t)

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {
		return nil
	}

	entries := []Oplog{{ID: 1, Entity: "test-entity"}}
	_, err := client.applyOplogEntries(context.Background(), entries)
	assert.NoError(t, err)
}

func TestApplyOplogEntries_WithUnknownEntity(t *testing.T) {
	client, _, _ := setupTestClient(t)

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {
		return nil
	}

	entries := []Oplog{
		{ID: 1, Entity: "test-entity"},
		{ID: 2, Entity: "unknown-entity"}, // This should be ignored
	}
	_, err := client.applyOplogEntries(context.Background(), entries)
	assert.NoError(t, err)
}
