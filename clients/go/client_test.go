package emcache

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"resty.dev/v3"
)

type MockHTTPClient struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

type MockRoundTripper struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

type MockEntity struct {
	getLastAppliedOplogIndexFunc func(ctx context.Context) (int64, error)
	applyOplogEntriesFunc        func(ctx context.Context, entries []Oplog) error
	dbFunc                       func() *sql.DB
	closeFunc                    func() error
}

func (m *MockEntity) GetMaxOplogId(ctx context.Context) (int64, error) {
	return m.getLastAppliedOplogIndexFunc(ctx)
}

func (m *MockEntity) ApplyOplogs(ctx context.Context, entries []Oplog) error {
	return m.applyOplogEntriesFunc(ctx, entries)
}

func (m *MockEntity) DB() *sql.DB {
	return m.dbFunc()
}

func (m *MockEntity) Close() error {
	return m.closeFunc()
}

func setupTestClient() (*Client, *MockRoundTripper) {
	mockTransport := &MockRoundTripper{}

	config := Config{
		ServerURL:    "http://test-server",
		Directory:    "/test/dir",
		Collections:  []string{},
		SyncInterval: time.Second,
		BatchSize:    10,
		Logger:       slog.Default(),
	}

	// Create a resty client with the mock transport
	restyClient := resty.New()
	restyClient.SetTransport(mockTransport)

	client := &Client{
		config:       config,
		entities:     make(map[string]*entityState),
		colNames:     []string{"test-entity"},
		lastOplogIdx: 0,
		httpClient:   restyClient,
		logger:       slog.Default(),
	}

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

	return client, mockTransport
}

func setupTestClientWithMock(mockFunc func(req *http.Request) (*http.Response, error)) (*Client, *MockRoundTripper) {
	mockTransport := &MockRoundTripper{
		doFunc: mockFunc,
	}

	config := Config{
		ServerURL:    "http://test-server",
		Directory:    "/test/dir",
		Collections:  []string{"test-entity"},
		SyncInterval: time.Second,
		BatchSize:    10,
		Logger:       slog.Default(),
	}

	// Create a resty client with the mock transport
	restyClient := resty.New()
	restyClient.SetTransport(mockTransport)

	client := &Client{
		config:       config,
		entities:     make(map[string]*entityState),
		colNames:     []string{"test-entity"},
		lastOplogIdx: 50,
		httpClient:   restyClient,
		logger:       slog.Default(),
	}

	mockEntity := &MockEntity{
		getLastAppliedOplogIndexFunc: func(ctx context.Context) (int64, error) {
			return 50, nil
		},
		closeFunc: func() error { return nil },
	}

	client.entities["test-entity"] = &entityState{
		config: EntityConfig{Name: "test-entity"},
		entity: mockEntity,
	}

	return client, mockTransport
}

func TestNewClientWithDependencies_Success(t *testing.T) {
	client, _ := setupTestClient()

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
		SyncInterval: 0,
		BatchSize:    0,
	}

	client := NewClient(context.Background(), config)
	assert.NotNil(t, client)
	assert.Error(t, client.validateConfig())
}

func TestInitializeLastOplogIdx_MultipleEntities(t *testing.T) {
	client := &Client{
		entities: make(map[string]*entityState),
	}

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

	err := client.initializeLastOplogIdx(context.Background(), 100)
	require.NoError(t, err)

	assert.Equal(t, int64(5), client.lastOplogIdx, "Should use the minimum oplog index")
}

func TestInitializeLastOplogIdx_NoEntities(t *testing.T) {
	client := &Client{
		entities: make(map[string]*entityState),
	}

	err := client.initializeLastOplogIdx(context.Background(), 100)
	require.NoError(t, err)

	assert.Equal(t, int64(100), client.lastOplogIdx, "Should use maxOplogID when no entities")
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

	err := client.initializeLastOplogIdx(context.Background(), 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get last applied oplog index")
}

func TestSyncToLatest_Success(t *testing.T) {
	client, mockTransport := setupTestClient()

	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/oplogs/status":

			status := OplogStatus{MaxID: 0}
			statusBytes, _ := json.Marshal(status)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(statusBytes)),
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}
	}

	err := client.SyncOnce(context.Background())
	assert.NoError(t, err)
}

func TestSyncWithNewWorkflow_Success(t *testing.T) {
	client, mockTransport := setupTestClient()

	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/oplogs/status":

			status := OplogStatus{MaxID: 100}
			statusBytes, _ := json.Marshal(status)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(statusBytes)),
			}, nil
		case "/api/oplogs":

			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		default:

			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}
	}

	err := client.SyncOnce(context.Background())
	assert.NoError(t, err)
}

func TestSyncWithNewWorkflow_BatchSynchronization(t *testing.T) {
	requestCount := 0
	mockFunc := func(req *http.Request) (*http.Response, error) {
		t.Logf("Mock called with path: %s, method: %s", req.URL.Path, req.Method)
		switch req.URL.Path {
		case "/api/oplogs/status":
			// Return max ID of 65 so we have 15 oplogs to sync (51-65)
			status := OplogStatus{MaxID: 65}
			statusBytes, _ := json.Marshal(status)
			t.Logf("Returning oplog status with MaxID: %d, JSON: %s", status.MaxID, string(statusBytes))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(statusBytes)),
			}, nil
		case "/api/oplogs":
			requestCount++
			t.Logf("Oplog request #%d", requestCount)
			if requestCount == 1 {
				// First batch returns 3 entries (51, 52, 53)
				entries := []Oplog{
					{ID: 51, Entity: "test-entity", Operation: OperationUpsert, DocID: "doc1"},
					{ID: 52, Entity: "test-entity", Operation: OperationUpsert, DocID: "doc2"},
					{ID: 53, Entity: "test-entity", Operation: OperationUpsert, DocID: "doc3"},
				}
				entriesBytes, _ := json.Marshal(entries)
				t.Logf("Returning %d oplog entries", len(entries))
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader(entriesBytes)),
				}, nil
			} else {
				// Subsequent requests return empty to end sync
				t.Logf("Returning empty oplog entries")
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
				}, nil
			}
		default:
			// Default response for other requests
			t.Logf("Unknown path: %s, returning empty response", req.URL.Path)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}
	}

	client, _ := setupTestClientWithMock(mockFunc)

	var appliedEntries []Oplog
	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {
		t.Logf("Mock entity applying %d entries", len(entries))
		appliedEntries = append(appliedEntries, entries...)
		return nil
	}

	t.Logf("Starting SyncOnce with client lastOplogIdx: %d", client.lastOplogIdx)
	err := client.SyncOnce(context.Background())
	assert.NoError(t, err)

	// Verify that the 3 entries were applied
	t.Logf("Applied entries count: %d", len(appliedEntries))
	assert.Len(t, appliedEntries, 3)
	if len(appliedEntries) >= 3 {
		assert.Equal(t, int64(51), appliedEntries[0].ID)
		assert.Equal(t, int64(52), appliedEntries[1].ID)
		assert.Equal(t, int64(53), appliedEntries[2].ID)
	}
}

func TestSyncToLatest_ErrorGettingEntries(t *testing.T) {
	client, mockTransport := setupTestClient()

	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("network error")
	}

	err := client.SyncOnce(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get oplog status")
}

func TestQuery_Success(t *testing.T) {
	client, _ := setupTestClient()

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.dbFunc = func() *sql.DB {
		return nil
	}

	_, err := client.DB(context.Background(), "test-entity")
	assert.NoError(t, err)
}

func TestQuery_EntityNotFound(t *testing.T) {
	client, _ := setupTestClient()

	_, err := client.DB(context.Background(), "non-existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "entity 'non-existent' not found")
}

func TestAddEntity_Success(t *testing.T) {
	client, mockTransport := setupTestClient()

	shape := &Shape{
		IdColumn: IdColumn{Type: IdTypeString},
		Columns:  []Column{{Name: "test-entity", Type: DataTypeString, Path: "name"}},
	}

	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		entity := Entity{ID: 1, Name: "test-entity"}
		body, _ := json.Marshal(entity)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	}

	_, err := client.CreateEntity(context.Background(), CreateEntityRequest{
		Name:   "test-entity",
		Client: "main",
		Source: "test-entity",
		Shape:  *shape,
	})
	assert.NoError(t, err)
}

func TestRemoveEntity_Success(t *testing.T) {
	client, mockTransport := setupTestClient()

	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(""))),
		}, nil
	}

	err := client.RemoveEntity(context.Background(), "test-entity")
	assert.NoError(t, err)
}

func TestStartSync_Success(t *testing.T) {
	client, mockTransport := setupTestClient()

	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
		}, nil
	}

	err := client.StartSync()
	assert.NoError(t, err)

	client.StopSync()
}

func TestStartSync_AlreadyStarted(t *testing.T) {
	client, mockTransport := setupTestClient()

	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
		}, nil
	}

	err := client.StartSync()
	assert.NoError(t, err)

	err = client.StartSync()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sync is already started")

	client.StopSync()
}

func TestClose_Success(t *testing.T) {
	client, _ := setupTestClient()

	err := client.Close()
	assert.NoError(t, err)
}

func TestApplyOplogEntries_Success(t *testing.T) {
	client, _ := setupTestClient()

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {
		return nil
	}

	entries := []Oplog{{ID: 1, Entity: "test-entity"}}
	_, err := client.applyOplogEntries(context.Background(), entries)
	assert.NoError(t, err)
}

func TestApplyOplogEntries_WithUnknownEntity(t *testing.T) {
	client, _ := setupTestClient()

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {
		return nil
	}

	entries := []Oplog{
		{ID: 1, Entity: "test-entity"},
		{ID: 2, Entity: "unknown-entity"},
	}
	_, err := client.applyOplogEntries(context.Background(), entries)
	assert.NoError(t, err)
}

func TestConcurrentOperations(t *testing.T) {
	client, mockTransport := setupTestClient()

	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/oplogs/status":
			status := OplogStatus{MaxID: 0}
			statusBytes, _ := json.Marshal(status)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(statusBytes)),
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.SyncOnce(context.Background())
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		assert.NoError(t, err)
	}
}

func TestOplogProcessingConsistentWithBackend(t *testing.T) {
	client, _ := setupTestClient()

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	var processedEntries []Oplog

	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {
		processedEntries = append(processedEntries, entries...)
		return nil
	}

	entries := []Oplog{
		{ID: 1, Entity: "test-entity", Operation: OperationUpsert, DocID: "doc1"},
		{ID: 3, Entity: "test-entity", Operation: OperationUpsert, DocID: "doc3"},
		{ID: 2, Entity: "test-entity", Operation: OperationDelete, DocID: "doc2"},
	}

	_, err := client.applyOplogEntries(context.Background(), entries)
	assert.NoError(t, err)

	assert.Len(t, processedEntries, 3)
	assert.Equal(t, entries, processedEntries)
}

func TestErrorRecoveryAndRetry(t *testing.T) {
	client, mockTransport := setupTestClient()

	callCount := 0
	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		callCount++
		if callCount == 1 {
			return nil, errors.New("network error")
		}

		switch req.URL.Path {
		case "/api/oplogs/status":
			status := OplogStatus{MaxID: 0}
			statusBytes, _ := json.Marshal(status)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(statusBytes)),
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}
	}

	err := client.SyncOnce(context.Background())
	assert.Error(t, err)

	err = client.SyncOnce(context.Background())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 2)
}

func TestBatchProcessingLimits(t *testing.T) {
	client, _ := setupTestClient()

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {

		assert.LessOrEqual(t, len(entries), client.config.BatchSize)
		return nil
	}

	entries := make([]Oplog, client.config.BatchSize)
	for i := 0; i < client.config.BatchSize; i++ {
		entries[i] = Oplog{
			ID:        int64(i + 1),
			Entity:    "test-entity",
			Operation: OperationUpsert,
			DocID:     fmt.Sprintf("doc%d", i),
		}
	}

	_, err := client.applyOplogEntries(context.Background(), entries)
	assert.NoError(t, err)
}

func TestMetadataConsistency(t *testing.T) {
	client, _ := setupTestClient()

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {
		return nil
	}

	entries := []Oplog{
		{ID: 5, Entity: "test-entity"},
		{ID: 10, Entity: "test-entity"},
		{ID: 3, Entity: "test-entity"},
	}

	lastIdx, err := client.applyOplogEntries(context.Background(), entries)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), lastIdx, "Should return the maximum ID processed")
}
