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

func (m *MockEntity) GetLastAppliedOplogIndex(ctx context.Context) (int64, error) {
	return m.getLastAppliedOplogIndexFunc(ctx)
}

func (m *MockEntity) ApplyOplogEntries(ctx context.Context, entries []Oplog) error {
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
	}

	client := &Client{
		config:       config,
		entities:     make(map[string]*entityState),
		colNames:     []string{"test-entity"},
		lastOplogIdx: 0,
		httpClient:   &http.Client{Transport: mockTransport},
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

	_, err := NewClient(context.Background(), config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server URL is required")
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
	client, mockTransport := setupTestClient()

	mockEntity := client.entities["test-entity"].entity.(*MockEntity)
	var appliedEntries []Oplog
	mockEntity.applyOplogEntriesFunc = func(ctx context.Context, entries []Oplog) error {
		appliedEntries = append(appliedEntries, entries...)
		return nil
	}

	requestCount := 0
	mockTransport.doFunc = func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/oplogs/status":

			status := OplogStatus{MaxID: 65}
			statusBytes, _ := json.Marshal(status)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(statusBytes)),
			}, nil
		case "/api/oplogs":
			requestCount++
			if requestCount == 1 {

				entries := []Oplog{
					{ID: 51, Entity: "test-entity", Operation: OperationUpsert, DocID: "doc1"},
					{ID: 52, Entity: "test-entity", Operation: OperationUpsert, DocID: "doc2"},
					{ID: 53, Entity: "test-entity", Operation: OperationUpsert, DocID: "doc3"},
				}
				entriesBytes, _ := json.Marshal(entries)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(entriesBytes)),
				}, nil
			} else {

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
				}, nil
			}
		default:

			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte("[]"))),
			}, nil
		}
	}

	err := client.SyncOnce(context.Background())
	assert.NoError(t, err)

	assert.Len(t, appliedEntries, 3)
	assert.Equal(t, int64(51), appliedEntries[0].ID)
	assert.Equal(t, int64(52), appliedEntries[1].ID)
	assert.Equal(t, int64(53), appliedEntries[2].ID)
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
		IdColumn: IdColumn{Path: "id", Type: IdTypeString},
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

	_, err := client.AddEntity(context.Background(), "test-entity", shape)
	assert.NoError(t, err)
}

func TestAddEntity_NilShape(t *testing.T) {
	client, _ := setupTestClient()

	_, err := client.AddEntity(context.Background(), "test-entity", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "shape cannot be nil")
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
				Body:       io.NopCloser(bytes.NewReader(statusBytes)),
			}, nil
		default:

			return &http.Response{
				StatusCode: http.StatusOK,
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
	assert.Error(t, err)

	err = client.SyncOnce(context.Background())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 2)
}

func TestCompressionSupport(t *testing.T) {

	testData := []byte("Hello, World! This is test data for compression.")

	var buf bytes.Buffer
	err := decompressStream(bytes.NewReader(testData), CompressionNone, &buf)
	assert.NoError(t, err)

	expected := "Hello, World! This is test data for compression."
	assert.Equal(t, expected, buf.String())
}

func TestDecompressionNoneType(t *testing.T) {
	testData := []byte("uncompressed data")
	reader := bytes.NewReader(testData)

	var buf bytes.Buffer
	err := decompressStream(reader, CompressionNone, &buf)
	assert.NoError(t, err)

	expected := "uncompressed data"
	assert.Equal(t, expected, buf.String())
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
