// Package emcache provides a client for syncing data from EmCache server to local SQLite databases.
//
// The client handles automatic synchronization of entities from the server,
// maintaining local SQLite databases that can be queried efficiently.
//
// Basic usage:
//
//	client, err := emcache.NewClient(ctx, emcache.Config{
//	    ServerURL: "http://localhost:8080",
//	    Directory: "./cache",
//	    Collections: []string{"users", "products"},
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Start automatic sync
//	if err := client.StartSync(); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Query data
//	rows, err := client.Query(ctx, "users", "SELECT * FROM data WHERE age > ?", 18)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer rows.Close()
package emcache

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	_ "github.com/mattn/go-sqlite3"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
)

// CompressionType represents compression types
type CompressionType int

const (
	CompressionNone CompressionType = iota
	CompressionZstd
	CompressionGzip
)

// Operation types for oplog entries
type Operation string

const (
	OperationUpsert Operation = "upsert"
	OperationDelete Operation = "delete"
)

// Data types for shape columns
type DataType string

const (
	DataTypeJsonb   DataType = "jsonb"
	DataTypeAny     DataType = "any"
	DataTypeBool    DataType = "bool"
	DataTypeNumber  DataType = "number"
	DataTypeInteger DataType = "integer"
	DataTypeString  DataType = "string"
	DataTypeBytes   DataType = "bytes"
)

// ID column types
type IdType string

const (
	IdTypeString IdType = "string"
	IdTypeNumber IdType = "number"
)

type IdColumn struct {
	Path string `json:"path"`
	Type IdType `json:"type"`
}

type Column struct {
	Name string   `json:"name"`
	Type DataType `json:"type"`
	Path string   `json:"path"`
}

type Index struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
}

type Shape struct {
	IdColumn IdColumn `json:"id_column"`
	Columns  []Column `json:"columns"`
	Indexes  []Index  `json:"indexes"`
}

type Entity struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Client    string    `json:"client"`
	Source    string    `json:"source"`
	Shape     Shape     `json:"shape"`
	CreatedAt time.Time `json:"created_at"`
}

type OplogStatus struct {
	MaxID int64 `json:"max_id"`
}

type Oplog struct {
	ID        int64     `json:"id"`
	Operation Operation `json:"operation"`
	DocID     string    `json:"doc_id"`
	Entity    string    `json:"entity"`
	Data      []any     `json:"data"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateEntityRequest struct {
	Name   string `json:"name"`
	Client string `json:"client"`
	Source string `json:"source"`
	Shape  Shape  `json:"shape"`
}

// Config holds the configuration for the EmCache client.
type Config struct {
	// ServerURL is the base URL of the EmCache server
	ServerURL string

	// Directory is the local directory where SQLite databases will be stored
	Directory string

	// Collections is the list of entity names to sync
	Collections []string

	// SyncInterval is how often to check for updates (default: 30 seconds)
	SyncInterval time.Duration

	// BatchSize is the number of oplog entries to process at once (default: 1000)
	BatchSize int

	// Logger is the logger to use for logging
	Logger *slog.Logger
}

type EntityConfig struct {
	Name string
}

type entityState struct {
	config EntityConfig
	entity LocalCache
}

type Client struct {
	config       Config
	entities     map[string]*entityState
	colNames     []string
	lastOplogIdx int64
	cancelFunc   context.CancelFunc
	stopWg       sync.WaitGroup

	// Internal dependencies
	httpClient *http.Client
	logger     *slog.Logger
}

// NewClient creates a new EmCache client with the given configuration.
func NewClient(ctx context.Context, config Config) (*Client, error) {
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	if config.SyncInterval == 0 {
		config.SyncInterval = 30 * time.Second
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1000
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	client := &Client{
		config:     config,
		entities:   make(map[string]*entityState),
		colNames:   config.Collections,
		httpClient: httpClient,
		logger:     config.Logger,
	}

	if err := client.initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize client: %w", err)
	}

	return client, nil
}

// DB executes a function against the specified entity's database.
func (c *Client) DB(ctx context.Context, entity string) (*sql.DB, error) {
	state, exists := c.entities[entity]
	if !exists {
		return nil, fmt.Errorf("entity '%s' not found", entity)
	}

	if state.entity == nil {
		return nil, fmt.Errorf("entity '%s' is not initialized", entity)
	}

	return state.entity.DB(), nil
}

// StartSync begins automatic synchronization with the server.
// It starts a background goroutine that periodically fetches and applies updates.
func (c *Client) StartSync() error {
	if c.cancelFunc != nil {
		return fmt.Errorf("sync is already started")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancelFunc = cancel
	c.stopWg.Add(1)

	go c.syncLoop(ctx)
	return nil
}

// StopSync stops the automatic synchronization and waits for it to complete.
func (c *Client) StopSync() {
	if c.cancelFunc != nil {
		c.cancelFunc()
		c.cancelFunc = nil
	}
	c.stopWg.Wait()
}

// SyncOnce performs a single synchronization cycle, fetching and applying updates.
func (c *Client) SyncOnce(ctx context.Context) error {
	oplogStatus, err := c.getOplogStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get oplog status: %w", err)
	}
	return c.syncToLatest(ctx, oplogStatus.MaxID)
}

func (c *Client) syncToLatest(ctx context.Context, maxOplogID int64) error {
	totalOplogs := maxOplogID - c.lastOplogIdx
	if totalOplogs <= 0 {
		slog.Info("No oplogs to sync", "min_id", c.lastOplogIdx, "max_id", maxOplogID)
		return nil
	}

	batchCount := int((totalOplogs + int64(c.config.BatchSize) - 1) / int64(c.config.BatchSize))
	slog.Info("Sync plan", "min_id", c.lastOplogIdx, "max_id", maxOplogID, "total_oplogs", totalOplogs, "batch_count", batchCount)

	_, err := c.syncBatches(ctx, batchCount)
	if err != nil {
		return fmt.Errorf("failed to sync batches: %w", err)
	}
	return nil
}

func (c *Client) AddEntity(ctx context.Context, name string, shape *Shape) (*Entity, error) {
	if shape == nil {
		return nil, fmt.Errorf("shape cannot be nil")
	}

	reqBody := CreateEntityRequest{
		Name:   name,
		Client: "go-client",
		Source: "unknown",
		Shape:  *shape,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/api/entities", c.config.ServerURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create entity: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to create entity: HTTP %d", resp.StatusCode)
	}

	var entity Entity
	if err := json.NewDecoder(resp.Body).Decode(&entity); err != nil {
		return nil, fmt.Errorf("failed to decode entity response: %w", err)
	}

	return &entity, nil
}

// RemoveEntity removes an entity from the server.
func (c *Client) RemoveEntity(ctx context.Context, name string) error {
	url := fmt.Sprintf("%s/api/entities/%s", c.config.ServerURL, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete entity: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete entity: HTTP %d", resp.StatusCode)
	}

	return nil
}

// GetEntities retrieves information about available entities from the server.
func (c *Client) GetEntities(ctx context.Context) ([]Entity, error) {
	url := fmt.Sprintf("%s/api/entities", c.config.ServerURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get entities: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get entities: HTTP %d", resp.StatusCode)
	}

	var entities []Entity
	if err := json.NewDecoder(resp.Body).Decode(&entities); err != nil {
		return nil, fmt.Errorf("failed to decode entities response: %w", err)
	}

	return entities, nil
}

// Close closes the client, stopping synchronization and closing all database connections.
func (c *Client) Close() error {
	c.StopSync()
	return c.closeConnections()
}

// closeConnections closes all entity database connections
func (c *Client) closeConnections() error {
	var firstErr error
	for name, state := range c.entities {
		if state.entity != nil {
			if err := state.entity.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("failed to close entity %s: %w", name, err)
			}
		}
	}
	return firstErr
}

// getEntitiesFromServer retrieves entities from the server
func (c *Client) getEntitiesFromServer(ctx context.Context, collections []string) ([]Entity, error) {
	url := fmt.Sprintf("%s/api/entities", c.config.ServerURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get entities: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get entities: HTTP %d", resp.StatusCode)
	}

	var entities []Entity
	if err := json.NewDecoder(resp.Body).Decode(&entities); err != nil {
		return nil, fmt.Errorf("failed to decode entities response: %w", err)
	}

	// Filter entities by requested collection names if provided
	if len(collections) > 0 {
		collectionSet := make(map[string]bool)
		for _, name := range collections {
			collectionSet[name] = true
		}

		filtered := make([]Entity, 0, len(entities))
		for _, entity := range entities {
			if collectionSet[entity.Name] {
				filtered = append(filtered, entity)
			}
		}
		return filtered, nil
	}

	return entities, nil
}

// initializeEntity initializes a single entity
func (c *Client) initializeEntity(ctx context.Context, entityName string, entitiesData []Entity) (*entityState, error) {
	entityConfig := EntityConfig{Name: entityName}
	return c.addEntityInternal(ctx, entityConfig, entitiesData)
}

// validateConfig validates the client configuration
func validateConfig(config Config) error {
	if config.ServerURL == "" {
		return fmt.Errorf("server URL is required")
	}
	if config.Directory == "" {
		return fmt.Errorf("directory is required")
	}
	if len(config.Collections) == 0 {
		return fmt.Errorf("at least one collection must be specified")
	}
	return nil
}

// initialize sets up the client's internal state
func (c *Client) initialize(ctx context.Context) error {
	if err := os.MkdirAll(c.config.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", c.config.Directory, err)
	}

	entitiesData, err := c.getEntitiesFromServer(ctx, c.colNames)
	if err != nil {
		return fmt.Errorf("failed to get entities from server: %w", err)
	}

	// Initialize entities in parallel
	results := lop.Map(c.config.Collections, func(entityName string, _ int) lo.Tuple2[*entityState, error] {
		state, err := c.initializeEntity(ctx, entityName, entitiesData)
		return lo.T2(state, err)
	})

	// Check for errors and register entities
	for _, result := range results {
		if result.B != nil {
			_ = c.Close()
			return fmt.Errorf("failed to initialize entity: %w", result.B)
		}
		c.entities[result.A.config.Name] = result.A
	}

	// Get the max oplog ID from the server
	oplogStatus, err := c.getOplogStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get oplog status: %w", err)
	}

	// Initialize the last oplog index using the new workflow
	if err := c.initializeLastOplogIdx(ctx, oplogStatus.MaxID); err != nil {
		_ = c.Close()
		return fmt.Errorf("failed to initialize last oplog index: %w", err)
	}

	// Perform initial sync using the new workflow
	if err := c.syncToLatest(ctx, oplogStatus.MaxID); err != nil {
		slog.Warn("Initial sync failed", "error", err)
	}

	return nil
}

// syncLoop runs the continuous synchronization process
func (c *Client) syncLoop(ctx context.Context) {
	defer c.stopWg.Done()

	ticker := time.NewTimer(c.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			more, err := c.syncBatches(ctx, 1)
			if err != nil {
				c.logger.Error("Failed to sync entities", "error", err)
			}
			if !more {
				ticker.Reset(c.config.SyncInterval)
			} else {
				ticker.Reset(0)
			}
		}
	}
}

// applyOplogEntries applies a batch of oplog entries to the appropriate entity databases.
// It returns the index of the last successfully applied entry across all entities.
func (c *Client) applyOplogEntries(ctx context.Context, entries []Oplog) (int64, error) {
	if len(entries) == 0 {
		return c.lastOplogIdx, nil
	}

	entriesByEntity := make(map[string][]Oplog)
	var maxProcessedID int64 = c.lastOplogIdx

	for _, entry := range entries {
		entriesByEntity[entry.Entity] = append(entriesByEntity[entry.Entity], entry)
		if entry.ID > maxProcessedID {
			maxProcessedID = entry.ID
		}
	}

	for entityName, entityEntries := range entriesByEntity {
		state, ok := c.entities[entityName]
		if !ok {
			slog.Warn("Entity not found, skipping oplogs", "entity", entityName)
			continue
		}
		if state.entity == nil {
			slog.Warn("Entity not initialized, skipping oplogs", "entity", entityName)
			continue
		}

		err := state.entity.ApplyOplogEntries(ctx, entityEntries)
		if err != nil {
			return c.lastOplogIdx, fmt.Errorf("failed to apply oplog entries to entity '%s': %w", entityName, err)
		}
	}

	return maxProcessedID, nil
}

// StopUpdates signals all running update goroutines to stop and waits for them to finish.
func (c *Client) StopUpdates() {
	if c.cancelFunc != nil {
		c.cancelFunc()
		c.cancelFunc = nil
	}

	c.stopWg.Wait()
}

// initializeLastOplogIdx determines the lowest offset from all databases to start sync correctly
func (c *Client) initializeLastOplogIdx(ctx context.Context, maxOplogID int64) error {
	minIndex := maxOplogID

	for entityName, state := range c.entities {
		idx, err := state.entity.GetLastAppliedOplogIndex(ctx)
		if err != nil {
			return fmt.Errorf("failed to get last applied oplog index for entity '%s': %w", entityName, err)
		}

		if idx < minIndex {
			minIndex = idx
		}
	}
	c.lastOplogIdx = minIndex
	return nil
}

func (c *Client) addEntityInternal(ctx context.Context, entityConfig EntityConfig, entitiesData []Entity) (*entityState, error) {
	entityDetails := c.entityDetails(entitiesData, entityConfig)
	if entityDetails == nil {
		return nil, fmt.Errorf("entity '%s' not found on server", entityConfig.Name)
	}

	dbPath := filepath.Join(c.config.Directory, entityConfig.Name+".db")

	// Download the database if it doesn't exist
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		lastOplogIdx, err := c.downloadDb(ctx, dbPath, entityConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to download database for entity '%s': %w", entityConfig.Name, err)
		}
		_ = lastOplogIdx
	}

	// Open the database
	db, err := openSQLiteDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database for entity '%s': %w", entityConfig.Name, err)
	}

	// Create the SQLite entity
	entity := &localCache{
		db:      db,
		details: entityDetails,
	}

	return &entityState{
		config: entityConfig,
		entity: entity,
	}, nil
}

func (*Client) entityDetails(serverEntities []Entity, entityConfig EntityConfig) *Entity {
	for _, entity := range serverEntities {
		if entity.Name == entityConfig.Name {
			return &entity
		}
	}
	return nil
}

func (c *Client) downloadDb(ctx context.Context, dbPath string, entityConfig EntityConfig) (int64, error) {
	file, err := os.Create(dbPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create database file: %w", err)
	}
	defer file.Close()

	return c.downloadDbForEntity(ctx, entityConfig.Name, file)
}

func (c *Client) downloadDbForEntity(ctx context.Context, entityName string, writer io.WriteCloser) (int64, error) {
	defer writer.Close()

	url := fmt.Sprintf("%s/api/entities/%s/snapshot", c.config.ServerURL, entityName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to download database: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to download database: HTTP %d", resp.StatusCode)
	}

	compression := CompressionNone
	if contentEncoding := resp.Header.Get("Content-Encoding"); contentEncoding != "" {
		switch contentEncoding {
		case "gzip":
			compression = CompressionGzip
		case "zstd":
			compression = CompressionZstd
		default:
			compression = CompressionNone
		}
	}

	return 0, decompressStream(resp.Body, compression, writer)
}

func (c *Client) getOplogEntries(ctx context.Context, entityNames []string, afterIndex int64) ([]Oplog, error) {
	url := fmt.Sprintf("%s/api/oplogs", c.config.ServerURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	q := req.URL.Query()
	q.Set("after", strconv.FormatInt(afterIndex, 10))
	q.Set("limit", strconv.Itoa(c.config.BatchSize))
	for _, name := range entityNames {
		q.Add("entity", name)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get oplog entries: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get oplog entries: HTTP %d", resp.StatusCode)
	}

	var entries []Oplog
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to decode oplog entries: %w", err)
	}

	return entries, nil
}

func (c *Client) getOplogStatus(ctx context.Context) (*OplogStatus, error) {
	url := fmt.Sprintf("%s/api/oplogs/status", c.config.ServerURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get oplog status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get oplog status: HTTP %d", resp.StatusCode)
	}

	var status OplogStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode oplog status: %w", err)
	}

	return &status, nil
}

func (c *Client) syncBatches(ctx context.Context, batchCount int) (bool, error) {
	currentOffset := c.lastOplogIdx

	for i := 0; i < batchCount; i++ {
		c.logger.Info("Syncing batch", "batch", i+1, "of", batchCount, "from", currentOffset)

		entries, err := c.getOplogEntries(ctx, c.colNames, currentOffset)
		if err != nil {
			return false, fmt.Errorf("failed to get oplog entries for batch %d: %w", i+1, err)
		}

		lastAppliedIdx, err := c.applyOplogEntries(ctx, entries)
		if err != nil {
			return false, fmt.Errorf("failed to apply oplog entries for batch %d: %w", i+1, err)
		}

		currentOffset = lastAppliedIdx
		c.lastOplogIdx = lastAppliedIdx

		// If we got fewer entries than batch size, we're done
		if len(entries) < c.config.BatchSize {
			c.logger.Info("Sync complete", "last_applied_id", lastAppliedIdx)
			return false, nil
		}
	}

	return true, nil
}

func decompressStream(
	reader io.Reader,
	compression CompressionType,
	writer io.Writer,
) error {
	switch compression {
	case CompressionZstd:
		return decompressZstdStream(reader, writer)
	case CompressionGzip:
		return decompressGzipStream(reader, writer)
	case CompressionNone:
		_, err := io.Copy(writer, reader)
		if err != nil {
			return fmt.Errorf("failed to write chunks: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression type %d", compression)
	}
	return nil
}

func decompressGzipStream(reader io.Reader, writer io.Writer) error {
	gr, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gr.Close()

	_, err = io.Copy(writer, gr)
	return err
}

func decompressZstdStream(reader io.Reader, writer io.Writer) error {
	zr, err := zstd.NewReader(reader)
	if err != nil {
		return fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer zr.Close()

	_, err = io.Copy(writer, zr)
	return err
}
