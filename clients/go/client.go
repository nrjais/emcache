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
//	    Entities: []string{"users", "products"},
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
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"resty.dev/v3"
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

type ErrorResponse struct {
	Errors []string `json:"errors"`
	Reason string   `json:"reason"`
}

func (e ErrorResponse) Error() string {
	if len(e.Errors) > 0 {
		return fmt.Sprintf("errors: %s", strings.Join(e.Errors, ", "))
	}
	return fmt.Sprintf("reason: %s", e.Reason)
}

// Config holds the configuration for the EmCache client.
type Config struct {
	// ServerURL is the base URL of the EmCache server
	ServerURL string

	// Directory is the local directory where SQLite databases will be stored
	Directory string

	// Entities is the list of entity names to sync
	Entities []string

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
	entityNames  []string
	lastOplogIdx int64
	cancelFunc   context.CancelFunc
	stopWg       sync.WaitGroup

	// Internal dependencies
	httpClient *resty.Client
	logger     *slog.Logger
}

// NewClient creates a new EmCache client with the given configuration.
func NewClient(ctx context.Context, config Config) *Client {
	if config.SyncInterval == 0 {
		config.SyncInterval = 30 * time.Second
	}
	if config.BatchSize == 0 {
		config.BatchSize = 1000
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	httpClient := resty.New()
	httpClient.SetTimeout(30 * time.Second)
	httpClient.AddContentDecompresser("zstd", decompressZstd)
	httpClient.SetOutputDirectory(config.Directory)

	client := &Client{
		config:      config,
		entities:    make(map[string]*entityState),
		entityNames: config.Entities,
		httpClient:  httpClient,
		logger:      config.Logger,
	}

	return client
}

func (c *Client) Initialize(ctx context.Context) error {
	if err := c.validateConfig(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	if err := os.MkdirAll(c.config.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", c.config.Directory, err)
	}

	return c.initialize(ctx)
}

func (c *Client) validateConfig() error {
	if c.config.ServerURL == "" {
		return fmt.Errorf("server URL is required")
	}
	if c.config.Directory == "" {
		return fmt.Errorf("directory is required")
	}
	if len(c.entityNames) == 0 {
		return fmt.Errorf("no entities specified")
	}

	return nil
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
		c.logger.Info("No oplogs to sync", "min_id", c.lastOplogIdx, "max_id", maxOplogID)
		return nil
	}

	batchCount := int((totalOplogs + int64(c.config.BatchSize) - 1) / int64(c.config.BatchSize))
	c.logger.Info("Sync plan", "min_id", c.lastOplogIdx, "max_id", maxOplogID, "total_oplogs", totalOplogs, "batch_count", batchCount)

	_, err := c.syncBatches(ctx, batchCount)
	if err != nil {
		return fmt.Errorf("failed to sync batches: %w", err)
	}
	return nil
}

func (c *Client) CreateEntity(ctx context.Context, req CreateEntityRequest) (*Entity, error) {
	url := fmt.Sprintf("%s/api/entities", c.config.ServerURL)
	var entity Entity
	var errorResponse ErrorResponse

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetBody(req).
		SetResult(&entity).
		SetError(&errorResponse).
		Post(url)

	if err != nil {
		return nil, fmt.Errorf("failed to create entity: %w", err)
	}

	if resp.StatusCode() != 200 {
		if len(errorResponse.Errors) > 0 {
			return nil, fmt.Errorf("failed to create entity: %w", errorResponse)
		}
		return nil, fmt.Errorf("failed to create entity: %w", errorResponse)
	}

	return &entity, nil
}

// RemoveEntity removes an entity from the server.
func (c *Client) RemoveEntity(ctx context.Context, name string) error {
	url := fmt.Sprintf("%s/api/entities/%s", c.config.ServerURL, name)

	resp, err := c.httpClient.R().
		SetContext(ctx).
		Delete(url)

	if err != nil {
		return fmt.Errorf("failed to delete entity: %w", err)
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("failed to delete entity: HTTP %d", resp.StatusCode())
	}

	return nil
}

// GetEntities retrieves information about available entities from the server.
func (c *Client) GetEntities(ctx context.Context) ([]Entity, error) {
	url := fmt.Sprintf("%s/api/entities", c.config.ServerURL)
	var entities []Entity

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetResult(&entities).
		Get(url)

	if err != nil {
		return nil, fmt.Errorf("failed to get entities: %w", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("failed to get entities: HTTP %d", resp.StatusCode())
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
func (c *Client) getEntitiesFromServer(ctx context.Context, entityNames []string) ([]Entity, error) {
	url := fmt.Sprintf("%s/api/entities", c.config.ServerURL)
	var entities []Entity
	var errorResponse ErrorResponse

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetResult(&entities).
		SetError(&errorResponse).
		Get(url)

	if err != nil {
		return nil, fmt.Errorf("failed to get entities: %w", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("failed to get entities: %w", errorResponse)
	}

	// Filter entities by requested entity names if provided
	if len(entityNames) > 0 {
		entitySet := make(map[string]bool)
		for _, name := range entityNames {
			entitySet[name] = true
		}

		filtered := make([]Entity, 0, len(entities))
		for _, entity := range entities {
			if entitySet[entity.Name] {
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

// initialize sets up the client's internal state
func (c *Client) initialize(ctx context.Context) error {
	if len(c.entityNames) == 0 {
		c.logger.Info("No entities specified, skipping initialization")
		return nil
	}

	if err := os.MkdirAll(c.config.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", c.config.Directory, err)
	}

	entitiesData, err := c.getEntitiesFromServer(ctx, c.entityNames)
	if err != nil {
		return fmt.Errorf("failed to get entities from server: %w", err)
	}

	// Initialize entities in parallel
	results := lop.Map(c.config.Entities, func(entityName string, _ int) lo.Tuple2[*entityState, error] {
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
		c.logger.Error("Initial sync failed", "error", err)
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
			c.logger.Warn("Entity not found, skipping oplogs", "entity", entityName)
			continue
		}
		if state.entity == nil {
			c.logger.Warn("Entity not initialized, skipping oplogs", "entity", entityName)
			continue
		}

		err := state.entity.ApplyOplogs(ctx, entityEntries)
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
		idx, err := state.entity.GetMaxOplogId(ctx)
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
		lastOplogIdx, err := c.downloadDb(ctx, entityConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to download database for entity '%s': %w", entityConfig.Name, err)
		}
		_ = lastOplogIdx
	}

	db, err := openSQLiteDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database for entity '%s': %w", entityConfig.Name, err)
	}

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

func (c *Client) downloadDb(ctx context.Context, entityConfig EntityConfig) (int64, error) {
	url := fmt.Sprintf("%s/api/snapshot/%s", c.config.ServerURL, entityConfig.Name)
	var errorResponse ErrorResponse

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetHeader("Accept-Encoding", "zstd, gzip").
		SetSaveResponse(true).
		SetOutputFileName(fmt.Sprintf("%s.db", entityConfig.Name)).
		SetError(&errorResponse).
		Get(url)

	if err != nil {
		return 0, fmt.Errorf("failed to download database: %w", err)
	}

	if resp.StatusCode() != 200 {
		return 0, fmt.Errorf("failed to download database: %w", errorResponse)
	}

	return 0, nil
}

func (c *Client) getOplogEntries(ctx context.Context, entityNames []string, afterIndex int64) ([]Oplog, error) {
	var entries []Oplog
	var errorResponse ErrorResponse

	queryParams := url.Values{
		"from":     {fmt.Sprint(afterIndex)},
		"limit":    {fmt.Sprint(c.config.BatchSize)},
		"entities": entityNames,
	}

	request := c.httpClient.R().
		SetContext(ctx).
		SetResult(&entries).
		SetError(&errorResponse).
		SetQueryParamsFromValues(queryParams)

	url := fmt.Sprintf("%s/api/oplogs", c.config.ServerURL)
	resp, err := request.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get oplog entries: %w", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("failed to get oplog entries: %w", errorResponse)
	}

	return entries, nil
}

func (c *Client) getOplogStatus(ctx context.Context) (*OplogStatus, error) {
	url := fmt.Sprintf("%s/api/oplogs/status", c.config.ServerURL)
	var status OplogStatus

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetResult(&status).
		Get(url)

	if err != nil {
		return nil, fmt.Errorf("failed to get oplog status: %w", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("failed to get oplog status: HTTP %d", resp.StatusCode())
	}

	return &status, nil
}

func (c *Client) syncBatches(ctx context.Context, batchCount int) (bool, error) {
	currentOffset := c.lastOplogIdx

	for i := 0; i < batchCount; i++ {
		c.logger.Info("Syncing batch", "batch_number", i+1, "current_offset", currentOffset)

		entries, err := c.getOplogEntries(ctx, c.entityNames, currentOffset)
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
