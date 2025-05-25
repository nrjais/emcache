package client

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	pb "github.com/nrjais/emcache/pkg/protos"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CollectionConfig struct {
	Name string
}

type ClientConfig struct {
	ServerAddr     string
	Directory      string
	Collections    []CollectionConfig
	UpdateInterval time.Duration
	BatchSize      int32
}

type collectionState struct {
	config     CollectionConfig
	collection CollectionInterface
}

// ClientDependencies holds all the dependencies for the client
type ClientDependencies struct {
	GrpcClient    pb.EmcacheServiceClient
	Decompression DecompressionInterface
	Conn          *grpc.ClientConn // Keep for closing
}

type Client struct {
	cancelFunc   context.CancelFunc
	deps         ClientDependencies
	config       ClientConfig
	colNames     []string
	lastOplogIdx int64
	collections  map[string]*collectionState
	stopWg       sync.WaitGroup
}

// NewClient initializes the emcache client based on the provided configuration.
// It establishes a connection to the server and performs the initial download
// for each configured collection.
func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	conn, err := grpc.NewClient(config.ServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server %s: %w", config.ServerAddr, err)
	}
	grpcClient := pb.NewEmcacheServiceClient(conn)

	deps := ClientDependencies{
		GrpcClient:    grpcClient,
		Decompression: NewDecompressionAdapter(),
		Conn:          conn,
	}

	return NewClientWithDependencies(ctx, config, deps)
}

// NewClientWithDependencies creates a client with injected dependencies for testing
func NewClientWithDependencies(ctx context.Context, config ClientConfig, deps ClientDependencies) (*Client, error) {
	c := &Client{
		deps:        deps,
		config:      config,
		collections: make(map[string]*collectionState),
	}

	if c.config.UpdateInterval <= 0 {
		return nil, fmt.Errorf("update interval must be greater than 0")
	}

	return c, c.init(ctx, config)
}

func (c *Client) init(ctx context.Context, config ClientConfig) error {
	if len(config.Collections) == 0 {
		return nil
	}

	if config.Directory == "" {
		return fmt.Errorf("directory is required")
	}

	if err := os.MkdirAll(config.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", config.Directory, err)
	}

	c.colNames = make([]string, 0, len(config.Collections))
	for _, collConfig := range config.Collections {
		c.colNames = append(c.colNames, collConfig.Name)
	}
	collectionsData, err := c.GetCollections(ctx, c.colNames)
	if err != nil {
		return fmt.Errorf("failed to get collections: %w", err)
	}

	results := lop.Map(config.Collections, func(collConfig CollectionConfig, _ int) lo.Tuple2[*collectionState, error] {
		state, err := c.addCollectionInternal(ctx, collConfig, collectionsData)
		return lo.T2(state, err)
	})

	for _, result := range results {
		if result.B != nil {
			_ = c.Close()
			return fmt.Errorf("failed to initialize one or more collections: %w", result.B)
		}
		c.collections[result.A.config.Name] = result.A
	}

	// Determine the lowest offset from all databases
	err = c.initializeLastOplogIdx(ctx)
	if err != nil {
		_ = c.Close()
		return fmt.Errorf("failed to initialize last oplog index: %w", err)
	}

	return nil
}

// initializeLastOplogIdx determines the lowest offset from all databases to start sync correctly
func (c *Client) initializeLastOplogIdx(ctx context.Context) error {
	minIndex := int64(-1)

	for collectionName, state := range c.collections {
		idx, err := state.collection.GetLastAppliedOplogIndex(ctx)
		if err != nil {
			return fmt.Errorf("failed to get last applied oplog index for collection '%s': %w", collectionName, err)
		}

		if minIndex == -1 || idx < minIndex {
			minIndex = idx
		}
	}

	if minIndex == -1 {
		minIndex = 0 // Default if no collections
	}

	c.lastOplogIdx = minIndex

	return nil
}

func (c *Client) addCollectionInternal(ctx context.Context, collConfig CollectionConfig, collectionsData *pb.GetCollectionsResponse) (*collectionState, error) {
	dbPath := filepath.Join(c.config.Directory, collConfig.Name+".sqlite")

	serverCollections := collectionsData.Collections
	expectedDetails := c.collectionDetails(serverCollections, collConfig)
	if expectedDetails == nil {
		return nil, fmt.Errorf("collection '%s' not found on server", collConfig.Name)
	}

	dbVersion := expectedDetails.Version
	var db SQLiteDBInterface
	var err error

	db, err = openSQLiteDB(dbPath)
	if err == nil {
		storedVersion, err := getDbVersion(ctx, db)
		if err != nil {
			_ = db.Close()
			db = nil
		} else if storedVersion != dbVersion {
			_ = db.Close()
			db = nil
		}
	}

	if db == nil {
		dbVersion, err = c.downloadDb(ctx, dbPath, collConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to download db for collection '%s': %w", collConfig.Name, err)
		}

		db, err = openSQLiteDB(dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed opening db for collection '%s': %w", collConfig.Name, err)
		}
	}

	collection := NewCollectionAdapter(db, dbVersion, expectedDetails)

	return &collectionState{
		config:     collConfig,
		collection: collection,
	}, nil
}

func (*Client) collectionDetails(serverCollections []*pb.Collection, collConfig CollectionConfig) *pb.Collection {
	for _, coll := range serverCollections {
		if coll.Name == collConfig.Name {
			return coll
		}
	}
	return nil
}

func (c *Client) downloadDb(ctx context.Context, dbPath string, collConfig CollectionConfig) (int32, error) {
	file, err := os.Create(dbPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create/open file %s for collection '%s': %w", dbPath, collConfig.Name, err)
	}
	defer file.Close()

	dbVersion, err := c.downloadDbForCollection(ctx, collConfig.Name, file)
	if err != nil {
		return 0, fmt.Errorf("failed to download db for collection '%s': %w", collConfig.Name, err)
	}
	return dbVersion, nil
}

func (c *Client) Close() error {
	c.StopUpdates()
	return c.close()
}

func (c *Client) close() error {
	var firstErr error
	for _, state := range c.collections {
		if state.collection != nil {
			if err := state.collection.Close(); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	if c.deps.Conn != nil {
		if err := c.deps.Conn.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

func (c *Client) downloadDbForCollection(ctx context.Context, collectionName string, writer io.WriteCloser) (int32, error) {
	reqComp := pb.Compression_ZSTD
	stream, err := c.deps.GrpcClient.DownloadDb(ctx, &pb.DownloadDbRequest{CollectionName: collectionName, Compression: reqComp})
	if err != nil {
		return 0, fmt.Errorf("failed to start download stream: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("error receiving first download chunk: %w", err)
	}
	_ = stream.CloseSend()

	resComp := resp.GetCompression()
	firstChunkData := resp.GetChunk()

	if resComp != reqComp && resComp != pb.Compression_NONE {
		return 0, fmt.Errorf("server sent compression %s, but %s was requested", resComp, reqComp)
	}

	err = c.deps.Decompression.DecompressStream(stream, firstChunkData, resComp, writer)
	if err != nil {
		return 0, fmt.Errorf("decompression failed: %w", err)
	}

	return resp.GetVersion(), nil
}

func (c *Client) getOplogEntries(ctx context.Context, collectionNames []string, afterIndex int64) (*pb.GetOplogEntriesResponse, error) {
	req := &pb.GetOplogEntriesRequest{
		CollectionNames: collectionNames,
		AfterIndex:      afterIndex,
		Limit:           c.config.BatchSize,
	}
	resp, err := c.deps.GrpcClient.GetOplogEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) AddCollection(ctx context.Context, collectionName string, shape *pb.Shape) (*pb.AddCollectionResponse, error) {
	if shape == nil {
		return nil, fmt.Errorf("shape cannot be nil")
	}
	req := &pb.AddCollectionRequest{
		CollectionName: collectionName,
		Shape:          shape,
	}
	resp, err := c.deps.GrpcClient.AddCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) RemoveCollection(ctx context.Context, collectionName string) (*pb.RemoveCollectionResponse, error) {
	req := &pb.RemoveCollectionRequest{CollectionName: collectionName}
	resp, err := c.deps.GrpcClient.RemoveCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// StartDbUpdates launches background goroutines for each collection to poll for oplog updates.
func (c *Client) StartDbUpdates() error {
	if c.cancelFunc != nil {
		return fmt.Errorf("db updates already started")
	}
	collCtx, cancel := context.WithCancel(context.Background())
	c.cancelFunc = cancel
	c.stopWg.Add(1)
	go c.runCollectionUpdates(collCtx)
	return nil
}

// runCollectionUpdates is the main loop for a single collection's update goroutine.
func (c *Client) runCollectionUpdates(ctx context.Context) {
	defer c.stopWg.Done()

	interval := c.config.UpdateInterval
	for {
		ticker := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := c.SyncToLatest(ctx, 10)
			if err != nil {
				slog.Error("Error syncing to latest", "error", err)
			}
		}
	}
}

// applyOplogEntries applies a batch of oplog entries to the appropriate collection databases.
// It returns the index of the last successfully applied entry across all collections.
func (c *Client) applyOplogEntries(ctx context.Context, entries []*pb.OplogEntry) (int64, error) {
	entriesByCollection := make(map[string][]*pb.OplogEntry)
	lastAppliedIdx := c.lastOplogIdx
	for _, entry := range entries {
		entriesByCollection[entry.Collection] = append(entriesByCollection[entry.Collection], entry)
		if entry.Index > lastAppliedIdx {
			lastAppliedIdx = entry.Index
		}
	}

	for collectionName, collectionEntries := range entriesByCollection {
		state, ok := c.collections[collectionName]
		if !ok {
			continue
		}
		if state.collection == nil {
			continue
		}

		err := state.collection.ApplyOplogEntries(ctx, collectionEntries)
		if err != nil {
			return c.lastOplogIdx, fmt.Errorf("failed to apply oplog entries to collection '%s': %w", collectionName, err)
		}
	}

	return lastAppliedIdx, nil
}

// StopDbUpdates signals all running update goroutines to stop and waits for them to finish.
func (c *Client) StopUpdates() {
	if c.cancelFunc != nil {
		c.cancelFunc()
		c.cancelFunc = nil
	}

	c.stopWg.Wait()
}

// Query executes a read-only SQL query against the specified collection's database.
// It returns the standard sql.Rows, which the caller is responsible for closing.
func (c *Client) Query(ctx context.Context, collectionName string, query string, args ...any) (*sql.Rows, error) {
	state, exists := c.collections[collectionName]

	if !exists {
		return nil, fmt.Errorf("collection '%s' not found or not configured", collectionName)
	}

	if state.collection == nil {
		return nil, fmt.Errorf("database connection for collection '%s' is not available", collectionName)
	}

	rows, err := state.collection.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query on collection '%s': %w", collectionName, err)
	}

	return rows, nil
}

// SyncToLatest fetches and applies oplog entries for the specified collections
// until they are up-to-date with the server.
// This performs the synchronization synchronously in the calling goroutine.
func (c *Client) SyncToLatest(ctx context.Context, maxIterations int) error {
	for range maxIterations {
		resp, err := c.getOplogEntries(ctx, c.colNames, c.lastOplogIdx)
		if err != nil {
			return fmt.Errorf("failed to get oplog entries: %w", err)
		}

		if len(resp.Entries) == 0 {
			break
		}

		lastAppliedIdx, err := c.applyOplogEntries(ctx, resp.Entries)
		if err != nil {
			return fmt.Errorf("failed to apply oplog entries: %w", err)
		}
		c.lastOplogIdx = lastAppliedIdx
		if len(resp.Entries) < int(c.config.BatchSize) {
			break
		}
	}

	return nil
}

// GetCollections retrieves the list of collections managed by the server.
// Assumes the server implements a GetCollections RPC method.
func (c *Client) GetCollections(ctx context.Context, collections []string) (*pb.GetCollectionsResponse, error) {
	req := &pb.GetCollectionsRequest{
		CollectionNames: collections,
	}

	resp, err := c.deps.GrpcClient.GetCollections(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC GetCollections failed: %w", err)
	}

	return resp, nil
}
