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
	config CollectionConfig
	sqlite *sqliteCollection
}

type Client struct {
	cancelFunc   context.CancelFunc
	conn         *grpc.ClientConn
	config       ClientConfig
	grpcClient   pb.EmcacheServiceClient
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

	c := &Client{
		conn:        conn,
		grpcClient:  grpcClient,
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

	return nil
}

func (c *Client) addCollectionInternal(ctx context.Context, collConfig CollectionConfig, collectionsData *pb.GetCollectionsResponse) (*collectionState, error) {
	dbPath := filepath.Join(c.config.Directory, collConfig.Name+".sqlite")
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s for collection '%s': %w", dir, collConfig.Name, err)
	}

	dbVersion, err := c.downloadDb(ctx, dbPath, collConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to download db for collection '%s': %w", collConfig.Name, err)
	}

	db, err := openSQLiteDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed opening db for collection '%s': %w", collConfig.Name, err)
	}

	details, err := c.getCollectionDetails(collConfig.Name, dbVersion, collectionsData.Collections)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to get collection details for collection '%s': %w", collConfig.Name, err)
	}

	return &collectionState{
		config: collConfig,
		sqlite: &sqliteCollection{
			db:      db,
			version: dbVersion,
			details: details,
		},
	}, nil
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

func (c *Client) getCollectionDetails(collectionName string, version int32, data []*pb.Collection) (*pb.Collection, error) {
	for _, collection := range data {
		if collection.Name == collectionName && collection.Version == version {
			return collection, nil
		}
	}
	return nil, fmt.Errorf("collection '%s' not found", collectionName)
}

func (c *Client) Close() error {
	c.StopUpdates()
	return c.close()
}

func (c *Client) close() error {
	var firstErr error
	for _, state := range c.collections {
		if state.sqlite != nil {
			if err := state.sqlite.close(); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

func (c *Client) downloadDbForCollection(ctx context.Context, collectionName string, writer io.Writer) (int32, error) {
	reqComp := pb.Compression_ZSTD
	stream, err := c.grpcClient.DownloadDb(ctx, &pb.DownloadDbRequest{CollectionName: collectionName, Compression: reqComp})
	if err != nil {
		return 0, fmt.Errorf("failed to start download stream: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return 0, fmt.Errorf("received empty stream")
		}
		return 0, fmt.Errorf("error receiving first download chunk: %w", err)
	}
	_ = stream.CloseSend()

	resComp := resp.GetCompression()
	firstChunkData := resp.GetChunk()

	if resComp != reqComp && resComp != pb.Compression_NONE {
		return 0, fmt.Errorf("server sent compression %s, but %s was requested", resComp, reqComp)
	}

	err = decompressStream(stream, firstChunkData, resComp, writer)
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
	resp, err := c.grpcClient.GetOplogEntries(ctx, req)
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
	resp, err := c.grpcClient.AddCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) RemoveCollection(ctx context.Context, collectionName string) (*pb.RemoveCollectionResponse, error) {
	req := &pb.RemoveCollectionRequest{CollectionName: collectionName}
	resp, err := c.grpcClient.RemoveCollection(ctx, req)
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
				// TODO: log error
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
		if state.sqlite == nil {
			continue
		}

		err := state.sqlite.applyOplogEntries(ctx, collectionEntries)
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

	if state.sqlite == nil {
		return nil, fmt.Errorf("database connection for collection '%s' is not available", collectionName)
	}

	rows, err := state.sqlite.query(ctx, query, args...)
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

	slog.Info("Synchronous sync finished for collections", "collections", c.collections)
	return nil
}

// GetCollections retrieves the list of collections managed by the server.
// Assumes the server implements a GetCollections RPC method.
func (c *Client) GetCollections(ctx context.Context, collections []string) (*pb.GetCollectionsResponse, error) {
	req := &pb.GetCollectionsRequest{
		CollectionNames: collections,
	}

	resp, err := c.grpcClient.GetCollections(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC GetCollections failed: %w", err)
	}

	return resp, nil
}
