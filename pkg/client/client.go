package client

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
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
	config  CollectionConfig
	version int32
	db      *sql.DB
	details *pb.Collection
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

	for _, collConfig := range config.Collections {
		if err := c.addCollectionInternal(ctx, collConfig, collectionsData); err != nil {
			_ = c.Close()
			return fmt.Errorf("failed to initialize collection %s: %w", collConfig.Name, err)
		}
	}
	return nil
}

func (c *Client) addCollectionInternal(ctx context.Context, collConfig CollectionConfig, collectionsData *pb.GetCollectionsResponse) error {
	if _, exists := c.collections[collConfig.Name]; exists {
		return fmt.Errorf("duplicate collection '%s' configured", collConfig.Name)
	}

	c.collections[collConfig.Name] = &collectionState{config: collConfig}

	dbPath := filepath.Join(c.config.Directory, collConfig.Name+".sqlite")
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s for collection '%s': %w", dir, collConfig.Name, err)
	}

	dbVersion, err := c.downloadDb(ctx, dbPath, collConfig)
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open sqlite db (%s) for collection '%s': %w", dbPath, collConfig.Name, err)
	}
	db.SetMaxOpenConns(1)

	details, err := c.getCollectionDetails(collConfig.Name, dbVersion, collectionsData.Collections)
	if err != nil {
		return fmt.Errorf("failed to get collection details for collection '%s': %w", collConfig.Name, err)
	}

	c.collections[collConfig.Name] = &collectionState{
		config:  collConfig,
		db:      db,
		version: dbVersion,
		details: details,
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
		if state.db != nil {
			if err := state.db.Close(); err != nil {
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
	req := &pb.DownloadDbRequest{CollectionName: collectionName}
	stream, err := c.grpcClient.DownloadDb(ctx, req)
	if err != nil {
		return 0, err
	}

	var dbVersion int32 = -1
	firstChunk := true

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return dbVersion, nil
		}
		if err != nil {
			return dbVersion, err
		}

		if firstChunk {
			dbVersion = resp.GetVersion()
			firstChunk = false
		}

		chunk := resp.GetChunk()
		n, err := writer.Write(chunk)
		if err != nil {
			_ = stream.CloseSend()
			return dbVersion, err
		}
		if n != len(chunk) {
			return dbVersion, fmt.Errorf("short write to writer (%d bytes written, %d bytes chunk)", n, len(chunk))
		}
	}
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

// applyOplogEntries applies a batch of oplog entries to the given database connection within a transaction.
// It constructs SQL statements based on the operation type (UPSERT/DELETE) defined in the OplogEntry.
// It returns the index of the last successfully applied entry.
func (c *Client) applyOplogEntries(ctx context.Context, entries []*pb.OplogEntry) (int64, error) {
	entriesByCollection := make(map[string][]*pb.OplogEntry)
	lastAppliedIdx := c.lastOplogIdx
	for _, entry := range entries {
		entriesByCollection[entry.Collection] = append(entriesByCollection[entry.Collection], entry)
		if entry.Index > lastAppliedIdx {
			lastAppliedIdx = entry.Index
		}
	}

	for collection, entries := range entriesByCollection {
		db, ok := c.collections[collection]
		if !ok {
			return c.lastOplogIdx, fmt.Errorf("database for collection '%s' not found", collection)
		}
		err := c.applyOplogEntriesToDb(ctx, db, entries)
		if err != nil {
			return c.lastOplogIdx, fmt.Errorf("failed to apply oplog entries to collection '%s': %w", collection, err)
		}
	}

	return lastAppliedIdx, nil
}

func (c *Client) applyOplogEntriesToDb(ctx context.Context, state *collectionState, entries []*pb.OplogEntry) error {
	tx, err := state.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	const dataTableName = "_emcache_data"

	for _, entry := range entries {
		const pkColumn = "id"
		pkValue := entry.Id
		if state.version != entry.Version {
			// Skip entries with a version that is different from the current collection's version
			// TODO: Add realtime swap of the database file here to latest version
			continue
		}

		switch entry.Operation {
		case pb.OplogEntry_UPSERT:
			columns := []string{quoteIdentifier(pkColumn)}
			values := []any{pkValue}
			placeholders := []string{"?"}

			for _, column := range state.details.Shape.Columns {
				columns = append(columns, quoteIdentifier(column.Name))
				values = append(values, valueFromProto(entry.Data.Fields[column.Name]))
				placeholders = append(placeholders, "?")
			}

			sql := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
				quoteIdentifier(dataTableName),
				strings.Join(columns, ", "),
				strings.Join(placeholders, ", "))

			_, err = tx.ExecContext(ctx, sql, values...)
			if err != nil {
				return fmt.Errorf("failed to execute UPSERT oplog entry index %d for ID %s (%s): %w", entry.Index, pkValue, sql, err)
			}

		case pb.OplogEntry_DELETE:
			sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdentifier(dataTableName), quoteIdentifier(pkColumn))

			_, err = tx.ExecContext(ctx, sql, pkValue)
			if err != nil {
				return fmt.Errorf("failed to execute DELETE oplog entry index %d for ID %s (%s): %w", entry.Index, pkValue, sql, err)
			}
		default:
			// Ignore unknown operation types
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction after applying oplog entries: %w", err)
	}

	return nil
}

func valueFromProto(v *structpb.Value) any {
	switch k := v.Kind.(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_NumberValue:
		return k.NumberValue
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_BoolValue:
		return k.BoolValue
	case *structpb.Value_StructValue:
		jsonBytes, err := k.StructValue.MarshalJSON()
		if err != nil {
			return nil
		}
		return string(jsonBytes)
	case *structpb.Value_ListValue:
		jsonBytes, err := k.ListValue.MarshalJSON()
		if err != nil {
			return nil
		}
		return string(jsonBytes)
	default:
		return nil
	}
}

func quoteIdentifier(name string) string {
	return "\"" + name + "\""
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

	if state.db == nil {
		return nil, fmt.Errorf("database connection for collection '%s' is not available", collectionName)
	}

	rows, err := state.db.QueryContext(ctx, query, args...)
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

	log.Printf("Synchronous sync finished for collections: %v", c.collections)
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
