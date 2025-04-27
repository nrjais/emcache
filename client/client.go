package client

import (
	"context"
	"fmt"
	"io"
	"log"

	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.EmcacheServiceClient
}

func NewClient(ctx context.Context, serverAddr string) (*Client, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := pb.NewEmcacheServiceClient(conn)

	return &Client{
		conn:   conn,
		client: c,
	}, nil
}

func (c *Client) Close() error {
	log.Println("Closing gRPC client connection.")
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) DownloadDb(ctx context.Context, collectionName string, writer io.Writer) (int32, error) {
	req := &pb.DownloadDbRequest{CollectionName: collectionName}
	stream, err := c.client.DownloadDb(ctx, req)
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

func (c *Client) GetOplogEntries(ctx context.Context, collectionNames []string, afterIndex int64, limit int32) (*pb.GetOplogEntriesResponse, error) {
	req := &pb.GetOplogEntriesRequest{
		CollectionNames: collectionNames,
		AfterIndex:      afterIndex,
		Limit:           limit,
	}
	resp, err := c.client.GetOplogEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) AddCollection(ctx context.Context, collectionName string) (*pb.AddCollectionResponse, error) {
	req := &pb.AddCollectionRequest{CollectionName: collectionName}
	resp, err := c.client.AddCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
