package client

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	pb "github.com/nrjais/emcache/pkg/protos"
	"google.golang.org/grpc"
)

func decompressStream(
	stream grpc.ClientStream,
	firstChunk []byte,
	compression pb.Compression,
	writer io.Writer,
) error {
	switch compression {
	case pb.Compression_ZSTD:
		return decompressZstdStream(stream, firstChunk, writer)
	case pb.Compression_NONE:
		if err := writeLoop(firstChunk, stream, writer); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to write chunks: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported compression type %s received", compression)
	}
}

func writeLoop(firstChunk []byte, stream grpc.ClientStream, writer io.Writer) error {
	if _, err := writer.Write(firstChunk); err != nil {
		return fmt.Errorf("failed to write first uncompressed chunk: %w", err)
	}
	for {
		resp := &pb.DownloadDbResponse{}
		if err := stream.RecvMsg(resp); err != nil {
			return err
		}
		chunk := resp.GetChunk()
		if _, err := writer.Write(chunk); err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}
	}
}

func decompressZstdStream(stream grpc.ClientStream, firstChunk []byte, writer io.Writer) error {
	pr, pw := io.Pipe()
	copyErrChan := make(chan error, 1)
	feedErrChan := make(chan error, 1)

	zr, err := zstd.NewReader(pr)
	if err != nil {
		return fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer zr.Close()

	go func() {
		defer pr.Close()
		_, copyErr := io.Copy(writer, zr)
		if copyErr != nil {
			copyErrChan <- fmt.Errorf("decompression write failed: %w", copyErr)
		} else {
			copyErrChan <- nil
		}
	}()

	go func() {
		defer pw.Close()
		if err := writeLoop(firstChunk, stream, pw); err != nil {
			if err == io.EOF {
				feedErrChan <- nil
			} else {
				feedErrChan <- err
			}
		}
	}()

	feedErr := <-feedErrChan
	copyErr := <-copyErrChan

	if feedErr != nil {
		return feedErr
	}
	if copyErr != nil {
		return copyErr
	}

	return nil
}
