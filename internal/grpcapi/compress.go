package grpcapi

import (
	"fmt"
	"io"
	"log/slog"

	"compress/gzip"

	"github.com/dustin/go-humanize"
	"github.com/klauspost/compress/zstd"
	pb "github.com/nrjais/emcache/pkg/protos"
)

func compressAndSendStream(
	reader io.Reader,
	stream pb.EmcacheService_DownloadDbServer,
	compression pb.Compression,
	chunkSize int,
) error {
	switch compression {
	case pb.Compression_ZSTD:
		return compressZstdAndSend(reader, stream, chunkSize)
	case pb.Compression_GZIP:
		return compressGzipAndSend(reader, stream, chunkSize)
	case pb.Compression_NONE:
		return sendChunks(reader, stream, chunkSize, "uncompressed")
	default:
		return fmt.Errorf("unsupported compression type requested: %s", compression)
	}
}

func sendChunks(reader io.Reader, stream pb.EmcacheService_DownloadDbServer, chunkSize int, desc string) error {
	totalSent := uint64(0)
	buffer := make([]byte, chunkSize)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			totalSent += uint64(n)
			data := &pb.DownloadDbResponse{Chunk: buffer[:n]}
			if sendErr := stream.Send(data); sendErr != nil {
				return fmt.Errorf("failed to stream %s database chunk: %w", desc, sendErr)
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read data for %s streaming: %w", desc, err)
		}
	}

	slog.Info("Total size of data sent: %s, for %s", humanize.Bytes(totalSent), desc)
	return nil
}

func compressZstdAndSend(reader io.Reader, stream pb.EmcacheService_DownloadDbServer, chunkSize int) error {
	pr, pw := io.Pipe()
	zw, err := zstd.NewWriter(pw)
	if err != nil {
		return fmt.Errorf("failed to create zstd writer: %w", err)
	}

	sendErrChan := make(chan error, 1)
	compressErrChan := make(chan error, 1)

	go func() {
		defer pr.Close()
		if err := sendChunks(pr, stream, chunkSize, "zstd compressed"); err != nil {
			sendErrChan <- err
		} else {
			sendErrChan <- nil
		}
	}()

	go func() {
		defer pw.Close()
		defer zw.Close()

		_, copyErr := io.Copy(zw, reader)
		if copyErr != nil {
			_ = pw.CloseWithError(fmt.Errorf("failed during compression copy: %w", copyErr))
			compressErrChan <- fmt.Errorf("failed during compression copy: %w", copyErr)
		} else {
			compressErrChan <- nil
		}
	}()

	sendErr := <-sendErrChan
	compressErr := <-compressErrChan

	if compressErr != nil {
		return compressErr
	}
	if sendErr != nil {
		return sendErr
	}

	return nil
}

func compressGzipAndSend(reader io.Reader, stream pb.EmcacheService_DownloadDbServer, chunkSize int) error {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)

	sendErrChan := make(chan error, 1)
	compressErrChan := make(chan error, 1)

	go func() {
		defer pr.Close()
		if err := sendChunks(pr, stream, chunkSize, "gzipped"); err != nil {
			sendErrChan <- err
		} else {
			sendErrChan <- nil
		}
	}()

	go func() {
		defer pw.Close()
		defer gw.Close()

		_, copyErr := io.Copy(gw, reader)
		if copyErr != nil {
			_ = pw.CloseWithError(fmt.Errorf("failed during gzip compression copy: %w", copyErr))
			compressErrChan <- fmt.Errorf("failed during gzip compression copy: %w", copyErr)
		} else {
			compressErrChan <- nil
		}
	}()

	sendErr := <-sendErrChan
	compressErr := <-compressErrChan

	if compressErr != nil {
		return compressErr
	}
	if sendErr != nil {
		return sendErr
	}

	return nil
}
