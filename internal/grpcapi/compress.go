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

type wrapper struct {
	io.Reader
	n uint64
}

func newWrapper(r io.Reader) *wrapper {
	return &wrapper{Reader: r, n: 0}
}

func (w *wrapper) Read(p []byte) (int, error) {
	n, err := w.Reader.Read(p)
	w.n += uint64(n)
	return n, err
}

func compressAndSendStream(
	reader io.Reader,
	stream pb.EmcacheService_DownloadDbServer,
	compression pb.Compression,
	chunkSize int,
) error {
	wrappedReader := newWrapper(reader)
	defer func() {
		slog.Info("Download completed original size", "size", humanize.Bytes(wrappedReader.n), "compression", compression)
	}()

	switch compression {
	case pb.Compression_ZSTD:
		return compressZstdAndSend(wrappedReader, stream, chunkSize)
	case pb.Compression_GZIP:
		return compressGzipAndSend(wrappedReader, stream, chunkSize)
	case pb.Compression_NONE:
		return sendChunks(wrappedReader, stream, chunkSize, "uncompressed")
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

	slog.Info("Download completed", "size", humanize.Bytes(totalSent), "compression", desc)
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
