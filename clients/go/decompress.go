package emcache

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// DecompressionInterface handles stream decompression
type DecompressionInterface interface {
	DecompressStream(reader io.Reader, firstChunk []byte, compression CompressionType, writer io.Writer) error
}

// CompressionType represents compression types
type CompressionType int

const (
	CompressionNone CompressionType = iota
	CompressionZstd
	CompressionGzip
)

// decompressStream decompresses data from reader to writer based on compression type
func decompressStream(
	reader io.Reader,
	firstChunk []byte,
	compression CompressionType,
	writer io.Writer,
) error {
	switch compression {
	case CompressionZstd:
		return decompressZstdStream(reader, firstChunk, writer)
	case CompressionNone:
		if err := writeLoop(firstChunk, reader, writer); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to write chunks: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported compression type %d", compression)
	}
}

func writeLoop(firstChunk []byte, reader io.Reader, writer io.Writer) error {
	if len(firstChunk) > 0 {
		if _, err := writer.Write(firstChunk); err != nil {
			return fmt.Errorf("failed to write first uncompressed chunk: %w", err)
		}
	}

	// Copy the rest of the data
	_, err := io.Copy(writer, reader)
	if err != nil {
		return fmt.Errorf("failed to copy data: %w", err)
	}

	return nil
}

func decompressZstdStream(reader io.Reader, firstChunk []byte, writer io.Writer) error {
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
		if err := writeLoop(firstChunk, reader, pw); err != nil {
			if err == io.EOF {
				feedErrChan <- nil
			} else {
				feedErrChan <- err
			}
		} else {
			feedErrChan <- nil
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
