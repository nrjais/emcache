package grpcapi

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrapper(t *testing.T) {
	t.Run("tracks bytes read correctly", func(t *testing.T) {
		data := "Hello, world! This is a test string for reading."
		reader := strings.NewReader(data)
		wrapper := newWrapper(reader)

		assert.Equal(t, uint64(0), wrapper.n)

		// Read some data
		buffer := make([]byte, 10)
		n, err := wrapper.Read(buffer)

		require.NoError(t, err)
		assert.Equal(t, 10, n)
		assert.Equal(t, uint64(10), wrapper.n)
		assert.Equal(t, "Hello, wor", string(buffer))
	})

	t.Run("tracks total bytes across multiple reads", func(t *testing.T) {
		data := "This is a longer test string that will be read in multiple chunks."
		reader := strings.NewReader(data)
		wrapper := newWrapper(reader)

		// Read in chunks
		buffer := make([]byte, 15)
		totalRead := 0

		for {
			n, err := wrapper.Read(buffer)
			totalRead += n
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		assert.Equal(t, len(data), totalRead)
		assert.Equal(t, uint64(len(data)), wrapper.n)
	})

	t.Run("handles empty reader", func(t *testing.T) {
		reader := strings.NewReader("")
		wrapper := newWrapper(reader)

		buffer := make([]byte, 10)
		n, err := wrapper.Read(buffer)

		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)
		assert.Equal(t, uint64(0), wrapper.n)
	})

	t.Run("handles read errors", func(t *testing.T) {
		// Create a reader that will fail
		reader := &errorReader{err: assert.AnError}
		wrapper := newWrapper(reader)

		buffer := make([]byte, 10)
		n, err := wrapper.Read(buffer)

		assert.Equal(t, 0, n)
		assert.Equal(t, assert.AnError, err)
		assert.Equal(t, uint64(0), wrapper.n)
	})

	t.Run("tracks partial reads correctly", func(t *testing.T) {
		// Reader that returns partial data
		reader := &partialReader{data: []byte("Hello, world!"), chunkSize: 5}
		wrapper := newWrapper(reader)

		buffer := make([]byte, 10)
		n, err := wrapper.Read(buffer)

		require.NoError(t, err)
		assert.Equal(t, 5, n) // Should read chunk size
		assert.Equal(t, uint64(5), wrapper.n)
		assert.Equal(t, "Hello", string(buffer[:n]))

		// Read again
		n, err = wrapper.Read(buffer)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, uint64(10), wrapper.n) // Total should be cumulative
	})

	t.Run("creates new wrapper correctly", func(t *testing.T) {
		reader := strings.NewReader("test")
		wrapper := newWrapper(reader)

		assert.NotNil(t, wrapper)
		assert.Equal(t, reader, wrapper.Reader)
		assert.Equal(t, uint64(0), wrapper.n)
	})

	t.Run("handles large data reads", func(t *testing.T) {
		// Create a large string for testing
		largeData := strings.Repeat("ABCDEFGHIJ", 1000) // 10KB
		reader := strings.NewReader(largeData)
		wrapper := newWrapper(reader)

		// Read all data in one go
		buffer := make([]byte, len(largeData))
		n, err := wrapper.Read(buffer)

		require.NoError(t, err)
		assert.Equal(t, len(largeData), n)
		assert.Equal(t, uint64(len(largeData)), wrapper.n)
		assert.Equal(t, largeData, string(buffer))
	})

	t.Run("tracks bytes correctly with small buffer", func(t *testing.T) {
		data := "Testing with a small buffer size"
		reader := strings.NewReader(data)
		wrapper := newWrapper(reader)

		buffer := make([]byte, 3) // Very small buffer
		totalBytesRead := uint64(0)

		for {
			n, err := wrapper.Read(buffer)
			totalBytesRead += uint64(n)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		assert.Equal(t, uint64(len(data)), totalBytesRead)
		assert.Equal(t, uint64(len(data)), wrapper.n)
	})
}

// Helper types for testing

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

type partialReader struct {
	data      []byte
	position  int
	chunkSize int
}

func (r *partialReader) Read(p []byte) (int, error) {
	if r.position >= len(r.data) {
		return 0, io.EOF
	}

	// Read at most chunkSize bytes
	remaining := len(r.data) - r.position
	toRead := r.chunkSize
	if toRead > remaining {
		toRead = remaining
	}
	if toRead > len(p) {
		toRead = len(p)
	}

	copy(p, r.data[r.position:r.position+toRead])
	r.position += toRead
	return toRead, nil
}
