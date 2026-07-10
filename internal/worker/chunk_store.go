// disk.go is purely a wrapper around the physical hard drive
package worker

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	DirPermissions = 0755 // Read/Write/Execute for owner, Read/Execute for others
	ZeroBytes      = 0
)

// ChunkStore manages the physical storage of chunk files on the local hard drive
type ChunkStore struct {
	DataDir string
}

func NewChunkStore(dataDir string) *ChunkStore {
	// Automatically create the directory if it doesn't exist yet
	os.MkdirAll(dataDir, DirPermissions)

	return &ChunkStore{
		DataDir: dataDir,
	}
}

// CreateChunk creates a new file and returns the open file pointer for writing
func (c *ChunkStore) CreateChunk(chunkID string) (*os.File, error) {
	chunkPath := filepath.Join(c.DataDir, chunkID+".dat")

	file, err := os.Create(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file on disk: %v", err)
	}
	return file, err
}

// DeleteChunk permanently removes the file from the hard drive
func (c *ChunkStore) DeleteChunk(chunkID string) error {
	chunkPath := filepath.Join(c.DataDir, chunkID+".dat")

	err := os.Remove(chunkPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete chunk from disk: %v", err)
	}
	return nil
}

// OpenChunk opens an existing file and returns the file pointer for reading
func (c *ChunkStore) OpenChunk(chunkID string) (*os.File, error) {
	chunkPath := filepath.Join(c.DataDir, chunkID+".dat")

	file, err := os.Open(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("chunk not found on this node: %v", err)
	}
	return file, nil
}

// StatChunk checks if a chunk exists and returns its size in bytes without opening it
func (c *ChunkStore) RequestChunkSize(chunkID string) (int64, bool, error) {
	chunkPath := filepath.Join(c.DataDir, chunkID+".dat")

	info, err := os.Stat(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, but that's not a system error
			return ZeroBytes, false, nil
		}
		return ZeroBytes, false, fmt.Errorf("failed to read disk metadata for %s: %v", chunkID, err)
	}

	return info.Size(), true, nil
}
