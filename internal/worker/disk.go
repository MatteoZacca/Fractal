// disk.go is purely a wrapper around the physical hard drive
package worker

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	DirPermissions = os.FileMode(0755) // Read/Write/Execute for owner, Read/Execute for others
	ZeroBytes      = 0
)

// DiskManager handles all physical hard drive operations
type DiskManager struct {
	DataDir string
}

func NewDiskManager(dataDir string) *DiskManager {
	// Automatically create the directory if it doesn't exist yet
	os.MkdirAll(dataDir, DirPermissions)

	return &DiskManager{
		DataDir: dataDir,
	}
}

// CreateChunk creates a new file and returns the open file pointer for writing
func (d *DiskManager) CreateChunk(chunkID string) (*os.File, error) {
	chunkPath := filepath.Join(d.DataDir, chunkID+".dat")

	file, err := os.Create(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file on disk: %v", err)
	}
	return file, err
}

// OpenChunk opens an existing file and returns the file pointer for reading
func (d *DiskManager) OpenChunk(chunkID string) (*os.File, error) {
	chunkPath := filepath.Join(d.DataDir, chunkID+".dat")

	file, err := os.Open(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("chunk not found on this node: %v", err)
	}
	return file, nil
}

// DeleteChunk permanently removes the file from the hard drive
func (d *DiskManager) DeleteChunk(chunkID string) error {
	chunkPath := filepath.Join(d.DataDir, chunkID+".dat")

	err := os.Remove(chunkPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete chunk from disk: %v", err)
	}
	return nil
}

// StatChunk checks if a chunk exists and returns its size in bytes without opening it
func (d *DiskManager) RequestChunkSize(chunkID string) (int64, bool, error) {
	chunkPath := filepath.Join(d.DataDir, chunkID+".dat")

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
