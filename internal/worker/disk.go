// disk.go is purely a wrapper around the physical hard drive
package worker

import (
	"fmt"
	"os"
	"path/filepath"
)

// DiskManager handles all physical hard drive operations
type DiskManager struct {
	DataDir string
}

func NewDiskManager(dataDir string) *DiskManager {
	// Automatically create the directory if it doesn't exist yet
	os.MkdirAll(dataDir, 0755)

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
