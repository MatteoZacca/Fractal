// This handles the raw data and physical disk I/O (SaveToDisk).
package master

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const FilePermissions = 0644 // Read/Write for owner; Read for others

// ClusterState represents the entire active state of the file system and DataNodes
type ClusterState struct {
	mu sync.RWMutex
	// Map 1: NodeID -> DataNode Info
	DataNodes map[string]*DataNode `json:"data_nodes"`
	// Map 2: File Name -> List of Chunk IDs
	Files map[string][]string `json:"files"`
	// Map 3: Chunk ID -> List of Node IDs holding it
	ChunkLocations map[string][]string `json:"chunk_locations"`
}

type DataNode struct {
	NodeID        string    `json:"node_id"`
	Address       string    `json:"address"`
	RackID        string    `json:"rack_id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

func NewClusterState() *ClusterState {
	return &ClusterState{
		DataNodes:      make(map[string]*DataNode),
		Files:          make(map[string][]string),
		ChunkLocations: make(map[string][]string),
	}
}

func (c *ClusterState) LoadFromDisk(fsImagePath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := os.Stat(fsImagePath); os.IsNotExist(err) {
		log.Printf("INFO: FSImage not found at %s. Creating a fresh, empty cluster state...", fsImagePath)

		emptyNameSpace, err := json.MarshalIndent(c, "", "")
		if err != nil {
			return fmt.Errorf("FATAL: failed to marshal initial cluster state: %v", err)
		}

		if writeErr := os.WriteFile(fsImagePath, emptyNameSpace, 0644); writeErr != nil {
			return fmt.Errorf("FATAL: cannot write to disk: %v", writeErr)
		}
	}

	data, err := os.ReadFile(fsImagePath)
	if err != nil {
		return fmt.Errorf("FATAL: failed to read FSImage file: %v", err)
	}

	// Convert the raw JSON bytes back into Go Maps
	err = json.Unmarshal(data, c)
	if err != nil {
		return fmt.Errorf("FATAL: failed to unmarshal FSImage: %v", err)
	}
	return nil
}

func (c *ClusterState) SaveToDisk(filePath string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Convert our Maps into raw JSON bytes
	data, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		return fmt.Errorf("FATAL: failed to marshal cluster state: %v", err)
	}

	// Write the bytes to the hard drive
	err = os.WriteFile(filePath, data, FilePermissions)
	if err != nil {
		return fmt.Errorf("FATAL: failed to write cluster state to disk: %v", err)
	}
	return nil
}
