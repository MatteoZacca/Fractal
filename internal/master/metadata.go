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

// Worker
type DataNode struct {
	NodeID        string    `json:"node_id"`
	Address       string    `json:"address"`
	RackID        string    `json:"rack_id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

type MetadataStore struct {
	mu sync.RWMutex
	// Map 1: NodeID -> DataNode Info
	DataNodes map[string]*DataNode `json:"data_nodes"`
	// Map 2: File Name -> List of Chunk IDs
	Files map[string][]string `json:"files"`
	// Map 3: Chunk ID -> List of Node IDs holding it
	ChunkLocations map[string][]string `json:"chunk_locations"`
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		DataNodes:      make(map[string]*DataNode),
		Files:          make(map[string][]string),
		ChunkLocations: make(map[string][]string),
	}
}

func (m *MetadataStore) SaveToDisk(filePath string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Convert our Maps into raw JSON bytes
	data, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	// Write the bytes to the hard drive
	err = os.WriteFile(filePath, data, FilePermissions)
	if err != nil {
		return fmt.Errorf("failed to write metadata to disk: %v", err)
	}
	return nil
}

func (m *MetadataStore) LoadFromDisk(filePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Printf("namespace.json not found. Creating a fresh skeleton...")

		emptyNameSpace, err := json.MarshalIndent(m, "", "")
		if err != nil {
			return fmt.Errorf("failedmarshal initial namespace: %v", err)
		}

		if writeErr := os.WriteFile(filePath, emptyNameSpace, 0644); writeErr != nil {
			return fmt.Errorf("cannot write to disk: %v", writeErr)
		}
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %v", err)
	}

	// Convert the raw JSON bytes back into Go Maps
	err = json.Unmarshal(data, m)
	if err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %v", err)
	}
	return nil
}
