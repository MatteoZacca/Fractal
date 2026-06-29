package client

import (
	"fmt"
	"log"
	"sync"
)

const WriteQuorum = 2

func uploadChunkWithQuorum(localPath string, startOffset int64, chunkID string, dataNodeIPs []string) error {
	// buffered channel to collect results without blocking goroutines
	outcomes := make(chan error, len(dataNodeIPs))
	var wg sync.WaitGroup

	for _, dataNodeIP := range dataNodeIPs {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			err := uploadChunkToDataNode(localPath, startOffset, chunkID, ip)
			outcomes <- err
		}(dataNodeIP)
	}

	successWrites := 0
	var errors []error

	for range dataNodeIPs {
		err := <-outcomes
		if err == nil {
			successWrites++
			if successWrites >= WriteQuorum {
				log.Printf("Quorum reached for %s", chunkID)
				return nil // DOESN'T CARE ABOUT THE 3RD NODE
			}
		} else {
			errors = append(errors, err)
		}
	}

	return fmt.Errorf("FAILURE: could not reach Write Quorum for %s: %v", chunkID, errors)

}
