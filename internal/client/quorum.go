package client

import (
	"fmt"
	"log"
	"os"
	"sync"
)

const (
	ReadQuorum  = 2
	WriteQuorum = 2
)

type pingOutcome struct {
	dataNodeIP string
	exists     bool
	sizeBytes  int64
	err        error
}

func downloadChunkWithQuorum(dockerPath string, startOffset int64, chunkID string, dataNodeIPs []string, outputFile *os.File) error {

	// CONCURRENT PING
	log.Printf("Pinging %v for %s metadata...", dataNodeIPs, chunkID)

	outcomes := make(chan pingOutcome, len(dataNodeIPs))
	var wg sync.WaitGroup

	for _, dataNodeIP := range dataNodeIPs {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			exists, sizeBytes, err := pingDataNode(ip, chunkID)
			outcomes <- pingOutcome{dataNodeIP: ip, exists: exists, sizeBytes: sizeBytes, err: err}
		}(dataNodeIP)
	}

	var healthyNodes []string
	var brokenNodes []string
	var expectedSize int64 = -1

	for range dataNodeIPs {
		res := <-outcomes

		if res.err == nil && res.exists {
			// record the size of the first healthy chunk found
			if expectedSize == -1 {
				expectedSize = res.sizeBytes
			}

			if res.sizeBytes == expectedSize {
				healthyNodes = append(healthyNodes, res.dataNodeIP)
			} else {
				log.Printf("size mismatch on %s. expected %d, got %d", res.dataNodeIP, expectedSize, res.sizeBytes)
				brokenNodes = append(brokenNodes, res.dataNodeIP)
			}
		} else {
			if res.err != nil {
				log.Printf("ping to %s failed: %v", res.dataNodeIP, res.err)
			} else {
				log.Printf("%s is missing the chunk %s", res.dataNodeIP, chunkID)
			}
			brokenNodes = append(brokenNodes, res.dataNodeIP)
		}

		if len(healthyNodes) >= ReadQuorum {
			log.Printf("Read Quorum reached for %s!", chunkID)
			break // STOP WAITING FOR 3RD PING
		}
	}

	if len(healthyNodes) < ReadQuorum {
		return fmt.Errorf("FAILURE: could not reach Read Quorum for %s. Cluster lost data", chunkID)
	}

	// SINGLE DOWNLOAD
	targetDataNode := healthyNodes[0]

	err := downloadChunk(targetDataNode, chunkID, outputFile)
	if err != nil {
		return fmt.Errorf("FAILURE: network failed during actual download from %s: %v", targetDataNode, err)
	}

	// READ REPAIR (WRITE BACK)
	if len(brokenNodes) > 0 {
		go func(nodesToHeal []string, dockerPath string, offset int64, chunkID string) {
			for _, brokenIP := range nodesToHeal {
				err := uploadChunkToDataNode(dockerPath, offset, chunkID, brokenIP)
				if err != nil {
					log.Printf("Background heal failed for %s: %v", brokenIP, err)
				} else {
					log.Printf("Background heal successful for %s!", brokenIP)
				}

			}
		}(brokenNodes, dockerPath, startOffset, chunkID)
	}

	return nil
}

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
				log.Printf("Write Quorum reached for %s", chunkID)
				return nil // DOESN'T CARE ABOUT THE 3RD NODE
			}
		} else {
			errors = append(errors, err)
		}
	}

	return fmt.Errorf("FAILURE: could not reach Write Quorum for %s: %v", chunkID, errors)

}
