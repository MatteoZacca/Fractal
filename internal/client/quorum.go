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

func downloadChunkWithQuorum(dockerPath string, startOffset int64, chunkID string, dataNodeIPs []string, outputFile *os.File, readRepairWg *sync.WaitGroup) error {

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
	var staleNodes []string

	sizeCounts := make(map[int64][]string)

	for range dataNodeIPs {
		res := <-outcomes

		if res.err != nil {
			// THE NODE IS DEAD. Do not try to repair a dead node.
			log.Printf("NODE UNREACHABLE: ping to %s failed. Ignoring for Read Repair.", res.dataNodeIP)
			continue
		}

		if !res.exists {
			log.Printf("%s is alive but missing chunk %s", res.dataNodeIP, chunkID)
			staleNodes = append(staleNodes, res.dataNodeIP) // Needs Read Repair
			continue
		}

		sizeCounts[res.sizeBytes] = append(sizeCounts[res.sizeBytes], res.dataNodeIP)

		if len(sizeCounts[res.sizeBytes]) >= ReadQuorum {
			healthyNodes = sizeCounts[res.sizeBytes]
			log.Printf("Read Quorum reached for %s!", chunkID)

			for size, nodes := range sizeCounts {
				if size != res.sizeBytes {
					log.Printf("Size mismatch detected in %s! Expected %d, but %v reported %d", chunkID, res.sizeBytes, nodes, size)
					staleNodes = append(staleNodes, nodes...)
				}
			}
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

	// READ REPAIR (only on alive datanodes that are missing data)
	if len(staleNodes) > 0 {
		readRepairWg.Add(1)

		go func(nodesToHeal []string, dockerPath string, offset int64, chunkID string) {
			defer readRepairWg.Done()

			for _, brokenIP := range nodesToHeal {
				err := uploadChunkToDataNode(dockerPath, offset, chunkID, brokenIP)
				if err != nil {
					log.Printf("Background heal of %s failed for %s: %v", chunkID, brokenIP, err)
				} else {
					log.Printf("Background heal of %s successful for %s!", chunkID, brokenIP)
				}

			}
		}(staleNodes, dockerPath, startOffset, chunkID)
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
