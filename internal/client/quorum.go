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

// downloadChunkWithQuorum coordinates pinging, consensus logic, downloading, and repairing.
func downloadChunkWithQuorum(dockerPath string, startOffset int64, chunkID string, dataNodeIPs []string, outputFile *os.File, readRepairWg *sync.WaitGroup) error {
	outcomes := broadcastPings(dataNodeIPs, chunkID)

	healthyNodes, staleNodes, err := evaluateReadQuorum(outcomes, len(dataNodeIPs), chunkID)
	if err != nil {
		return err
	}

	var downloadErr error
	for _, targetDataNode := range healthyNodes {
		downloadErr = downloadChunk(targetDataNode, chunkID, outputFile)
		if downloadErr == nil {
			break
		}
		log.Printf("[WARNING] Network failed during download of %s from %s. Trying next replica...", chunkID, targetDataNode)
	}

	if downloadErr != nil {
		return fmt.Errorf("FAILURE: all healthy replicas failed during download: %v", downloadErr)
	}

	if len(staleNodes) > 0 {
		triggerReadRepair(staleNodes, dockerPath, startOffset, chunkID, readRepairWg)
	}

	return nil
}

// uploadChunkWithQuorum coordinates concurrent uploads and verifies Write Quorum.
func uploadChunkWithQuorum(localPath string, startOffset int64, chunkID string, dataNodeIPs []string) error {
	outcomes := make(chan error, len(dataNodeIPs))

	for _, dataNodeIP := range dataNodeIPs {
		go func(ip string) {
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
				return nil // We don't care about the remaining nodes once quorum is reached
			}
		} else {
			errors = append(errors, err)
		}
	}

	return fmt.Errorf("FAILURE: could not reach Write Quorum for %s: %v", chunkID, errors)
}

// ==========================================
// HELPERS
// ==========================================

// broadcastPings sends concurrent CheckChunk requests to all target DataNodes.
func broadcastPings(dataNodeIPs []string, chunkID string) chan pingOutcome {
	log.Printf("Pinging %v for %s metadata...", dataNodeIPs, chunkID)
	outcomes := make(chan pingOutcome, len(dataNodeIPs))

	for _, dataNodeIP := range dataNodeIPs {
		go func(ip string) {
			exists, sizeBytes, err := pingDataNode(ip, chunkID)
			outcomes <- pingOutcome{dataNodeIP: ip, exists: exists, sizeBytes: sizeBytes, err: err}
		}(dataNodeIP)
	}

	return outcomes
}

// evaluateReadQuorum mathematically determines if R=2 consensus is reached based on file sizes.
func evaluateReadQuorum(outcomes chan pingOutcome, totalNodes int, chunkID string) ([]string, []string, error) {
	var healthyNodes []string
	var staleNodes []string
	sizeCounts := make(map[int64][]string)

	for i := 0; i < totalNodes; i++ {
		res := <-outcomes

		if res.err != nil {
			log.Printf("NODE UNREACHABLE: ping to %s failed. Ignoring for Read Repair.", res.dataNodeIP)
			continue
		}

		if !res.exists {
			log.Printf("%s is alive but missing %s", res.dataNodeIP, chunkID)
			staleNodes = append(staleNodes, res.dataNodeIP)
			continue
		}

		// Group nodes by the file size they report
		sizeCounts[res.sizeBytes] = append(sizeCounts[res.sizeBytes], res.dataNodeIP)

		if len(sizeCounts[res.sizeBytes]) >= ReadQuorum {
			healthyNodes = sizeCounts[res.sizeBytes]
			log.Printf("Read Quorum reached for %s!", chunkID)

			// Flag any node that reported a mismatched size as stale
			for size, nodes := range sizeCounts {
				if size != res.sizeBytes {
					log.Printf("Size mismatch detected in %s! Expected %d, but %v reported %d", chunkID, res.sizeBytes, nodes, size)
					staleNodes = append(staleNodes, nodes...)
				}
			}
			return healthyNodes, staleNodes, nil
		}
	}

	return nil, nil, fmt.Errorf("FAILURE: could not reach Read Quorum for %s. Cluster lost data", chunkID)
}

// triggerReadRepair launches an asynchronous upload to heal broken replicas.
func triggerReadRepair(nodesToHeal []string, dockerPath string, offset int64, chunkID string, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, brokenIP := range nodesToHeal {
			err := uploadChunkToDataNode(dockerPath, offset, chunkID, brokenIP)
			if err != nil {
				log.Printf("Background heal of %s failed for %s: %v", chunkID, brokenIP, err)
			} else {
				log.Printf("Background heal of %s successful for %s!", chunkID, brokenIP)
			}
		}
	}()
}
