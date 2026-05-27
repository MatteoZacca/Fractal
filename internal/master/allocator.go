package master

import (
	"fmt"
	"math/rand"
	"time"
)

const ChunkSize = 64 * 1024 * 1024 // 64 MB

// looks for healhy workers, and picks R of them?
func (m *MetadataStore) AllocateDataNodes(replicationFactor int) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var healthyDataNodes []string

	// 1. THE HEALTH CHECK: Filter out the dead nodes
	for nodeID, dataNodeInfo := range m.DataNodes {
		// If a worker hasn't sent a heartbeat in teh last 15 seconds, we consider it DEAD.
		// We will NOT assign new files to it.
		if time.Since(dataNodeInfo.LastHeartbeat) < 15*time.Second {
			healthyDataNodes = append(healthyDataNodes, nodeID)
		}
	}

	if len(healthyDataNodes) < replicationFactor {
		// We sacrifice Availability for Consistency. We refuse the upload!
		return nil, fmt.Errorf("system unavailable: need %d replicas, but only %d data nodes are alive", replicationFactor, len(healthyDataNodes))
	}

	// Shuffle the healthy workers to balance the load randomly
	// (I'll replace this with HDFS Rack-Aware logic)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(healthyDataNodes), func(i, j int) {
		healthyDataNodes[i], healthyDataNodes[j] = healthyDataNodes[j], healthyDataNodes[i]
	})

	// 4. Return the first 'R' workers from the shuffled list
	return healthyDataNodes[:replicationFactor], nil
}
