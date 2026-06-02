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

	var healthyNodes []DataNode

	// HEALTH CHECK
	for _, dataNodeInfo := range m.DataNodes {
		// If a worker hasn't sent a heartbeat in the last 15 seconds, we consider it DEAD.
		// We will NOT assign new files to it.
		if time.Since(dataNodeInfo.LastHeartbeat) < 15*time.Second {
			healthyNodes = append(healthyNodes, *dataNodeInfo)
		}
	}

	// CAP THEOREM CHECK
	if len(healthyNodes) < replicationFactor {
		// We sacrifice Availability for Consistency. We refuse the upload!
		return nil, fmt.Errorf("system unavailable: need %d replicas, but only %d data nodes are alive", replicationFactor, len(healthyNodes))
	}

	// TOPOLOGY MATH
	return rackAwareness(healthyNodes, replicationFactor), nil
}

func rackAwareness(healthyNodes []DataNode, replicationFactor int) []string {

	racks := make(map[string][]DataNode)
	for _, node := range healthyNodes {
		racks[node.RackID] = append(racks[node.RackID], node)
	}

	var uniqueRacks []string
	for rackID := range racks {
		uniqueRacks = append(uniqueRacks, rackID)
	}

	var selectedAddresses []string
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Fallback: if everything is on 1 rack shuffle randomly
	if len(uniqueRacks) < 2 {
		r.Shuffle(len(healthyNodes), func(i, j int) {
			healthyNodes[i], healthyNodes[j] = healthyNodes[j], healthyNodes[i]
		})
		for i := range replicationFactor {
			selectedAddresses = append(selectedAddresses, healthyNodes[i].Address)
		}
		return selectedAddresses
	}

	// 2 Racks Available

	// Shuffle the racks so we don't always pick the same one first
	r.Shuffle(len(uniqueRacks), func(i, j int) {
		uniqueRacks[i], uniqueRacks[j] = uniqueRacks[j], uniqueRacks[i]
	})

	rackA := uniqueRacks[0]
	rackB := uniqueRacks[1]

	nodesInRackA := racks[rackA]
	nodesInRackB := racks[rackB]

	// Shuffle the nodes inside the chosen racks for load balancing
	r.Shuffle(len(nodesInRackA), func(i, j int) { nodesInRackA[i], nodesInRackA[j] = nodesInRackA[j], nodesInRackA[i] })
	r.Shuffle(len(nodesInRackB), func(i, j int) { nodesInRackB[i], nodesInRackB[j] = nodesInRackB[j], nodesInRackB[i] })

	// Replica 1: First node from Rack A
	selectedAddresses = append(selectedAddresses, nodesInRackA[0].Address)

	// Replica 2: First node from Rack B
	selectedAddresses = append(selectedAddresses, nodesInRackB[0].Address)

	// Replica 3: Second node from Rack B
	if len(nodesInRackB) > 1 {
		selectedAddresses = append(selectedAddresses, nodesInRackB[1].Address)
	} else if len(nodesInRackA) > 1 {
		// Rack B only had 1 node, put the 3rd replica in Rack A
		selectedAddresses = append(selectedAddresses, nodesInRackA[1].Address)
	}

	return selectedAddresses
}
