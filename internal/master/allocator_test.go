package master

import "testing"

func TestRackAwareness(t *testing.T) {
	mockNodes := []DataNode{
		{Address: "192.168.1.1", RackID: "Rack-A"},
		{Address: "192.168.1.2", RackID: "Rack-A"},
		{Address: "192.168.1.3", RackID: "Rack-B"},
		{Address: "192.168.1.4", RackID: "Rack-B"},
	}

	tests := []struct {
		name                  string
		nodes                 []DataNode
		replicationFactor     int
		expectedCount         int
		expectedMultipleRacks bool
	}{
		{
			name:                  "2-Rack Distribution",
			nodes:                 mockNodes,
			replicationFactor:     3,
			expectedCount:         3,
			expectedMultipleRacks: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicasIPs := rackAwareness(tt.nodes, tt.replicationFactor)

			if len(replicasIPs) != tt.expectedCount {
				t.Errorf("expected %d replicas, got %d", tt.expectedCount, len(replicasIPs))
			}

			if tt.expectedMultipleRacks {
				rackCount := countUniqueRacks(tt.nodes, replicasIPs)
				if rackCount < 2 {
					t.Errorf("expected replicas to be split across at least 2 racks, but they were all on %d rack", rackCount)
				}

			}
		})
	}
}

func countUniqueRacks(allNodes []DataNode, selectedIPs []string) int {
	racksUsed := make(map[string]bool)
	for _, ip := range selectedIPs {
		for _, node := range allNodes {
			if node.Address == ip {
				racksUsed[node.RackID] = true
			}
		}
	}
	return len(racksUsed)
}
