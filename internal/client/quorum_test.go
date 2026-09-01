package client

import (
	"errors"
	"testing"
)

func TestEvaluateReadQuorum(t *testing.T) {
	tests := []struct {
		name            string
		mockOutcomes    []pingOutcome
		expectedHealthy int
		expectedStale   int
		expectError     bool
	}{
		{
			name: "Happy Path - Quorum Reached Immediately",
			mockOutcomes: []pingOutcome{
				{dataNodeIP: "192.168.1.1", exists: true, sizeBytes: 64000, err: nil},
				{dataNodeIP: "192.168.1.2", exists: true, sizeBytes: 64000, err: nil},
				{dataNodeIP: "192.168.1.3", exists: true, sizeBytes: 64000, err: nil},
			},
			expectedHealthy: 2, // Short-circuits as soon as 2 nodes agree
			expectedStale:   0,
			expectError:     false,
		},
		{
			name: "Read Repair Triggered - Size Mismatch",
			mockOutcomes: []pingOutcome{
				{dataNodeIP: "192.168.1.1", exists: true, sizeBytes: 64000, err: nil},
				{dataNodeIP: "192.168.1.2", exists: true, sizeBytes: 32000, err: nil}, // Corrupted chunk
				{dataNodeIP: "192.168.1.3", exists: true, sizeBytes: 64000, err: nil},
			},
			expectedHealthy: 2,
			expectedStale:   1, // Node 2 must be flagged for Read Repair
			expectError:     false,
		},
		{
			name: "Quorum Failure - Missing Data and Network Errors",
			mockOutcomes: []pingOutcome{
				{dataNodeIP: "192.168.1.1", exists: true, sizeBytes: 64000, err: nil},
				{dataNodeIP: "192.168.1.2", exists: false, sizeBytes: 0, err: nil},                   // Node alive but chunk missing
				{dataNodeIP: "192.168.1.3", exists: false, sizeBytes: 0, err: errors.New("timeout")}, // Node dead
			},
			expectedHealthy: 0,
			expectedStale:   0,
			expectError:     true, // Fails because only 1 replica exists
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Buffer the channel so the function can pull outcomes instantly
			outcomesChan := make(chan pingOutcome, len(tt.mockOutcomes))
			for _, outcome := range tt.mockOutcomes {
				outcomesChan <- outcome
			}

			healthy, stale, err := evaluateReadQuorum(outcomesChan, len(tt.mockOutcomes), "chunk-1")

			if (err != nil) != tt.expectError {
				t.Fatalf("expected error: %v, got: %v", tt.expectError, err)
			}

			if len(healthy) != tt.expectedHealthy {
				t.Errorf("expected %d healthy nodes, got %d", tt.expectedHealthy, len(healthy))
			}

			if len(stale) != tt.expectedStale {
				t.Errorf("expected %d stale nodes, got %d", tt.expectedStale, len(stale))
			}
		})
	}
}
