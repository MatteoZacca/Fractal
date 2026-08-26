package tests

import (
	"testing"

	"github.com/MatteoZacca/Fractal/internal/client"
)

// TestWriteQuorum_FailsWhenQuorumNotReached verifies that the `create“ operation
// fails if the Write Quorum (W=2) cannot be satisfied during the upload phase.
func TestWriteQuorum_FailsWhenQuorumNotReached(t *testing.T) {
	// ARRANGE
	setupCluster(t)

	dataNodesToKill := []string{"datanode-1", "datanode-2", "datanode-3"}

	stopArgs := append([]string{"stop"}, dataNodesToKill...)
	t.Logf("Killing containers: %v", dataNodesToKill)
	err := runDockerCompose(stopArgs...)
	if err != nil {
		t.Fatalf("Failed to kill targeted containers: %v", err)
	}

	// ACT
	t.Log("Attempting to upload the file...")

	err = client.UploadFile(LocalTestFilePath, RemoteTestFilePath)

	// ASSERT
	if err == nil {
		t.Fatalf("FATAL: system allowed a `create` operation even though 3 DataNodes were destroyed (Write Quorum < 2)")
	} else {
		t.Logf("SUCCESS: system correctly rejected `create` operation due to killing %v (Write Quorum < 2). Error caught: %v", dataNodesToKill, err)
	}
}

// TestWriteQuorum_SuccessWhenOneNodeDies verifies fault tolerance during writes.
// If 1 node fails, the system must seamlessly mask the failure and complete the upload.
func TestWriteQuorum_SuccessWhenOneNodeDies(t *testing.T) {
	// ARRANGE
	setupCluster(t)

	dataNodesToKill := []string{"datanode-1"}

	stopArgs := append([]string{"stop"}, dataNodesToKill...)
	t.Logf("Killing containers: %v", dataNodesToKill)
	err := runDockerCompose(stopArgs...)
	if err != nil {
		t.Fatalf("Failed to kill targeted containers: %v", err)
	}

	// ACT
	t.Log("Attempting to create and upload the file...")

	err = client.UploadFile(LocalTestFilePath, RemoteTestFilePath)

	// ASSERT
	if err == nil {
		t.Logf("SUCCESS: system correctly uploaded %s despite 1 dead node", RemoteTestFilePath)
	} else {
		t.Fatalf("FATAL: system rejected upload even though only one node was destroyed! Quorum logic failed. Error: %v", err)
	}
}
