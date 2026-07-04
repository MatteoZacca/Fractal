package tests

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	HeartbeatInizializationDelay = 10 * time.Second
	LocalTestFilePath            = "../mock_files/150mb.mp4"
	RemoteTestFilePath           = "150mb.mp4"
)

func runDockerCompose(args ...string) error {
	cmd := exec.Command("docker-compose", args...)
	cmd.Dir = "../"

	return cmd.Run()
}

func setupClusterAndUpload(t *testing.T) pb.MasterServiceClient {
	t.Log("Booting cluster...")

	err := runDockerCompose("up", "--build", "-d")
	if err != nil {
		t.Fatalf("failed to boot cluster: %v", err)
	}

	t.Cleanup(func() {
		t.Log("Tearing down cluster...")
		runDockerCompose("down", "-v")
	})

	time.Sleep(HeartbeatInizializationDelay)

	err = client.UploadFile(LocalTestFilePath, RemoteTestFilePath)
	if err != nil {
		t.Fatalf("failed to upload %s in the cluster: %v", LocalTestFilePath, err)
	}

	masterConn, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to Master: %v", err)
	}
	t.Cleanup(func() { masterConn.Close() })

	return pb.NewMasterServiceClient(masterConn)
}

// TestReadQuorum_FailsWhenTwoReplicasDie ensures adherence to Strong Consistency (W+R>N).
// By destroying 2 out of 3 replicas for a specific chunk, the available replicas drop to 1.
// Since 1 is strictly less than the Read Quorum (R=2), the system must proactively abort
// the download to protect the user from stale or corrupted data.
func TestReadQuorum_FailsWhenTwoReplicasDie(t *testing.T) {
	// ARRANGE
	masterClient := setupClusterAndUpload(t)

	// ACT
	res, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
		FilePath: RemoteTestFilePath,
	})
	if err != nil {
		t.Fatalf("failed to fetch blueprint: %v", err)
	}

	var dataNodesToKill []string

	for _, dataNodeList := range res.ChunkLocations {
		dataNodesToKill = dataNodeList.WorkerIps[:2]
		break
	}

	stopArgs := []string{"stop"}
	for _, dataNodeIP := range dataNodesToKill {
		dataNodeContainerName := strings.Split(dataNodeIP, ":")[0]
		stopArgs = append(stopArgs, dataNodeContainerName)
	}

	t.Logf("killing containers: %v", stopArgs[1:])
	err = runDockerCompose(stopArgs...)
	if err != nil {
		t.Fatalf("failed to kill targeted containers: %v", err)
	}

	t.Log("Attempting to read the file...")

	err = client.DownloadFile(RemoteTestFilePath)

	// ASSERT
	if err == nil {
		t.Fatalf("FATAL: system allowed a read operation even though 2 replicas were destroyed! Quorum logic failed.")
	} else {
		t.Logf("SUCCESS: system correctly rejected download due to killing %v. Error caught: %v", dataNodesToKill, err)
	}
}

// TestReadQuorum_SuccessWhenOneReplicaDies verifies the fault tolerance of the Read operation.
// By destroying exactly 1 replica, 2 healthy replicas remain. Since the remaining nodes
// satisfy the Read Quorum (R=2), the system must seamlessly mask the failure and
// successfully reconstruct the file for the user.
func TestReadQuorum_SuccessWhenOneReplicaDies(t *testing.T) {
	// ARRANGE
	masterClient := setupClusterAndUpload(t)

	// ACT
	res, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
		FilePath: RemoteTestFilePath,
	})
	if err != nil {
		t.Fatalf("failed to fetch blueprint: %v", err)
	}

	var dataNodesToKill []string

	for _, dataNodeList := range res.ChunkLocations {
		dataNodesToKill = dataNodeList.WorkerIps[:1]
		break
	}

	stopArgs := []string{"stop"}
	for _, dataNodeIP := range dataNodesToKill {
		dataNodeContainerName := strings.Split(dataNodeIP, ":")[0]
		stopArgs = append(stopArgs, dataNodeContainerName)
	}

	t.Logf("killing containers: %v", stopArgs[1:])
	err = runDockerCompose(stopArgs...)
	if err != nil {
		t.Fatalf("failed to kill targeted containers: %v", err)
	}

	t.Log("Attempting to read the file...")

	err = client.DownloadFile(RemoteTestFilePath)

	// ASSERT
	if err == nil {
		t.Logf("SUCCESS: system correctly downloaded %s", RemoteTestFilePath)
	} else {
		t.Fatalf("FATAL: system rejected download even though only one replica was destroyed! Quorum logic failed.")
	}
}
