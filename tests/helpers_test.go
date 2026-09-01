package tests

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	ClusterHealthPollingInterval = 1 * time.Second
	FailSafeTimeout              = 45 * time.Second
	LocalTestFilePath            = "../mock_files/150mb.mp4"
	RemoteTestFilePath           = "150mb.mp4"
)

func runDockerCompose(args ...string) error {
	cmd := exec.Command("docker-compose", args...)
	cmd.Dir = "../"

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker-compose failed: %v\nDocker Output:\n%s", err, out)
	}

	return nil
}

func setupCluster(t *testing.T) {
	t.Log("Booting cluster...")

	err := runDockerCompose("up", "--build", "-d")
	if err != nil {
		t.Fatalf("failed to boot cluster: %v", err)
	}

	t.Cleanup(func() {
		t.Log("Tearing down cluster...")
		runDockerCompose("down", "-v")
	})

	waitForClusterReady(t)
}

func uploadSimulation(t *testing.T) pb.MasterServiceClient {
	err := client.UploadFile(LocalTestFilePath, RemoteTestFilePath)
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

func waitForClusterReady(t *testing.T) {
	t.Log("Waiting for 4 DataNodes to register heartbeats...")
	// 45-second fail-safe timeout in case Docker gets stuck
	ctx, cancel := context.WithTimeout(context.Background(), FailSafeTimeout)
	defer cancel()

	ticker := time.NewTicker(ClusterHealthPollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("FATAL: Cluster failed to initialize within FailSafeTimeout.")
		case <-ticker.C:
			masterConn, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue // NameNode isn't listening yet, try again
			}

			masterClient := pb.NewMasterServiceClient(masterConn)
			res, err := masterClient.GetClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
			masterConn.Close()

			if err == nil && res.ActiveNodesCount == 4 {
				t.Log("SUCCESS: All 4 DataNodes are online and ready!")
				return
			}
		}
	}

}
