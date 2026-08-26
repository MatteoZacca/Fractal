package tests

import (
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
	HeartbeatInitializationDelay = 15 * time.Second
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

	time.Sleep(HeartbeatInitializationDelay)
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
