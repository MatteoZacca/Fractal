package tests

import (
	"fmt"
	"os/exec"
	"testing"
	"time"
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
