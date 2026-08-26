package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/MatteoZacca/Fractal/internal/client"
	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestReadRepair_RestoresMissingChunk(t *testing.T) {
	// ARRANGE
	setupCluster(t)
	masterClient := uploadSimulation(t)

	res, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
		FilePath: RemoteTestFilePath,
	})
	if err != nil {
		t.Fatalf("failed to fetch blueprint: %v", err)
	}

	// Pick a single chunk and one of its DataNodes to sabotage
	var targetChunkID string
	var sabotagedDataNodeIP string
	for chunkID, nodeList := range res.ChunkLocations {
		targetChunkID = chunkID
		sabotagedDataNodeIP = nodeList.WorkerIps[0]
		break
	}

	parts := strings.Split(sabotagedDataNodeIP, ":")
	localhostAddr := "localhost:" + parts[1]

	t.Logf("Targeting node %s for sabotage on chunk %s...", localhostAddr, targetChunkID)

	workerConn, err := grpc.NewClient(localhostAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect to DataNode: %v", err)
	}
	defer workerConn.Close()
	workerClient := pb.NewWorkerServiceClient(workerConn)

	_, err = workerClient.DeleteChunk(context.Background(), &pb.DeleteChunkRequest{
		ChunkId: targetChunkID,
	})
	if err != nil {
		t.Fatalf("failed to sabotage %s: %v", targetChunkID, err)
	}

	// ACT
	t.Logf("%s on %s sabotaged. Triggering download to force Read Repair...", targetChunkID, sabotagedDataNodeIP)
	err = client.DownloadFile(RemoteTestFilePath)
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}

	// ASSERT
	checkRes, err := workerClient.CheckChunk(context.Background(), &pb.CheckChunkRequest{
		ChunkId: targetChunkID,
	})
	if err != nil {
		t.Fatalf("failed to ping DataNode for chunk status: %v", err)
	}

	if !checkRes.Exists {
		t.Fatalf("FATAL: Read Repair failed! Chunk %s is still missing from %s", targetChunkID, sabotagedDataNodeIP)
	} else {
		t.Logf("SUCCESS: Read Repair worked! Chunk %s was successfully restored on %s", targetChunkID, sabotagedDataNodeIP)
	}
}
