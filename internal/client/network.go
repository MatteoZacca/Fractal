package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DataNodePingTimeout = 2 * time.Second
	ZeroBytes           = 0
)

func getDataNodeClient(dockeraddress string) (pb.WorkerServiceClient, *grpc.ClientConn, error) {
	reachableAddress := resolveLocalAddress(dockeraddress)
	conn, err := grpc.NewClient(reachableAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to DataNode: %v", err)
	}
	return pb.NewWorkerServiceClient(conn), conn, nil
}

func getNameNodeClient() (pb.MasterServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to NameNode: %v", err)
	}
	return pb.NewMasterServiceClient(conn), conn, nil
}

func pingDataNode(dataNodeIP string, chunkID string) (bool, int64, error) {
	dataNodeClient, conn, err := getDataNodeClient(dataNodeIP)
	if err != nil {
		return false, ZeroBytes, err
	}
	defer conn.Close()

	// Enforce a 2-second timeout: if a DataNode is offline or hanging, don't wait
	// forever, just assume the node is dead
	ctx, cancel := context.WithTimeout(context.Background(), DataNodePingTimeout)
	defer cancel()

	res, err := dataNodeClient.CheckChunk(ctx, &pb.CheckChunkRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return false, ZeroBytes, fmt.Errorf("network ping to datanode %s failed: %v", dataNodeIP, err)
	}

	return res.Exists, res.SizeBytes, nil
}

func resolveLocalAddress(dockerAddress string) string {
	parts := strings.Split(dockerAddress, ":")
	if len(parts) == 2 {
		return "localhost:" + parts[1]
	}
	return dockerAddress
}
