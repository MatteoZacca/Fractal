package client

import (
	"fmt"
	"strings"

	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func resolveLocalAddress(dockerAddress string) string {
	parts := strings.Split(dockerAddress, ":")
	if len(parts) == 2 {
		return "localhost:" + parts[1]
	}
	return dockerAddress
}

func getNameNodeClient() (pb.MasterServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to NameNode: %v", err)
	}
	return pb.NewMasterServiceClient(conn), conn, nil
}

func getDataNodeClient(dockeraddress string) (pb.WorkerServiceClient, *grpc.ClientConn, error) {
	reachableAddress := resolveLocalAddress(dockeraddress)
	conn, err := grpc.NewClient(reachableAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to DataNode: %v", err)
	}
	return pb.NewWorkerServiceClient(conn), conn, nil
}
