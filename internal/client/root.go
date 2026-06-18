package client

import (
	"fmt"
	"os"
	"strings"

	"github.com/MatteoZacca/Fractal/pb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var rootCmd = &cobra.Command{
	Use:   "fractal",
	Short: "CLI for Fractal distributed file system",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

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
