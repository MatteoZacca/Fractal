package client

import (
	"context"
	"fmt"

	"github.com/MatteoZacca/Fractal/pb"
)

func GetClusterStatus() error {
	masterClient, conn, err := getNameNodeClient()
	if err != nil {
		return fmt.Errorf("failed to connect to NameNode: %v", err)
	}
	defer conn.Close()

	res, err := masterClient.GetClusterStatus(context.Background(), &pb.ClusterStatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to retrieve cluster status: %v", err)
	}

	fmt.Println("\n FRACTAL CLUSTER STATUS")
	fmt.Println("-------------------------------------------------")
	fmt.Printf("Active DataNodes:  %d\n", res.ActiveNodesCount)
	//fmt.Printf("Total Capacity:    %d bytes\n", res.TotalDiskCapacity)
	//fmt.Printf("Total Usage:       %d bytes\n", res.TotalDiskUsage)
	fmt.Println("-------------------------------------------------")

	return nil

}
