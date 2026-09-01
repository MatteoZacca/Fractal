package client

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

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
	fmt.Printf("\nACTIVE DATANODES: %d\n", res.ActiveNodesCount)
	//fmt.Printf("Total Capacity:    %d bytes\n", res.TotalDiskCapacity)
	//fmt.Printf("Total Usage:       %d bytes\n", res.TotalDiskUsage)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
	fmt.Fprintln(w, "NODE ID\tADDRESS\tRACK\tLAST HEARTBEAT")
	fmt.Fprintln(w, "-------\t-------\t----\t--------------")

	for _, node := range res.ActiveNodes {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s ago\n", node.NodeId, node.Address, node.RackId, node.LastHeartbeat)
	}
	w.Flush()
	fmt.Println("-------------------------------------------------")

	return nil

}
