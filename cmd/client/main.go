package main

import (
	"log"

	"github.com/MatteoZacca/distributed-file-system/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	masterConnection, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to NameNode: %v", err)
	}
	defer masterConnection.Close()
	masterClient := pb.NewMasterServiceClient(masterConnection)
}
