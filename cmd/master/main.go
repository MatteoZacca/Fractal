package main

import (
	"log"
	"net"
	"os"

	"github.com/MatteoZacca/Fractal/internal/master"
	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
)

func main() {
	port := os.Getenv("MASTER_PORT")
	if port == "" {
		port = ":9000"
	}
	masterPort := ":" + port

	metadataFile := "/app/data/namespace.json"

	leader := master.NewMetadataStore()

	log.Printf("Booting NameNode... Attempting to load state from %s", metadataFile)
	err := leader.LoadFromDisk(metadataFile)
	if err != nil {
		log.Fatalf("Critical Failure: Could not load metadata: %v", err)
	}
	log.Println("Metadata loaded successfully. System state restored.")

	listener, err := net.Listen("tcp", masterPort)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", masterPort, err)
	}

	grpcServer := grpc.NewServer()

	nameNodeLogic := &master.NameNode{
		Metadata: leader,
	}
	pb.RegisterMasterServiceServer(grpcServer, nameNodeLogic)

	log.Printf("NameNode is ALIVE and listening on port %s", masterPort)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}
