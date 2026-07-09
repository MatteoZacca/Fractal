package main

import (
	"log"
	"net"
	"os"

	"github.com/MatteoZacca/Fractal/internal/master"
	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
)

var (
	nameNodePort string
	fsImagePath  string
)

func init() {
	nameNodePort = os.Getenv("NAMENODE_PORT")
	fsImagePath = os.Getenv("FSIMAGE_PATH")

	if nameNodePort == "" {
		log.Fatal("NAMENODE_PORT environment variable is not set")
	}

	if fsImagePath == "" {
		log.Fatal("FSIMAGE_PATH environment variable is not set")
	}
}

func main() {
	leader := master.NewMetadataStore()

	log.Printf("Booting NameNode... Attempting to load state from %s", fsImagePath)
	err := leader.LoadFromDisk(fsImagePath)
	if err != nil {
		log.Fatalf("Critical Failure: Could not load metadata: %v", err)
	}
	log.Println("Metadata loaded successfully. System state restored.")

	listener, err := net.Listen("tcp", ":"+nameNodePort)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", nameNodePort, err)
	}

	grpcServer := grpc.NewServer()

	nameNodeLogic := &master.NameNode{
		Metadata:    leader,
		FSImagePath: fsImagePath,
	}
	pb.RegisterMasterServiceServer(grpcServer, nameNodeLogic)

	log.Printf("NameNode is ALIVE and listening on port %s", nameNodePort)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve gRPC: %v", err)
	}
}
