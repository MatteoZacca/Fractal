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
		log.Fatal("FATAL: NAMENODE_PORT environment variable is not set")
	}

	if fsImagePath == "" {
		log.Fatal("FATAL: FSIMAGE_PATH environment variable is not set")
	}
}

func main() {
	clusterState := master.NewClusterState()

	log.Printf("INFO: booting NameNode... Attempting to load state from %s", fsImagePath)
	err := clusterState.LoadFromDisk(fsImagePath)
	if err != nil {
		log.Fatalf("FATAL: failed to initialize NameNode state from FSImage: %v", err)
	}
	log.Println("SUCCESS: FSImage loaded. Cluster state fully restored.")

	listener, err := net.Listen("tcp", ":"+nameNodePort)
	if err != nil {
		log.Fatalf("FATAL: failed to bind TCP listener on port %s: %v", nameNodePort, err)
	}

	grpcServer := grpc.NewServer()

	nameNodeServer := &master.NameNode{
		State:       clusterState,
		FSImagePath: fsImagePath,
	}
	pb.RegisterMasterServiceServer(grpcServer, nameNodeServer)

	log.Printf("INFO: NameNode is ALIVE and listening on port %s", nameNodePort)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("FATAL: gRPC server crashed: %v", err)
	}
}
