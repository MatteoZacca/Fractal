package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"github.com/MatteoZacca/Fractal/internal/worker"
	"github.com/MatteoZacca/Fractal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "datanode-1"
	}

	port := os.Getenv("DATANODE_PORT")
	if port == "" {
		port = "8001"
	}
	dataNodePort := ":" + port

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./data/datanode_1"
	}

	nameNodeAddress := os.Getenv("NAMENODE_ADDRESS")
	if nameNodeAddress == "" {
		nameNodeAddress = "localhost:9000"
	}

	rackID := os.Getenv("RACK_ID")
	if rackID == "" {
		rackID = "rack-default"
	}

	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// ==========================================
	// PART A: THE WORKER AS A SERVER
	// (Listening for CLI Clients)
	// ==========================================

	listener, err := net.Listen("tcp", dataNodePort) // 1 -> TCP socket: you always need a network port to listen on
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", dataNodePort, err)
	}

	grpcServer := grpc.NewServer()         // 2 -> Empty server: ask the gRPC library to give you a blank server
	dataNodeLogic := &worker.WorkerServer{ // 3 -> Registration: attach your logic to the blank server
		NodeId:  nodeID,
		DataDir: dataDir,
	}
	pb.RegisterWorkerServiceServer(grpcServer, dataNodeLogic)

	// ==========================================
	// PART B: THE WORKER AS A CLIENT
	// (Talking to the Master Node)
	// ==========================================

	// 5. Create a Client Connection to the Master Node
	masterConnection, err := grpc.NewClient(nameNodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to NameNode: %v", err)
	}
	defer masterConnection.Close()

	// 6. Create a client object that knows how to speak the Master's language
	masterClient := pb.NewMasterServiceClient(masterConnection)

	dataNodeAddress := nodeID + ":" + port
	go startHeartbeat(masterClient, nodeID, dataNodeAddress, nameNodeAddress, rackID)

	// ==========================================
	// PART C: START LISTENING
	// ==========================================
	log.Printf("--> Worker [%s] starting on port %s... saving data to %s", nodeID, dataNodePort, dataDir)

	// 8. This line blocks the program from exiting. It sits here waiting for incoming traffic.
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func startHeartbeat(client pb.MasterServiceClient, nodeID string, dataNodeAddress string, nameNodeAddress string, rackID string) {
	ticker := time.NewTicker(5 * time.Second)

	for {
		<-ticker.C // This pauses the loop until 5 seconds have passed

		// Call the Master's 'SendHeartbeat' RPC Verb
		_, err := client.SendHeartbeat(context.Background(), &pb.HeartbeatMsg{
			NodeId:         nodeID,
			Address:        dataNodeAddress,
			DiskUsage:      0,          // TODO: We will calculate this later
			DiskCapacity:   1000000000, // Fake 1GB capacity for now
			StoredChunkIds: []string{}, // TODO: We will scan the folder for these later
			RackId:         rackID,
		})

		if err != nil {
			log.Printf("Warning: Failed to reach Master at %s -> %v", nameNodeAddress, err)
		} else {
			log.Printf("Heartbeat successfully sent to Master.")
		}
	}

}
