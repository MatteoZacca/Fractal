package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"github.com/MatteoZacca/distributed-file-system/internal/worker"
	"github.com/MatteoZacca/distributed-file-system/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	workerID := "datanode-1"
	workerPort := ":8001"
	masterAddress := "localhost:9000"
	dataDir := "./data/datanode_1"

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// ==========================================
	// PART A: THE WORKER AS A SERVER
	// (Listening for CLI Clients)
	// ==========================================

	listener, err := net.Listen("tcp", workerPort) // 1 -> TCP socket: you always need a network port to listen on
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", workerPort, err)
	}

	grpcServer := grpc.NewServer()       // 2 -> Empty server: ask the gRPC library to give you a blank server
	workerLogic := &worker.WorkerServer{ // 3 -> Registration: attach your logic to the blank server
		NodeId:  workerID,
		DataDir: dataDir,
	}
	pb.RegisterWorkerServiceServer(grpcServer, workerLogic)

	// ==========================================
	// PART B: THE WORKER AS A CLIENT
	// (Talking to the Master Node)
	// ==========================================

	// 5. Create a Client Connection to the Master Node
	conn, err := grpc.NewClient(masterAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to create client connection to master: %v", err)
	}
	defer conn.Close()

	// 6. Create a client object that knows how to speak the Master's language
	masterClient := pb.NewMasterServiceClient(conn)

	// 7. Launch the Heartbeat in a background Goroutine!
	// (If we didn't use 'go' here, the program would get stuck in the heartbeat loop forever)
	go startHeartbeat(masterClient, workerID, "localhost"+workerPort)

	// ==========================================
	// PART C: START LISTENING
	// ==========================================

	log.Printf("--> Worker [%s] starting on port %s... saving data to %s", workerID, workerPort, dataDir)

	// 8. This line blocks the program from exiting. It sits here waiting for incoming traffic.
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func startHeartbeat(client pb.MasterServiceClient, nodeID string, address string) {
	ticker := time.NewTicker(5 * time.Second)

	for {
		<-ticker.C // This pauses the loop until 5 seconds have passed

		// Call the Master's 'SendHeartbeat' RPC Verb
		_, err := client.SendHeartbeat(context.Background(), &pb.HeartbeatMsg{
			NodeId:         nodeID,
			Address:        address,
			DiskUsage:      0,          // TODO: We will calculate this later
			DiskCapacity:   1000000000, // Fake 1GB capacity for now
			StoredChunkIds: []string{}, // TODO: We will scan the folder for these later
		})

		if err != nil {
			log.Printf("Warning: Failed to reach Master at localhost:9000 -> %v", err)
		} else {
			log.Printf("Heartbeat successfully sent to Master.")
		}
	}

}
