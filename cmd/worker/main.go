package main

import (
	"log"
	"net"
	"os"

	"github.com/MatteoZacca/Fractal/internal/worker"
	"github.com/MatteoZacca/Fractal/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	dataNodeID      string
	dataNodePort    string
	dataDir         string
	nameNodeAddress string
	rackID          string
)

const DirPermissions = 0755 // Read/Write/Execute for owner, Read/Execute for others

func init() {
	dataNodeID = os.Getenv("DATANODE_ID")
	dataNodePort = os.Getenv("DATANODE_PORT")
	dataDir = os.Getenv("DATA_DIR")
	nameNodeAddress = os.Getenv("NAMENODE_ADDRESS")
	rackID = os.Getenv("RACK_ID")

	if dataNodeID == "" || dataNodePort == "" || dataDir == "" || nameNodeAddress == "" || rackID == "" {
		log.Fatalf("[FATAL] Missing one or more required environment variables (NODE_ID, DATANODE_PORT, DATA_DIR, NAMENODE_ADDRESS, RACK_ID) -> (%s, %s, %s, %s, %s)", dataNodeID, dataNodePort, dataDir, nameNodeAddress, rackID)
	}
}

func main() {
	/* ------------------- DATANODE AS A SERVER (Receiving Chunks) ----------------------- */
	listener, err := net.Listen("tcp", ":"+dataNodePort)
	if err != nil {
		log.Fatalf("[FATAL] Failed to bind TCP listener on port %s: %v", dataNodePort, err)
	}

	grpcServer := grpc.NewServer()
	dataNodeServer := &worker.DataNodeServer{
		DataNodeID: dataNodeID,
		ChunkStore: worker.NewChunkStore(dataDir),
	}
	pb.RegisterWorkerServiceServer(grpcServer, dataNodeServer)

	/* ------------------- DATANODE AS A CLIENT (Talking to NameNode) -------------------- */
	// Create a Client Connection to the NameNode
	nameNodeConn, err := grpc.NewClient(nameNodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[FATAL] Failed to establish connection to NameNode at %s: %v", nameNodeAddress, err)
	}
	defer nameNodeConn.Close()

	// Create a client object that knows how to speak the NameNode's language
	nameNodeClient := pb.NewMasterServiceClient(nameNodeConn)
	/* ----------------------------------------------------------------------------------- */

	dataNodeAddress := dataNodeID + ":" + dataNodePort
	// Start the background heartbeat thread
	go worker.StartHeartbeat(nameNodeClient, dataNodeID, dataNodeAddress, nameNodeAddress, rackID)

	log.Printf("[INFO] [%s] is ALIVE on port %s. Local storage mapped to: %s", dataNodeID, dataNodePort, dataDir)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("[FATAL] gRPC server crashed: %v", err)
	}
}
