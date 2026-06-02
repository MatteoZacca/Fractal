package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/MatteoZacca/distributed-file-system/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const ChunkSize = 64 * 1024 * 1024 // 64MB
const StreamChunkSize = 64 * 1024  // 64KB

func main() {

	if os.Args[1] != "upload" {
		log.Fatalf("Type: upload <path-to-local-file>")
	}

	filePath := os.Args[2]
	fileName := filepath.Base(filePath)

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Could not open file %s: %v", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Could not get file info: %v", err)
	}
	fileSize := fileInfo.Size()

	log.Printf("Starting upload for '%s' (size: %d bytes)", fileName, fileSize)

	// ==========================================
	// Talk to the NameNode
	// ==========================================

	// Connect to the NameNode
	masterConnection, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to NameNode: %v", err)
	}
	defer masterConnection.Close()
	masterClient := pb.NewMasterServiceClient(masterConnection)

	// Ask for the blueprint
	log.Println("Requesting blueprint from NameNode...")
	createFileResponse, err := masterClient.CreateFile(context.Background(), &pb.CreateFileRequest{
		FilePath: fileName,
		FileSize: fileSize,
	})
	if err != nil {
		log.Fatalf("NameNode rejected upload: %v", err)
	}

	chunksMapping := createFileResponse.ChunkLocations
	log.Printf("Blueprint received! File split into %d chunks.", len(chunksMapping))

	// ==========================================
	// Talk to the DataNodes
	// ==========================================

	var chunkIDs []string

	for chunkID, nodeList := range chunksMapping {
		chunkIDs = append(chunkIDs, chunkID)

		for _, dataNodeIP := range nodeList.WorkerIps {
			log.Printf("Streaming %s to DataNode at %s...", chunkID, dataNodeIP)

			err := uploadChunkToDataNode(file, chunkID, dataNodeIP)
			if err != nil {
				log.Fatalf("Failed to upload %s to %s: %v", chunkID, dataNodeIP, err)
			}
		}
	}

}

func uploadChunkToDataNode(file *os.File, chunkID string, dataNodeIP string) error {
	// Connect to DataNode
	conn, err := grpc.NewClient(dataNodeIP, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to DataNode at %s: %v", dataNodeIP, err)
	}
	defer conn.Close()

	dataNodeClient := pb.NewWorkerServiceClient(conn)

	// Open a gRPC Stream
	stream, err := dataNodeClient.StoreChunk(context.Background())
	if err != nil {
		return fmt.Errorf("failed  to open stream: %v", err)
	}

	// Read the file in 64KB chunks and stream them
	buffer := make([]byte, StreamChunkSize)
	var bytesSent int64 = 0

	for bytesSent < ChunkSize {
		bytesRead, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading file: %v", err)
		}

		// Send the chunk over  the network
		err = stream.Send(&pb.ChunkData{
			ChunkId: chunkID,
			Data:    buffer[:bytesRead],
		})
		if err != nil {
			return fmt.Errorf("error sending data trough stream: %v", err)
		}

		bytesSent += int64(bytesRead)
	}

	// Tell the DataNode we're done sending this chunk
	_, err = stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("DataNode returned an error after streaming: %v", err)
	}

	return nil
}
