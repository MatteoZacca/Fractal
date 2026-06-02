package main

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/MatteoZacca/distributed-file-system/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

	masterConnection, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to NameNode: %v", err)
	}
	defer masterConnection.Close()
	masterClient := pb.NewMasterServiceClient(masterConnection)

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

}
