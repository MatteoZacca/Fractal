package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/MatteoZacca/Fractal/pb"
)

const (
	StorageChunkSize = 64 * 1024 * 1024 // 64MB
	StreamChunkSize  = 64 * 1024        // 64KB
)

func uploadFile(localFilePath string, targetFileName string) {
	file, err := os.Open(localFilePath)
	if err != nil {
		log.Fatalf("could not open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("could not get file info: %v", err)
	}
	fileSize := fileInfo.Size()

	log.Printf("Starting upload for '%s' (size: %d bytes)", targetFileName, fileSize)

	// Connect to the NameNode
	masterClient, conn, err := getNameNodeClient()
	if err != nil {
		log.Fatalf("failed to connect to NameNode: %v", err)
	}
	defer conn.Close()

	res, err := masterClient.CreateFile(context.Background(), &pb.CreateFileRequest{
		FilePath: targetFileName,
		FileSize: fileSize,
	})

	if err != nil {
		log.Fatalf("NameNode reject upload: %v", err)
	}

	chunksMapping := res.ChunkLocations
	log.Printf("File split into into %d chunks", len(chunksMapping))

	// Stream to DataNodes
	var chunkIDs []string
	var currentChunkIndex int64 = 0

	for chunkID, nodeList := range res.ChunkLocations {
		chunkIDs = append(chunkIDs, chunkID)
		startOffset := currentChunkIndex * StorageChunkSize

		for _, dataNodeIP := range nodeList.WorkerIps {
			log.Printf("Streaming %s to DataNode at %s...", chunkID, resolveLocalAddress(dataNodeIP))

			_, err := file.Seek(startOffset, io.SeekStart)
			if err != nil {
				log.Fatalf("Failed to seek file playhead: %v", err)
			}

			if err := uploadChunkToDataNode(file, chunkID, dataNodeIP); err != nil {
				log.Fatalf("Failed to upload to %s: %v", dataNodeIP, err)
			}
		}

		currentChunkIndex++
	}

	// Commit the file
	_, err = masterClient.CommitFile(context.Background(), &pb.CommitFileRequest{
		FilePath:       targetFileName,
		ChunkIds:       chunkIDs,
		ChunkLocations: res.ChunkLocations,
	})
	if err != nil {
		log.Fatalf("Failed to commit: %v", err)
	}
	log.Printf("Success! File %s is safely stored.", targetFileName)
}

func uploadChunkToDataNode(file *os.File, chunkID string, dataNodeIP string) error {
	// Connect to DataNode
	dataNodeClient, conn, err := getDataNodeClient(dataNodeIP)
	if err != nil {
		return fmt.Errorf("failed to connect to DataNode at %s: %v", dataNodeIP, err)
	}
	defer conn.Close()

	// Open a gRPC Stream
	stream, err := dataNodeClient.StoreChunk(context.Background())
	if err != nil {
		return fmt.Errorf("failed  to open stream: %v", err)
	}

	// Read the file in 64KB chunks and stream them
	buffer := make([]byte, StreamChunkSize)
	var bytesSent int64 = 0

	for bytesSent < StorageChunkSize {
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
