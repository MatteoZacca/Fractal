package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/MatteoZacca/Fractal/pb"
)

const (
	StorageChunkSize = 64 * 1024 * 1024 // 64MB
	StreamChunkSize  = 64 * 1024        // 64KB
	WriteQuorum      = 2
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
		log.Fatalf("NameNode rejected upload: %v", err)
	}

	log.Printf("File split into into %d chunks", len(res.ChunkLocations))

	// Stream to DataNodes
	var chunkIDs []string
	var currentChunkIndex int64 = 0

	for chunkID, nodeList := range res.ChunkLocations {
		chunkIDs = append(chunkIDs, chunkID)
		startOffset := currentChunkIndex * StorageChunkSize

		log.Printf("Broadcasting %s to %d nodes...", chunkID, len(nodeList.WorkerIps))

		// QUORUM CONSENSUS LOGIC

		// buffered channel to collect results without blocking goroutines
		outcomes := make(chan error, len(nodeList.WorkerIps))
		var wg sync.WaitGroup

		for _, dataNodeIP := range nodeList.WorkerIps {
			wg.Add(1)
			go func(ip string) {
				defer wg.Done()
				err := uploadChunkToDataNode(localFilePath, startOffset, chunkID, ip)
				outcomes <- err
			}(dataNodeIP)
		}

		successWrites := 0
		var errors []error

		for i := 0; i < len(nodeList.WorkerIps); i++ {
			err := <-outcomes
			if err == nil {
				successWrites++
				if successWrites >= WriteQuorum {
					log.Printf("Quorum reached for %s", chunkID)
					break // DOESN'T CARE ABOUT THE 3RD NODE
				}
			} else {
				errors = append(errors, err)
			}
		}

		if successWrites < WriteQuorum {
			log.Fatalf("FAILURE: could not reach Write Quorum for %s: %v", chunkID, errors)
		}

		currentChunkIndex++
	}

	// Commit the file to NameNode
	_, err = masterClient.CommitFile(context.Background(), &pb.CommitFileRequest{
		FilePath:       targetFileName,
		ChunkIds:       chunkIDs,
		ChunkLocations: res.ChunkLocations,
	})
	if err != nil {
		log.Fatalf("failed to commit to NameNode: %v", err)
	}
	log.Printf("SUCCESS: file %s is safely stored.", targetFileName)
}

func uploadChunkToDataNode(localFilePath string, startOffset int64, chunkID string, dataNodeIP string) error {

	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("could not open file: %v", err)
	}
	defer file.Close()

	_, err = file.Seek(startOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek file playhead: %v", err)
	}

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
			return fmt.Errorf("failed to read the disk: %v", err)
		}

		// Send the chunk over the network
		err = stream.Send(&pb.ChunkData{
			ChunkId: chunkID,
			Data:    buffer[:bytesRead],
		})
		if err != nil {
			return fmt.Errorf("failed to stream data over the network: %v", err)
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
