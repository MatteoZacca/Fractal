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

func UploadFile(localPath string, targetFileName string) error {
	file, err := os.Open(localPath)
	if err != nil {
		fmt.Errorf("could not open file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("could not get file info: %v", err)
	}
	fileSize := fileInfo.Size()

	log.Printf("Starting upload for '%s' (size: %d bytes)", targetFileName, fileSize)

	// Connect to the NameNode
	masterClient, conn, err := getNameNodeClient()
	if err != nil {
		return fmt.Errorf("failed to connect to NameNode: %v", err)
	}
	defer conn.Close()

	res, err := masterClient.CreateFile(context.Background(), &pb.CreateFileRequest{
		FilePath: targetFileName,
		FileSize: fileSize,
	})
	if err != nil {
		return fmt.Errorf("NameNode rejected upload: %v", err)
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
		err := uploadChunkWithQuorum(localPath, startOffset, chunkID, nodeList.WorkerIps)
		if err != nil {
			fmt.Errorf("FAILURE: something in uploadChunkWithQuorum went wrong...")
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
		return fmt.Errorf("failed to commit to NameNode: %v", err)
	}
	log.Printf("SUCCESS: file %s is safely stored.", targetFileName)

	return nil
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
