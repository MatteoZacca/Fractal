package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/MatteoZacca/distributed-file-system/pb"
	"github.com/spf13/cobra"
)

const StorageChunkSize = 64 * 1024 * 1024 // 64MB
const StreamChunkSize = 64 * 1024         // 64KB

var createCmd = &cobra.Command{
	Use:   "create [filepath]",
	Short: "create and upload a file to the cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]
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

		// Connect to the NameNode
		masterClient, conn, err := getNameNodeClient()
		if err != nil {
			log.Fatalf("NameNode connection failed: %v", err)
		}
		defer conn.Close()

		log.Println("Requesting blueprint...")
		res, err := masterClient.CreateFile(context.Background(), &pb.CreateFileRequest{
			FilePath: fileName,
			FileSize: fileSize,
		})

		if err != nil {
			log.Fatalf("NameNode rejected upload: %v", err)
		}

		chunksMapping := res.ChunkLocations
		log.Printf("Blueprint received! File split into %d chunks.", len(chunksMapping))

		// Stream to DataNodes
		var chunkIDs []string
		for chunkID, nodeList := range res.ChunkLocations {
			chunkIDs = append(chunkIDs, chunkID)

			for _, dataNodeIP := range nodeList.WorkerIps {
				log.Printf("Streaming %s to DataNode at %s...", chunkID, resolveLocalAddress(dataNodeIP))

				file.Seek(0, io.SeekStart)

				if err := uploadChunkToDataNode(file, chunkID, dataNodeIP); err != nil {
					log.Fatalf("Failed to upload to %s: %v", dataNodeIP, err)
				}
			}
		}

		// Commit the file
		_, err = masterClient.CommitFile(context.Background(), &pb.CommitFileRequest{
			FilePath:       fileName,
			ChunkIds:       chunkIDs,
			ChunkLocations: res.ChunkLocations,
		})
		if err != nil {
			log.Fatalf("Failed to commit: %v", err)
		}
		log.Printf("Success! File %s is safely stored.", fileName)
	},
}

func init() {
	rootCmd.AddCommand(createCmd)
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
