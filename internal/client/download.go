package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/MatteoZacca/Fractal/pb"
)

const (
	DownloadsDir   = "downloads"
	DirPermissions = 0755 // Read/Write/Execute for owner, Read/Execute for others
)

func DownloadFile(fileName string) error {
	log.Printf("Requesting download blueprint for '%s'...", fileName)

	// Ask Master for the blueprint
	masterClient, conn, err := getNameNodeClient()
	if err != nil {
		return fmt.Errorf("failed to connect to NameNode: %v", err)
	}
	defer conn.Close()

	res, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
		FilePath: fileName,
	})
	if err != nil {
		return fmt.Errorf("error locating file: %v", err)
	}

	// Create 'downloads' directory and output file
	err = os.MkdirAll(DownloadsDir, DirPermissions)
	if err != nil {
		return fmt.Errorf("failed to create downloads directory: %v", err)
	}

	outputPath := filepath.Join(DownloadsDir, fileName)
	tmpOutputPath := outputPath + ".tmp"

	outputFile, err := os.Create(tmpOutputPath)
	if err != nil {
		return fmt.Errorf("failed to create local temporary output file: %v", err)
	}
	defer outputFile.Close()

	// Download and stitch the chunks in correct order
	totalChunks := len(res.ChunkLocations)
	log.Printf("Blueprint received! file is split into %d chunks. starting assembly...", totalChunks)

	var currentChunkIndex int64 = 0

	for i := range totalChunks {
		var targetChunkID string
		var targetNodes []string
		suffix := fmt.Sprintf("-chunk-%d", i)

		for id, nodeList := range res.ChunkLocations {
			if len(id) >= len(suffix) && id[len(id)-len(suffix):] == suffix {
				targetChunkID = id
				targetNodes = nodeList.WorkerIps
				break
			}
		}

		if targetChunkID == "" {
			outputFile.Close()
			os.Remove(tmpOutputPath)
			return fmt.Errorf("Blueprint is missing chunk index %d!", i)
		}

		startOffset := currentChunkIndex * StorageChunkSize

		log.Printf("Pulling %s (enforcing R=2 Quorum)...", targetChunkID)

		// Pass outputPath so Read Repair can read the bytes back from the hard drive if needed
		err := downloadChunkWithQuorum(outputPath, startOffset, targetChunkID, targetNodes, outputFile)

		if err != nil {
			outputFile.Close()
			os.Remove(tmpOutputPath)
			return fmt.Errorf("Cluster failed to serve data: %v", err)
		}

		currentChunkIndex++
	}

	outputFile.Close()

	err = os.Rename(tmpOutputPath, outputPath)
	if err != nil {
		return fmt.Errorf("failed to remove tmp extension in downloaded file: %v", err)
	}
	log.Printf("Success! File fully reassembled and saved as '%s'", outputPath)

	return nil
}

// Helper function to stream bytes directly from the network to the hard drive
func downloadChunk(dataNodeIP string, chunkID string, outFile *os.File) error {
	dataNodeClient, conn, err := getDataNodeClient(dataNodeIP)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := dataNodeClient.RetrieveChunk(context.Background(), &pb.RetrieveChunkRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return err
	}

	// Stream the bytes from network directly to the local file
	for {
		chunkData, err := stream.Recv()
		if err == io.EOF {
			break // The DataNode finished sending this chunk
		}

		if err != nil {
			return fmt.Errorf("network stream interrupted: %v", err)
		}

		// Append the bytes
		_, err = outFile.Write(chunkData.Data)
		if err != nil {
			return fmt.Errorf("failed to write to local disk: %v", err)
		}
	}
	return nil
}
