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
	DirPermissions = os.FileMode(0755) // Read/Write/Execute for owner, Read/Execute for others
)

func DownloadFile(fileName string) {
	log.Printf("Requesting download blueprint for '%s'...", fileName)

	// Ask Master for the blueprint
	masterClient, conn, err := getNameNodeClient()
	if err != nil {
		log.Fatalf("failed to connect to NameNode: %v", err)
	}
	defer conn.Close()

	res, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
		FilePath: fileName,
	})
	if err != nil {
		log.Fatalf("error localiting file: %v", err)
	}

	// Create 'downloads' directory and output file
	err = os.MkdirAll(DownloadsDir, DirPermissions)
	if err != nil {
		log.Fatalf("failed to create downloads directory: %v", err)
	}

	outputPath := filepath.Join(DownloadsDir, fileName)

	outputFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatalf("failed to create local output file: %v", err)
	}
	defer outputFile.Close()

	// Download and stitch the chunks in correct order
	totalChunks := len(res.ChunkLocations)
	log.Printf("blueprint received! file is split into %d chunks. starting assemply...", totalChunks)

	for i := 0; i < totalChunks; i++ {
		chunkID := fmt.Sprintf("%s-chunk-%d", fileName, i)
		nodeList, exists := res.ChunkLocations[chunkID]
		if !exists {
			log.Fatalf("Blueprint is missing %s!", chunkID)
		}

		chunkDownloaded := false
		for _, dataNodeIP := range nodeList.WorkerIps {
			log.Printf("Pulling %s from %s...", chunkID, dataNodeIP)
			err := downloadChunk(dataNodeIP, chunkID, outputFile)

			if err == nil {
				chunkDownloaded = true
				break
			}
			log.Printf("failed to download from %s, trying next replica...: %v", dataNodeIP, err)
		}

		if !chunkDownloaded {
			log.Fatalf("failed to download %s from all available replicas. Cluster has lost data!", chunkID)
		}
	}

	log.Printf("Success! File fully reassembled and saved as '%s'", outputPath)
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
