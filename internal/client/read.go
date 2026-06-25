package client

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/MatteoZacca/Fractal/pb"
	"github.com/spf13/cobra"
)

var readCmd = &cobra.Command{
	Use:   "read `filename`",
	Short: "Download and reassemble a file from the Fractal cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
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

		// Create the final output file  locally
		outputName := "download..." + fileName
		outFile, err := os.Create(outputName)
		if err != nil {
			log.Fatalf("failed to create local output file: %v", err)
		}
		defer outFile.Close()

		// Download and stithc the chunks in correct order
		totalChunks := len(res.ChunkLocations)
		log.Printf("blueprint received! file is split into %d chunks. starting assemply...", totalChunks)

		for i := 0; i < totalChunks; i++ {
			chunkID := fmt.Sprintf("%s-chunk-%d", fileName, i)
			nodeList, exists := res.ChunkLocations[chunkID]
			if !exists {
				log.Fatalf("Blueprint is missing %s!", chunkID)
			}

			chunkDownloaded := false
			for _, workerIP := range nodeList.WorkerIps {
				log.Printf("Pulling %s from %s...", chunkID, workerIP)
				err := downloadChunk(workerIP, chunkID, outFile) // TO DO

				if err == nil {
					chunkDownloaded = true
					break
				}
				log.Printf("failed to download from %s, trying next replica...: %v", workerIP, err)

				if !chunkDownloaded {
					log.Fatalf("CRITICAL: Failed to download %s from all available replicas. Cluster has lost data!", chunkID)
				}
			}

			log.Printf("Success! File fully reassembled and saved as '%s'", outputName)

		}
	},
}

func downloadChunk() {
	// TO DO
}
