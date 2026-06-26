package client

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/MatteoZacca/Fractal/pb"
	"github.com/spf13/cobra"
)

var updateCmd = &cobra.Command{
	Use:   "update [path\\to\\local\\file]",
	Short: "Overwrite an existing file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		localPath := args[0]
		dockerFileName := filepath.Base(localPath)

		versionStamp := time.Now().Unix()
		tmpName := fmt.Sprintf("v%d_%s", versionStamp, dockerFileName)

		log.Printf("Start updating '%s' (Version ID: %d)", dockerFileName, versionStamp)

		masterClient, conn, err := getNameNodeClient()
		if err != nil {
			log.Fatalf("failed to connect to NameNode: %v", err)
		}

		oldChunks, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
			FilePath: dockerFileName,
		})
		if err != nil {
			conn.Close()
			log.Fatalf("'%s' doesn't exist. Use 'create' to upload a new file.", dockerFileName)
		}

		uploadFile(localPath, tmpName)

		_, err = masterClient.SwapFileName(context.Background(), &pb.SwapFileNameRequest{
			OldPath: tmpName,
			NewPath: dockerFileName,
		})
		if err != nil {
			conn.Close()
			log.Fatalf("error during metadata swap: %v", err)
		}

		log.Printf("Update completed! Cleaning up old orphaned chunks in the background...")
		for chunkID, nodeList := range oldChunks.ChunkLocations {
			for _, datanodeIP := range nodeList.WorkerIps {
				err := sendDeleteToWorker(datanodeIP, chunkID)
				if err != nil {
					log.Printf("failed to delete old chunk '%s' from '%s'", chunkID, datanodeIP)
				}
			}

		}

		conn.Close()
		log.Printf("'%s' has been correctly updated.", dockerFileName)
	},
}

func init() {
	rootCmd.AddCommand(updateCmd)
}
