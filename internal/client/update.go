package client

import (
	"context"
	"log"

	"github.com/MatteoZacca/Fractal/pb"
	"github.com/spf13/cobra"
)

var updateCmd = &cobra.Command{
	Use:   "update [filename]",
	Short: "Overwrite an existing file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		originalName := args[0]
		tmpName := "tmp_" + originalName

		log.Printf("Start updating '%s'...", originalName)

		masterClient, conn, err := getNameNodeClient()
		if err != nil {
			log.Fatalf("failed to connect to NameNode: %v", err)
		}

		oldChunks, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
			FilePath: originalName,
		})
		if err != nil {
			conn.Close()
			log.Fatalf("'%s' doesn't exist. Use 'create' to upload a new file.", originalName)
		}

		args[0] = tmpName
		createCmd.Run(cmd, args)

		_, err = masterClient.SwapFileName(context.Background(), &pb.SwapFileNameRequest{
			OldPath: tmpName,
			NewPath: originalName,
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
		log.Printf("'%s' has been correctly updated.", originalName)
	},
}

func init() {
	rootCmd.AddCommand(updateCmd)
}
