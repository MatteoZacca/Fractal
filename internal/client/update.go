package client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/MatteoZacca/Fractal/pb"
)

func UpdateFile(localPath string, dockerFileName string) error {
	versionStamp := time.Now().Unix()
	tmpName := fmt.Sprintf("v%d_%s", versionStamp, dockerFileName)

	log.Printf("Start updating '%s' (Version ID: %d)", dockerFileName, versionStamp)

	// Connect to NameNode
	masterClient, conn, err := getNameNodeClient()
	if err != nil {
		return fmt.Errorf("failed to connect to NameNode: %v", err)
	}

	oldChunks, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
		FilePath: dockerFileName,
	})
	if err != nil {
		conn.Close()
		return fmt.Errorf("'%s' doesn't exist. Use 'create' to upload a new file.", dockerFileName)
	}

	UploadFile(localPath, tmpName)

	_, err = masterClient.SwapFileName(context.Background(), &pb.SwapFileNameRequest{
		OldPath: tmpName,
		NewPath: dockerFileName,
	})
	if err != nil {
		conn.Close()
		fmt.Errorf("error during metadata swap: %v", err)
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

	return nil
}
