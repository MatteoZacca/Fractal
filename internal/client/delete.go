package client

import (
	"context"
	"log"

	"github.com/MatteoZacca/Fractal/pb"
)

func DeleteFile(fileName string) {
	log.Printf("Initiating deletion for '%s'...", fileName)

	masterClient, conn, err := getNameNodeClient()
	if err != nil {
		log.Fatalf("failed to connect to NameNode: %v", err)
	}
	defer conn.Close()

	// Get the Blueprint so the system knows which DataNodes to attack
	res, err := masterClient.GetFileLocations(context.Background(), &pb.GetFileRequest{
		FilePath: fileName,
	})
	if err != nil {
		log.Fatalf("error locating file: %v", err)
	}

	// Loop through the blueprint and delete replicas
	for chunkID, nodeList := range res.ChunkLocations {
		for _, workerIP := range nodeList.WorkerIps {
			log.Printf("deleting %s from %s...", chunkID, workerIP)
			err := sendDeleteToWorker(workerIP, chunkID)
			if err != nil {
				log.Printf("failed to delete %s from %s: %v", chunkID, workerIP, err)
			}
		}
	}

	// Tell the Master to forget the file forever
	_, err = masterClient.DeleteFile(context.Background(), &pb.DeleteFileRequest{
		FilePath: fileName,
	})
	if err != nil {
		log.Fatalf("failed to finalize deletion on Master: %v", err)
	}

	log.Printf("Success! '%s' has been completely burnt from the system.", fileName)
}

// Helper function to send the kill command to specific DataNodes
func sendDeleteToWorker(workerIP string, chunkID string) error {
	workerClient, conn, err := getDataNodeClient(workerIP)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = workerClient.DeleteChunk(context.Background(), &pb.DeleteChunkRequest{
		ChunkId: chunkID,
	})
	return err
}
