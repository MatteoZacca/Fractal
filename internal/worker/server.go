package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
)

const bufferSize = 64 * 1024 // 64KB

// DataNodeServer handles network requests and delegates physical I/O to the ChunkStore
type DataNodeServer struct {
	pb.UnimplementedWorkerServiceServer
	DataNodeID string
	ChunkStore *ChunkStore
}

// Client -> Worker
func (d *DataNodeServer) CheckChunk(ctx context.Context, req *pb.CheckChunkRequest) (*pb.CheckChunkResponse, error) {
	size, exists, err := d.ChunkStore.RequestChunkSize(req.ChunkId)
	if err != nil {
		log.Printf("[ERROR] Failed to check chunk %s: %v", req.ChunkId, err)
		return nil, err
	}

	return &pb.CheckChunkResponse{
		Exists:    exists,
		SizeBytes: size,
	}, nil
}

// Client -> Worker
func (d *DataNodeServer) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.StandardResponse, error) {
	err := d.ChunkStore.DeleteChunk(req.ChunkId)
	if err != nil {
		log.Printf("[ERROR] Failed to delete chunk %s: %v", req.ChunkId, err)
		return nil, err
	}

	log.Printf("[INFO] Successfully deleted chunk %s from disk", req.ChunkId)
	return &pb.StandardResponse{Success: true}, nil
}

// Client <- Worker
func (d *DataNodeServer) RetrieveChunk(req *pb.RetrieveChunkRequest, stream grpc.ServerStreamingServer[pb.ChunkData]) error {
	log.Printf("[INFO] Client requested download for chunk: %s", req.ChunkId)

	file, err := d.ChunkStore.OpenChunk(req.ChunkId)
	if err != nil {
		log.Printf("[ERROR] Could not open chunk %s for retrieval: %v", req.ChunkId, err)
		return err
	}
	defer file.Close()

	buffer := make([]byte, bufferSize)

	for {
		bytesRead, err := file.Read(buffer)
		if err == io.EOF {
			break // Done reading
		}
		if err != nil {
			log.Printf("[ERROR] Disk read error during streaming of %s: %v", req.ChunkId, err)
			return fmt.Errorf("failed to read chunk from disk: %v", err)
		}

		sendErr := stream.Send(&pb.ChunkData{
			ChunkId: req.ChunkId,
			Data:    buffer[:bytesRead],
		})
		if sendErr != nil {
			log.Printf("[ERROR] Network failure while streaming %s to client: %v", req.ChunkId, sendErr)
			return fmt.Errorf("failed to send chunk data: %v", sendErr)
		}
	}

	log.Printf("[SUCCESS] Finished streaming chunk %s to client", req.ChunkId)
	return nil
}

// Client -> Worker
func (d *DataNodeServer) StoreChunk(stream grpc.ClientStreamingServer[pb.ChunkData, pb.StandardResponse]) error {
	var file *os.File
	var currentChunkID string

	for {
		msg, err := stream.Recv()

		if err == io.EOF {
			if file != nil {
				file.Close()
				log.Printf("[SUCCESS] Finished receiving and storing chunk: %s", currentChunkID)
			}

			return stream.SendAndClose(&pb.StandardResponse{Success: true})
		}

		if err != nil {
			if file != nil {
				file.Close()
			}
			log.Printf("[ERROR] Network stream interrupted: %v", err)
			return fmt.Errorf("failed to receive chunk stream: %v", err)
		}

		if file == nil {
			currentChunkID = msg.ChunkId
			log.Printf("[INFO] Starting to receive stream for new chunk: %s", currentChunkID)

			file, err = d.ChunkStore.CreateChunk(currentChunkID)
			if err != nil {
				log.Printf("[ERROR] Failed to allocate disk space for chunk %s: %v", currentChunkID, err)
				return err
			}
		}

		// Write the network bytes straight to the physical disk
		_, err = file.Write(msg.Data)
		if err != nil {
			log.Printf("[ERROR] Disk write failure while saving %s: %v", currentChunkID, err)
			return fmt.Errorf("failed to write to disk: %v", err)
		}
	}
}
