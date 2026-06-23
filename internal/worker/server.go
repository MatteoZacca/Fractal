package worker

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/MatteoZacca/Fractal/pb"
	"google.golang.org/grpc"
)

const bufferSize = 64 * 1024 // 64KB

// WorkerServer handles network requests and delegates storage to DiskManager
type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	NodeId      string
	DiskManager *DiskManager
}

// Client -> Worker (Upload)
func (s *WorkerServer) StoreChunk(stream grpc.ClientStreamingServer[pb.ChunkData, pb.StandardResponse]) error {
	var file *os.File

	for {
		msg, err := stream.Recv()

		if err == io.EOF {
			if file != nil {
				file.Close()
			}

			return stream.SendAndClose(&pb.StandardResponse{Success: true})
		}

		if err != nil {
			return fmt.Errorf("failed to receive chunk stream: %v", err)
		}

		// Ask DiskManager to create the file the first time the system receives bytes
		if file == nil {
			file, err = s.DiskManager.CreateChunk(msg.ChunkId)
			if err != nil {
				return err
			}
		}

		// Write the network bytes straight to the physical disk
		_, err = file.Write(msg.Data)
		if err != nil {
			return fmt.Errorf("failed to write to disk: %v", err)
		}
	}
}

// Client <- Worker (Download)
func (s *WorkerServer) RetrieveChunk(req *pb.RetrieveChunkRequest, stream grpc.ServerStreamingServer[pb.ChunkData]) error {
	// Ask DiskManager to fetch the file pointer
	file, err := s.DiskManager.OpenChunk(req.ChunkId)
	if err != nil {
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
			return fmt.Errorf("failed to read chunk from disk: %v", err)
		}

		// Send the piece over the network
		sendErr := stream.Send(&pb.ChunkData{
			ChunkId: req.ChunkId,
			Data:    buffer[:bytesRead],
		})
		if sendErr != nil {
			return fmt.Errorf("failed to send chunk data: %v", sendErr)
		}
	}
	return nil
}

// Client -> Worker (Delete)
func (s *WorkerServer) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.StandardResponse, error) {
	// Ask DiskManager to destroy the file
	err := s.DiskManager.DeleteChunk(req.ChunkId)
	if err != nil {
		return nil, err
	}

	return &pb.StandardResponse{Success: true}, nil
}
