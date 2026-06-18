package worker

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/MatteoZacca/Fractal/pb"

	"google.golang.org/grpc"
)

const (
	bufferSize = 64 * 1024 // 64KB
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	NodeId  string
	DataDir string
}

func (s *WorkerServer) StoreChunk(stream grpc.ClientStreamingServer[pb.ChunkData, pb.StandardResponse]) error {
	var file *os.File
	var chunkPath string

	for {
		msg, err := stream.Recv()

		if err == io.EOF {
			if file != nil {
				file.Close()
			}

			return stream.SendAndClose(&pb.StandardResponse{
				Success: true,
			})
		}

		if err != nil {
			return fmt.Errorf("failed to receive chunk stream")
		}

		if file == nil {
			chunkPath = filepath.Join(s.DataDir, msg.ChunkId+".dat")
			file, err = os.Create(chunkPath)
			if err != nil {
				return fmt.Errorf("failed to create file on disk: %v", err)
			}
		}

		_, err = file.Write(msg.Data)
		if err != nil {
			return fmt.Errorf("failed to write to disk: %v", err)
		}
	}
}

func (s *WorkerServer) RetrieveChunk(req *pb.RetrieveChunkRequest, stream grpc.ServerStreamingServer[pb.ChunkData]) error {
	chunkPath := filepath.Join(s.DataDir, req.ChunkId+".dat")

	file, err := os.Open(chunkPath)
	if err != nil {
		return fmt.Errorf("chunk not found on this node: %v", err)
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
