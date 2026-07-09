// Its only job is to talk to the network (gRPC). It receives the request, unpacks it,
// calls internal functions, and packs up the response.

package master

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/MatteoZacca/Fractal/pb"
)

type NameNode struct {
	pb.UnimplementedMasterServiceServer
	State       *ClusterState
	FSImagePath string
}

const (
	ReplicationFactor = 3
)

// Client -> NameNode
func (n *NameNode) CommitFile(ctx context.Context, req *pb.CommitFileRequest) (*pb.StandardResponse, error) {
	n.State.mu.Lock()
	n.State.Files[req.FilePath] = req.ChunkIds
	for chunkID, nodeIDs := range req.ChunkLocations {
		n.State.ChunkLocations[chunkID] = nodeIDs.WorkerIps
	}
	n.State.mu.Unlock()

	err := n.State.SaveToDisk(n.FSImagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to save metadata to disk: %v", err)
	}

	return &pb.StandardResponse{Success: true}, nil
}

// Client -> NomeNode
func (n *NameNode) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	var totalChunks int = int(req.FileSize / ChunkSize)
	if req.FileSize%ChunkSize != 0 {
		totalChunks++
	}

	// 3. Create the empty blueprint to hand back to the Client
	blueprint := make(map[string]*pb.NodeList)

	for i := 0; i < totalChunks; i++ {
		// Generate a unique ID (e.g., "thesis.pdf-chunk-0")
		chunkID := fmt.Sprintf("%s-chunk-%d", req.FilePath, i)

		// Ask our Allocator logic for 2 healthy workers
		dataNodeIPs, err := n.State.AllocateDataNodes(ReplicationFactor)
		if err != nil {
			return nil, err // Fails the whole upload if the cluster is unhealthy
		}

		blueprint[chunkID] = &pb.NodeList{WorkerIps: dataNodeIPs}
	}
	// 5. Hand the blueprint to the Client
	return &pb.CreateFileResponse{
		ChunkLocations: blueprint,
	}, nil
}

func (n *NameNode) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.StandardResponse, error) {
	n.State.mu.Lock()

	chunkIDs, exists := n.State.Files[req.FilePath]
	if !exists {
		n.State.mu.Unlock()
		return nil, fmt.Errorf("file %s not found in the system", req.FilePath)
	}

	// Erase the physical chunk mappings (3x replicas)
	for _, chunkID := range chunkIDs {
		delete(n.State.ChunkLocations, chunkID)
	}

	// Erase the logical file mapping
	delete(n.State.Files, req.FilePath)

	n.State.mu.Unlock()

	err := n.State.SaveToDisk(n.FSImagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to save metadata: %v", err)
	}

	return &pb.StandardResponse{Success: true}, nil
}

// Client -> NameNode
func (n *NameNode) GetFileLocations(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	// Multiple clients can read the notebook at the exact same time safely
	n.State.mu.RLock()
	defer n.State.mu.RUnlock()

	chunkIDs, exists := n.State.Files[req.FilePath]
	if !exists {
		return nil, fmt.Errorf("file %s not found in the system", req.FilePath)
	}

	responseMap := make(map[string]*pb.NodeList)

	for _, chunkID := range chunkIDs {
		dataNodeIPs := n.State.ChunkLocations[chunkID]
		responseMap[chunkID] = &pb.NodeList{WorkerIps: dataNodeIPs}
	}

	return &pb.GetFileResponse{ChunkLocations: responseMap}, nil
}

// Client <-> NameNode
func (n *NameNode) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	n.State.mu.RLock()
	defer n.State.mu.RUnlock()

	var fileList []*pb.FileInfo

	for fileName, chunkIDs := range n.State.Files {
		fileList = append(fileList, &pb.FileInfo{
			FileName:   fileName,
			ChunkCount: int32(len(chunkIDs)),
		})
	}

	return &pb.ListFilesResponse{
		Files: fileList,
	}, nil

}

// DataNode -> NameNode
func (n *NameNode) SendHeartbeat(ctx context.Context, req *pb.HeartbeatMsg) (*pb.StandardResponse, error) {
	n.State.mu.Lock()

	n.State.DataNodes[req.NodeId] = &DataNode{
		NodeID:        req.NodeId,
		Address:       req.Address,
		RackID:        req.RackId,
		LastHeartbeat: time.Now(),
	}

	n.State.mu.Unlock()

	err := n.State.SaveToDisk(n.FSImagePath)
	if err != nil {
		log.Printf("failed to save heartbeat to disk: %v", err)
	}

	return &pb.StandardResponse{Success: true}, nil
}

func (n *NameNode) SwapFileName(ctx context.Context, req *pb.SwapFileNameRequest) (*pb.StandardResponse, error) {
	n.State.mu.Lock()

	newChunks, exists := n.State.Files[req.OldPath]
	if !exists {
		n.State.mu.Unlock()
		return nil, fmt.Errorf("file %s not found in namespace", req.OldPath)
	}

	oldChunks, oldExists := n.State.Files[req.NewPath]

	n.State.Files[req.NewPath] = newChunks
	delete(n.State.Files, req.OldPath)

	if oldExists {
		for _, chunkID := range oldChunks {
			delete(n.State.ChunkLocations, chunkID)
		}
	}

	n.State.mu.Unlock()

	err := n.State.SaveToDisk(n.FSImagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to save metadata: %v", err)
	}

	return &pb.StandardResponse{Success: true}, nil
}
