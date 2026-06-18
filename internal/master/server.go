package master

import (
	"context"
	"fmt"
	"time"

	"github.com/MatteoZacca/Fractal/pb"
)

// Master
type NameNode struct {
	pb.UnimplementedMasterServiceServer
	Metadata *MetadataStore
}

const replicationFactor = 3

// DataNode -> NameNode
func (n *NameNode) SendHeartbeat(ctx context.Context, req *pb.HeartbeatMsg) (*pb.StandardResponse, error) {
	n.Metadata.mu.Lock()
	defer n.Metadata.mu.Unlock()

	n.Metadata.DataNodes[req.NodeId] = &DataNode{
		NodeID:        req.NodeId,
		Address:       req.Address,
		RackID:        "rack-1", // hardcoded for now until HDFS logic is implemented
		LastHeartbeat: time.Now(),
	}

	return &pb.StandardResponse{Success: true}, nil
}

// Client -> NameNode
func (n *NameNode) GetFileLocations(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	// Multiple clients can read the notebook at the exact same time safely
	n.Metadata.mu.RLock()
	defer n.Metadata.mu.RUnlock()

	chunkIDs, exists := n.Metadata.Files[req.FilePath]
	if !exists {
		return nil, fmt.Errorf("file %s not found in the system", req.FilePath)
	}

	responseMap := make(map[string]*pb.NodeList)

	for _, chunkID := range chunkIDs {
		dataNodesIDs := n.Metadata.ChunkLocations[chunkID]
		var dataNodeIPs []string

		for _, nodeID := range dataNodesIDs {
			if dataNodeInfo, isOnline := n.Metadata.DataNodes[nodeID]; isOnline {
				dataNodeIPs = append(dataNodeIPs, dataNodeInfo.Address)
			}
		}

		responseMap[chunkID] = &pb.NodeList{WorkerIps: dataNodeIPs}
	}

	return &pb.GetFileResponse{
		ChunkLocations: responseMap,
	}, nil
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
		dataNodeIPs, err := n.Metadata.AllocateDataNodes(replicationFactor)
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

// Client -> NameNode
func (n *NameNode) CommitFile(ctx context.Context, req *pb.CommitFileRequest) (*pb.StandardResponse, error) {
	n.Metadata.mu.Lock()
	defer n.Metadata.mu.Unlock()

	n.Metadata.Files[req.FilePath] = req.ChunkIds

	for chunkID, nodeIDs := range req.ChunkLocations {
		n.Metadata.ChunkLocations[chunkID] = nodeIDs.WorkerIps
	}

	err := n.Metadata.SaveToDisk("namespace.json")
	if err != nil {
		return nil, fmt.Errorf("failed to save metadata to disk: %v", err)
	}

	return &pb.StandardResponse{Success: true}, nil

}
