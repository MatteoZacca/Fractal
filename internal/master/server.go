package master

import (
	"context"
	"fmt"
	"time"

	"github.com/MatteoZacca/distributed-file-system/pb"
)

type NameNode struct {
	pb.UnimplementedMasterServiceServer
	Metadata *MetadataStore
}

func (s *NameNode) SendHeartbeat(ctx context.Context, req *pb.HeartbeatMsg) (*pb.StandardResponse, error) {
	s.Metadata.mu.Lock()
	defer s.Metadata.mu.Unlock()

	s.Metadata.DataNodes[req.NodeId] = &DataNode{
		NodeID:        req.NodeId,
		Address:       req.Address,
		RackID:        "rack-1", // hardcoded for now until HDFS logic is implemented
		LastHeartbeat: time.Now(),
	}

	return &pb.StandardResponse{Success: true}, nil
}

func (s *NameNode) GetFileLocations(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	// Multiple clients can read the notebook at the exact same time safely
	s.Metadata.mu.RLock()
	defer s.Metadata.mu.RUnlock()

	chunkIDs, exists := s.Metadata.Files[req.FilePath]
	if !exists {
		return nil, fmt.Errorf("file %s not found in the system", req.FilePath)
	}

	responseMap := make(map[string]*pb.NodeList)

	for _, chunkID := range chunkIDs {
		dataNodesIDs := s.Metadata.ChunkLocations[chunkID]
		var dataNodeIPs []string

		for _, nodeID := range dataNodesIDs {
			if dataNodeInfo, isOnline := s.Metadata.DataNodes[nodeID]; isOnline {
				dataNodeIPs = append(dataNodeIPs, dataNodeInfo.Address)
			}
		}

		responseMap[chunkID] = &pb.NodeList{WorkerIps: dataNodeIPs}
	}

	return &pb.GetFileResponse{
		ChunkLocations: responseMap,
	}, nil
}
