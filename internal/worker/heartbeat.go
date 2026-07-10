package worker

import (
	"context"
	"log"
	"time"

	"github.com/MatteoZacca/Fractal/pb"
)

const HeartbeatInterval = 3 * time.Second

// StartHeartbeat runs an infinite loop that pings the NameNode to prove this DataNode is alive.
func StartHeartbeat(client pb.MasterServiceClient, dataNodeID string, dataNodeAddress string, nameNodeAddress string, rackID string) {
	ticker := time.NewTicker(HeartbeatInterval)

	for {
		<-ticker.C // pauses the loop until 3 seconds have passed

		_, err := client.SendHeartbeat(context.Background(), &pb.HeartbeatMsg{
			NodeId:         dataNodeID,
			Address:        dataNodeAddress,
			DiskUsage:      0,          // TODO -> Load Balancing
			DiskCapacity:   0,          // TODO -> Load Balancing
			StoredChunkIds: []string{}, // TODO -> Garbage Collection
			RackId:         rackID,
		})

		if err != nil {
			log.Printf("[ERROR] Heartbeat failed: could not reach NameNode at [%s]: %v", nameNodeAddress, err)
		} else {
			log.Printf("[INFO] Heartbeat successfully acknowledged by NameNode.")
		}
	}
}
