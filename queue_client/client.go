package queue_client

import (
	"context"

	pbgs "github.com/brotherlogic/goserver"
	pb "github.com/brotherlogic/queue/proto"
)

type QueueClient struct {
	Gs   *pbgs.GoServer
	Test bool
}

func (c *QueueClient) AddQueueItem(ctx context.Context, req *pb.AddQueueItemRequest) (*pb.AddQueueItemResponse, error) {
	conn, err := c.Gs.FDialServer(ctx, "queue")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewQueueServiceClient(conn)
	return client.AddQueueItem(ctx, req)
}
