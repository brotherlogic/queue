package main

import (
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	dspb "github.com/brotherlogic/dstore/proto"
	pb "github.com/brotherlogic/queue/proto"
	google_protobuf "github.com/golang/protobuf/ptypes/any"
)

func (s *Server) AddQueue(ctx context.Context, req *pb.AddQueueRequest) (*pb.AddQueueResponse, error) {
	conn, err := s.FDialServer(ctx, "dstore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := dspb.NewDStoreServiceClient(conn)

	res, err := client.Read(ctx, &dspb.ReadRequest{Key: fmt.Sprintf("/github.com/brotherlogic/queue/queues/%v", CONFIG_KEY)})
	if status.Convert(err).Code() != codes.NotFound {

		if err != nil {
			return nil, err
		}

		if res.GetConsensus() < 0.5 {
			return nil, fmt.Errorf("could not get read consensus (%v)", res.GetConsensus())
		}
	}

	config := &pb.Config{}
	err = proto.Unmarshal(res.GetValue().GetValue(), config)
	if err != nil {
		return nil, err
	}

	for _, q := range config.GetQueues() {
		if q == req.GetName() {
			return nil, status.Errorf(codes.AlreadyExists, "This queue (%v) already exists", req.GetName())
		}
	}

	queue := &pb.Queue{
		Name:     req.GetName(),
		Endpoint: req.GetEndpoint(),
		Deadline: req.GetDeadline(),
	}

	data, err := proto.Marshal(queue)
	if err != nil {
		return nil, err
	}
	wres, err := client.Write(ctx, &dspb.WriteRequest{
		Key:   fmt.Sprintf("%v/%v", QUEUE_KEY, queue.GetName()),
		Value: &google_protobuf.Any{Value: data},
	})

	if err != nil {
		return nil, err
	}

	config.Queues = append(config.Queues, queue.GetName())
	if wres.GetConsensus() < 0.5 {
		return nil, fmt.Errorf("no consensus on write: %v", res.GetConsensus())
	}

	err = s.writeConfig(ctx, client, config)
	if err != nil {
		return nil, err
	}

	// Kick off running of the queue
	s.runQueue(queue.GetName())

	return &pb.AddQueueResponse{}, nil
}
