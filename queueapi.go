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
	if req.GetType() == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "You must supply a type")
	}

	conn, err := s.FDialServer(ctx, "dstore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := dspb.NewDStoreServiceClient(conn)

	res, err := client.Read(ctx, &dspb.ReadRequest{Key: CONFIG_KEY})
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
		Type:     req.GetType(),
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

func (s *Server) AddQueueItem(ctx context.Context, req *pb.AddQueueItemRequest) (*pb.AddQueueItemResponse, error) {
	queue, err := s.loadQueue(ctx, req.GetQueueName())
	if err != nil {
		return nil, err
	}

	queue.Entries = append(queue.Entries, &pb.Entry{
		Key:     req.GetKey(),
		Payload: req.GetPayload(),
		RunTime: req.GetRunTime(),
	})
	err = s.saveQueue(ctx, queue)
	if err != nil {
		return nil, err
	}

	// Trip another pass at the queue
	if ch, ok := s.chanmap[req.GetQueueName()]; ok {
		ch <- true
	} else {
		s.Log(fmt.Sprintf("Cannot find channel for %v", req.GetQueueName()))
	}

	return &pb.AddQueueItemResponse{}, nil
}

func (s *Server) DeleteQueueItem(ctx context.Context, req *pb.DeleteQueueItemRequest) (*pb.DeleteQueueItemResponse, error) {
	queue, err := s.loadQueue(ctx, req.GetQueueName())
	if err != nil {
		return nil, err
	}

	var latest *pb.Entry
	for _, q := range queue.GetEntries() {
		if latest == nil || q.GetRunTime() < latest.GetRunTime() {
			latest = q
		}
	}

	var entries []*pb.Entry
	for _, q := range queue.GetEntries() {
		if q.GetKey() != latest.GetKey() || q.GetRunTime() != latest.GetRunTime() {
			entries = append(entries, q)
		}
	}
	queue.Entries = entries

	return &pb.DeleteQueueItemResponse{}, s.saveQueue(ctx, queue)
}
