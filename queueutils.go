package main

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/brotherlogic/goserver/utils"
	google_protobuf "github.com/golang/protobuf/ptypes/any"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/proto"

	dspb "github.com/brotherlogic/dstore/proto"
	pb "github.com/brotherlogic/queue/proto"
)

func (s *Server) loadQueue(ctx context.Context, name string) (*pb.Queue, error) {
	conn, err := s.FDialServer(ctx, "dstore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := dspb.NewDStoreServiceClient(conn)
	res, err := client.Read(ctx, &dspb.ReadRequest{Key: fmt.Sprintf("/github.com/brotherlogic/queue/queues/%v", name)})
	if err != nil {
		return nil, err
	}

	if res.GetConsensus() < 0.5 {
		return nil, fmt.Errorf("could not get read consensus (%v)", res.GetConsensus())
	}

	queue := &pb.Queue{}
	err = proto.Unmarshal(res.GetValue().GetValue(), queue)
	if err != nil {
		return nil, err
	}

	return queue, nil
}

func (s *Server) getNextRunTime(name string) int64 {
	ctx, cancel := utils.ManualContext("queue-"+name, time.Minute)
	defer cancel()

	queue, err := s.loadQueue(ctx, name)
	if err != nil {
		return time.Now().Add(time.Minute).Unix()
	}
	next := int64(-1)
	for _, entry := range queue.GetEntries() {
		if next < 0 || entry.GetRunTime() < next {
			next = entry.GetRunTime()
		}
	}

	return next
}

func (s *Server) runQueue(queueName string) error {
	for running := range s.running {
		if queueName == running {
			return fmt.Errorf("This queue is already running")
		}
	}

	s.running[queueName] = true

	go func() {
		for s.running[queueName] {
			nextRunTime := s.getNextRunTime(queueName)

			// Sleep this out
			time.Sleep(time.Until(time.Unix(nextRunTime, 0)))
		}
	}()

	return nil
}

func (s *Server) runRPC(ctx context.Context, service, method string, payload proto.Message) error {
	elements := strings.Split(service, ".")
	conn, err := s.FDialServer(ctx, elements[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	clientf, err := s.buildClient(service)
	if err != nil {
		return err
	}

	fc := reflect.ValueOf(clientf)
	pieces := fc.Call([]reflect.Value{reflect.ValueOf(conn)})
	methodFunc := reflect.ValueOf(pieces[0].Interface()).MethodByName(method).Interface()

	results := reflect.ValueOf(methodFunc).Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(payload)})

	err = nil
	if results[1].Interface() != nil {
		return results[1].Interface().(error)

	}

	return nil
}

func (s *Server) buildClient(service string) (interface{}, error) {
	if val, ok := s.cmap[service]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("not quite implemented")
}

func (s *Server) writeConfig(ctx context.Context, dsc dspb.DStoreServiceClient, config *pb.Config) error {
	data, err := proto.Marshal(config)
	if err != nil {
		return err
	}
	wres, err := dsc.Write(ctx, &dspb.WriteRequest{
		Key:   CONFIG_KEY,
		Value: &google_protobuf.Any{Value: data},
	})

	if err != nil {
		return err
	}

	if wres.GetConsensus() < 0.5 {
		return fmt.Errorf("no consensus on write: %v", wres.GetConsensus())
	}
	return nil
}
