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

func (s *Server) saveQueue(ctx context.Context, queue *pb.Queue) error {
	conn, err := s.FDialServer(ctx, "dstore")
	if err != nil {
		return err
	}
	defer conn.Close()

	data, err := proto.Marshal(queue)
	if err != nil {
		return err
	}

	client := dspb.NewDStoreServiceClient(conn)
	res, err := client.Write(ctx, &dspb.WriteRequest{Key: fmt.Sprintf("/github.com/brotherlogic/queue/queues/%v", queue.GetName()), Value: &google_protobuf.Any{Value: data}})
	if err != nil {
		return err
	}

	if res.GetConsensus() < 0.5 {
		return fmt.Errorf("could not get read consensus (%v)", res.GetConsensus())
	}

	return nil
}

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

func (s *Server) getNextRunTime(name string) (int64, time.Duration) {
	ctx, cancel := utils.ManualContext("queue-"+name, time.Minute)
	defer cancel()

	queue, err := s.loadQueue(ctx, name)
	if err != nil {
		return time.Now().Add(time.Minute).Unix(), time.Minute
	}
	next := int64(-1)
	for _, entry := range queue.GetEntries() {
		if next < 0 || entry.GetRunTime() < next {
			next = entry.GetRunTime()
		}
	}

	return next, time.Second * time.Duration(queue.GetDeadline())
}

func (s *Server) runQueueElement(name string, deadline time.Duration) error {
	ctx, cancel := utils.ManualContext("queue-rqe", deadline)
	defer cancel()

	// Acquire a lock to run the queue element
	unlockKey, err := s.RunLockingElection(ctx, "queuelock-"+name)
	if err != nil {
		return err
	}

	queue, err := s.loadQueue(ctx, name)
	if err != nil {
		return err
	}

	// Get the latest entry
	var latest *pb.Entry
	for _, entry := range queue.GetEntries() {
		if latest == nil || latest.GetRunTime() < entry.GetRunTime() {
			latest = entry
		}
	}

	if time.Since(time.Unix(latest.GetRunTime(), 0)) > 0 {
		var pt proto.Message
		err = proto.Unmarshal(latest.GetPayload().GetValue(), pt)
		if err != nil {
			return err
		}

		elems := strings.Split(queue.GetEndpoint(), "/")
		err := s.runRPC(ctx, elems[0], elems[1], pt)
		if err != nil {
			return err
		}
	}

	// Remove the entry from the queue
	var entries []*pb.Entry
	for _, entry := range queue.GetEntries() {
		if entry.GetKey() != latest.GetKey() {
			entries = append(entries, entry)
		}
	}
	queue.Entries = entries
	err = s.saveQueue(ctx, queue)
	if err != nil {
		return err
	}

	return s.ReleaseLockingElection(ctx, "queuelock-"+name, unlockKey)
}

func (s *Server) timeout(queue string, nrt int64) {
	chn := s.chanmap[queue]

	select {
	case <-chn:
		break
	case <-time.After(time.Second * time.Duration(nrt)):
		break
	}

}

func (s *Server) runQueue(queueName string) error {
	s.Log(fmt.Sprintf("Running queue: %v", queueName))
	for running := range s.running {
		if queueName == running {
			return fmt.Errorf("this queue is already running")
		}
	}

	s.running[queueName] = true

	numQueues.Set(float64(len(s.running)))

	go func() {
		for s.running[queueName] {
			nextRunTime, deadline := s.getNextRunTime(queueName)
			s.Log(fmt.Sprintf("Got nrt %v and deadline %v", nextRunTime, deadline))

			s.timeout(queueName, nextRunTime)

			err := s.runQueueElement(queueName, deadline)
			s.Log(fmt.Sprintf("Ran queue (%v) -> %v", queueName, err))
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
