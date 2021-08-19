package main

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/brotherlogic/goserver/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	dspb "github.com/brotherlogic/dstore/proto"
	pb "github.com/brotherlogic/queue/proto"
	google_protobuf "github.com/golang/protobuf/ptypes/any"
)

var (
	queueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_queuelength",
		Help: "The number of running queues",
	}, []string{"queue_name"})
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
	res, err := client.Write(ctx, &dspb.WriteRequest{Key: fmt.Sprintf("%v/%v", QUEUE_KEY, queue.GetName()), Value: &google_protobuf.Any{Value: data}})
	if err != nil {
		return err
	}

	if res.GetConsensus() < 0.5 {
		return fmt.Errorf("could not get write consensus (%v)", res.GetConsensus())
	}

	queueLength.With(prometheus.Labels{"queue_name": queue.GetName()}).Set(float64(len(queue.GetEntries())))

	return nil
}

func (s *Server) loadQueue(ctx context.Context, name string) (*pb.Queue, error) {
	conn, err := s.FDialServer(ctx, "dstore")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := dspb.NewDStoreServiceClient(conn)
	res, err := client.Read(ctx, &dspb.ReadRequest{Key: fmt.Sprintf("%v/%v", QUEUE_KEY, name)})
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

	queueLength.With(prometheus.Labels{"queue_name": queue.GetName()}).Set(float64(len(queue.GetEntries())))

	if queue.GetName() == "record_adder" {
		queue.Type = "recordadder.ProcAddedRequest"
	}

	return queue, nil
}

func (s *Server) getNextRunTime(name string) (time.Time, time.Duration) {
	ctx, cancel := utils.ManualContext("queue-"+name, time.Minute)
	defer cancel()

	queue, err := s.loadQueue(ctx, name)
	if err != nil {
		return time.Now().Add(time.Minute), time.Minute
	}
	next := int64(-1)
	for _, entry := range queue.GetEntries() {
		if next < 0 || entry.GetRunTime() < next {
			next = entry.GetRunTime()
		}
	}

	if next == -1 {
		return time.Now().Add(time.Hour), time.Second * time.Duration(queue.GetDeadline())
	}

	return time.Unix(next, 0), time.Second * time.Duration(queue.GetDeadline())
}

func (s *Server) runQueueElement(name string, deadline time.Duration) error {
	ctx, cancel := utils.ManualContext("queue-rqe", deadline)
	defer cancel()

	// Acquire a lock to run the queue element
	unlockKey, err := s.RunLockingElection(ctx, "queuelock-"+name)
	if err != nil {
		time.Sleep(time.Minute)
		return err
	}
	defer s.ReleaseLockingElection(ctx, "queuelock-"+name, unlockKey)

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
		s.Log(fmt.Sprintf("Running request %v since %v", time.Unix(latest.GetRunTime(), 0), time.Since(time.Unix(latest.GetRunTime(), 0))))
		t, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(queue.GetType()))
		if err != nil {
			return fmt.Errorf("error finding proto (%v) -> %v", queue.GetType(), err)
		}
		pt := t.New().Interface()
		err = proto.Unmarshal(latest.GetPayload().GetValue(), pt)
		if err != nil {
			return err
		}

		elems := strings.Split(queue.GetEndpoint(), "/")
		err = s.runRPC(ctx, elems[0], elems[1], pt)
		if err != nil {
			return err
		}

		// Remove the entry from the queue - do a reload to stop stomping on changes
		queue, err = s.loadQueue(ctx, name)
		if err != nil {
			return err
		}

		var entries []*pb.Entry
		for _, entry := range queue.GetEntries() {
			if entry.GetKey() != latest.GetKey() || latest.GetRunTime() != entry.GetRunTime() {
				entries = append(entries, entry)
			}
		}

		queue.Entries = entries
		err = s.saveQueue(ctx, queue)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) timeout(queue string, nrt time.Time) {
	chn := s.chanmap[queue]

	select {
	case <-chn:
		break
	case <-time.After(time.Until(nrt)):
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
	s.chanmap[queueName] = make(chan bool, 100)

	numQueues.Set(float64(len(s.running)))

	go func() {
		for s.running[queueName] {
			nextRunTime, deadline := s.getNextRunTime(queueName)

			s.timeout(queueName, nextRunTime)

			err := s.runQueueElement(queueName, deadline)
			if status.Convert(err).Code() != codes.AlreadyExists {
				s.Log(fmt.Sprintf("Ran queue (%v) -> %v", queueName, err))
			}
			if err != nil {
				time.Sleep(time.Minute)
			}
		}
	}()

	return nil
}

var (
	queueCode = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_rpc_code",
		Help: "The number of running queues",
	}, []string{"code"})
)

func (s *Server) runRPC(ctx context.Context, service, method string, payload proto.Message) error {
	s.Log(fmt.Sprintf("Running %v, %v -> %v", service, method, payload))
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

	s.Log(fmt.Sprintf("Turned into %v -> %+v (%v),%v", reflect.ValueOf(payload), methodFunc, method, reflect.ValueOf(methodFunc)))

	results := reflect.ValueOf(methodFunc).Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(payload)})

	err = nil
	if results[1].Interface() != nil {
		err = results[1].Interface().(error)
	}

	queueCode.With(prometheus.Labels{"code": fmt.Sprintf("%v", status.Convert(err).Code())}).Inc()

	return err
}

func (s *Server) buildClient(service string) (interface{}, error) {
	s.Log(fmt.Sprintf("BUILT FOR %v", service))
	if val, ok := s.cmap[service]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("%v is not quite implemented", service)
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
