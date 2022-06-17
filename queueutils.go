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

	releases = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_releases",
		Help: "The number of running queues",
	}, []string{"error", "queue"})

	chanLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_chanlength",
		Help: "The number of running queues",
	}, []string{"queue_name"})

	errors = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_errors",
		Help: "The number of running queues",
	}, []string{"queue_name"})
	runtime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_runtime",
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

	s.CtxLog(ctx, fmt.Sprintf("Written %v:%v [%v]", res.GetHash(), res.GetTimestamp(), len(queue.GetEntries())))

	if res.GetConsensus() < 0.5 {
		return fmt.Errorf("could not get write for queue %v consensus (%v)", queue.GetName(), res.GetConsensus())
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
		return nil, fmt.Errorf("could not find read consensus for queue %v (%v)", name, res.GetConsensus())
	}

	s.CtxLog(ctx, fmt.Sprintf("Read %v:%v", res.GetHash(), res.GetTimestamp()))

	queue := &pb.Queue{}
	err = proto.Unmarshal(res.GetValue().GetValue(), queue)
	if err != nil {
		return nil, err
	}

	queueLength.With(prometheus.Labels{"queue_name": queue.GetName()}).Set(float64(len(queue.GetEntries())))

	/*if queue.GetName() == "sale_update" {
		s.RaiseIssue("Correcting", "Correcting sale update")
		queue.UniqueKeys = true
	}*/

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
	ctx, cancel := utils.ManualContext("queue-rqe-"+name, deadline)
	defer cancel()

	// Acquire a lock to run the queue element
	unlockKey, err := s.RunLockingElection(ctx, "queuelock-"+name, "Locking for a queue run")
	if err != nil {
		time.Sleep(time.Minute)
		return err
	}
	defer func() {
		lerr := s.ReleaseLockingElection(ctx, "queuelock-"+name, unlockKey)
		releases.With(prometheus.Labels{"queue": name, "error": fmt.Sprintf("%v", lerr)}).Inc()
	}()
	queue, err := s.loadQueue(ctx, name)
	if err != nil {
		return err
	}

	// Get the latest entry
	var latest *pb.Entry
	for _, entry := range queue.GetEntries() {
		if latest == nil || entry.GetRunTime() < latest.GetRunTime() {
			latest = entry
		}
	}

	if latest == nil {
		s.RaiseIssue("No Entries in Queue", fmt.Sprintf("Queue %v has no entries to run", name))
		return nil
	}

	latest.State = pb.Entry_RUNNING
	err = s.saveQueue(ctx, queue)
	if err != nil {
		return err
	}

	if time.Since(time.Unix(latest.GetRunTime(), 0)) > 0 {
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
		s.DLog(ctx, fmt.Sprintf("Running %v on queue %v", latest.GetKey(), name))
		err = s.runRPC(ctx, elems[0], elems[1], pt)
		if err != nil {
			return err
		}

		// Remove the entry from the queue - do a reload to stop stomping on recent additions
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
	} else {
		return status.Errorf(codes.InvalidArgument, "nothing to run here (%v) until %v", latest.GetKey(), time.Unix(latest.GetRunTime(), 0))
	}

	return nil
}

func (s *Server) timeout(queue string, nrt time.Time) {
	chn := s.chanmap[queue]

	chanLength.With(prometheus.Labels{"queue_name": queue}).Set(float64(len(chn)))

	s.DLog(context.Background(), fmt.Sprintf("Sleeping %v for %v", queue, time.Until(nrt)))
	select {
	case <-chn:
		s.DLog(context.Background(), "Read from channel")
		break
	case <-time.After(time.Until(nrt)):
		s.DLog(context.Background(), "Time out")
		break
	}

}

func (s *Server) runQueue(queueName string) error {
	s.Log(fmt.Sprintf("Running queue: %v", queueName))

	s.runningLock.Lock()
	for running := range s.running {
		if queueName == running {
			s.runningLock.Unlock()
			return fmt.Errorf("this queue is already running")
		}
	}

	s.running[queueName] = true
	s.runningLock.Unlock()
	s.chanmap[queueName] = make(chan bool, 100)

	numQueues.Set(float64(len(s.running)))

	go func() {
		s.runningLock.Lock()
		for s.running[queueName] {
			s.runningLock.Unlock()
			nextRunTime, deadline := s.getNextRunTime(queueName)
			runtime.With(prometheus.Labels{"queue_name": queueName}).Set(float64(nextRunTime.Unix()))

			s.timeout(queueName, nextRunTime)

			err := s.runQueueElement(queueName, deadline)
			if status.Convert(err).Code() != codes.AlreadyExists {
				s.Log(fmt.Sprintf("Ran queue (%v) -> %v", queueName, err))
			}
			if err != nil {
				s.errorCount[queueName]++
				if s.errorCount[queueName] > 50 && status.Convert(err).Code() == codes.Unknown {
					s.RaiseIssue(fmt.Sprintf("Error running queue: %v", queueName), fmt.Sprintf("Last error: %v", err))
				}
				s.DLog(context.Background(), fmt.Sprintf("Sleeping for %v for %v", queueName, time.Minute))
				time.Sleep(time.Minute)
			} else {
				s.errorCount[queueName] = 0
			}
			errors.With(prometheus.Labels{"queue_name": queueName}).Set(float64(s.errorCount[queueName]))
			s.runningLock.Lock()
		}
		s.runningLock.Unlock()
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
		err = results[1].Interface().(error)
	}

	queueCode.With(prometheus.Labels{"code": fmt.Sprintf("%v", status.Convert(err).Code())}).Inc()

	return err
}

func (s *Server) buildClient(service string) (interface{}, error) {
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
