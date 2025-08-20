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
	queueDue = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_due",
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
	delay = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_delay",
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

	s.CtxLog(ctx, fmt.Sprintf("Written %v:%v [%v] with %v", res.GetHash(), res.GetTimestamp(), len(queue.GetEntries()), res.GetConsensus()))

	if res.GetConsensus() < 0.55 {
		return fmt.Errorf("could not get write for queue %v consensus (%v)", queue.GetName(), res.GetConsensus())
	}

	queueLength.With(prometheus.Labels{"queue_name": queue.GetName()}).Set(float64(len(queue.GetEntries())))

	return nil
}

func (s *Server) loadQueue(ctx context.Context, name string, cons float32) (*pb.Queue, error) {
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

	if res.GetConsensus() < cons {
		return nil, fmt.Errorf("could not find read consensus for queue %v (%v)", name, res.GetConsensus())
	}

	s.CtxLog(ctx, fmt.Sprintf("Read %v:%v with %v", res.GetHash(), res.GetTimestamp(), res.GetConsensus()))

	queue := &pb.Queue{}
	err = proto.Unmarshal(res.GetValue().GetValue(), queue)
	if err != nil {
		return nil, err
	}

	queueLength.With(prometheus.Labels{"queue_name": queue.GetName()}).Set(float64(len(queue.GetEntries())))
	count := float64(0)
	for _, entry := range queue.GetEntries() {
		if time.Unix(entry.GetRunTime(), 0).Before(time.Now()) {
			count++
		}
	}
	queueDue.With(prometheus.Labels{"queue_name": queue.GetName()}).Set(count)

	/*if queue.GetName() == "sale_update" {
		s.RaiseIssue("Correcting", "Correcting sale update")
		queue.UniqueKeys = true
	}*/

	return queue, nil
}

func (s *Server) getNextRunTime(name string) (time.Time, time.Duration) {
	ctx, cancel := utils.ManualContext("queue-"+name, time.Minute)
	defer cancel()

	queue, err := s.loadQueue(ctx, name, 0.55)
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

func (s *Server) runQueueElement(name string, deadline time.Duration) (*pb.Entry, error) {
	ctx, cancel := utils.ManualContext("queue-rqe-"+name, deadline)
	defer cancel()

	// Acquire a lock to run the queue element
	unlockKey, err := s.RunLockingElection(ctx, "queuelock-"+name, "Locking for a queue run")
	if err != nil {
		time.Sleep(time.Minute)
		return nil, err
	}
	defer func() {
		lerr := s.ReleaseLockingElection(ctx, "queuelock-"+name, unlockKey)
		releases.With(prometheus.Labels{"queue": name, "error": fmt.Sprintf("%v", lerr)}).Inc()
	}()
	queue, err := s.loadQueue(ctx, name, 0.55)
	if err != nil {
		return nil, err
	}

	// Get the latest entry
	var latest *pb.Entry
	for _, entry := range queue.GetEntries() {
		if latest == nil || entry.GetRunTime() < latest.GetRunTime() {
			latest = entry
		}
	}

	if queue.GetName() == "record_adder" && len(queue.GetEntries()) < 4 {
		s.RaiseIssue("Not enough adds in queue", fmt.Sprintf("Needs more entries to add records"))
	}

	if latest == nil {
		if name != "wants_sync" && name != "temp" {
			s.RaiseIssue("No Entries in Queue", fmt.Sprintf("Queue %v has no entries to run", name))
		}
		return nil, nil
	}

	latest.State = pb.Entry_RUNNING
	err = s.saveQueue(ctx, queue)
	if err != nil {
		return nil, err
	}

	if time.Since(time.Unix(latest.GetRunTime(), 0)) > 0 {
		t, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(queue.GetType()))
		if err != nil {
			return nil, fmt.Errorf("error finding proto (%v) -> %v", queue.GetType(), err)
		}
		pt := t.New().Interface()
		err = proto.Unmarshal(latest.GetPayload().GetValue(), pt)
		if err != nil {
			return nil, err
		}

		elems := strings.Split(queue.GetEndpoint(), "/")
		s.DLog(ctx, fmt.Sprintf("Running %v on queue %v", latest.GetKey(), name))
		delay.With(prometheus.Labels{"queue_name": name}).Set(time.Since(time.Unix(latest.GetRunTime(), 0)).Seconds())
		err = s.runRPC(ctx, elems[0], elems[1], pt)
		if err != nil {
			s.CtxLog(ctx, fmt.Sprintf("Failed %v so adjusting time", err))
			queue, lerr := s.loadQueue(ctx, name, 0.55)
			if lerr != nil {
				return nil, lerr
			}

			for _, q := range queue.GetEntries() {
				if q.GetKey() == latest.GetKey() && q.GetRunTime() == latest.GetRunTime() {
					q.RunTime = q.RunTime + 600
					serr := s.saveQueue(ctx, queue)
					s.CtxLog(ctx, fmt.Sprintf("Saved with adjust time %v -> %v", q, serr))
					if serr != nil {
						return q, serr
					}
					return q, err
				}
			}
			return latest, nil
		}

		// Remove the entry from the queue - do a reload to stop stomping on recent additions
		queue, err = s.loadQueue(ctx, name, 0.55)
		if err != nil {
			return nil, err
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
			return nil, err
		}
	} else {
		return nil, status.Errorf(codes.InvalidArgument, "nothing to run here (%v) until %v", latest.GetKey(), time.Unix(latest.GetRunTime(), 0))
	}

	return latest, nil
}

func (s *Server) timeout(ctx context.Context, queue string, nrt time.Time) {
	chn := s.chanmap[queue]

	chanLength.With(prometheus.Labels{"queue_name": queue}).Set(float64(len(chn)))

	s.DLog(ctx, fmt.Sprintf("Sleeping %v for %v", queue, time.Until(nrt)))
	select {
	case <-chn:
		s.DLog(ctx, "Read from channel")

		break
	case <-time.After(time.Until(nrt)):
		s.DLog(ctx, "Time out")
		break
	}

}

func (s *Server) runQueue(queueName string) error {
	ctx, cancel := utils.ManualContext("queue-run", time.Minute)
	s.CtxLog(ctx, fmt.Sprintf("Running queue: %v", queueName))
	cancel()

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
			ctx, cancel := utils.ManualContext("queue-run-"+queueName, time.Minute)
			s.runningLock.Unlock()
			nextRunTime, deadline := s.getNextRunTime(queueName)
			runtime.With(prometheus.Labels{"queue_name": queueName}).Set(float64(nextRunTime.Unix()))

			s.timeout(ctx, queueName, nextRunTime)

			elem, err := s.runQueueElement(queueName, deadline)
			if status.Convert(err).Code() != codes.AlreadyExists {
				s.CtxLog(ctx, fmt.Sprintf("Ran queue (%v) -> %v from %v [%v]", queueName, err, elem.GetKey(), time.Since(time.Unix(elem.GetRunTime(), 0))))
			}
			if err != nil {
				s.errorCount[queueName]++
				if s.errorCount[queueName] > 50 && status.Convert(err).Code() == codes.Unknown {
					s.RaiseIssue(fmt.Sprintf("Error running queue: %v", queueName), fmt.Sprintf("Last error: %v", err))
				}
				if status.Code(err) != codes.FailedPrecondition {
					s.DLog(ctx, fmt.Sprintf("Sleeping for %v for %v (%v)", queueName, time.Minute*2, err))

					time.Sleep(time.Second * 30)

					if status.Code(err) == codes.ResourceExhausted {
						s.CtxLog(ctx, fmt.Sprintf("Sleeping for re"))
						time.Sleep(time.Minute)
					}
				}
			} else {
				s.errorCount[queueName] = 0
			}
			errors.With(prometheus.Labels{"queue_name": queueName}).Set(float64(s.errorCount[queueName]))
			s.runningLock.Lock()
			cancel()
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
