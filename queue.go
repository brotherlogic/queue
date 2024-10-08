package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/brotherlogic/goserver"
	"github.com/brotherlogic/goserver/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	bpb "github.com/brotherlogic/backup/proto"
	dspb "github.com/brotherlogic/dstore/proto"
	gspb "github.com/brotherlogic/goserver/proto"
	pb "github.com/brotherlogic/queue/proto"
	rapb "github.com/brotherlogic/recordadder/proto"
	rfopb "github.com/brotherlogic/recordfanout/proto"
	rspb "github.com/brotherlogic/recordsales/proto"
	rwpb "github.com/brotherlogic/recordwants/proto"
	tpb "github.com/brotherlogic/temp/proto"
)

var (
	numQueues = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_numqueues",
		Help: "The number of running queues",
	})
)

const (
	CONFIG_KEY = "github.com/brotherlogic/queue/config"
	QUEUE_KEY  = "github.com/brotherlogic/queue/queues"
)

// Server main server type
type Server struct {
	*goserver.GoServer
	cmap        map[string]interface{}
	running     map[string]bool
	chanmap     map[string]chan bool
	errorCount  map[string]int
	runningLock *sync.Mutex
}

// Init builds the server
func Init() *Server {
	s := &Server{
		GoServer:    &goserver.GoServer{},
		cmap:        make(map[string]interface{}),
		running:     make(map[string]bool),
		chanmap:     make(map[string]chan bool, 5000),
		errorCount:  make(map[string]int),
		runningLock: &sync.Mutex{},
	}

	s.buildClients()

	return s
}

func (s *Server) buildClients() {
	s.cmap["recordfanout.RecordFanoutService"] = rfopb.NewRecordFanoutServiceClient
	s.cmap["recordadder.AddRecordService"] = rapb.NewAddRecordServiceClient
	s.cmap["recordwants.WantsService"] = rwpb.NewWantServiceClient
	s.cmap["temp.TempService"] = tpb.NewTempServiceClient
	s.cmap["recordsales.SaleService"] = rspb.NewSaleServiceClient
	s.cmap["backup.BackupService"] = bpb.NewBackupServiceClient
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {
	pb.RegisterQueueServiceServer(server, s)
}

// ReportHealth alerts if we're not healthy
func (s *Server) ReportHealth() bool {
	return true
}

// Shutdown the server
func (s *Server) Shutdown(ctx context.Context) error {
	return nil
}

// Mote promotes/demotes this server
func (s *Server) Mote(ctx context.Context, master bool) error {
	return nil
}

// GetState gets the state of the server
func (s *Server) GetState() []*gspb.State {
	return []*gspb.State{}
}

func (s *Server) runQueues(ctx context.Context) error {
	conn, err := s.FDialServer(ctx, "dstore")
	if err != nil {
		return err
	}
	defer conn.Close()

	client := dspb.NewDStoreServiceClient(conn)
	res, err := client.Read(ctx, &dspb.ReadRequest{Key: CONFIG_KEY})
	if err != nil {
		// NotFound is expected if we have no data - and we do nothing here
		if status.Convert(err).Code() == codes.NotFound {
			return nil
		}

		return err
	}

	if res.GetConsensus() < 0.5 {
		return fmt.Errorf("could not get read consensus (%v)", res.GetConsensus())
	}

	config := &pb.Config{}
	err = proto.Unmarshal(res.GetValue().GetValue(), config)
	if err != nil {
		return err
	}

	if len(config.GetQueues()) == 0 {
		config.Queues = []string{"record_fanout", "record_adder"}
		s.writeConfig(ctx, client, config)
	}

	for _, queue := range config.GetQueues() {
		s.runQueue(queue)
	}

	s.CtxLog(ctx, fmt.Sprintf("Ran %v queues", len(config.GetQueues())))

	return nil
}

func main() {
	server := Init()
	server.PrepServer("queue")
	server.Register = server

	err := server.RegisterServerV2(false)
	if err != nil {
		return
	}

	ctx, cancel := utils.ManualContext("queue-init", time.Minute)
	err = server.runQueues(ctx)
	if err != nil {
		cancel()
		log.Fatalf("Unable to run initial queues: %v", err)
	}
	cancel()

	fmt.Printf("%v", server.Serve())
}
