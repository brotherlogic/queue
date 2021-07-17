package main

import (
	"fmt"
	"time"

	"github.com/brotherlogic/goserver"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pbg "github.com/brotherlogic/goserver/proto"
	"github.com/brotherlogic/goserver/utils"
	rcpb "github.com/brotherlogic/recordcollection/proto"
)

//Server main server type
type Server struct {
	*goserver.GoServer
	cmap map[string]interface{}
}

// Init builds the server
func Init() *Server {
	s := &Server{
		GoServer: &goserver.GoServer{},
		cmap:     make(map[string]interface{}),
	}
	return s
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {

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
func (s *Server) GetState() []*pbg.State {
	return []*pbg.State{
		&pbg.State{Key: "magic", Value: int64(12345)},
	}
}

func main() {
	server := Init()
	server.PrepServer()
	server.Register = server

	err := server.RegisterServerV2("queue", false, true)
	if err != nil {
		return
	}

	time.Sleep(time.Second * 5)
	server.cmap["recordcollection.RecordCollectionService"] = rcpb.NewRecordCollectionServiceClient
	ctx, cancel := utils.ManualContext("queue-test", time.Minute)
	defer cancel()
	res, err := server.runRPC(ctx, "recordcollection.RecordCollectionService", "GetRecord", &rcpb.GetRecordRequest{InstanceId: 365221819})
	server.Log(fmt.Sprintf("Run result: %v and %v", res, err))

	fmt.Printf("%v", server.Serve())
}
