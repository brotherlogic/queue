package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brotherlogic/goserver/utils"
	"google.golang.org/grpc/resolver"
	"google.golang.org/protobuf/proto"

	dspb "github.com/brotherlogic/dstore/proto"
	pb "github.com/brotherlogic/queue/proto"
	fopb "github.com/brotherlogic/recordfanout/proto"
	google_protobuf "github.com/golang/protobuf/ptypes/any"
)

func init() {
	resolver.Register(&utils.DiscoveryClientResolverBuilder{})
}

func main() {
	ctx, cancel := utils.ManualContext("dstore-cli", time.Minute)
	defer cancel()

	conn, err := utils.LFDialServer(ctx, "queue")
	if err != nil {
		log.Fatalf("Unable to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewQueueServiceClient(conn)

	switch os.Args[1] {
	case "add":
		addFlags := flag.NewFlagSet("AddQueue", flag.ExitOnError)
		var name = addFlags.String("name", "", "Name of the queue")
		var deadline = addFlags.Int("deadline", 0, "Overall Deadline in seconds")
		var endpoint = addFlags.String("endpoint", "", "Endpoint to call")
		var typen = addFlags.String("type", "", "The type of the request")
		if err := addFlags.Parse(os.Args[2:]); err == nil {
			res, err := client.AddQueue(ctx, &pb.AddQueueRequest{
				Name:     *name,
				Deadline: int32(*deadline),
				Endpoint: *endpoint,
				Type:     *typen,
			})
			fmt.Printf("%v -> %v\n", res, err)
		}

	case "item":
		itemFlags := flag.NewFlagSet("AddQueue", flag.ExitOnError)
		var name = itemFlags.String("name", "", "Name of the queue")
		var runTime = itemFlags.Int("run_time", int(time.Now().Unix()), "run time")
		var iid = itemFlags.Int("iid", -1, "Endpoint to call")
		if err := itemFlags.Parse(os.Args[2:]); err == nil {
			update := &fopb.FanoutRequest{InstanceId: int32(*iid)}
			data, _ := proto.Marshal(update)
			res, err := client.AddQueueItem(ctx, &pb.AddQueueItemRequest{
				QueueName: *name,
				RunTime:   int64(*runTime),
				Payload:   &google_protobuf.Any{Value: data},
				Key:       fmt.Sprintf("%v", *iid),
			})
			fmt.Printf("%v -> %v\n", res, err)
		}
	case "peek":
		itemFlags := flag.NewFlagSet("AddQueue", flag.ExitOnError)
		var name = itemFlags.String("name", "", "Name of the queue")
		if err := itemFlags.Parse(os.Args[2:]); err == nil {
			conn, err := utils.LFDialServer(ctx, "dstore")
			if err != nil {
				log.Fatalf("Err: %v", err)
			}
			defer conn.Close()

			client := dspb.NewDStoreServiceClient(conn)
			res, err := client.Read(ctx, &dspb.ReadRequest{Key: fmt.Sprintf("%v/%v", "github.com/brotherlogic/queue/queues", *name)})
			if err != nil {
				log.Fatalf("Err: %v", err)
			}

			if res.GetConsensus() < 0.5 {
				log.Fatalf("could not get read consensus (%v)", res.GetConsensus())
			}

			queue := &pb.Queue{}
			err = proto.Unmarshal(res.GetValue().GetValue(), queue)
			if err != nil {
				log.Fatalf("Err: %v", err)
			}

			for i, elem := range queue.GetEntries() {
				log.Printf("%v - %v [%v]", i, elem, time.Unix(elem.GetRunTime(), 0))
			}
		}
	}

}
