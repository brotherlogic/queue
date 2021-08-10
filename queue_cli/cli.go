package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/brotherlogic/goserver/utils"

	pb "github.com/brotherlogic/queue/proto"

	//Needed to pull in gzip encoding init

	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/resolver"
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
		if err := addFlags.Parse(os.Args[2:]); err == nil {
			res, err := client.AddQueue(ctx, &pb.AddQueueRequest{
				Name:     *name,
				Deadline: int32(*deadline),
				Endpoint: *endpoint,
			})
			fmt.Printf("%v -> %v\n", res, err)
		}
	}

}
