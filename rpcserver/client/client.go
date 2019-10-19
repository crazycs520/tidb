package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/pingcap/kvproto/pkg/mpp_processor"
	"github.com/pingcap/kvproto/pkg/tidbpb"
	"google.golang.org/grpc"
)

const (
	address     = "localhost:10080"
	defaultName = "world"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := tidbpb.NewTidbClient(conn)

	// Contact the server and print out its response.
	name := defaultName
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.MppProcessor(ctx, &mpp_processor.Request{Data: []byte(name)})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Data)
}
