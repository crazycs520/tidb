package helloworld

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"net"

	pb "github.com/pingcap/tidb/rpcserver/helloworld"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	logutil.BgLogger().Info("get rpc msg", zap.String("name", in.Name))
	return &pb.HelloReply{Message: "hello, this is cs, you are " + in.Name}, nil
}

func (s *server) SayHelloAgain(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello again " + in.Name, Body: "tidb"}, nil
}

func RunRPCServer(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
