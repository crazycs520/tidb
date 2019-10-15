package rpcserver

import (
	"context"
	pb "github.com/pingcap/tidb/rpcserver/rpcserver_proto"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

func CreateRPCServer() *grpc.Server {
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	return s
}
