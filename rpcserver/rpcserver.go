package rpcserver

import (
	"context"
	"github.com/pingcap/kvproto/pkg/mpp_processor"
	"github.com/pingcap/kvproto/pkg/tidbpb"
	"google.golang.org/grpc"
)

func CreateRPCServer() *grpc.Server {
	s := grpc.NewServer()
	tidbpb.RegisterTidbServer(s, &mppServer{})
	return s
}

type mppServer struct {
}

func (s *mppServer) MppProcessor(ctx context.Context, req *mpp_processor.Request) (*mpp_processor.Response, error) {
	//fmt.Println(string(req.Data))
	handler := &mppHandler{}
	return handler.handleDAGRequest(req), nil
}
