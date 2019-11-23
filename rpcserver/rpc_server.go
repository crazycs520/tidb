// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rpcserver

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var globalDomain *domain.Domain

func SetGlobalDomain(do *domain.Domain) {
	globalDomain = do
}

// CreateTiDBRPCServer creates a TiDB rpc server.
func CreateTiDBRPCServer() *grpc.Server {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server", zap.Any("stack", v))
		}
	}()
	s := grpc.NewServer()
	srv := &tidbRPCServer{}
	tikvpb.RegisterTikvServer(s, srv)
	return s
}

// tidbRPCServer is TiDB RPC Server, it reuse the TikvServer interface, but only support the Coprocessor interface now.
type tidbRPCServer struct {
	tikvpb.TikvServer
}

// Coprocessor implements the TiKVServer interface.
func (c *tidbRPCServer) Coprocessor(ctx context.Context, in *coprocessor.Request) (resp *coprocessor.Response, err error) {
	resp = &coprocessor.Response{}
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server coprocessor", zap.Any("stack", v))
			resp.OtherError = "rpc coprocessor panic"
		}
	}()
	fmt.Printf("rpc server handle coprocessor\n------------\n")
	resp = c.handleCopDAGRequest(ctx, in)
	return resp, nil
}

func (c *tidbRPCServer) handleCopDAGRequest(ctx context.Context, req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	createSessionFunc := session.CreateSessionWithDomainFunc(globalDomain.Store())
	re, err := createSessionFunc(globalDomain)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	sctx := re.(session.Session)
	do := domain.GetDomain(sctx)
	is := do.InfoSchema()
	sctx.GetSessionVars().TxnCtx.InfoSchema = is
	sctx.GetSessionVars().InRestrictedSQL = true
	sctx.SetSessionManager(util.GetglobalSessionManager())
	return executor.HandleCopDAGRequest(ctx, sctx, req)
}
