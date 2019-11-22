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
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// CreateTiDBRPCServer creates a TiDB rpc server.
func CreateTiDBRPCServer() *grpc.Server {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server", zap.Any("stack", v))
		}
	}()

	s := grpc.NewServer()
	tikvpb.RegisterTikvServer(s, &tidbRPCServer{})
	return s
}

// tidbRPCServer is TiDB RPC Server, it reuse the TikvServer interface, but only support the Coprocessor interface now.
type tidbRPCServer struct {
	tikvpb.TikvServer
	store kv.Storage
	sctx  sessionctx.Context
}

// Coprocessor implements the TiKVServer interface.
func (c *tidbRPCServer) Coprocessor(ctx context.Context, in *coprocessor.Request) (*coprocessor.Response, error) {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server coprocessor", zap.Any("stack", v))
		}
	}()
	handler := &rpcHandler{}
	res := handler.handleCopDAGRequest(in)
	return res, nil
}

type rpcHandler struct {
	sctx sessionctx.Context
}

type evalContext struct {
	sctx sessionctx.Context
	b    *executor.ExecutorBuilder
}

func (h *evalContext) buildDAGExecutor(req *coprocessor.Request) (*dagContext, executor, *tipb.DAGRequest, error) {
	if len(req.Ranges) == 0 && req.Context.GetRegionId() != 0 {
		return nil, nil, nil, errors.New("request range is null")
	}
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	sc := stmtctx.FlagsToStatementContext(dagReq.Flags)
	sc.TimeZone, err = constructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	h.sctx.GetSessionVars().StmtCtx = sc
	ctx := h.sctx
	e, err := h.buildDAG(ctx, dagReq.Executors)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return ctx, e, dagReq, err
}

func (h *evalContext) buildDAG(ctx *evalContext, executors []*tipb.Executor) (executor.Executor, error) {
	var src executor
	for i := len(executors) - 1; i > 0; i-- {
		curr, err := h.buildExec(executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.
		src = curr
	}
	return src, nil
}

func (h *evalContext) buildExec(curr *tipb.Executor) (executor.Executor, error) {
	var currExec executor.Executor
	var err error
	switch curr.GetTp() {
	//case tipb.ExecType_TypeTableScan:
	//	currExec, err = h.buildTableScan(ctx, curr)
	//case tipb.ExecType_TypeIndexScan:
	//	currExec, err = h.buildIndexScan(ctx, curr)
	//case tipb.ExecType_TypeSelection:
	//	currExec, err = h.buildSelection(ctx, curr)
	//case tipb.ExecType_TypeAggregation:
	//	currExec, err = h.buildHashAgg(ctx, curr)
	//case tipb.ExecType_TypeStreamAgg:
	//	currExec, err = h.buildStreamAgg(ctx, curr)
	//case tipb.ExecType_TypeTopN:
	//	currExec, err = h.buildTopN(ctx, curr)
	//case tipb.ExecType_TypeLimit:
	//	currExec = &limitExec{limit: curr.Limit.GetLimit(), execDetail: new(execDetail)}
	case tipb.ExecType_TypeMemTableScan:
		currExec, err = h.buildMemTableScan(h.sctx, curr)
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", curr.GetTp())
	}

	return currExec, errors.Trace(err)
}

func (h *evalContext) buildMemTableScan(sctx sessionctx.Context, e *tipb.Executor) (executor.Executor, error) {
	memTblScan := e.MemTblScan
	if !infoschema.IsClusterTable(memTblScan.TableName) {
		return nil, errors.Errorf("table %s is not a tidb memory table", memTblScan.TableName)
	}
	p, err := core.PBToPhysicalPlan(sctx, e)
	if err != nil {
		return nil, err
	}
	return h.b.Build(p), nil

}

// constructTimeZone constructs timezone by name first. When the timezone name
// is set, the daylight saving problem must be considered. Otherwise the
// timezone offset in seconds east of UTC is used to constructed the timezone.
func constructTimeZone(name string, offset int) (*time.Location, error) {
	if name != "" {
		return timeutil.LoadLocation(name)
	}

	return time.FixedZone("", offset), nil
}
