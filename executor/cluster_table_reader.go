package executor

import (
	"context"
	"github.com/pingcap/kvproto/pkg/mpp_processor"
	"github.com/pingcap/tidb/rpcserver/mpp_client"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type ClusterTableReaderExecutor struct {
	baseExecutor

	table table.Table
	dagPB *tipb.DAGRequest
	done  bool
}

// Open initialzes necessary variables for using this executor.
func (e *ClusterTableReaderExecutor) Open(ctx context.Context) error {
	return nil
}

func (e *ClusterTableReaderExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true
	data, err := e.dagPB.Marshal()
	if err != nil {
		return err
	}
	req := &mpp_processor.Request{Data: data}
	return mpp_client.SendRPCToALLServer(e.ctx, req, chk, retTypes(e))
}
