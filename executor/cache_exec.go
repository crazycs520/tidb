package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/ticdcutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func replaceTableReader(e Executor) (Executor, error) {
	switch v := e.(type) {
	case *TableReaderExecutor:
		err := v.Close()
		if err != nil {
			return nil, err
		}
		return BuildTableSinkerExecutor(v)
	case *MPPGather:
		err := v.Close()
		if err != nil {
			return nil, err
		}
		return BuildTableSinkerExecutorForMpp(v)
	}
	for i, child := range e.base().children {
		exec, err := replaceTableReader(child)
		if err != nil {
			return nil, err
		}
		e.base().children[i] = exec
	}
	return e, nil
}

type IncrementTableReaderExecutor struct {
	baseExecutor

	table table.Table

	ranges  []*ranger.Range
	dagPB   *tipb.DAGRequest
	startTS uint64
}

func BuildTableSinkerExecutor(src *TableReaderExecutor) (*IncrementTableReaderExecutor, error) {
	var err error
	ts := &IncrementTableReaderExecutor{
		baseExecutor: newBaseExecutor(src.ctx, src.schema, src.id),
		table:        src.table,
		ranges:       src.ranges,
		dagPB:        src.dagPB,
		startTS:      src.startTS,
	}
	ranges := make([]*coprocessor.KeyRange, len(src.kvRanges))
	for i, r := range src.kvRanges {
		ranges[i] = &coprocessor.KeyRange{
			Start: r.StartKey,
			End:   r.EndKey,
		}
	}
	copExec, err := buildCopExecutor(src.ctx, ranges, src.dagPB)
	if err != nil {
		return nil, err
	}
	err = copExec.Open(context.Background())
	if err != nil {
		return nil, err
	}
	ts.children = append(ts.children, copExec)
	return ts, err
}

func BuildTableSinkerExecutorForMpp(src *MPPGather) (*IncrementTableReaderExecutor, error) {
	is := sessiontxn.GetTxnManager(src.ctx).GetTxnInfoSchema()
	b := newExecutorBuilder(src.ctx, is, nil)
	e := b.build(src.tableReader)
	if b.err != nil {
		return nil, errors.Errorf("build table reader failed, err: %v", b.err)
	}
	tableReader, ok := e.(*TableReaderExecutor)
	if !ok {
		return nil, errors.Errorf("unknow executor %#v", e)
	}
	e.Open(context.Background())
	defer e.Close()
	return BuildTableSinkerExecutor(tableReader)
}

func buildCopExecutor(ctx sessionctx.Context, ranges []*coprocessor.KeyRange, dag *tipb.DAGRequest) (Executor, error) {
	copHandler := NewCoprocessorDAGHandler(ctx)
	is := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()
	return copHandler.buildCopExecutor(is, ranges, dag)
}

func (e *IncrementTableReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	return Next(ctx, e.children[0], req)
}

func (e *IncrementTableReaderExecutor) Reset() error {
	for _, child := range e.children {
		copExec, ok := child.(CopExecutor)
		if !ok {
			msg := fmt.Sprintf("%#v is not cop executor", child)
			panic(msg)
		}
		err := copExec.ResetAndClean()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *IncrementTableReaderExecutor) ResetCtx(ctx sessionctx.Context, p plannercore.PhysicalPlan) error {
	e.id = p.ID()
	e.ctx = ctx
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		if e.id > 0 {
			e.runtimeStats = &execdetails.BasicRuntimeStats{}
			e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.runtimeStats)
		}
	}
	tableReader, ok := p.(*plannercore.PhysicalTableReader)
	if !ok {
		return errors.Errorf("the expected plan is table reader")
	}
	return e.children[0].ResetCtx(ctx, tableReader.GetTablePlan())
}

type TableScanSinker struct {
	baseExecutor
	dbInfo  *model.DBInfo
	tbl     *model.TableInfo
	columns []*model.ColumnInfo
	sinker  ticdcutil.Changefeed
}

func BuildTableScanSinker(ctx sessionctx.Context, v *plannercore.PhysicalTableScanSinker) (*TableScanSinker, error) {
	txn, err := ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	e := &TableScanSinker{
		baseExecutor: newBaseExecutor(ctx, v.Schema(), v.ID()),
		dbInfo:       v.DBInfo,
		tbl:          v.Table,
		columns:      v.Columns,
	}
	e.sinker, err = ticdcutil.NewChangefeed(context.Background(), txn.StartTS(), v.DBInfo.Name.L, v.Table.Name.L)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *TableScanSinker) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	//sc := e.ctx.GetSessionVars().StmtCtx
	defer func() {
		logutil.BgLogger().Debug("table scan sinker next-----", zap.Int("rows", req.NumRows()))
	}()
	for {
		event, err := e.sinker.Next(ctx)
		if err != nil || event == nil {
			return nil
		}
		switch event.Tp {
		case ticdcutil.EventTypeInsert:
			//buf := bytes.NewBuffer(nil)
			//for i, v := range event.Columns {
			//	s, err := v.ToString()
			//	if err != nil {
			//		return err
			//	}
			//	if i > 0 {
			//		buf.WriteString(", ")
			//	}
			//	buf.WriteString(s)
			//}
			//logutil.BgLogger().Info("sinker receive change feed", zap.String("table", e.tbl.Name.L), zap.String("row", buf.String()))
			for idx, col := range e.columns {
				if col.Offset >= len(event.Columns) {
					return fmt.Errorf("column offset %v more than event data len %v", col.Offset, len(event.Columns))
				}
				//v, err := event.Columns[col.Offset].ConvertTo(sc, &col.FieldType)
				//if err != nil {
				//	return err
				//}
				req.AppendDatum(idx, &event.Columns[col.Offset])
				//req.AppendDatum(idx, &v)
			}
		}
		if req.IsFull() {
			return nil
		}
	}
}

func (e *TableScanSinker) Close() error {
	logutil.BgLogger().Info("close table scan sinker", zap.String("table", e.tbl.Name.L))
	return e.sinker.Close()
}

func (e *TableScanSinker) Reset() error {
	return nil
}

func (e *TableScanSinker) ResetCtx(ctx sessionctx.Context, p plannercore.PhysicalPlan) error {
	e.id = p.ID()
	p.TP()
	e.ctx = ctx
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		if e.id > 0 {
			e.runtimeStats = &execdetails.BasicRuntimeStats{}
			e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.runtimeStats)
		}
	}
	if tableScan, ok := p.(*plannercore.PhysicalTableScan); ok {
		tableScan.MarkIsSinker()
	}
	return nil
}

func (e *TableScanSinker) ResetAndClean() error {
	return nil
}

type MockTableScanSinker struct {
	baseExecutor
	dbInfo  *model.DBInfo
	tbl     *model.TableInfo
	columns []*model.ColumnInfo

	seq      int
	executed bool
}

func (e *MockTableScanSinker) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.executed {
		return nil
	}
	e.executed = true
	e.seq += 1
	sc := e.ctx.GetSessionVars().StmtCtx
	for idx, col := range e.columns {
		v := types.NewDatum(e.seq)
		v1, err := v.ConvertTo(sc, &col.FieldType)
		if err != nil {
			return err
		}
		req.AppendDatum(idx, &v1)
	}
	return nil
}

func (e *MockTableScanSinker) Reset() error {
	e.executed = false
	return nil
}

func (e *MockTableScanSinker) ResetAndClean() error {
	e.executed = false
	return nil
}

func (e *MockTableScanSinker) ResetCtx(ctx sessionctx.Context, p plannercore.PhysicalPlan) error {
	e.id = p.ID()
	e.ctx = ctx
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		if e.id > 0 {
			e.runtimeStats = &execdetails.BasicRuntimeStats{}
			e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.runtimeStats)
		}
	}
	return nil
}
