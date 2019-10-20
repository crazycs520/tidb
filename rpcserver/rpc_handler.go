package rpcserver

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/mpp_processor"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"time"
)

type mppHandler struct {
	ctx *dagContext
}

type dagContext struct {
	dagReq  *tipb.DAGRequest
	evalCtx *evalContext
}

type evalContext struct {
	colIDs      map[int64]int
	columnInfos []*tipb.ColumnInfo
	fieldTps    []*types.FieldType
	sc          *stmtctx.StatementContext
}

func (e *evalContext) setColumnInfo(cols []*tipb.ColumnInfo) {
	e.columnInfos = make([]*tipb.ColumnInfo, len(cols))
	copy(e.columnInfos, cols)

	e.colIDs = make(map[int64]int)
	e.fieldTps = make([]*types.FieldType, 0, len(e.columnInfos))
	for i, col := range e.columnInfos {
		ft := fieldTypeFromPBColumn(col)
		e.fieldTps = append(e.fieldTps, ft)
		e.colIDs[col.GetColumnId()] = i
	}
}

func (h *mppHandler) handleDAGRequest(req *mpp_processor.Request) *mpp_processor.Response {
	resp := &mpp_processor.Response{}
	dagCtx, e, dagReq, err := h.buildDAGExecutor(req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}

	var (
		chunks []tipb.Chunk
		rowCnt int
	)
	ctx := context.TODO()
	for {
		var row []types.Datum
		row, err = e.Next(ctx)
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		data := make([]byte, 0)
		for _, offset := range dagReq.OutputOffsets {
			data, err = codec.EncodeValue(dagCtx.evalCtx.sc, data, row[offset])
			if err != nil {
				break
			}
		}

		chunks = appendRow(chunks, data, rowCnt)
		rowCnt++
	}
	warnings := dagCtx.evalCtx.sc.GetWarnings()

	return buildResp(chunks, e.Counts(), err, warnings)
}

func toPBError(err error) *tipb.Error {
	if err == nil {
		return nil
	}
	perr := new(tipb.Error)
	switch x := err.(type) {
	case *terror.Error:
		sqlErr := x.ToSQLError()
		perr.Code = int32(sqlErr.Code)
		perr.Msg = sqlErr.Message
	default:
		perr.Code = int32(1)
		perr.Msg = err.Error()
	}
	return perr
}

func buildResp(chunks []tipb.Chunk, counts []int64, err error, warnings []stmtctx.SQLWarn) *mpp_processor.Response {
	resp := &mpp_processor.Response{}
	selResp := &tipb.SelectResponse{
		Error:        toPBError(err),
		Chunks:       chunks,
		OutputCounts: counts,
	}
	if len(warnings) > 0 {
		selResp.Warnings = make([]*tipb.Error, 0, len(warnings))
		for i := range warnings {
			selResp.Warnings = append(selResp.Warnings, toPBError(warnings[i].Err))
		}
	}
	if err != nil {
		resp.OtherError = err.Error()
	}
	data, err := proto.Marshal(selResp)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	resp.Data = data
	return resp
}

const rowsPerChunk = 64

func appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

func (h *mppHandler) buildDAGExecutor(req *mpp_processor.Request) (*dagContext, executor, *tipb.DAGRequest, error) {
	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	sc := flagsToStatementContext(dagReq.Flags)
	sc.TimeZone, err = constructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	ctx := &dagContext{
		dagReq:  dagReq,
		evalCtx: &evalContext{sc: sc},
	}
	h.ctx = ctx
	e, err := h.buildDAG(ctx, dagReq.Executors)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return ctx, e, dagReq, err
}

func (h *mppHandler) buildDAG(ctx *dagContext, executors []*tipb.Executor) (executor, error) {
	var src executor
	for i := 0; i < len(executors); i++ {
		curr, err := h.buildExec(ctx, executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(src)
		src = curr
	}
	return src, nil
}

type execDetail struct {
	timeProcessed   time.Duration
	numProducedRows int
	numIterations   int
}

func (e *execDetail) update(begin time.Time, row [][]byte) {
	e.timeProcessed += time.Since(begin)
	e.numIterations++
	if row != nil {
		e.numProducedRows++
	}
}

type executor interface {
	SetSrcExec(executor)
	GetSrcExec() executor
	Counts() []int64
	Next(ctx context.Context) ([]types.Datum, error)
}

func (h *mppHandler) buildExec(ctx *dagContext, curr *tipb.Executor) (executor, error) {
	var currExec executor
	var err error
	switch curr.GetTp() {
	case tipb.ExecType_TypeMemTableScan:
		currExec, err = h.buildMemTableScan(ctx, curr)
	case tipb.ExecType_TypeSelection:
		currExec, err = h.buildSelection(ctx, curr)
	case tipb.ExecType_TypeTopN:
		currExec, err = h.buildTopN(ctx, curr)
	case tipb.ExecType_TypeAggregation:
		currExec, err = h.buildHashAgg(ctx, curr)
	case tipb.ExecType_TypeStreamAgg:
		currExec, err = h.buildStreamAgg(ctx, curr)
	case tipb.ExecType_TypeLimit:
		currExec = &limitExec{limit: curr.Limit.GetLimit()}

	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", curr.GetTp())
	}

	return currExec, errors.Trace(err)
}

// flagsToStatementContext creates a StatementContext from a `tipb.SelectRequest.Flags`.
func flagsToStatementContext(flags uint64) *stmtctx.StatementContext {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = (flags & model.FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & model.FlagTruncateAsWarning) > 0
	sc.PadCharToFullLength = (flags & model.FlagPadCharToFullLength) > 0
	return sc
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

func (h *mppHandler) buildMemTableScan(ctx *dagContext, executor *tipb.Executor) (*memTableScanExec, error) {
	memTblScan := executor.MemTblScan
	if !infoschema.IsClusterTable(memTblScan.TableName) {
		return nil, errors.Errorf("table %s is not a cluster memory table", memTblScan.TableName)
	}

	columns := memTblScan.Columns
	ctx.evalCtx.setColumnInfo(columns)
	ids := make([]int64, len(columns))
	for i, col := range columns {
		ids[i] = col.ColumnId
	}

	return &memTableScanExec{
		tableName: memTblScan.TableName,
		columnIDs: ids,
	}, nil
}

type memTableScanExec struct {
	tableName string
	columnIDs []int64

	src executor

	rows   [][]types.Datum
	cursor int
}

func (e *memTableScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *memTableScanExec) GetSrcExec() executor {
	return e.src
}

func (e *memTableScanExec) Counts() []int64 {
	return []int64{int64(e.cursor)}
}

func (e *memTableScanExec) Next(ctx context.Context) ([]types.Datum, error) {
	if e.rows == nil {
		sctx := mock.NewContext()
		rows, err := infoschema.GetClusterMemTableRows(sctx, e.tableName)
		if err != nil {
			return nil, err
		}
		e.rows = rows
	}
	var row []types.Datum
	if e.cursor < len(e.rows) {
		row = make([]types.Datum, len(e.columnIDs))
		for i := range e.columnIDs {
			// For mem-table, the column offset should equal to column id -1 .
			offset := int(e.columnIDs[i] - 1)
			row[i] = e.rows[e.cursor][offset]
		}
		e.cursor++
	}
	return row, nil
}

func isDuplicated(offsets []int, offset int) bool {
	for _, idx := range offsets {
		if idx == offset {
			return true
		}
	}
	return false
}

func convertToExprs(sc *stmtctx.StatementContext, fieldTps []*types.FieldType, pbExprs []*tipb.Expr) ([]expression.Expression, error) {
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := expression.PBToExpr(expr, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

func (h *mppHandler) buildSelection(ctx *dagContext, executor *tipb.Executor) (*selectionExec, error) {
	var err error
	var relatedColOffsets []int
	pbConds := executor.Selection.Conditions
	for _, cond := range pbConds {
		relatedColOffsets, err = extractOffsetsInExpr(cond, ctx.evalCtx.columnInfos, relatedColOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &selectionExec{
		evalCtx:           ctx.evalCtx,
		relatedColOffsets: relatedColOffsets,
		conditions:        conds,
	}, nil
}

func extractOffsetsInExpr(expr *tipb.Expr, columns []*tipb.ColumnInfo, collector []int) ([]int, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.GetTp() == tipb.ExprType_ColumnRef {
		_, idx, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !isDuplicated(collector, int(idx)) {
			collector = append(collector, int(idx))
		}
		return collector, nil
	}
	var err error
	for _, child := range expr.Children {
		collector, err = extractOffsetsInExpr(child, columns, collector)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return collector, nil
}

type selectionExec struct {
	conditions        []expression.Expression
	relatedColOffsets []int
	evalCtx           *evalContext
	src               executor
}

func (e *selectionExec) Next(ctx context.Context) (row []types.Datum, err error) {
	for {
		row, err = e.src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			return nil, nil
		}

		match, err := evalBool(e.conditions, row, e.evalCtx.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return row, nil
		}
	}
}

// evalBool evaluates expression to a boolean value.
func evalBool(exprs []expression.Expression, row []types.Datum, ctx *stmtctx.StatementContext) (bool, error) {
	for _, expr := range exprs {
		data, err := expr.Eval(chunk.MutRowFromDatums(row).ToRow())
		if err != nil {
			return false, errors.Trace(err)
		}
		if data.IsNull() {
			return false, nil
		}

		isBool, err := data.ToBool(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		if isBool == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *selectionExec) GetSrcExec() executor {
	return e.src
}

func (e *selectionExec) Counts() []int64 {
	return e.src.Counts()
}

type limitExec struct {
	limit  uint64
	cursor uint64

	src executor
}

func (e *limitExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *limitExec) GetSrcExec() executor {
	return e.src
}

func (e *limitExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *limitExec) Next(ctx context.Context) (value []types.Datum, err error) {
	if e.cursor >= e.limit {
		return nil, nil
	}

	value, err = e.src.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		return nil, nil
	}
	e.cursor++
	return value, nil
}

// fieldTypeFromPBColumn creates a types.FieldType from tipb.ColumnInfo.
func fieldTypeFromPBColumn(col *tipb.ColumnInfo) *types.FieldType {
	return &types.FieldType{
		Tp:      byte(col.GetTp()),
		Flag:    uint(col.Flag),
		Flen:    int(col.GetColumnLen()),
		Decimal: int(col.GetDecimal()),
		Elems:   col.Elems,
		Collate: mysql.Collations[uint8(col.GetCollation())],
	}
}
