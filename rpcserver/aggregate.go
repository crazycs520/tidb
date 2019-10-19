package rpcserver

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

type aggCtxsMapper map[string][]*aggregation.AggEvaluateContext

var (
	_ executor = &hashAggExec{}
	_ executor = &streamAggExec{}
)

type hashAggExec struct {
	evalCtx           *evalContext
	aggExprs          []aggregation.Aggregation
	aggCtxsMap        aggCtxsMapper
	groupByExprs      []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	groups            map[string]struct{}
	groupKeys         [][]byte
	groupKeyRows      [][]types.Datum
	executed          bool
	currGroupIdx      int
	count             int64

	src executor
}

func (e *hashAggExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *hashAggExec) GetSrcExec() executor {
	return e.src
}

func (e *hashAggExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *hashAggExec) innerNext(ctx context.Context) (bool, error) {
	values, err := e.src.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if values == nil {
		return false, nil
	}
	err = e.aggregate(values)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *hashAggExec) Cursor() ([]byte, bool) {
	panic("don't not use coprocessor streaming API for hash aggregation!")
}

func (e *hashAggExec) Next(ctx context.Context) (value []types.Datum, err error) {
	e.count++
	if e.aggCtxsMap == nil {
		e.aggCtxsMap = make(aggCtxsMapper)
	}
	if !e.executed {
		for {
			hasMore, err := e.innerNext(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
	}

	if e.currGroupIdx >= len(e.groups) {
		return nil, nil
	}
	gk := e.groupKeys[e.currGroupIdx]
	value = make([]types.Datum, 0, len(e.groupByExprs)+2*len(e.aggExprs))
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		partialResults := agg.GetPartialResult(aggCtxs[i])
		for _, result := range partialResults {
			value = append(value, result)
		}
	}
	value = append(value, e.groupKeyRows[e.currGroupIdx]...)
	e.currGroupIdx++

	return value, nil
}

func (e *hashAggExec) getGroupKey() ([]byte, []types.Datum, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil, nil
	}
	bufLen := 0
	row := make([]types.Datum, 0, length)
	buf := make([]byte, 0, bufLen)
	for _, item := range e.groupByExprs {
		v, err := item.Eval(chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		b, err := codec.EncodeValue(e.evalCtx.sc, nil, v)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		bufLen += len(b)
		row = append(row, v)
		buf = append(buf, b...)
	}
	return buf, row, nil
}

// aggregate updates aggregate functions with row.
func (e *hashAggExec) aggregate(value []types.Datum) error {
	e.row = value
	// Get group key.
	gk, gbyKeyRow, err := e.getGroupKey()
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
		e.groupKeyRows = append(e.groupKeyRows, gbyKeyRow)
	}
	// Update aggregate expressions.
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		err = agg.Update(aggCtxs[i], e.evalCtx.sc, chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *hashAggExec) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	groupKeyString := string(groupKey)
	aggCtxs, ok := e.aggCtxsMap[groupKeyString]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.evalCtx.sc))
		}
		e.aggCtxsMap[groupKeyString] = aggCtxs
	}
	return aggCtxs
}

type streamAggExec struct {
	evalCtx           *evalContext
	aggExprs          []aggregation.Aggregation
	aggCtxs           []*aggregation.AggEvaluateContext
	groupByExprs      []expression.Expression
	relatedColOffsets []int
	row               []types.Datum
	tmpGroupByRow     []types.Datum
	currGroupByRow    []types.Datum
	nextGroupByRow    []types.Datum
	currGroupByValues []types.Datum
	executed          bool
	hasData           bool
	count             int64
	execDetail        *execDetail

	src executor
}

func (e *streamAggExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *streamAggExec) GetSrcExec() executor {
	return e.src
}

func (e *streamAggExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *streamAggExec) getPartialResult() ([]types.Datum, error) {
	value := make([]types.Datum, 0, len(e.groupByExprs)+2*len(e.aggExprs))
	for i, agg := range e.aggExprs {
		partialResults := agg.GetPartialResult(e.aggCtxs[i])
		value = append(value, partialResults...)
		// Clear the aggregate context.
		e.aggCtxs[i] = agg.CreateContext(e.evalCtx.sc)
	}
	e.currGroupByValues = e.currGroupByValues[:0]
	e.currGroupByValues = append(e.currGroupByValues, e.currGroupByRow...)
	e.currGroupByRow = types.CloneRow(e.nextGroupByRow)
	return append(value, e.currGroupByValues...), nil
}

func (e *streamAggExec) meetNewGroup() (bool, error) {
	if len(e.groupByExprs) == 0 {
		return false, nil
	}

	e.tmpGroupByRow = e.tmpGroupByRow[:0]
	matched, firstGroup := true, false
	if e.nextGroupByRow == nil {
		matched, firstGroup = false, true
	}
	for i, item := range e.groupByExprs {
		d, err := item.Eval(chunk.MutRowFromDatums(e.row).ToRow())
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			c, err := d.CompareDatum(e.evalCtx.sc, &e.nextGroupByRow[i])
			if err != nil {
				return false, errors.Trace(err)
			}
			matched = c == 0
		}
		e.tmpGroupByRow = append(e.tmpGroupByRow, d)
	}
	if firstGroup {
		e.currGroupByRow = types.CloneRow(e.tmpGroupByRow)
	}
	if matched {
		return false, nil
	}
	e.nextGroupByRow = e.tmpGroupByRow
	return !firstGroup, nil
}

func (e *streamAggExec) Cursor() ([]byte, bool) {
	panic("don't not use coprocessor streaming API for stream aggregation!")
}

func (e *streamAggExec) Next(ctx context.Context) (retRow []types.Datum, err error) {
	e.count++
	if e.executed {
		return nil, nil
	}

	for {
		values, err := e.src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if values == nil {
			e.executed = true
			if !e.hasData && len(e.groupByExprs) > 0 {
				return nil, nil
			}
			return e.getPartialResult()
		}

		e.hasData = true
		e.row = values
		newGroup, err := e.meetNewGroup()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if newGroup {
			retRow, err = e.getPartialResult()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		for i, agg := range e.aggExprs {
			err = agg.Update(e.aggCtxs[i], e.evalCtx.sc, chunk.MutRowFromDatums(e.row).ToRow())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if newGroup {
			return retRow, nil
		}
	}
}
