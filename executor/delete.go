// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"github.com/pingcap/tidb/infoschema"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

// DeleteExec represents a delete executor.
// See https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteExec struct {
	baseExecutor

	is           infoschema.InfoSchema
	IsMultiTable bool
	tblID2Table  map[int64]table.Table

	// tblColPosInfos stores relationship between column ordinal to its table handle.
	// the columns ordinals is present in ordinal range format, @see plannercore.TblColPosInfos
	tblColPosInfos plannercore.TblColPosInfoSlice
	memTracker     *memory.Tracker

	deleteRowFKCheckers map[int64][]*foreignKeyChecker
}

// Next implements the Executor Next interface.
func (e *DeleteExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	for _, fkchckers := range e.deleteRowFKCheckers {
		for _, fkc := range fkchckers {
			fkc.resetToBeCheckedKeys()
		}
	}
	if e.IsMultiTable {
		// todo: add fk check for this
		return e.deleteMultiTablesByChunk(ctx)
	}
	return e.deleteSingleTableByChunk(ctx)
}

func (e *DeleteExec) initForeignKeyChecker() error {
	e.deleteRowFKCheckers = make(map[int64][]*foreignKeyChecker)
	for tid, tbl := range e.tblID2Table {
		deleteRowFKCheckers, err := initDeleteRowForeignKeyChecker(e.ctx, e.is, tbl.Meta())
		if err != nil {
			return err
		}
		e.deleteRowFKCheckers[tid] = deleteRowFKCheckers
	}
	return nil
}

func (e *DeleteExec) deleteOneRow(tbl table.Table, handleCols plannercore.HandleCols, isExtraHandle bool, row []types.Datum) error {
	end := len(row)
	if isExtraHandle {
		end--
	}
	handle, err := handleCols.BuildHandleByDatums(row)
	if err != nil {
		return err
	}
	err = e.removeRow(e.ctx, tbl, handle, row[:end])
	if err != nil {
		return err
	}
	return nil
}

func (e *DeleteExec) deleteSingleTableByChunk(ctx context.Context) error {
	var (
		tbl           table.Table
		isExtrahandle bool
		handleCols    plannercore.HandleCols
		rowCount      int
	)
	for _, info := range e.tblColPosInfos {
		tbl = e.tblID2Table[info.TblID]
		handleCols = info.HandleCols
		if !tbl.Meta().IsCommonHandle {
			isExtrahandle = handleCols.IsInt() && handleCols.GetCol(0).ID == model.ExtraHandleID
		}
	}

	batchDMLSize := e.ctx.GetSessionVars().DMLBatchSize
	// If tidb_batch_delete is ON and not in a transaction, we could use BatchDelete mode.
	batchDelete := e.ctx.GetSessionVars().BatchDelete && !e.ctx.GetSessionVars().InTxn() &&
		variable.EnableBatchDML.Load() && batchDMLSize > 0
	fields := retTypes(e.children[0])
	chk := newFirstChunk(e.children[0])
	columns := e.children[0].Schema().Columns
	if len(columns) != len(fields) {
		logutil.BgLogger().Error("schema columns and fields mismatch",
			zap.Int("len(columns)", len(columns)),
			zap.Int("len(fields)", len(fields)))
		// Should never run here, so the error code is not defined.
		return errors.New("schema columns and fields mismatch")
	}
	memUsageOfChk := int64(0)
	for {
		e.memTracker.Consume(-memUsageOfChk)
		iter := chunk.NewIterator4Chunk(chk)
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)
		for chunkRow := iter.Begin(); chunkRow != iter.End(); chunkRow = iter.Next() {
			if batchDelete && rowCount >= batchDMLSize {
				if err := e.doBatchDelete(ctx); err != nil {
					return err
				}
				rowCount = 0
			}

			datumRow := make([]types.Datum, 0, len(fields))
			for i, field := range fields {
				if columns[i].ID == model.ExtraPidColID || columns[i].ID == model.ExtraPhysTblID {
					continue
				}

				datum := chunkRow.GetDatum(i, field)
				datumRow = append(datumRow, datum)
			}

			err = e.deleteOneRow(tbl, handleCols, isExtrahandle, datumRow)
			if err != nil {
				return err
			}
			rowCount++
		}
		chk = chunk.Renew(chk, e.maxChunkSize)
	}

	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	for _, fkchckers := range e.deleteRowFKCheckers {
		logutil.BgLogger().Warn("------delete fk check", zap.Int("size", len(fkchckers)))
		for _, fkc := range fkchckers {
			logutil.BgLogger().Warn("------delete fk check", zap.Bool("expectExist", fkc.expectedExist))
			err := fkc.checkValueExistInReferTable(ctx, txn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *DeleteExec) doBatchDelete(ctx context.Context) error {
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return ErrBatchInsertFail.GenWithStack("BatchDelete failed with error: %v", err)
	}
	e.memTracker.Consume(-int64(txn.Size()))
	e.ctx.StmtCommit()
	if err := sessiontxn.NewTxnInStmt(ctx, e.ctx); err != nil {
		// We should return a special error for batch insert.
		return ErrBatchInsertFail.GenWithStack("BatchDelete failed with error: %v", err)
	}
	return nil
}

func (e *DeleteExec) composeTblRowMap(tblRowMap tableRowMapType, colPosInfos []plannercore.TblColPosInfo, joinedRow []types.Datum) error {
	// iterate all the joined tables, and got the corresponding rows in joinedRow.
	for _, info := range colPosInfos {
		if unmatchedOuterRow(info, joinedRow) {
			continue
		}
		if tblRowMap[info.TblID] == nil {
			tblRowMap[info.TblID] = kv.NewMemAwareHandleMap[[]types.Datum]()
		}
		handle, err := info.HandleCols.BuildHandleByDatums(joinedRow)
		if err != nil {
			return err
		}
		// tblRowMap[info.TblID][handle] hold the row datas binding to this table and this handle.
		_, exist := tblRowMap[info.TblID].Get(handle)
		memDelta := tblRowMap[info.TblID].Set(handle, joinedRow[info.Start:info.End])
		if !exist {
			memDelta += types.EstimatedMemUsage(joinedRow, 1)
			memDelta += int64(handle.ExtraMemSize())
		}
		e.memTracker.Consume(memDelta)
	}
	return nil
}

func (e *DeleteExec) deleteMultiTablesByChunk(ctx context.Context) error {
	colPosInfos := e.tblColPosInfos
	tblRowMap := make(tableRowMapType)
	fields := retTypes(e.children[0])
	chk := newFirstChunk(e.children[0])
	memUsageOfChk := int64(0)
	for {
		e.memTracker.Consume(-memUsageOfChk)
		iter := chunk.NewIterator4Chunk(chk)
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)

		for joinedChunkRow := iter.Begin(); joinedChunkRow != iter.End(); joinedChunkRow = iter.Next() {
			joinedDatumRow := joinedChunkRow.GetDatumRow(fields)
			err := e.composeTblRowMap(tblRowMap, colPosInfos, joinedDatumRow)
			if err != nil {
				return err
			}
		}
		chk = chunk.Renew(chk, e.maxChunkSize)
	}

	return e.removeRowsInTblRowMap(ctx, tblRowMap)
}

func (e *DeleteExec) removeRowsInTblRowMap(ctx context.Context, tblRowMap tableRowMapType) error {
	for id, rowMap := range tblRowMap {
		var err error
		rowMap.Range(func(h kv.Handle, val interface{}) bool {
			err = e.removeRow(e.ctx, e.tblID2Table[id], h, val.([]types.Datum))
			return err == nil
		})
		if err != nil {
			return err
		}
	}
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	for _, fkchckers := range e.deleteRowFKCheckers {
		for _, fkc := range fkchckers {
			err := fkc.checkValueExistInReferTable(ctx, txn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *DeleteExec) removeRow(ctx sessionctx.Context, t table.Table, h kv.Handle, data []types.Datum) error {
	txnState, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	memUsageOfTxnState := txnState.Size()
	err = t.RemoveRecord(ctx, h, data)
	if err != nil {
		return err
	}

	deleteRowFKCheckers := e.deleteRowFKCheckers[t.Meta().ID]
	for _, fkc := range deleteRowFKCheckers {
		err = fkc.addRowNeedToCheck(ctx.GetSessionVars().StmtCtx, data)
		if err != nil {
			return err
		}
	}
	e.memTracker.Consume(int64(txnState.Size() - memUsageOfTxnState))
	ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	return nil
}

// Close implements the Executor Close interface.
func (e *DeleteExec) Close() error {
	defer e.memTracker.ReplaceBytesUsed(0)
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *DeleteExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	return e.children[0].Open(ctx)
}

// tableRowMapType is a map for unique (Table, Row) pair. key is the tableID.
// the key in map[int64]Row is the joined table handle, which represent a unique reference row.
// the value in map[int64]Row is the deleting row.
type tableRowMapType map[int64]*kv.MemAwareHandleMap[[]types.Datum]
