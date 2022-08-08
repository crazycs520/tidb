package executor

import (
	"context"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/set"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
)

type ExecutorWithForeignKeyTrigger interface {
	Executor
	GetForeignKeyTriggerExecs() []*ForeignKeyTriggerExec
}

type ForeignKeyChecker struct {
	tbl             table.Table
	idx             table.Index
	idxIsExclusive  bool
	idxIsPrimaryKey bool
	handleCols      []*table.Column

	checkExist bool
	failedErr  error

	toBeCheckedHandleKeys []kv.Handle
	toBeCheckedUniqueKeys []kv.Key
	toBeCheckedIndexKeys  []kv.Key

	toBeLockedKeys []kv.Key
}

func (fkc *ForeignKeyChecker) resetToBeCheckedKeys() {
	fkc.toBeCheckedHandleKeys = fkc.toBeCheckedHandleKeys[:0]
	fkc.toBeCheckedUniqueKeys = fkc.toBeCheckedUniqueKeys[:0]
	fkc.toBeCheckedIndexKeys = fkc.toBeCheckedIndexKeys[:0]
}

func (fkc *ForeignKeyChecker) addRowNeedToCheck(sc *stmtctx.StatementContext, vals []types.Datum) error {
	if fkc.idxIsPrimaryKey {
		handleKey, err := fkc.buildHandleFromFKValues(sc, vals)
		if err != nil {
			return err
		}
		if fkc.idxIsExclusive {
			fkc.toBeCheckedHandleKeys = append(fkc.toBeCheckedHandleKeys, handleKey)
		} else {
			key := tablecodec.EncodeRecordKey(fkc.tbl.RecordPrefix(), handleKey)
			fkc.toBeCheckedIndexKeys = append(fkc.toBeCheckedIndexKeys, key)
		}
		return nil
	}
	key, distinct, err := fkc.idx.GenIndexKey(sc, vals, nil, nil)
	if err != nil {
		return err
	}
	if distinct && fkc.idxIsExclusive {
		fkc.toBeCheckedUniqueKeys = append(fkc.toBeCheckedUniqueKeys, key)
	} else {
		fkc.toBeCheckedIndexKeys = append(fkc.toBeCheckedIndexKeys, key)
	}
	return nil
}

func (fkc *ForeignKeyChecker) updateRowNeedToCheck(sc *stmtctx.StatementContext, oldVals, newVals []types.Datum) error {
	if fkc.checkExist {
		return fkc.addRowNeedToCheck(sc, newVals)
	}
	return fkc.addRowNeedToCheck(sc, oldVals)
}

func (fkc *ForeignKeyChecker) buildHandleFromFKValues(sc *stmtctx.StatementContext, vals []types.Datum) (kv.Handle, error) {
	if len(vals) == 1 && fkc.idx == nil {
		return kv.IntHandle(vals[0].GetInt64()), nil
	}
	pkDts := make([]types.Datum, 0, len(vals))
	for i, val := range vals {
		if fkc.idx != nil && len(fkc.handleCols) > 0 {
			tablecodec.TruncateIndexValue(&val, fkc.idx.Meta().Columns[i], fkc.handleCols[i].ColumnInfo)
		}
		pkDts = append(pkDts, val)
	}
	handleBytes, err := codec.EncodeKey(sc, nil, pkDts...)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

//func (fkc ForeignKeyCheckExec) Next(ctx context.Context, req *chunk.Chunk) error {
//	if fkc.checked {
//		return nil
//	}
//	fkc.checked = true
//
//	txn, err := fkc.ctx.Txn(false)
//	if err != nil {
//		return err
//	}
//	err = fkc.checkHandleKeys(ctx, txn)
//	if err != nil {
//		return err
//	}
//	err = fkc.checkUniqueKeys(ctx, txn)
//	if err != nil {
//		return err
//	}
//	err = fkc.checkIndexKeys(ctx, txn)
//	if err != nil {
//		return err
//	}
//	if len(fkc.toBeLockedKeys) == 0 {
//		return nil
//	}
//	lockWaitTime := fkc.ctx.GetSessionVars().LockWaitTimeout
//	lockCtx, err := newLockCtx(fkc.ctx, lockWaitTime, len(fkc.toBeLockedKeys))
//	if err != nil {
//		return err
//	}
//	return doLockKeys(ctx, fkc.ctx, lockCtx, fkc.toBeLockedKeys...)
//}

func (fkc *ForeignKeyChecker) checkHandleKeys(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedHandleKeys) == 0 {
		return nil
	}
	// Fill cache using BatchGet
	keys := make([]kv.Key, len(fkc.toBeCheckedHandleKeys))
	for i, handle := range fkc.toBeCheckedHandleKeys {
		keys[i] = tablecodec.EncodeRecordKey(fkc.tbl.RecordPrefix(), handle)
	}

	_, err := txn.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	for _, k := range keys {
		_, err := txn.Get(ctx, k)
		if err == nil {
			if !fkc.checkExist {
				return fkc.failedErr
			}
			fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, k)
			continue
		}
		if kv.IsErrNotFound(err) {
			if fkc.checkExist {
				return fkc.failedErr
			}
			continue
		}
		return err
	}
	return nil
}

func (fkc *ForeignKeyChecker) checkUniqueKeys(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedUniqueKeys) == 0 {
		return nil
	}
	// Fill cache using BatchGet
	_, err := txn.BatchGet(ctx, fkc.toBeCheckedUniqueKeys)
	if err != nil {
		return err
	}
	for _, uk := range fkc.toBeCheckedUniqueKeys {
		_, err := txn.Get(ctx, uk)
		if err == nil {
			if !fkc.checkExist {
				return fkc.failedErr
			}
			fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, uk)
			continue
		}
		if kv.IsErrNotFound(err) {
			if fkc.checkExist {
				return fkc.failedErr
			}
			continue
		}
		return err
	}
	return nil
}

func (fkc *ForeignKeyChecker) checkIndexKeys(ctx context.Context, txn kv.Transaction) error {
	if len(fkc.toBeCheckedIndexKeys) == 0 {
		return nil
	}
	memBuffer := txn.GetMemBuffer()
	snap := txn.GetSnapshot()
	snap.SetOption(kv.ScanBatchSize, 2)
	defer func() {
		snap.SetOption(kv.ScanBatchSize, 256)
	}()
	for _, key := range fkc.toBeCheckedIndexKeys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		key, value, err := fkc.getIndexKeyExistInReferTable(memBuffer, snap, key)
		if err != nil {
			return err
		}
		exist := len(value) > 0
		if !exist && fkc.checkExist {
			return fkc.failedErr
		}
		if exist {
			if !fkc.checkExist {
				return fkc.failedErr
			}
			if fkc.idx.Meta().Primary && fkc.tbl.Meta().IsCommonHandle {
				fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, key)
			} else {
				handle, err := tablecodec.DecodeIndexHandle(key, value, len(fkc.idx.Meta().Columns))
				if err != nil {
					return err
				}
				handleKey := tablecodec.EncodeRecordKey(fkc.tbl.RecordPrefix(), handle)
				fkc.toBeLockedKeys = append(fkc.toBeLockedKeys, handleKey)
			}
		}
	}
	return nil
}

func (fkc *ForeignKeyChecker) getIndexKeyExistInReferTable(memBuffer kv.MemBuffer, snap kv.Snapshot, key kv.Key) (k []byte, v []byte, _ error) {
	memIter, err := memBuffer.Iter(key, key.PrefixNext())
	if err != nil {
		return nil, nil, err
	}
	deletedKeys := set.NewStringSet()
	defer memIter.Close()
	for ; memIter.Valid(); err = memIter.Next() {
		if err != nil {
			return nil, nil, err
		}
		k := memIter.Key()
		// TODO: better decode to column datum and compare the datum value
		if !k.HasPrefix(key) {
			break
		}
		// check whether the key was been deleted.
		if len(memIter.Value()) > 0 {
			return k, memIter.Value(), nil
		}
		deletedKeys.Insert(string(k))
	}

	it, err := snap.Iter(key, key.PrefixNext())
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	for ; it.Valid(); err = it.Next() {
		if err != nil {
			return nil, nil, err
		}
		k := it.Key()
		if !k.HasPrefix(key) {
			break
		}
		// TODO: better decode to column datum and compare the datum value
		if !deletedKeys.Exist(string(k)) {
			return k, it.Value(), nil
		}
	}
	return nil, nil, nil
}

type ForeignKeyTriggerExec struct {
	b *executorBuilder

	fkChecker     *ForeignKeyChecker
	fkTriggerPlan plannercore.FKTriggerPlan
	fkTrigger     *plannercore.ForeignKeyTrigger
	colsOffsets   []int
	fkValues      [][]types.Datum
	fkValuesSet   set.StringSet
	// new-value-key => updatedValuesCouple
	fkUpdatedValuesMap map[string]*updatedValuesCouple

	buildFKValues set.StringSet
}

type updatedValuesCouple struct {
	newVals     []types.Datum
	oldValsList [][]types.Datum
}

func (fkt *ForeignKeyTriggerExec) addRowNeedToTrigger(sc *stmtctx.StatementContext, row []types.Datum) error {
	vals, err := fetchFKValues(row, fkt.colsOffsets)
	if err != nil || hasNullValue(vals) {
		return err
	}
	keyBuf, err := codec.EncodeKey(sc, nil, vals...)
	if err != nil {
		return err
	}
	key := string(keyBuf)
	if fkt.fkValuesSet.Exist(key) {
		return nil
	}
	fkt.fkValuesSet.Insert(key)
	if fkt.fkChecker != nil {
		return fkt.fkChecker.addRowNeedToCheck(sc, vals)
	}
	fkt.fkValues = append(fkt.fkValues, vals)
	return nil
}

func (fkt *ForeignKeyTriggerExec) updateRowNeedToTrigger(sc *stmtctx.StatementContext, oldRow, newRow []types.Datum) error {
	oldVals, err := fetchFKValues(oldRow, fkt.colsOffsets)
	if err != nil {
		return err
	}
	if fkt.fkTrigger.Tp != plannercore.FKTriggerOnInsertOrUpdateChildTable && hasNullValue(oldVals) {
		return nil
	}
	newVals, err := fetchFKValues(newRow, fkt.colsOffsets)
	if err != nil {
		return err
	}
	if fkt.fkTrigger.Tp == plannercore.FKTriggerOnInsertOrUpdateChildTable && hasNullValue(newVals) {
		return nil
	}
	if fkt.fkChecker != nil {
		return fkt.fkChecker.updateRowNeedToCheck(sc, oldVals, newVals)
	}
	keyBuf, err := codec.EncodeKey(sc, nil, newVals...)
	if err != nil {
		return err
	}
	couple := fkt.fkUpdatedValuesMap[string(keyBuf)]
	if couple == nil {
		couple = &updatedValuesCouple{
			newVals: newVals,
		}
	}
	couple.oldValsList = append(couple.oldValsList, oldVals)
	fkt.fkUpdatedValuesMap[string(keyBuf)] = couple
	return nil
}

func fetchFKValues(row []types.Datum, colsOffsets []int) ([]types.Datum, error) {
	vals := make([]types.Datum, len(colsOffsets))
	for i, offset := range colsOffsets {
		if offset >= len(row) {
			return nil, table.ErrIndexOutBound.GenWithStackByArgs("", offset, row)
		}
		vals[i] = row[offset]
	}
	return vals, nil
}

func hasNullValue(vals []types.Datum) bool {
	// If any foreign key column value is null, no need to check this row.
	// test case:
	// create table t1 (id int key,a int, b int, index(a, b));
	// create table t2 (id int key,a int, b int, foreign key fk(a, b) references t1(a, b) ON DELETE CASCADE);
	// > insert into t2 values (2, null, 1);
	// Query OK, 1 row affected
	// > insert into t2 values (3, 1, null);
	// Query OK, 1 row affected
	// > insert into t2 values (4, null, null);
	// Query OK, 1 row affected
	// > select * from t2;
	// 	+----+--------+--------+
	// 	| id | a      | b      |
	// 		+----+--------+--------+
	// 	| 4  | <null> | <null> |
	// 	| 2  | <null> | 1      |
	// 	| 3  | 1      | <null> |
	// 	+----+--------+--------+
	for _, val := range vals {
		if val.IsNull() {
			return true
		}
	}
	return false
}

func (fkt *ForeignKeyTriggerExec) isNeedTrigger() bool {
	return len(fkt.fkValues) > 0 || len(fkt.fkUpdatedValuesMap) > 0
}

func (fkt *ForeignKeyTriggerExec) buildIndexReaderRange(fkTriggerPlan plannercore.FKTriggerPlan) (bool, error) {
	switch p := fkTriggerPlan.(type) {
	case *plannercore.FKOnUpdateCascadePlan:
		for key, couple := range fkt.fkUpdatedValuesMap {
			if fkt.buildFKValues.Exist(key) {
				continue
			}
			fkt.buildFKValues.Insert(key)
			err := p.SetRangeForSelectPlan(couple.oldValsList)
			if err != nil {
				return false, err
			}
			return len(fkt.buildFKValues) == len(fkt.fkUpdatedValuesMap), p.SetUpdatedValues(couple.newVals)
		}
		return true, nil
	}
	valsList := make([][]types.Datum, 0, len(fkt.fkValues))
	valsList = append(valsList, fkt.fkValues...)
	switch fkt.fkTrigger.Tp {
	case plannercore.FKTriggerOnInsertOrUpdateChildTable:
		for _, couple := range fkt.fkUpdatedValuesMap {
			valsList = append(valsList, couple.newVals)
		}
	default:
		for _, couple := range fkt.fkUpdatedValuesMap {
			valsList = append(valsList, couple.oldValsList...)
		}
	}
	return true, fkTriggerPlan.SetRangeForSelectPlan(valsList)
}

func (fkt *ForeignKeyTriggerExec) buildFKTriggerPlan(ctx context.Context) (plannercore.FKTriggerPlan, error) {
	planBuilder, _ := plannercore.NewPlanBuilder().Init(fkt.b.ctx, fkt.b.is, &hint.BlockHintProcessor{})
	switch fkt.fkTrigger.Tp {
	case plannercore.FKTriggerOnDelete:
		return planBuilder.BuildOnDeleteFKTriggerPlan(ctx, fkt.fkTrigger.OnModifyReferredTable)
	case plannercore.FKTriggerOnUpdate:
		return planBuilder.BuildOnUpdateFKTriggerPlan(ctx, fkt.fkTrigger.OnModifyReferredTable)
	}
	return nil, nil
}

func (fkt *ForeignKeyTriggerExec) buildExecutor(ctx context.Context) (Executor, bool, error) {
	if fkt.fkTriggerPlan == nil {
		p, err := fkt.buildFKTriggerPlan(ctx)
		if err != nil {
			return nil, false, err
		}
		if p == nil {
			return nil, true, nil
		}
		fkt.fkTriggerPlan = p
	}

	done, err := fkt.buildIndexReaderRange(fkt.fkTriggerPlan)
	if err != nil {
		return nil, false, err
	}

	var e Executor
	switch x := fkt.fkTriggerPlan.(type) {
	case *plannercore.FKOnDeleteCascadePlan:
		e = fkt.b.build(x.Delete)
	case *plannercore.FKOnUpdateCascadePlan:
		e = fkt.b.build(x.Update)
	case *plannercore.FKUpdateSetNullPlan:
		e = fkt.b.build(x.Update)
	}
	return e, done, fkt.b.err
}

func (b *executorBuilder) buildTblID2ForeignKeyTriggerExecs(tblID2Table map[int64]table.Table, tblID2FKTriggers map[int64][]*plannercore.ForeignKeyTrigger) (map[int64][]*ForeignKeyTriggerExec, error) {
	var err error
	fkTriggerExecs := make(map[int64][]*ForeignKeyTriggerExec)
	for tid, tbl := range tblID2Table {
		fkTriggerExecs[tid], err = b.buildTblForeignKeyTriggerExecs(tbl, tblID2FKTriggers[tid])
		if err != nil {
			return nil, err
		}
	}
	return fkTriggerExecs, nil
}

func (b *executorBuilder) buildTblForeignKeyTriggerExecs(tbl table.Table, fkTriggerPlans []*plannercore.ForeignKeyTrigger) ([]*ForeignKeyTriggerExec, error) {
	fkTriggerExecs := make([]*ForeignKeyTriggerExec, 0, len(fkTriggerPlans))
	for _, fkTriggers := range fkTriggerPlans {
		fkTriggerExec, err := b.buildForeignKeyTriggerExec(tbl.Meta(), fkTriggers)
		if err != nil {
			return nil, err
		}
		fkTriggerExecs = append(fkTriggerExecs, fkTriggerExec)
	}

	return fkTriggerExecs, nil
}

func (b *executorBuilder) buildForeignKeyTriggerExec(tbInfo *model.TableInfo, fkTrigger *plannercore.ForeignKeyTrigger) (*ForeignKeyTriggerExec, error) {
	var cols []model.CIStr
	if fkTrigger.OnModifyChildTable != nil {
		cols = fkTrigger.OnModifyChildTable.FK.Cols
	} else if fkTrigger.OnModifyReferredTable != nil {
		cols = fkTrigger.OnModifyReferredTable.ReferredFK.Cols
	}
	colsOffsets, err := getColumnsOffsets(tbInfo, cols)
	if err != nil {
		return nil, err
	}

	fkt := &ForeignKeyTriggerExec{
		b:                  b,
		fkTrigger:          fkTrigger,
		colsOffsets:        colsOffsets,
		fkValuesSet:        set.NewStringSet(),
		buildFKValues:      set.NewStringSet(),
		fkUpdatedValuesMap: make(map[string]*updatedValuesCouple),
	}
	fkt.fkChecker, err = fkt.buildFKChecker()
	return fkt, err
}

func (fkt *ForeignKeyTriggerExec) buildFKChecker() (*ForeignKeyChecker, error) {
	switch fkt.fkTrigger.Tp {
	case plannercore.FKTriggerOnDelete:
		return fkt.buildOnDeleteFKChecker()
	case plannercore.FKTriggerOnUpdate:
		return fkt.buildOnUpdateFKChecker()
	case plannercore.FKTriggerOnInsertOrUpdateChildTable:
		return fkt.BuildOnInsertFKChecker()
	}
	return nil, nil
}

func (fkt *ForeignKeyTriggerExec) buildOnUpdateFKChecker() (*ForeignKeyChecker, error) {
	onModifyReferredTable := fkt.fkTrigger.OnModifyReferredTable
	fk, referredFK, childTable := onModifyReferredTable.FK, onModifyReferredTable.ReferredFK, onModifyReferredTable.ChildTable
	switch model.ReferOptionType(fk.OnUpdate) {
	case model.ReferOptionRestrict, model.ReferOptionNoOption, model.ReferOptionNoAction, model.ReferOptionSetDefault:
		failedErr := plannercore.ErrRowIsReferenced2.GenWithStackByArgs(fk.String(referredFK.ChildSchema.L, referredFK.ChildTable.L))
		return buildForeignKeyChecker(childTable, fk.Cols, false, failedErr)
	}
	return nil, nil
}

func (fkt *ForeignKeyTriggerExec) buildOnDeleteFKChecker() (*ForeignKeyChecker, error) {
	onModifyReferredTable := fkt.fkTrigger.OnModifyReferredTable
	fk, referredFK, childTable := onModifyReferredTable.FK, onModifyReferredTable.ReferredFK, onModifyReferredTable.ChildTable
	switch model.ReferOptionType(fk.OnDelete) {
	case model.ReferOptionRestrict, model.ReferOptionNoOption, model.ReferOptionNoAction, model.ReferOptionSetDefault:
		failedErr := plannercore.ErrRowIsReferenced2.GenWithStackByArgs(fk.String(referredFK.ChildSchema.L, referredFK.ChildTable.L))
		return buildForeignKeyChecker(childTable, fk.Cols, false, failedErr)
	}
	return nil, nil
}

func (fkt *ForeignKeyTriggerExec) BuildOnInsertFKChecker() (*ForeignKeyChecker, error) {
	info := fkt.fkTrigger.OnModifyChildTable
	failedErr := plannercore.ErrNoReferencedRow2.FastGenByArgs(info.FK.String(info.DBName, info.TblName))
	return buildForeignKeyChecker(info.ReferTable, info.FK.RefCols, true, failedErr)
}

func buildForeignKeyChecker(tbl table.Table, cols []model.CIStr, checkExist bool, failedErr error) (*ForeignKeyChecker, error) {
	tblInfo := tbl.Meta()
	if tblInfo.PKIsHandle && len(cols) == 1 {
		refColInfo := model.FindColumnInfo(tblInfo.Columns, cols[0].L)
		if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
			refCol := table.FindCol(tbl.Cols(), refColInfo.Name.O)
			return &ForeignKeyChecker{
				tbl:             tbl,
				idxIsPrimaryKey: true,
				idxIsExclusive:  true,
				handleCols:      []*table.Column{refCol},
				checkExist:      checkExist,
				failedErr:       failedErr,
			}, nil
		}
	}

	referTbIdxInfo := model.FindIndexByColumns(tblInfo, cols...)
	if referTbIdxInfo == nil {
		return nil, failedErr
	}
	var tblIdx table.Index
	for _, idx := range tbl.Indices() {
		if idx.Meta().ID == referTbIdxInfo.ID {
			tblIdx = idx
		}
	}
	if tblIdx == nil {
		return nil, failedErr
	}

	var handleCols []*table.Column
	if referTbIdxInfo.Primary && tblInfo.IsCommonHandle {
		cols := tbl.Cols()
		for _, idxCol := range referTbIdxInfo.Columns {
			handleCols = append(handleCols, cols[idxCol.Offset])
		}
	}

	return &ForeignKeyChecker{
		tbl:             tbl,
		idx:             tblIdx,
		idxIsExclusive:  len(cols) == len(referTbIdxInfo.Columns),
		idxIsPrimaryKey: referTbIdxInfo.Primary && tblInfo.IsCommonHandle,
		checkExist:      checkExist,
		failedErr:       failedErr,
	}, nil
}

func getColumnsOffsets(tbInfo *model.TableInfo, cols []model.CIStr) ([]int, error) {
	colsOffsets := make([]int, len(cols))
	for i, col := range cols {
		offset := -1
		for i := range tbInfo.Columns {
			if tbInfo.Columns[i].Name.L == col.L {
				offset = tbInfo.Columns[i].Offset
				break
			}
		}
		if offset < 0 {
			return nil, table.ErrUnknownColumn.GenWithStackByArgs(col.L)
		}
		colsOffsets[i] = offset
	}
	return colsOffsets, nil
}
