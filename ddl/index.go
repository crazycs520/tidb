// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dagpb"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/schemautil"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
)

const maxPrefixLength = 3072
const maxCommentLength = 1024

func buildIndexColumns(columns []*model.ColumnInfo, idxColNames []*ast.IndexColName) ([]*model.IndexColumn, error) {
	// Build offsets.
	idxColumns := make([]*model.IndexColumn, 0, len(idxColNames))

	// The sum of length of all index columns.
	sumLength := 0

	for _, ic := range idxColNames {
		col := model.FindColumnInfo(columns, ic.Column.Name.O)
		if col == nil {
			return nil, errKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Column.Name)
		}

		if col.Flen == 0 {
			return nil, errors.Trace(errWrongKeyColumn.GenWithStackByArgs(ic.Column.Name))
		}

		// JSON column cannot index.
		if col.FieldType.Tp == mysql.TypeJSON {
			return nil, errors.Trace(errJSONUsedAsKey.GenWithStackByArgs(col.Name.O))
		}

		// Length must be specified for BLOB and TEXT column indexes.
		if types.IsTypeBlob(col.FieldType.Tp) && ic.Length == types.UnspecifiedLength {
			return nil, errors.Trace(errBlobKeyWithoutLength)
		}

		// Length can only be specified for specifiable types.
		if ic.Length != types.UnspecifiedLength && !types.IsTypePrefixable(col.FieldType.Tp) {
			return nil, errors.Trace(errIncorrectPrefixKey)
		}

		// Key length must be shorter or equal to the column length.
		if ic.Length != types.UnspecifiedLength &&
			types.IsTypeChar(col.FieldType.Tp) && col.Flen < ic.Length {
			return nil, errors.Trace(errIncorrectPrefixKey)
		}

		// Specified length must be shorter than the max length for prefix.
		if ic.Length > maxPrefixLength {
			return nil, errors.Trace(errTooLongKey)
		}

		// Take care of the sum of length of all index columns.
		if ic.Length != types.UnspecifiedLength {
			sumLength += ic.Length
		} else {
			// Specified data types.
			if col.Flen != types.UnspecifiedLength {
				// Special case for the bit type.
				if col.FieldType.Tp == mysql.TypeBit {
					sumLength += (col.Flen + 7) >> 3
				} else {
					sumLength += col.Flen
				}
			} else {
				if length, ok := mysql.DefaultLengthOfMysqlTypes[col.FieldType.Tp]; ok {
					sumLength += length
				} else {
					return nil, errUnknownTypeLength.GenWithStackByArgs(col.FieldType.Tp)
				}

				// Special case for time fraction.
				if types.IsTypeFractionable(col.FieldType.Tp) &&
					col.FieldType.Decimal != types.UnspecifiedLength {
					if length, ok := mysql.DefaultLengthOfTimeFraction[col.FieldType.Decimal]; ok {
						sumLength += length
					} else {
						return nil, errUnknownFractionLength.GenWithStackByArgs(col.FieldType.Tp, col.FieldType.Decimal)
					}
				}
			}
		}

		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > maxPrefixLength {
			return nil, errors.Trace(errTooLongKey)
		}

		idxColumns = append(idxColumns, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ic.Length,
		})
	}

	return idxColumns, nil
}

func buildIndexInfo(tblInfo *model.TableInfo, indexName model.CIStr, idxColNames []*ast.IndexColName, state model.SchemaState) (*model.IndexInfo, error) {
	idxColumns, err := buildIndexColumns(tblInfo.Columns, idxColNames)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		State:   state,
	}
	return idxInfo, nil
}

func addIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].Flag |= mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[col.Offset].Flag |= mysql.MultipleKeyFlag
	}
}

func dropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].Flag &= ^mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[col.Offset].Flag &= ^mysql.MultipleKeyFlag
	}

	// other index may still cover this col
	for _, index := range tblInfo.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.Columns[0].Name.L != col.Name.L {
			continue
		}

		addIndexColumnFlag(tblInfo, index)
	}
}

func validateRenameIndex(from, to model.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
	if fromIdx := schemautil.FindIndexByName(from.L, tbl.Indices); fromIdx == nil {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := schemautil.FindIndexByName(to.L, tbl.Indices); toIdx != nil && from.L != to.L {
		return false, errors.Trace(infoschema.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
	}
	return false, nil
}

func onRenameIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var from, to model.CIStr
	if err := job.DecodeArgs(&from, &to); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// Double check. See function `RenameIndex` in ddl_api.go
	duplicate, err := validateRenameIndex(from, to, tblInfo)
	if duplicate {
		return ver, nil
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	idx := schemautil.FindIndexByName(from.L, tblInfo.Indices)
	idx.Name = to
	if ver, err = updateVersionAndTableInfo(t, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onCreateIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		unique      bool
		indexName   model.CIStr
		idxColNames []*ast.IndexColName
		indexOption *ast.IndexOption
	)
	err = job.DecodeArgs(&unique, &indexName, &idxColNames, &indexOption)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := schemautil.FindIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo != nil && indexInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, ErrDupKeyName.GenWithStack("index already exist %s", indexName)
	}

	if indexInfo == nil {
		indexInfo, err = buildIndexInfo(tblInfo, indexName, idxColNames, model.StateNone)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		if indexOption != nil {
			indexInfo.Comment = indexOption.Comment
			if indexOption.Tp == model.IndexTypeInvalid {
				// Use btree as default index type.
				indexInfo.Tp = model.IndexTypeBtree
			} else {
				indexInfo.Tp = indexOption.Tp
			}
		} else {
			// Use btree as default index type.
			indexInfo.Tp = model.IndexTypeBtree
		}
		indexInfo.Primary = false
		indexInfo.Unique = unique
		indexInfo.ID = allocateIndexID(tblInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
		log.Infof("[ddl] add index, run DDL job %s, index info %#v", job, indexInfo)
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		indexInfo.State = model.StateWriteReorganization
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteReorganization:
		// reorganization -> public
		var tbl table.Table
		tbl, err = getTable(d.store, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		var reorgInfo *reorgInfo
		reorgInfo, err = getReorgInfo(d, t, job, tbl)
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		err = w.runReorgJob(t, reorgInfo, d.lease, func() error {
			return w.addTableIndex(tbl, indexInfo, reorgInfo)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if kv.ErrKeyExists.Equal(err) || errCancelledDDLJob.Equal(err) {
				log.Warnf("[ddl] run DDL job %v err %v, convert job to rollback job", job, err)
				ver, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			w.reorgCtx.cleanNotifyReorgCancel()
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()

		indexInfo.State = model.StatePublic
		// Set column index flag.
		addIndexColumnFlag(tblInfo, indexInfo)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = ErrInvalidIndexState.GenWithStack("invalid index state %v", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func onDropIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var indexName model.CIStr
	if err = job.DecodeArgs(&indexName); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := schemautil.FindIndexByName(indexName.L, tblInfo.Indices)
	if indexInfo == nil {
		job.State = model.JobStateCancelled
		return ver, ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		indexInfo.State = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteReorganization:
		// reorganization -> absent
		newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			if idx.Name.L != indexName.L {
				newIndices = append(newIndices, idx)
			}
		}
		tblInfo.Indices = newIndices
		// Set column index flag.
		dropIndexColumnFlag(tblInfo, indexInfo)

		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			job.Args[0] = indexInfo.ID
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compability,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexInfo.ID, getPartitionIDs(tblInfo))
		}
	default:
		err = ErrInvalidIndexState.GenWithStack("invalid index state %v", indexInfo.State)
	}
	return ver, errors.Trace(err)
}

const (
	// DefaultTaskHandleCnt is default batch size of adding indices.
	DefaultTaskHandleCnt = 128
)

// indexRecord is the record information of an index.
type indexRecord struct {
	handle int64
	skip   bool // skip indicates that the index key is already exists, we should not add it.
}

type addIndexWorker struct {
	id        int
	ddlWorker *worker
	batchCnt  int
	sessCtx   sessionctx.Context
	taskCh    chan *reorgIndexTask
	resultCh  chan *addIndexResult
	index     table.Index
	table     table.Table
	closed    bool
	priority  int

	// Table scan return columns.
	columns       []*model.ColumnInfo
	colFieldTypes []*types.FieldType
	handleIdx     int

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
	// cache for table scan.
	srcChunk    *chunk.Chunk
	idxRecords  []indexRecord
	idxValsBufs [][]types.Datum

	// For calculated generated column.
	haveGenCol   bool
	mutRow       chunk.MutRow
	decodeColMap map[int64]decoder.Column
	sysZone      *time.Location
}

type reorgIndexTask struct {
	physicalTableID int64
	startHandle     int64
	endHandle       int64
	// endIncluded indicates whether the range include the endHandle.
	// When the last handle is math.MaxInt64, set endIncluded to true to
	// tell worker backfilling index of endHandle.
	endIncluded bool
}

type addIndexResult struct {
	addedCount int
	scanCount  int
	nextHandle int64
	err        error
}

// addIndexTaskContext is the context of the batch adding indices.
// After finishing the batch adding indices, result in addIndexTaskContext will be merged into addIndexResult.
type addIndexTaskContext struct {
	nextHandle int64
	done       bool
	addedCount int
	scanCount  int
}

// mergeAddIndexCtxToResult merge partial result in taskCtx into result.
func mergeAddIndexCtxToResult(taskCtx *addIndexTaskContext, result *addIndexResult) {
	result.nextHandle = taskCtx.nextHandle
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

func newAddIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo,
	decodeColMap map[int64]decoder.Column, indexColumns []*model.ColumnInfo, colFieldTypes []*types.FieldType) *addIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	w := &addIndexWorker{
		id:            id,
		ddlWorker:     worker,
		batchCnt:      DefaultTaskHandleCnt,
		sessCtx:       sessCtx,
		taskCh:        make(chan *reorgIndexTask, 1),
		resultCh:      make(chan *addIndexResult, 1),
		index:         index,
		table:         t,
		priority:      kv.PriorityLow,
		columns:       indexColumns,
		colFieldTypes: colFieldTypes,
		decodeColMap:  decodeColMap,
		sysZone:       timeutil.SystemLocation(),
	}

	w.srcChunk = chunk.New(w.colFieldTypes, w.batchCnt, w.batchCnt)
	w.idxRecords = make([]indexRecord, w.batchCnt)
	w.idxValsBufs = make([][]types.Datum, w.batchCnt)
	w.haveGenCol = len(indexColumns) > (len(indexInfo.Columns) + 1)
	if w.haveGenCol {
		cols := t.Meta().Cols()
		tps := make([]*types.FieldType, len(cols))
		for _, col := range cols {
			tps[col.Offset] = &col.FieldType
		}
		w.mutRow = chunk.MutRowFromTypes(tps)
	}
	w.sessCtx.GetSessionVars().MaxChunkSize = w.batchCnt
	return w
}

func (w *addIndexWorker) close() {
	if !w.closed {
		w.closed = true
		close(w.taskCh)
	}
}

// getNextHandle gets next handle of entry and taskDone that we are going to process.
func (w *addIndexWorker) getNextHandle(taskRange reorgIndexTask, taskDone bool) (int64, bool) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		nextHandle := w.idxRecords[len(w.idxRecords)-1].handle + 1
		taskDone = nextHandle >= taskRange.endHandle && !taskRange.endIncluded
		return nextHandle, taskDone
	}
	// The task is done. So we need to choose a handle outside this range.
	// Some corner cases should be considered:
	// - The end of task range is MaxInt64.
	// - The end of the task is excluded in the range.
	if taskRange.endHandle == math.MaxInt64 || !taskRange.endIncluded {
		return taskRange.endHandle, taskDone
	}
	return taskRange.endHandle + 1, taskDone
}

func (w *addIndexWorker) logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&variable.DDLSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		log.Infof("[ddl-reorg][SLOW-OPERATIONS] elapsed time: %v, message: %v", elapsed, slowMsg)
	}
}

func (w *addIndexWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
}

func (w *addIndexWorker) batchCheckUniqueKey(txn kv.Transaction) error {
	idxInfo := w.index.Meta()
	if !idxInfo.Unique {
		// non-unique key need not to check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	w.initBatchCheckBufs(len(w.idxRecords))
	stmtCtx := w.sessCtx.GetSessionVars().StmtCtx
	for i, record := range w.idxRecords {
		idxKey, distinct, err := w.index.GenIndexKey(stmtCtx, w.idxValsBufs[i], record.handle, w.idxKeyBufs[i])
		if err != nil {
			return errors.Trace(err)
		}
		// save the buffer to reduce memory allocations.
		w.idxKeyBufs[i] = idxKey

		w.batchCheckKeys = append(w.batchCheckKeys, idxKey)
		w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
	}

	batchVals, err := kv.BatchGetValues(txn, w.batchCheckKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key is duplicate and the handle is equal, skip it.
	// 2. unique-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if val, found := batchVals[string(key)]; found {
			if w.distinctCheckFlags[i] {
				handle, err1 := tables.DecodeHandle(val)
				if err1 != nil {
					return errors.Trace(err1)
				}

				if handle != w.idxRecords[i].handle {
					return errors.Trace(kv.ErrKeyExists)
				}
			}
			w.idxRecords[i].skip = true
		} else {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			if w.distinctCheckFlags[i] {
				batchVals[string(key)] = tables.EncodeHandle(w.idxRecords[i].handle)
			}
		}
	}
	// Constrains is already checked.
	stmtCtx.BatchCheck = true
	return nil
}

func constructTableScanPB(tableID int64, pbColumnInfos []*tipb.ColumnInfo) *tipb.Executor {
	tblScan := &tipb.TableScan{
		TableId: tableID,
		Columns: pbColumnInfos,
	}

	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}
}

func (w *addIndexWorker) buildDAGPB(txn kv.Transaction, tableID int64) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.StartTs = txn.StartTS()

	// Use UTC time zone to read record to avoid convert time.
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(time.UTC)
	sc := w.sessCtx.GetSessionVars().StmtCtx
	dagReq.Flags = dagpb.StatementContextToFlags(sc)
	for i := range w.columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}

	tblInfo := w.table.Meta()
	pbColumnInfos := model.ColumnsToProto(w.columns, tblInfo.PKIsHandle)

	w.sessCtx.GetSessionVars().StmtCtx.TimeZone = timeutil.SystemLocation()
	err := dagpb.SetPBColumnsDefaultValue(w.sessCtx, pbColumnInfos, w.columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	w.sessCtx.GetSessionVars().StmtCtx.TimeZone = time.UTC
	w.sessCtx.GetSessionVars().TimeZone = time.UTC

	tblScanExec := constructTableScanPB(tableID, pbColumnInfos)
	dagReq.Executors = append(dagReq.Executors, tblScanExec)

	limitExec := constructLimitPB(uint64(w.batchCnt))
	dagReq.Executors = append(dagReq.Executors, limitExec)

	return dagReq, nil
}

func (w *addIndexWorker) buildTableScan(ctx context.Context, txn kv.Transaction, t table.Table, handleRange reorgIndexTask) (distsql.SelectResult, error) {
	dagPB, err := w.buildDAGPB(txn, handleRange.physicalTableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ranges := []*ranger.Range{{LowVal: []types.Datum{types.NewIntDatum(handleRange.startHandle)}, HighVal: []types.Datum{types.NewIntDatum(handleRange.endHandle)}, HighExclude: !handleRange.endIncluded}}
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(handleRange.physicalTableID, ranges, nil).
		SetDAGRequest(dagPB).
		SetKeepOrder(true).
		SetConcurrency(1).
		SetFromSessionVars(w.sessCtx.GetSessionVars()).
		Build()

	result, err := distsql.Select(ctx, w.sessCtx, kvReq, w.colFieldTypes, statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

// fetchRowColVals fetch w.batchCnt count rows that need to backfill indices, and build the corresponding indexRecord slice.
// fetchRowColVals returns:
// 1. Next handle of entry that we need to process.
// 2. Boolean indicates whether the task is done.
// 3. error occurs in fetchRowColVals. nil if no error occurs.
func (w *addIndexWorker) fetchRowColVals(ctx context.Context, txn kv.Transaction, taskRange reorgIndexTask) (nextHandle int64, done bool, err error) {
	if (taskRange.startHandle > taskRange.endHandle) || (taskRange.startHandle >= taskRange.endHandle && !taskRange.endIncluded) {
		return taskRange.startHandle, true, nil
	}
	startTime := time.Now()
	srcResult, err := w.buildTableScan(ctx, txn, w.table, taskRange)
	if err != nil {
		return taskRange.startHandle, false, errors.Trace(err)
	}
	defer terror.Call(srcResult.Close)

	nextHandle, done, err = w.fetchBackfillRows(ctx, srcResult, taskRange)
	log.Debugf("[ddl] txn %v fetches handle info %v, takes time %v", txn.StartTS(), taskRange, time.Since(startTime))
	return
}

// fetchBackfillRows fetch table record value and construct index value.
func (w *addIndexWorker) fetchBackfillRows(ctx context.Context, srcResult distsql.SelectResult, taskRange reorgIndexTask) (nextHandle int64, taskDone bool, err error) {
	handleIdx := len(w.index.Meta().Columns)
	nextHandle = taskRange.startHandle
	w.idxRecords = w.idxRecords[:0]
	if len(w.idxValsBufs) < w.batchCnt {
		w.idxValsBufs = make([][]types.Datum, w.batchCnt)
	}
LOOP1:
	for {
		if len(w.idxRecords) >= w.batchCnt {
			break
		}
		err = srcResult.Next(ctx, w.srcChunk)
		if err != nil {
			break
		}
		if w.srcChunk.NumRows() == 0 {
			break
		}
		iter := chunk.NewIterator4Chunk(w.srcChunk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle := row.GetInt64(handleIdx)
			w.idxValsBufs[len(w.idxRecords)], err = w.decodeIndexVals(row, w.colFieldTypes, handleIdx, w.idxValsBufs[len(w.idxRecords)])
			if err != nil {
				break LOOP1
			}
			w.idxRecords = append(w.idxRecords, indexRecord{handle: handle, skip: false})
			if len(w.idxRecords) >= w.batchCnt {
				break LOOP1
			}
		}
	}
	taskDone = len(w.idxRecords) == 0
	if !taskDone {
		taskDone = w.idxRecords[len(w.idxRecords)-1].handle >= taskRange.endHandle
	}
	nextHandle, taskDone = w.getNextHandle(taskRange, taskDone)
	return nextHandle, taskDone, errors.Trace(err)
}

func (w *addIndexWorker) decodeIndexVals(row chunk.Row, tps []*types.FieldType, handleIndex int, idxValues []types.Datum) ([]types.Datum, error) {
	if idxValues == nil {
		idxValues = make([]types.Datum, 0, handleIndex)
	} else {
		idxValues = idxValues[:0]
	}

	for i := 0; i < handleIndex; i++ {
		colVal := row.GetDatum(i, tps[i])
		idxValues = append(idxValues, *colVal.Copy())
	}
	// Calculated generated column.
	if w.haveGenCol {
		for i, v := range idxValues {
			w.mutRow.SetValue(w.columns[i].Offset, v.GetValue())
		}
		for i := handleIndex + 1; i < len(w.columns); i++ {
			colVal := row.GetDatum(i, tps[i])
			w.mutRow.SetValue(w.columns[i].Offset, colVal.GetValue())
		}
		for id, col := range w.decodeColMap {
			if col.GenExpr == nil {
				continue
			}
			// Eval the column value
			val, err := col.GenExpr.Eval(w.mutRow.ToRow())
			if err != nil {
				return nil, errors.Trace(err)
			}
			val, err = table.CastValue(w.sessCtx, val, col.Info)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if val.Kind() == types.KindMysqlTime && w.sysZone != time.UTC {
				t := val.GetMysqlTime()
				if t.Type == mysql.TypeTimestamp {
					err := t.ConvertTimeZone(w.sysZone, time.UTC)
					if err != nil {
						return nil, errors.Trace(err)
					}
					val.SetMysqlTime(t)
				}
			}
			idxValues[id] = val
		}
	}
	// Table Scan will return the default value if the column data is null.
	// So we need not consider default value here again.

	return idxValues, nil
}

// backfillIndexInTxn will backfill table index in a transaction, lock corresponding rowKey, if the value of rowKey is changed,
// indicate that index columns values may changed, index is not allowed to be added, so the txn will rollback and retry.
// backfillIndexInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
// TODO: make w.batchCnt can be modified by system variable.
func (w *addIndexWorker) backfillIndexInTxn(handleRange reorgIndexTask) (taskCtx addIndexTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := context.Background()
	errInTxn = kv.RunInNewTxn(w.sessCtx.GetStore(), true, func(txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)

		nextHandle, taskDone, err := w.fetchRowColVals(ctx, txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextHandle = nextHandle
		taskCtx.done = taskDone

		err = w.batchCheckUniqueKey(txn)
		if err != nil {
			return errors.Trace(err)
		}

		for i, idxRecord := range w.idxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// Lock the row key to notify us that someone delete or update the row,
			// then we should not backfill the index of it, otherwise the adding index is redundant.
			recordKey := w.table.RecordKey(idxRecord.handle)
			err := txn.LockKeys(recordKey)
			if err != nil {
				return errors.Trace(err)
			}

			// Create the index.
			handle, err := w.index.Create(w.sessCtx, txn, w.idxValsBufs[i], idxRecord.handle)
			if err != nil {
				if kv.ErrKeyExists.Equal(err) && idxRecord.handle == handle {
					// Index already exists, skip it.
					continue
				}

				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}

		return nil
	})
	w.logSlowOperations(time.Since(oprStartTime), "backfillIndexInTxn", 3000)

	return
}

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to table.
func (w *addIndexWorker) handleBackfillTask(d *ddlCtx, task *reorgIndexTask) *addIndexResult {
	handleRange := *task
	result := &addIndexResult{addedCount: 0, nextHandle: handleRange.startHandle, err: nil}
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime

	for {
		taskCtx, err := w.backfillIndexInTxn(handleRange)
		if err == nil {
			// Because reorgIndexTask may run a long time,
			// we should check whether this ddl job is still runnable.
			err = w.ddlWorker.isReorgRunnable(d)
		}
		if err != nil {
			result.err = err
			return result
		}

		mergeAddIndexCtxToResult(&taskCtx, result)
		w.ddlWorker.reorgCtx.increaseRowCount(int64(taskCtx.addedCount))

		if num := result.scanCount - lastLogCount; num >= 30000 {
			lastLogCount = result.scanCount
			log.Infof("[ddl-reorg] worker(%v), finish batch addedCount:%v backfill, task addedCount:%v, task scanCount:%v, nextHandle:%v, avg row time(ms):%v",
				w.id, taskCtx.addedCount, result.addedCount, result.scanCount, taskCtx.nextHandle, time.Since(lastLogTime).Seconds()*1000/float64(num))
			lastLogTime = time.Now()
		}

		handleRange.startHandle = taskCtx.nextHandle
		if taskCtx.done {
			break
		}
	}
	rightParenthesis := ")"
	if task.endIncluded {
		rightParenthesis = "]"
	}
	log.Infof("[ddl-reorg] worker(%v), finish region %v ranges [%v,%v%s, addedCount:%v, scanCount:%v, nextHandle:%v, elapsed time(s):%v",
		w.id, task.physicalTableID, task.startHandle, task.endHandle, rightParenthesis, result.addedCount, result.scanCount, result.nextHandle, time.Since(startTime).Seconds())

	return result
}

var gofailMockAddindexErrOnceGuard bool

func (w *addIndexWorker) run(d *ddlCtx) {
	log.Infof("[ddl-reorg] worker[%v] start", w.id)
	defer func() {
		r := recover()
		if r != nil {
			buf := util.GetStack()
			log.Errorf("[ddl-reorg] addIndexWorker %v %s", r, buf)
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
		}
		w.resultCh <- &addIndexResult{err: errReorgPanic}
	}()
	for {
		task, more := <-w.taskCh
		if !more {
			break
		}

		log.Debug("[ddl-reorg] got backfill index task:#v", task)
		// gofail: var mockAddIndexErr bool
		//if w.id == 0 && mockAddIndexErr && !gofailMockAddindexErrOnceGuard {
		//	gofailMockAddindexErrOnceGuard = true
		//	result := &addIndexResult{addedCount: 0, nextHandle: 0, err: errors.Errorf("mock add index error")}
		//	w.resultCh <- result
		//	continue
		//}

		// Dynamic change batch size.
		w.batchCnt = int(variable.GetDDLReorgBatchSize())
		result := w.handleBackfillTask(d, task)
		w.resultCh <- result
	}
	log.Infof("[ddl-reorg] worker[%v] exit", w.id)
}

func buildIndexColsAndDecodeMap(sessCtx sessionctx.Context, t table.Table, indexInfo *model.IndexInfo) ([]*model.ColumnInfo, []*types.FieldType, map[int64]decoder.Column, error) {
	cols := t.Cols()
	columns := make([]*model.ColumnInfo, len(indexInfo.Columns), len(indexInfo.Columns)+1)
	decodeColMap := make(map[int64]decoder.Column, len(indexInfo.Columns)+1)
	extraDecodeCols := make(map[int64]*model.ColumnInfo)
	for i, idxCol := range indexInfo.Columns {
		col := cols[idxCol.Offset]
		columns[i] = col.ToInfo()
		tpExpr := decoder.Column{
			Info: columns[i],
		}
		if col.IsGenerated() && !col.GeneratedStored {
			for _, c := range cols {
				if _, ok := col.Dependences[c.Name.L]; ok {
					extraDecodeCols[c.ID] = c.ToInfo()
				}
			}
			e, err := expression.ParseSimpleExprCastWithTableInfo(sessCtx, col.GeneratedExprString, t.Meta(), &col.FieldType)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			tpExpr.GenExpr = e
		}
		decodeColMap[int64(i)] = tpExpr
	}

	handleOffset := len(columns)
	handleColsInfo := &model.ColumnInfo{
		ID:     model.ExtraHandleID,
		Name:   model.ExtraHandleName,
		Offset: handleOffset,
	}
	handleColsInfo.FieldType = *types.NewFieldType(mysql.TypeLonglong)
	columns = append(columns, handleColsInfo)
	decodeColMap[int64(len(columns)-1)] = decoder.Column{
		Info: handleColsInfo,
	}

	for _, extraCol := range extraDecodeCols {
		columns = append(columns, extraCol)
		decodeColMap[int64(len(columns)-1)] = decoder.Column{
			Info: extraCol,
		}
	}
	colFieldTypes := make([]*types.FieldType, 0, len(columns))
	for _, col := range columns {
		colFieldTypes = append(colFieldTypes, &col.FieldType)
	}
	return columns, colFieldTypes, decodeColMap, nil
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up adding index in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startHandle, endHandle int64) ([]kv.KeyRange, error) {
	startRecordKey := t.RecordKey(startHandle)
	endRecordKey := t.RecordKey(endHandle).Next()

	log.Infof("[ddl-reorg] split partition %v range [%v, %v] from PD", t.GetPhysicalID(), startHandle, endHandle)
	kvRange := kv.KeyRange{StartKey: startRecordKey, EndKey: endRecordKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := tikv.NewBackoffer(context.Background(), maxSleep)
	ranges, err := tikv.SplitRegionRanges(bo, s.GetRegionCache(), []kv.KeyRange{kvRange})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ranges) == 0 {
		return nil, errors.Trace(errInvalidSplitRegionRanges)
	}
	return ranges, nil
}

func decodeHandleRange(keyRange kv.KeyRange) (int64, int64, error) {
	_, startHandle, err := tablecodec.DecodeRecordKey(keyRange.StartKey)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	_, endHandle, err := tablecodec.DecodeRecordKey(keyRange.EndKey)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	return startHandle, endHandle, nil
}

func closeAddIndexWorkers(workers []*addIndexWorker) {
	for _, worker := range workers {
		worker.close()
	}
}

func (w *worker) waitTaskResults(workers []*addIndexWorker, taskCnt int, totalAddedCount *int64, startHandle int64) (int64, int64, error) {
	var (
		addedCount int64
		nextHandle = startHandle
		firstErr   error
	)
	for i := 0; i < taskCnt; i++ {
		worker := workers[i]
		result := <-worker.resultCh
		if firstErr == nil && result.err != nil {
			firstErr = result.err
			// We should wait all working workers exits, any way.
			continue
		}

		if result.err != nil {
			log.Warnf("[ddl-reorg] worker[%v] return err:%v", i, result.err)
		}

		if firstErr == nil {
			*totalAddedCount += int64(result.addedCount)
			addedCount += int64(result.addedCount)
			nextHandle = result.nextHandle
		}
	}

	return nextHandle, addedCount, errors.Trace(firstErr)
}

// handleReorgTasks sends tasks to workers, and waits for all the running workers to return results,
// there are taskCnt running workers.
func (w *worker) handleReorgTasks(reorgInfo *reorgInfo, totalAddedCount *int64, workers []*addIndexWorker, batchTasks []*reorgIndexTask) error {
	for i, task := range batchTasks {
		workers[i].taskCh <- task
	}

	startHandle := batchTasks[0].startHandle
	taskCnt := len(batchTasks)
	startTime := time.Now()
	nextHandle, taskAddedCount, err := w.waitTaskResults(workers, taskCnt, totalAddedCount, startHandle)
	elapsedTime := time.Since(startTime).Seconds()
	if err == nil {
		err = w.isReorgRunnable(reorgInfo.d)
	}

	if err != nil {
		// update the reorg handle that has been processed.
		err1 := kv.RunInNewTxn(reorgInfo.d.store, true, func(txn kv.Transaction) error {
			return errors.Trace(reorgInfo.UpdateReorgMeta(txn, nextHandle, reorgInfo.EndHandle, reorgInfo.PhysicalTableID))
		})
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime)
		log.Warnf("[ddl-reorg] total added index for %d rows, this task [%d,%d) add index for %d failed %v, take time %v, update handle err %v",
			*totalAddedCount, startHandle, nextHandle, taskAddedCount, err, elapsedTime, err1)
		return errors.Trace(err)
	}

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
	w.reorgCtx.setNextHandle(nextHandle)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime)
	log.Infof("[ddl-reorg] total added index for %d rows, this task [%d,%d) added index for %d rows, take time %v",
		*totalAddedCount, startHandle, nextHandle, taskAddedCount, elapsedTime)
	return nil
}

// sendRangeTaskToWorkers sends tasks to workers, and returns remaining kvRanges that is not handled.
func (w *worker) sendRangeTaskToWorkers(t table.Table, workers []*addIndexWorker, reorgInfo *reorgInfo, totalAddedCount *int64, kvRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	batchTasks := make([]*reorgIndexTask, 0, len(workers))
	physicalTableID := reorgInfo.PhysicalTableID

	// Build reorg indices tasks.
	for _, keyRange := range kvRanges {
		startHandle, endHandle, err := decodeHandleRange(keyRange)
		if err != nil {
			return nil, errors.Trace(err)
		}

		endKey := t.RecordKey(endHandle)
		endIncluded := false
		if endKey.Cmp(keyRange.EndKey) < 0 {
			endIncluded = true
		}
		task := &reorgIndexTask{physicalTableID, startHandle, endHandle, endIncluded}
		batchTasks = append(batchTasks, task)

		if len(batchTasks) >= len(workers) {
			break
		}
	}

	if len(batchTasks) == 0 {
		return nil, nil
	}

	// Wait tasks finish.
	err := w.handleReorgTasks(reorgInfo, totalAddedCount, workers, batchTasks)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(batchTasks) < len(kvRanges) {
		// there are kvRanges not handled.
		remains := kvRanges[len(batchTasks):]
		return remains, nil
	}

	return nil, nil
}

// buildIndexForReorgInfo build backfilling tasks from [reorgInfo.StartHandle, reorgInfo.EndHandle),
// and send these tasks to add index workers, till we finish adding the indices.
func (w *worker) buildIndexForReorgInfo(t table.PhysicalTable, workers []*addIndexWorker, job *model.Job, reorgInfo *reorgInfo) error {
	totalAddedCount := job.GetRowCount()

	startHandle, endHandle := reorgInfo.StartHandle, reorgInfo.EndHandle
	for {
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startHandle, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[ddl-reorg] start to reorg index of %v region ranges, handle range:[%v, %v).", len(kvRanges), startHandle, endHandle)
		remains, err := w.sendRangeTaskToWorkers(t, workers, reorgInfo, &totalAddedCount, kvRanges)
		if err != nil {
			return errors.Trace(err)
		}

		if len(remains) == 0 {
			break
		}
		startHandle, _, err = decodeHandleRange(remains[0])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// addPhysicalTableIndex handles the add index reorganization state for a non-partitioned table or a partition.
// For a partitioned table, it should be handled partition by partition.
//
// How to add index in reorganization state?
// Concurrently process the defaultTaskHandleCnt tasks. Each task deals with a handle range of the index record.
// The handle range is split from PD regions now. Each worker deal with a region table key range one time.
// Each handle range by estimation, concurrent processing needs to perform after the handle range has been acquired.
// The operation flow is as follows:
//	1. Open numbers of defaultWorkers goroutines.
//	2. Split table key range from PD regions.
//	3. Send tasks to running workers by workers's task channel. Each task deals with a region key ranges.
//	4. Wait all these running tasks finished, then continue to step 3, until all tasks is done.
// The above operations are completed in a transaction.
// Finally, update the concurrent processing of the total number of rows, and store the completed handle value.
func (w *worker) addPhysicalTableIndex(t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) error {
	job := reorgInfo.Job
	log.Infof("[ddl-reorg] addTableIndex, job:%s, reorgInfo:%#v", job, reorgInfo)
	sessCtx := newContext(reorgInfo.d.store)

	indexCols, colFieldTypes, decodeColMap, err := buildIndexColsAndDecodeMap(sessCtx, t, indexInfo)
	if err != nil {
		return errors.Trace(err)
	}

	// variable.ddlReorgWorkerCounter can be modified by system variable "tidb_ddl_reorg_worker_cnt".
	workerCnt := variable.GetDDLReorgWorkerCounter()
	idxWorkers := make([]*addIndexWorker, workerCnt)
	for i := 0; i < int(workerCnt); i++ {
		sessCtx := newContext(reorgInfo.d.store)
		idxWorkers[i] = newAddIndexWorker(sessCtx, w, i, t, indexInfo, decodeColMap, indexCols, colFieldTypes)
		idxWorkers[i].priority = job.Priority
		go idxWorkers[i].run(reorgInfo.d)
	}
	defer closeAddIndexWorkers(idxWorkers)
	err = w.buildIndexForReorgInfo(t, idxWorkers, job, reorgInfo)
	return errors.Trace(err)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return errors.Errorf("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			err = w.addPhysicalTableIndex(p, idx, reorgInfo)
			if err != nil {
				break
			}
			finish, err = w.updateReorgInfo(tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		err = w.addPhysicalTableIndex(t.(table.PhysicalTable), idx, reorgInfo)
	}
	return errors.Trace(err)
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateReorgInfo(t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	pid, err := findNextPartitionID(reorg.PhysicalTableID, pi.Definitions)
	if err != nil {
		// Fatal error, should not run here.
		log.Errorf("[ddl-reorg] update reorg fail, %v error stack: %s", t, errors.ErrorStack(err))
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}

	start, end, err := getTableRange(reorg.d, t.GetPartition(pid), reorg.Job.SnapshotVer, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	log.Infof("[ddl-reorg] job %v update reorgInfo partition %d range [%d %d]", reorg.Job.ID, pid, start, end)
	reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = kv.RunInNewTxn(reorg.d.store, true, func(txn kv.Transaction) error {
		return errors.Trace(reorg.UpdateReorgMeta(txn, reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID))
	})
	return false, errors.Trace(err)
}

// findNextPartitionID finds the next partition ID in the PartitionDefinition array.
// Returns 0 if current partition is already the last one.
func findNextPartitionID(currentPartition int64, defs []model.PartitionDefinition) (int64, error) {
	for i, def := range defs {
		if currentPartition == def.ID {
			if i == len(defs)-1 {
				return 0, nil
			}
			return defs[i+1].ID, nil
		}
	}
	return 0, errors.Errorf("partition id not found %d", currentPartition)
}

func allocateIndexID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h int64, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(store kv.Storage, priority int, t table.Table, version uint64, startHandle int64, endHandle int64, endIncluded bool, fn recordIterFunc) error {
	ver := kv.Version{Ver: version}

	snap, err := store.GetSnapshot(ver)
	snap.SetPriority(priority)
	if err != nil {
		return errors.Trace(err)
	}
	firstKey := t.RecordKey(startHandle)

	// Calculate the exclusive upper bound
	var upperBound kv.Key
	if endIncluded {
		if endHandle == math.MaxInt64 {
			upperBound = t.RecordKey(endHandle).PrefixNext()
		} else {
			// PrefixNext is time costing. Try to avoid it if possible.
			upperBound = t.RecordKey(endHandle + 1)
		}
	} else {
		upperBound = t.RecordKey(endHandle)
	}

	it, err := snap.Iter(firstKey, upperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	for it.Valid() {
		if !it.Key().HasPrefix(t.RecordPrefix()) {
			break
		}

		var handle int64
		handle, err = tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		rk := t.RecordKey(handle)

		more, err := fn(handle, rk, it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}

		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			if kv.ErrNotExist.Equal(err) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
}
