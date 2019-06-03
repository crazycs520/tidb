package executor

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

type memIndexReader struct {
	baseExecutor
	index         *model.IndexInfo
	table         *model.TableInfo
	kvRanges      []kv.KeyRange
	desc          bool
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	retFieldTypes []*types.FieldType
	outputOffset  []int
	// cache for decode handle.
	handleBytes []byte
}

func buildMemIndexReader(us *UnionScanExec, idxReader *IndexReaderExecutor) *memIndexReader {
	kvRanges := idxReader.kvRanges
	outputOffset := make([]int, 0, len(us.columns))
	for _, col := range idxReader.outputColumns {
		outputOffset = append(outputOffset, col.Index)
	}
	return &memIndexReader{
		baseExecutor:  us.baseExecutor,
		index:         idxReader.index,
		table:         idxReader.table.Meta(),
		kvRanges:      kvRanges,
		desc:          us.desc,
		conditions:    us.conditions,
		addedRows:     make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes: us.retTypes(),
		outputOffset:  outputOffset,
		handleBytes:   make([]byte, 0, 16),
	}
}

func (m *memIndexReader) getMemRows() ([][]types.Datum, error) {
	tps := make([]*types.FieldType, 0, len(m.index.Columns)+1)
	cols := m.table.Columns
	for _, col := range m.index.Columns {
		tps = append(tps, &cols[col.Offset].FieldType)
	}
	if m.table.PKIsHandle {
		for _, col := range m.table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				tps = append(tps, &col.FieldType)
				break
			}
		}
	} else {
		// ExtraHandle Column tp.
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}

	txn, err := m.ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	for _, rg := range m.kvRanges {
		iter, err := txn.GetMemBuffer().Iter(rg.StartKey, rg.EndKey)
		if err != nil {
			return nil, err
		}
		for ; iter.Valid(); err = iter.Next() {
			if err != nil {
				return nil, err
			}
			// check whether the key was been deleted.
			if len(iter.Value()) == 0 {
				continue
			}
			err = m.decodeIndexKeyValue(iter.Key(), iter.Value(), tps, &mutableRow)
			if err != nil {
				return nil, err
			}

			matched, _, err := expression.EvalBool(m.ctx, m.conditions, mutableRow.ToRow())
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			newData := make([]types.Datum, len(m.outputOffset))
			for i := range m.outputOffset {
				newData[i] = mutableRow.ToRow().GetDatum(i, m.retFieldTypes[i])
			}
			m.addedRows = append(m.addedRows, newData)
		}
	}
	// TODO: After refine `IterReverse`, remove below logic and use `IterReverse` when do reverse scan.
	if m.desc {
		reverseDatumSlice(m.addedRows)
	}
	return m.addedRows, nil
}

func (m *memIndexReader) getMemRowsHandle() ([]int64, error) {
	pkTp := types.NewFieldType(mysql.TypeLonglong)
	if m.table.PKIsHandle {
		for _, col := range m.table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				pkTp = &col.FieldType
				break
			}
		}
	}
	handles := make([]int64, 0, cap(m.addedRows))
	txn, err := m.ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	for _, rg := range m.kvRanges {
		iter, err := txn.GetMemBuffer().Iter(rg.StartKey, rg.EndKey)
		if err != nil {
			return nil, err
		}
		for ; iter.Valid(); err = iter.Next() {
			if err != nil {
				return nil, err
			}
			// check whether the key was been deleted.
			if len(iter.Value()) == 0 {
				continue
			}

			handle, err := m.decodeIndexHandle(iter.Key(), iter.Value(), pkTp)
			if err != nil {
				return nil, err
			}
			handles = append(handles, handle)
		}
	}
	if m.desc {
		for i, j := 0, len(handles)-1; i < j; i, j = i+1, j-1 {
			handles[i], handles[j] = handles[j], handles[i]
		}
	}
	return handles, nil
}

func (m *memIndexReader) decodeIndexKeyValue(key, value []byte, tps []*types.FieldType, mutableRow *chunk.MutRow) error {
	// this is from indexScanExec decodeIndexKV method.
	values, b, err := tablecodec.CutIndexKeyNew(key, len(m.index.Columns))
	if err != nil {
		return errors.Trace(err)
	}
	if len(b) > 0 {
		values = append(values, b)
	} else if len(value) >= 8 {
		handle, err := decodeHandle(value)
		if err != nil {
			return errors.Trace(err)
		}
		var handleDatum types.Datum
		if mysql.HasUnsignedFlag(tps[len(tps)-1].Flag) {
			handleDatum = types.NewUintDatum(uint64(handle))
		} else {
			handleDatum = types.NewIntDatum(handle)
		}
		m.handleBytes, err = codec.EncodeValue(m.ctx.GetSessionVars().StmtCtx, m.handleBytes[:0], handleDatum)
		if err != nil {
			return errors.Trace(err)
		}
		values = append(values, m.handleBytes)
	}

	for i, offset := range m.outputOffset {
		d, err := tablecodec.DecodeColumnValue(values[offset], tps[offset], m.ctx.GetSessionVars().TimeZone)
		if err != nil {
			return err
		}
		mutableRow.SetDatum(i, d)
	}
	return nil
}

func (m *memIndexReader) decodeIndexHandle(key, value []byte, pkTp *types.FieldType) (int64, error) {
	_, b, err := tablecodec.CutIndexKeyNew(key, len(m.index.Columns))
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(b) > 0 {
		d, err := tablecodec.DecodeColumnValue(b, pkTp, m.ctx.GetSessionVars().TimeZone)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.GetInt64(), nil

	} else if len(value) >= 8 {
		return decodeHandle(value)
	}
	// Should never execute to here.
	return 0, errors.Errorf("no handle in index key: %v, value: %v", key, value)
}

func decodeHandle(data []byte) (int64, error) {
	var h int64
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, &h)
	return h, errors.Trace(err)
}

type memTableReader struct {
	baseExecutor
	table         *model.TableInfo
	columns       []*model.ColumnInfo
	kvRanges      []kv.KeyRange
	desc          bool
	conditions    []expression.Expression
	addedRows     [][]types.Datum
	retFieldTypes []*types.FieldType
	colIDs        map[int64]int
	// cache for decode handle.
	handleBytes []byte
}

func buildMemTableReader(us *UnionScanExec, tblReader *TableReaderExecutor) *memTableReader {
	kvRanges := tblReader.kvRanges
	colIDs := make(map[int64]int)
	for i, col := range tblReader.columns {
		colIDs[col.ID] = i
	}

	return &memTableReader{
		baseExecutor:  us.baseExecutor,
		table:         tblReader.table.Meta(),
		columns:       us.columns,
		kvRanges:      kvRanges,
		desc:          us.desc,
		conditions:    us.conditions,
		addedRows:     make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes: us.retTypes(),
		colIDs:        colIDs,
		handleBytes:   make([]byte, 0, 16),
	}
}

func (m *memTableReader) getMemRows() ([][]types.Datum, error) {
	txn, err := m.ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	mutableRow := chunk.MutRowFromTypes(m.retFieldTypes)
	for _, rg := range m.kvRanges {
		// todo: consider desc scan.
		iter, err := txn.GetMemBuffer().Iter(rg.StartKey, rg.EndKey)
		if err != nil {
			return nil, err
		}
		for ; iter.Valid(); err = iter.Next() {
			if err != nil {
				return nil, err
			}

			// check whether the key was been deleted.
			if len(iter.Value()) == 0 {
				continue
			}

			err = m.decodeRecordKeyValue(iter.Key(), iter.Value(), &mutableRow)
			if err != nil {
				return nil, err
			}

			matched, _, err := expression.EvalBool(m.ctx, m.conditions, mutableRow.ToRow())
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			newData := make([]types.Datum, len(m.columns))
			for i := range m.columns {
				newData[i] = mutableRow.ToRow().GetDatum(i, m.retFieldTypes[i])
			}
			m.addedRows = append(m.addedRows, newData)
		}
	}
	// TODO: After refine `IterReverse`, remove below logic and use `IterReverse` when do reverse scan.
	if m.desc {
		reverseDatumSlice(m.addedRows)
	}
	return m.addedRows, nil
}

func (m *memTableReader) decodeRecordKeyValue(key, value []byte, mutableRow *chunk.MutRow) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	rowValues, err := m.getRowData(m.columns, m.colIDs, handle, value)
	if err != nil {
		return errors.Trace(err)
	}
	return m.decodeRowData(rowValues, mutableRow)
}

// getRowData decodes raw byte slice to row data.
func (m *memTableReader) getRowData(columns []*model.ColumnInfo, colIDs map[int64]int, handle int64, value []byte) ([][]byte, error) {
	pkIsHandle := m.table.PKIsHandle
	values, err := tablecodec.CutRowNew(value, colIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		values = make([][]byte, len(colIDs))
	}
	// Fill the handle and null columns.
	for _, col := range columns {
		id := col.ID
		offset := colIDs[id]
		if (pkIsHandle && mysql.HasPriKeyFlag(col.Flag)) || id == model.ExtraHandleID {
			var handleDatum types.Datum
			if mysql.HasUnsignedFlag(col.Flag) {
				// PK column is Unsigned.
				handleDatum = types.NewUintDatum(uint64(handle))
			} else {
				handleDatum = types.NewIntDatum(handle)
			}
			handleData, err1 := codec.EncodeValue(m.ctx.GetSessionVars().StmtCtx, m.handleBytes[:0], handleDatum)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[offset] = handleData
			continue
		}
		if hasColVal(values, colIDs, id) {
			continue
		}
		// no need to fill default value.
		values[offset] = []byte{codec.NilFlag}
	}

	return values, nil
}

func (m *memTableReader) decodeRowData(values [][]byte, mutableRow *chunk.MutRow) error {
	for i, col := range m.columns {
		offset := m.colIDs[col.ID]
		d, err := tablecodec.DecodeColumnValue(values[offset], &col.FieldType, m.ctx.GetSessionVars().TimeZone)
		if err != nil {
			return err
		}
		mutableRow.SetDatum(i, d)
	}
	return nil
}

func hasColVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

type memIndexLookUpReader struct {
	baseExecutor
	index         *model.IndexInfo
	columns       []*model.ColumnInfo
	table         table.Table
	desc          bool
	conditions    []expression.Expression
	retFieldTypes []*types.FieldType

	idxReader *memIndexReader
}

func buildMemIndexLookUpReader(us *UnionScanExec, idxLookUpReader *IndexLookUpExecutor) *memIndexLookUpReader {
	kvRanges := idxLookUpReader.kvRanges
	outputOffset := []int{len(idxLookUpReader.index.Columns)}
	memIdxReader := &memIndexReader{
		baseExecutor:  us.baseExecutor,
		index:         idxLookUpReader.index,
		table:         idxLookUpReader.table.Meta(),
		kvRanges:      kvRanges,
		desc:          idxLookUpReader.desc,
		addedRows:     make([][]types.Datum, 0, len(us.dirty.addedRows)),
		retFieldTypes: us.retTypes(),
		outputOffset:  outputOffset,
		handleBytes:   make([]byte, 0, 16),
	}

	return &memIndexLookUpReader{
		baseExecutor:  us.baseExecutor,
		index:         idxLookUpReader.index,
		columns:       idxLookUpReader.columns,
		table:         idxLookUpReader.table,
		desc:          idxLookUpReader.desc,
		conditions:    us.conditions,
		retFieldTypes: us.retTypes(),

		idxReader: memIdxReader,
	}
}

func (m *memIndexLookUpReader) getMemRows() ([][]types.Datum, error) {
	handles, err := m.idxReader.getMemRowsHandle()
	if err != nil || len(handles) == 0 {
		return nil, err
	}

	tblKvRanges := distsql.TableHandlesToKVRanges(getPhysicalTableID(m.table), handles)
	colIDs := make(map[int64]int)
	for i, col := range m.columns {
		colIDs[col.ID] = i
	}

	memTblReader := &memTableReader{
		baseExecutor:  m.baseExecutor,
		table:         m.table.Meta(),
		columns:       m.columns,
		kvRanges:      tblKvRanges,
		conditions:    m.conditions,
		addedRows:     make([][]types.Datum, 0, len(handles)),
		retFieldTypes: m.retTypes(),
		colIDs:        colIDs,
		handleBytes:   m.idxReader.handleBytes,
	}

	return memTblReader.getMemRows()
}

func reverseDatumSlice(rows [][]types.Datum) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}
