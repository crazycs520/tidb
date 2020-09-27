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
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"bytes"
	"context"
	stderr "errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap"
)

// Both partition and partitionedTable implement the table.Table interface.
var _ table.Table = &partition{}
var _ table.Table = &partitionedTable{}

// partitionedTable implements the table.PartitionedTable interface.
var _ table.PartitionedTable = &partitionedTable{}

// partition is a feature from MySQL:
// See https://dev.mysql.com/doc/refman/8.0/en/partitioning.html
// A partition table may contain many partitions, each partition has a unique partition
// id. The underlying representation of a partition and a normal table (a table with no
// partitions) is basically the same.
// partition also implements the table.Table interface.
type partition struct {
	TableCommon
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (p *partition) GetPhysicalID() int64 {
	return p.physicalTableID
}

// partitionedTable implements the table.PartitionedTable interface.
// partitionedTable is a table, it contains many Partitions.
type partitionedTable struct {
	TableCommon
	partitionExpr *PartitionExpr
	partitions    map[int64]*partition
}

func newPartitionedTable(tbl *TableCommon, tblInfo *model.TableInfo) (table.Table, error) {
	ret := &partitionedTable{TableCommon: *tbl}
	partitionExpr, err := newPartitionExpr(tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.partitionExpr = partitionExpr

	if err := initTableIndices(&ret.TableCommon); err != nil {
		return nil, errors.Trace(err)
	}
	pi := tblInfo.GetPartitionInfo()
	partitions := make(map[int64]*partition, len(pi.Definitions))
	for _, p := range pi.Definitions {
		var t partition
		err := initTableCommonWithIndices(&t.TableCommon, tblInfo, p.ID, tbl.Columns, tbl.allocs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		partitions[p.ID] = &t
	}
	ret.partitions = partitions
	return ret, nil
}

func newPartitionExpr(tblInfo *model.TableInfo) (*PartitionExpr, error) {
	ctx := mock.NewContext()
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(ctx, dbName, tblInfo.Name, tblInfo.Columns, tblInfo)
	if err != nil {
		return nil, err
	}
	pi := tblInfo.GetPartitionInfo()
	switch pi.Type {
	case model.PartitionTypeRange:
		return generateRangePartitionExpr(ctx, pi, columns, names)
	case model.PartitionTypeHash:
		return generateHashPartitionExpr(ctx, pi, columns, names)
	case model.PartitionTypeList:
		return generateListPartitionExpr(ctx, pi, columns, names)
	}
	panic("cannot reach here")
}

// PartitionExpr is the partition definition expressions.
type PartitionExpr struct {
	// UpperBounds: (x < y1); (x < y2); (x < y3), used by locatePartition.
	UpperBounds []expression.Expression
	// OrigExpr is the partition expression ast used in point get.
	OrigExpr ast.ExprNode
	// Expr is the hash partition expression.
	Expr expression.Expression
	// Used in the range pruning process.
	*ForRangePruning
	// Used in the range column pruning process.
	*ForRangeColumnsPruning
	// InValues: x in (1,2); x in (3,4); x in (5,6), used for list partition.
	InValues []expression.Expression
}

// ForRangeColumnsPruning is used for range partition pruning.
type ForRangeColumnsPruning struct {
	LessThan []expression.Expression
	MaxValue bool
}

func dataForRangeColumnsPruning(ctx sessionctx.Context, pi *model.PartitionInfo, schema *expression.Schema, names []*types.FieldName, p *parser.Parser) (*ForRangeColumnsPruning, error) {
	var res ForRangeColumnsPruning
	res.LessThan = make([]expression.Expression, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Use a bool flag instead of math.MaxInt64 to avoid the corner cases.
			res.MaxValue = true
		} else {
			tmp, err := parseSimpleExprWithNames(p, ctx, pi.Definitions[i].LessThan[0], schema, names)
			if err != nil {
				return nil, err
			}
			res.LessThan[i] = tmp
		}
	}
	return &res, nil
}

// parseSimpleExprWithNames parses simple expression string to Expression.
// The expression string must only reference the column in the given NameSlice.
func parseSimpleExprWithNames(p *parser.Parser, ctx sessionctx.Context, exprStr string, schema *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	exprNode, err := parseExpr(p, exprStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return expression.RewriteSimpleExprWithNames(ctx, exprNode, schema, names)
}

// ForRangePruning is used for range partition pruning.
type ForRangePruning struct {
	LessThan []int64
	MaxValue bool
	Unsigned bool
}

// dataForRangePruning extracts the less than parts from 'partition p0 less than xx ... partitoin p1 less than ...'
func dataForRangePruning(sctx sessionctx.Context, pi *model.PartitionInfo) (*ForRangePruning, error) {
	var maxValue bool
	var unsigned bool
	lessThan := make([]int64, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Use a bool flag instead of math.MaxInt64 to avoid the corner cases.
			maxValue = true
		} else {
			var err error
			lessThan[i], err = strconv.ParseInt(pi.Definitions[i].LessThan[0], 10, 64)
			var numErr *strconv.NumError
			if stderr.As(err, &numErr) && numErr.Err == strconv.ErrRange {
				var tmp uint64
				tmp, err = strconv.ParseUint(pi.Definitions[i].LessThan[0], 10, 64)
				lessThan[i] = int64(tmp)
				unsigned = true
			}
			if err != nil {
				val, ok := fixOldVersionPartitionInfo(sctx, pi.Definitions[i].LessThan[0])
				if !ok {
					logutil.BgLogger().Error("wrong partition definition", zap.String("less than", pi.Definitions[i].LessThan[0]))
					return nil, errors.WithStack(err)
				}
				lessThan[i] = val
			}
		}
	}
	return &ForRangePruning{
		LessThan: lessThan,
		MaxValue: maxValue,
		Unsigned: unsigned,
	}, nil
}

func fixOldVersionPartitionInfo(sctx sessionctx.Context, str string) (int64, bool) {
	// less than value should be calculate to integer before persistent.
	// Old version TiDB may not do it and store the raw expression.
	tmp, err := parseSimpleExprWithNames(parser.New(), sctx, str, nil, nil)
	if err != nil {
		return 0, false
	}
	ret, isNull, err := tmp.EvalInt(sctx, chunk.Row{})
	if err != nil || isNull {
		return 0, false
	}
	return ret, true
}

// rangePartitionString returns the partition string for a range typed partition.
func rangePartitionString(pi *model.PartitionInfo) string {
	// partition by range expr
	if len(pi.Columns) == 0 {
		return pi.Expr
	}

	// partition by range columns (c1)
	if len(pi.Columns) == 1 {
		return pi.Columns[0].L
	}

	// partition by range columns (c1, c2, ...)
	panic("create table assert len(columns) = 1")
}

func generateRangePartitionExpr(ctx sessionctx.Context, pi *model.PartitionInfo,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	locateExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf bytes.Buffer
	schema := expression.NewSchema(columns...)
	partStr := rangePartitionString(pi)
	p := parser.New()
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Expr less than maxvalue is always true.
			fmt.Fprintf(&buf, "true")
		} else {
			fmt.Fprintf(&buf, "((%s) < (%s))", partStr, pi.Definitions[i].LessThan[0])
		}

		expr, err := parseSimpleExprWithNames(p, ctx, buf.String(), schema, names)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		locateExprs = append(locateExprs, expr)
		buf.Reset()
	}
	ret := &PartitionExpr{
		UpperBounds: locateExprs,
	}

	switch len(pi.Columns) {
	case 0:
		tmp, err := dataForRangePruning(ctx, pi)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.ForRangePruning = tmp
	case 1:
		tmp, err := dataForRangeColumnsPruning(ctx, pi, schema, names, p)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.ForRangeColumnsPruning = tmp
	default:
		panic("range column partition currently support only one column")
	}
	return ret, nil
}

func generateListPartitionExpr(ctx sessionctx.Context, pi *model.PartitionInfo,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	locateExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf, nullCondBuf bytes.Buffer
	schema := expression.NewSchema(columns...)
	partStr := pi.Expr
	p := parser.New()
	for _, def := range pi.Definitions {
		buf.WriteString("((%s) in (")
		for i, vs := range def.InValues {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString("('")
			buf.WriteString(strings.Join(vs, "','"))
			buf.WriteString("')")
		}
		buf.WriteString(")")
		for _, vs := range def.InValues {
			nullCondBuf.Reset()
			for _, value := range vs {
				expr, err := parseSimpleExprWithNames(p, ctx, value, schema, names)
				if err != nil {
					return nil, errors.Trace(err)
				}
				v, err := expr.Eval(chunk.Row{})
				if err != nil {
					return nil, errors.Trace(err)
				}
				if v.IsNull() {
					hasNull = true
					break
				}
			}
		}

		expr, err := parseSimpleExprWithNames(p, ctx, buf.String(), schema, names)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		locateExprs = append(locateExprs, expr)
		buf.Reset()
	}
	ret := &PartitionExpr{
		InValues: locateExprs,
	}
	return ret, nil
}

func generateHashPartitionExpr(ctx sessionctx.Context, pi *model.PartitionInfo,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	schema := expression.NewSchema(columns...)
	origExpr, err := parseExpr(parser.New(), pi.Expr)
	if err != nil {
		return nil, err
	}
	exprs, err := rewritePartitionExpr(ctx, origExpr, schema, names)
	if err != nil {
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", pi.Expr), zap.Error(err))
		return nil, errors.Trace(err)
	}
	exprs.HashCode(ctx.GetSessionVars().StmtCtx)
	return &PartitionExpr{
		Expr:     exprs,
		OrigExpr: origExpr,
	}, nil
}

// PartitionExpr returns the partition expression.
func (t *partitionedTable) PartitionExpr() (*PartitionExpr, error) {
	return t.partitionExpr, nil
}

// PartitionRecordKey is exported for test.
func PartitionRecordKey(pid int64, handle int64) kv.Key {
	recordPrefix := tablecodec.GenTableRecordPrefix(pid)
	return tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(handle))
}

// locatePartition returns the partition ID of the input record.
func (t *partitionedTable) locatePartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int64, error) {
	var err error
	var idx int
	switch t.meta.Partition.Type {
	case model.PartitionTypeRange:
		idx, err = t.locateRangePartition(ctx, pi, r)
	case model.PartitionTypeHash:
		idx, err = t.locateHashPartition(ctx, pi, r)
	case model.PartitionTypeList:
		idx, err = t.locateListPartition(ctx, pi, r)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return pi.Definitions[idx].ID, nil
}

func (t *partitionedTable) locateRangePartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	var err error
	var isNull bool
	partitionExprs := t.partitionExpr.UpperBounds
	idx := sort.Search(len(partitionExprs), func(i int) bool {
		var ret int64
		ret, isNull, err = partitionExprs[i].EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
		if err != nil {
			return true // Break the search.
		}
		if isNull {
			// If the column value used to determine the partition is NULL, the row is inserted into the lowest partition.
			// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-handling-nulls.html
			return true // Break the search.
		}
		return ret > 0
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		idx = 0
	}
	if idx < 0 || idx >= len(partitionExprs) {
		// The data does not belong to any of the partition returns `table has no partition for value %s`.
		var valueMsg string
		if pi.Expr != "" {
			e, err := expression.ParseSimpleExprWithTableInfo(ctx, pi.Expr, t.meta)
			if err == nil {
				val, _, err := e.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
				if err == nil {
					valueMsg = fmt.Sprintf("%d", val)
				}
			}
		} else {
			// When the table is partitioned by range columns.
			valueMsg = "from column_list"
		}
		return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
	}
	return idx, nil
}

func (t *partitionedTable) locateListPartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	for i, expr := range t.partitionExpr.InValues {
		ret, _, err := expr.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
		if err != nil {
			return 0, errors.Trace(err)
		}
		if ret > 0 {
			return i, nil
		}
	}
	// The data does not belong to any of the partition returns `table has no partition for value %s`.
	var valueMsg string
	if pi.Expr != "" {
		e, err := expression.ParseSimpleExprWithTableInfo(ctx, pi.Expr, t.meta)
		if err == nil {
			val, _, err := e.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
			if err == nil {
				valueMsg = fmt.Sprintf("%d", val)
			}
		}
	} else {
		// When the table is partitioned by list columns.
		valueMsg = "from column_list"
	}
	return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
}

// TODO: supports linear hashing
func (t *partitionedTable) locateHashPartition(ctx sessionctx.Context, pi *model.PartitionInfo, r []types.Datum) (int, error) {
	ret, isNull, err := t.partitionExpr.Expr.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
	if err != nil {
		return 0, err
	}
	if isNull {
		return 0, nil
	}
	ret = ret % int64(t.meta.Partition.Num)
	if ret < 0 {
		ret = -ret
	}
	return int(ret), nil
}

// GetPartition returns a Table, which is actually a partition.
func (t *partitionedTable) GetPartition(pid int64) table.PhysicalTable {
	// Attention, can't simply use `return t.partitions[pid]` here.
	// Because A nil of type *partition is a kind of `table.PhysicalTable`
	p, ok := t.partitions[pid]
	if !ok {
		return nil
	}
	return p
}

// GetPartitionByRow returns a Table, which is actually a Partition.
func (t *partitionedTable) GetPartitionByRow(ctx sessionctx.Context, r []types.Datum) (table.PhysicalTable, error) {
	pid, err := t.locatePartition(ctx, t.Meta().GetPartitionInfo(), r)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.partitions[pid], nil
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *partitionedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return partitionedTableAddRecord(ctx, t, r, nil, opts)
}

func partitionedTableAddRecord(ctx sessionctx.Context, t *partitionedTable, r []types.Datum, partitionSelection map[int64]struct{}, opts []table.AddRecordOption) (recordID kv.Handle, err error) {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if partitionSelection != nil {
		if _, ok := partitionSelection[pid]; !ok {
			return nil, errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}
	tbl := t.GetPartition(pid)
	return tbl.AddRecord(ctx, r, opts...)
}

// partitionTableWithGivenSets is used for this kind of grammar: partition (p0,p1)
// Basically it is the same as partitionedTable except that partitionTableWithGivenSets
// checks the given partition set for AddRecord/UpdateRecord operations.
type partitionTableWithGivenSets struct {
	*partitionedTable
	partitions map[int64]struct{}
}

// NewPartitionTableithGivenSets creates a new partition table from a partition table.
func NewPartitionTableithGivenSets(tbl table.PartitionedTable, partitions map[int64]struct{}) table.PartitionedTable {
	if raw, ok := tbl.(*partitionedTable); ok {
		return &partitionTableWithGivenSets{
			partitionedTable: raw,
			partitions:       partitions,
		}
	}
	return tbl
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *partitionTableWithGivenSets) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return partitionedTableAddRecord(ctx, t.partitionedTable, r, t.partitions, opts)
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *partitionedTable) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return errors.Trace(err)
	}

	tbl := t.GetPartition(pid)
	return tbl.RemoveRecord(ctx, h, r)
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *partitionedTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error {
	return partitionedTableUpdateRecord(ctx, sctx, t, h, currData, newData, touched, nil)
}

func (t *partitionTableWithGivenSets) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error {
	return partitionedTableUpdateRecord(ctx, sctx, t.partitionedTable, h, currData, newData, touched, t.partitions)
}

func partitionedTableUpdateRecord(gctx context.Context, ctx sessionctx.Context, t *partitionedTable, h kv.Handle, currData, newData []types.Datum, touched []bool, partitionSelection map[int64]struct{}) error {
	partitionInfo := t.meta.GetPartitionInfo()
	from, err := t.locatePartition(ctx, partitionInfo, currData)
	if err != nil {
		return errors.Trace(err)
	}
	to, err := t.locatePartition(ctx, partitionInfo, newData)
	if err != nil {
		return errors.Trace(err)
	}
	if partitionSelection != nil {
		if _, ok := partitionSelection[to]; !ok {
			return errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}

	// The old and new data locate in different partitions.
	// Remove record from old partition and add record to new partition.
	if from != to {
		_, err = t.GetPartition(to).AddRecord(ctx, newData)
		if err != nil {
			return errors.Trace(err)
		}
		// UpdateRecord should be side effect free, but there're two steps here.
		// What would happen if step1 succeed but step2 meets error? It's hard
		// to rollback.
		// So this special order is chosen: add record first, errors such as
		// 'Key Already Exists' will generally happen during step1, errors are
		// unlikely to happen in step2.
		err = t.GetPartition(from).RemoveRecord(ctx, h, currData)
		if err != nil {
			logutil.BgLogger().Error("update partition record fails", zap.String("message", "new record inserted while old record is not removed"), zap.Error(err))
			return errors.Trace(err)
		}
		return nil
	}

	tbl := t.GetPartition(to)
	return tbl.UpdateRecord(gctx, ctx, h, currData, newData, touched)
}

// FindPartitionByName finds partition in table meta by name.
func FindPartitionByName(meta *model.TableInfo, parName string) (int64, error) {
	// Hash partition table use p0, p1, p2, p3 as partition names automatically.
	parName = strings.ToLower(parName)
	for _, def := range meta.Partition.Definitions {
		if strings.EqualFold(def.Name.L, parName) {
			return def.ID, nil
		}
	}
	return -1, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs(parName, meta.Name.O))
}

func parseExpr(p *parser.Parser, exprStr string) (ast.ExprNode, error) {
	exprStr = "select " + exprStr
	stmts, _, err := p.Parse(exprStr, "", "")
	if err != nil {
		return nil, util.SyntaxWarn(err)
	}
	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	return fields[0].Expr, nil
}

func rewritePartitionExpr(ctx sessionctx.Context, field ast.ExprNode, schema *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	expr, err := expression.RewriteSimpleExprWithNames(ctx, field, schema, names)
	return expr, err
}
