package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

type pbPlanBuilder struct {
	sctx    sessionctx.Context
	tps     []*types.FieldType
	columns []*model.ColumnInfo
	is      infoschema.InfoSchema
}

func NewPBPlanBuilder(sctx sessionctx.Context, is infoschema.InfoSchema) *pbPlanBuilder {
	return &pbPlanBuilder{sctx: sctx, is: is}
}

func (b *pbPlanBuilder) PBToPhysicalPlan(e *tipb.Executor) (p PhysicalPlan, err error) {
	switch e.Tp {
	case tipb.ExecType_TypeMemTableScan:
		p, err = b.pbToMemTableScan(e)
	case tipb.ExecType_TypeSelection:
		p, err = b.pbToSelection(e)
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", e.GetTp())
	}
	return p, err
}

func (b *pbPlanBuilder) pbToMemTableScan(e *tipb.Executor) (PhysicalPlan, error) {
	memTbl := e.MemTblScan
	if !infoschema.IsClusterTable(memTbl.TableName) {
		return nil, errors.Errorf("table %s is not a tidb memory table", memTbl.TableName)
	}
	tbl, err := b.is.TableByName(model.NewCIStr("information_schema"), model.NewCIStr(memTbl.TableName))
	if err != nil {
		return nil, err
	}
	columns, tps, err := convertColumnInfo(tbl.Meta(), memTbl)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	for _, col := range tbl.Cols() {
		for _, colInfo := range columns {
			if col.ID == colInfo.ID {
				newCol := &expression.Column{
					UniqueID: b.sctx.GetSessionVars().AllocPlanColumnID(),
					ID:       col.ID,
					RetType:  &col.FieldType,
				}
				schema.Append(newCol)
			}
		}
	}
	b.tps = tps
	b.columns = columns

	p := PhysicalMemTable{
		DBName:  model.NewCIStr("information_schema"),
		Table:   tbl.Meta(),
		Columns: columns,
	}.Init(b.sctx, nil, 0)
	p.SetSchema(schema)
	return p, nil
}

func (b *pbPlanBuilder) pbToSelection(e *tipb.Executor) (PhysicalPlan, error) {
	conds, err := convertToExprs(b.sctx.GetSessionVars().StmtCtx, b.tps, e.Selection.Conditions)
	if err != nil {
		return nil, err
	}
	p := PhysicalSelection{
		Conditions: conds,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func convertColumnInfo(tblInfo *model.TableInfo, memTbl *tipb.MemTableScan) ([]*model.ColumnInfo, []*types.FieldType, error) {
	columns := make([]*model.ColumnInfo, 0, len(memTbl.Columns))
	tps := make([]*types.FieldType, 0, len(memTbl.Columns))
	for _, col := range memTbl.Columns {
		found := false
		for _, colInfo := range tblInfo.Columns {
			if col.ColumnId == colInfo.ID {
				columns = append(columns, colInfo)
				tps = append(tps, colInfo.FieldType.Clone())
				found = true
				break
			}
		}
		if !found {
			return nil, nil, errors.Errorf("Column ID %v of table not found", col.ColumnId, "information_schema."+memTbl.TableName)
		}
	}
	return columns, tps, nil
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
