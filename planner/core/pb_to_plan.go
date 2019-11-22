package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tipb/go-tipb"
)

func PBToPhysicalPlan(sctx sessionctx.Context, e *tipb.Executor) (p PhysicalPlan, err error) {
	switch e.Tp {
	case tipb.ExecType_TypeMemTableScan:
		p, err = PBToPhysicalTableScan(sctx, e)
	}
	return p, err
}

func PBToPhysicalTableScan(sctx sessionctx.Context, e *tipb.Executor) (PhysicalPlan, error) {
	memTbl := e.MemTblScan
	tbl, err := domain.GetDomain(sctx).InfoSchema().TableByName(model.NewCIStr("information_schema"), model.NewCIStr(memTbl.TableName))
	if err != nil {
		return nil, err
	}
	columns, err := convertColumnInfo(tbl.Meta(), memTbl)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	for _, col := range tbl.Cols() {
		for _, colInfo := range columns {
			if col.ID == colInfo.ID {
				newCol := &expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					ID:       col.ID,
					RetType:  &col.FieldType,
				}
				schema.Append(newCol)
			}
		}
	}

	p := PhysicalMemTable{
		DBName:  model.NewCIStr("information_schema"),
		Table:   tbl.Meta(),
		Columns: columns,
	}.Init(sctx, nil, 0)
	p.SetSchema(schema)
	return p, nil
}

func convertColumnInfo(tblInfo *model.TableInfo, memTbl *tipb.MemTableScan) ([]*model.ColumnInfo, error) {
	columns := make([]*model.ColumnInfo, 0, len(memTbl.Columns))
	for _, col := range memTbl.Columns {
		found := false
		for _, colInfo := range tblInfo.Columns {
			if col.ColumnId == colInfo.ID {
				columns = append(columns, colInfo)
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf("Column ID %v of table not found", col.ColumnId, "information_schema."+memTbl.TableName)
		}
	}
	return columns, nil
}
