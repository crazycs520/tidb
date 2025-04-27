package core

import (
	"context"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/filter"
	"strings"
)

type queryCacheableChecker struct {
	sctx      sessionctx.Context
	cacheable bool
	is        infoschema.InfoSchema
}

func IsStmtQueryCacheable(ctx sessionctx.Context, stmt ast.StmtNode, is infoschema.InfoSchema) bool {
	checker := &queryCacheableChecker{
		sctx:      ctx,
		cacheable: true,
		is:        is,
	}
	stmt.Accept(checker)
	return checker.cacheable
}

func (checker *queryCacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt, *ast.FieldList, *ast.SelectField, *ast.TableRefsClause, *ast.Join, *ast.BetweenExpr, *ast.OnCondition,
		*ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.Assignment, *ast.ParenthesesExpr, *ast.RowExpr,
		*ast.TableSource, *ast.ColumnNameExpr, *ast.PatternInExpr, *ast.BinaryOperationExpr, *ast.ByItem, *ast.AggregateFuncExpr:
		return in, !checker.cacheable
	case *ast.OrderByClause, *ast.GroupByClause, *driver.ValueExpr, ast.ParamMarkerExpr, *ast.ColumnName, *ast.Limit:
		return in, !checker.cacheable
	case *ast.FuncCallExpr:
		if _, found := expression.QueryUnCacheableFunctions[node.FnName.L]; found {
			checker.cacheable = false
		}
		return in, !checker.cacheable
	case *ast.TableName:
		if filter.IsSystemSchema(node.Schema.O) {
			checker.cacheable = false
			return in, !checker.cacheable
		}
		if checker.is != nil {
			checker.cacheable = checkTableQueryCacheable(checker.sctx, checker.is, node)
		}
		return in, !checker.cacheable
	}
	checker.cacheable = false // unexpected cases
	return in, !checker.cacheable
}

func (checker *queryCacheableChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.cacheable
}

func checkTableQueryCacheable(sctx sessionctx.Context, schema infoschema.InfoSchema, node *ast.TableName) (cacheable bool) {
	tableSchema := node.Schema
	if tableSchema.L == "" {
		tableSchema.O = sctx.GetSessionVars().CurrentDB
		tableSchema.L = strings.ToLower(tableSchema.O)
	}
	tb, err := schema.TableByName(context.Background(), tableSchema, node.Name)
	if err != nil {
		return false
	}

	if tb.Meta().GetPartitionInfo() != nil {
		return false
	}

	if tb.Meta().TempTableType != model.TempTableNone {
		return false
	}

	if !tb.Type().IsNormalTable() {
		return false
	}

	return true
}
