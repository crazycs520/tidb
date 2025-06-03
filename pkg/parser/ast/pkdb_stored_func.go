package ast

import (
	"github.com/pingcap/tidb/pkg/parser/format"
)

// DropFunctionStmt represents the ast of `DROP FUNCTION ...`
type DropFunctionStmt struct {
	stmtNode

	IfExists bool
	Name     *TableName
}

// Restore implements Node interface.
func (n *DropFunctionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP FUNCTION ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	err := n.Name.Restore(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Accept implements Node interface.
func (n *DropFunctionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropFunctionStmt)
	return v.Leave(n)
}
