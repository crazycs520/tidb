package expression

import "C"
import (
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// [2]string -> StoredFuncClass
var StoredFunc sync.Map

type StoredFuncClass struct {
	Name    [2]string
	RetType *types.FieldType
}

var (
	Eval4StoredFunc            func(sctx sessionctx.Context, stmt any, node *ast.CallStmt) (*types.Datum, error)
	GetCallStmt4StoredFuncExpr func(ctx context.Context, sctx sessionctx.Context, node *ast.CallStmt) (any, error)
)

func (s *StoredFuncClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	funcName := s.Name[0] + "." + s.Name[1]
	bf, err := newBaseBuiltinFunc(ctx, funcName, args, s.RetType.Clone())
	if err != nil {
		return nil, err
	}
	node := &ast.CallStmt{Procedure: &ast.FuncCallExpr{}}
	node.Procedure.Schema = model.NewCIStr(s.Name[0])
	node.Procedure.FnName = model.NewCIStr(s.Name[1])
	node.Procedure.Args = make([]ast.ExprNode, len(args))
	ectx := ctx.GetEvalCtx().(*sessionexpr.EvalContext)
	callStmt, err := GetCallStmt4StoredFuncExpr(context.Background(), ectx.Sctx(), node)
	if err != nil {
		return nil, err
	}

	// Though currently, `getFunction` does not require too much information that makes it safe to be cached,
	// we still skip the plan cache for loadable functions because there are no strong requirements to do it.
	// Skipping the plan cache can make the behavior simple.
	ctx.SetSkipPlanCache("loadable function should not be cached")
	sig := &storedFuncSig{
		baseBuiltinFunc: bf,
		sctx:            ectx.Sctx(),
		callStmt:        callStmt,
		node:            node,
	}
	return sig, nil
}

func (s *StoredFuncClass) verifyArgsByCount(l int) error {
	return nil
}

type storedFuncSig struct {
	baseBuiltinFunc

	sctx     sessionctx.Context
	callStmt any
	node     *ast.CallStmt
}

func (b *storedFuncSig) Clone() builtinFunc {
	newSig := &storedFuncSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.sctx = b.sctx
	newSig.callStmt = b.callStmt
	newSig.node = b.node
	return newSig
}

func (b *storedFuncSig) setArgs(ctx EvalContext, row chunk.Row) error {
	b.node.Procedure.Args = make([]ast.ExprNode, len(b.args))
	for i, arg := range b.args {
		d, err := arg.Eval(ctx, row)
		if err != nil {
			return err
		}
		b.node.Procedure.Args[i] = &driver.ValueExpr{
			Datum: d,
		}
	}
	return nil
}

func (b *storedFuncSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	if err := b.setArgs(ctx, row); err != nil {
		return "", false, err
	}

	d, err := Eval4StoredFunc(b.sctx, b.callStmt, b.node)
	if err != nil {
		return "", false, err
	}
	if d.IsNull() {
		return "", true, nil
	}
	return d.GetString(), false, nil
}

func (b *storedFuncSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	if err := b.setArgs(ctx, row); err != nil {
		return 0, false, err
	}

	d, err := Eval4StoredFunc(b.sctx, b.callStmt, b.node)
	if err != nil {
		return 0, false, err
	}
	if d.IsNull() {
		return 0, true, nil
	}
	return d.GetInt64(), false, nil
}

func (b *storedFuncSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	if err := b.setArgs(ctx, row); err != nil {
		return 0, false, err
	}

	d, err := Eval4StoredFunc(b.sctx, b.callStmt, b.node)
	if err != nil {
		return 0, false, err
	}
	if d.IsNull() {
		return 0, true, nil
	}
	return d.GetFloat64(), false, nil
}

func (b *storedFuncSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	if err := b.setArgs(ctx, row); err != nil {
		return nil, false, err
	}

	d, err := Eval4StoredFunc(b.sctx, b.callStmt, b.node)
	if err != nil {
		return nil, false, err
	}
	if d.IsNull() {
		return nil, true, nil
	}
	return d.GetMysqlDecimal(), false, nil
}

func (b *storedFuncSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	if err := b.setArgs(ctx, row); err != nil {
		return types.ZeroTime, false, err
	}

	d, err := Eval4StoredFunc(b.sctx, b.callStmt, b.node)
	if err != nil {
		return types.ZeroTime, false, err
	}
	if d.IsNull() {
		return types.ZeroTime, true, nil
	}
	return d.GetMysqlTime(), false, nil
}

func (b *storedFuncSig) evalDuration(ctx EvalContext, row chunk.Row) (val types.Duration, isNull bool, err error) {
	if err := b.setArgs(ctx, row); err != nil {
		return types.Duration{}, false, err
	}

	d, err := Eval4StoredFunc(b.sctx, b.callStmt, b.node)
	if err != nil {
		return types.Duration{}, false, err
	}
	if d.IsNull() {
		return types.Duration{}, true, nil
	}
	return d.GetMysqlDuration(), false, nil
}

func (b *storedFuncSig) evalJSON(ctx EvalContext, row chunk.Row) (val types.BinaryJSON, isNull bool, err error) {
	if err := b.setArgs(ctx, row); err != nil {
		return types.BinaryJSON{}, false, err
	}

	d, err := Eval4StoredFunc(b.sctx, b.callStmt, b.node)
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	if d.IsNull() {
		return types.BinaryJSON{}, true, nil
	}
	return d.GetMysqlJSON(), false, nil
}
