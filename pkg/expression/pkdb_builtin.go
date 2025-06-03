package expression

import (
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func init() {
	funcs[ast.SysContext] = &sysContextFunctionClass{baseFunctionClass{ast.SysContext, 2, 3}}
}

var (
	_ functionClass = &sysContextFunctionClass{}
)

var (
	_ builtinFunc = &builtinSysContextSig{}
)

type sysContextFunctionClass struct {
	baseFunctionClass
}

// getFunction gets a function signature by the types and the counts of given arguments.
func (c *sysContextFunctionClass) getFunction(ctx BuildContext, args []Expression) (_ builtinFunc, _ error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETString, types.ETString, types.ETInt}
	if len(args) == 2 {
		argTps = argTps[:2]
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinSysContextSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinSysContextSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinSysContextSig) Clone() builtinFunc {
	newSig := &builtinSysContextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinSysContextSig) RequiredOptionalEvalProps() (set OptionalEvalPropKeySet) {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalString evals a builtinSysContextSig.
// See https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/SYS_CONTEXT.html
func (b *builtinSysContextSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	namespace, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	}
	if isNull || len(namespace) == 0 {
		return "", false, nil
	}
	parameter, isNull, err := b.args[1].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	}
	if isNull || len(parameter) == 0 {
		return "", false, nil
	}
	length := int64(256)
	if len(b.args) == 3 {
		length, isNull, err = b.args[2].EvalInt(ctx, row)
		if err != nil {
			return "", true, err
		}
		if isNull || length <= 0 || length >= 4000 {
			length = 256
		}
	}
	if !strings.EqualFold(namespace, "USERENV") {
		return "", true, errors.Errorf("invalid namespace")
	}
	data, err := b.GetSessionVars(ctx)
	if err != nil {
		return "", true, err
	}
	if data == nil {
		return "", true, errors.Errorf("Missing session variable")
	}
	var ret string
	switch strings.ToUpper(parameter) {
	case "CURRENT_SCHEMA":
		ret = data.CurrentDB
	case "CURRENT_USER":
		ret = data.User.Username
	case "SESSIONID":
		ret = strconv.FormatUint(data.ConnectionID, 10)
	default:
		return "", true, errors.Errorf("invalid USERENV parameter")
	}
	if length < int64(len(ret)) {
		ret = ret[:length]
	}
	return ret, false, nil
}
