package expression

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestSysContext(t *testing.T) {
	ctx := createContext(t)
	sessionVars := ctx.GetSessionVars()
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	sessionVars.CurrentDB = "test"
	sessionVars.ConnectionID = uint64(1)
	fc := funcs[ast.SysContext]
	cases := []struct {
		args      []any
		res       any
		expectErr string
	}{
		{args: []any{"USERENV", "CURRENT_SCHEMA"}, res: "test"},
		{args: []any{"USERENV", "CURRENT_USER"}, res: "root"},
		{args: []any{"USERENV", "SESSIONID"}, res: "1"},
		{args: []any{"USERENV", "CURRENT_SCHEMA", 2}, res: "te"},
		{args: []any{"USERENV", "CURRENT_SCHEMA", -1}, res: "test"},
		{args: []any{"USERENV", "CURRENT_SCHEMA", 65536}, res: "test"},
		{args: []any{"USERENV", "NE"}, expectErr: "invalid USERENV parameter"},
		{args: []any{"NE", "NE"}, expectErr: "invalid namespace"},
	}
	for _, tc := range cases {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		res, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		if len(tc.expectErr) == 0 {
			require.NoError(t, err)
			require.Equal(t, tc.res, res.GetString())
		} else {
			require.ErrorContains(t, err, tc.expectErr)
		}
	}
}
