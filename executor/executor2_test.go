package executor_test

import (
	"github.com/pingcap/tidb/testkit"
	"testing"
)

func TestDebug(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (pk int key, val int)")
	tk.MustExec("insert into t values(1,1);")

	tk.MustExec("begin")
	tk.MustQuery("select * from t where pk > 0 for update")
	tk.MustExec("rollback")
}
