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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sessiontest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestFailStatementCommitInRetry(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (id int)")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2),(3),(4),(5)")
	tk.MustExec("insert into t values (6)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockCommitError8942", `return(true)`))
	_, err := tk.Exec("commit")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockCommitError8942"))

	tk.MustExec("insert into t values (6)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("6"))
}

func TestGetTSFailDirtyState(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (id int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockGetTSFail", "return"))
	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return fpname == "github.com/pingcap/tidb/session/mockGetTSFail"
	})
	_, err := tk.Session().Execute(ctx, "select * from t")
	if config.GetGlobalConfig().Store == "unistore" {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	// Fix a bug that active txn fail set TxnState.fail to error, and then the following write
	// affected by this fail flag.
	tk.MustExec("insert into t values (1)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockGetTSFail"))
}

func TestGetTSFailDirtyStateInretry(t *testing.T) {
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/mockCommitError"))
		require.NoError(t, failpoint.Disable("tikvclient/mockGetTSErrorInRetry"))
	}()

	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t (id int)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/mockCommitError", `return(true)`))
	// This test will mock a PD timeout error, and recover then.
	// Just make mockGetTSErrorInRetry return true once, and then return false.
	require.NoError(t, failpoint.Enable("tikvclient/mockGetTSErrorInRetry",
		`1*return(true)->return(false)`))
	tk.MustExec("insert into t values (2)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2"))
}

func TestKillFlagInBackoff(t *testing.T) {
	// This test checks the `killed` flag is passed down to the backoffer through
	// session.KVVars.
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table kill_backoff (id int)")
	// Inject 1 time timeout. If `Killed` is not successfully passed, it will retry and complete query.
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("timeout")->return("")`))
	defer failpoint.Disable("tikvclient/tikvStoreSendReqResult")
	// Set kill flag and check its passed to backoffer.
	tk.Session().GetSessionVars().Killed = 1
	rs, err := tk.Exec("select * from kill_backoff")
	require.NoError(t, err)
	_, err = session.ResultSetToStringSlice(context.TODO(), tk.Session(), rs)
	// `interrupted` is returned when `Killed` is set.
	require.Regexp(t, ".*Query execution was interrupted.*", err.Error())
	rs.Close()
}

func TestClusterTableSendError(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	require.NoError(t, failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("requestTiDBStoreError")`))
	defer func() { require.NoError(t, failpoint.Disable("tikvclient/tikvStoreSendReqResult")) }()
	tk.MustQuery("select * from information_schema.cluster_slow_query")
	require.Equal(t, tk.Session().GetSessionVars().StmtCtx.WarningCount(), uint16(1))
	require.Regexp(t, ".*TiDB server timeout, address is.*", tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error())
}

func TestIngestMVIndexOnPartitionTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	cases := []string{
		//"alter table t add index idx((cast(a as signed array)));",
		//"alter table t add unique index idx(pk, (cast(a as signed array)));",
		"alter table t add column d bigint generated always as (pk + 1);",
	}
	for _, c := range cases {
		tk.MustExec("drop database if exists addindexlit;")
		tk.MustExec("create database addindexlit;")
		tk.MustExec("use addindexlit;")
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

		var sb strings.Builder

		tk.MustExec("drop table if exists t")
		//tk.MustExec("create table t (pk int primary key, a json)")
		tk.MustExec("create table t (pk int primary key, a json) partition by hash(pk) partitions 4;")
		tk.MustExec(sb.String())
		var wg sync.WaitGroup
		wg.Add(1)
		var addIndexDone atomic.Bool
		go func() {
			defer wg.Done()
			n := 10240
			internalTK := testkit.NewTestKit(t, store)
			internalTK.MustExec("use addindexlit;")

			for i := 0; i < 1024; i++ {
				internalTK.MustExec(fmt.Sprintf("insert into t (pk, a) values (%d, '[%d, %d, %d]')", n, n, n+1, n+2))
				internalTK.MustQuery(fmt.Sprintf("select * from t where pk = %d", n-10)).Rows()
				internalTK.MustExec(fmt.Sprintf("delete from t where pk = %d", n-10))
				internalTK.MustExec(fmt.Sprintf("update t set a = '[%d, %d, %d]' where pk = %d", n-3, n-2, n+1000, n-5))
				n++
				if i > 256 && addIndexDone.Load() {
					break
				}
			}
		}()
		time.Sleep(time.Millisecond * 100)
		tk.MustExec(c)
		rows := tk.MustQuery("admin show ddl jobs 1;").Rows()
		require.Len(t, rows, 1)
		//jobTp := rows[0][12].(string)
		//require.True(t, strings.Contains(jobTp, "ingest"), jobTp)
		//addIndexDone.Store(true)
		wg.Wait()
		tk.MustExec("admin check table t")
	}
}

func TestAutoCommitNeedNotLinearizability(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t1;")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1 (c int)`)

	require.NoError(t, failpoint.Enable("tikvclient/getMinCommitTSFromTSO", `panic`))
	defer func() { require.NoError(t, failpoint.Disable("tikvclient/getMinCommitTSFromTSO")) }()

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_async_commit", "1"))
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_guarantee_linearizability", "1"))

	// Auto-commit transactions don't need to get minCommitTS from TSO
	tk.MustExec("INSERT INTO t1 VALUES (1)")

	tk.MustExec("BEGIN")
	tk.MustExec("INSERT INTO t1 VALUES (2)")
	// An explicit transaction needs to get minCommitTS from TSO
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	tk.MustExec("set autocommit = 0")
	tk.MustExec("INSERT INTO t1 VALUES (3)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	// Same for 1PC
	tk.MustExec("set autocommit = 1")
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_1pc", "1"))
	tk.MustExec("INSERT INTO t1 VALUES (4)")

	tk.MustExec("BEGIN")
	tk.MustExec("INSERT INTO t1 VALUES (5)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()

	tk.MustExec("set autocommit = 0")
	tk.MustExec("INSERT INTO t1 VALUES (6)")
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		tk.MustExec("COMMIT")
	}()
}

func TestKill(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("kill connection_id();")
}
