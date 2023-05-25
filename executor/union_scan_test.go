// Copyright 2016 PingCAP, Inc.
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

package executor_test

import (
	"encoding/hex"
	"fmt"
	"github.com/pingcap/tidb/kv"
	"testing"
	"time"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/benchdaily"
	"github.com/stretchr/testify/require"
)

func TestDirtyTransaction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
	tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, index idx_b (b));")
	tk.MustExec("insert t value (2, 3), (4, 8), (6, 8)")
	tk.MustExec("begin")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 3", "4 8", "6 8"))
	tk.MustExec("insert t values (1, 5), (3, 4), (7, 6)")
	tk.MustQuery("select * from information_schema.columns")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 5", "2 3", "3 4", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 5"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Rows("7 6", "6 8", "4 8", "3 4", "2 3", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Rows("2 3", "3 4", "1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Rows("6 8", "4 8", "7 6", "1 5", "3 4", "2 3"))
	tk.MustQuery("select b from t where b = 8 order by b desc").Check(testkit.Rows("8", "8"))
	// Delete a snapshot row and a dirty row.
	tk.MustExec("delete from t where a = 2 or a = 3")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 5", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Rows("7 6", "6 8", "4 8", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Rows("1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Rows("6 8", "4 8", "7 6", "1 5"))
	// Add deleted row back.
	tk.MustExec("insert t values (2, 3), (3, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 5", "2 3", "3 4", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Rows("7 6", "6 8", "4 8", "3 4", "2 3", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Rows("2 3", "3 4", "1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Rows("6 8", "4 8", "7 6", "1 5", "3 4", "2 3"))
	// Truncate Table
	tk.MustExec("truncate table t")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert t values (1, 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("truncate table t")
	tk.MustExec("insert t values (3, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("3 4"))
	tk.MustExec("commit")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert t values (2, 3), (4, 5), (6, 7)")
	tk.MustExec("begin")
	tk.MustExec("insert t values (0, 1)")
	tk.MustQuery("select * from t where b = 3").Check(testkit.Rows("2 3"))
	tk.MustExec("commit")

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a json, b bigint);`)
	tk.MustExec(`begin;`)
	tk.MustExec(`insert into t values("\"1\"", 1);`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows(`"1" 1`))
	tk.MustExec(`commit;`)

	tk.MustExec(`drop table if exists t`)
	tk.MustExec("create table t(a int, b int, c int, d int, index idx(c, d))")
	tk.MustExec("begin")
	tk.MustExec("insert into t values(1, 2, 3, 4)")
	tk.MustQuery("select * from t use index(idx) where c > 1 and d = 4").Check(testkit.Rows("1 2 3 4"))
	tk.MustExec("commit")

	// Test partitioned table use wrong table ID.
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (c1 smallint(6) NOT NULL, c2 char(5) DEFAULT NULL) PARTITION BY RANGE ( c1 ) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN (20),
			PARTITION p2 VALUES LESS THAN (30),
			PARTITION p3 VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t where c1 < 5").Check(testkit.Rows("1 1"))
	tk.MustQuery("select c2 from t").Check(testkit.Rows("1"))
	tk.MustExec("commit")

	// Test general virtual column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int as (a+1), c int as (b+1), index(c));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1, default, default), (2, default, default), (3, default, default);")
	// TableReader
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3", "2 3 4", "3 4 5"))
	tk.MustQuery("select b from t;").Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select c from t;").Check(testkit.Rows("3", "4", "5"))
	tk.MustQuery("select a from t;").Check(testkit.Rows("1", "2", "3"))
	// IndexReader
	tk.MustQuery("select c from t where c > 3;").Check(testkit.Rows("4", "5"))
	tk.MustQuery("select c from t order by c;").Check(testkit.Rows("3", "4", "5"))
	// IndexLookup
	tk.MustQuery("select * from t where c > 3;").Check(testkit.Rows("2 3 4", "3 4 5"))
	tk.MustQuery("select a, b from t use index(c) where c > 3;").Check(testkit.Rows("2 3", "3 4"))
	tk.MustQuery("select a, c from t use index(c) where c > 3;").Check(testkit.Rows("2 4", "3 5"))
	tk.MustQuery("select b, c from t use index(c) where c > 3;").Check(testkit.Rows("3 4", "4 5"))
	// Delete and update some data
	tk.MustExec("delete from t where c > 4;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3", "2 3 4"))
	tk.MustExec("update t set a = 3 where b > 1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4 5", "3 4 5"))
	tk.MustExec("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4 5", "3 4 5"))
	// Again with non-empty table
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1, default, default), (2, default, default), (3, default, default);")
	// TableReader
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4 5", "3 4 5", "1 2 3", "2 3 4", "3 4 5"))
	tk.MustQuery("select b from t;").Check(testkit.Rows("4", "4", "2", "3", "4"))
	tk.MustQuery("select c from t;").Check(testkit.Rows("3", "4", "5", "5", "5"))
	tk.MustQuery("select a from t;").Check(testkit.Rows("3", "3", "1", "2", "3"))
	// IndexReader
	tk.MustQuery("select c from t where c > 3;").Check(testkit.Rows("4", "5", "5", "5"))
	tk.MustQuery("select c from t order by c;").Check(testkit.Rows("3", "4", "5", "5", "5"))
	// IndexLookup
	tk.MustQuery("select * from t where c > 3;").Check(testkit.Rows("3 4 5", "3 4 5", "2 3 4", "3 4 5"))
	tk.MustQuery("select a, b from t use index(c) where c > 3;").Check(testkit.Rows("2 3", "3 4", "3 4", "3 4"))
	tk.MustQuery("select a, c from t use index(c) where c > 3;").Check(testkit.Rows("2 4", "3 5", "3 5", "3 5"))
	tk.MustQuery("select b, c from t use index(c) where c > 3;").Check(testkit.Rows("3 4", "4 5", "4 5", "4 5"))
	// Delete and update some data
	tk.MustExec("delete from t where c > 4;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3", "2 3 4"))
	tk.MustExec("update t set a = 3 where b > 2;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3", "3 4 5"))
	tk.MustExec("commit;")
}

func TestUnionScanWithCastCondition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table ta (a varchar(20))")
	tk.MustExec("insert ta values ('1'), ('2')")
	tk.MustExec("create table tb (a varchar(20))")
	tk.MustExec("begin")
	tk.MustQuery("select * from ta where a = 1").Check(testkit.Rows("1"))
	tk.MustExec("insert tb values ('0')")
	tk.MustQuery("select * from ta where a = 1").Check(testkit.Rows("1"))
	tk.MustExec("rollback")
}

func TestUnionScanForMemBufferReader(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int, index idx(b))")
	tk.MustExec("insert t values (1,1),(2,2)")

	// Test for delete in union scan
	tk.MustExec("begin")
	tk.MustExec("delete from t")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert t values (1,1)")
	tk.MustQuery("select a,b from t").Check(testkit.Rows("1 1"))
	tk.MustQuery("select a,b from t use index(idx)").Check(testkit.Rows("1 1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test update with untouched index columns.
	tk.MustExec("delete from t")
	tk.MustExec("insert t values (1,1),(2,2)")
	tk.MustExec("begin")
	tk.MustExec("update t set a=a+1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2"))
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("2 1", "3 2"))
	tk.MustQuery("select * from t use index (idx) order by b desc").Check(testkit.Rows("3 2", "2 1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test update with index column.
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2"))
	tk.MustExec("begin")
	tk.MustExec("update t set b=b+1 where a=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 2", "3 2"))
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("2 2", "3 2"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test index reader order.
	tk.MustQuery("select * from t").Check(testkit.Rows("2 2", "3 2"))
	tk.MustExec("begin")
	tk.MustExec("insert t values (3,3),(1,1),(4,4),(-1,-1);")
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("-1 -1", "1 1", "2 2", "3 2", "3 3", "4 4"))
	tk.MustQuery("select b from t use index (idx) order by b desc").Check(testkit.Rows("4", "3", "2", "2", "1", "-1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// test for update unique index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int, unique index idx(b))")
	tk.MustExec("insert t values (1,1),(2,2)")
	tk.MustExec("begin")
	tk.MustGetErrMsg("update t set b=b+1", "[kv:1062]Duplicate entry '2' for key 't.idx'")
	// update with unchange index column.
	tk.MustExec("update t set a=a+1")
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("2 1", "3 2"))
	tk.MustQuery("select b from t use index (idx)").Check(testkit.Rows("1", "2"))
	tk.MustExec("update t set b=b+2 where a=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 3", "3 2"))
	tk.MustQuery("select * from t use index (idx) order by b desc").Check(testkit.Rows("2 3", "3 2"))
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("3 2", "2 3"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test for getMissIndexRowsByHandle return nil.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int, index idx(a))")
	tk.MustExec("insert into t values (1,1),(2,2),(3,3)")
	tk.MustExec("begin")
	tk.MustExec("update t set b=0 where a=2")
	tk.MustQuery("select * from t ignore index (idx) where a>0 and b>0;").Check(testkit.Rows("1 1", "3 3"))
	tk.MustQuery("select * from t use index (idx) where a>0 and b>0;").Check(testkit.Rows("1 1", "3 3"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test index lookup reader corner case.
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt (a bigint, b int,c int,primary key (a,b));")
	tk.MustExec("insert into tt set a=1,b=1;")
	tk.MustExec("begin;")
	tk.MustExec("update tt set c=1;")
	tk.MustQuery("select * from tt use index (PRIMARY) where c is not null;").Check(testkit.Rows("1 1 1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table tt")

	// Test index reader corner case.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int,primary key(a,b));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t1 values(1, 1);")
	tk.MustQuery("select * from t1 use index(primary) where a=1;").Check(testkit.Rows("1 1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t1;")

	// Test index reader with pk handle.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int unsigned key,b int,c varchar(10), index idx(b,a,c));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t1 (a,b) values (0, 0), (1, 1);")
	tk.MustQuery("select a,b from t1 use index(idx) where b>0;").Check(testkit.Rows("1 1"))
	tk.MustQuery("select a,b,c from t1 ignore index(idx) where a>=1 order by a desc").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("insert into t1 values (2, 2, null), (3, 3, 'a');")
	tk.MustQuery("select a,b from t1 use index(idx) where b>1 and c is not null;").Check(testkit.Rows("3 3"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t1;")

	// Test insert and update with untouched index.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int,c int,index idx(b));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2);")
	tk.MustExec("update t1 set c=c+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx);").Check(testkit.Rows("1 1 2", "2 2 2"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t1;")

	// Test insert and update with untouched unique index.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int,c int,unique index idx(b));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2);")
	tk.MustExec("update t1 set c=c+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx);").Check(testkit.Rows("1 1 2", "2 2 2"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t1;")

	// Test update with 2 index, one untouched, the other index is touched.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int,c int,unique index idx1(a), index idx2(b));")
	tk.MustExec("insert into t1 values (1, 1, 1);")
	tk.MustExec("update t1 set b=b+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx2);").Check(testkit.Rows("1 2 1"))
	tk.MustExec("admin check table t1;")
}

func TestForUpdateUntouchedIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	checkFunc := func() {
		tk.MustExec("begin")
		tk.MustExec("insert into t values ('a', 1), ('b', 3), ('a', 2) on duplicate key update b = b + 1;")
		tk.MustExec("commit")
		tk.MustExec("admin check table t")

		// Test for autocommit
		tk.MustExec("set autocommit=0")
		tk.MustExec("insert into t values ('a', 1), ('b', 3), ('a', 2) on duplicate key update b = b + 1;")
		tk.MustExec("set autocommit=1")
		tk.MustExec("admin check table t")
	}

	// Test for primary key.
	tk.MustExec("create table t (a varchar(10) primary key,b int)")
	checkFunc()

	// Test for unique key.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10),b int, unique index(a))")
	checkFunc()

	// Test for on duplicate update also conflict too.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int, unique index(a))")
	tk.MustExec("begin")
	_, err := tk.Exec("insert into t values (1, 1), (2, 2), (1, 3) on duplicate key update a = a + 1;")
	require.NotNil(t, err)
	require.EqualError(t, err, "[kv:1062]Duplicate entry '2' for key 't.a'")
	tk.MustExec("commit")
	tk.MustExec("admin check table t")
}

func TestUpdateScanningHandles(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int primary key, b int);")
	tk.MustExec("begin")
	var insertSQL string
	for i := 2; i < 10000; i++ {
		if i == 2 {
			insertSQL += fmt.Sprintf("(%d, %d)", i, i)
		} else {
			insertSQL += fmt.Sprintf(",(%d, %d)", i, i)
		}
	}
	tk.MustExec(fmt.Sprintf("insert into t values %s;", insertSQL))
	tk.MustExec("commit;")

	tk.MustExec("set tidb_distsql_scan_concurrency = 1;")
	tk.MustExec("set tidb_index_lookup_join_concurrency = 1;")
	tk.MustExec("set tidb_projection_concurrency=1;")
	tk.MustExec("set tidb_init_chunk_size=1;")
	tk.MustExec("set tidb_max_chunk_size=32;")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("update /*+ INL_JOIN(t1) */ t t1, (select a, b from t) t2 set t1.b = t2.b where t1.a = t2.a + 1000;")
	result := tk.MustQuery("select a, a-b from t where a > 1000 and a - b != 1000;")
	require.Len(t, result.Rows(), 0)
	tk.MustExec("rollback;")
}

// See https://github.com/pingcap/tidb/issues/19136
func TestForApplyAndUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t ( c_int int, c_str varchar(40), primary key(c_int, c_str) )")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1, 'amazing almeida'), (2, 'boring bardeen'), (3, 'busy wescoff')")
	tk.MustQuery("select c_int, (select t1.c_int from t t1 where t1.c_int = 3 and t1.c_int > t.c_int order by t1.c_int limit 1) x from t").Check(testkit.Rows("1 3", "2 3", "3 <nil>"))
	tk.MustExec("commit")
	tk.MustQuery("select c_int, (select t1.c_int from t t1 where t1.c_int = 3 and t1.c_int > t.c_int order by t1.c_int limit 1) x from t").Check(testkit.Rows("1 3", "2 3", "3 <nil>"))

	// See https://github.com/pingcap/tidb/issues/19435
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t1(c_int int)")
	tk.MustExec("create table t(c_int int)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5),(6),(7),(8),(9)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(18)")
	tk.MustQuery("select (select min(t1.c_int) from t1 where t1.c_int > t.c_int), (select max(t1.c_int) from t1 where t1.c_int> t.c_int), (select sum(t1.c_int) from t1 where t1.c_int> t.c_int) from t").Check(testkit.Rows("18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18"))
	tk.MustExec("rollback")

	// See https://github.com/pingcap/tidb/issues/19431
	tk.MustExec("DROP TABLE IF EXISTS `t`")
	tk.MustExec("CREATE TABLE `t` ( `c_int` int(11) NOT NULL, `c_str` varchar(40) NOT NULL, `c_datetime` datetime NOT NULL, PRIMARY KEY (`c_int`,`c_str`,`c_datetime`), KEY `c_str` (`c_str`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("INSERT INTO `t` VALUES (1,'cool pasteur','2020-04-21 19:01:04'),(3,'friendly stonebraker','2020-06-09 18:58:00'),(5,'happy shannon','2020-02-29 21:39:08'),(6,'competent torvalds','2020-05-24 04:18:45'),(7,'fervent kapitsa','2020-05-21 16:58:12'),(8,'quirky jennings','2020-03-12 12:52:58'),(9,'adoring swartz','2020-04-19 02:20:32'),(14,'intelligent keller','2020-01-08 09:47:42'),(15,'vibrant zhukovsky','2020-04-15 15:15:55'),(18,'keen chatterjee','2020-02-09 06:39:31'),(20,'elastic gauss','2020-03-01 13:34:06'),(21,'affectionate margulis','2020-06-20 10:20:29'),(27,'busy keldysh','2020-05-21 09:10:45'),(31,'flamboyant banach','2020-03-04 21:28:44'),(39,'keen banach','2020-06-09 03:07:57'),(41,'nervous gagarin','2020-06-12 23:43:04'),(47,'wonderful chebyshev','2020-04-15 14:51:17'),(50,'reverent brahmagupta','2020-06-25 21:50:52'),(52,'suspicious elbakyan','2020-05-28 04:55:34'),(55,'epic lichterman','2020-05-16 19:24:09'),(57,'determined taussig','2020-06-18 22:51:37')")
	tk.MustExec("DROP TABLE IF EXISTS `t1`")
	tk.MustExec("CREATE TABLE `t1` ( `c_int` int(11) DEFAULT NULL, `c_str` varchar(40) NOT NULL, `c_datetime` datetime DEFAULT NULL, PRIMARY KEY (`c_str`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("INSERT INTO `t1` VALUES (19,'nervous johnson','2020-05-04 13:15:19'),(22,'pedantic tu','2020-02-19 09:32:44'),(24,'wizardly robinson','2020-02-03 18:39:36'),(33,'eager stonebraker','2020-05-03 08:20:54'),(34,'zen taussig','2020-06-29 01:18:48'),(36,'epic ganguly','2020-04-23 17:25:13'),(38,'objective euclid','2020-05-21 01:04:27'),(40,'infallible hodgkin','2020-05-07 03:52:52'),(43,'wizardly hellman','2020-04-11 20:20:05'),(46,'inspiring hoover','2020-06-28 14:47:34'),(48,'amazing cerf','2020-05-15 08:04:32'),(49,'objective hermann','2020-04-25 18:01:06'),(51,'upbeat spence','2020-01-27 21:59:54'),(53,'hardcore nightingale','2020-01-20 18:57:37'),(54,'silly hellman','2020-06-24 00:22:47'),(56,'elastic driscoll','2020-02-27 22:46:57'),(58,'nifty buck','2020-03-12 03:56:16')")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (59, 'suspicious feistel', '2020-01-29 19:52:14')")
	tk.MustExec("insert into t1 values (60, 'practical thompson', '2020-03-25 04:33:10')")
	tk.MustQuery("select c_int, c_str from t where (select count(*) from t1 where t1.c_int in (t.c_int, t.c_int + 2, t.c_int + 10)) > 2").Check(testkit.Rows())
	tk.MustExec("rollback")
}

func TestIssue28073(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c_int int, c_str varchar(40), primary key (c_int, c_str) , key(c_int)) partition by hash (c_int) partitions 4")
	tk.MustExec("create table t2 like t1")
	tk.MustExec("insert into t1 values (1, 'flamboyant mcclintock')")
	tk.MustExec("insert into t2 select * from t1")

	tk.MustExec("begin")
	tk.MustExec("insert into t2 (c_int, c_str) values (2, 'romantic grothendieck')")
	tk.MustQuery("select * from t2 left join t1 on t1.c_int = t2.c_int for update").Sort().Check(
		testkit.Rows(
			"1 flamboyant mcclintock 1 flamboyant mcclintock",
			"2 romantic grothendieck <nil> <nil>",
		))
	tk.MustExec("commit")

	// Check no key is written to table ID 0
	txn, err := store.Begin()
	require.NoError(t, err)
	start := tablecodec.EncodeTablePrefix(0)
	end := tablecodec.EncodeTablePrefix(1)
	iter, err := txn.Iter(start, end)
	require.NoError(t, err)

	exist := false
	for iter.Valid() {
		require.Nil(t, iter.Next())
		exist = true
		break
	}
	require.False(t, exist)

	// Another case, left join on partition table should not generate locks on physical ID = 0
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int, c_str));")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key (c_int)) partition by hash (c_int) partitions 4;")
	tk.MustExec("insert into t1 (`c_int`, `c_str`) values (1, 'upbeat solomon'), (5, 'sharp rubin');")
	tk.MustExec("insert into t2 (`c_int`, `c_str`) values (1, 'clever haibt'), (4, 'kind margulis');")
	tk.MustExec("begin pessimistic;")
	tk.MustQuery("select * from t1 left join t2 on t1.c_int = t2.c_int for update;").Check(testkit.Rows(
		"1 upbeat solomon 1 clever haibt",
		"5 sharp rubin <nil> <nil>",
	))
	key, err := hex.DecodeString("7480000000000000005F728000000000000000")
	require.NoError(t, err)
	h := helper.NewHelper(store.(helper.Storage))
	resp, err := h.GetMvccByEncodedKey(key)
	require.NoError(t, err)
	require.Nil(t, resp.Info.Lock)
	require.Len(t, resp.Info.Writes, 0)
	require.Len(t, resp.Info.Values, 0)

	tk.MustExec("rollback;")
}

func TestIssue32422(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (id int, c int, index(id));")
	tk.MustExec("insert into t values (3,3), (4,4), (5,5);")
	tk.MustExec("alter table t cache;")

	var cacheUsed bool
	for i := 0; i < 20; i++ {
		tk.MustQuery("select id+1, c from t where c = 4;").Check(testkit.Rows("5 4"))
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			cacheUsed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, cacheUsed)

	tk.MustQuery("select id+1, c from t where c = 4;").Check(testkit.Rows("5 4"))

	// Some extra tests.
	// Since cached table use UnionScanExec utilities, check what happens when they work together.
	// In these cases, the cache data serve as the snapshot, tikv is skipped, and txn membuffer works the same way.
	tk.MustExec("begin")
	tk.MustQuery("select id+1, c from t where c = 4;").Check(testkit.Rows("5 4"))
	tk.MustExec("insert into t values (6, 6)")
	// Check for the new added data.
	tk.HasPlan("select id+1, c from t where c = 6;", "UnionScan")
	tk.MustQuery("select id+1, c from t where c = 6;").Check(testkit.Rows("7 6"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)
	// Check for the old data.
	tk.MustQuery("select id+1, c from t where c = 4;").Check(testkit.Rows("5 4"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)

	// Point get
	tk.HasPlan("select id+1, c from t where id = 6", "PointGet")
	tk.MustQuery("select id+1, c from t where id = 6").Check(testkit.Rows("7 6"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)
	tk.MustQuery("select id+1, c from t where id = 4").Check(testkit.Rows("5 4"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)

	// Index Lookup
	tk.HasPlan("select id+1, c from t where id = 6", "IndexLookUp")
	tk.MustQuery("select id+1, c from t use index(id) where id = 6").Check(testkit.Rows("7 6"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)
	tk.MustQuery("select id+1, c from t use index(id) where id = 4").Check(testkit.Rows("5 4"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)

	// Index Reader
	tk.HasPlan("select id from t where id = 6", "IndexReader")
	tk.MustQuery("select id from t use index(id) where id = 6").Check(testkit.Rows("6"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)
	tk.MustQuery("select id from t use index(id) where id = 4").Check(testkit.Rows("4"))
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache)

	tk.MustExec("rollback")
}

func TestIssue36903(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_vwvgdc")

	tk.MustExec("CREATE TABLE t_vwvgdc (wkey int, pkey int NOT NULL, c_rdsfbc double DEFAULT NULL, PRIMARY KEY (`pkey`));")
	tk.MustExec("insert into t_vwvgdc values (2, 15000, 61.75);")
	tk.MustExec("BEGIN OPTIMISTIC;")
	tk.MustExec("insert into t_vwvgdc (wkey, pkey, c_rdsfbc) values (155, 228000, 99.50);")
	tk.MustQuery("select pkey from t_vwvgdc where 0 <> 0 union select pkey from t_vwvgdc;").Sort().Check(testkit.Rows("15000", "228000"))
}

func BenchmarkUnionScanRead(b *testing.B) {
	store := testkit.CreateMockStore(b)

	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t_us (
c1 varchar(10),
c2 varchar(30),
c3 varchar(1),
c4 varchar(12),
c5 varchar(10),
c6 datetime);`)
	tk.MustExec(`begin;`)
	for i := 0; i < 8000; i++ {
		tk.MustExec("insert into t_us values ('54321', '1234', '1', '000000', '7518', '2014-05-08')")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustQuery("select * from t_us where c1 = '12345'").Check(testkit.Rows())
	}
	b.StopTimer()
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		executor.BenchmarkReadLastLinesOfHugeLine,
		BenchmarkUnionScanRead,
	)
}

func TestUnionScanIssue24195(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_constraint_check_in_place = false")
	tk1.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set @@tidb_constraint_check_in_place_pessimistic=on")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("set @@tidb_constraint_check_in_place = false")
	tk2.MustExec("set tidb_txn_mode = 'pessimistic'")
	tk2.MustExec("set @@tidb_constraint_check_in_place_pessimistic=on")

	// case-1: query return duplicate primary key which is non-clustered.
	tk1.MustExec("set @@tidb_enable_clustered_index = 0;")
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec("create table t (c1 varchar(10), c2 int, primary key (c1) nonclustered);")
	tk1.MustExec("insert into t values ('a', 1);")
	tk1.MustExec("begin;")
	tk1.MustExec("update t set c1='b' where c1='a';")
	ch := make(chan struct{}, 0)
	go func() {
		defer func() { ch <- struct{}{} }()
		tk2.MustExec("begin")
		tk2.MustExec("insert into t values ('a', 2);")
		tk2.MustQuery("select * from t use index(primary) order by c1, c2;").Check(testkit.Rows("a 2"))
		tk2.MustQuery("select * from t use index(primary) order by c1, c2 for update;").Check(testkit.Rows("a 2", "b 1"))
		tk2.MustExec("commit")
	}()
	time.Sleep(time.Second * 2)
	tk1.MustExec("commit")
	_ = <-ch

	// case-2: query return duplicate unique key
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec("create table t (c1 varchar(10), c2 int, unique key idx(c1));")
	tk1.MustExec("insert into t values ('a', 1);")
	tk1.MustExec("begin;")
	tk1.MustExec("update t set c1='b' where c1='a';")
	go func() {
		defer func() { ch <- struct{}{} }()
		tk2.MustExec("begin")
		tk2.MustExec("insert into t values ('a', 2);")
		tk2.MustQuery("select * from t use index(idx);").Check(testkit.Rows("a 2"))
		tk2.MustQuery("select c2 from t;").Check(testkit.Rows("2"))
		tk2.MustQuery("select * from t use index(idx) order by c1, c2 for update;").Check(testkit.Rows("a 2", "b 1"))
		tk2.MustExec("commit")
	}()
	time.Sleep(time.Second * 2)
	tk1.MustExec("commit")
	_ = <-ch

	// case-3: when primary key is clustered index
	tk1.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec("create table t (c1 varchar(10), c2 int, c3 char(20), primary key (c1, c2));")
	tk1.MustExec("insert into t values ('tag', 10, 't'), ('cat', 20, 'c');")
	tk1.MustExec("begin;")
	tk1.MustExec("update t set c1=reverse(c1) where c1='tag';")
	ch = make(chan struct{}, 0)
	go func() {
		tk2.MustExec("begin")
		tk2.MustExec("insert into t values('dress',40,'d'),('tag', 10, 't');")
		tk2.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "dress 40 d", "tag 10 t"))
		tk2.MustQuery("select * from t use index(primary) order by c1,c2 for update;").Check(testkit.Rows("cat 20 c", "dress 40 d", "gat 10 t", "tag 10 t"))
		tk2.MustExec("commit")
		tk2.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "dress 40 d", "gat 10 t", "tag 10 t"))
		ch <- struct{}{}
	}()
	time.Sleep(time.Second * 2)
	tk1.MustExec("commit")
	_ = <-ch

	// case-4: when primary key is non-clustered.
	tk1.MustExec("set @@tidb_enable_clustered_index = 0;")
	tk1.MustExec("drop table if exists t;")
	tk1.MustExec("create table t (c1 varchar(10), c2 int, c3 char(20), primary key (c1, c2));")
	tk1.MustExec("insert into t values ('tag', 10, 't'), ('cat', 20, 'c');")
	tk1.MustExec("begin;")
	tk1.MustExec("update t set c1=reverse(c1) where c1='tag';")
	tk1.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "gat 10 t"))
	ch = make(chan struct{}, 0)
	go func() {
		tk2.MustExec("begin")
		tk2.MustExec("insert into t values('dress',40,'d'),('tag', 10, 't');")
		tk2.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "dress 40 d", "tag 10 t"))
		tk2.MustQuery("select * from t use index(primary) order by c1,c2 for update;").Check(testkit.Rows("cat 20 c", "dress 40 d", "gat 10 t", "tag 10 t"))
		tk2.MustExec("commit")
		tk2.MustQuery("select * from t use index(primary) order by c1,c2;").Check(testkit.Rows("cat 20 c", "dress 40 d", "gat 10 t", "tag 10 t"))
		ch <- struct{}{}
	}()
	time.Sleep(time.Second * 2)
	tk1.MustExec("commit")
	_ = <-ch
}

func TestUnionScanDuplicateRecord(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_constraint_check_in_place = false")
	// case-1: duplicate primary key in insert, and primary key is handle.
	tk.MustExec("create table t (pk int key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (1,2)") // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustGetDBError("commit", kv.ErrKeyExists)

	// case-2: duplicate unique key in insert, and primary key is handle.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int, unique index (val))")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (2,1)") // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1"))
	tk.MustGetDBError("commit", kv.ErrKeyExists)

	// case-3: duplicate primary key in insert, and primary key is not handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 0;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk varchar(10) key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustQuery("select _tidb_rowid, pk, val from t").Check(testkit.Rows("1 1 1"))
	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (1,2)") // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustGetDBError("commit", kv.ErrKeyExists)

	// case-4: in pessimistic transaction, duplicate primary key in insert, and primary key is handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1,2)") // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)

	// case-5: in pessimistic transaction, duplicate unique key in insert, and primary key is handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int, unique index (val))")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (2,1)") // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1"))
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)

	// case-6: duplicate primary key in insert, and primary key is not handle.
	tk.MustExec("set @@tidb_enable_clustered_index = 0;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk varchar(10) key, val int)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustQuery("select _tidb_rowid, pk, val from t").Check(testkit.Rows("1 1 1"))
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (1,2)") // MySQL returns error here, so following query result in MySQL is [1 1].
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)

	// case-7: duplicate unique key in insert, and query condition filtered the conflict row in tikv.
	tk.MustExec("set @@tidb_enable_clustered_index = 1;")
	tk.MustExec("set @@tidb_constraint_check_in_place_pessimistic=off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int key, val int, unique index(val))")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (2,1)") // MySQL returns error here, so following query result in MySQL is empty.
	tk.MustQuery("select * from t where pk > 1").Check(testkit.Rows("2 1"))
	tk.MustGetDBError("commit", kv.ErrAssertionFailed)
}
