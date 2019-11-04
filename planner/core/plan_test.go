package core_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPlanNormalize{})

type testPlanNormalize struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testPlanNormalize) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *testPlanNormalize) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testPlanNormalize) TestNormalizedPlan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t2 (a int key,b int,c int, index (b));")
	normalizedtCases := []struct {
		sql        string
		normalized string
	}{
		{
			sql:        "select * from t1;",
			normalized: "0\t32_5\t0\tdata:TableScan_4\n" + "1\t10_4\t1\ttable:t1, range:[?,?], keep order:false\n",
		},
		{
			sql:        "select * from t1 where a<1",
			normalized: "0\t32_6\t0\tdata:TableScan_5\n" + "1\t10_5\t1\ttable:t1, range:[?,?], keep order:false\n",
		},
		{
			sql:        "select * from t1 where a>1",
			normalized: "0\t32_6\t0\tdata:TableScan_5\n" + "1\t10_5\t1\ttable:t1, range:[?,?], keep order:false\n",
		},
		{
			sql:        "select * from t1 where a=1",
			normalized: "0\t37_1\t0\ttable:t1, handle:?\n",
		},
		{
			sql: "select * from t1 where b=1",
			normalized: "0\t31_10\t0\t\n" +
				"1\t13_8\t1\ttable:t1, index:b, range:[?,?], keep order:false\n" +
				"1\t10_9\t1\ttable:t1, keep order:false\n",
		},
	}

	for _, testCase := range normalizedtCases {
		testNormalized(tk, c, testCase.sql, testCase.normalized)
	}

	normalizedDigestCases := []struct {
		sql1   string
		sql2   string
		isSame bool
	}{
		{
			sql1:   "select * from t1;",
			sql2:   "select * from t2;",
			isSame: false,
		},
		{ // test for tableReader and tableScan.
			sql1:   "select * from t1 where a<1",
			sql2:   "select * from t1 where a<2",
			isSame: true,
		},
		{
			sql1:   "select * from t1 where a<1",
			sql2:   "select * from t1 where a=2",
			isSame: false,
		},
		{ // test for point get.
			sql1:   "select * from t1 where a=3",
			sql2:   "select * from t1 where a=2",
			isSame: true,
		},
		{ // test for indexLookUp.
			sql1:   "select * from t1 use index(b) where b=3",
			sql2:   "select * from t1 use index(b) where b=1",
			isSame: true,
		},
		{ // test for indexLookUp.
			sql1:   "select a+1,b+2 from t1 use index(b) where b=3",
			sql2:   "select a+2,b+3 from t1 use index(b) where b>2",
			isSame: true,
		},
		{ // test for merge join.
			sql1:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>2;",
			isSame: true,
		},
		{ // test for indexLookUpJoin.
			sql1:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: true,
		},
		{ // test for hashJoin.
			sql1:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: true,
		},
		{ // test for diff join.
			sql1:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: false,
		},
		{ // test for diff join.
			sql1:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: false,
		},
	}

	for _, testCase := range normalizedDigestCases {
		testNormalizeDigest(tk, c, testCase.sql1, testCase.sql2, testCase.isSame)
	}
}

func testNormalized(tk *testkit.TestKit, c *C, sql, normalize string) {
	tk.Se.GetSessionVars().PlanID = 0
	tk.MustQuery(sql)
	info := tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	physicalPlan, ok := info.Plan.(core.PhysicalPlan)
	c.Assert(ok, IsTrue)
	normalized1, _ := core.NormalizePlan(physicalPlan)
	c.Assert(normalized1, Equals, normalize, Commentf("sql: %v\n", sql))
}

func testNormalizeDigest(tk *testkit.TestKit, c *C, sql1, sql2 string, isSame bool) {
	tk.Se.GetSessionVars().PlanID = 0
	tk.MustQuery(sql1)
	info := tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	physicalPlan, ok := info.Plan.(core.PhysicalPlan)
	c.Assert(ok, IsTrue)
	normalized1, digest1 := core.NormalizePlan(physicalPlan)

	tk.Se.GetSessionVars().PlanID = 0
	tk.MustQuery(sql2)
	info = tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	physicalPlan, ok = info.Plan.(core.PhysicalPlan)
	c.Assert(ok, IsTrue)
	normalized2, digest2 := core.NormalizePlan(physicalPlan)
	if isSame {
		c.Assert(normalized1, Equals, normalized2, Commentf("sql1: %v, sql2: %v\n", sql1, sql2))
		c.Assert(digest1, Equals, digest2, Commentf("sql1: %v, sql2: %v\n", sql1, sql2))
	} else {
		c.Assert(normalized1 != normalized2, IsTrue, Commentf("sql1: %v, sql2: %v\n", sql1, sql2))
		c.Assert(digest1 != digest2, IsTrue, Commentf("sql1: %v, sql2: %v\n", sql1, sql2))

	}
}
