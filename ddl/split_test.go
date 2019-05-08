package ddl

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
)

var _ = Suite(&testSplitIndex{})

type testSplitIndex struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

func (s *testSplitIndex) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_table")
	s.d = testNewDDL(context.Background(), nil, s.store, nil, nil, testLease)

	s.dbInfo = testSchemaInfo(c, s.d, "test")
	testCreateSchema(c, testNewContext(s.d), s.d, s.dbInfo)
}

func (s *testSplitIndex) TearDownSuite(c *C) {
	testDropSchema(c, testNewContext(s.d), s.d, s.dbInfo)
	s.d.Stop()
	s.store.Close()

}

func (s *testSplitIndex) TestLongestCommonPrefixLen(c *C) {
	cases := []struct {
		s1 string
		s2 string
		l  int
	}{
		{"", "", 0},
		{"", "a", 0},
		{"a", "", 0},
		{"a", "a", 1},
		{"ab", "a", 1},
		{"a", "ab", 1},
		{"b", "ab", 0},
		{"ba", "ab", 0},
	}

	for _, ca := range cases {
		re := longestCommonPrefixLen([]byte(ca.s1), []byte(ca.s2))
		c.Assert(re, Equals, ca.l)
	}
}

func (s *testSplitIndex) TestGetDiffBytesValue(c *C) {
	cases := []struct {
		min []byte
		max []byte
		l   int
		v   uint64
	}{
		{[]byte{}, []byte{}, 0, math.MaxUint64},
		{[]byte{0}, []byte{128}, 0, binary.BigEndian.Uint64([]byte{128, 255, 255, 255, 255, 255, 255, 255})},
		{[]byte{'a'}, []byte{'z'}, 0, binary.BigEndian.Uint64([]byte{'z' - 'a', 255, 255, 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("xyz"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z' - 'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("axyz"), 1, binary.BigEndian.Uint64([]byte{'x' - 'b', 'y' - 'c', 'z', 255, 255, 255, 255, 255})},
		{[]byte("abc0123456"), []byte("xyz01234"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z' - 'c', 0, 0, 0, 0, 0})},
	}

	for _, ca := range cases {
		l0 := longestCommonPrefixLen(ca.min, ca.max)
		c.Assert(l0, Equals, ca.l)
		v0 := getDiffBytesValue(l0, ca.min, ca.max)
		c.Assert(v0, Equals, ca.v)
	}
}

func (s *testSplitIndex) TestSplitIndex(c *C) {
	tbInfo := testTableInfo(c, s.d, "t1", 3)
	idxCols := []*model.IndexColumn{{Name: tbInfo.Columns[0].Name, Offset: 0, Length: types.UnspecifiedLength}}
	idxInfo := &model.IndexInfo{
		ID:      1,
		Name:    model.NewCIStr("idx1"),
		Table:   model.NewCIStr("t1"),
		Columns: idxCols,
		State:   model.StatePublic,
	}

	// Test for int index.
	// range is 0 ~ 100, and split into 10 region.
	// So 10 regions range is like below:
	// region0: -inf ~ 0
	// region1: 0 ~ 10
	// region2: 10 ~ 20
	// region3: 20 ~ 30
	// region4: 30 ~ 40
	// region5: 40 ~ 50
	// region6: 50 ~ 60
	// region7: 60 ~ 70
	// region8: 70 ~ 80
	// region9: 80 ~ 80
	// region10: 90 ~ +inf
	splitOpt := &ast.SplitOption{
		Min: []ast.ExprNode{&driver.ValueExpr{Datum: types.NewDatum(0)}},
		Max: []ast.ExprNode{&driver.ValueExpr{Datum: types.NewDatum(100)}},
		Num: 10,
	}
	ctx := testNewContext(s.d)
	splitOption, err := validateIndexSplitOptions(ctx, tbInfo, idxInfo, splitOpt)
	c.Assert(err, IsNil)
	valueList := getValuesList(splitOption.Min, splitOption.Max, splitOption.Num)
	c.Assert(len(valueList), Equals, splitOption.Num+1)

	cases := []struct {
		value        int
		lessEqualIdx int
	}{
		{-1, -1},
		{0, 0},
		{1, 0},
		{10, 1},
		{11, 1},
		{20, 2},
		{21, 2},
		{21, 2},
		{31, 3},
		{41, 4},
		{51, 5},
		{61, 6},
		{71, 7},
		{81, 8},
		{91, 9},
		{100, 10},
		{1000, 10},
	}

	index := tables.NewIndex(tbInfo.ID, tbInfo, idxInfo)
	for _, ca := range cases {
		// test for minInt64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, math.MinInt64, nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, math.MaxInt64, nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}

	// Test for varchar index.
	// range is a ~ z, and split into 26 region.
	// So 26 regions range is like below:
	// region0: -inf ~ a
	// region1: a ~ b
	// .
	// .
	// .
	// region26: z ~ +inf
	splitOpt = &ast.SplitOption{
		Min: []ast.ExprNode{&driver.ValueExpr{Datum: types.NewDatum("a")}},
		Max: []ast.ExprNode{&driver.ValueExpr{Datum: types.NewDatum("z")}},
		Num: 26,
	}
	// change index column type to varchar
	tbInfo.Columns[0].FieldType = *types.NewFieldType(mysql.TypeVarchar)

	splitOption, err = validateIndexSplitOptions(ctx, tbInfo, idxInfo, splitOpt)
	c.Assert(err, IsNil)
	valueList = getValuesList(splitOption.Min, splitOption.Max, splitOption.Num)
	c.Assert(len(valueList), Equals, splitOption.Num+1)

	cases2 := []struct {
		value        string
		lessEqualIdx int
	}{
		{"", -1},
		{"a", 0},
		{"abcde", 0},
		{"b", 1},
		{"bzzzz", 1},
		{"c", 2},
		{"czzzz", 2},
		{"z", 26},
		{"zabcd", 26},
	}

	for _, ca := range cases2 {
		// test for minInt64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, math.MinInt64, nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, math.MaxInt64, nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}

	// Test for timestamp index.
	// range is 2010-01-01 00:00:00 ~ 2020-01-01 00:00:00, and split into 10 region.
	// So 10 regions range is like below:
	// region0: -inf			    ~ 2010-01-01 00:00:00
	// region1: 2010-01-01 00:00:00 ~ 2011-01-01 00:00:00
	// .
	// .
	// .
	// region10: 2020-01-01 00:00:00 ~ +inf

	minTime := types.Time{
		Time: types.FromDate(2010, 1, 1, 0, 0, 0, 0),
		Type: mysql.TypeTimestamp,
	}
	maxTime := types.Time{
		Time: types.FromDate(2020, 1, 1, 0, 0, 0, 0),
		Type: mysql.TypeTimestamp,
	}
	splitOpt = &ast.SplitOption{
		Min: []ast.ExprNode{&driver.ValueExpr{Datum: types.NewDatum(minTime)}},
		Max: []ast.ExprNode{&driver.ValueExpr{Datum: types.NewDatum(maxTime)}},
		Num: 10,
	}

	// change index column type to timestamp
	tbInfo.Columns[0].FieldType = *types.NewFieldType(mysql.TypeTimestamp)

	splitOption, err = validateIndexSplitOptions(ctx, tbInfo, idxInfo, splitOpt)
	c.Assert(err, IsNil)
	valueList = getValuesList(splitOption.Min, splitOption.Max, splitOption.Num)
	c.Assert(len(valueList), Equals, splitOption.Num+1)

	cases3 := []struct {
		value        types.MysqlTime
		lessEqualIdx int
	}{
		{types.FromDate(2009, 11, 20, 12, 50, 59, 0), -1},
		{types.FromDate(2010, 1, 1, 0, 0, 0, 0), 0},
		{types.FromDate(2011, 12, 31, 23, 59, 59, 0), 1},
		{types.FromDate(2011, 2, 1, 0, 0, 0, 0), 1},
		{types.FromDate(2012, 3, 1, 0, 0, 0, 0), 2},
		{types.FromDate(2013, 4, 1, 0, 0, 0, 0), 3},
		{types.FromDate(2014, 5, 1, 0, 0, 0, 0), 4},
		{types.FromDate(2015, 6, 1, 0, 0, 0, 0), 5},
		{types.FromDate(2016, 8, 1, 0, 0, 0, 0), 6},
		{types.FromDate(2017, 9, 1, 0, 0, 0, 0), 7},
		{types.FromDate(2018, 10, 1, 0, 0, 0, 0), 8},
		{types.FromDate(2019, 11, 1, 0, 0, 0, 0), 9},
		{types.FromDate(2020, 12, 1, 0, 0, 0, 0), 10},
		{types.FromDate(2030, 12, 1, 0, 0, 0, 0), 10},
	}

	for _, ca := range cases3 {
		value := types.Time{
			Time: ca.value,
			Type: mysql.TypeTimestamp,
		}
		// test for minInt64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(value)}, math.MinInt64, nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(value)}, math.MaxInt64, nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
}

func searchLessEqualIdx(valueList [][]byte, value []byte) int {
	idx := -1
	for i, v := range valueList {
		if bytes.Compare(value, v) >= 0 {
			idx = i
			continue
		}
		break
	}
	return idx
}
