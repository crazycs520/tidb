package ddl

import (
	"encoding/binary"
	"math"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSplitIndex{})

type testSplitIndex struct {
}

func (s *testSplitIndex) SetUpSuite(c *C) {
}

func (s *testSplitIndex) TearDownSuite(c *C) {
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
		{[]byte("abc"), []byte("xyz"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z'-'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("axyz"), 1, binary.BigEndian.Uint64([]byte{'x' - 'b', 'y' - 'c', 'z', 255, 255, 255, 255, 255})},
		{[]byte("abc0123456"), []byte("xyz01234"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z'-'c', 0, 0, 0, 0, 0})},
	}

	for _, ca := range cases {
		l0 := longestCommonPrefixLen(ca.min, ca.max)
		c.Assert(l0, Equals, ca.l)
		v0 := getDiffBytesValue(l0, ca.min, ca.max)
		c.Assert(v0, Equals, ca.v)
	}
}

func (s *testSplitIndex) TestGetValuesList(c *C) {
	cases := []struct {
		min []byte
		max []byte
		l   in
		v   uint64
	}{
		{[]byte{}, []byte{}, 0, math.MaxUint64},
		{[]byte{0}, []byte{128}, 0, binary.BigEndian.Uint64([]byte{128, 255, 255, 255, 255, 255, 255, 255})},
		{[]byte{'a'}, []byte{'z'}, 0, binary.BigEndian.Uint64([]byte{'z' - 'a', 255, 255, 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("xyz"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z'-'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("axyz"), 1, binary.BigEndian.Uint64([]byte{'x' - 'b', 'y' - 'c', 'z', 255, 255, 255, 255, 255})},
		{[]byte("abc0123456"), []byte("xyz01234"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z'-'c', 0, 0, 0, 0, 0})},
	}

	for _, ca := range cases {
		l0 := longestCommonPrefixLen(ca.min, ca.max)
		c.Assert(l0, Equals, ca.l)
		v0 := getDiffBytesValue(l0, ca.min, ca.max)
		c.Assert(v0, Equals, ca.v)
	}
}
