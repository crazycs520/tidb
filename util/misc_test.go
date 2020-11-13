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
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"crypto/x509/pkix"
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/fastrand"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testMiscSuite{})

type testMiscSuite struct {
}

func (s *testMiscSuite) SetUpSuite(c *C) {
}

func (s *testMiscSuite) TearDownSuite(c *C) {
}

func (s *testMiscSuite) TestRunWithRetry(c *C) {
	defer testleak.AfterTest(c)()
	// Run succ.
	cnt := 0
	err := RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 2 {
			return true, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 2)

	// Run failed.
	cnt = 0
	err = RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 4 {
			return true, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, NotNil)
	c.Assert(cnt, Equals, 3)

	// Run failed.
	cnt = 0
	err = RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 2 {
			return false, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, NotNil)
	c.Assert(cnt, Equals, 1)
}

func (s *testMiscSuite) TestCompatibleParseGCTime(c *C) {
	values := []string{
		"20181218-19:53:37 +0800 CST",
		"20181218-19:53:37 +0800 MST",
		"20181218-19:53:37 +0800 FOO",
		"20181218-19:53:37 +0800 +08",
		"20181218-19:53:37 +0800",
		"20181218-19:53:37 +0800 ",
		"20181218-11:53:37 +0000",
	}

	invalidValues := []string{
		"",
		" ",
		"foo",
		"20181218-11:53:37",
		"20181218-19:53:37 +0800CST",
		"20181218-19:53:37 +0800 FOO BAR",
		"20181218-19:53:37 +0800FOOOOOOO BAR",
		"20181218-19:53:37 ",
	}

	expectedTime := time.Date(2018, 12, 18, 11, 53, 37, 0, time.UTC)
	expectedTimeFormatted := "20181218-19:53:37 +0800"

	beijing, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)

	for _, value := range values {
		t, err := CompatibleParseGCTime(value)
		c.Assert(err, IsNil)
		c.Assert(t.Equal(expectedTime), Equals, true)

		formatted := t.In(beijing).Format(GCTimeFormat)
		c.Assert(formatted, Equals, expectedTimeFormatted)
	}

	for _, value := range invalidValues {
		_, err := CompatibleParseGCTime(value)
		c.Assert(err, NotNil)
	}
}

func (s *testMiscSuite) TestX509NameParseMatch(c *C) {
	check := pkix.Name{
		Names: []pkix.AttributeTypeAndValue{
			MockPkixAttribute(Country, "SE"),
			MockPkixAttribute(Province, "Stockholm2"),
			MockPkixAttribute(Locality, "Stockholm"),
			MockPkixAttribute(Organization, "MySQL demo client certificate"),
			MockPkixAttribute(OrganizationalUnit, "testUnit"),
			MockPkixAttribute(CommonName, "client"),
			MockPkixAttribute(Email, "client@example.com"),
		},
	}
	c.Assert(X509NameOnline(check), Equals, "/C=SE/ST=Stockholm2/L=Stockholm/O=MySQL demo client certificate/OU=testUnit/CN=client/emailAddress=client@example.com")
	check = pkix.Name{}
	c.Assert(X509NameOnline(check), Equals, "")
}

func (s *testMiscSuite) TestBasicFunc(c *C) {
	// Test for GetStack.
	b := GetStack()
	c.Assert(len(b) < 4096, IsTrue)

	// Test for WithRecovery.
	var recover interface{}
	WithRecovery(func() {
		panic("test")
	}, func(r interface{}) {
		recover = r
	})
	c.Assert(recover, Equals, "test")

	// Test for SyntaxError.
	c.Assert(SyntaxError(nil), IsNil)
	c.Assert(terror.ErrorEqual(SyntaxError(errors.New("test")), parser.ErrParse), IsTrue)
	c.Assert(terror.ErrorEqual(SyntaxError(parser.ErrSyntax.GenWithStackByArgs()), parser.ErrSyntax), IsTrue)

	// Test for SyntaxWarn.
	c.Assert(SyntaxWarn(nil), IsNil)
	c.Assert(terror.ErrorEqual(SyntaxWarn(errors.New("test")), parser.ErrParse), IsTrue)

	// Test for ProcessInfo.
	pi := ProcessInfo{
		ID:      1,
		User:    "test",
		Host:    "www",
		DB:      "db",
		Command: mysql.ComSleep,
		Plan:    nil,
		Time:    time.Now(),
		State:   3,
		Info:    "test",
		StmtCtx: &stmtctx.StatementContext{
			MemTracker: memory.NewTracker(-1, -1),
		},
	}
	row := pi.ToRowForShow(false)
	row2 := pi.ToRowForShow(true)
	c.Assert(row, DeepEquals, row2)
	c.Assert(len(row), Equals, 8)
	c.Assert(row[0], Equals, pi.ID)
	c.Assert(row[1], Equals, pi.User)
	c.Assert(row[2], Equals, pi.Host)
	c.Assert(row[3], Equals, pi.DB)
	c.Assert(row[4], Equals, "Sleep")
	c.Assert(row[5], Equals, uint64(0))
	c.Assert(row[6], Equals, "in transaction; autocommit")
	c.Assert(row[7], Equals, "test")

	row3 := pi.ToRow(time.UTC)
	c.Assert(row3[:8], DeepEquals, row)
	c.Assert(row3[9], Equals, int64(0))

	// Test for RandomBuf.
	buf := fastrand.Buf(5)
	c.Assert(len(buf), Equals, 5)
	c.Assert(bytes.Contains(buf, []byte("$")), IsFalse)
	c.Assert(bytes.Contains(buf, []byte{0}), IsFalse)
}

func (*testMiscSuite) TestToPB(c *C) {
	column := &model.ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	column.Collate = "utf8mb4_general_ci"

	column2 := &model.ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	column2.Collate = "utf8mb4_bin"

	c.Assert(ColumnToProto(column).String(), Equals, "column_id:1 collation:45 columnLen:-1 decimal:-1 ")
	c.Assert(ColumnsToProto([]*model.ColumnInfo{column, column2}, false)[0].String(), Equals, "column_id:1 collation:45 columnLen:-1 decimal:-1 ")
}

type SampleDuration time.Duration

func (*testMiscSuite) TestToCs(c *C) {
	cases := []struct {
		t time.Duration
		s string
	}{
		{0, "0s"},
		{1 * time.Nanosecond, "1ns"},
		{9 * time.Nanosecond, "9ns"},
		{10 * time.Nanosecond, "10ns"},
		{999 * time.Nanosecond, "999ns"},
		{1 * time.Microsecond, "1µs"},
		{1*time.Microsecond + 123*time.Nanosecond, "1.12µs"},
		{1*time.Microsecond + 23*time.Nanosecond, "1.02µs"},
		{1*time.Microsecond + 3*time.Nanosecond, "1µs"},
		{10*time.Microsecond + 456*time.Nanosecond, "10.5µs"},
		{10*time.Microsecond + 956*time.Nanosecond, "11µs"},
		{999*time.Microsecond + 56*time.Nanosecond, "999.1µs"},
		{999*time.Microsecond + 988*time.Nanosecond, "1ms"},
	}
	for _, ca := range cases {
		result := formatDuration(ca.t)
		c.Assert(result, Equals, ca.s)
	}
}

const (
	TenSeconds     = 10 * time.Second
	TenMillisecond = 10 * time.Millisecond
	TenMicrosecond = 10 * time.Microsecond
	TenNanosecond  = 10 * time.Nanosecond
)

func formatDuration(d time.Duration) string {
	unit := getUnit(d)
	if unit == time.Nanosecond {
		return d.String()
	}
	integer := (d / unit) * unit
	decimal := float64(d%unit) / float64(unit)
	if d < 10*unit {
		decimal = math.Round(decimal*100) / 100
	} else {
		decimal = math.Round(decimal*10) / 10
	}
	d = integer + time.Duration(decimal*float64(unit))
	return d.String()
}

func getUnitAndDigital(d time.Duration) (time.Duration, int64) {
	if d >= time.Second {
		if d >= TenSeconds {
			return time.Second, 1
		}
		return time.Second, 2
	} else if d >= time.Millisecond {
		if d >= TenMillisecond {
			return time.Millisecond, 1
		}
		return time.Millisecond, 2
	} else if d >= time.Microsecond {
		if d >= TenMicrosecond {
			return time.Microsecond, 1
		}
		return time.Microsecond, 2
	}
	if d >= TenNanosecond {
		return time.Nanosecond, 1
	}
	return time.Nanosecond, 2
}

func getUnit(d time.Duration) time.Duration {
	if d >= time.Second {
		return time.Second
	} else if d >= time.Millisecond {
		return time.Millisecond
	} else if d >= time.Microsecond {
		return time.Microsecond
	}
	return time.Nanosecond
}
