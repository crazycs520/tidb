// Copyright 2021 PingCAP, Inc.
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

package stmtstats

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// String is only used for debugging.
func (d SQLPlanDigest) String() string {
	bs := bytes.NewBufferString("")
	if len(d.SQLDigest) >= 5 {
		bs.Write([]byte(d.SQLDigest)[:5])
	}
	if len(d.PlanDigest) >= 5 {
		bs.WriteRune('-')
		bs.Write([]byte(d.PlanDigest)[:5])
	}
	return bs.String()
}

// String is only used for debugging.
func (m StatementStatsMap) String() string {
	if len(m) == 0 {
		return "StatementStatsMap {}"
	}
	bs := bytes.NewBufferString("")
	bs.WriteString("StatementStatsMap {\n")
	for k, v := range m {
		bs.WriteString(fmt.Sprintf("    %s => %s\n", k, v))
	}
	bs.WriteString("}")
	return bs.String()
}

// String is only used for debugging.
func (i *StatementStatsItem) String() string {
	if i == nil {
		return "<nil>"
	}
	b, _ := json.Marshal(i)
	return string(b)
}

func TestKvStatementStatsItem_Merge(t *testing.T) {
	item1 := KvStatementStatsItem{
		KvExecCount: map[string]uint64{
			"127.0.0.1:10001": 1,
			"127.0.0.1:10002": 2,
		},
	}
	item2 := KvStatementStatsItem{
		KvExecCount: map[string]uint64{
			"127.0.0.1:10002": 2,
			"127.0.0.1:10003": 3,
		},
	}
	assert.Len(t, item1.KvExecCount, 2)
	assert.Len(t, item2.KvExecCount, 2)
	item1.Merge(item2)
	assert.Len(t, item1.KvExecCount, 3)
	assert.Len(t, item2.KvExecCount, 2)
	assert.Equal(t, uint64(1), item1.KvExecCount["127.0.0.1:10001"])
	assert.Equal(t, uint64(3), item1.KvExecCount["127.0.0.1:10003"])
	assert.Equal(t, uint64(3), item1.KvExecCount["127.0.0.1:10003"])
}

func TestStatementsStatsItem_Merge(t *testing.T) {
	item1 := &StatementStatsItem{
		ExecCount:           1,
		SumExecNanoDuration: 100,
		KvStatsItem:         NewKvStatementStatsItem(),
	}
	item2 := &StatementStatsItem{
		ExecCount:           2,
		SumExecNanoDuration: 50,
		KvStatsItem:         NewKvStatementStatsItem(),
	}
	item1.Merge(item2)
	assert.Equal(t, uint64(3), item1.ExecCount)
	assert.Equal(t, uint64(150), item1.SumExecNanoDuration)
}

func TestStatementStatsMap_Merge(t *testing.T) {
	m1 := StatementStatsMap{
		SQLPlanDigest{SQLDigest: "SQL-1"}: &StatementStatsItem{
			ExecCount:           1,
			SumExecNanoDuration: 100,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
		SQLPlanDigest{SQLDigest: "SQL-2"}: &StatementStatsItem{
			ExecCount:           1,
			SumExecNanoDuration: 200,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
	}
	m2 := StatementStatsMap{
		SQLPlanDigest{SQLDigest: "SQL-2"}: &StatementStatsItem{
			ExecCount:           1,
			SumExecNanoDuration: 100,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
		SQLPlanDigest{SQLDigest: "SQL-3"}: &StatementStatsItem{
			ExecCount:           1,
			SumExecNanoDuration: 50,
			KvStatsItem: KvStatementStatsItem{
				KvExecCount: map[string]uint64{
					"KV-1": 1,
					"KV-2": 2,
				},
			},
		},
	}
	assert.Len(t, m1, 2)
	assert.Len(t, m2, 2)
	m1.Merge(m2)
	assert.Len(t, m1, 3)
	assert.Len(t, m2, 2)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(100), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].SumExecNanoDuration)
	assert.Equal(t, uint64(300), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].SumExecNanoDuration)
	assert.Equal(t, uint64(50), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].SumExecNanoDuration)
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-1"}].KvStatsItem.KvExecCount["KV-2"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(4), m1[SQLPlanDigest{SQLDigest: "SQL-2"}].KvStatsItem.KvExecCount["KV-2"])
	assert.Equal(t, uint64(1), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].KvStatsItem.KvExecCount["KV-1"])
	assert.Equal(t, uint64(2), m1[SQLPlanDigest{SQLDigest: "SQL-3"}].KvStatsItem.KvExecCount["KV-2"])
	m1.Merge(nil)
	assert.Len(t, m1, 3)
}

func TestCreateStatementStats(t *testing.T) {
	stats := CreateStatementStats()
	assert.NotNil(t, stats)
	_, ok := globalAggregator.statsSet.Load(stats)
	assert.True(t, ok)
	assert.False(t, stats.Finished())
	stats.SetFinished()
	assert.True(t, stats.Finished())
}

func TestExecCounter_AddExecCount_Take(t *testing.T) {
	stats := CreateStatementStats()
	m := stats.Take()
	assert.Len(t, m, 0)
	nowFunc = mockNow
	defer func() {
		nowFunc = time.Now
	}()
	for n := 0; n < 2; n++ {
		stats.OnDispatchBegin()
		stats.OnHandleQueryBegin()
		stats.OnHandleStmtBegin()
		stats.OnStmtReadyToExecute([]byte("SQL-1"), nil)
		stats.OnHandleStmtFinish()
		stats.OnHandleQueryFinish()
		stats.OnDispatchFinish()
	}
	for n := 0; n < 3; n++ {
		stats.OnDispatchBegin()
		stats.OnHandleStmtExecuteBegin()
		stats.OnStmtReadyToExecute([]byte("SQL-2"), nil)
		stats.OnHandleStmtExecuteFinish()
		stats.OnDispatchFinish()
	}

	stats.OnDispatchBegin()
	stats.OnHandleStmtExecuteBegin()
	stats.OnStmtReadyToExecute([]byte("SQL-3"), nil)
	// mock for statement SQL-3 doesn't execute finish.

	m = stats.Take()
	assert.Len(t, m, 3)
	assert.Equal(t, uint64(2), m[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(400), m[SQLPlanDigest{SQLDigest: "SQL-1"}].SumExecNanoDuration)
	assert.Equal(t, uint64(3), m[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(300), m[SQLPlanDigest{SQLDigest: "SQL-2"}].SumExecNanoDuration)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(0), m[SQLPlanDigest{SQLDigest: "SQL-3"}].SumExecNanoDuration)

	m = stats.Take()
	assert.Len(t, m, 0)

	// mock for statement SQL-3 execute finish.
	stats.OnHandleStmtExecuteFinish()
	stats.OnDispatchFinish()
	// mock for fetch SQL-3 data.
	stats.OnDispatchBegin()
	stats.OnHandleStmtFetchBegin()

	m = stats.Take()
	assert.Len(t, m, 1)
	assert.Equal(t, uint64(0), m[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(100), m[SQLPlanDigest{SQLDigest: "SQL-3"}].SumExecNanoDuration)

	// mock for fetch SQL-3 data finish.
	stats.OnHandleStmtFetchFinish([]byte("SQL-3"), nil)
	stats.OnDispatchFinish()
	stats.OnDispatchBegin()
	stats.OnStmtReadyToExecute([]byte("ignore-cmd"), nil)
	stats.OnDispatchFinish()

	m = stats.Take()
	assert.Len(t, m, 1)
	assert.Equal(t, uint64(0), m[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(100), m[SQLPlanDigest{SQLDigest: "SQL-3"}].SumExecNanoDuration)
}

func TestForHandleDiffCMD(t *testing.T) {
	stats := CreateStatementStats()
	nowFunc = mockNow
	defer func() {
		nowFunc = time.Now
	}()

	// test for handle 1 stmt.
	stats.OnDispatchBegin()
	stats.OnHandleQueryBegin()
	stats.OnHandleStmtBegin()
	stats.OnStmtReadyToExecute([]byte("SQL-1"), nil)
	stats.OnHandleStmtFinish()
	stats.OnHandleQueryFinish()
	stats.OnDispatchFinish()

	// test for handle 3 stmts in handle query
	stats.OnDispatchBegin()
	stats.OnHandleQueryBegin()

	stats.OnHandleStmtBegin()
	stats.OnStmtReadyToExecute([]byte("SQL-2"), nil)
	stats.OnHandleStmtFinish()

	stats.OnHandleStmtBegin()
	stats.OnStmtReadyToExecute([]byte("SQL-3"), nil)
	stats.OnHandleStmtFinish()

	stats.OnHandleStmtBegin()
	stats.OnStmtReadyToExecute(nil, nil)
	stats.OnHandleStmtFinish()

	stats.OnHandleQueryFinish()
	stats.OnDispatchFinish()

	// test for execute prepare stmt.
	stats.OnDispatchBegin()
	stats.OnHandleStmtExecuteBegin()
	stats.OnStmtReadyToExecute(nil, nil)
	stats.OnStmtReadyToExecute([]byte("SQL-4"), nil)
	stats.OnHandleStmtExecuteFinish()
	stats.OnDispatchFinish()

	// test for fetch executed stmt data.
	stats.OnDispatchBegin()
	stats.OnHandleStmtFetchBegin()
	stats.OnHandleStmtFetchFinish([]byte("SQL-4"), nil)
	stats.OnDispatchFinish()

	// test use db command.
	stats.OnDispatchBegin()
	stats.OnUseDBBegin()
	stats.OnStmtReadyToExecute([]byte("use test"), nil)
	stats.OnUseDBFinish()
	stats.OnDispatchFinish()

	m := stats.Take()
	assert.Len(t, m, 5)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "SQL-1"}].ExecCount)
	assert.Equal(t, uint64(200), m[SQLPlanDigest{SQLDigest: "SQL-1"}].SumExecNanoDuration)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "SQL-2"}].ExecCount)
	assert.Equal(t, uint64(200), m[SQLPlanDigest{SQLDigest: "SQL-2"}].SumExecNanoDuration)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "SQL-3"}].ExecCount)
	assert.Equal(t, uint64(100), m[SQLPlanDigest{SQLDigest: "SQL-3"}].SumExecNanoDuration)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "SQL-4"}].ExecCount)
	assert.Equal(t, uint64(200), m[SQLPlanDigest{SQLDigest: "SQL-4"}].SumExecNanoDuration)
	assert.Equal(t, uint64(1), m[SQLPlanDigest{SQLDigest: "use test"}].ExecCount)
	assert.Equal(t, uint64(0), m[SQLPlanDigest{SQLDigest: "use test"}].SumExecNanoDuration)
}
