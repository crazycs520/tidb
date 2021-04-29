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
// See the License for the specific language governing permissions and
// limitations under the License.

package stmtcost

import (
	"fmt"
	"github.com/pingcap/tidb/util/stmtsummary"
	"runtime/metrics"
	"sync"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
)

// stmtCostByDigestKey defines key for stmtSummaryByDigestMap.summaryMap.
type stmtCostByDigestKey struct {
	// Same statements may appear in different schema, but they refer to different tables.
	schemaName string
	digest     string
	// `hash` is the hash value of this object.
	hash []byte
}

// Hash implements SimpleLRUCache.Key.
// Only when current SQL is `commit` do we record `prevSQL`. Otherwise, `prevSQL` is empty.
// `prevSQL` is included in the key To distinguish different transactions.
func (key *stmtCostByDigestKey) Hash() []byte {
	if len(key.hash) == 0 {
		key.hash = make([]byte, 0, len(key.schemaName)+len(key.digest))
		key.hash = append(key.hash, hack.Slice(key.digest)...)
		key.hash = append(key.hash, hack.Slice(key.schemaName)...)
	}
	return key.hash
}

type stmtCostCollector struct {
	sync.Mutex
	current    *stmtCostByDigestMap
	history    []*stmtCostByDigestMap
	historyIdx int
	sm         util.SessionManager
}

type stmtCostByDigestMap struct {
	sync.Mutex
	costMap *kvcache.SimpleLRUCache
	// todo: add other
	other stmtCostStats
	begin int64 // unix second
	end   int64
}

type stmtCostByDigest struct {
	schemaName    string
	digest        string
	normalizedSQL string
	sampleSQL     string
	stmtCostStats
}

type stmtCostStats struct {
	execCount  int64
	sumCPUTime int64 // nanosecond

	sumQueryTime       time.Duration
	sumParseTime       time.Duration
	sumCompileTime     time.Duration
	sumOptimizeTime    time.Duration
	sumWaitTsTime      time.Duration
	sumCopProcessTime  time.Duration
	sumCopWaitTime     time.Duration
	sumBackoffTime     time.Duration
	sumPreWriteTime    time.Duration
	sumGetCommitTsTime time.Duration
	sumCommitTime      time.Duration
	sumResolveLockTime time.Duration
}

var StmtCostCollector = newStmtCostCollector()

func newStmtCostCollector() *stmtCostCollector {
	sc := &stmtCostCollector{
		current: newStmtCostByDigestMap(),
	}
	go sc.historyLoop()
	return sc
}

func newStmtCostByDigestMap() *stmtCostByDigestMap {
	maxStmtCount := config.GetGlobalConfig().StmtCost.MaxStmtCount
	begin := (int(time.Now().Unix()) / config.GetGlobalConfig().StmtCost.RefreshInterval) * config.GetGlobalConfig().StmtCost.RefreshInterval
	return &stmtCostByDigestMap{
		costMap: kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
		begin:   int64(begin),
	}
}

func (sc *stmtCostCollector) SetSessionManager(sm util.SessionManager) {
	if sm != nil {
		sc.sm = sm
	}
}

func (sc *stmtCostCollector) historyLoop() {
	now := int(time.Now().Unix())
	next := (now/config.GetGlobalConfig().StmtCost.RefreshInterval + 1) * config.GetGlobalConfig().StmtCost.RefreshInterval
	tick := time.NewTicker(time.Duration(next-now) * time.Second)
	bootstrap := true
	for {
		select {
		case <-tick.C:
			if bootstrap {
				bootstrap = false
				tick.Reset(time.Duration(config.GetGlobalConfig().StmtCost.RefreshInterval) * time.Second)
			}
			sc.saveToHistory()
		}
	}
}

func (sc *stmtCostCollector) saveToHistory() {
	historySize := config.GetGlobalConfig().StmtCost.HistorySize
	sc.Lock()
	sc.current.end = time.Now().Unix()
	if len(sc.history) < historySize {
		sc.history = append(sc.history, sc.current)
	} else {
		idx := sc.historyIdx % len(sc.history)
		sc.history[idx] = sc.current
	}
	old := sc.current
	sc.current = newStmtCostByDigestMap()
	sc.historyIdx++
	sc.Unlock()

	old.Lock()
	old.addRunningStmtCost(sc.sm, true)
	old.Unlock()
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (sc *stmtCostCollector) AddStatement(sei *stmtsummary.StmtExecInfo) {
	cfg := config.GetGlobalConfig().StmtCost
	if !cfg.Enable {
		return
	}
	key := &stmtCostByDigestKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
	}
	key.Hash()
	sc.Lock()
	defer sc.Unlock()
	sc.current.AddStatement(key, sei)
}

func (scm *stmtCostByDigestMap) AddStatement(key *stmtCostByDigestKey, sei *stmtsummary.StmtExecInfo) {
	value, ok := scm.costMap.Get(key)
	var stmt *stmtCostByDigest
	if !ok {
		// Lazy initialize it to release ssMap.mutex ASAP.
		stmt = &stmtCostByDigest{
			schemaName:    sei.SchemaName,
			digest:        sei.Digest,
			normalizedSQL: formatSQL(sei.NormalizedSQL),
			sampleSQL:     formatSQL(sei.OriginalSQL),
		}
		scm.costMap.Put(key, stmt)
	} else {
		stmt = value.(*stmtCostByDigest)
	}
	stmt.execCount++
	stmt.sumCPUTime += sei.CPUTime

	stmt.sumQueryTime += sei.TotalLatency
	stmt.sumParseTime += sei.ParseLatency
	stmt.sumCompileTime += sei.CompileLatency
	stmt.sumOptimizeTime += sei.OptimizeLatency
	stmt.sumWaitTsTime += sei.WaitTsLatency
	if sei.ExecDetail != nil {
		stmt.sumCopProcessTime += sei.ExecDetail.TimeDetail.ProcessTime
		stmt.sumCopWaitTime += sei.ExecDetail.TimeDetail.WaitTime
		if sei.ExecDetail.CommitDetail != nil {
			stmt.sumPreWriteTime += sei.ExecDetail.CommitDetail.PrewriteTime
			stmt.sumGetCommitTsTime += sei.ExecDetail.CommitDetail.GetCommitTsTime
			stmt.sumCommitTime += sei.ExecDetail.CommitDetail.CommitTime
			stmt.sumResolveLockTime += time.Duration(sei.ExecDetail.CommitDetail.ResolveLockTime)
		}
	}
}

// Clear removes all statement summaries.
func (sc *stmtCostCollector) Clear() {
	sc.Lock()
	defer sc.Unlock()

	sc.current.costMap.DeleteAll()
	sc.history = nil
	sc.historyIdx = 0
}

// ToCurrentDatum converts current statement summaries to datum.
func (sc *stmtCostCollector) ToCurrentDatum() [][]types.Datum {
	cfg := config.GetGlobalConfig().StmtCost
	var current *stmtCostByDigestMap
	sc.Lock()
	current = sc.current.Copy()
	sc.Unlock()

	current.Lock()
	defer current.Unlock()
	current.addRunningStmtCost(sc.sm, false)

	values := current.costMap.Values()
	beginTime := current.begin
	end := beginTime + int64(cfg.RefreshInterval)
	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		record := value.(*stmtCostByDigest).toCurrentDatum(beginTime, end)
		if record != nil {
			rows = append(rows, record)
		}
	}
	return rows
}

// ToHistoryDatum converts history statements summaries to datum.
func (sc *stmtCostCollector) ToHistoryDatum() [][]types.Datum {
	sc.Lock()
	sc.Unlock()
	rows := make([][]types.Datum, 0, len(sc.history)*10)
	for _, scbd := range sc.history {
		values := scbd.costMap.Values()
		for _, value := range values {
			record := value.(*stmtCostByDigest).toCurrentDatum(scbd.begin, scbd.end)
			if record != nil {
				rows = append(rows, record)
			}
		}
	}
	return rows
}

func (scbd *stmtCostByDigest) toCurrentDatum(beginTime, endTime int64) []types.Datum {
	// Actually, there's a small chance that endTime is out of date, but it's hard to keep it up to date all the time.
	return types.MakeDatums(
		types.NewTime(types.FromGoTime(time.Unix(beginTime, 0)), mysql.TypeTimestamp, 0),
		types.NewTime(types.FromGoTime(time.Unix(endTime, 0)), mysql.TypeTimestamp, 0),
		scbd.schemaName,
		scbd.digest,
		scbd.execCount,
		scbd.sumCPUTime,
		int64(scbd.sumQueryTime),
		int64(scbd.sumParseTime),
		int64(scbd.sumCompileTime),
		int64(scbd.sumOptimizeTime),
		int64(scbd.sumWaitTsTime),
		int64(scbd.sumCopProcessTime),
		int64(scbd.sumCopWaitTime),
		int64(scbd.sumBackoffTime),
		int64(scbd.sumPreWriteTime),
		int64(scbd.sumGetCommitTsTime),
		int64(scbd.sumCommitTime),
		int64(scbd.sumResolveLockTime),
		scbd.normalizedSQL,
		scbd.sampleSQL,
	)
}

func (scm *stmtCostByDigestMap) Copy() *stmtCostByDigestMap {
	to := &stmtCostByDigestMap{
		costMap: kvcache.NewSimpleLRUCache(scm.costMap.GetCapacity(), 0, 0),
		begin:   scm.begin,
		end:     scm.end,
	}
	scm.costMap.Iter(func(key kvcache.Key, value kvcache.Value) bool {
		stmt := value.(*stmtCostByDigest)
		newStmt := *stmt
		to.costMap.Put(key, &newStmt)
		return false
	})
	return to
}

func (scm *stmtCostByDigestMap) addRunningStmtCost(sm util.SessionManager, saveToHistory bool) {
	if sm == nil {
		return
	}
	pl := sm.ShowProcessList()
	for _, pi := range pl {
		key := &stmtCostByDigestKey{
			schemaName: pi.DB,
			digest:     pi.Digest,
		}
		key.Hash()

		cpuTime := int64(0)
		if pi.ExecStats != nil && pi.ExecStats.TaskGroup != nil {
			var taskGroupMetrics = []metrics.Sample{
				{Name: "/taskgroup/sched/cputime:nanoseconds"},
			}
			metrics.ReadTaskGroup(pi.ExecStats.TaskGroup, taskGroupMetrics)
			cpuTime = int64(taskGroupMetrics[0].Value.Uint64()) - pi.ExecStats.ConsumedCPUTime
			if saveToHistory {
				pi.ExecStats.ConsumedCPUTime += cpuTime
			}
		}

		scm.AddStatement(key, &stmtsummary.StmtExecInfo{
			SchemaName:    pi.DB,
			Digest:        pi.Digest,
			NormalizedSQL: formatSQL(pi.NormalizedSQL),
			OriginalSQL:   formatSQL(pi.Info),
			CPUTime:       cpuTime,
		})
	}
}

// Truncate SQL to maxSQLLength.
func formatSQL(sql string) string {
	maxSQLLength := config.GetGlobalConfig().StmtCost.MaxSQLLength
	length := len(sql)
	if uint(length) > maxSQLLength {
		sql = fmt.Sprintf("%.*s(len:%d)", maxSQLLength, sql, length)
	}
	return sql
}
