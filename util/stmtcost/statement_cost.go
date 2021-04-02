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
	"github.com/pingcap/tidb/config"
	"sync"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
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
}

type stmtCostByDigestMap struct {
	costMap *kvcache.SimpleLRUCache
	other   stmtCostStats
	begin   int64 // unix second
	end     int64
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
	return &stmtCostByDigestMap{
		costMap: kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
		begin:   time.Now().Unix(),
	}
}

func (sc *stmtCostCollector) historyLoop() {
	tick := time.NewTicker(time.Duration(config.GetGlobalConfig().StmtCost.RefreshInterval) * time.Second)
	for {
		select {
		case <-tick.C:
			sc.saveToHistory()
		}
	}
}

func (sc *stmtCostCollector) saveToHistory() {
	historySize := config.GetGlobalConfig().StmtCost.HistorySize
	sc.Lock()
	defer sc.Unlock()
	sc.current.end = time.Now().Unix()
	if len(sc.history) < historySize {
		sc.history = append(sc.history, sc.current)
	} else {
		idx := sc.historyIdx % len(sc.history)
		sc.history[idx] = sc.current
	}
	sc.current = newStmtCostByDigestMap()
	sc.historyIdx++
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (sc *stmtCostCollector) AddStatement(sei *StmtExecInfo) {
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
	current := sc.current
	value, ok := current.costMap.Get(key)
	var stmt *stmtCostByDigest
	if !ok {
		// Lazy initialize it to release ssMap.mutex ASAP.
		stmt = &stmtCostByDigest{
			schemaName:    sei.SchemaName,
			digest:        sei.Digest,
			normalizedSQL: sei.NormalizedSQL,
			sampleSQL:     sei.OriginalSQL,
		}
		current.costMap.Put(key, stmt)
	} else {
		stmt = value.(*stmtCostByDigest)
	}
	stmt.execCount++
	stmt.sumCPUTime += sei.CPUTime
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
	sc.Lock()
	defer sc.Unlock()
	values := sc.current.costMap.Values()
	beginTime := sc.current.begin
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
		values := sc.current.costMap.Values()
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
		scbd.sumCPUTime,
		scbd.execCount,
		scbd.normalizedSQL,
		scbd.sampleSQL,
	)
}

// StmtExecInfo records execution information of each statement.
type StmtExecInfo struct {
	SchemaName    string
	Digest        string
	NormalizedSQL string
	OriginalSQL   string
	CPUTime       int64
}
