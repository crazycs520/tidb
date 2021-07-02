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

package reporter

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/wangjohn/quickselect"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	dialTimeout               = 5 * time.Second
	reportTimeout             = 40 * time.Second
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

var _ TopSQLReporter = &RemoteTopSQLReporter{}

// TopSQLReporter collects Top SQL metrics.
type TopSQLReporter interface {
	tracecpu.Collector
	RegisterSQL(sqlDigest []byte, normalizedSQL string)
	RegisterPlan(planDigest []byte, normalizedPlan string)
	Close()
}

type cpuData struct {
	timestamp uint64
	records   []tracecpu.SQLCPUTimeRecord
}

// dataPoints represents the cumulative SQL plan CPU time in current minute window
type dataPoints struct {
	SQLDigest      []byte
	PlanDigest     []byte
	TimestampList  []uint64
	CPUTimeMsList  []uint32
	CPUTimeMsTotal uint64
}

type dataPointsOrderByCPUTime []*dataPoints

func (t dataPointsOrderByCPUTime) Len() int {
	return len(t)
}

func (t dataPointsOrderByCPUTime) Less(i, j int) bool {
	// We need find the kth largest value, so here should use >
	return t[i].CPUTimeMsTotal > t[j].CPUTimeMsTotal
}
func (t dataPointsOrderByCPUTime) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type sqlCPUTimeRecordSlice []tracecpu.SQLCPUTimeRecord

func (t sqlCPUTimeRecordSlice) Len() int {
	return len(t)
}

func (t sqlCPUTimeRecordSlice) Less(i, j int) bool {
	// We need find the kth largest value, so here should use >
	return t[i].CPUTimeMs > t[j].CPUTimeMs
}
func (t sqlCPUTimeRecordSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type planBinaryDecodeFunc func(string) (string, error)

// RemoteTopSQLReporter implements a TopSQL reporter that sends data to a remote agent
// This should be called periodically to collect TopSQL resource usage metrics
type RemoteTopSQLReporter struct {
	ctx    context.Context
	cancel context.CancelFunc
	client ReportClient

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are normalized SQL strings
	normalizedSQLMap atomic.Value // sync.Map
	sqlMapLength     atomic2.Int64

	// normalizedPlanMap is an map, whose keys are plan digest strings and values are normalized plans **in binary**.
	// The normalized plans in binary can be decoded to string using the `planBinaryDecoder`.
	normalizedPlanMap atomic.Value // sync.Map
	planMapLength     atomic2.Int64

	collectCPUDataChan chan cpuData
	reportDataChan     chan reportData
}

// NewRemoteTopSQLReporter creates a new TopSQL reporter
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// MaxStatementsNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewRemoteTopSQLReporter(client ReportClient) *RemoteTopSQLReporter {
	ctx, cancel := context.WithCancel(context.Background())
	tsr := &RemoteTopSQLReporter{
		ctx:                ctx,
		cancel:             cancel,
		client:             client,
		collectCPUDataChan: make(chan cpuData, 1),
		reportDataChan:     make(chan reportData, 1),
	}
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})

	go tsr.collectWorker()
	go tsr.reportWorker()

	return tsr
}

// RegisterSQL registers a normalized SQL string to a SQL digest.
// This function is thread-safe and efficient.
//
// Note that the normalized SQL string can be of >1M long.
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
func (tsr *RemoteTopSQLReporter) RegisterSQL(sqlDigest []byte, normalizedSQL string) {
	if tsr.sqlMapLength.Load() >= variable.TopSQLVariable.MaxCollect.Load() {
		return
	}
	m := tsr.normalizedSQLMap.Load().(*sync.Map)
	key := string(sqlDigest)
	_, loaded := m.LoadOrStore(key, normalizedSQL)
	if !loaded {
		tsr.sqlMapLength.Add(1)
	}
}

// RegisterPlan is like RegisterSQL, but for normalized plan strings.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) RegisterPlan(planDigest []byte, normalizedBinaryPlan string) {
	if tsr.planMapLength.Load() >= variable.TopSQLVariable.MaxCollect.Load() {
		return
	}
	m := tsr.normalizedPlanMap.Load().(*sync.Map)
	key := string(planDigest)
	_, loaded := m.LoadOrStore(key, normalizedBinaryPlan)
	if !loaded {
		tsr.planMapLength.Add(1)
	}
}

// Collect receives CPU time records for processing. WARN: It will drop the records if the processing is not in time.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) Collect(timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	if len(records) == 0 {
		return
	}
	select {
	case tsr.collectCPUDataChan <- cpuData{
		timestamp: timestamp,
		records:   records,
	}:
	default:
		// ignore if chan blocked
	}
}

// Close uses to close and release the reporter resource.
func (tsr *RemoteTopSQLReporter) Close() {
	tsr.cancel()
	tsr.client.Close()
}

type collectedData struct {
	data   map[string]*dataPoints
	others *dataPoints
}

func newCollectedData() *collectedData {
	return &collectedData{
		data:   make(map[string]*dataPoints),
		others: &dataPoints{},
	}
}

func (c *collectedData) addEvictedCPUTime(timestamp uint64, totalCPUTimeMs uint32) {
	if len(c.others.TimestampList) == 0 {
		c.others.TimestampList = []uint64{timestamp}
		c.others.CPUTimeMsList = []uint32{totalCPUTimeMs}
	} else {
		c.others.TimestampList = append(c.others.TimestampList, timestamp)
		c.others.CPUTimeMsList = append(c.others.CPUTimeMsList, totalCPUTimeMs)
	}
	c.others.CPUTimeMsTotal += uint64(totalCPUTimeMs)
}

func (d *dataPoints) merge(other *dataPoints) {
	if other == nil || len(other.TimestampList) == 0 {
		return
	}
	if len(d.TimestampList) == 0 {
		d.TimestampList = other.TimestampList
		d.CPUTimeMsList = other.CPUTimeMsList
		d.CPUTimeMsTotal = other.CPUTimeMsTotal
		return
	}
	d.CPUTimeMsTotal += other.CPUTimeMsTotal
	findIdx := 0
	for idx, ts := range other.TimestampList {
		cpuTimeMs := other.CPUTimeMsList[idx]
		for ; findIdx < len(d.TimestampList); findIdx++ {
			findTs := d.TimestampList[findIdx]
			if ts == findTs {
				d.CPUTimeMsList[findIdx] += cpuTimeMs
				break
			}
			if ts < findTs {
				d.TimestampList = append(d.TimestampList, 0)
				copy(d.TimestampList[findIdx+1:], d.TimestampList[findIdx:])
				d.TimestampList[findIdx] = ts
				d.CPUTimeMsList = append(d.CPUTimeMsList, 0)
				copy(d.CPUTimeMsList[findIdx+1:], d.CPUTimeMsList[findIdx:])
				d.CPUTimeMsList[findIdx] = cpuTimeMs
				break
			}
		}
		if ts > d.TimestampList[len(d.TimestampList)-1] {
			d.TimestampList = append(d.TimestampList, ts)
			d.CPUTimeMsList = append(d.CPUTimeMsList, cpuTimeMs)
		}
	}
}

func (c *collectedData) addEvictedDataPoints(evict *dataPoints) {
	if len(evict.TimestampList) != len(evict.CPUTimeMsList) {
		logutil.BgLogger().Error("[top-sql] add evicted data error, should never happen",
			zap.Uint64s("timestamp-list", evict.TimestampList),
			zap.Uint32s("cpu-time-list", evict.CPUTimeMsList))
		return
	}
	c.others.merge(evict)
}

func (tsr *RemoteTopSQLReporter) collectWorker() {
	defer util.Recover("top-sql", "collectWorker", nil, false)

	collectedData := newCollectedData()
	currentReportInterval := variable.TopSQLVariable.ReportIntervalSeconds.Load()
	reportTicker := time.NewTicker(time.Second * time.Duration(currentReportInterval))
	for {
		select {
		case data := <-tsr.collectCPUDataChan:
			// On receiving data to collect: Write to local data array, and retain records with most CPU time.
			tsr.doCollect(collectedData, data.timestamp, data.records)
		case <-reportTicker.C:
			oldData := collectedData
			collectedData = newCollectedData()
			tsr.takeDataAndSendToReportChan(oldData)

			// Update `reportTicker` if report interval changed.
			if newInterval := variable.TopSQLVariable.ReportIntervalSeconds.Load(); newInterval != currentReportInterval {
				currentReportInterval = newInterval
				reportTicker.Reset(time.Second * time.Duration(currentReportInterval))
			}
		case <-tsr.ctx.Done():
			return
		}
	}
}

func encodeKey(buf *bytes.Buffer, sqlDigest, planDigest []byte) string {
	buf.Reset()
	buf.Write(sqlDigest)
	buf.Write(planDigest)
	return buf.String()
}

func getTopNRecords(records []tracecpu.SQLCPUTimeRecord) (topN, shouldEvict []tracecpu.SQLCPUTimeRecord) {
	maxStmt := int(variable.TopSQLVariable.MaxStatementCount.Load())
	if len(records) <= maxStmt {
		return records, nil
	}
	if err := quickselect.QuickSelect(sqlCPUTimeRecordSlice(records), maxStmt); err != nil {
		//	skip eviction
		return records, nil
	}
	return records[:maxStmt], records[maxStmt:]
}

func getTopNDataPoints(records []*dataPoints) (topN, shouldEvict []*dataPoints) {
	maxStmt := int(variable.TopSQLVariable.MaxStatementCount.Load())
	if len(records) <= maxStmt {
		return records, nil
	}
	if err := quickselect.QuickSelect(dataPointsOrderByCPUTime(records), maxStmt); err != nil {
		//	skip eviction
		return records, nil
	}
	return records[:maxStmt], records[maxStmt:]
}

// doCollect collects top N records of each round into collectTarget, and evict the data that is not in top N.
func (tsr *RemoteTopSQLReporter) doCollect(
	collectTarget *collectedData, timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	defer util.Recover("top-sql", "doCollect", nil, false)

	// Get top N records of each round records.
	var evicted []tracecpu.SQLCPUTimeRecord
	records, evicted = getTopNRecords(records)

	keyBuf := bytes.NewBuffer(make([]byte, 0, 64))
	listCapacity := int(variable.TopSQLVariable.ReportIntervalSeconds.Load()/variable.TopSQLVariable.PrecisionSeconds.Load() + 1)
	if listCapacity < 1 {
		listCapacity = 1
	}
	// Collect the top N records to collectTarget for each round.
	for _, record := range records {
		key := encodeKey(keyBuf, record.SQLDigest, record.PlanDigest)
		entry, exist := collectTarget.data[key]
		if !exist {
			entry = &dataPoints{
				SQLDigest:     record.SQLDigest,
				PlanDigest:    record.PlanDigest,
				CPUTimeMsList: make([]uint32, 1, listCapacity),
				TimestampList: make([]uint64, 1, listCapacity),
			}
			entry.CPUTimeMsList[0] = record.CPUTimeMs
			entry.TimestampList[0] = timestamp
			collectTarget.data[key] = entry
		} else {
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		entry.CPUTimeMsTotal += uint64(record.CPUTimeMs)
	}

	// Evict redundant data.
	normalizedSQLMap := tsr.normalizedSQLMap.Load().(*sync.Map)
	normalizedPlanMap := tsr.normalizedPlanMap.Load().(*sync.Map)
	totalEvictedCPUTime := uint32(0)
	for _, evict := range evicted {
		totalEvictedCPUTime += evict.CPUTimeMs
		key := encodeKey(keyBuf, evict.SQLDigest, evict.PlanDigest)
		_, ok := collectTarget.data[key]
		if ok {
			continue
		}
		_, loaded := normalizedSQLMap.LoadAndDelete(string(evict.SQLDigest))
		if loaded {
			tsr.sqlMapLength.Add(-1)
		}
		_, loaded = normalizedPlanMap.LoadAndDelete(string(evict.PlanDigest))
		if loaded {
			tsr.planMapLength.Add(-1)
		}
	}
	collectTarget.addEvictedCPUTime(timestamp, totalEvictedCPUTime)
}

// takeDataAndSendToReportChan takes out (resets) collected data. These data will be send to a report channel
// for reporting later.
func (tsr *RemoteTopSQLReporter) takeDataAndSendToReportChan(collectTarget *collectedData) {
	// Fetch TopN dataPoints.
	records := make([]*dataPoints, 0, len(collectTarget.data))
	for _, v := range collectTarget.data {
		records = append(records, v)
	}
	normalizedSQLMap := tsr.normalizedSQLMap.Load().(*sync.Map)
	normalizedPlanMap := tsr.normalizedPlanMap.Load().(*sync.Map)

	// Reset data for next report.
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})
	tsr.sqlMapLength.Store(0)
	tsr.planMapLength.Store(0)

	// Evict redundant data.
	var evicted []*dataPoints
	records, evicted = getTopNDataPoints(records)
	for _, evict := range evicted {
		normalizedSQLMap.LoadAndDelete(string(evict.SQLDigest))
		normalizedPlanMap.LoadAndDelete(string(evict.PlanDigest))
		collectTarget.addEvictedDataPoints(evict)
	}

	// append others which contains all evicted item's cpu-time.
	if collectTarget.others.CPUTimeMsTotal > 0 {
		records = append(records, collectTarget.others)
	}
	data := reportData{
		collectedData:     records,
		normalizedSQLMap:  normalizedSQLMap,
		normalizedPlanMap: normalizedPlanMap,
	}

	// Send to report channel. When channel is full, data will be dropped.
	select {
	case tsr.reportDataChan <- data:
	default:
	}
}

// reportData contains data that reporter sends to the agent
type reportData struct {
	collectedData     []*dataPoints
	normalizedSQLMap  *sync.Map
	normalizedPlanMap *sync.Map
}

func (d *reportData) hasData() bool {
	if len(d.collectedData) > 0 {
		return true
	}
	cnt := 0
	d.normalizedSQLMap.Range(func(key, value interface{}) bool {
		cnt++
		return false
	})
	if cnt > 0 {
		return true
	}
	d.normalizedPlanMap.Range(func(key, value interface{}) bool {
		cnt++
		return false
	})
	return cnt > 0
}

// reportWorker sends data to the gRPC endpoint from the `reportDataChan` one by one.
func (tsr *RemoteTopSQLReporter) reportWorker() {
	defer util.Recover("top-sql", "reportWorker", nil, false)

	for {
		select {
		case data := <-tsr.reportDataChan:
			// When `reportDataChan` receives something, there could be ongoing `RegisterSQL` and `RegisterPlan` running,
			// who writes to the data structure that `data` contains. So we wait for a little while to ensure that
			// these writes are finished.
			time.Sleep(time.Millisecond * 100)
			tsr.doReport(data)
		case <-tsr.ctx.Done():
			return
		}
	}
}

func (tsr *RemoteTopSQLReporter) doReport(data reportData) {
	defer util.Recover("top-sql", "doReport", nil, false)

	if !data.hasData() {
		return
	}

	agentAddr := variable.TopSQLVariable.AgentAddress.Load()

	timeout := reportTimeout
	failpoint.Inject("resetTimeoutForTest", func(val failpoint.Value) {
		if val.(bool) {
			interval := time.Duration(variable.TopSQLVariable.ReportIntervalSeconds.Load()) * time.Second
			if interval < timeout {
				timeout = interval
			}
		}
	})
	ctx, cancel := context.WithTimeout(tsr.ctx, timeout)

	err := tsr.client.Send(ctx, agentAddr, data)
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] client failed to send data", zap.Error(err))
	}
	cancel()
}
