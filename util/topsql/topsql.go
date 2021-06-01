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

package topsql

import (
	"context"
	"runtime/pprof"

	"github.com/pingcap/tidb/util/topsql/collector"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
)

// SetupTopSQL sets up the top-sql worker.
func SetupTopSQL() {
	tracecpu.GlobalSQLCPUProfiler.Run()
}

// AttachSQLInfo attach the sql information info top sql.
func AttachSQLInfo(ctx context.Context, normalizedSQL string, sqlDigest []byte, normalizedPlan string, planDigest []byte) {
	if len(normalizedSQL) == 0 || len(sqlDigest) == 0 {
		return
	}
	ctx = tracecpu.CtxWithDigest(ctx, sqlDigest, planDigest)
	pprof.SetGoroutineLabels(ctx)

	if len(normalizedSQL) == 0 || len(planDigest) == 0 {
		// If plan digest is '', indicate it is the first time to attach the SQL info, since it only know the sql digest.
		linkSQLTextWithDigest(sqlDigest, normalizedSQL)
	} else {
		linkPlanTextWithDigest(planDigest, normalizedPlan)
	}
}

func linkSQLTextWithDigest(sqlDigest []byte, normalizedSQL string) {
	c := tracecpu.GlobalSQLCPUProfiler.GetCollector()
	if c == nil {
		return
	}
	topc, ok := c.(collector.TopSQLCollector)
	if ok {
		topc.RegisterSQL(sqlDigest, normalizedSQL)
	}
}

func linkPlanTextWithDigest(planDigest []byte, normalizedPlan string) {
	c := tracecpu.GlobalSQLCPUProfiler.GetCollector()
	if c == nil {
		return
	}
	topc, ok := c.(collector.TopSQLCollector)
	if ok {
		topc.RegisterPlan(planDigest, normalizedPlan)
	}
}
