// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	pmodel "github.com/prometheus/common/model"
)

const promReadTimeout = time.Second * 10

// MetricRetriever uses to read metric data.
type MetricRetriever struct {
	dummyCloser
	table     *model.TableInfo
	tblDef    *infoschema.MetricTableDef
	extractor *plannercore.MetricTableExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true

	failpoint.InjectContext(ctx, "mockMetricsTableData", func() {
		m, ok := ctx.Value("__mockMetricsTableData").(map[string][][]types.Datum)
		if ok && m[e.table.Name.L] != nil {
			failpoint.Return(m[e.table.Name.L], nil)
		}
	})

	tblDef, err := infoschema.GetMetricTableDef(e.table.Name.L)
	if err != nil {
		return nil, err
	}
	e.tblDef = tblDef
	queryRange := e.getQueryRange(sctx)
	totalRows := make([][]types.Datum, 0)
	quantiles := e.extractor.Quantiles
	if len(quantiles) == 0 {
		quantiles = []float64{tblDef.Quantile}
	}
	for _, quantile := range quantiles {
		var queryValue pmodel.Value
		// Add retry to avoid network error.
		for i := 0; i < 10; i++ {
			queryValue, err = e.queryMetric(ctx, sctx, queryRange, quantile)
			if err == nil || strings.Contains(err.Error(), "parse error") {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			return nil, err
		}
		partRows := e.genRows(queryValue, quantile)
		totalRows = append(totalRows, partRows...)
	}
	return totalRows, nil
}

func (e *MetricRetriever) queryMetric(ctx context.Context, sctx sessionctx.Context, queryRange promv1.Range, quantile float64) (pmodel.Value, error) {
	failpoint.InjectContext(ctx, "mockMetricsPromData", func() {
		failpoint.Return(ctx.Value("__mockMetricsPromData").(pmodel.Matrix), nil)
	})

	addr, err := e.getMetricAddr(sctx)
	if err != nil {
		return nil, err
	}

	queryClient, err := newQueryClient(addr)
	if err != nil {
		return nil, err
	}

	promQLAPI := promv1.NewAPI(queryClient)
	ctx, cancel := context.WithTimeout(ctx, promReadTimeout)
	defer cancel()

	promQL := e.tblDef.GenPromQL(sctx, e.extractor.LabelConditions, quantile)
	result, _, err := promQLAPI.QueryRange(ctx, promQL, queryRange)
	return result, err
}

func (e *MetricRetriever) getMetricAddr(sctx sessionctx.Context) (string, error) {
	// Get PD servers info.
	store := sctx.GetStore()
	etcd, ok := store.(tikv.EtcdBackend)
	if !ok {
		return "", errors.Errorf("%T not an etcd backend", store)
	}
	for _, addr := range etcd.EtcdAddrs() {
		return addr, nil
	}
	return "", errors.Errorf("pd address was not found")
}

type promQLQueryRange = promv1.Range

func (e *MetricRetriever) getQueryRange(sctx sessionctx.Context) promQLQueryRange {
	startTime, endTime := e.extractor.StartTime, e.extractor.EndTime
	step := time.Second * time.Duration(sctx.GetSessionVars().MetricSchemaStep)
	return promQLQueryRange{Start: startTime, End: endTime, Step: step}
}

func (e *MetricRetriever) genRows(value pmodel.Value, quantile float64) [][]types.Datum {
	var rows [][]types.Datum
	switch value.Type() {
	case pmodel.ValMatrix:
		matrix := value.(pmodel.Matrix)
		for _, m := range matrix {
			for _, v := range m.Values {
				record := e.genRecord(m.Metric, v, quantile)
				rows = append(rows, record)
			}
		}
	}
	return rows
}

func (e *MetricRetriever) genRecord(metric pmodel.Metric, pair pmodel.SamplePair, quantile float64) []types.Datum {
	record := make([]types.Datum, 0, 2+len(e.tblDef.Labels)+1)
	// Record order should keep same with genColumnInfos.
	record = append(record, types.NewTimeDatum(types.NewTime(
		types.FromGoTime(time.Unix(int64(pair.Timestamp/1000), int64(pair.Timestamp%1000)*1e6)),
		mysql.TypeDatetime,
		types.MaxFsp,
	)))
	for _, label := range e.tblDef.Labels {
		v := ""
		if metric != nil {
			v = string(metric[pmodel.LabelName(label)])
		}
		if len(v) == 0 {
			v = infoschema.GenLabelConditionValues(e.extractor.LabelConditions[strings.ToLower(label)])
		}
		record = append(record, types.NewStringDatum(v))
	}
	if e.tblDef.Quantile > 0 {
		record = append(record, types.NewFloat64Datum(quantile))
	}
	if math.IsNaN(float64(pair.Value)) {
		record = append(record, types.NewDatum(nil))
	} else {
		record = append(record, types.NewFloat64Datum(float64(pair.Value)))
	}
	return record
}

type queryClient struct {
	api.Client
}

func newQueryClient(addr string) (api.Client, error) {
	promClient, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s", addr),
	})
	if err != nil {
		return nil, err
	}
	return &queryClient{
		promClient,
	}, nil
}

// URL implement the api.Client interface.
// This is use to convert prometheus api path to PD API path.
func (c *queryClient) URL(ep string, args map[string]string) *url.URL {
	// FIXME: add `PD-Allow-follower-handle: true` in http header, let pd follower can handle this request too.
	ep = strings.Replace(ep, "api/v1", "pd/api/v1/metric", 1)
	return c.Client.URL(ep, args)
}

// MetricsSummaryRetriever uses to read metric data.
type MetricsSummaryRetriever struct {
	dummyCloser
	table     *model.TableInfo
	extractor *plannercore.MetricSummaryTableExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricsSummaryRetriever) retrieve(_ context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	totalRows := make([][]types.Datum, 0, len(infoschema.MetricTableMap))
	tables := make([]string, 0, len(infoschema.MetricTableMap))
	for name := range infoschema.MetricTableMap {
		tables = append(tables, name)
	}
	sort.Strings(tables)

	filter := inspectionFilter{set: e.extractor.MetricsNames}
	condition := e.timeRange.Condition()
	for _, name := range tables {
		if !filter.enable(name) {
			continue
		}
		def, found := infoschema.MetricTableMap[name]
		if !found {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("metrics table: %s not found", name))
			continue
		}
		var sql string
		if def.Quantile > 0 {
			var qs []string
			if len(e.extractor.Quantiles) > 0 {
				for _, q := range e.extractor.Quantiles {
					qs = append(qs, fmt.Sprintf("%f", q))
				}
			} else {
				qs = []string{"0.99"}
			}
			sql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value),quantile from `%[2]s`.`%[1]s` %[3]s and quantile in (%[4]s) group by quantile order by quantile",
				name, util.MetricSchemaName.L, condition, strings.Join(qs, ","))
		} else {
			sql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value) from `%[2]s`.`%[1]s` %[3]s",
				name, util.MetricSchemaName.L, condition)
		}

		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
		if err != nil {
			return nil, errors.Errorf("execute '%s' failed: %v", sql, err)
		}
		for _, row := range rows {
			var quantile interface{}
			if def.Quantile > 0 {
				quantile = row.GetFloat64(row.Len() - 1)
			}
			totalRows = append(totalRows, types.MakeDatums(
				name,
				quantile,
				row.GetFloat64(0),
				row.GetFloat64(1),
				row.GetFloat64(2),
				row.GetFloat64(3),
				def.Comment,
			))
		}
	}
	return totalRows, nil
}

// MetricsSummaryByLabelRetriever uses to read metric detail data.
type MetricsSummaryByLabelRetriever struct {
	dummyCloser
	table     *model.TableInfo
	extractor *plannercore.MetricSummaryTableExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricsSummaryByLabelRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	totalRows := make([][]types.Datum, 0, len(infoschema.MetricTableMap))
	tables := make([]string, 0, len(infoschema.MetricTableMap))
	for name := range infoschema.MetricTableMap {
		tables = append(tables, name)
	}
	sort.Strings(tables)

	filter := inspectionFilter{set: e.extractor.MetricsNames}
	condition := e.timeRange.Condition()
	for _, name := range tables {
		if !filter.enable(name) {
			continue
		}
		def, found := infoschema.MetricTableMap[name]
		if !found {
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("metrics table: %s not found", name))
			continue
		}
		cols := def.Labels
		cond := condition
		if def.Quantile > 0 {
			cols = append(cols, "quantile")
			if len(e.extractor.Quantiles) > 0 {
				qs := make([]string, len(e.extractor.Quantiles))
				for i, q := range e.extractor.Quantiles {
					qs[i] = fmt.Sprintf("%f", q)
				}
				cond += " and quantile in (" + strings.Join(qs, ",") + ")"
			} else {
				cond += " and quantile=0.99"
			}
		}
		var sql string
		if len(cols) > 0 {
			sql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value),`%s` from `%s`.`%s` %s group by `%[1]s` order by `%[1]s`",
				strings.Join(cols, "`,`"), util.MetricSchemaName.L, name, cond)
		} else {
			sql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value) from `%s`.`%s` %s",
				util.MetricSchemaName.L, name, cond)
		}
		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(ctx, sql)
		if err != nil {
			return nil, errors.Errorf("execute '%s' failed: %v", sql, err)
		}
		nonInstanceLabelIndex := 0
		if len(def.Labels) > 0 && def.Labels[0] == "instance" {
			nonInstanceLabelIndex = 1
		}
		// skip sum/avg/min/max
		const skipCols = 4
		for _, row := range rows {
			instance := ""
			if nonInstanceLabelIndex > 0 {
				instance = row.GetString(skipCols) // sum/avg/min/max
			}
			var labels []string
			for i, label := range def.Labels[nonInstanceLabelIndex:] {
				// skip min/max/avg/instance
				val := row.GetString(skipCols + nonInstanceLabelIndex + i)
				if label == "store" || label == "store_id" {
					val = fmt.Sprintf("store_id:%s", val)
				}
				labels = append(labels, val)
			}
			var quantile interface{}
			if def.Quantile > 0 {
				quantile = row.GetFloat64(row.Len() - 1) // quantile will be the last column
			}
			totalRows = append(totalRows, types.MakeDatums(
				instance,
				name,
				strings.Join(labels, ", "),
				quantile,
				row.GetFloat64(0), // sum
				row.GetFloat64(1), // avg
				row.GetFloat64(2), // min
				row.GetFloat64(3), // max
				def.Comment,
			))
		}
	}
	return totalRows, nil
}

type MetricTotalTimeRetriever struct {
	dummyCloser
	table     *model.TableInfo
	extractor *plannercore.MetricTableExtractor
	retrieved bool
}

func (e *MetricTotalTimeRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}

	totalTimeMetrics := []struct {
		name  string
		tbl   string
		label string
	}{
		{name: "user_query", tbl: "tidb_query", label: "sql_type"}, // sql_type
		{name: "get_token(us)", tbl: "tidb_get_token"},
		{name: "parse", tbl: "tidb_parse"},                                                           // no
		{name: "compile", tbl: "tidb_compile", label: "sql_type"},                                    // sql_type
		{name: "tidb_execute", tbl: "tidb_execute", label: "sql_type"},                               // sql_type
		{name: "tidb_slow_query", tbl: "tidb_slow_query", label: "instance"},                         // sql_type
		{name: "tidb_slow_query_cop_process", tbl: "tidb_slow_query_cop_process", label: "instance"}, // sql_type
		{name: "tidb_slow_query_cop_wait", tbl: "tidb_slow_query_cop_wait", label: "instance"},       // sql_type
		{name: "tidb_transaction_local_latch_wait", tbl: "tidb_transaction_local_latch_wait"},
		{name: "distsql_execution", tbl: "tidb_distsql_execution", label: "type"}, //-- type
		{name: "cop_send", tbl: "tidb_cop"},
		{name: "kv_request", tbl: "tidb_kv_request", label: "type"},
		{name: "kv_backoff", tbl: "tidb_kv_backoff", label: "type"},
		{name: "tso_wait", tbl: "pd_tso_wait"},
		{name: "tso_rpc", tbl: "pd_tso_rpc"},
		{name: "load_schema", tbl: "tidb_load_schema"},
		{name: "do_ddl_job", tbl: "tidb_ddl", label: "type"},
		{name: "ddl_worker", tbl: "tidb_ddl_worker", label: "action"},
		{name: "ddl_owner_handle_syncer", tbl: "tidb_owner_handle_syncer", label: "type"},
		{name: "auto_analyze", tbl: "tidb_statistics_auto_analyze"},
		{name: "auto_id_request", tbl: "tidb_auto_id_request", label: "type"},

		// PD
		{name: "pd_client_cmd", tbl: "pd_client_cmd", label: "type"},
		{name: "etcd_wal_fsync", tbl: "etcd_wal_fsync", label: "instance"},

		// TiKV
		{name: "tikv_ingest_sst", tbl: "tikv_ingest_sst", label: "db"},

		{name: "pd_grpc_completed_commands", tbl: "pd_grpc_completed_commands", label: "instance"},
		{name: "pd_handle_request", tbl: "pd_handle_request", label: "type"},
		{name: "pd_handle_requests", tbl: "pd_handle_requests", label: "type"},
		{name: "pd_handle_transactions", tbl: "pd_handle_transactions", label: "instance"},
		{name: "pd_operator_finish", tbl: "pd_operator_finish", label: "type"},
		{name: "pd_operator_step_finish", tbl: "pd_operator_step_finish", label: "type"},
		{name: "pd_peer_round_trip", tbl: "pd_peer_round_trip", label: "instance"},
		{name: "pd_start_tso_wait", tbl: "pd_start_tso_wait", label: "instance"},
		{name: "tidb_batch_client_unavailable", tbl: "tidb_batch_client_unavailable", label: "instance"},
		{name: "tidb_batch_client_wait", tbl: "tidb_batch_client_wait", label: "instance"},
		{name: "tidb_ddl_batch_add_index", tbl: "tidb_ddl_batch_add_index", label: "type"},
		{name: "tidb_ddl_deploy_syncer", tbl: "tidb_ddl_deploy_syncer", label: "type"},
		{name: "tidb_ddl_update_self_version", tbl: "tidb_ddl_update_self_version", label: "instance"},
		{name: "tidb_gc_push_task", tbl: "tidb_gc_push_task", label: "type"},
		{name: "tidb_gc", tbl: "tidb_gc", label: "instance"},
		{name: "tidb_meta_operation", tbl: "tidb_meta_operation", label: "type"},
		{name: "tidb_new_etcd_session", tbl: "tidb_new_etcd_session", label: "type"},
		{name: "tidb_slow_query_cop_wait", tbl: "tidb_slow_query_cop_wait", label: "instance"},
		{name: "tidb_transaction", tbl: "tidb_transaction", label: "type"},
		{name: "tikv_append_log", tbl: "tikv_append_log", label: "instance"},
		{name: "tikv_apply_log", tbl: "tikv_apply_log", label: "instance"},
		{name: "tikv_apply_wait", tbl: "tikv_apply_wait", label: "instance"},
		{name: "tikv_backup_range", tbl: "tikv_backup_range", label: "type"},
		{name: "tikv_backup", tbl: "tikv_backup", label: "instance"},
		{name: "tikv_check_split", tbl: "tikv_check_split", label: "instance"},
		{name: "tikv_commit_log", tbl: "tikv_commit_log", label: "instance"},
		{name: "tikv_cop_handle", tbl: "tikv_cop_handle", label: "instance"},
		{name: "tikv_cop_request", tbl: "tikv_cop_request", label: "instance"},
		{name: "tikv_cop_wait", tbl: "tikv_cop_wait", label: "instance"},
		{name: "tikv_gc_tasks", tbl: "tikv_gc_tasks", label: "instance"},
		{name: "tikv_grpc_messge", tbl: "tikv_grpc_messge", label: "type"},
		{name: "tikv_handle_snapshot", tbl: "tikv_handle_snapshot", label: "type"},
		{name: "tikv_lock_manager_deadlock_detect", tbl: "tikv_lock_manager_deadlock_detect", label: "instance"},
		{name: "tikv_lock_manager_waiter_lifetime", tbl: "tikv_lock_manager_waiter_lifetime", label: "instance"},
		{name: "tikv_process", tbl: "tikv_process", label: "type"},
		{name: "tikv_propose_wait", tbl: "tikv_propose_wait", label: "instance"},
		{name: "tikv_raft_store_events", tbl: "tikv_raft_store_events", label: "type"},
		{name: "tikv_scheduler_command", tbl: "tikv_scheduler_command", label: "type"},
		{name: "tikv_scheduler_latch_wait", tbl: "tikv_scheduler_latch_wait", label: "type"},
		{name: "tikv_send_snapshot", tbl: "tikv_send_snapshot", label: "instance"},
		{name: "tikv_storage_async_request", tbl: "tikv_storage_async_request", label: "type"},
	}

	specialHandle := func(row []types.Datum, tps []*types.FieldType) []types.Datum {
		name := row[0].GetString()
		switch name {
		case "get_token(us)":
			if row[3].IsNull() {
				return row
			}
			v := row[3].GetFloat64()
			row[3].SetFloat64(v / 10e5)
			for i := 5; i < len(row); i++ {
				v := row[i].GetFloat64()
				row[i].SetFloat64(v / 10e5)
			}
			row[0].SetString(name[:len(name)-4], tps[0].Collate, tps[0].Flen)
		}
		return row
	}

	e.retrieved = true
	totalRows := make([][]types.Datum, 0, len(totalTimeMetrics))
	tps := make([]*types.FieldType, 0, len(e.table.Columns))
	for _, col := range e.table.Columns {
		tps = append(tps, &col.FieldType)
	}
	quantiles := []float64{0.999, 0.99, 0.90, 0.80}
	startTime := e.extractor.StartTime.Format(plannercore.MetricTableTimeFormat)
	endTime := e.extractor.EndTime.Format(plannercore.MetricTableTimeFormat)
	fmt.Printf("%v, %v---------\n\n", startTime, endTime)
	for _, t := range totalTimeMetrics {
		sqls := []string{
			fmt.Sprintf("select '%s', min(time),'' , sum(value) from metrics_schema.%s_total_time where time >= '%s' and time < '%s'",
				t.name, t.tbl, startTime, endTime),
			fmt.Sprintf("select sum(value) from metrics_schema.%s_total_count where time >= '%s' and time < '%s'",
				t.tbl, startTime, endTime),
		}
		for _, quantile := range quantiles {
			sql := fmt.Sprintf("select max(value) as max_value from metrics_schema.%s_duration where time >= '%s' and time < '%s' and quantile=%f",
				t.tbl, startTime, endTime, quantile)
			sqls = append(sqls, sql)
		}
		row := make([]types.Datum, 0, len(e.table.Columns))
		tpIdx := 0
		for _, sql := range sqls {
			rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
			if err != nil {
				sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", sql, err))
				break
			}
			if len(rows) != 1 {
				sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' return %v rows, should never hapen", sql, len(rows)))
				break
			}
			row = append(row, rows[0].GetDatumRow(tps[tpIdx:tpIdx+rows[0].Len()])...)
			tpIdx += rows[0].Len()
		}
		if len(row) != len(tps) {
			continue
		}
		row = specialHandle(row, tps)
		totalRows = append(totalRows, row)
		if len(t.label) == 0 {
			continue
		}
		joinSql := "select t0.*,t1.count"
		sqls = []string{
			fmt.Sprintf("select '%[1]s', min(time), `%[5]s` , sum(value) as total from metrics_schema.%[2]s_total_time where time >= '%[3]s' and time < '%[4]s' group by `%[5]s`",
				t.name, t.tbl, startTime, endTime, t.label),
			fmt.Sprintf("select `%[4]s`, sum(value) as count from metrics_schema.%[1]s_total_count where time >= '%[2]s' and time < '%[3]s' group by `%[4]s`",
				t.tbl, startTime, endTime, t.label),
		}
		for i, quantile := range quantiles {
			sql := fmt.Sprintf("select `%[5]s`, max(value) as max_value from metrics_schema.%[1]s_duration where time >= '%[2]s' and time < '%[3]s' and quantile=%[4]f group by `%[5]s`",
				t.tbl, startTime, endTime, quantile, t.label)
			sqls = append(sqls, sql)
			joinSql += fmt.Sprintf(",t%v.max_value", i+2)
		}
		joinSql += " from "
		for i, sql := range sqls {
			joinSql += fmt.Sprintf(" (%s) as t%v ", sql, i)
			if i != len(sqls)-1 {
				joinSql += "join "
			}
		}
		joinSql += " where "
		for i := 0; i < len(sqls)-1; i++ {
			if i > 0 {
				joinSql += "and "
			}
			joinSql += fmt.Sprintf(" t%v.%s = t%v.%s ", i, t.label, i+1, t.label)
		}
		joinSql += " order by t0.total desc"

		rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(joinSql)
		if err != nil {
			fmt.Printf("%v\n-------------\n", joinSql)
			sctx.GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", joinSql, err))
			continue
		}
		if len(rows) == 0 {
			continue
		}
		for i := range rows {
			row := rows[i].GetDatumRow(tps)
			if len(row) != len(tps) {
				continue
			}
			row = specialHandle(row, tps)
			totalRows = append(totalRows, row)
		}
	}
	return totalRows, nil
}
