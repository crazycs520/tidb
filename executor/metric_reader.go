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
	record = append(record, types.NewStringDatum(e.tblDef.Comment))
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

// MetricSummaryRetriever uses to read metric data.
type MetricSummaryRetriever struct {
	dummyCloser
	table     *model.TableInfo
	extractor *plannercore.MetricTableExtractor
	retrieved bool
}

func (e *MetricSummaryRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	totalRows := make([][]types.Datum, 0, len(infoschema.MetricTableMap))
	quantiles := []float64{1, 0.999, 0.99, 0.90, 0.80}
	tps := make([]*types.FieldType, 0, len(e.table.Columns))
	for _, col := range e.table.Columns {
		tps = append(tps, &col.FieldType)
	}
	startTime := e.extractor.StartTime.Format(plannercore.MetricTableTimeFormat)
	endTime := e.extractor.EndTime.Format(plannercore.MetricTableTimeFormat)
	tables := make([]string, 0, len(infoschema.MetricTableMap))
	for name := range infoschema.MetricTableMap {
		tables = append(tables, name)
	}
	sort.Strings(tables)
	for _, name := range tables {
		def := infoschema.MetricTableMap[name]
		sqls := e.genMetricQuerySQLS(name, startTime, endTime, def.Quantile, quantiles)
		for _, sql := range sqls {
			rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
			if err != nil {
				return nil, errors.Trace(err)
			}
			for _, row := range rows {
				totalRows = append(totalRows, row.GetDatumRow(tps))
			}
		}
	}
	return totalRows, nil
}

func (e *MetricSummaryRetriever) genMetricQuerySQLS(name, startTime, endTime string, quantile float64, quantiles []float64) []string {
	if quantile == 0 {
		sql := fmt.Sprintf(`select "%s",min(time),sum(value),avg(value),min(value),max(value),comment from metric_schema.%s where time > '%s' and time < '%s'`, name, name, startTime, endTime)
		return []string{sql}
	}
	sqls := []string{}
	for _, quantile := range quantiles {
		sql := fmt.Sprintf(`select "%s_%v",min(time),sum(value),avg(value),min(value),max(value),comment from metric_schema.%s where time > '%s' and time < '%s' and quantile=%v`, name, quantile, name, startTime, endTime, quantile)
		sqls = append(sqls, sql)
	}
	return sqls
}

// MetricSummaryByLabelRetriever uses to read metric detail data.
type MetricSummaryByLabelRetriever struct {
	dummyCloser
	table     *model.TableInfo
	extractor *plannercore.MetricTableExtractor
	retrieved bool
}

func (e *MetricSummaryByLabelRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	totalRows := make([][]types.Datum, 0, len(infoschema.MetricTableMap))
	quantiles := []float64{1, 0.999, 0.99, 0.90, 0.80}
	tps := make([]*types.FieldType, 0, len(e.table.Columns))
	for _, col := range e.table.Columns {
		tps = append(tps, &col.FieldType)
	}
	startTime := e.extractor.StartTime.Format(plannercore.MetricTableTimeFormat)
	endTime := e.extractor.EndTime.Format(plannercore.MetricTableTimeFormat)
	tables := make([]string, 0, len(infoschema.MetricTableMap))
	for name := range infoschema.MetricTableMap {
		tables = append(tables, name)
	}
	sort.Strings(tables)
	for _, name := range tables {
		def := infoschema.MetricTableMap[name]
		sqls := e.genMetricQuerySQLS(name, startTime, endTime, quantiles, def)
		for _, sql := range sqls {
			rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
			if err != nil {
				return nil, errors.Trace(err)
			}
			for _, row := range rows {
				totalRows = append(totalRows, row.GetDatumRow(tps))
			}
		}
	}
	return totalRows, nil
}

func (e *MetricSummaryByLabelRetriever) genMetricQuerySQLS(name, startTime, endTime string, quantiles []float64, def infoschema.MetricTableDef) []string {
	labels := ""
	labelsColumn := `""`
	if len(def.Labels) > 0 {
		labels = "`" + strings.Join(def.Labels, "`, `") + "`"
		labelsColumn = fmt.Sprintf("concat_ws(' = ', '%s', concat_ws(', ', %s))", strings.Join(def.Labels, ", "), labels)
	}
	if def.Quantile == 0 {
		quantiles = []float64{0}
	}
	sqls := []string{}
	for _, quantile := range quantiles {
		var sql string
		if quantile == 0 {
			sql = fmt.Sprintf(`select "%[1]s", %[2]s as label,min(time),sum(value),avg(value),min(value),max(value),comment from metric_schema.%[1]s where time > '%[3]s' and time < '%[4]s'`,
				name, labelsColumn, startTime, endTime)
		} else {
			sql = fmt.Sprintf(`select "%[1]s_%[5]v", %[2]s as label,min(time),sum(value),avg(value),min(value),max(value),comment from metric_schema.%[1]s where time > '%[3]s' and time < '%[4]s' and quantile=%[5]v`,
				name, labelsColumn, startTime, endTime, quantile)
		}
		if len(def.Labels) > 0 {
			sql += " group by " + labels
		}
		sqls = append(sqls, sql)
	}
	return sqls
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
		name string
		tbl  string
	}{
		{name: "user_query", tbl: "tidb_query"},
		{name: "get_token(us)", tbl: "tidb_get_token"},
		{name: "parse", tbl: "tidb_parse"},
		{name: "compile", tbl: "tidb_compile"},
		{name: "distsql_execution", tbl: "tidb_distsql_execution"},
		{name: "cop_send", tbl: "tidb_cop"},
		{name: "kv_request", tbl: "tidb_kv_request"},
		{name: "kv_backoff", tbl: "tidb_kv_backoff"},
		{name: "tso_wait", tbl: "pd_tso_wait"},
		{name: "tso_rpc", tbl: "pd_tso_rpc"},
		{name: "load_schema", tbl: "tidb_load_schema"},
		{name: "do_ddl_job", tbl: "tidb_ddl"},
		{name: "ddl_worker", tbl: "tidb_ddl_worker"},
		{name: "ddl_owner_handle_syncer", tbl: "tidb_owner_handle_syncer"},
		{name: "auto_analyze", tbl: "tidb_statistics_auto_analyze"},
		{name: "auto_id_request", tbl: "tidb_auto_id_request"},
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
	for _, t := range totalTimeMetrics {
		sqls := []string{
			fmt.Sprintf("select '%s', min(time), sum(value) from metric_schema.%s_total_time where time > '%s' and time < '%s'",
				t.name, t.tbl, startTime, endTime),
			fmt.Sprintf("select sum(value) from metric_schema.%s_total_count where time > '%s' and time < '%s'",
				t.tbl, startTime, endTime),
		}
		for _, quantile := range quantiles {
			sql := fmt.Sprintf("select max(value) as max_value from metric_schema.%s_duration where time > '%s' and time < '%s' and quantile=%f",
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
		if len(row) == len(tps) {
			totalRows = append(totalRows, row)
		}
	}
	return totalRows, nil
}
