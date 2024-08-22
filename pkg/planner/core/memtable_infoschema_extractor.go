// Copyright 2024 PingCAP, Inc.
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

package core

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/set"
	"golang.org/x/exp/maps"
)

// columnName is the type for column names used by tables information_schema.
type columnName string
type extractableCols struct {
	schema      columnName
	table       columnName
	tableID     columnName
	partitionID columnName

	partitionName columnName
	indexName     columnName
	columnName    columnName
	constrName    columnName
	constrSchema  columnName
}

const (
	_tableSchema      columnName = "table_schema"
	_tableName        columnName = "table_name"
	_tidbTableID      columnName = "tidb_table_id"
	_partitionName    columnName = "partition_name"
	_tidbPartitionID  columnName = "tidb_partition_id"
	_indexName        columnName = "index_name"
	_schemaName       columnName = "schema_name"
	_constraintSchema columnName = "constraint_schema"
	_constraintName   columnName = "constraint_name"
	_tableID          columnName = "table_id"
	_sequenceSchema   columnName = "sequence_schema"
	_sequenceName     columnName = "sequence_name"
	_columnName       columnName = "column_name"
)

const (
	primaryKeyName string = "primary"
)

// InfoSchemaBaseExtractor is used to extract infoSchema tables related predicates.
type InfoSchemaBaseExtractor struct {
	extractHelper
	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest   bool
	ColPredicates map[string]set.StringSet
	// columns occurs in predicate will be extracted.
	colNames []columnName

	extractableColumns extractableCols
}

// GetBase is only used for test.
func (e *InfoSchemaBaseExtractor) GetBase() *InfoSchemaBaseExtractor {
	return e
}

// ListSchemas lists related schemas from predicate.
func (e *InfoSchemaBaseExtractor) ListSchemas(is infoschema.InfoSchema) []model.CIStr {
	ec := e.extractableColumns
	schemas := e.getSchemaObjectNames(ec.schema)
	if len(schemas) == 0 {
		ret := is.AllSchemaNames()
		slices.SortFunc(ret, func(a, b model.CIStr) int {
			return strings.Compare(a.L, b.L)
		})
		return ret
	}
	ret := schemas[:0]
	for _, s := range schemas {
		if n, ok := is.SchemaByName(s); ok {
			ret = append(ret, n.Name)
		}
	}
	return ret
}

// ListSchemasAndTables lists related tables and their corresponding schemas from predicate.
// If there is no error, returning schema slice and table slice are guaranteed to have the same length.
func (e *InfoSchemaBaseExtractor) ListSchemasAndTables(
	ctx context.Context,
	is infoschema.InfoSchema,
) ([]model.CIStr, []*model.TableInfo, error) {
	ec := e.extractableColumns
	schemas := e.ListSchemas(is)
	var tableNames []model.CIStr
	if ec.table != "" {
		tableNames = e.getSchemaObjectNames(ec.table)
	}
	var tableIDs []model.CIStr
	if ec.tableID != "" {
		tableIDs = e.getSchemaObjectNames(ec.tableID)
		if len(tableIDs) > 0 {
			tableMap := make(map[int64]*model.TableInfo, len(tableIDs))
			findTablesByID(is, tableIDs, tableNames, tableMap)
			return findSchemasForTables(ctx, is, schemas, maps.Values(tableMap))
		}
	}
	if ec.partitionID != "" {
		partIDs := e.getSchemaObjectNames(ec.partitionID)
		if len(partIDs) > 0 {
			tableMap := make(map[int64]*model.TableInfo, len(partIDs))
			findTablesByPartID(is, partIDs, tableNames, tableMap)
			return findSchemasForTables(ctx, is, schemas, maps.Values(tableMap))
		}
	}
	if len(tableNames) > 0 {
		return findTableAndSchemaByName(ctx, is, schemas, tableNames)
	}
	return listTablesForEachSchema(ctx, is, schemas)
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *InfoSchemaBaseExtractor) Extract(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	var resultSet, resultSet1 set.StringSet
	e.ColPredicates = make(map[string]set.StringSet)
	remained = predicates
	for _, colName := range e.colNames {
		colName := string(colName)
		remained, e.SkipRequest, resultSet = e.extractColWithLower(ctx, schema, names, remained, colName)
		if e.SkipRequest {
			break
		}
		remained, e.SkipRequest, resultSet1 = e.extractCol(ctx, schema, names, remained, colName, true)
		if e.SkipRequest {
			break
		}
		for elt := range resultSet1 {
			resultSet.Insert(elt)
		}
		if len(resultSet) == 0 {
			continue
		}
		e.ColPredicates[colName] = resultSet
	}
	return remained
}

// ExplainInfo implements base.MemTablePredicateExtractor interface.
func (e *InfoSchemaBaseExtractor) ExplainInfo(_ base.PhysicalPlan) string {
	if e.SkipRequest {
		return "skip_request:true"
	}

	r := new(bytes.Buffer)
	colNames := maps.Keys(e.ColPredicates)
	sort.Strings(colNames)
	for _, colName := range colNames {
		if len(e.ColPredicates[colName]) > 0 {
			fmt.Fprintf(r, "%s:[%s], ", colName, extractStringFromStringSet(e.ColPredicates[colName]))
		}
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// Filter use the col predicates to filter records.
// Return true if the underlying row does not match predicate,
// then it should be filtered and not shown in the result.
func (e *InfoSchemaBaseExtractor) filter(colName columnName, val string) bool {
	if e.SkipRequest {
		return true
	}
	predVals, ok := e.ColPredicates[string(colName)]
	if ok && len(predVals) > 0 {
		lower, ok := e.isLower[string(colName)]
		if ok {
			var valStr string
			// only have varchar string type, safe to do that.
			if lower {
				valStr = strings.ToLower(val)
			} else {
				valStr = strings.ToUpper(val)
			}
			return !predVals.Exist(valStr)
		}
		return !predVals.Exist(val)
	}
	// No need to filter records since no predicate for the column exists.
	return false
}

// InfoSchemaIndexesExtractor is the predicate extractor for information_schema.tidb_indexes.
type InfoSchemaIndexesExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaIndexesExtractor creates a new InfoSchemaIndexesExtractor.
func NewInfoSchemaIndexesExtractor() *InfoSchemaIndexesExtractor {
	e := &InfoSchemaIndexesExtractor{}
	e.extractableColumns = extractableCols{
		schema: _tableSchema,
		table:  _tableName,
	}
	e.colNames = []columnName{_tableSchema, _tableName}
	return e
}

// InfoSchemaTablesExtractor is the predicate extractor for information_schema.tables.
type InfoSchemaTablesExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaTablesExtractor creates a new InfoSchemaTablesExtractor.
func NewInfoSchemaTablesExtractor() *InfoSchemaTablesExtractor {
	e := &InfoSchemaTablesExtractor{}
	e.extractableColumns = extractableCols{
		schema:  _tableSchema,
		table:   _tableName,
		tableID: _tidbTableID,
	}
	e.colNames = []columnName{_tableSchema, _tableName, _tidbTableID}
	return e
}

// InfoSchemaViewsExtractor is the predicate extractor for information_schema.views.
type InfoSchemaViewsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaViewsExtractor creates a new InfoSchemaViewsExtractor.
func NewInfoSchemaViewsExtractor() *InfoSchemaViewsExtractor {
	e := &InfoSchemaViewsExtractor{}
	e.extractableColumns = extractableCols{
		schema: _tableSchema,
		table:  _tableName,
	}
	e.colNames = []columnName{_tableSchema, _tableName}
	return e
}

// InfoSchemaKeyColumnUsageExtractor is the predicate extractor for information_schema.key_column_usage.
type InfoSchemaKeyColumnUsageExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaKeyColumnUsageExtractor creates a new InfoSchemaKeyColumnUsageExtractor.
func NewInfoSchemaKeyColumnUsageExtractor() *InfoSchemaKeyColumnUsageExtractor {
	e := &InfoSchemaKeyColumnUsageExtractor{}
	e.extractableColumns = extractableCols{
		schema:       _tableSchema,
		table:        _tableName,
		constrName:   _constraintName,
		constrSchema: _constraintSchema,
	}
	e.colNames = []columnName{_tableSchema, _tableName, _constraintName, _constraintSchema}
	return e
}

// HasConstraint returns true if constraint name is specified in predicates.
func (e *InfoSchemaKeyColumnUsageExtractor) HasConstraint(name string) bool {
	return !e.filter(_constraintName, name)
}

// HasPrimaryKey returns true if primary key is specified in predicates.
func (e *InfoSchemaKeyColumnUsageExtractor) HasPrimaryKey() bool {
	return !e.filter(_constraintName, primaryKeyName)
}

// HasConstraintSchema returns true if constraint schema is specified in predicates.
func (e *InfoSchemaKeyColumnUsageExtractor) HasConstraintSchema(name string) bool {
	return !e.filter(_constraintSchema, name)
}

// InfoSchemaTableConstraintsExtractor is the predicate extractor for information_schema.constraints.
type InfoSchemaTableConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaTableConstraintsExtractor creates a new InfoSchemaTableConstraintsExtractor.
func NewInfoSchemaTableConstraintsExtractor() *InfoSchemaTableConstraintsExtractor {
	e := &InfoSchemaTableConstraintsExtractor{}
	e.extractableColumns = extractableCols{
		schema:       _tableSchema,
		table:        _tableName,
		constrName:   _constraintName,
		constrSchema: _constraintSchema,
	}
	e.colNames = []columnName{_tableSchema, _tableName, _constraintName, _constraintSchema}
	return e
}

// HasConstraintSchema returns true if constraint schema is specified in predicates.
func (e *InfoSchemaTableConstraintsExtractor) HasConstraintSchema(name string) bool {
	return !e.filter(_constraintSchema, name)
}

// HasConstraint returns true if constraint is specified in predicates.
func (e *InfoSchemaTableConstraintsExtractor) HasConstraint(name string) bool {
	return !e.filter(_constraintName, name)
}

// HasPrimaryKey returns true if primary key is specified in predicates.
func (e *InfoSchemaTableConstraintsExtractor) HasPrimaryKey() bool {
	return !e.filter(_constraintName, primaryKeyName)
}

// InfoSchemaPartitionsExtractor is the predicate extractor for information_schema.partitions.
type InfoSchemaPartitionsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaPartitionsExtractor creates a new InfoSchemaPartitionsExtractor.
func NewInfoSchemaPartitionsExtractor() *InfoSchemaPartitionsExtractor {
	e := &InfoSchemaPartitionsExtractor{}
	e.extractableColumns = extractableCols{
		schema:        _tableSchema,
		table:         _tableName,
		partitionID:   _tidbPartitionID,
		partitionName: _partitionName,
	}
	e.colNames = []columnName{_tableSchema, _tableName, _tidbPartitionID, _partitionName}
	return e
}

// HasPartition returns true if partition name is specified in predicates.
func (e *InfoSchemaPartitionsExtractor) HasPartition(name string) bool {
	return !e.filter(_partitionName, name)
}

// InfoSchemaStatisticsExtractor is the predicate extractor for  information_schema.statistics.
type InfoSchemaStatisticsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaStatisticsExtractor creates a new InfoSchemaStatisticsExtractor.
func NewInfoSchemaStatisticsExtractor() *InfoSchemaStatisticsExtractor {
	e := &InfoSchemaStatisticsExtractor{}
	e.extractableColumns = extractableCols{
		schema:    _tableSchema,
		table:     _tableName,
		indexName: _indexName,
	}
	e.colNames = []columnName{_tableSchema, _tableName, _indexName}
	return e
}

// HasIndex returns true if index name is specified in predicates.
func (e *InfoSchemaStatisticsExtractor) HasIndex(val string) bool {
	return !e.filter(_indexName, val)
}

// HasPrimaryKey returns true if primary key is specified in predicates.
func (e *InfoSchemaStatisticsExtractor) HasPrimaryKey() bool {
	return !e.filter(_indexName, primaryKeyName)
}

// InfoSchemaSchemataExtractor is the predicate extractor for information_schema.schemata.
type InfoSchemaSchemataExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaSchemataExtractor creates a new InfoSchemaSchemataExtractor.
func NewInfoSchemaSchemataExtractor() *InfoSchemaSchemataExtractor {
	e := &InfoSchemaSchemataExtractor{}
	e.extractableColumns = extractableCols{
		schema: _schemaName,
	}
	e.colNames = []columnName{_schemaName}
	return e
}

// InfoSchemaCheckConstraintsExtractor is the predicate extractor for information_schema.check_constraints.
type InfoSchemaCheckConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaCheckConstraintsExtractor creates a new InfoSchemaCheckConstraintsExtractor.
func NewInfoSchemaCheckConstraintsExtractor() *InfoSchemaCheckConstraintsExtractor {
	e := &InfoSchemaCheckConstraintsExtractor{}
	e.extractableColumns = extractableCols{
		schema:     _constraintSchema,
		constrName: _constraintName,
	}
	e.colNames = []columnName{_constraintSchema, _constraintName}
	return e
}

// HasConstraint returns true if constraint name is specified in predicates.
func (e *InfoSchemaCheckConstraintsExtractor) HasConstraint(name string) bool {
	return !e.filter(_constraintName, name)
}

// InfoSchemaTiDBCheckConstraintsExtractor is the predicate extractor for information_schema.tidb_check_constraints.
type InfoSchemaTiDBCheckConstraintsExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaTiDBCheckConstraintsExtractor creates a new InfoSchemaTiDBCheckConstraintsExtractor.
func NewInfoSchemaTiDBCheckConstraintsExtractor() *InfoSchemaTiDBCheckConstraintsExtractor {
	e := &InfoSchemaTiDBCheckConstraintsExtractor{}
	e.extractableColumns = extractableCols{
		schema:     _constraintSchema,
		table:      _tableName,
		tableID:    _tableID,
		constrName: _constraintName,
	}
	e.colNames = []columnName{_constraintSchema, _tableName, _tableID, _constraintName}
	return e
}

// HasConstraint returns true if constraint name is specified in predicates.
func (e *InfoSchemaTiDBCheckConstraintsExtractor) HasConstraint(name string) bool {
	return !e.filter(_constraintName, name)
}

// InfoSchemaReferConstExtractor is the predicate extractor for information_schema.referential_constraints.
type InfoSchemaReferConstExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaReferConstExtractor creates a new InfoSchemaReferConstExtractor.
func NewInfoSchemaReferConstExtractor() *InfoSchemaReferConstExtractor {
	e := &InfoSchemaReferConstExtractor{}
	e.extractableColumns = extractableCols{
		schema:     _constraintSchema,
		table:      _tableName,
		constrName: _constraintName,
	}
	e.colNames = []columnName{_constraintSchema, _tableName, _constraintName}
	return e
}

// HasConstraint returns true if constraint name is specified in predicates.
func (e *InfoSchemaReferConstExtractor) HasConstraint(name string) bool {
	return !e.filter(_constraintName, name)
}

// InfoSchemaSequenceExtractor is the predicate extractor for information_schema.sequences.
type InfoSchemaSequenceExtractor struct {
	InfoSchemaBaseExtractor
}

// NewInfoSchemaSequenceExtractor creates a new InfoSchemaSequenceExtractor.
func NewInfoSchemaSequenceExtractor() *InfoSchemaSequenceExtractor {
	e := &InfoSchemaSequenceExtractor{}
	e.extractableColumns = extractableCols{
		schema: _sequenceSchema,
		table:  _sequenceName,
	}
	e.colNames = []columnName{_sequenceSchema, _sequenceName}
	return e
}

// findTablesByID finds tables by table IDs and append them to table map.
func findTablesByID(
	is infoschema.InfoSchema,
	tableIDs []model.CIStr,
	tableNames []model.CIStr,
	tables map[int64]*model.TableInfo,
) {
	tblNameMap := make(map[string]struct{}, len(tableNames))
	for _, n := range tableNames {
		tblNameMap[n.L] = struct{}{}
	}
	for _, tid := range parseIDs(tableIDs) {
		tbl, ok := is.TableByID(context.Background(), tid)
		if !ok {
			continue
		}
		tblInfo := tbl.Meta()
		if tblInfo.TempTableType == model.TempTableLocal {
			continue
		}
		if len(tableNames) > 0 {
			if _, found := tblNameMap[tblInfo.Name.L]; !found {
				// table_id does not match table_name, skip it.
				continue
			}
		}
		tables[tblInfo.ID] = tblInfo
	}
}

// findTablesByPartID finds tables by partition IDs and append them to table map.
func findTablesByPartID(
	is infoschema.InfoSchema,
	partIDs []model.CIStr,
	tableNames []model.CIStr,
	tables map[int64]*model.TableInfo,
) {
	tblNameMap := make(map[string]struct{}, len(tableNames))
	for _, n := range tableNames {
		tblNameMap[n.L] = struct{}{}
	}
	for _, pid := range parseIDs(partIDs) {
		tbl, _, _ := is.FindTableByPartitionID(pid)
		if tbl == nil {
			continue
		}
		if len(tableNames) > 0 {
			if _, found := tblNameMap[tbl.Meta().Name.L]; !found {
				// partition_id does not match table_name, skip it.
				continue
			}
		}
		tblInfo := tbl.Meta()
		tables[tblInfo.ID] = tblInfo
	}
}

func findTableAndSchemaByName(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemas []model.CIStr,
	tableNames []model.CIStr,
) ([]model.CIStr, []*model.TableInfo, error) {
	type schemaAndTable struct {
		schema model.CIStr
		table  *model.TableInfo
	}
	tableMap := make(map[int64]schemaAndTable, len(tableNames))
	ctx = infoschema.WithRefillOption(ctx, false)
	for _, n := range tableNames {
		for _, s := range schemas {
			tbl, err := is.TableByName(ctx, s, n)
			if err != nil {
				if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
					continue
				}
				return nil, nil, errors.Trace(err)
			}
			tblInfo := tbl.Meta()
			if tblInfo.TempTableType == model.TempTableLocal {
				continue
			}
			tableMap[tblInfo.ID] = schemaAndTable{s, tblInfo}
		}
	}
	schemaSlice := make([]model.CIStr, 0, len(tableMap))
	tableSlice := make([]*model.TableInfo, 0, len(tableMap))
	for _, st := range tableMap {
		schemaSlice = append(schemaSlice, st.schema)
		tableSlice = append(tableSlice, st.table)
	}
	return schemaSlice, tableSlice, nil
}

func listTablesForEachSchema(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemas []model.CIStr,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemaSlice := make([]model.CIStr, 0, 8)
	tableSlice := make([]*model.TableInfo, 0, 8)
	for _, s := range schemas {
		tables, err := is.SchemaTableInfos(ctx, s)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for _, t := range tables {
			schemaSlice = append(schemaSlice, s)
			tableSlice = append(tableSlice, t)
		}
	}
	return schemaSlice, tableSlice, nil
}

func findSchemasForTables(
	ctx context.Context,
	is infoschema.InfoSchema,
	schemas []model.CIStr,
	tableSlice []*model.TableInfo,
) ([]model.CIStr, []*model.TableInfo, error) {
	schemaSlice := make([]model.CIStr, 0, len(tableSlice))
	for i, tbl := range tableSlice {
		found := false
		for _, s := range schemas {
			isTbl, err := is.TableByName(ctx, s, tbl.Name)
			if err != nil {
				if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
					continue
				}
				return nil, nil, errors.Trace(err)
			}
			if isTbl.Meta().ID == tbl.ID {
				schemaSlice = append(schemaSlice, s)
				found = true
				break
			}
		}
		if !found {
			tableSlice[i] = nil
		}
	}
	// Remove nil elements in tableSlice.
	remains := tableSlice[:0]
	for _, tbl := range tableSlice {
		if tbl != nil {
			remains = append(remains, tbl)
		}
	}
	return schemaSlice, remains, nil
}

func parseIDs(ids []model.CIStr) []int64 {
	tableIDs := make([]int64, 0, len(ids))
	for _, s := range ids {
		v, err := strconv.ParseInt(s.L, 10, 64)
		if err != nil {
			continue
		}
		tableIDs = append(tableIDs, v)
	}
	slices.Sort(tableIDs)
	return tableIDs
}

// getSchemaObjectNames gets the schema object names specified in predicate of given column name.
func (e *InfoSchemaBaseExtractor) getSchemaObjectNames(colName columnName) []model.CIStr {
	predVals, ok := e.ColPredicates[string(colName)]
	if ok && len(predVals) > 0 {
		tableNames := make([]model.CIStr, 0, len(predVals))
		predVals.IterateWith(func(n string) {
			tableNames = append(tableNames, model.NewCIStr(n))
		})
		slices.SortFunc(tableNames, func(a, b model.CIStr) int {
			return strings.Compare(a.L, b.L)
		})
		return tableNames
	}
	return nil
}

// InfoSchemaTableNameExtractor is a base struct to list matching schemas and tables in predicates,
// so there is no need to call `Filter` for returns from `ListSchemas` and `ListTables`.
// But for other columns, Subclass **must** reimplement `Filter` method to use like operators for filtering.
// Currently, table_id is not taken into consideration.
type InfoSchemaTableNameExtractor struct {
	InfoSchemaBaseExtractor

	listTableFunc func(
		ctx context.Context,
		s model.CIStr,
		is infoschema.InfoSchema,
	) ([]*model.TableInfo, error)

	// table names from predicate, used by `ListTables`
	tableNames []model.CIStr

	// all predicates in lower case
	colsPredLower map[string]set.StringSet

	// all built regexp in predicates
	colsRegexp map[string][]collate.WildcardPattern

	// used for EXPLAIN only
	LikePatterns map[string][]string
}

// Extract all names and like operators in predicates
func (e *InfoSchemaTableNameExtractor) Extract(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained := e.InfoSchemaBaseExtractor.Extract(ctx, schema, names, predicates)
	if e.SkipRequest {
		return remained
	}

	e.LikePatterns = make(map[string][]string, len(e.colNames))
	e.colsRegexp = make(map[string][]collate.WildcardPattern, len(e.colNames))
	e.colsPredLower = make(map[string]set.StringSet, len(e.colNames))
	var likePatterns []string
	for _, colName := range e.colNames {
		colName := string(colName)
		remained, likePatterns = e.extractLikePatternCol(ctx, schema, names, remained, colName, true, false)
		regexp := make([]collate.WildcardPattern, len(likePatterns))
		predColLower := set.StringSet{}
		for i, pattern := range likePatterns {
			regexp[i] = collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
			regexp[i].Compile(pattern, byte('\\'))
		}
		if vals, ok := e.ColPredicates[colName]; ok {
			vals.IterateWith(func(n string) {
				predColLower.Insert(strings.ToLower(n))
			})
		}
		e.colsPredLower[colName] = predColLower
		e.LikePatterns[colName] = likePatterns
		e.colsRegexp[colName] = regexp
	}

	return remained
}

// getPredicates gets all names and regexps related to given column names.
func (e *InfoSchemaTableNameExtractor) getPredicates(colNames ...columnName) (
	set.StringSet, []collate.WildcardPattern, bool) {
	filters := set.StringSet{}
	regexp := []collate.WildcardPattern{}
	hasPredicates := false

	// Extract all filters and like patterns
	for _, col := range colNames {
		if rs, ok := e.colsRegexp[string(col)]; ok && len(rs) > 0 {
			regexp = append(regexp, rs...)
		}
		if f, ok := e.colsPredLower[string(col)]; ok && len(f) > 0 {
			if !hasPredicates {
				filters = f
				hasPredicates = true
			} else {
				filters = filters.Intersection(f)
			}
		}
	}

	return filters, regexp, hasPredicates
}

// Get all predicates related to schema extraction.
// Add more columns if necessary.
func (e *InfoSchemaTableNameExtractor) getSchemaNames() (
	set.StringSet, []collate.WildcardPattern, bool) {
	return e.getPredicates(_tableSchema, _schemaName, _constraintSchema)
}

// ListSchemas lists related schemas from predicates.
// Returned schemas is examined by like operators, so there is no need to call Filter again.
func (e *InfoSchemaTableNameExtractor) ListSchemas(
	is infoschema.InfoSchema,
) []model.CIStr {
	schemaFilters, schemaRegexp, hasPredicates := e.getSchemaNames()

	// Get all schema names
	var schemas []model.CIStr
	if hasPredicates {
		schemas = make([]model.CIStr, 0, len(schemaFilters))
		schemaFilters.IterateWith(func(n string) {
			s := model.CIStr{O: n, L: n}
			if n, ok := is.SchemaByName(s); ok {
				schemas = append(schemas, n.Name)
			}
		})
	} else {
		schemas = is.AllSchemaNames()
	}
	slices.SortFunc(schemas, func(a, b model.CIStr) int {
		return strings.Compare(a.L, b.L)
	})

	// Filter with regexp
	filteredSchemas := make([]model.CIStr, 0, len(schemas))
ForLoop:
	for _, schema := range schemas {
		for _, re := range schemaRegexp {
			if !re.DoMatch(schema.L) {
				continue ForLoop
			}
		}
		filteredSchemas = append(filteredSchemas, schema)
	}

	// TODO: add table_id here
	tableNames := e.getSchemaObjectNames(_tableName)
	e.tableNames = tableNames
	if len(tableNames) > 0 {
		e.listTableFunc = e.listSchemaTablesByName
	} else {
		e.listTableFunc = listSchemaTables
	}

	return filteredSchemas
}

// ListTables lists related tables for given schema from predicate.
// If no table found in predicate, it return all tables.
func (e *InfoSchemaTableNameExtractor) ListTables(
	ctx context.Context,
	s model.CIStr,
	is infoschema.InfoSchema,
) ([]*model.TableInfo, error) {
	allTbls, err := e.listTableFunc(ctx, s, is)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if regexp, ok := e.colsRegexp[string(_tableName)]; ok {
		tbls := make([]*model.TableInfo, 0, len(allTbls))
	ForLoop:
		for _, tbl := range allTbls {
			for _, re := range regexp {
				if !re.DoMatch(tbl.Name.L) {
					continue ForLoop
				}
			}
			tbls = append(tbls, tbl)
		}
		allTbls = tbls
	}

	return allTbls, nil
}

func (e *InfoSchemaTableNameExtractor) listSchemaTablesByName(
	ctx context.Context,
	s model.CIStr,
	is infoschema.InfoSchema,
) ([]*model.TableInfo, error) {
	tbls := make([]*model.TableInfo, 0, len(e.tableNames))
	for _, n := range e.tableNames {
		tbl, err := is.TableByName(ctx, s, n)
		if err != nil {
			if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
				continue
			}
			return nil, errors.Trace(err)
		}
		tbls = append(tbls, tbl.Meta())
	}

	return tbls, nil
}

func listSchemaTables(
	ctx context.Context,
	s model.CIStr,
	is infoschema.InfoSchema,
) ([]*model.TableInfo, error) {
	return is.SchemaTableInfos(ctx, s)
}

// ExplainInfo implements base.MemTablePredicateExtractor interface.
func (e *InfoSchemaTableNameExtractor) ExplainInfo(_ base.PhysicalPlan) string {
	if e.SkipRequest {
		return "skip_request:true"
	}

	r := new(bytes.Buffer)

	for _, colName := range e.colNames {
		if pred, ok := e.ColPredicates[string(colName)]; ok && len(pred) > 0 {
			fmt.Fprintf(r, "%s:[%s], ", colName, extractStringFromStringSet(pred))
		}
	}

	for _, colName := range e.colNames {
		if patterns, ok := e.LikePatterns[string(colName)]; ok && len(patterns) > 0 {
			fmt.Fprintf(r, "%s_pattern:[%s], ", colName, extractStringFromStringSlice(patterns))
		}
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// InfoSchemaColumnsExtractor is the predicate extractor for information_schema.columns.
type InfoSchemaColumnsExtractor struct {
	InfoSchemaTableNameExtractor
}

// NewInfoSchemaColumnsExtractor creates a new InfoSchemaColumnsExtractor.
func NewInfoSchemaColumnsExtractor() *InfoSchemaColumnsExtractor {
	e := &InfoSchemaColumnsExtractor{}
	e.extractableColumns = extractableCols{
		schema:     _tableSchema,
		table:      _tableName,
		columnName: _columnName,
	}
	e.colNames = []columnName{_tableSchema, _tableName, _columnName}
	return e
}

// ListColumns lists unhidden columns and corresponding ordinal positions for given table from predicates.
// If no column found in predicate, it return all visible columns.
func (e *InfoSchemaTableNameExtractor) ListColumns(
	tbl *model.TableInfo,
) ([]*model.ColumnInfo, []int) {
	predCol, regexp, _ := e.getPredicates(_columnName)

	columns := make([]*model.ColumnInfo, 0, len(predCol))
	ordinalPos := make([]int, 0, len(predCol))
	ord := 0
ForLoop:
	for _, column := range tbl.Columns {
		if column.Hidden {
			continue
		}
		ord++
		if len(predCol) > 0 && !predCol.Exist(column.Name.L) {
			continue
		}
		for _, re := range regexp {
			if !re.DoMatch(column.Name.L) {
				continue ForLoop
			}
		}
		columns = append(columns, column)
		ordinalPos = append(ordinalPos, ord)
	}

	return columns, ordinalPos
}

// InfoSchemaTiDBIndexUsageExtractor is the predicate extractor for information_schema.tidb_index_usage.
type InfoSchemaTiDBIndexUsageExtractor struct {
	InfoSchemaTableNameExtractor
}

// NewInfoSchemaTiDBIndexUsageExtractor creates a new InfoSchemaTiDBIndexUsageExtractor.
func NewInfoSchemaTiDBIndexUsageExtractor() *InfoSchemaTiDBIndexUsageExtractor {
	e := &InfoSchemaTiDBIndexUsageExtractor{}
	e.extractableColumns = extractableCols{
		schema:    _tableSchema,
		table:     _tableName,
		indexName: _indexName,
	}
	e.colNames = []columnName{_tableSchema, _tableName, _indexName}
	return e
}

// ListIndexes lists related indexes for given table from predicate.
// If no index found in predicate, it return all indexes.
func (e *InfoSchemaTiDBIndexUsageExtractor) ListIndexes(
	tbl *model.TableInfo,
) []*model.IndexInfo {
	predCol, regexp, _ := e.getPredicates(_indexName)
	if len(predCol) == 0 && len(regexp) == 0 {
		return tbl.Indices
	}

	indexes := make([]*model.IndexInfo, 0, len(predCol))
ForLoop:
	for _, index := range tbl.Indices {
		if len(predCol) > 0 && !predCol.Exist(index.Name.L) {
			continue
		}
		for _, re := range regexp {
			if !re.DoMatch(index.Name.L) {
				continue ForLoop
			}
		}
		indexes = append(indexes, index)
	}

	return indexes
}
