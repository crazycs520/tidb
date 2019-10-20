package infoschema

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"strings"
)

const clusterTableSuffix = "_CLUSTER"

// Cluster table list.
const (
	clusterTableSlowLog     = tableSlowLog + clusterTableSuffix
	clusterTableProcesslist = tableProcesslist + clusterTableSuffix
)

// cluster table columns
var (
	clusterSlowQueryCols   []columnInfo
	clusterProcesslistCols []columnInfo
)

// register for cluster memory tables;
var clusterTableMap = map[string]struct{}{
	clusterTableSlowLog:     {},
	clusterTableProcesslist: {},
}

var clusterTableCols = []columnInfo{
	{"TiDB_ID", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
}

func init() {
	// Slow query
	clusterSlowQueryCols = append(clusterSlowQueryCols, slowQueryCols...)
	clusterSlowQueryCols = append(clusterSlowQueryCols, clusterTableCols...)
	// ProcessList
	clusterProcesslistCols = append(clusterProcesslistCols, tableProcesslistCols...)
	clusterProcesslistCols = append(clusterProcesslistCols, clusterTableCols...)

	// Register information_schema tables.
	tableNameToColumns[clusterTableSlowLog] = clusterSlowQueryCols
	tableNameToColumns[clusterTableProcesslist] = clusterProcesslistCols
}

func IsClusterTable(tableName string) bool {
	tableName = strings.ToUpper(tableName)
	_, ok := clusterTableMap[tableName]
	return ok
}

func dataForClusterSlowLog(ctx sessionctx.Context) ([][]types.Datum, error) {
	rows, err := dataForSlowLog(ctx)
	if err != nil {
		return nil, err
	}

	return appendClusterColumnsToRows(rows), nil
}

func dataForClusterProcesslist(ctx sessionctx.Context) ([][]types.Datum, error) {
	rows := dataForProcesslist(ctx)
	return appendClusterColumnsToRows(rows), nil
}

func appendClusterColumnsToRows(rows [][]types.Datum) [][]types.Datum {
	for i := range rows {
		rows[i] = append(rows[i], types.NewUintDatum(uint64(infosync.GetGlobalServerID())))
	}
	return rows
}
