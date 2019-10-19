package infoschema

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"strings"
)

const (
	clusterTableSuffix = "_CLUSTER"

	clusterTableSlowLog = tableSlowLog + clusterTableSuffix
)

var clusterTableMap = map[string]struct{}{
	clusterTableSlowLog: {},
}

var clusterSlowQueryCols []columnInfo

var clusterTableCols = []columnInfo{
	{"TiDB_ID", mysql.TypeLonglong, 20, mysql.UnsignedFlag, nil, nil},
}

func init() {
	clusterSlowQueryCols = append(clusterSlowQueryCols, slowQueryCols...)
	clusterSlowQueryCols = append(clusterSlowQueryCols, clusterTableCols...)

	tableNameToColumns[clusterTableSlowLog] = clusterSlowQueryCols
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

	for i := range rows {
		rows[i] = append(rows[i], types.NewUintDatum(1))
	}
	return rows, nil
}
