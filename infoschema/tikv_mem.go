package infoschema

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"strings"
)

const (
	tableTiKVInfo = "TIKV_INFOS"
)

// register for tikv memory tables;
var tikvMemTableMap = map[string]struct{}{
	tableTiKVInfo: {},
}

func IsTiKVMemTable(tableName string) bool {
	tableName = strings.ToUpper(tableName)
	_, ok := tikvMemTableMap[tableName]
	return ok
}

func GetTiKVMemTableRows(tableName string) (rows [][]types.Datum, err error) {
	tableName = strings.ToUpper(tableName)
	switch tableName {
	case tableTiKVInfo:
		rows = dataForTiKVInfo()
	}
	return rows, err
}

func dataForTiKVInfo() (records [][]types.Datum) {
	records = append(records,
		types.MakeDatums(
			float64(0.1),
			float64(0.1),
			float64(0.1),
			float64(0.1),
			"hello tikv",
		),
	)
	return records
}

var tikvInfoCols = []columnInfo{
	{"CPU", mysql.TypeDouble, 22, 0, nil, nil},
	{"MEM", mysql.TypeDouble, 22, 0, nil, nil},
	{"NET", mysql.TypeDouble, 22, 0, nil, nil},
	{"DISK", mysql.TypeDouble, 22, 0, nil, nil},
	{"OTHER", mysql.TypeVarchar, 64, 0, nil, nil},
}
