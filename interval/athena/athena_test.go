package athena

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

func TestAll(t *testing.T) {
	region := "us-west-2"
	cli, err := CreateCli(region)
	require.NoError(t, err)
	//
	err = CreateDatabase(cli, "db01")
	require.NoError(t, err)
	err = CreateDatabase(cli, "db02")
	require.NoError(t, err)
	err = DropDatabase(cli, "db02")
	require.NoError(t, err)
	err = DropDatabase(cli, "db_not_exist")
	require.NoError(t, err)

	err = CreateTable(cli, "db01", "t01", "tidb-interval-partition-t1-p147", &model.TableInfo{
		Columns: []*model.ColumnInfo{
			{Name: model.NewCIStr("a"), State: model.StatePublic},
			{Name: model.NewCIStr("b"), State: model.StatePublic},
		},
	})
	require.NoError(t, err)
	err = DropDatabase(cli, "db01")
	require.Error(t, err)
	err = DropDatabaseAndAllTables(cli, "db01")
	require.NoError(t, err)

	dbs, err := GetAllDatabase(cli)
	require.NoError(t, err)
	fmt.Println("all database: ", dbs)

	tables, err := GetAllTables(cli, "test")
	require.NoError(t, err)
	fmt.Println("all tables: ", tables)
}

func TestAthenaDB(t *testing.T) {
	region := "us-west-2"
	db := NewAthenaDB(region)
	err := db.Init()
	require.NoError(t, err)
	fmt.Println(db.dbTables)
}
