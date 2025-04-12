package querycache

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/assert"
)

func TestQueryCache(t *testing.T) {
	q1 := &QueryCacheKey{
		SchemaName: "test1",
		Sql:        "select * from t where a = ?",
		Args: []param.BinaryParam{
			{
				Tp:         1,
				IsUnsigned: true,
				IsNull:     false,
				Val:        []byte{1, 2, 3},
			},
		},
		Vars: QueryVars{
			TimeZone: time.Local,
			SQLMode:  mysql.SetSQLMode(mysql.SQLMode(0), mysql.ModeMsSQL),
		},
	}
	assert.Equal(t, q1.hashSize(), len(q1.Hash()))
	value := GlobalQueryCache.GetQueryCache(q1)
	assert.Nil(t, value)

	GlobalQueryCache.AddQueryCache(q1, &QueryCacheValue{
		ReadTs: 1,
	})

	value = GlobalQueryCache.GetQueryCache(q1)
	assert.NotNil(t, value)
	assert.Equal(t, value.ReadTs, uint64(1))

	q1.Vars.SQLMode = mysql.SetSQLMode(mysql.SQLMode(0), mysql.ModeDb2)
	q1.hash = nil
	value = GlobalQueryCache.GetQueryCache(q1)
	assert.Nil(t, value)
}
