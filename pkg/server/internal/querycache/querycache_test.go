package querycache

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/assert"
)

func TestQueryCache(t *testing.T) {
	q1 := &QueryCacheKey{
		schemaName: "test1",
		sql:        "select * from t",
		args:       nil,
		vars: QueryVars{
			TimeZone: time.Local,
			SQLMode:  mysql.SetSQLMode(mysql.SQLMode(0), mysql.ModeMsSQL),
		},
	}
	value := GlobalQueryCache.GetQueryCache(q1)
	assert.Nil(t, value)

	GlobalQueryCache.AddQueryCache(q1, &QueryCacheValue{
		ReadTs: 1,
	})

	value = GlobalQueryCache.GetQueryCache(q1)
	assert.NotNil(t, value)
	assert.Equal(t, value.ReadTs, int64(1))
}
