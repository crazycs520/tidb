package querycache

import (
	"github.com/stretchr/testify/require"
	"log"
	"net/http"
	_ "net/http/pprof"

	"encoding/binary"
	"fmt"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/assert"
)

func TestQueryCache(t *testing.T) {
	q1 := &QueryCacheKey{
		SchemaName: "test1",
		Sql:        "select * from sbtest1 where a = ?",
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
	value, _ := GlobalQueryCache.GetQueryCache(q1)
	assert.Nil(t, value)

	GlobalQueryCache.AddQueryCache(q1, &QueryCacheValue{
		ReadTs: 1,
	})

	value, _ = GlobalQueryCache.GetQueryCache(q1)
	assert.NotNil(t, value)
	assert.Equal(t, value.ReadTs, uint64(1))

	q1.Vars.SQLMode = mysql.SetSQLMode(mysql.SQLMode(0), mysql.ModeDb2)
	q1.hash = nil
	value, _ = GlobalQueryCache.GetQueryCache(q1)
	assert.Nil(t, value)
}

func genKey(i int) *QueryCacheKey {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return &QueryCacheKey{
		SchemaName: "test1",
		Sql:        "select * from sbtest1 where a = ?",
		Args: []param.BinaryParam{
			{
				Tp:         1,
				IsUnsigned: true,
				IsNull:     false,
				Val:        buf,
			},
		},
		Vars: QueryVars{
			TimeZone: time.Local,
			SQLMode:  mysql.SetSQLMode(mysql.SQLMode(0), mysql.ModeMsSQL),
		},
	}
}

func genValue() *QueryCacheValue {
	ts := time.Now().Unix()
	colLen := 5
	fieldTypes := make([]*types.FieldType, colLen)
	resultFields := make([]*resolve.ResultField, colLen)

	for i := range fieldTypes {
		if i == 0 {
			fieldTypes[i] = types.NewFieldType(mysql.TypeVarchar)
			fieldTypes[i].SetFlen(255)
		} else {
			fieldTypes[i] = types.NewFieldType(mysql.TypeLonglong)
		}
	}

	for i := range resultFields {
		resultFields[i] = &resolve.ResultField{
			Column: &model.ColumnInfo{
				ID:        int64(i),
				Name:      pmodel.NewCIStr("col_" + strconv.Itoa(i)),
				FieldType: *fieldTypes[i],
			},
			EmptyOrgName: false,
			Table:        nil,
			DBName:       pmodel.NewCIStr("test"),
		}
	}
	chks := make([]*chunk.Chunk, 0)
	chk := chunk.New(fieldTypes, 1, 1)
	for i := range fieldTypes {
		for j := 0; j < 1; j++ {
			var d types.Datum
			switch fieldTypes[i].GetType() {
			case mysql.TypeVarchar:
				d = types.NewDatum(fmt.Sprintf("abcdefghijklmnopq-" + strconv.Itoa(j)))
			default:
				d = types.NewDatum(j)
			}
			chk.AppendDatum(i, &d)
		}
	}
	chks = append(chks, chk)
	return &QueryCacheValue{
		ReadTs:       uint64(ts),
		ResultFields: resultFields,
		FieldTypes:   fieldTypes,
		Chunks:       chks,
		ts:           ts,
	}
}

func BenchmarkQueryCache(b *testing.B) {
	cache := NewQueryCache(2000000)
	//memSize := int64(0)
	for i := 0; i < b.N; i++ {
		k := genKey(i)
		v := genValue()
		//memSize += k.MemoryUsage()
		//memSize += v.MemoryUsage()
		cache.GetQueryCache(k)
		cache.AddQueryCache(k, v)
	}
}

func TestQueryCacheMemUsage(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("127.0.0.1:8081", nil))
	}()

	cache := NewQueryCache(2000000)
	count := 1000000
	memSize := int64(0)
	for i := 0; i < count; i++ {
		k := genKey(i)
		v := genValue()
		memSize += k.MemoryUsage()
		memSize += v.MemoryUsage()
		cache.AddQueryCache(k, v)
	}
	require.Equal(t, count, cache.Len())

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("cache-mem = %v MiB\n", float64(memSize)/1024/1024)
	fmt.Printf("Alloc = %v MiB", m.Alloc/1024/1024)
	fmt.Printf("\tTotalAlloc = %v MiB", m.TotalAlloc/1024/1024)
	fmt.Printf("\tHeapInUse = %v MiB", m.HeapInuse/1024/1024)
	fmt.Printf("\tSys = %v MiB", m.Sys/1024/1024)
	fmt.Printf("\tNumGC = %v\n", m.NumGC)

	time.Sleep(time.Second * 300)
}

func TestQueryCache2(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000000; j++ {
				k := &QueryCacheKey{
					Sql: fmt.Sprintf("sbtest_id_%v", rand.Intn(100000)),
				}

				v, _ := GlobalQueryCache.GetQueryCache(k)
				if v != nil {
					continue
				}

				v = &QueryCacheValue{
					ReadTs: 2,
				}
				GlobalQueryCache.AddQueryCache(k, v)
				//require.Less(t, GlobalQueryCache.Len(), 10000+200)
			}
		}(i)
	}

	wg.Wait()
}
