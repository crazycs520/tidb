package querycache

import (
	"encoding/binary"
	"fmt"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/assert"
)

func TestQueryCache(t *testing.T) {
	q1 := &QueryCacheKey{
		StmtKey: StmtKey{
			SchemaName: "test1",
			Sql:        "select * from sbtest1 where a = ?",
			Vars: QueryVars{
				TimeZone: time.Local,
				SQLMode:  mysql.SetSQLMode(mysql.SQLMode(0), mysql.ModeMsSQL),
			},
		},
		Args: []param.BinaryParam{
			{
				Tp:         1,
				IsUnsigned: true,
				IsNull:     false,
				Val:        []byte{1, 2, 3},
			},
		},
	}
	assert.Equal(t, q1.argHashSize(), len(q1.ArgHash()))
	value, _ := GlobalQueryCache.GetQueryCache(q1)
	assert.Nil(t, value)

	GlobalQueryCache.AddQueryCache(q1, &QueryCacheValue{
		Chunks: make([]*chunk.Chunk, 1),
	})

	value, _ = GlobalQueryCache.GetQueryCache(q1)
	assert.NotNil(t, value)
	assert.Equal(t, len(value.Chunks), 1)

	q1.Vars.SQLMode = mysql.SetSQLMode(mysql.SQLMode(0), mysql.ModeDb2)
	q1.argHash = nil
	value, _ = GlobalQueryCache.GetQueryCache(q1)
	assert.Nil(t, value)
}

func TestQueryCacheBasic(t *testing.T) {
	cache := NewPreparedQueryCache(16 * 1024)
	key := genKey(0)
	value := genValue()
	require.Equal(t, 10, len(key.ArgHash()))
	require.Equal(t, 694, value.ChunksSize())
	require.Equal(t, 4509, value.ResultFieldsSize())
	succ := cache.AddQueryCache(key, value)
	require.True(t, succ)
	require.Equal(t, 0, cache.cm.remain())
	require.Equal(t, len(key.ArgHash())+value.ChunksSize()+value.ResultFieldsSize(), cache.Size())

	key = genKey(0)
	key.SchemaName = "test2"
	succ = cache.AddQueryCache(key, value)
	require.False(t, succ)

	key = genKey(1)
	succ = cache.AddQueryCache(key, value)
	require.True(t, succ)
	require.Equal(t, len(key.ArgHash())*2+value.ChunksSize()*2+value.ResultFieldsSize(), cache.Size())

	cnt := 0
	for i := 2; i < 20; i++ {
		key = genKey(i)
		succ = cache.AddQueryCache(key, value)
		cnt = i
		if !succ {
			require.Less(t, cache.cm.capacity, cache.Size()+len(key.ArgHash())+value.ChunksSize())
			break
		}
		fmt.Printf("%v -> %v\n\n", i, cache.Size())
	}

	for i := 0; i <= cnt; i++ {
		key = genKey(i)
		value, canCache := cache.GetQueryCache(key)
		if i < cnt {
			require.NotNil(t, value)
			require.False(t, canCache)
		} else {
			require.Nil(t, value)
			require.False(t, canCache)
		}
	}

	v, canCache := cache.GetQueryCache(genKey(cnt))
	require.Nil(t, v)
	require.False(t, canCache)
	succ = cache.AddQueryCache(genKey(cnt), value)
	require.False(t, succ)

	config.GetGlobalConfig().Performance.QueryCache.InactiveTTL = 1
	time.Sleep(time.Second * 2)
	v, canCache = cache.GetQueryCache(genKey(0))
	require.NotNil(t, v)
	require.False(t, canCache)

	v, canCache = cache.GetQueryCache(genKey(cnt))
	require.Nil(t, v)
	require.True(t, canCache)
	require.Equal(t, 1, cache.Len())

	succ = cache.AddQueryCache(genKey(cnt), value)
	require.True(t, succ)
	require.Equal(t, 2, cache.Len())
}

func genKey(i int) *QueryCacheKey {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return &QueryCacheKey{
		StmtKey: StmtKey{
			SchemaName: "test1",
			Sql:        "select * from sbtest1 where a = ?",
			Vars: QueryVars{
				TimeZone: time.Local,
				SQLMode:  mysql.SetSQLMode(mysql.SQLMode(0), mysql.ModeMsSQL),
			},
		},
		Args: []param.BinaryParam{
			{
				Tp:         1,
				IsUnsigned: true,
				IsNull:     false,
				Val:        buf,
			},
		},
	}
}

func genValue() *QueryCacheValue {
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
		var d types.Datum
		switch fieldTypes[i].GetType() {
		case mysql.TypeVarchar:
			d = types.NewDatum(fmt.Sprintf("abcdefghijklmnopq-" + strconv.Itoa(i)))
		default:
			d = types.NewDatum(i)
		}
		chk.AppendDatum(i, &d)
	}
	chks = append(chks, chk)
	return &QueryCacheValue{
		ResultFields: resultFields,
		Chunks:       chks,
	}
}

func BenchmarkQueryCache(b *testing.B) {
	cache := NewPreparedQueryCache(1000000)
	k := genKey(0)
	v := genValue()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		k.Args[0].Val = buf
		cache.GetQueryCache(k)
		cache.AddQueryCache(k, v)
	}
}

func TestQueryCacheMemUsage(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe("127.0.0.1:8081", nil))
	}()

	cache := NewPreparedQueryCache(2000000)
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
	require.Equal(t, 1, cache.StmtCount())

	size0 := int64(0)
	size0 += int64(unsafe.Sizeof(cache))
	cache.stmtCache.Range(func(key, value any) bool {
		cache := value.(*PreparedStmtCache)
		size0 += int64(unsafe.Sizeof(cache))
		for _, tp := range cache.ResultFields {
			size0 += int64(unsafe.Sizeof(*tp))
			size0 += int64(unsafe.Sizeof(*tp.Column))
			size0 += int64(unsafe.Sizeof(*tp.Table))
		}
		return true
	})
	memSize += size0

	runtime.GC()

	k := genKey(0)
	k.Sql = "select * from sbtest1 where b = ?"
	v := genValue()
	cache.AddQueryCache(k, v)
	require.Equal(t, count+1, cache.Len())
	require.Equal(t, 2, cache.StmtCount())

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("cache-mem = %v MiB\n", float64(memSize)/1024/1024)
	fmt.Printf("Alloc = %v MiB", m.Alloc/1024/1024)
	fmt.Printf("\tTotalAlloc = %v MiB", m.TotalAlloc/1024/1024)
	fmt.Printf("\tHeapInUse = %v MiB", m.HeapInuse/1024/1024)
	fmt.Printf("\tSys = %v MiB", m.Sys/1024/1024)
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func TestQueryCache2(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000000; j++ {
				k := &QueryCacheKey{
					StmtKey: StmtKey{
						Sql: fmt.Sprintf("sbtest_id_%v", rand.Intn(100000)),
					},
				}

				v, _ := GlobalQueryCache.GetQueryCache(k)
				if v != nil {
					continue
				}

				v = &QueryCacheValue{
					Chunks: make([]*chunk.Chunk, 1),
				}
				GlobalQueryCache.AddQueryCache(k, v)
			}
		}(i)
	}

	wg.Wait()
	require.Equal(t, 100000, GlobalQueryCache.Len())
	require.Equal(t, 100000, GlobalQueryCache.StmtCount())
}
