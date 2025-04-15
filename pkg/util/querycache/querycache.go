package querycache

import (
	"encoding/binary"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/kvcache"
)

var GlobalQueryCache = NewQueryCache()

type QueryCache struct {
	sync.RWMutex
	queryMap *kvcache.SimpleLRUCache
}

func NewQueryCache() *QueryCache {
	return &QueryCache{
		queryMap: kvcache.NewSimpleLRUCache(10000, 0, 0),
	}
}

func (qc *QueryCache) GetQueryCache(key *QueryCacheKey) (value *QueryCacheValue) {
	qc.RLock()
	defer func() {
		qc.RUnlock()
		failpoint.InjectCall("AfterGetQueryCache", key, value)
	}()

	v, _ := qc.queryMap.Get(key)
	if v == nil {
		return nil
	}
	value = v.(*QueryCacheValue)
	return value
}

func (qc *QueryCache) AddQueryCache(key *QueryCacheKey, value *QueryCacheValue) {
	if key == nil || value == nil {
		return
	}
	qc.Lock()
	defer func() {
		qc.Unlock()
		failpoint.InjectCall("AfterAddQueryCache", key, value)
	}()

	k := *key
	v := *value
	qc.queryMap.Put(&k, &v)
}

func (qc *QueryCache) DeleteQueryCache() {
	return
}

type QueryCacheKey struct {
	SchemaName string
	Sql        string
	Args       []param.BinaryParam
	Vars       QueryVars

	hash []byte
}

type QueryVars struct {
	TimeZone *time.Location
	SQLMode  mysql.SQLMode
}

func (k *QueryCacheKey) Hash() []byte {
	if len(k.hash) > 0 {
		return k.hash
	}
	k.hash = make([]byte, 0, k.hashSize())
	k.hash = append(k.hash, hack.Slice(k.SchemaName)...)
	k.hash = append(k.hash, hack.Slice(k.Sql)...)
	for _, arg := range k.Args {
		k.hash = append(k.hash, paramToBytes(arg)...)
	}
	timezone := k.Vars.TimeZone.String()
	k.hash = append(k.hash, hack.Slice(timezone)...)
	items := [8]byte{}
	binary.BigEndian.PutUint64(items[:], uint64(k.Vars.SQLMode))
	k.hash = append(k.hash, items[:]...)
	return k.hash
}

func (k *QueryCacheKey) hashSize() int {
	length := len(k.Sql) + len(k.SchemaName)
	for _, arg := range k.Args {
		length += paramSize(arg)
	}
	length += len(k.Vars.TimeZone.String())
	length += 8
	return length
}

func paramToBytes(arg param.BinaryParam) []byte {
	flag := byte(0)
	if arg.IsUnsigned {
		flag |= 0x01
	}
	if arg.IsNull {
		flag |= 0x02
	}
	buf := make([]byte, 0, paramSize(arg))
	buf = append(buf, arg.Tp)
	buf = append(buf, flag)
	buf = append(buf, arg.Val...)
	return buf
}

func paramSize(arg param.BinaryParam) int {
	return 2 + len(arg.Val)
}

type QueryCacheValue struct {
	ReadTs uint64

	ResultFields []*resolve.ResultField
	FieldTypes   []*types.FieldType
	Chunks       []*chunk.Chunk
}
