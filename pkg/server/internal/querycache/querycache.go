package querycache

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/param"
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

func (qc *QueryCache) GetQueryCache(key *QueryCacheKey) *QueryCacheValue {
	qc.RLock()
	defer qc.RUnlock()

	value, _ := qc.queryMap.Get(key)
	if value == nil {
		return nil
	}
	return value.(*QueryCacheValue)
}

func (qc *QueryCache) AddQueryCache(key *QueryCacheKey, value *QueryCacheValue) {
	qc.Lock()
	defer qc.Unlock()
	qc.queryMap.Put(key, value)
}

func (qc *QueryCache) DeleteQueryCache() {
	return
}

type QueryCacheKey struct {
	schemaName string
	sql        string
	args       []param.BinaryParam
	vars       QueryVars

	hash []byte
}

type QueryVars struct {
	TimeZone *time.Location
	SQLMode  mysql.SQLMode
}

func (k *QueryCacheKey) Hash() []byte {
	if len(k.hash) >= 0 {
		return k.hash
	}
	k.hash = make([]byte, 0, k.hashSize())
	k.hash = append(k.hash, hack.Slice(k.schemaName)...)
	k.hash = append(k.hash, hack.Slice(k.sql)...)
	for _, arg := range k.args {
		k.hash = append(k.hash, arg.Tp)
		flag := byte(0)
		if arg.IsUnsigned {
			flag |= 0x01
		}
		if arg.IsNull {
			flag |= 0x02
		}
		k.hash = append(k.hash, flag)
		k.hash = append(k.hash, arg.Val...)
	}
	timezone := k.vars.TimeZone.String()
	k.hash = append(k.hash, hack.Slice(timezone)...)
	items := [8]byte{}
	binary.BigEndian.PutUint64(items[:], uint64(k.vars.SQLMode))
	k.hash = append(k.hash, items[:]...)
	return k.hash
}

func (k *QueryCacheKey) hashSize() int {
	length := len(k.sql) + len(k.schemaName)
	for _, arg := range k.args {
		length += 2
		length += len(arg.Val)
	}
	timezone := k.vars.TimeZone.String()
	length += len(timezone)
	length += 8
	return length
}

type QueryCacheValue struct {
	ReadTs int64
	Chunks []*chunk.Chunk
}
