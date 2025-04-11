package querycache

import (
	"encoding/binary"
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
	SchemaName string
	Sql        string
	Args       string
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
	k.hash = append(k.hash, hack.Slice(k.Args)...)
	timezone := k.Vars.TimeZone.String()
	k.hash = append(k.hash, hack.Slice(timezone)...)
	items := [8]byte{}
	binary.BigEndian.PutUint64(items[:], uint64(k.Vars.SQLMode))
	k.hash = append(k.hash, items[:]...)
	return k.hash
}

func (k *QueryCacheKey) hashSize() int {
	length := len(k.Sql) + len(k.SchemaName) + len(k.Args)
	length += len(k.Vars.TimeZone.String())
	length += 8
	return length
}

type QueryCacheValue struct {
	ReadTs uint64
	Chunks []*chunk.Chunk
}
