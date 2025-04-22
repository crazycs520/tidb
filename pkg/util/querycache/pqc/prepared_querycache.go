package querycache

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var GlobalQueryCache *PreparedQueryCache

func init() {
	//capacity := config.GetGlobalConfig().Performance.QueryCache.Capacity
	//GlobalQueryCache = NewQueryCache(capacity)
}

type PreparedQueryCache struct {
	sync.Mutex
	stmtCache sync.Map // map[StmtKey]*StmtCache
	capacity  uint
}

type StmtKey struct {
	SchemaName string
	Sql        string
	Vars       QueryVars
}

type StmtCache struct {
	*PreparedStmtCache
}

type PreparedStmtCache struct {
	sync.RWMutex
	ResultFields []*resolve.ResultField
	FieldTypes   []*types.FieldType

	cache map[string]*preparedStmtCacheValue

	lastRemove int64
}

type preparedStmtCacheValue struct {
	Chunks []*chunk.Chunk
	ts     int64
}

func (c *PreparedStmtCache) Add(k []byte, v *QueryCacheValue, capacity int) bool {
	succ := false
	ts := time.Now().Unix()
	c.Lock()
	if len(c.FieldTypes) == 0 {
		c.FieldTypes = v.FieldTypes
		c.ResultFields = v.ResultFields
	}
	if len(c.cache) < capacity {
		c.cache[string(k)] = &preparedStmtCacheValue{
			Chunks: v.Chunks,
			ts:     ts,
		}
		succ = true
	}
	c.Unlock()
	return succ
}

func (c *PreparedStmtCache) removeUseless(ts int64) int {
	deleted := 0
	memoryUsage := int64(0)
	ttl := int64(config.GetGlobalConfig().Performance.QueryCache.InactiveTTL)
	c.Lock()
	for k, v := range c.cache {
		if (ts - v.ts) > ttl {
			deleted++
			memoryUsage += int64(len(k))
			memoryUsage += v.MemoryUsage()
			delete(c.cache, k)
		}
	}
	c.Unlock()
	metrics.QueryCacheCounter.WithLabelValues("delete").Add(float64(deleted))
	metrics.QueryCacheMemUsage.Add(-float64(memoryUsage))
	return deleted
}

func (c *PreparedStmtCache) Get(k []byte) (*QueryCacheValue, bool) {
	c.RLock()
	v := c.cache[string(k)]
	c.RUnlock()
	if v != nil {
		atomic.StoreInt64(&v.ts, time.Now().Unix())
		return v, false
	}

	// evict old entry if needed.
	if len(c.cache) >= c.capacity {
		if time.Now().Unix()-c.lastRemove > 60 {
			ts := time.Now().Unix()
			c.lastRemove = ts
			deleted := c.removeUseless(ts)
			return nil, deleted > 0
		}
		return nil, false
	}
	return nil, true
}

func (c *PreparedStmtCache) ReSize(capacity int) {
	cache := make(map[string]*QueryCacheValue, capacity)
	ttl := int64(config.GetGlobalConfig().Performance.QueryCache.InactiveTTL)
	ts := time.Now().Unix()
	memSize := int64(0)
	c.RLock()
	for k, v := range c.cache {
		if (ts - v.ts) > ttl {
			memSize += int64(len(k))
			memSize += v.MemoryUsage()
			continue
		}
		cache[k] = v
		if len(cache) >= capacity {
			break
		}
	}
	c.RUnlock()

	metrics.QueryCacheMemUsage.Add(-float64(memSize))

	c.Lock()
	c.cache = cache
	c.capacity = capacity
	c.Unlock()
	atomic.AddInt64()
}

func (c *PreparedStmtCache) Size() int {
	c.RLock()
	size := len(c.cache)
	c.RUnlock()
	return size
}

func NewPreparedQueryCache(capacity uint) *PreparedQueryCache {
	return &PreparedQueryCache{
		stmtCache: sync.Map{},
		capacity:  capacity,
	}
}

func (qc *PreparedQueryCache) SetCapacity(capacity uint) {
	qc.capacity = capacity
	//size := (capacity / uint(len(qc.slots))) + 1
	//for i := range qc.slots {
	//	qc.slots[i].ReSize(int(size))
	//}
}

func (qc *PreparedQueryCache) GetQueryCache(key *QueryCacheKey) (value *QueryCacheValue, _ bool) {
	v, ok := qc.stmtCache.Load(key.StmtKey)
	if !ok {
		metrics.QueryCacheCounter.WithLabelValues("miss").Inc()
		return nil, true
	}
	stmtCache := v.(*StmtCache)

	v, canCached := stmtCache.cache.Get(key.ArgHash())
	if v == nil {
		metrics.QueryCacheCounter.WithLabelValues("miss").Inc()
		return nil, canCached
	}
	metrics.QueryCacheCounter.WithLabelValues("hit").Inc()
	value = v.Clone()
	return value, canCached
}

func (qc *PreparedQueryCache) getOrCreateStmtCache(key *QueryCacheKey) *StmtCache {
	v, ok := qc.stmtCache.Load(key.StmtKey)
	if ok {
		return v.(*StmtCache)
	}
}

func (qc *PreparedQueryCache) AddQueryCache(key *QueryCacheKey, value *QueryCacheValue) {
	if !strings.Contains(key.Sql, "sbtest") {
		return
	}
	hasher := fnv.New64()
	hasher.Write(key.Hash())
	idx := int(hasher.Sum64() % uint64(len(qc.slots)))

	value.ts = time.Now().Unix()
	defer func() {
	}()
	succ := qc.slots[idx].Add(key.Hash(), value)
	if succ {
		metrics.QueryCacheCounter.WithLabelValues("add").Inc()
		metrics.QueryCacheMemUsage.Add(float64(key.MemoryUsage() + value.MemoryUsage()))
		failpoint.InjectCall("AfterAddQueryCache", key, value)
	}
}

func (qc *QueryCache) Len() int {
	size := 0
	for i := range qc.slots {
		size += qc.slots[i].Size()
	}
	return size
}

func (qc *QueryCache) DeleteQueryCache() {
	return
}

type QueryCacheKey struct {
	StmtKey

	Args []param.BinaryParam

	hash []byte
}

type QueryVars struct {
	TimeZone string
	SQLMode  mysql.SQLMode
}

func (k *QueryCacheKey) ArgHash() []byte {
	if len(k.hash) > 0 {
		return k.hash
	}
	k.hash = make([]byte, 0, k.argHashSize())
	for _, arg := range k.Args {
		k.hash = append(k.hash, paramToBytes(arg)...)
	}
	return k.hash
}

func (k *QueryCacheKey) argHashSize() int {
	length := 0
	for _, arg := range k.Args {
		length += paramSize(arg)
	}
	return length
}

func (k *QueryCacheKey) MemoryUsage() int64 {
	return int64(len(k.ArgHash()))
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
	ResultFields []*resolve.ResultField
	FieldTypes   []*types.FieldType
	Chunks       []*chunk.Chunk
}

func (v *QueryCacheValue) MemoryUsage() int64 {
	size := int64(8 * 3)
	for _, chk := range v.Chunks {
		size += chk.MemoryUsage()
	}
	return size
}

func (v *preparedStmtCacheValue) MemoryUsage() int64 {
	size := int64(8 * 3)
	for _, chk := range v.Chunks {
		size += chk.MemoryUsage()
	}
	return size
}
