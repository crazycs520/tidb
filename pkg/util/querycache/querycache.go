package querycache

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var GlobalQueryCache *PreparedQueryCache

func init() {
	capacity := config.GetGlobalConfig().Performance.QueryCache.Capacity
	GlobalQueryCache = NewPreparedQueryCache(capacity)
}

type PreparedQueryCache struct {
	sync.Mutex
	stmtCache sync.Map // map[StmtKey]*PreparedStmtCache
	cm        *capacityManager
	//lastRemove int64
}

type capacityManager struct {
	sync.Mutex
	allocated int
	capacity  int // byte
}

func (m *capacityManager) alloc(size int) int {

	return 0
}

func (m *capacityManager) release(size int) {

}

func (m *capacityManager) setCapacity(capacity int) {
	m.Lock()
	m.capacity = capacity
	m.Unlock()
}

type PreparedStmtCache struct {
	sync.RWMutex
	cache      map[string]*preparedStmtCacheValue
	size       int // used size
	capacity   int // current capacity
	cm         *capacityManager
	lastRemove int64

	// result meta
	ResultFields []*resolve.ResultField
}

func NewPreparedStmtCache(capacity int, cm *capacityManager) *PreparedStmtCache {
	return &PreparedStmtCache{
		cache:    make(map[string]*preparedStmtCacheValue),
		cm:       cm,
		capacity: capacity,
	}
}

func (c *PreparedStmtCache) Add(k []byte, v *QueryCacheValue) bool {
	succ := false
	ts := time.Now().Unix()
	c.Lock()
	if len(c.ResultFields) == 0 {
		c.ResultFields = v.ResultFields
		c.size += v.ResultFieldsSize()
	}
	chkSize := v.ChunksSize()
	if v.

	if len(c.cache) < c.capacity {
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
		return &QueryCacheValue{
			ResultFields: c.ResultFields,
			Chunks:       v.Chunks,
		}, false
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
	cache := make(map[string]*preparedStmtCacheValue)
	ttl := int64(config.GetGlobalConfig().Performance.QueryCache.InactiveTTL)
	ts := time.Now().Unix()
	memSize := int64(0)
	c.RLock()
	for k, v := range c.cache {
		if (ts-v.ts) > ttl || len(cache) >= capacity {
			memSize += int64(len(k))
			memSize += v.MemoryUsage()
			continue
		}
		cache[k] = v
	}
	c.RUnlock()

	metrics.QueryCacheMemUsage.Add(-float64(memSize))

	c.Lock()
	c.cache = cache
	c.capacity = capacity
	c.Unlock()
}

func (c *PreparedStmtCache) Size() int {
	c.RLock()
	size := len(c.cache)
	c.RUnlock()
	return size
}

func NewPreparedQueryCache(capacity int) *PreparedQueryCache {
	return &PreparedQueryCache{
		stmtCache: sync.Map{},
		cm: &capacityManager{
			capacity: capacity,
		},
	}
}

func (qc *PreparedQueryCache) SetCapacity(capacity uint) {
	qc.cm.setCapacity(int(capacity))
	qc.stmtCache.Range(func(k, v any) bool {
		stmtCache := v.(*PreparedStmtCache)
		stmtCache.ReSize(int(capacity))
		return true
	})
}

func (qc *PreparedQueryCache) GetQueryCache(key *QueryCacheKey) (value *QueryCacheValue, _ bool) {
	cache, ok := qc.stmtCache.Load(key.StmtKey)
	if !ok {
		metrics.QueryCacheCounter.WithLabelValues("miss").Inc()
		return nil, true
	}
	stmtCache := cache.(*PreparedStmtCache)
	v, canCached := stmtCache.Get(key.ArgHash())
	if v == nil {
		metrics.QueryCacheCounter.WithLabelValues("miss").Inc()
		return nil, canCached
	}
	metrics.QueryCacheCounter.WithLabelValues("hit").Inc()
	value = v.Clone()
	return value, canCached
}

func (qc *PreparedQueryCache) getOrCreateStmtCache(key *QueryCacheKey) *PreparedStmtCache {
	v, ok := qc.stmtCache.Load(key.StmtKey)
	if ok {
		return v.(*PreparedStmtCache)
	}
	qc.Lock()
	defer qc.Unlock()
	v, ok = qc.stmtCache.Load(key.StmtKey)
	if ok {
		return v.(*PreparedStmtCache)
	}
	cache := NewPreparedStmtCache(4*1024, qc.cm)
	qc.stmtCache.Store(key.StmtKey, cache)
	return cache
}

func (qc *PreparedQueryCache) AddQueryCache(key *QueryCacheKey, value *QueryCacheValue) {
	if !strings.Contains(key.Sql, "sbtest") {
		return
	}
	cache := qc.getOrCreateStmtCache(key)
	succ := cache.Add(key.ArgHash(), value)
	if succ {
		metrics.QueryCacheCounter.WithLabelValues("add").Inc()
		metrics.QueryCacheMemUsage.Add(float64(key.MemoryUsage() + value.MemoryUsage()))
		failpoint.InjectCall("AfterAddQueryCache", key, value)
	}
}

func (qc *PreparedQueryCache) DeleteQueryCache() {
	return
}

func (qc *PreparedQueryCache) StmtCount() int {
	cnt := 0
	qc.stmtCache.Range(func(_, _ any) bool {
		cnt++
		return true
	})
	return cnt
}

func (qc *PreparedQueryCache) Len() int {
	length := 0
	qc.stmtCache.Range(func(_, v any) bool {
		stmtCache := v.(*PreparedStmtCache)
		stmtCache.RLock()
		length += len(stmtCache.cache)
		stmtCache.RUnlock()
		return true
	})
	return length
}

type StmtKey struct {
	SchemaName string
	Sql        string
	Vars       QueryVars
}

type QueryCacheKey struct {
	StmtKey

	Args []param.BinaryParam

	argHash []byte
}

type QueryVars struct {
	TimeZone *time.Location
	SQLMode  mysql.SQLMode
}

func (k *QueryCacheKey) ArgHash() []byte {
	if len(k.argHash) > 0 {
		return k.argHash
	}
	k.argHash = make([]byte, 0, k.argHashSize())
	for _, arg := range k.Args {
		k.argHash = append(k.argHash, paramToBytes(arg)...)
	}
	return k.argHash
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
	Chunks       []*chunk.Chunk
}

func (v *QueryCacheValue) ResultFieldsSize() int {
	size := int(unsafe.Sizeof(v.ResultFields)) + len(v.ResultFields)
	for _,field := range v.ResultFields {
		size += int(unsafe.Sizeof(*field))
		size += int(unsafe.Sizeof(*field.Table))
		size += int(unsafe.Sizeof(*field.Column))
	}
	return size
}

func (v *QueryCacheValue) ChunksSize() int {
	size := int64(unsafe.Sizeof(v.Chunks)) + int64(len(v.Chunks))
	for _, chk := range v.Chunks {
		size += chk.MemoryUsage()
	}
	return int(size)
}

func (v *QueryCacheValue) MemoryUsage() int64 {
	size := int64(8 * 3)
	for _, chk := range v.Chunks {
		size += chk.MemoryUsage()
	}
	return size
}

func (v *QueryCacheValue) Clone() *QueryCacheValue {
	result := &QueryCacheValue{
		ResultFields: make([]*resolve.ResultField, 0, len(v.ResultFields)),
		Chunks:       make([]*chunk.Chunk, 0, len(v.Chunks)),
	}
	result.ResultFields = append(result.ResultFields, v.ResultFields...)
	result.Chunks = append(result.Chunks, v.Chunks...)
	return result
}

type preparedStmtCacheValue struct {
	Chunks []*chunk.Chunk
	ts     int64
}

func (v *preparedStmtCacheValue) MemoryUsage() int64 {
	size := int64(8 * 3)
	for _, chk := range v.Chunks {
		size += chk.MemoryUsage()
	}
	return size
}
