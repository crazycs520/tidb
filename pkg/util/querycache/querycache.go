package querycache

import (
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var GlobalQueryCache *PreparedQueryCache

func init() {
	capacity := config.GetGlobalConfig().Performance.QueryCache.MemSize
	GlobalQueryCache = NewPreparedQueryCache(int(capacity))
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
	if m.allocated >= m.capacity {
		return 0
	}
	result := 0
	m.Lock()
	if (m.allocated + size) < m.capacity {
		result = size
	} else if m.allocated < m.capacity {
		result = m.capacity - m.allocated
	}
	m.allocated += result
	m.Unlock()
	return result
}

func (m *capacityManager) remain() int {
	return m.capacity - m.allocated
}

func (m *capacityManager) release(size int) {

}

type PreparedStmtCache struct {
	sync.RWMutex
	cache      map[string]*preparedStmtCacheValue
	size       int // used size
	capacity   int // current capacity
	cm         *capacityManager
	full       bool
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

func (c *PreparedStmtCache) Add(k []byte, v *QueryCacheValue) int {
	ts := time.Now().Unix()
	addedSize := 0
	size := v.ChunksSize() + len(k)
	c.Lock()
	if len(c.ResultFields) == 0 {
		c.ResultFields = v.ResultFields
		fieldSize := v.ResultFieldsSize()
		c.size += fieldSize
		addedSize += fieldSize
	}

	if (c.size + size) > c.capacity {
		needSize := c.capacity
		for needSize < size && needSize < 100*1024*1024 {
			needSize = needSize * 2
		}
		if needSize > size {
			c.capacity += c.cm.alloc(c.capacity)
		}
	}

	if (c.size + size) <= c.capacity {
		c.cache[string(k)] = &preparedStmtCacheValue{
			Chunks: v.Chunks,
			ts:     ts,
		}
		c.size += size
		addedSize += size
	} else {
		c.full = true
	}
	c.Unlock()
	return addedSize
}

func (c *PreparedStmtCache) removeUseless(ts int64) int {
	deleted := 0
	memoryUsage := 0
	ttl := int64(config.GetGlobalConfig().Performance.QueryCache.InactiveTTL)
	c.Lock()
	for k, v := range c.cache {
		if (ts - v.ts) > ttl {
			deleted++
			memoryUsage += len(k)
			memoryUsage += v.MemoryUsage()
			delete(c.cache, k)
		}
	}
	if deleted > 0 {
		c.full = false
	}
	c.size = c.size - memoryUsage
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
	if c.full {
		if time.Now().Unix()-c.lastRemove > int64(config.GetGlobalConfig().Performance.QueryCache.InactiveTTL) {
			ts := time.Now().Unix()
			c.lastRemove = ts
			deleted := c.removeUseless(ts)
			return nil, deleted > 0
		}
		return nil, false
	}
	return nil, !c.full
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
	if qc.cm.capacity == int(capacity) {
		return
	}
	qc.Lock()
	qc.stmtCache = sync.Map{}
	qc.cm = &capacityManager{
		capacity: int(capacity),
	}
	qc.Unlock()
	metrics.QueryCacheMemUsage.Set(float64(0))
}

func (qc *PreparedQueryCache) GetQueryCache(key *QueryCacheKey) (value *QueryCacheValue, _ bool) {
	cache, ok := qc.stmtCache.Load(key.StmtKey)
	if !ok {
		metrics.QueryCacheCounter.WithLabelValues("miss").Inc()
		return nil, qc.cm.remain() > 0
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
	size := qc.cm.alloc(16 * 1024)
	if size == 0 {
		return nil
	}
	cache := NewPreparedStmtCache(size, qc.cm)
	qc.stmtCache.Store(key.StmtKey, cache)
	return cache
}

func (qc *PreparedQueryCache) AddQueryCache(key *QueryCacheKey, value *QueryCacheValue) bool {
	//if !strings.Contains(key.Sql, "sbtest") {
	//	return false
	//}
	cache := qc.getOrCreateStmtCache(key)
	if cache == nil {
		return false
	}
	size := cache.Add(key.ArgHash(), value)
	if size > 0 {
		metrics.QueryCacheCounter.WithLabelValues("add").Inc()
		metrics.QueryCacheMemUsage.Add(float64(size))
		return true
	}
	return false
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

func (qc *PreparedQueryCache) Size() int {
	size := 0
	qc.stmtCache.Range(func(_, v any) bool {
		stmtCache := v.(*PreparedStmtCache)
		stmtCache.RLock()
		size += stmtCache.size
		stmtCache.RUnlock()
		return true
	})
	return size
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
	for _, field := range v.ResultFields {
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
	size := int64(unsafe.Sizeof(v.Chunks)) + int64(len(v.Chunks))
	for _, chk := range v.Chunks {
		size += chk.MemoryUsage()
	}
	return size
}

func (v *preparedStmtCacheValue) MemoryUsage() int {
	size := int64(unsafe.Sizeof(v.Chunks)) + int64(len(v.Chunks))
	for _, chk := range v.Chunks {
		size += chk.MemoryUsage()
	}
	return int(size)
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
