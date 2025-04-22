package querycache

import (
	"encoding/binary"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
)

var GlobalQueryCache *QueryCache

func init() {
	capacity := config.GetGlobalConfig().Performance.QueryCache.Capacity
	GlobalQueryCache = NewQueryCache(capacity)
}

type QueryCache struct {
	slots    []*ThreadSafeLRUCache
	capacity uint
}

type ThreadSafeLRUCache struct {
	sync.RWMutex
	cache    map[string]*QueryCacheValue
	capacity int

	lastRemove int64
}

func (c *ThreadSafeLRUCache) Add(k []byte, v *QueryCacheValue) bool {
	succ := false
	c.Lock()
	if len(c.cache) < c.capacity {
		c.cache[string(k)] = v
		succ = true
	}
	c.Unlock()
	return succ
}

func (c *ThreadSafeLRUCache) removeUseless(ts int64) int {
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

func (c *ThreadSafeLRUCache) Get(k []byte) (*QueryCacheValue, bool) {
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

func (c *ThreadSafeLRUCache) ReSize(capacity int) {
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
}

func (c *ThreadSafeLRUCache) Size() int {
	c.RLock()
	size := len(c.cache)
	c.RUnlock()
	return size
}

func NewQueryCache(capacity uint) *QueryCache {
	slotNum := 100
	slots := make([]*ThreadSafeLRUCache, slotNum)
	size := (capacity / uint(slotNum))
	if capacity%uint(slotNum) > 0 {
		size++
	}

	for i := range slots {
		slots[i] = &ThreadSafeLRUCache{
			cache:    make(map[string]*QueryCacheValue, size),
			capacity: int(size),
		}
	}
	return &QueryCache{
		slots:    slots,
		capacity: capacity,
	}
}

func (qc *QueryCache) SetCapacity(capacity uint) {
	qc.capacity = capacity
	size := (capacity / uint(len(qc.slots))) + 1
	for i := range qc.slots {
		qc.slots[i].ReSize(int(size))
	}
}

func (qc *QueryCache) GetQueryCache(key *QueryCacheKey) (value *QueryCacheValue, _ bool) {
	hasher := fnv.New64()
	hasher.Write(key.Hash())
	idx := int(hasher.Sum64() % uint64(len(qc.slots)))

	defer func() {
		failpoint.InjectCall("AfterGetQueryCache", key, value)
	}()

	v, canCached := qc.slots[idx].Get(key.Hash())
	if v == nil {
		metrics.QueryCacheCounter.WithLabelValues("miss").Inc()
		return nil, canCached
	}
	metrics.QueryCacheCounter.WithLabelValues("hit").Inc()
	value = v.Clone()
	return value, canCached
}

func (qc *QueryCache) AddQueryCache(key *QueryCacheKey, value *QueryCacheValue) {
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

func (k *QueryCacheKey) MemoryUsage() int64 {
	return int64(len(k.Hash()))
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

	ts int64
}

func (v *QueryCacheValue) MemoryUsage() int64 {
	size := int64(8 * 3)
	for _, chk := range v.Chunks {
		size += chk.MemoryUsage()
	}
	for _, field := range v.ResultFields {
		size += int64(unsafe.Sizeof(*field)) + int64(cap(v.ResultFields)*8)
	}
	for _, field := range v.FieldTypes {
		size += int64(unsafe.Sizeof(*field)) + int64(cap(v.FieldTypes)*8)
	}
	return size
}

func (v *QueryCacheValue) Clone() *QueryCacheValue {
	result := &QueryCacheValue{
		ReadTs:       v.ReadTs,
		ResultFields: make([]*resolve.ResultField, 0, len(v.ResultFields)),
		FieldTypes:   make([]*types.FieldType, 0, len(v.FieldTypes)),
		Chunks:       make([]*chunk.Chunk, 0, len(v.Chunks)),
	}
	result.ResultFields = append(result.ResultFields, v.ResultFields...)
	result.FieldTypes = append(result.FieldTypes, v.FieldTypes...)
	result.Chunks = append(result.Chunks, v.Chunks...)
	return result
}
