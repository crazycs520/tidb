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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
)

var GlobalQueryCache = NewQueryCache()

type QueryCache struct {
	slots    []*ThreadSafeLRUCache
	capacity uint
}

type ThreadSafeLRUCache struct {
	sync.RWMutex
	cache    map[string]*QueryCacheValue
	capacity int

	miss       int64
	full       bool
	lastRemove int64
}

func (c *ThreadSafeLRUCache) Add(k []byte, v *QueryCacheValue) bool {
	if len(c.cache) >= c.capacity {
		miss := atomic.AddInt64(&c.miss, 1)
		if miss%100000 == 0 && !c.full && time.Now().Unix()-c.lastRemove > 10 {
			c.lastRemove = time.Now().Unix()
			deleted := c.removeUseless()
			if deleted == 0 {
				c.full = true
				return false
			}
		} else {
			return false
		}
	}
	succ := false
	c.Lock()
	if len(c.cache) < c.capacity {
		c.cache[string(k)] = v
		succ = true
	}
	c.Unlock()
	return succ
}

func (c *ThreadSafeLRUCache) removeUseless() int {
	deleted := 0
	metrics.QueryCacheCounter.WithLabelValues("delete").Inc()
	c.Lock()
	for k, v := range c.cache {
		if v.hit == 0 {
			deleted++
			delete(c.cache, k)
		}
	}
	c.Unlock()
	return deleted
}

func (c *ThreadSafeLRUCache) Get(k []byte) *QueryCacheValue {
	c.RLock()
	v := c.cache[string(k)]
	c.RUnlock()
	if v != nil {
		atomic.AddInt64(&v.hit, 1)
	}
	return v
}

func (c *ThreadSafeLRUCache) ReSize(capacity int) {
	c.Lock()
	c.cache = make(map[string]*QueryCacheValue, capacity)
	c.capacity = capacity
	c.Unlock()
}

func (c *ThreadSafeLRUCache) Size() int {
	c.RLock()
	size := len(c.cache)
	c.RUnlock()
	return size
}

func NewQueryCache() *QueryCache {
	capacity := config.GetGlobalConfig().Performance.QueryCache.Capacity
	slots := make([]*ThreadSafeLRUCache, 100)
	size := (capacity / 100) + 1
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

func (qc *QueryCache) GetQueryCache(key *QueryCacheKey) (value *QueryCacheValue) {
	hasher := fnv.New64()
	hasher.Write(key.Hash())
	idx := int(hasher.Sum64() % uint64(len(qc.slots)))

	defer func() {
		failpoint.InjectCall("AfterGetQueryCache", key, value)
	}()

	v := qc.slots[idx].Get(key.Hash())
	if v == nil {
		metrics.QueryCacheCounter.WithLabelValues("miss").Inc()
		return nil
	}
	metrics.QueryCacheCounter.WithLabelValues("hit").Inc()
	value = v.Clone()
	return value
}

func (qc *QueryCache) AddQueryCache(key *QueryCacheKey, value *QueryCacheValue) {
	if !strings.Contains(key.Sql, "sbtest") {
		return
	}
	hasher := fnv.New64()
	hasher.Write(key.Hash())
	idx := int(hasher.Sum64() % uint64(len(qc.slots)))

	defer func() {
	}()
	succ := qc.slots[idx].Add(key.Hash(), value)
	if succ {
		metrics.QueryCacheCounter.WithLabelValues("add").Inc()
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

	hit int64
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
