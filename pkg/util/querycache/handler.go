package querycache

import (
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type QueryCacheHandler struct {
	Key       *QueryCacheKey
	Value     *QueryCacheValue
	ranges    []TableRange
	cacheSize uint64
}

type TableRange struct {
	tid    int64
	ranges []kv.KeyRange
}

func (h *QueryCacheHandler) NeedCache() bool {
	return h.Key != nil
}

func (h *QueryCacheHandler) AddQueryResult(fields []*resolve.ResultField, chk *chunk.Chunk) {
	size := uint64(chk.MemoryUsage())
	if (h.cacheSize + size) > config.GetGlobalConfig().Performance.QueryCache.MaxQuerySize {
		h.Reset()
		metrics.QueryCacheCounter.WithLabelValues("big-result-add-fail").Inc()
		return
	}
	if h.Value == nil {
		h.Value = &QueryCacheValue{
			ResultFields: fields,
		}
	}
	h.Value.Chunks = append(h.Value.Chunks, chk.CopyConstructSel())
	h.cacheSize += size
}

func (h *QueryCacheHandler) AddQueryCache() {
	if h.Key == nil || h.Value == nil {
		return
	}
	h.Value.Ranges = h.ranges
	//if h.Key.SchemaName == "query_cache_db" && strings.Contains(h.Key.Sql, "select * from t1") {
	//	logutil.BgLogger().Info("add query cache --cs--", zap.String("schema", h.Key.SchemaName), zap.String("sql", h.Key.Sql), zap.String("ranges", fmt.Sprintf("%#v", h.ranges)))
	//}
	GlobalQueryCache.AddQueryCache(h.Key, h.Value)
}

func (h *QueryCacheHandler) AddReadRange(tid int64, keyRanges *kv.KeyRanges) {
	if h.Key == nil {
		return
	}

	for _, tr := range h.ranges {
		if tr.tid == tid {
			tr.ranges = keyRanges.AppendSelfTo(tr.ranges)
			return
		}
	}
	ranges := make([]kv.KeyRange, 0, keyRanges.Len())
	ranges = keyRanges.AppendSelfTo(ranges)
	h.ranges = append(h.ranges, TableRange{tid: tid, ranges: ranges})
}

func (h *QueryCacheHandler) AddPointGetRange(tid int64, key kv.Key) {
	if h.Key == nil {
		return
	}

	kr := kv.KeyRange{
		StartKey: key,
		EndKey:   key.PrefixNext(),
	}
	// todo: add key range size later.
	//h.cacheSize += uint64(len(kr.StartKey) + len(kr.EndKey))
	for _, tr := range h.ranges {
		if tr.tid == tid {
			tr.ranges = append(tr.ranges, kr)
			return
		}
	}
	ranges := make([]kv.KeyRange, 0, 1)
	ranges = append(ranges, kr)
	h.ranges = append(h.ranges, TableRange{tid: tid, ranges: ranges})
}

func (h *QueryCacheHandler) AddBatchPointGetRange(tid int64, keys []kv.Key) {
	if h.Key == nil {
		return
	}

	krs := make([]kv.KeyRange, 0, len(keys))
	for _, key := range keys {
		krs = append(krs, kv.KeyRange{
			StartKey: key,
			EndKey:   key.PrefixNext(),
		})
	}
	for _, tr := range h.ranges {
		if tr.tid == tid {
			tr.ranges = append(tr.ranges, krs...)
			return
		}
	}
	h.ranges = append(h.ranges, TableRange{tid: tid, ranges: krs})
}

func (h *QueryCacheHandler) Reset() {
	h.Key = nil
	h.Value = nil
	h.ranges = nil
}
