package interval

import (
	"context"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type IntervalPartitionManager struct {
	ctx          context.Context
	cancel       context.CancelFunc
	sessPool     *sessionPool
	infoCache    *infoschema.InfoCache
	ownerManager owner.Manager
}

func NewIntervalPartitionManager(ctxPool *pools.ResourcePool, infoCache *infoschema.InfoCache, ownerManager owner.Manager) *IntervalPartitionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &IntervalPartitionManager{
		ctx:          ctx,
		cancel:       cancel,
		sessPool:     newSessionPool(ctxPool),
		infoCache:    infoCache,
		ownerManager: ownerManager,
	}
}

func (pm *IntervalPartitionManager) Start() {
	logutil.BgLogger().Info("[interval-partition] manager started")
	go util.WithRecovery(pm.Run, nil)
}

func (pm *IntervalPartitionManager) Stop() {
	if pm.cancel != nil {
		pm.cancel()
	}
	logutil.BgLogger().Info("[interval-partition] manager stopped")
}

var defCheckInterval = time.Second

func (pm *IntervalPartitionManager) Run() {
	ticker := time.NewTicker(defCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !pm.ownerManager.IsOwner() {
				continue
			}
			pm.GetNeedIntervalTablePartitions(2)
		}
	}
}

func (pm *IntervalPartitionManager) GetNeedIntervalTablePartitions(cnt int) []TablePartition {
	result := make([]TablePartition, 0, cnt)
	is := pm.infoCache.GetLatest()
	dbs := is.AllSchemas()
	ctx, err := pm.sessPool.get()
	if err != nil {
		return nil
	}
	defer pm.sessPool.put(ctx)
	for _, db := range dbs {
		tbs := is.SchemaTables(db.Name)
		for _, tb := range tbs {
			tmp := pm.getTableNeedIntervalPartition(ctx, tb.Meta())
			if len(tmp) == 0 {
				continue
			}
			result = append(result, tmp...)
			if len(result) >= cnt {
				return result[:cnt]
			}
		}
	}
	return result
}

type TablePartition struct {
	tbInfo *model.TableInfo
	ptInfo *model.PartitionDefinition
}

func (pm *IntervalPartitionManager) getTableNeedIntervalPartition(ctx sessionctx.Context, tbInfo *model.TableInfo) []TablePartition {
	pi := tbInfo.GetPartitionInfo()
	if pi == nil || pi.Type != model.PartitionTypeRange || pi.Expr == "" ||
		len(pi.Definitions) == 0 || len(pi.Definitions[0].LessThan) != 1 || pi.Interval.MovePartitionExpr == "" {
		return nil
	}

	isUnsigned := isColUnsigned(tbInfo.Columns, pi)
	moveExprValue, _, err := getRangeValue(ctx, pi.Interval.MovePartitionExpr, isUnsigned)
	if err != nil {
		return nil
	}
	result := []TablePartition{}
	for _, pd := range pi.Definitions {
		rangeValueStr := pd.LessThan[0]
		if rangeValueStr == "MAXVALUE" {
			return nil
		}
		rangeValue, _, err := getRangeValue(ctx, rangeValueStr, isUnsigned)
		if err != nil {
			return nil
		}
		less := false
		if isUnsigned {
			less = rangeValue.(uint64) < moveExprValue.(uint64)
		} else {
			less = rangeValue.(int64) < moveExprValue.(int64)
		}
		if !less {
			return result
		}
		// check running table

		result = append(result, TablePartition{
			tbInfo: tbInfo,
			ptInfo: &pd,
		})
		logutil.BgLogger().Info("[interval-partition] find need moved table partition",
			zap.String("table", tbInfo.Name.O),
			zap.String("partition", pd.Name.O),
			zap.Reflect("range-v", rangeValue),
			zap.Reflect("move-v", moveExprValue))
	}
	return result
}
