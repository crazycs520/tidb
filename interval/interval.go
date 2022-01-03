package interval

import (
	"context"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type IntervalPartitionManager struct {
	ctx          context.Context
	cancel       context.CancelFunc
	sessPool     *sessionPool
	ddl          ddl.DDL
	infoCache    *infoschema.InfoCache
	ownerManager owner.Manager

	jobCh         chan *TablePartition
	mu            sync.Mutex
	handlingInfos map[int64]struct{} // partition id -> struct

	// For auto create partition.
	taskCh chan *AutoCreatePartitionTask
}

func NewIntervalPartitionManager(ctxPool *pools.ResourcePool, ddl ddl.DDL, infoCache *infoschema.InfoCache, ownerManager owner.Manager) *IntervalPartitionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &IntervalPartitionManager{
		ctx:           ctx,
		cancel:        cancel,
		sessPool:      newSessionPool(ctxPool),
		ddl:           ddl,
		infoCache:     infoCache,
		ownerManager:  ownerManager,
		jobCh:         make(chan *TablePartition),
		handlingInfos: make(map[int64]struct{}),
		taskCh:        make(chan *AutoCreatePartitionTask),
	}
}

func (pm *IntervalPartitionManager) Start() {
	logutil.BgLogger().Info("[interval-partition] manager started")
	go util.WithRecovery(pm.RunCheckerLoop, nil)
	go util.WithRecovery(pm.RunWorkerLoop, nil)
	go util.WithRecovery(pm.RunAutoCreatePartitionLoop, nil)
}

func (pm *IntervalPartitionManager) Stop() {
	if pm.cancel != nil {
		pm.cancel()
	}
	logutil.BgLogger().Info("[interval-partition] manager stopped")
}

var defCheckInterval = time.Second

func (pm *IntervalPartitionManager) RunCheckerLoop() {
	ticker := time.NewTicker(defCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !pm.ownerManager.IsOwner() {
				continue
			}
			if pm.getHandlingNum() > 0 {
				continue
			}
			infos := pm.GetNeedIntervalTablePartitions(1)
			for _, info := range infos {
				select {
				case pm.jobCh <- info:
					pm.mu.Lock()
					pm.handlingInfos[info.pdInfo.ID] = struct{}{}
					pm.mu.Unlock()
				default:
				}
			}
		}
	}
}

func (pm *IntervalPartitionManager) getHandlingNum() int {
	pm.mu.Lock()
	n := len(pm.handlingInfos)
	pm.mu.Unlock()
	return n
}

func (pm *IntervalPartitionManager) RunWorkerLoop() {
	var info *TablePartition
	var job *Job
	finishOldJob := false
	for {
		var err error
		if info != nil && job != nil && job.state == JobStateMovingData {
			time.Sleep(time.Second)
		}

		if info == nil {
			if !finishOldJob {
				info = pm.getOldJob()
			}
			if info == nil {
				finishOldJob = true
				info = <-pm.jobCh
			}
		}

		job, err = pm.LoadOrCreateJobInfo(info)
		if err != nil {
			logutil.BgLogger().Error("[interval-partition] load or create job info failed", zap.Error(err))
			continue
		}

		err = pm.HandleJob(job, info)
		if err != nil {
			logutil.BgLogger().Error("[interval-partition] handle job failed", zap.Error(err))
			continue
		}

		err = pm.updateJobState(job)
		if err != nil {
			logutil.BgLogger().Error("[interval-partition] update job state failed", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		if job.state == JobStateDone || job.state == JobStateCancelled {
			err = pm.FinishJob(job)
			if err != nil {
				logutil.BgLogger().Error("[interval-partition] finish job failed", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			pm.finishHandleInfo(info.pdInfo.ID)
			logutil.BgLogger().Info("[interval-partition] finish job", zap.Int64("job-id", job.id), zap.String("table", info.tbInfo.Name.O),
				zap.String("partition", info.pdInfo.Name.O))
			info = nil
		}
	}
}

func (pm *IntervalPartitionManager) getOldJob() *TablePartition {
	job, err := pm.LoadOldJobInfo()
	if err != nil {
		logutil.BgLogger().Error("[interval-partition] load old job info failed", zap.Error(err))
	}
	if job == nil {
		return nil
	}
	is := pm.infoCache.GetLatest()
	tb, ok1 := is.TableByID(job.tableID)
	db, ok2 := is.SchemaByName(model.NewCIStr(job.dbName))
	if !ok1 || !ok2 {
		err := pm.cancelAndFinishJob(job)
		if err != nil {
			logutil.BgLogger().Error("[interval-partition] cancel and finish job failed", zap.Error(err))
		}
		return nil
	}
	pi := tb.Meta().GetPartitionInfo()
	var pdInfo *model.PartitionDefinition
	for i := range pi.Definitions {
		if pi.Definitions[i].ID == job.partitionID {
			pdInfo = &pi.Definitions[i]
			break
		}
	}
	if pdInfo == nil {
		err := pm.cancelAndFinishJob(job)
		if err != nil {
			logutil.BgLogger().Error("[interval-partition] cancel and finish job failed", zap.Error(err))
		}
		return nil
	}
	return &TablePartition{
		dbInfo: db,
		tbInfo: tb.Meta(),
		pdInfo: pdInfo,
	}
}

func (pm *IntervalPartitionManager) cancelAndFinishJob(job *Job) error {
	job.state = JobStateCancelled
	err := pm.updateJobState(job)
	if err != nil {
		logutil.BgLogger().Error("[interval-partition] update job state failed", zap.Error(err))
		return err
	}
	err = pm.FinishJob(job)
	if err != nil {
		logutil.BgLogger().Error("[interval-partition] finish job failed", zap.Error(err))
		return err
	}
	pm.finishHandleInfo(job.partitionID)
	logutil.BgLogger().Error("[interval-partition] finish canceled job", zap.Int64("job-id", job.id), zap.String("table", job.tableName),
		zap.String("partition", job.partitionName))
	return nil
}

func (pm *IntervalPartitionManager) finishHandleInfo(pid int64) {
	pm.mu.Lock()
	delete(pm.handlingInfos, pid)
	pm.mu.Unlock()
}

func (pm *IntervalPartitionManager) GetNeedIntervalTablePartitions(cnt int) []*TablePartition {
	result := make([]*TablePartition, 0, cnt)
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
			tmp := pm.getTableNeedIntervalPartition(ctx, db, tb.Meta())
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
	dbInfo *model.DBInfo
	tbInfo *model.TableInfo
	pdInfo *model.PartitionDefinition
}

func (pm *IntervalPartitionManager) getTableNeedIntervalPartition(ctx sessionctx.Context, dbInfo *model.DBInfo, tbInfo *model.TableInfo) []*TablePartition {
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
	result := []*TablePartition{}
	for i := range pi.Definitions {
		rangeValueStr := pi.Definitions[i].LessThan[0]
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

		logutil.BgLogger().Info("fina need move data partition", zap.Reflect("range-value", rangeValue), zap.Reflect("move-expr", moveExprValue))
		result = append(result, &TablePartition{
			dbInfo: dbInfo,
			tbInfo: tbInfo,
			pdInfo: &pi.Definitions[i],
		})
		logutil.BgLogger().Info("[interval-partition] find need moved table partition",
			zap.String("table", tbInfo.Name.O),
			zap.String("partition", pi.Definitions[i].Name.O),
			zap.Reflect("range-v", rangeValue),
			zap.Reflect("move-v", moveExprValue))
	}
	return result
}
