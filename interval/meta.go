package interval

import (
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func (pm *IntervalPartitionManager) RunMetaMaintainLoop() {
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()

	version := int64(0)
	pm.buildAwsTableMeta()
	for {
		select {
		case <-ticker.C:
			if !pm.ownerManager.IsOwner() {
				continue
			}
			is := pm.infoCache.GetLatest()
			latest := is.SchemaMetaVersion()
			if latest == version {
				continue
			}
			version = latest
			pm.removeDroppedTableInAWSS3()
		}
	}
}

const awsRegion = "us-west-2"

func (pm *IntervalPartitionManager) removeDroppedTableInAWSS3() {
	is := pm.infoCache.GetLatest()
	pm.awsTableMeta.Range(func(key, value interface{}) bool {
		pid := key.(int64)
		meta := value.(*PartitionTableMeta)
		exist := pm.checkPartitionExist(is, pid, meta)
		if !exist {
			err := RemoveDataInAWSS3(meta.db, meta.tableName, pid, awsRegion)
			if err != nil {
				logutil.BgLogger().Warn("[interval-partition] remove data in aws s3 failed",
					zap.String("table", meta.tableName), zap.Int64("table-id", meta.tableID),
					zap.Int64("partition-id", pid), zap.Error(err))
			} else {
				logutil.BgLogger().Warn("[interval-partition] remove data in aws s3 successs",
					zap.String("table", meta.tableName), zap.Int64("table-id", meta.tableID), zap.Int64("partition-id", pid))
				pm.awsTableMeta.Delete(pid)
			}
		}
		return true
	})
}

func (pm *IntervalPartitionManager) checkPartitionExist(is infoschema.InfoSchema, pid int64, meta *PartitionTableMeta) bool {
	tb, ok := is.TableByID(meta.tableID)
	if !ok {
		return false
	}
	pi := tb.Meta().GetPartitionInfo()
	exist := false
	for _, pd := range pi.Definitions {
		if pd.ID == pid {
			exist = true
			break
		}
	}
	return exist
}

func (pm *IntervalPartitionManager) buildAwsTableMeta() {
	is := pm.infoCache.GetLatest()
	dbs := is.AllSchemas()
	for _, db := range dbs {
		if util.IsMemDB(db.Name.L) || util.IsSysDB(db.Name.L) {
			continue
		}
		tbs := is.SchemaTables(db.Name)
		for _, tb := range tbs {
			tbInfo := tb.Meta()
			pi := tbInfo.GetPartitionInfo()
			if pi == nil || pi.Type != model.PartitionTypeRange || pi.Expr == "" ||
				len(pi.Definitions) == 0 || len(pi.Definitions[0].LessThan) != 1 || pi.Interval.MovePartitionExpr == "" {
				continue
			}
			for _, pd := range pi.Definitions {
				if pd.Readonly && pd.Engine == AWSS3Engine {
					pm.awsTableMeta.Store(pd.ID, &PartitionTableMeta{
						tableID:   tbInfo.ID,
						pid:       pd.ID,
						db:        db.Name.L,
						tableName: tbInfo.Name.L,
					})
				}
			}
		}
	}
}
