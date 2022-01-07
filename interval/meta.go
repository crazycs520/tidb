package interval

import (
	"github.com/pingcap/tidb/interval/athena"
	util2 "github.com/pingcap/tidb/interval/util"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func (pm *IntervalPartitionManager) RunMetaMaintainLoop() {
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()

	version := int64(0)
	err := pm.loadAwsTableMeta()
	if err != nil {
		logutil.BgLogger().Warn("load aws table meta failed", zap.Error(err))
	}
	for {
		select {
		case <-ticker.C:
			if !pm.ownerManager.IsOwner() {
				continue
			}
			err := pm.loadAwsTableMeta()
			if err != nil {
				logutil.BgLogger().Warn("load aws table meta failed", zap.Error(err))
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

func (pm *IntervalPartitionManager) removeDroppedTableInAWSS3() {
	is := pm.infoCache.GetLatest()
	pm.awsTableMeta.Range(func(key, value interface{}) bool {
		pid := key.(int64)
		meta := value.(*PartitionTableMeta)
		exist := pm.checkPartitionExist(is, pid, meta)
		if !exist {
			err := RemoveDataInAWSS3(meta.db, meta.tableName, pid)
			if err != nil {
				logutil.BgLogger().Warn("[interval-partition] remove data in aws s3 failed",
					zap.String("table", meta.tableName), zap.Int64("table-id", meta.tableID),
					zap.Int64("partition-id", pid), zap.Error(err))
			} else {
				logutil.BgLogger().Warn("[interval-partition] remove data in aws s3 success",
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

func (pm *IntervalPartitionManager) loadAwsTableMeta() error {
	if pm.loadedMeta {
		return nil
	}

	cli, err := athena.CreateCli(pm.s3Region)
	if err != nil {
		return err
	}
	is := pm.infoCache.GetLatest()

	dbs, err := athena.GetAllDatabase(cli)
	if err != nil {
		return nil
	}
	for _, dbName := range dbs {
		tbs, err := athena.GetAllTables(cli, dbName)
		if err != nil {
			return nil
		}
		for _, tpName := range tbs {
			tbName, pid, valid := util2.ParseTablePartitionName(tpName)
			if !valid {
				continue
			}
			table, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tbName))
			if err != nil || table == nil {
				continue
			}
			pm.awsTableMeta.Store(pid, &PartitionTableMeta{
				tableID:   table.Meta().ID,
				pid:       pid,
				db:        dbName,
				tableName: tbName,
			})
			logutil.BgLogger().Info("load table meta from s3 succ",
				zap.String("table", tbName), zap.String("db", dbName),
				zap.Int64("tid", table.Meta().ID), zap.Int64("pid", pid))
		}
	}
	pm.loadedMeta = true
	return nil
}
