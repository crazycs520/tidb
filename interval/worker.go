package interval

import (
	"context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

type Job struct {
	id            int64
	dbName        string
	tableName     string
	tableID       int64
	partitionName string
	partitionID   int64
	state         string
}

const (
	loadJobSQL        = "SELECT HIGH_PRIORITY id, db_name, table_name, table_id, partition_name, partition_id, state FROM mysql.interval_partition_jobs WHERE partition_id = %?"
	loadOldJobSQL     = "SELECT HIGH_PRIORITY id, db_name, table_name, table_id, partition_name, partition_id, state FROM mysql.interval_partition_jobs LIMIT 1"
	insertJobSQL      = "INSERT IGNORE INTO mysql.interval_partition_jobs (id, db_name, table_name, table_id, partition_name, partition_id, state) VALUES (%?, %?, %?, %?, %?, %?, %?)"
	updateJobStateSQL = "UPDATE mysql.interval_partition_jobs SET state = %? WHERE id = %?"
	insertDoneJobSQL  = "INSERT IGNORE INTO mysql.interval_partition_jobs_done SELECT * FROM mysql.interval_partition_jobs where id = %?"
	deleteJobSQL      = "DELETE FROM mysql.interval_partition_jobs WHERE id = %?"
	genJobIdSQL       = "SELECT nextval(mysql.interval_partition_jobs_seq)"
)

func (pm *IntervalPartitionManager) HandleJob(job *Job, info *TablePartition) error {
	// check table info.
	ok := pm.checkJobValid(job)
	if !ok {
		job.state = JobStateCancelled
		return nil
	}

	ctx, err := pm.sessPool.get()
	if err != nil {
		return err
	}
	defer pm.sessPool.put(ctx)

	switch job.state {
	case JobStateNone:
		err := pm.markPartitionReadOnly(ctx, info)
		if err != nil {
			return err
		}
		job.state = JobStateMovingData
	case JobStateMovingData:
		start := time.Now()
		suite := NewCopyDataSuite(job, info, "us-west-2")
		err := suite.CopyDataToAWSS3()
		if err != nil {
			job.state = JobStateCancelled
			logutil.BgLogger().Info("[interval-partition] move table partition data to aws s3 failed",
				zap.Int64("job-id", job.id),
				zap.Duration("cost", time.Since(start)),
				zap.String("table", job.tableName),
				zap.String("partition", job.partitionName),
				zap.Error(err))
			break
		}

		job.state = JobStateUpdateTableMeta
		logutil.BgLogger().Info("[interval-partition] move table partition data to aws s3 success",
			zap.Int64("job-id", job.id),
			zap.Duration("cost", time.Since(start)),
			zap.String("table", job.tableName),
			zap.String("partition", job.partitionName))
	case JobStateUpdateTableMeta:
		err := pm.updatePartitionEngine(ctx, info)
		if err != nil {
			return err
		}
		job.state = JobStateDone
	case JobStateCancelled:
	default:
		logutil.BgLogger().Info("[interval-partition]  unknown state",
			zap.String("table", job.tableName),
			zap.String("partition", job.partitionName),
			zap.String("state", job.state))

		time.Sleep(time.Second)
	}
	return nil
}

func (pm *IntervalPartitionManager) checkJobValid(job *Job) bool {
	is := pm.infoCache.GetLatest()
	tb, ok := is.TableByID(job.tableID)
	if !ok || tb == nil {
		return false
	}
	pi := tb.Meta().GetPartitionInfo()
	if pi == nil || pi.AutoAction.MovePartitionExpr == "" || pi.AutoAction.MoveToEngine == "" {
		return false
	}
	found := false
	for i := range pi.Definitions {
		if pi.Definitions[i].ID == job.partitionID {
			found = true
			break
		}
	}
	if !found {
		return false
	}
	// add more check...

	return true
}

func (pm *IntervalPartitionManager) updateJobState(job *Job) error {
	ctx, err := pm.sessPool.get()
	if err != nil {
		return err
	}
	defer pm.sessPool.put(ctx)

	_, err = ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), updateJobStateSQL, job.state, job.id)
	return errors.Trace(err)
}

func (pm *IntervalPartitionManager) FinishJob(job *Job) error {
	ctx, err := pm.sessPool.get()
	if err != nil {
		return err
	}
	defer pm.sessPool.put(ctx)

	if job.state != JobStateDone {
		err = RemoveDataInAWSS3(job.dbName, job.tableName, job.partitionID, "us-west-2")
		if err != nil {
			logutil.BgLogger().Warn("[interval-partition] remove data in aws s3 failed", zap.Error(err))
		}
	} else if job.state == JobStateDone {
		pm.awsTableMeta.Store(job.partitionID, &PartitionTableMeta{
			tableID:   job.tableID,
			pid:       job.partitionID,
			db:        job.dbName,
			tableName: job.tableName,
		})
	}

	_, err = ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), insertDoneJobSQL, job.id)
	if err != nil {
		return err
	}

	_, err = ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), deleteJobSQL, job.id)
	return err
}

func (pm *IntervalPartitionManager) markPartitionReadOnly(ctx sessionctx.Context, info *TablePartition) error {
	return pm.ddl.AlterTablePartitionMeta(ctx, info.dbInfo, info.tbInfo, &ddl.AlterTablePartitionInfo{
		PID:      info.pdInfo.ID,
		ReadOnly: true,
	})
}

func (pm *IntervalPartitionManager) updatePartitionEngine(ctx sessionctx.Context, info *TablePartition) error {
	return pm.ddl.AlterTablePartitionMeta(ctx, info.dbInfo, info.tbInfo, &ddl.AlterTablePartitionInfo{
		PID:      info.pdInfo.ID,
		ReadOnly: true,
		Engine:   AWSS3Engine,
	})
}

func (pm *IntervalPartitionManager) LoadOrCreateJobInfo(info *TablePartition) (*Job, error) {
	job, err := pm.LoadJobInfo(info)
	if err != nil {
		return nil, err
	}
	if job != nil {
		return job, nil
	}

	id, err := pm.genJobID()
	if err != nil {
		return nil, err
	}

	job = &Job{
		id:            id,
		dbName:        info.dbInfo.Name.O,
		tableName:     info.tbInfo.Name.O,
		tableID:       info.tbInfo.ID,
		partitionName: info.pdInfo.Name.O,
		partitionID:   info.pdInfo.ID,
		state:         JobStateNone,
	}

	err = pm.createJobInfo(job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (pm *IntervalPartitionManager) createJobInfo(job *Job) error {
	ctx, err := pm.sessPool.get()
	if err != nil {
		return err
	}
	defer pm.sessPool.put(ctx)

	_, err = ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), insertJobSQL, job.id, job.dbName, job.tableName, job.tableID, job.partitionName, job.partitionID, job.state)
	return err
}

func (pm *IntervalPartitionManager) genJobID() (int64, error) {
	ctx, err := pm.sessPool.get()
	if err != nil {
		return 0, err
	}
	defer pm.sessPool.put(ctx)

	rs, err := ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), genJobIdSQL)
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}

	req := rs.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}

		for row := it.Begin(); row != it.End(); row = it.Next() {
			id := row.GetInt64(0)
			return id, nil
		}
	}
	return 0, errors.New("gen job id return null, should never happen")
}

func (pm *IntervalPartitionManager) LoadJobInfo(info *TablePartition) (*Job, error) {
	return pm.loadJobInfo(loadJobSQL, info.pdInfo.ID)
}

func (pm *IntervalPartitionManager) LoadOldJobInfo() (*Job, error) {
	return pm.loadJobInfo(loadOldJobSQL)
}

func (pm *IntervalPartitionManager) loadJobInfo(query string, args ...interface{}) (*Job, error) {
	ctx, err := pm.sessPool.get()
	if err != nil {
		return nil, err
	}
	defer pm.sessPool.put(ctx)

	rs, err := ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), query, args...)
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	req := rs.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}

		for row := it.Begin(); row != it.End(); row = it.Next() {
			job := &Job{
				id:            row.GetInt64(0),
				dbName:        row.GetString(1),
				tableName:     row.GetString(2),
				tableID:       row.GetInt64(3),
				partitionName: row.GetString(4),
				partitionID:   row.GetInt64(5),
				state:         row.GetString(6),
			}
			return job, nil
		}
	}
	return nil, nil
}
