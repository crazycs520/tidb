package interval

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"sync"
	"time"
)

type AutoCreatePartitionTask struct {
	TimeZone *time.Location
	dbName   string
	tbInfo   *model.TableInfo
	value    int64
	unsigned bool
	wg       sync.WaitGroup
	err      error
	succ     bool
}

func (pm *IntervalPartitionManager) TryAutoCreateIntervalPartition(ctx sessionctx.Context, dbName string, tbInfo *model.TableInfo, val int64, unsigned bool) (bool, error) {
	task := &AutoCreatePartitionTask{
		TimeZone: ctx.GetSessionVars().StmtCtx.TimeZone,
		dbName:   dbName,
		tbInfo:   tbInfo,
		value:    val,
		unsigned: unsigned,
	}
	task.wg.Add(1)
	pm.taskCh <- task
	task.wg.Wait()
	return task.succ, task.err
}

func (pm *IntervalPartitionManager) RunAutoCreatePartitionLoop() {
	for {
		select {
		case task := <-pm.taskCh:
			task.succ, task.err = pm.handleAutoCreatePartitionTask(task)
			task.wg.Done()
		}
	}
}

func (pm *IntervalPartitionManager) handleAutoCreatePartitionTask(task *AutoCreatePartitionTask) (bool, error) {
	if !pm.isValidTask(task) {
		return false, nil
	}
	ctx, err := pm.sessPool.get()
	if err != nil {
		return false, err
	}
	defer pm.sessPool.put(ctx)

	ctx.GetSessionVars().TimeZone = task.TimeZone
	ctx.GetSessionVars().StmtCtx.TimeZone = task.TimeZone
	value, isMaxValue, err := pm.getTablePartitionMaxValue(ctx, task.tbInfo, task.unsigned)
	if err != nil {
		return false, err
	}
	if isMaxValue {
		return false, nil
	}
	nextValue, err := pm.calculateNextPartitionValue(ctx, task.tbInfo, value)
	if err != nil {
		return false, err
	}
	if nextValue < task.value {
		return false, nil
	}
	partName := fmt.Sprintf("auto_p%v", len(task.tbInfo.Partition.Definitions))
	ddlSQL := fmt.Sprintf("ALTER TABLE `%v`.`%v` ADD PARTITION ( PARTITION %v VALUES LESS THAN (%v))",
		task.dbName, task.tbInfo.Name.O, partName, nextValue)

	_, err = ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), ddlSQL)
	if err != nil {
		return false, err
	}
	logutil.BgLogger().Error("[interval-partition] auto create interval partition success",
		zap.String("db", task.dbName),
		zap.String("table", task.tbInfo.Name.O),
		zap.Int64("next-value", nextValue))
	return true, nil
}

func (pm *IntervalPartitionManager) calculateNextPartitionValue(ctx sessionctx.Context, tbInfo *model.TableInfo, lastValue int64) (int64, error) {
	pi := tbInfo.GetPartitionInfo()
	if pi.Interval.AutoIntervalUnit == "" {
		return lastValue + pi.Interval.AutoIntervalValue, nil
	}
	exprStr := fmt.Sprintf("CAST(UNIX_TIMESTAMP(DATE_ADD(FROM_UNIXTIME(%v), INTERVAL %v %v)) as SIGNED)", lastValue, pi.Interval.AutoIntervalValue, pi.Interval.AutoIntervalUnit)
	e, err := expression.ParseSimpleExprWithTableInfo(ctx, exprStr, &model.TableInfo{})
	if err != nil {
		return 0, err
	}
	res, _, err := e.EvalInt(ctx, chunk.Row{})
	return res, err
}

func (pm *IntervalPartitionManager) getTablePartitionMaxValue(ctx sessionctx.Context, tbInfo *model.TableInfo, unsigned bool) (int64, bool, error) {
	pi := tbInfo.GetPartitionInfo()
	idx := len(pi.Definitions) - 1
	valueStr := pi.Definitions[idx].LessThan[0]
	if valueStr == "MAXVALUE" {
		return 0, true, nil
	}
	value, _, err := getRangeValue(ctx, valueStr, unsigned)
	if err != nil {
		return 0, false, err
	}
	if unsigned {
		return int64(value.(uint64)), false, nil
	}
	return value.(int64), false, nil
}

func (pm *IntervalPartitionManager) isValidTask(task *AutoCreatePartitionTask) bool {
	if task.tbInfo == nil {
		return false
	}
	pi := task.tbInfo.GetPartitionInfo()
	if pi == nil || pi.Type != model.PartitionTypeRange || !pi.Interval.Enable || pi.Interval.AutoIntervalValue == 0 ||
		pi.Expr == "" || len(pi.Definitions[0].LessThan) != 1 {
		return false
	}
	return true
}
