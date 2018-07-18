// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"encoding/hex"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"golang.org/x/net/context"
)

const (
	loadDeleteRangeSQL        = `SELECT HIGH_PRIORITY job_id, element_id, start_key, end_key FROM mysql.gc_delete_range WHERE ts < %v`
	recordDoneDeletedRangeSQL = `INSERT IGNORE INTO mysql.gc_delete_range_done SELECT * FROM mysql.gc_delete_range WHERE job_id = %d AND element_id = %d`
	completeDeleteRangeSQL    = `DELETE FROM mysql.gc_delete_range WHERE job_id = %d AND element_id = %d`
	updateDeleteRangeSQL      = `UPDATE mysql.gc_delete_range SET start_key = "%s" WHERE job_id = %d AND element_id = %d AND start_key = "%s"`
)

// DelRangeTask is for run delete-range command in gc_worker.
type DelRangeTask struct {
	JobID, ElementID int64
	StartKey, EndKey []byte
}

// Range returns the range [start, end) to delete.
func (t DelRangeTask) Range() ([]byte, []byte) {
	return t.StartKey, t.EndKey
}

// LoadDeleteRanges loads delete range tasks from gc_delete_range table.
func LoadDeleteRanges(ctx sessionctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	sql := fmt.Sprintf(loadDeleteRangeSQL, safePoint)
	rss, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rss) > 0 {
		defer terror.Call(rss[0].Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	rs := rss[0]
	chk := rs.NewChunk()
	it := chunk.NewIterator4Chunk(chk)
	for {
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}

		for row := it.Begin(); row != it.End(); row = it.Next() {
			startKey, err := hex.DecodeString(row.GetString(2))
			if err != nil {
				return nil, errors.Trace(err)
			}
			endKey, err := hex.DecodeString(row.GetString(3))
			if err != nil {
				return nil, errors.Trace(err)
			}
			ranges = append(ranges, DelRangeTask{
				JobID:     row.GetInt64(0),
				ElementID: row.GetInt64(1),
				StartKey:  startKey,
				EndKey:    endKey,
			})
		}
	}
	return ranges, nil
}

// CompleteDeleteRange moves a record from gc_delete_range table to gc_delete_range_done table.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func CompleteDeleteRange(ctx sessionctx.Context, dr DelRangeTask) error {
	sql := fmt.Sprintf(recordDoneDeletedRangeSQL, dr.JobID, dr.ElementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if err != nil {
		return errors.Trace(err)
	}

	sql = fmt.Sprintf(completeDeleteRangeSQL, dr.JobID, dr.ElementID)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	return errors.Trace(err)
}

// UpdateDeleteRange is only for emulator.
func UpdateDeleteRange(ctx sessionctx.Context, dr DelRangeTask, newStartKey, oldStartKey kv.Key) error {
	newStartKeyHex := hex.EncodeToString(newStartKey)
	oldStartKeyHex := hex.EncodeToString(oldStartKey)
	sql := fmt.Sprintf(updateDeleteRangeSQL, newStartKeyHex, dr.JobID, dr.ElementID, oldStartKeyHex)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	return errors.Trace(err)
}

// DDLServerInfo is DDL server static information.
// DDLServerInfo will store into PD when server start up and delete when DDL closed.
// It will not update when DDL server running. So please only put static information in DDLServerInfo struct.
type DDLServerInfo struct {
	ServerVersionInfo
	ID         string `json:"ddl_id"`
	IP         string `json:"ip"`
	StatusPort uint   `json:"status_port"`
	Lease      string `json:"lease"`
}

// ServerVersionInfo is the server version and git_hash
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}
