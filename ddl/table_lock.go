// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
)

func onLockTables(t *meta.Meta, job *model.Job) (ver int64, err error) {
	arg := &lockTablesArg{}
	if err := job.DecodeArgs(arg); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	fmt.Printf("on lock table: arg: %#v\n---------\n\n", arg)

	// Unlock table first.
	if arg.IndexOfUnlock < len(arg.UnlockTables) {
		return unlockTableReq(t, job, arg)
	}

	// Check table locked by other, this can be only checked at the first time.
	if arg.IndexOfLock == 0 {
		for i, tl := range arg.LockTables {
			job.SchemaID = tl.SchemaID
			job.TableID = tl.TableID
			tbInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
			if err != nil {
				return ver, err
			}
			err = checkTableLocked(tbInfo, arg.LockTables[i].Tp, arg.SessionInfo)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
		job.SchemaState = model.StateDeleteOnly
	}

	// Lock table.
	if arg.IndexOfLock < len(arg.LockTables) {
		job.SchemaID = arg.LockTables[arg.IndexOfLock].SchemaID
		job.TableID = arg.LockTables[arg.IndexOfLock].TableID
		tbInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
		if err != nil {
			return ver, err
		}
		err = checkAndLockTable(tbInfo, arg.IndexOfLock, arg)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		switch tbInfo.Lock.State {
		case model.TableLockStateNone:
			// none -> pre_lock
			tbInfo.Lock.State = model.TableLockStatePreLock
			tbInfo.Lock.TS = t.StartTS
			ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
		case model.TableLockStatePreLock, model.TableLockStatePublic:
			tbInfo.Lock.State = model.TableLockStatePublic
			tbInfo.Lock.TS = t.StartTS
			ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
			arg.IndexOfLock++
			job.Args = []interface{}{arg}
			if arg.IndexOfLock == len(arg.LockTables) {
				// Finish this job.
				job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, nil)
			}
		default:
			job.State = model.JobStateCancelled
			return ver, ErrInvalidTableLockState.GenWithStack("invalid table lock state %v", tbInfo.Lock.State)

		}
	}

	return ver, err
}

func unlockTable(tbInfo *model.TableInfo, arg *lockTablesArg) error {
	if tbInfo.Lock == nil {
		return nil
	}
	sessionIndex := indexOfLockHolder(tbInfo.Lock.Sessions, arg.SessionInfo)
	if sessionIndex < 0 {
		return nil
		// todo: when clean table  lock , session maybe send unlock table even the table lock  maybe not hold by the session.
		//return errors.Errorf("%s isn't holding table %s lock", arg.SessionInfo, tbInfo.Name)
	}
	oldSessionInfo := tbInfo.Lock.Sessions
	tbInfo.Lock.Sessions = oldSessionInfo[:sessionIndex]
	tbInfo.Lock.Sessions = append(tbInfo.Lock.Sessions, oldSessionInfo[sessionIndex+1:]...)
	if len(tbInfo.Lock.Sessions) == 0 {
		tbInfo.Lock = nil
	}
	return nil
}

//
func indexOfLockHolder(sessions []model.SessionInfo, sessionInfo model.SessionInfo) int {
	for i := range sessions {
		if sessions[i].ServerID == sessionInfo.ServerID && sessions[i].SessionID == sessionInfo.SessionID {
			return i
		}
	}
	return -1
}

func checkAndLockTable(tbInfo *model.TableInfo, idx int, arg *lockTablesArg) error {
	if tbInfo.Lock == nil || len(tbInfo.Lock.Sessions) == 0 {
		tbInfo.Lock = &model.TableLockInfo{
			Tp: arg.LockTables[idx].Tp,
		}
		tbInfo.Lock.Sessions = append(tbInfo.Lock.Sessions, arg.SessionInfo)
		return nil
	}
	if tbInfo.Lock.State == model.TableLockStatePreLock {
		return nil
	}
	if tbInfo.Lock.Tp == model.TableLockRead && arg.LockTables[idx].Tp == model.TableLockRead {
		sessionIndex := indexOfLockHolder(tbInfo.Lock.Sessions, arg.SessionInfo)
		// repeat lock.
		if sessionIndex >= 0 {
			return nil
		}
		tbInfo.Lock.Sessions = append(tbInfo.Lock.Sessions, arg.SessionInfo)
		return nil
	}
	sessionIndex := indexOfLockHolder(tbInfo.Lock.Sessions, arg.SessionInfo)
	// repeat lock.
	if sessionIndex >= 0 {
		if tbInfo.Lock.Tp == arg.LockTables[idx].Tp {
			return nil
		}
		if len(tbInfo.Lock.Sessions) == 1 {
			// just change lock tp directly.
			tbInfo.Lock.Tp = arg.LockTables[idx].Tp
			return nil
		}
	}
	return infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.Sessions[0])
}

func checkTableLocked(tbInfo *model.TableInfo, lockTp model.TableLockType, sessionInfo model.SessionInfo) error {
	if tbInfo.Lock == nil || len(tbInfo.Lock.Sessions) == 0 {
		return nil
	}
	// remove this?
	if tbInfo.Lock.State == model.TableLockStatePreLock {
		return nil
	}
	if tbInfo.Lock.Tp == model.TableLockRead && lockTp == model.TableLockRead {
		return nil
	}
	sessionIndex := indexOfLockHolder(tbInfo.Lock.Sessions, sessionInfo)
	// repeat lock.
	if sessionIndex >= 0 {
		if tbInfo.Lock.Tp == lockTp {
			return nil
		}
		if len(tbInfo.Lock.Sessions) == 1 {
			// just change lock tp directly.
			return nil
		}
	}
	return infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.Sessions[0])
}

func unlockTableReq(t *meta.Meta, job *model.Job, arg *lockTablesArg) (ver int64, err error) {
	// Unlock table first.
	if arg.IndexOfUnlock < len(arg.UnlockTables) {
		job.SchemaID = arg.UnlockTables[arg.IndexOfUnlock].SchemaID
		job.TableID = arg.UnlockTables[arg.IndexOfUnlock].TableID
		tbInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
		if err != nil {
			return ver, err
		}
		err = unlockTable(tbInfo, arg)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		ver, err = updateVersionAndTableInfo(t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		arg.IndexOfUnlock++
		job.Args = []interface{}{arg}
		job.SchemaState = model.StateDeleteOnly
	}
	return ver, nil
}

func onUnlockTables(t *meta.Meta, job *model.Job) (ver int64, err error) {
	arg := &lockTablesArg{}
	if err := job.DecodeArgs(arg); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	fmt.Printf("on unlock table: arg: %#v\n---------\n\n", arg)

	ver, err = unlockTableReq(t, job, arg)

	if arg.IndexOfUnlock == len(arg.UnlockTables) {
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, nil)
	}
	return ver, err
}

func hasServerAndSessionID(sessions []model.SessionInfo, serverID string, sessionID uint64) bool {
	for i := range sessions {
		if sessions[i].ServerID == serverID && sessions[i].SessionID == sessionID {
			return true
		}
	}
	return false
}
