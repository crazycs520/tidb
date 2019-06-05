// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SplitIndexRegionExec represents a split index regions executor.
type SplitIndexRegionExec struct {
	baseExecutor

	tableInfo  *model.TableInfo
	indexInfo  *model.IndexInfo
	lower      []types.Datum
	upper      []types.Datum
	num        int
	valueLists [][]types.Datum
}

type splitableStore interface {
	SplitRegionAndScatter(splitKey kv.Key) (uint64, error)
	WaitScatterRegionFinish(regionID uint64) error
}

// Next implements the Executor Next interface.
func (e *SplitIndexRegionExec) Next(ctx context.Context, _ *chunk.RecordBatch) error {
	store := e.ctx.GetStore()
	s, ok := store.(splitableStore)
	if !ok {
		return nil
	}
	splitIdxKeys, err := e.getSplitIdxKeys()
	if err != nil {
		return err
	}
	regionIDs := make([]uint64, 0, len(splitIdxKeys))
	for _, idxKey := range splitIdxKeys {
		regionID, err := s.SplitRegionAndScatter(idxKey)
		if err != nil {
			logutil.Logger(context.Background()).Warn("split table index region failed",
				zap.String("table", e.tableInfo.Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
			continue
		}
		regionIDs = append(regionIDs, regionID)

	}
	if !e.ctx.GetSessionVars().WaitTableSplitFinish {
		return nil
	}
	for _, regionID := range regionIDs {
		err := s.WaitScatterRegionFinish(regionID)
		if err != nil {
			logutil.Logger(context.Background()).Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.String("table", e.tableInfo.Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
		}
	}
	return nil
}

func (e *SplitIndexRegionExec) getSplitIdxKeys() ([][]byte, error) {
	var idxKeys [][]byte
	if e.num > 0 {
		idxKeys = make([][]byte, 0, e.num)
	} else {
		idxKeys = make([][]byte, 0, len(e.valueLists)+1)
	}
	// Split in the start of the index key.
	startIdxKey := tablecodec.EncodeTableIndexPrefix(e.tableInfo.ID, e.indexInfo.ID)
	idxKeys = append(idxKeys, startIdxKey)

	index := tables.NewIndex(e.tableInfo.ID, e.tableInfo, e.indexInfo)
	// Split index regions by user specified value lists.
	if len(e.valueLists) > 0 {
		for _, v := range e.valueLists {
			idxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, v, math.MinInt64, nil)
			if err != nil {
				return nil, err
			}
			idxKeys = append(idxKeys, idxKey)
		}
		return idxKeys, nil
	}
	// Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
	lowerIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.lower, math.MinInt64, nil)
	if err != nil {
		return nil, err
	}
	upperIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.upper, math.MinInt64, nil)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(lowerIdxKey, upperIdxKey) >= 0 {
		lowerStr, err1 := datumSliceToString(e.lower)
		upperStr, err2 := datumSliceToString(e.upper)
		if err1 != nil || err2 != nil {
			return nil, errors.Errorf("Split index region `%v` lower value %v should less than the upper value %v", e.indexInfo.Name, e.lower, e.upper)
		}
		return nil, errors.Errorf("Split index region `%v` lower value %v should less than the upper value %v", e.indexInfo.Name, lowerStr, upperStr)
	}
	return getValuesList(lowerIdxKey, upperIdxKey, e.num, idxKeys), nil
}

// getValuesList is used to get `num` values between lower and upper value.
// To Simplify the explain, suppose lower and upper value type is int64, and lower=0, upper=100, num=10,
// then calculate the step=(upper-lower)/num=10, then the function should return 0+10, 10+10, 20+10... all together 9 (num-1) values.
// then the function will return [10,20,30,40,50,60,70,80,90].
// The difference is the value type of upper,lower is []byte, So I use getUint64FromBytes to convert []byte to uint64.
func getValuesList(lower, upper []byte, num int, valuesList [][]byte) [][]byte {
	commonPrefixIdx := longestCommonPrefixLen(lower, upper)
	step := getStepValue(lower[commonPrefixIdx:], upper[commonPrefixIdx:], num)
	startV := getUint64FromBytes(lower[commonPrefixIdx:], 0)
	// To get `num` regions, only need to split `num-1` idx keys.
	buf := make([]byte, 8)
	for i := 0; i < num-1; i++ {
		value := make([]byte, 0, commonPrefixIdx+8)
		value = append(value, lower[:commonPrefixIdx]...)
		startV += step
		binary.BigEndian.PutUint64(buf, startV)
		value = append(value, buf...)
		valuesList = append(valuesList, value)
	}
	return valuesList
}

// longestCommonPrefixLen gets the longest common prefix byte length.
func longestCommonPrefixLen(s1, s2 []byte) int {
	l := mathutil.Min(len(s1), len(s2))
	i := 0
	for ; i < l; i++ {
		if s1[i] != s2[i] {
			break
		}
	}
	return i
}

// getStepValue gets the step of between the lower and upper value. step = (upper-lower)/num.
// convert byte slice to uint64 first.
func getStepValue(lower, upper []byte, num int) uint64 {
	lowerUint := getUint64FromBytes(lower, 0)
	upperUint := getUint64FromBytes(upper, 0xff)
	return (upperUint - lowerUint) / uint64(num)
}

// getUint64FromBytes gets a uint64 from the `bs` byte slice.
// If len(bs) < 8, then padding with `pad`.
func getUint64FromBytes(bs []byte, pad byte) uint64 {
	buf := bs
	if len(buf) < 8 {
		buf = make([]byte, 0, 8)
		buf = append(buf, bs...)
		for i := len(buf); i < 8; i++ {
			buf = append(buf, pad)
		}
	}
	return binary.BigEndian.Uint64(buf)
}

func datumSliceToString(ds []types.Datum) (string, error) {
	str := "("
	for i, d := range ds {
		s, err := d.ToString()
		if err != nil {
			return str, err
		}
		if i > 0 {
			str += ","
		}
		str += s
	}
	str += ")"
	return str, nil
}
