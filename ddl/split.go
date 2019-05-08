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

package ddl

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type SplitIndexRegionOpt struct {
	Table *model.TableInfo
	Index *model.IndexInfo

	Min        []byte
	Max        []byte
	Num        int
	ValueLists [][]byte
}

const MaxPreSplitRegionNum = 256

func validateIndexSplitOptions(ctx sessionctx.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo, splitOpt *ast.SplitOption) (*SplitIndexRegionOpt, error) {
	if splitOpt == nil {
		return nil, nil
	}
	checkValue := func(valuesItem []ast.ExprNode) ([]types.Datum, error) {

		values := make([]types.Datum, 0, len(valuesItem))
		for j, valueItem := range valuesItem {
			var expr expression.Expression
			var err error
			switch x := valueItem.(type) {
			case *driver.ValueExpr:
				expr = &expression.Constant{
					Value:   x.Datum,
					RetType: &x.Type,
				}
			default:
				buf := new(bytes.Buffer)
				valueItem.Format(buf)
				expr, err = expression.ParseSimpleExprWithTableInfo(ctx, buf.String(), tblInfo)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}

			constant, ok := expr.(*expression.Constant)
			if !ok {
				return nil, errors.New("expect constant values")
			}
			value, err := constant.Eval(chunk.Row{})
			if err != nil {
				return nil, err
			}
			colOffset := indexInfo.Columns[j].Offset
			value, err = value.ConvertTo(ctx.GetSessionVars().StmtCtx, &tblInfo.Columns[colOffset].FieldType)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			continue
		}
		return values, nil
	}
	splitIndexOpt := &SplitIndexRegionOpt{
		Table: tblInfo,
		Index: indexInfo,
	}

	index := tables.NewIndex(tblInfo.ID, tblInfo, indexInfo)
	if len(splitOpt.ValueLists) > 0 {
		for i, valuesItem := range splitOpt.ValueLists {
			if len(valuesItem) > len(indexInfo.Columns) {
				return nil, table.ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			values, err := checkValue(valuesItem)
			if err != nil {
				return nil, err
			}

			// TODO: store index key in table info.
			idxKey, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, values, math.MinInt64, nil)
			if err != nil {
				return nil, err
			}
			splitIndexOpt.ValueLists = append(splitIndexOpt.ValueLists, idxKey)
		}
		return splitIndexOpt, nil
	}

	checkAndGenValue := func(v []ast.ExprNode, name string) ([]byte, error) {
		if len(v) == 0 {
			return nil, errors.Errorf("Pre-split index `%v` %s value num should more than 0", index.Meta().Name, name)
		}
		value, err := checkValue(v)
		if err != nil {
			return nil, err
		}
		valueBytes, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, value, math.MinInt64, nil)
		if err != nil {
			return nil, err
		}
		return valueBytes, nil
	}

	minValue, err := checkAndGenValue(splitOpt.Min, "min")
	if err != nil {
		return nil, err
	}
	maxValue, err := checkAndGenValue(splitOpt.Max, "max")
	if err != nil {
		return nil, err
	}
	if bytes.Compare(minValue, maxValue) >= 0 {
		return nil, errors.Errorf("Pre-split index `%v` min value %v should less than the max value %v", indexInfo.Name, minValue, maxValue)
	}
	splitIndexOpt.Min = minValue
	splitIndexOpt.Max = maxValue

	if splitOpt.Num > MaxPreSplitRegionNum {
		return nil, errors.Errorf("The pre-split region num is exceed the limit %v", MaxPreSplitRegionNum)
	}
	splitIndexOpt.Num = int(splitOpt.Num)

	return splitIndexOpt, nil
}

func splitIndexRegion(ctx sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) {
	for _, constr := range s.Constraints {
		if constr.Option == nil || constr.Option.SplitOpt == nil {
			continue
		}

		idxInfo := tbInfo.FindIndexByName(strings.ToLower(constr.Name))
		if idxInfo == nil {
			return
		}

		splitOpt, err := validateIndexSplitOptions(ctx, tbInfo, idxInfo, constr.Option.SplitOpt)
		if err != nil {
			return
		}
		if splitOpt == nil {
			return
		}
		err = splitOpt.Split(ctx)
		if err != nil {
			logutil.Logger(context.Background()).Warn("split table index region failed",
				zap.String("table", tbInfo.Name.L),
				zap.String("index", idxInfo.Name.L),
				zap.Error(err))
		}
	}
}

func longestCommonPrefixLen(s1, s2 []byte) int {
	l := len(s1)
	if len(s2) < len(s1) {
		l = len(s2)
	}
	i := 0
	for ; i < l; i++ {
		if s1[i] != s2[i] {
			break
		}
	}
	return i
}

func getDiffBytesValue(startIdx int, min, max []byte) uint64 {
	l := len(min)
	if len(max) < len(min) {
		l = len(max)
	}
	if l-startIdx > 8 {
		l = startIdx + 8
	}
	diff := make([]byte, 0, 8)
	for i := startIdx; i < l; i++ {
		diff = append(diff, max[i]-min[i])
	}
	if len(max) > l {
		for i:=l;i<len(max);i++{
			diff = append(diff, max[i])
			if len(diff) >= 8{
				break
			}
		}
	}
	for i := len(diff); i < 8; i++ {
		diff = append(diff, 0xff)
	}
	diffValue := binary.BigEndian.Uint64(diff)
	fmt.Printf("split index \n common prefix: %x\n \nmin: %x\nmax: %x\ndif: %x\ndiffValue: %v\n-----------------------\n", min[:startIdx], min, max, diff, diffValue)
	return diffValue
}

func getValuesList(min, max []byte, num int) [][]byte {
	startIdx := longestCommonPrefixLen(min, max)
	diffValue := getDiffBytesValue(startIdx, min, max)
	step := diffValue / uint64(num)

	startValueTemp := min[startIdx:]
	if len(startValueTemp) > 8 {
		startValueTemp = startValueTemp[:8]
	}
	startValue := make([]byte, 0, 8)
	startValue = append(startValue, startValueTemp...)
	for i := len(startValue); i < 8; i++ {
		startValue = append(startValue, 0)
	}
	startV := binary.BigEndian.Uint64(startValue)
	valuesList := make([][]byte, 0, num+1)
	valuesList = append(valuesList, min)
	tmp := make([]byte, 8)
	for i := 0; i < num; i++ {
		value := make([]byte, 0, startIdx+8)
		value = append(value, min[:startIdx]...)
		startV += step
		binary.BigEndian.PutUint64(tmp, startV)
		value = append(value, tmp...)
		valuesList = append(valuesList, value)
	}
	return valuesList
}

func (e *SplitIndexRegionOpt) Split(ctx sessionctx.Context) error {
	store := ctx.GetStore()
	s, ok := store.(splitableStore)
	if !ok {
		return nil
	}
	logutil.Logger(context.Background()).Info("start split table index region",
		zap.String("table", e.Table.Name.L),
		zap.String("index", e.Index.Name.L))

	if len(e.ValueLists) == 0 {
		valuesList := getValuesList(e.Min, e.Max, e.Num-1)
		for _, vs := range valuesList {
			fmt.Printf("in : %x\n", vs)
		}
		fmt.Printf("\n--------------------------------------\n\n")
		e.ValueLists = valuesList
	}

	regionIDs := make([]uint64, 0, len(e.ValueLists))

	for _, values := range e.ValueLists {
		regionID, err := s.SplitRegionAndScatter(values)
		if err != nil {
			logutil.Logger(context.Background()).Warn("split table index region failed",
				zap.String("table", e.Table.Name.L),
				zap.String("index", e.Index.Name.L),
				zap.Error(err))
			continue
		}
		regionIDs = append(regionIDs, regionID)

	}
	if !ctx.GetSessionVars().WaitTableSplitFinish {
		return nil
	}
	for _, regionID := range regionIDs {
		err := s.WaitScatterRegionFinish(regionID)
		if err != nil {
			logutil.Logger(context.Background()).Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.String("table", e.Table.Name.L),
				zap.String("index", e.Index.Name.L),
				zap.Error(err))
		}
	}
	return nil
}
