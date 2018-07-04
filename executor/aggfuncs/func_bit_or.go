// Copyright 2018 PingCAP, Inc.
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

package aggfuncs

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type bitOrUint64 struct {
	baseAggFunc
}

type partialResult4BitFunc struct {
	value uint64
}

func (e *bitOrUint64) AllocPartialResult() PartialResult {
	return PartialResult(&partialResult4BitFunc{})
}

func (e *bitOrUint64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4BitFunc)(pr)
	p.value = 0
}

func (e *bitOrUint64) AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4BitFunc)(pr)
	chk.AppendUint64(e.ordinal, p.value)
	return nil
}

func (e *bitOrUint64) UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4BitFunc)(pr)
	for _, row := range rowsInGroup {
		inputValue, isNull, err := e.args[0].EvalInt(sctx, row)
		if err != nil {
			return errors.Trace(err)
		}

		if isNull {
			continue
		}
		p.value |= uint64(inputValue)
	}
	return nil
}
