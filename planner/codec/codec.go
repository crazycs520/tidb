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

package codec

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
)

const (
	// TreeBody indicates the current operator sub-tree is not finished, still
	// has child operators to be attached on.
	TreeBody = '│'
	// TreeMiddleNode indicates this operator is not the last child of the
	// current sub-tree rooted by its parent.
	TreeMiddleNode = '├'
	// TreeLastNode indicates this operator is the last child of the current
	// sub-tree rooted by its parent.
	TreeLastNode = '└'
	// TreeGap is used to represent the gap between the branches of the tree.
	TreeGap = ' '
	// TreeNodeIdentifier is used to replace the TreeGap once we need to attach
	// a node to a sub-tree.
	TreeNodeIdentifier = '─'
)

const (
	rootTaskType = "0"
	copTaskType  = "1"
)

const (
	lineBreaker    = '\n'
	lineBreakerStr = "\n"
	separator      = '\t'
	separatorStr   = "\t"
)

var decoderPool = sync.Pool{
	New: func() interface{} {
		return &planDecoder{}
	},
}

// DecodePlan use to decode the string to plan tree.
func DecodePlan(planString string) (string, error) {
	if len(planString) == 0 {
		return "", nil
	}
	pd := decoderPool.Get().(*planDecoder)
	defer decoderPool.Put(pd)
	return pd.decode(planString)
}

type planDecoder struct {
	buf       bytes.Buffer
	depths    []int
	indents   [][]rune
	planInfos []*planInfo
}

type planInfo struct {
	depth  int
	fields []string
}

func (pn *planDecoder) decode(planString string) (string, error) {
	str, err := decompress(planString)
	if err != nil {
		return "", err
	}

	nodes := strings.Split(str, lineBreakerStr)
	if len(pn.depths) < len(nodes) {
		pn.depths = make([]int, 0, len(nodes))
		pn.planInfos = make([]*planInfo, 0, len(nodes))
		pn.indents = make([][]rune, 0, len(nodes))
	}
	pn.depths = pn.depths[:0]
	pn.planInfos = pn.planInfos[:0]
	planInfos := pn.planInfos
	for _, node := range nodes {
		p, err := decodePlanInfo(node)
		if err != nil {
			return "", err
		}
		if p == nil {
			continue
		}
		planInfos = append(planInfos, p)
		pn.depths = append(pn.depths, p.depth)
	}

	pn.initPlanTreeIndents()

	for i := 1; i < len(pn.depths); i++ {
		parentIndex := pn.findParentIndex(i)
		pn.fillIndent(parentIndex, i)
	}

	pn.buf.Reset()
	for i, p := range planInfos {
		if i > 0 {
			pn.buf.WriteByte(lineBreaker)
		}
		pn.buf.WriteString(string(pn.indents[i]))
		for j := 0; j < len(p.fields); j++ {
			if j > 0 {
				pn.buf.WriteByte(separator)
			}
			pn.buf.WriteString(p.fields[j])
		}
	}

	return pn.buf.String(), nil
}

func (pn *planDecoder) initPlanTreeIndents() {
	pn.indents = pn.indents[:0]
	for i := 0; i < len(pn.depths); i++ {
		indent := make([]rune, 2*pn.depths[i])
		pn.indents = append(pn.indents, indent)
		if len(indent) == 0 {
			continue
		}
		for i := 0; i < len(indent)-2; i++ {
			indent[i] = ' '
		}
		indent[len(indent)-2] = TreeLastNode
		indent[len(indent)-1] = TreeNodeIdentifier
	}
}

func (pn *planDecoder) findParentIndex(childIndex int) int {
	for i := childIndex - 1; i > 0; i-- {
		if pn.depths[i]+1 == pn.depths[childIndex] {
			return i
		}
	}
	return 0
}
func (pn *planDecoder) fillIndent(parentIndex, childIndex int) {
	depth := pn.depths[childIndex]
	if depth == 0 {
		return
	}
	idx := depth*2 - 2
	for i := childIndex - 1; i > parentIndex; i-- {
		if pn.indents[i][idx] == TreeLastNode {
			pn.indents[i][idx] = TreeMiddleNode
			break
		}
		pn.indents[i][idx] = TreeBody
	}
}

func decodePlanInfo(str string) (*planInfo, error) {
	values := strings.Split(str, separatorStr)
	if len(values) < 2 {
		return nil, nil
	}

	p := &planInfo{
		fields: make([]string, 0, len(values)-1),
	}
	for i, v := range values {
		switch i {
		// depth
		case 0:
			depth, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, depth: %v, error: %v", str, v, err)
			}
			p.depth = depth
		// plan ID
		case 1:
			ids := strings.Split(v, idSeparator)
			if len(ids) != 2 {
				return nil, errors.Errorf("decode plan: %v error, invalid plan id: %v", str, v)
			}
			planID, err := strconv.Atoi(ids[0])
			if err != err {
				return nil, errors.Errorf("decode plan: %v, plan id: %v, error: %v", str, v, err)
			}
			p.fields = append(p.fields, PhysicalIDToTypeString(planID)+idSeparator+ids[1])
		// task type
		case 2:
			if v == rootTaskType {
				p.fields = append(p.fields, "root")
			} else {
				p.fields = append(p.fields, "cop")
			}
		default:
			p.fields = append(p.fields, v)
		}
	}
	return p, nil
}

// EncodePlanNode is used to encode the plan to a string.
func EncodePlanNode(depth, pid int, planType string, isRoot bool, rowCount float64, explainInfo string, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(depth))
	buf.WriteByte(separator)
	buf.WriteString(encodeID(planType, pid))
	buf.WriteByte(separator)
	if isRoot {
		buf.WriteString(rootTaskType)
	} else {
		buf.WriteString(copTaskType)
	}
	buf.WriteByte(separator)
	buf.WriteString(strconv.FormatFloat(rowCount, 'f', -1, 64))
	buf.WriteByte(separator)
	buf.WriteString(explainInfo)
	buf.WriteByte(lineBreaker)
}

const idSeparator = "_"

func encodeID(planType string, id int) string {
	planID := TypeStringToPhysicalID(planType)
	return strconv.Itoa(planID) + idSeparator + strconv.Itoa(id)
}

// Compress is used to compress the input with zlib.
func Compress(input []byte) (string, error) {
	var in bytes.Buffer
	w, err := zlib.NewWriterLevel(&in, zlib.BestCompression)
	if err != nil {
		return "", err
	}
	_, err = w.Write(input)
	if err != nil {
		return "", err
	}
	err = w.Close()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(in.Bytes()), nil
}

func decompress(str string) (string, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}
	reader := bytes.NewReader(decodeBytes)
	out, err := zlib.NewReader(reader)
	if err != nil {
		return "", err
	}
	var outbuf bytes.Buffer
	_, err = io.Copy(&outbuf, out)
	if err != nil {
		return "", err
	}
	return outbuf.String(), nil
}
