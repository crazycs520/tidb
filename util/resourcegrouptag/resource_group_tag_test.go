// Copyright 2021 PingCAP, Inc.
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

package resourcegrouptag

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tipb/go-tipb"
)

type testUtilsSuite struct{}

var _ = Suite(&testUtilsSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testUtilsSuite) TestResourceGroupTagEncoding(c *C) {
	sqlDigest := parser.NewDigest(nil)
	tag := EncodeResourceGroupTag(sqlDigest, nil)
	c.Assert(len(tag), Equals, 0)
	decodedSQLDigest, err := DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(len(decodedSQLDigest), Equals, 0)

	sqlDigest = parser.NewDigest([]byte{'a', 'a'})
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	// version(1) + prefix(1) + length(1) + content(2hex -> 1byte)
	c.Assert(len(tag), Equals, 4)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, DeepEquals, sqlDigest.Bytes())

	sqlDigest = parser.NewDigest(genRandHex(64))
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, DeepEquals, sqlDigest.Bytes())

	sqlDigest = parser.NewDigest(genRandHex(510))
	tag = EncodeResourceGroupTag(sqlDigest, nil)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	c.Assert(err, IsNil)
	c.Assert(decodedSQLDigest, DeepEquals, sqlDigest.Bytes())
}

func genRandHex(length int) []byte {
	const chars = "0123456789abcdef"
	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = chars[rand.Intn(len(chars))]
	}
	return res
}

func genDigest(str string) []byte {
	hasher := sha256.New()
	hasher.Write([]byte(str))
	return hasher.Sum(nil)
}

func (s *testUtilsSuite) TestResourceGroupTagEncodingPB(c *C) {
	digest1 := genDigest("abc")
	digest2 := genDigest("abcdefg")
	// Test for manualEncode
	data := manualEncodeResourceGroupTag(digest1, digest2)
	c.Assert(len(data), Equals, 69)
	sqlDigest, planDigest, err := manualDecodeResourceGroupTag(data)
	c.Assert(err, IsNil)
	c.Assert(sqlDigest, DeepEquals, digest1)
	c.Assert(planDigest, DeepEquals, digest2)

	// Test for manualEncode sql_digest only
	data = manualEncodeResourceGroupTag(digest1, nil)
	c.Assert(len(data), Equals, 35)
	sqlDigest, planDigest, err = manualDecodeResourceGroupTag(data)
	c.Assert(err, IsNil)
	c.Assert(sqlDigest, DeepEquals, digest1)
	c.Assert(planDigest, IsNil)

	// Test for protobuf
	resourceTag := &tipb.ResourceGroupTag{
		SqlDigest:  digest1,
		PlanDigest: digest2,
	}
	buf, err := resourceTag.Marshal()
	c.Assert(err, IsNil)
	c.Assert(len(buf), Equals, 68)
	tag := &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	c.Assert(err, IsNil)
	c.Assert(tag.SqlDigest, DeepEquals, digest1)
	c.Assert(tag.PlanDigest, DeepEquals, digest2)

	// Test for protobuf sql_digest only
	resourceTag = &tipb.ResourceGroupTag{
		SqlDigest: digest1,
	}
	buf, err = resourceTag.Marshal()
	c.Assert(err, IsNil)
	c.Assert(len(buf), Equals, 34)
	tag = &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	c.Assert(err, IsNil)
	c.Assert(tag.SqlDigest, DeepEquals, digest1)
	c.Assert(tag.PlanDigest, IsNil)

}

func encodeVarint(b []byte, v int64) []byte {
	var data [binary.MaxVarintLen64]byte
	n := binary.PutVarint(data[:], v)
	return append(b, data[:n]...)
}

func decodeVarint(b []byte) ([]byte, int64, error) {
	v, n := binary.Varint(b)
	if n > 0 {
		return b[n:], v, nil
	}
	if n < 0 {
		return nil, 0, errors.New("value larger than 64 bits")
	}
	return nil, 0, errors.New("insufficient bytes to decode value")
}

func manualEncodeResourceGroupTag(sqlDigest []byte, planDigest []byte) []byte {
	buf := make([]byte, 1, len(sqlDigest)+len(planDigest)+8)
	buf[0] = 1 // version
	if len(sqlDigest) > 0 {
		buf = append(buf, 1) // sql digest flag
		buf = encodeVarint(buf, int64(len(sqlDigest)))
		buf = append(buf, sqlDigest...)
	}
	if len(planDigest) > 0 {
		buf = append(buf, 2) // plan digest flag
		buf = encodeVarint(buf, int64(len(planDigest)))
		buf = append(buf, planDigest...)
	}
	return buf
}

func manualDecodeResourceGroupTag(buf []byte) (sqlDigest []byte, planDigest []byte, err error) {
	if len(buf) == 0 {
		return nil, nil, errors.New("invalid")
	}
	if buf[0] != 1 {
		return nil, nil, errors.New("invalid")
	}
	buf = buf[1:]
	var l int64
	for len(buf) > 0 {
		flag := buf[0]
		buf, l, err = decodeVarint(buf[1:])
		if err != nil {
			return nil, nil, errors.New("invalid")
		}
		if len(buf) < int(l) {
			return nil, nil, errors.New("invalid")
		}
		data := make([]byte, l)
		copy(data, buf[:l])
		buf = buf[l:]
		switch flag {
		case 1: // sql_digest
			sqlDigest = data
		case 2: // plan digest
			planDigest = data
		default:
			return nil, nil, errors.New("invalid")
		}
	}
	return
}

func BenchmarkResourceGroupManualEncode(b *testing.B) {
	digest1 := genDigest("abc")
	digest2 := genDigest("abcdefg")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manualEncodeResourceGroupTag(digest1, digest2)
	}
}

func BenchmarkResourceGroupTagPBEncode(b *testing.B) {
	digest1 := genDigest("abc")
	digest2 := genDigest("abcdefg")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resourceTag := &tipb.ResourceGroupTag{
			SqlDigest:  digest1,
			PlanDigest: digest2,
		}
		resourceTag.Marshal()
	}
}

func BenchmarkResourceGroupTagManualDecode(b *testing.B) {
	digest1 := genDigest("abc")
	digest2 := genDigest("abcdefg")
	data := manualEncodeResourceGroupTag(digest1, digest2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manualDecodeResourceGroupTag(data)
	}
}

func BenchmarkResourceGroupTagPBDecode(b *testing.B) {
	digest1 := genDigest("abc")
	digest2 := genDigest("abcdefg")
	resourceTag := &tipb.ResourceGroupTag{
		SqlDigest:  digest1,
		PlanDigest: digest2,
	}
	data, _ := resourceTag.Marshal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tag := &tipb.ResourceGroupTag{}
		tag.Unmarshal(data)
	}
}

func BenchmarkHexDecode(b *testing.B) {
	digest1 := genDigest("abc")
	b.ResetTimer()
	hash := fmt.Sprintf("%x", digest1)
	for i := 0; i < b.N; i++ {
		hex.DecodeString(hash)
	}
}
