package querycache

import (
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"time"
)

type QueryCacheKey struct {
	// 必要
	sql  string
	args []param.BinaryParam
	vars QueryVars
}

type QueryVars struct {
	TimeZone *time.Location
	SQLMode  mysql.SQLMode
}

type QueryCacheValue struct {
	ReadTs int64
	Chunks []*chunk.Chunk
}
