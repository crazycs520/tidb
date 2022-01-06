package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAll(t *testing.T) {
	name := GetTablePartitionBucketName("t0", 1)
	require.Equal(t, "tidb-interval-partition-t0-p1", name)

	name = GetTablePartitionName("t12345", 67890)
	require.Equal(t, "t12345-p67890", name)

	tb, pid, valid := ParseTablePartitionName("t-p12345")
	require.Equal(t, "t", tb)
	require.Equal(t, int64(12345), pid)
	require.True(t, valid)
	tb, pid, valid = ParseTablePartitionName("t-t-t-p123")
	require.Equal(t, "t-t-t", tb)
	require.Equal(t, int64(123), pid)
	require.True(t, valid)

	invalidName := []string{"", "t0", "t0-p", "t0-"}
	for _, name := range invalidName {
		tb, pid, valid := ParseTablePartitionName(name)
		require.Equal(t, "", tb)
		require.Equal(t, int64(0), pid)
		require.False(t, valid)
	}
}
