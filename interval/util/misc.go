package util

import (
	"strconv"
	"strings"
)

const bucketNamePrefix = "tidb-interval-partition-"

func GetTablePartitionBucketName(table string, pid int64) string {
	table = strings.ToLower(table)
	return bucketNamePrefix + table + "-p" + strconv.FormatInt(pid, 10)
}

func GetTablePartitionName(table string, pid int64) string {
	table = strings.ToLower(table)
	return table + "-p" + strconv.FormatInt(pid, 10)
}

func ParseTablePartitionName(tableName string) (string, int64, bool) {
	idx := strings.LastIndex(tableName, "-p")
	if idx <= 0 || idx+2 >= len(tableName) {
		return "", 0, false
	}
	pid, err := strconv.ParseInt(tableName[idx+2:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return tableName[:idx], pid, true
}
