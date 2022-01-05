package util

import "strconv"

const bucketNamePrefix = "tidb-interval-partition-"

func GetTablePartitionBucketName(table string, pid int64) string {
	return bucketNamePrefix + table + "-p" + strconv.FormatInt(pid, 10)
}

func GetTablePartitionName(table string, pid int64) string {
	return table + "-p" + strconv.FormatInt(pid, 10)
}
