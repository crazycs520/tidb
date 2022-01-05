//package awss3
package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const bucketNamePrefix = "tidb-interval-partition-"

func main() {
	region := "us-west-2"
	cli, err := createS3Client(region)
	if err != nil {
		fmt.Println(err)
		return
	}
	//err = createBucketForTablePartition(cli, "t0", 0, region)
	err = deleteBucketForTablePartition(cli, "t0", 0, region)
	fmt.Println(err)
}

func createS3Client(region string) (*s3.S3, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

func createBucketForTablePartition(svc *s3.S3, table string, pid int64, region string) error {
	name := getTablePartitionBucketName(table, pid)
	input := &s3.CreateBucketInput{
		Bucket: aws.String(name),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region),
		},
	}

	result, err := svc.CreateBucket(input)
	fmt.Println(result)
	return err
}

func deleteBucketForTablePartition(svc *s3.S3, table string, pid int64, region string) error {
	name := getTablePartitionBucketName(table, pid)

	iter := s3manager.NewDeleteListIterator(svc, &s3.ListObjectsInput{
		Bucket: aws.String(name),
	})

	err := s3manager.NewBatchDeleteWithClient(svc).Delete(aws.BackgroundContext(), iter)
	if err != nil {
		return err
	}

	result, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	fmt.Println(result)
	return err
}

func deleteDumplingMeta(svc *s3.S3, table string, pid int64, region string) error {
	name := getTablePartitionBucketName(table, pid)
	result, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	fmt.Println(result)
	return err
}

func getTablePartitionBucketName(table string, pid int64) string {
	return bucketNamePrefix + table + "-p" + strconv.FormatInt(pid, 10)
}
