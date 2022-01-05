//package awss3
package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const bucketNamePrefix = "tidb-interval-partition-"

func main() {
	region := "us-west-2"
	cli, err := CreateS3Client(region)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = CreateBucketForTablePartition(cli, "t0", 0, region)
	//err = deleteBucketForTablePartition(cli, "t0", 0, region)
	fmt.Println(err)
}

func CreateS3Client(region string) (*s3.S3, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

func CreateBucketForTablePartition(svc *s3.S3, table string, pid int64, region string) error {
	name := getTablePartitionBucketName(table, pid)
	input := &s3.CreateBucketInput{
		Bucket: aws.String(name),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region),
		},
	}

	_, err := svc.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists, s3.ErrCodeBucketAlreadyOwnedByYou:
				// delete all item
				return deleteAllBucketItem(svc, name)
			}
		}
	}
	return err
}

func deleteBucketForTablePartition(svc *s3.S3, table string, pid int64) error {
	name := getTablePartitionBucketName(table, pid)

	err := deleteAllBucketItem(svc, name)
	if err != nil {
		return err
	}

	_, err = svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}

func deleteAllBucketItem(svc *s3.S3, bucked string) error {
	iter := s3manager.NewDeleteListIterator(svc, &s3.ListObjectsInput{
		Bucket: aws.String(bucked),
	})

	err := s3manager.NewBatchDeleteWithClient(svc).Delete(aws.BackgroundContext(), iter)
	if err != nil {
		if strings.Contains(err.Error(), "BatchedDeleteIncomplete") {
			return nil
		}
		return err
	}
	return nil
}

func getTablePartitionBucketName(table string, pid int64) string {
	return bucketNamePrefix + table + "-p" + strconv.FormatInt(pid, 10)
}
