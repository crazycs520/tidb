package interval

import (
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/interval/athena"
	"github.com/pingcap/tidb/interval/awss3"
	"github.com/pingcap/tidb/interval/dumpling"
	"github.com/pingcap/tidb/interval/util"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type CopyDataSuite struct {
	db            string
	table         string
	partitionName string
	pid           int64
	tbInfo        *model.TableInfo

	region       string
	s3BucketName string
}

func NewCopyDataSuite(job *Job, info *TablePartition, region string) *CopyDataSuite {
	return &CopyDataSuite{
		db:            job.dbName,
		table:         job.tableName,
		partitionName: job.partitionName,
		pid:           job.partitionID,
		tbInfo:        info.tbInfo,
		region:        region,
		s3BucketName:  util.GetTablePartitionBucketName(job.tableName, job.partitionID),
	}
}

func (s *CopyDataSuite) CopyDataToAWSS3() error {
	var err error
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = errors.New("copy data to aws s3 paniced")
		logutil.BgLogger().Error("panic in the CopyDataToAWSS3",
			zap.Reflect("r", r),
			zap.Stack("stack"))
	}()

	err = s.prepareAWSS3Bucket()
	if err != nil {
		return errors.Trace(err)
	}
	err = s.dumpTableDataToS3Bucket()
	if err != nil {
		return errors.Trace(err)
	}
	err = s.createTableInAthena()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func RemoveDataInAWSS3(table string, pid int64, region string) error {
	s3Cli, err := awss3.CreateS3Client(region)
	if err != nil {
		return err
	}
	s3BucketName := util.GetTablePartitionBucketName(table, pid)
	return awss3.DeleteBucketForTablePartition(s3Cli, s3BucketName)
}

func (s *CopyDataSuite) prepareAWSS3Bucket() error {
	s3Cli, err := awss3.CreateS3Client(s.region)
	if err != nil {
		return err
	}

	return awss3.CreateBucketForTablePartition(s3Cli, s.s3BucketName, s.region)
}

func (s *CopyDataSuite) dumpTableDataToS3Bucket() error {
	query := fmt.Sprintf("SELECT * FROM `%v`.`%v` partition (`%v`)", s.db, s.table, s.partitionName)
	cfg := config.GetGlobalConfig()
	host := cfg.Host
	if host == "" {
		host = "0.0.0.0"
	}
	port := strconv.Itoa(int(cfg.Port))
	return dumpling.DumpDataToS3Bucket(host, port, s.s3BucketName, s.region, query)
}

func (s *CopyDataSuite) createTableInAthena() error {
	cli, err := athena.CreateCli(s.region)
	if err != nil {
		return err
	}

	return athena.CreateTable(cli, s.db, s.table, s.pid, s.s3BucketName, s.tbInfo)
}
