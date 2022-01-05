package athena

import (
	"bytes"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/interval/util"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func CreateCli(region string) (*athena.Athena, error) {
	awscfg := &aws.Config{}
	awscfg.WithRegion(region)
	sess, err := session.NewSession(awscfg)
	if err != nil {
		return nil, err
	}

	svc := athena.New(sess, aws.NewConfig().WithRegion(region))
	return svc, nil
}

func CreateTable(cli *athena.Athena, db, table string, pid int64, s3BucketName string, tbInfo *model.TableInfo) error {
	ddlSQL := buildCreateTableSQL(table, pid, s3BucketName, tbInfo)
	_, err := execQuery(cli, db, ddlSQL)
	logutil.BgLogger().Info("[athena] create table", zap.String("SQL", ddlSQL), zap.Error(err))
	return err
}

func DropTable(cli *athena.Athena, db, table string, pid int64) error {
	ddlSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%v`.`%v`;", db, util.GetTablePartitionName(table, pid))
	_, err := execQuery(cli, db, ddlSQL)
	logutil.BgLogger().Info("[athena] drop table", zap.String("SQL", ddlSQL), zap.Error(err))
	return err
}

func QueryTableData(cli *athena.Athena, db, query string) (*athena.ResultSet, error) {
	result, err := execQuery(cli, db, query)
	if err != nil {
		return nil, err
	}
	fmt.Printf("------------------------\n%#v\n", result)
	return result, nil
}

const (
	QuerySucceeded = "SUCCEEDED"
	QueryFailed    = "FAILED"
	QueryCancelled = "CANCELLED"
)

func execQuery(cli *athena.Athena, db, query string) (*athena.ResultSet, error) {
	var s athena.StartQueryExecutionInput
	s.SetQueryString(query)

	var q athena.QueryExecutionContext
	q.SetDatabase(db)
	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	r.SetOutputLocation("s3://athena-query-result-chenshuang-dev3")
	s.SetResultConfiguration(&r)

	result, err := cli.StartQueryExecution(&s)
	if err != nil {
		return nil, err
	}

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*result.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	var state string
	for {
		qrop, err = cli.GetQueryExecutionWithContext(context.Background(), &qri)
		if err != nil {
			return nil, err
		}
		state = *qrop.QueryExecution.Status.State
		if state == QuerySucceeded || state == QueryFailed || state == QueryCancelled {
			break
		}
		time.Sleep(time.Millisecond * 10)

	}
	if state != QuerySucceeded {
		return nil, fmt.Errorf("execute query %v", state)
	}

	var ip athena.GetQueryResultsInput
	ip.SetQueryExecutionId(*result.QueryExecutionId)

	op, err := cli.GetQueryResults(&ip)
	if err != nil {
		return nil, err
	}
	return op.ResultSet, nil
}

type DDLEngine struct{}

func buildCreateTableSQL(table string, pid int64, s3BucketName string, tbInfo *model.TableInfo) string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("CREATE EXTERNAL TABLE ")
	writeKey(buf, util.GetTablePartitionName(table, pid))
	buf.WriteString(" (")
	for i, col := range tbInfo.Columns {
		if col.State != model.StatePublic || col.Hidden {
			continue
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		writeKey(buf, col.Name.L)
		buf.WriteString(" string")
	}
	buf.WriteString(" ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' ")
	buf.WriteString(" OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
	buf.WriteString(" LOCATION ")
	buf.WriteString(fmt.Sprintf(" 's3://%v/' ", s3BucketName))
	buf.WriteString(" TBLPROPERTIES ( 'has_encrypted_data'='false' )")
	return buf.String()
}

func writeKey(buf *bytes.Buffer, name string) {
	buf.WriteByte('`')
	buf.WriteString(name)
	buf.WriteByte('`')
}
