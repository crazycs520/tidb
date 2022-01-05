//package athena
package main

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/interval/util"
	"os"
	"time"
)

func main() {
	s3Path := util.GetTablePartitionBucketName("t0", 0)
	cols := []string{"name", "begin"}

	sql := buildCreateTableSQL("t0", 0, s3Path, cols)
	fmt.Println(sql)
	region := "us-west-2"
	cli, err := createCli(region)
	mustNil(err)
	err = createTable(cli, "test", "t0", 0, s3Path, cols)
	mustNil(err)
	err = queryTableData(cli, "test", "t0", 0)
	mustNil(err)
}

func mustNil(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func createCli(region string) (*athena.Athena, error) {
	awscfg := &aws.Config{}
	awscfg.WithRegion(region)
	sess, err := session.NewSession(awscfg)
	if err != nil {
		return nil, err
	}

	svc := athena.New(sess, aws.NewConfig().WithRegion(region))
	return svc, nil
}

func createTable(cli *athena.Athena, db, table string, pid int64, s3Path string, cols []string) error {
	ddlSQL := buildCreateTableSQL(table, pid, util.GetTablePartitionBucketName(table, pid), cols)
	_, err := execQuery(cli, db, ddlSQL)
	return err
}

func queryTableData(cli *athena.Athena, db, table string, pid int64) error {
	tableName := util.GetTablePartitionName(table, pid)
	query := fmt.Sprintf("SELECT * FROM \"%v\".\"%v\" limit 10;", db, tableName)
	result, err := execQuery(cli, db, query)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", result)
	return nil
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

func buildCreateTableSQL(table string, pid int64, s3BucketName string, cols []string) string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("CREATE EXTERNAL TABLE ")
	writeKey(buf, util.GetTablePartitionName(table, pid))
	buf.WriteString(" (")
	for i, col := range cols {
		if i > 0 {
			buf.WriteString(", ")
		}
		writeKey(buf, col)
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
