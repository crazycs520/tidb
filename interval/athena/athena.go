package athena

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type AthenaDB struct {
	region string
	cli    *athena.Athena

	metaMu   sync.Mutex
	dbTables map[string]map[string]struct{} // dbName -> []tableName
}

func NewAthenaDB(region string) *AthenaDB {
	return &AthenaDB{
		region:   region,
		dbTables: make(map[string]map[string]struct{}),
	}
}

func (db *AthenaDB) Init() error {
	cli, err := CreateCli(db.region)
	if err != nil {
		return err
	}

	dbs, err := GetAllDatabase(cli)
	if err != nil {
		return nil
	}
	for _, dbName := range dbs {
		tbs, err := GetAllTables(cli, dbName)
		if err != nil {
			return nil
		}
		tbMaps := make(map[string]struct{})
		for _, tb := range tbs {
			tbMaps[tb] = struct{}{}
		}
		db.dbTables[dbName] = tbMaps
	}
	return nil
}

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

func CreateTable(cli *athena.Athena, db, table string, s3BucketName string, tbInfo *model.TableInfo) error {
	ddlSQL := buildCreateTableSQL(table, s3BucketName, tbInfo)
	_, err := ExecSQL(cli, db, ddlSQL)
	logutil.BgLogger().Info("[athena] create table", zap.String("SQL", ddlSQL), zap.Error(err))
	return err
}

func DropTable(cli *athena.Athena, db, table string) error {
	ddlSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%v`.`%v`;", db, table)
	_, err := ExecSQL(cli, db, ddlSQL)
	logutil.BgLogger().Info("[athena] drop table", zap.String("SQL", ddlSQL), zap.Error(err))
	return err
}

func GetAllTables(cli *athena.Athena, db string) ([]string, error) {
	query := "show tables"
	result, err := ExecSQL(cli, db, query)
	if err != nil {
		return nil, err
	}
	return fetchOneColumnResult(result), nil
}

func GetAllDatabase(cli *athena.Athena) ([]string, error) {
	query := "show databases"
	result, err := ExecSQL(cli, "", query)
	if err != nil {
		return nil, err
	}
	return fetchOneColumnResult(result), nil
}

func fetchOneColumnResult(result *athena.ResultSet) []string {
	if result == nil || len(result.Rows) == 0 {
		return nil
	}
	rows := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row.Data) == 0 || row.Data[0].VarCharValue == nil {
			continue
		}
		rows = append(rows, *row.Data[0].VarCharValue)
	}
	return rows
}

func CreateDatabase(cli *athena.Athena, db string) error {
	query := fmt.Sprintf("create database if not exists `%v`;", db)
	_, err := ExecSQL(cli, "", query)
	return err
}

func DropDatabase(cli *athena.Athena, db string) error {
	query := fmt.Sprintf("drop database if exists `%v`;", db)
	_, err := ExecSQL(cli, db, query)
	return err
}

func DropDatabaseAndAllTables(cli *athena.Athena, db string) error {
	err := DropDatabase(cli, db)
	if err == nil {
		return nil
	}
	if !strings.Contains(err.Error(), "is not empty") {
		return err
	}
	tbs, err := GetAllTables(cli, db)
	if err != nil {
		return err
	}
	for _, tb := range tbs {
		err = DropTable(cli, db, tb)
		if err != nil {
			return err
		}
	}
	return DropDatabase(cli, db)
}

func QueryTableData(cli *athena.Athena, db, query string) (*athena.ResultSet, error) {
	result, err := ExecSQL(cli, db, query)
	if err != nil {
		return nil, err
	}
	return result, nil
}

const (
	QuerySucceeded = "SUCCEEDED"
	QueryFailed    = "FAILED"
	QueryCancelled = "CANCELLED"
)

var defaultDB = "default"

func ExecSQL(cli *athena.Athena, db, query string) (*athena.ResultSet, error) {
	if db == "" {
		db = defaultDB
	}
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
	var state, reason string
	for {
		qrop, err = cli.GetQueryExecutionWithContext(context.Background(), &qri)
		if err != nil {
			return nil, err
		}
		state = *qrop.QueryExecution.Status.State
		if qrop.QueryExecution.Status.StateChangeReason != nil {
			reason = *qrop.QueryExecution.Status.StateChangeReason
		}
		if state == QuerySucceeded || state == QueryFailed || state == QueryCancelled {
			break
		}
		time.Sleep(time.Millisecond * 10)

	}
	if state != QuerySucceeded {
		return nil, fmt.Errorf("execute %v, detail: %v, sql: %v", state, reason, query)
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

func buildCreateTableSQL(table string, s3BucketName string, tbInfo *model.TableInfo) string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString("CREATE EXTERNAL TABLE ")
	writeKey(buf, table)
	buf.WriteString(" (")
	for i, col := range tbInfo.Columns {
		if col.State != model.StatePublic || col.Hidden {
			continue
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		writeKey(buf, col.Name.L)
		buf.WriteString(" ")
		buf.WriteString(getColumnType(col))
	}
	buf.WriteString(" ) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' ")
	buf.WriteString(" STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'  ")
	buf.WriteString(" OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
	buf.WriteString(" LOCATION ")
	buf.WriteString(fmt.Sprintf(" 's3://%v/' ", s3BucketName))
	buf.WriteString(" TBLPROPERTIES ( 'has_encrypted_data'='false' )")
	return buf.String()
}

func getColumnType(col *model.ColumnInfo) string {
	switch col.Tp {
	case mysql.TypeTiny:
		return "TINYINT"
	case mysql.TypeShort:
		return "SMALLINT"
	case mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return "BIGINT"
	case mysql.TypeTimestamp:
		return "VARCHAR(64)"
	case mysql.TypeDouble, mysql.TypeFloat:
		return col.FieldType.String()
	case mysql.TypeVarchar:
		return fmt.Sprintf("VARCHAR(%d)", col.FieldType.Flen)
	case mysql.TypeYear:
		return "BIGINT"
	default:
		return "STRING"
	}
}

func writeKey(buf *bytes.Buffer, name string) {
	buf.WriteByte('`')
	buf.WriteString(name)
	buf.WriteByte('`')
}
