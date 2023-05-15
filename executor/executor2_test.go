package executor_test

import (
	"bufio"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func TestIssueCs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cycle (pk int key, val int);")
	tk.MustExec("insert into cycle values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")
	tk.MustQuery("select (val) from cycle where pk = 4;").Check(testkit.Rows("4"))
	// tk2 := testkit.NewTestKit(t, store)
	// tk2.MustExec("use test")
	// tk2.MustExec("create table t (pk int);")
	tk.MustExec("COMMIT;")
	tk.MustExec("set autocommit=1;")
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
	tk.MustQuery("select @@tidb_last_txn_info info;")
	tk.MustExec("set @@tidb_enable_async_commit = 1, @@tidb_enable_1pc = 0;")
	tk.MustQuery("SELECT @@transaction_isolation;")
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
	tk.MustExec("set autocommit=0;")
	tk.MustQuery("select (val) from cycle where pk = 3;").Check(testkit.Rows("3"))
	tk.MustQuery("select (val) from cycle where pk = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select (val) from cycle where pk = 4;").Check(testkit.Rows("4"))
	tk.MustQuery("select (val) from cycle where pk = 5;").Check(testkit.Rows("5"))
	tk.MustQuery("select (val) from cycle where pk = 6;").Check(testkit.Rows("6"))
	tk.MustQuery("select (val) from cycle where pk = 2;").Check(testkit.Rows("2"))
	tk.MustQuery("select (val) from cycle where pk = 0;").Check(testkit.Rows("0"))
	tk.MustQuery("select (val) from cycle where pk = 7;").Check(testkit.Rows("7"))

}

func TestIssueCs2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cycle (pk int key, val int);")
	tk.MustExec("insert into cycle values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);")
	file, err := os.Open("/Users/cs/conn.log")
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	contains := []string{"GENERAL_LOG", "conn=1877950467078619541"}
	sqls := make([]string,0,10240)
	for scanner.Scan() {
		line := scanner.Text()
		match := true
		for _,str := range contains{
			if !strings.Contains(line, str){
				match=false
				break
			}
		}
		if !match{
			continue
		}
		startPrefix := "[sql=\""
		idx := strings.Index(line, startPrefix)
		if idx < 0 {
			continue
		}
		sql := line[idx+len(startPrefix):]
		sql = strings.Trim(sql, "\"]")
		sqls = append(sqls, sql)
	}
	err = scanner.Err()
	require.NoError(t, err)

	for i:=0;i<len(sqls)-1;i++{
		sql := strings.ToLower(sqls[i])
		//fmt.Println(sql)
		if strings.Contains(sql, "select"){
			tk.MustQuery(sql).Rows()
		}else{
			tk.MustExec(sql)
		}
	}
	//tk.MustQuery("select pk,val from cycle order by pk").Check(testkit.Rows("0 2581","1 2518", "2 2557","3 2605","4 2594","5 2559","6 2627", "7 2617"))
	tk.MustQuery("select (val) from cycle where pk = 7").Check(testkit.Rows("2617"))
	tk.MustQuery(sqls[len(sqls)-1]).Check(testkit.Rows("2617"))
}
