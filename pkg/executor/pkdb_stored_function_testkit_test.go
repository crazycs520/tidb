package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestName(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`CREATE FUNCTION fn_get_department_avg_salary(dept_id INT)
		RETURNS DECIMAL(8,2)
		RETURN (SELECT AVG(salary) FROM employees_622 WHERE department_id = dept_id)`)
}
