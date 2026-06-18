// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nontransactionaltest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

type prometheusCounterCheck struct {
	metric prometheus.Counter
	diff   int
}

func readPrometheusCounter(t *testing.T, counter prometheus.Counter) float64 {
	var metric dto.Metric
	require.NoError(t, counter.Write(&metric))
	return metric.Counter.GetValue()
}

func readPrometheusCounters(t *testing.T, checks []prometheusCounterCheck) []float64 {
	counters := make([]float64, len(checks))
	for i, check := range checks {
		counters[i] = readPrometheusCounter(t, check.metric)
	}
	return counters
}

func checkPrometheusCounterDiffs(t *testing.T, checks []prometheusCounterCheck, before []float64) {
	after := readPrometheusCounters(t, checks)
	for i, check := range checks {
		require.Equal(t, check.diff, int(after[i]-before[i]+0.001), "metric %s should increase by %d", check.metric.Desc().String(), check.diff)
	}
}

func TestNonTransactionalDMLSharding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")

	// On int
	tables := []string{
		"create table t(a int, b int, primary key(a, b) clustered)",
		"create table t(a int, b int, primary key(a, b) nonclustered)",
		"create table t(a int, b int, primary key(a) clustered)",
		"create table t(a int, b int, primary key(a) nonclustered)",
		"create table t(a int, b int, key(a, b))",
		"create table t(a int, b int, key(a))",
		"create table t(a int, b int, unique key(a, b))",
		"create table t(a int, b int, unique key(a))",
	}
	testSharding(tables, tk, "int")

	// On varchar
	tables = []string{
		"create table t(a varchar(30), b int, primary key(a, b) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a) clustered)",
		"create table t(a varchar(30), b int, primary key(a) nonclustered)",
		"create table t(a varchar(30), b int, key(a, b))",
		"create table t(a varchar(30), b int, key(a))",
		"create table t(a varchar(30), b int, unique key(a, b))",
		"create table t(a varchar(30), b int, unique key(a))",
	}
	testSharding(tables, tk, "varchar(30)")
}

func testSharding(tables []string, tk *testkit.TestKit, tp string) {
	compositions := []struct{ tableSize, batchSize int }{
		{0, 10},
		{1, 1},
		{1, 2},
		{30, 25},
		{30, 35},
		{35, 25},
		{35, 35},
		{35, 40},
		{40, 25},
		{40, 35},
		{100, 25},
		{100, 40},
	}
	tk.MustExec("drop table if exists t2")
	tk.MustExec(fmt.Sprintf("create table t2(a %s, b int, primary key(a) clustered)", tp))
	for _, table := range tables {
		tk.MustExec("drop table if exists t, t1")
		tk.MustExec(table)
		tk.MustExec(strings.Replace(table, "create table t", "create table t1", 1))
		for _, c := range compositions {
			tk.MustExec("truncate t2")
			rows := make([]string, 0, c.tableSize)
			for i := 0; i < c.tableSize; i++ {
				tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
				tk.MustExec(fmt.Sprintf("insert into t2 values ('%d', %d)", i, i))
				rows = append(rows, fmt.Sprintf("%d %d", i, i*2))
			}
			tk.MustExec("truncate t1")
			tk.MustQuery(
				fmt.Sprintf("batch on a limit %d insert into t1 select * from t", c.batchSize),
			).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (c.tableSize+c.batchSize-1)/c.batchSize)))
			tk.MustQuery("select a, b from t1 order by b").
				Check(testkit.Rows(rows...))
			tk.MustQuery(
				fmt.Sprintf("batch on a limit %d insert into t2 select * from t on duplicate key update t2.b = t.b", c.batchSize),
			).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (c.tableSize+c.batchSize-1)/c.batchSize)))
			tk.MustQuery("select a, b from t2 order by b").
				Check(testkit.Rows(rows...))
			tk.MustQuery(
				fmt.Sprintf(
					"batch on a limit %d update t set b = b * 2", c.batchSize,
				),
			).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (c.tableSize+c.batchSize-1)/c.batchSize)))
			tk.MustQuery("select coalesce(sum(b), 0) from t").Check(
				testkit.Rows(
					fmt.Sprintf(
						"%d", (c.tableSize-1)*c.tableSize*2,
					),
				),
			)
			tk.MustQuery(
				fmt.Sprintf(
					"batch on a limit %d delete from t", c.batchSize,
				),
			).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (c.tableSize+c.batchSize-1)/c.batchSize)))
			tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
		}
	}
}

func TestNonTransactionalDMLErrorMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, primary key(a, b) clustered)")
	tk.MustExec("create table t1(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/batchDMLError")
	err := tk.ExecToErr("batch on a limit 3 insert into t1 select * from t")
	require.EqualError(
		t, err,
		"Early return: error occurred in the first job. All jobs are canceled: injected batch(non-transactional) DML error",
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.EqualError(
		t, err,
		"Early return: error occurred in the first job. All jobs are canceled: injected batch(non-transactional) DML error",
	)
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.EqualError(
		t, err,
		"Early return: error occurred in the first job. All jobs are canceled: injected batch(non-transactional) DML error",
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = 42")
	require.EqualError(
		t, err,
		"Early return: error occurred in the first job. All jobs are canceled: injected batch(non-transactional) DML error",
	)

	tk.MustExec("truncate t")
	tk.MustExec("truncate t1")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=1")

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, injected batch(non-transactional) DML error;\n",
	)
	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5 ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`, injected batch(non-transactional) DML error;\n",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = 42")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: UPDATE `test`.`t` SET `b`=42 WHERE `a` BETWEEN 3 AND 5, injected batch(non-transactional) DML error;\n",
	)

	tk.MustExec("set @@tidb_redact_log=marker")
	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = 32")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: ‹UPDATE `test`.`t` SET `b`=32 WHERE `a` BETWEEN 3 AND 5›, injected batch(non-transactional) DML error;\n",
	)
	tk.MustExec("set @@tidb_redact_log=0")

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.ErrorContains(
		t, err,
		"33/34 jobs failed in the non-transactional DML: job id: 2, estimated size: 3, sql: DELETE FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, injected batch(non-transactional) DML error;\n",
	)

	tk.MustExec("truncate t")
	tk.MustExec("truncate t1")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_nontransactional_ignore_error=0")

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: INSERT INTO `test`.`t1` SELECT * FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5 ON DUPLICATE KEY UPDATE `t1`.`b`=`t`.`b`, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 update t set b = b + 42")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: UPDATE `test`.`t` SET `b`=(`b` + 42) WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)

	require.NoError(
		t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/batchDMLError", `1*return(false)->return(true)`),
	)
	err = tk.ExecToErr("batch on a limit 3 delete from t")
	require.EqualError(
		t, err,
		"[session:8143]non-transactional job failed, job id: 2, total jobs: 34. job range: [KindInt64 3, KindInt64 5], job sql: job id: 2, estimated size: 3, sql: DELETE FROM `test`.`t` WHERE `a` BETWEEN 3 AND 5, err: injected batch(non-transactional) DML error",
	)
}

func TestNonTransactionalWithCheckConstraint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t1(a int, b int, key(a))")

	checkFn := func() {
		tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
		tk.MustQuery("select count(*) from t1").Check(testkit.Rows("0"))
	}

	// For mocked tikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	now := time.Now()
	safePointValue := now.Format(tikvutil.GCTimeFormat)
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(
		"INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s') ON DUPLICATE KEY UPDATE variable_value = '%[2]s', comment = '%[3]s'",
		safePointName, safePointValue, safePointComment,
	)
	tk.MustExec(updateSafePoint)

	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("set @a=now(6)")

	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.MustExec("set @@tidb_snapshot=@a")
	err := tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("set @@tidb_snapshot=''")
	checkFn()

	tk.MustExec("set @@tidb_read_consistency=weak")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("set @@tidb_read_consistency=strict")
	checkFn()

	tk.MustExec("set autocommit=0")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("commit")
	tk.MustExec("set autocommit=1")
	checkFn()

	tk.MustExec("begin")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("commit")
	checkFn()

	tk.MustExec("SET GLOBAL tidb_enable_batch_dml = 1")
	tk.MustExec("SET tidb_batch_insert = 1")
	tk.MustExec("SET tidb_dml_batch_size = 1")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b")
	require.Error(t, err)
	err = tk.ExecToErr("batch on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustExec("SET GLOBAL tidb_enable_batch_dml = 0")
	tk.MustExec("SET tidb_batch_insert = 0")
	tk.MustExec("SET tidb_dml_batch_size = 0")
	checkFn()

	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t limit 10")
	require.EqualError(t, err, "Non-transactional statements don't support limit")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t limit 10 on duplicate key update t1.b=t.b")
	require.EqualError(t, err, "Non-transactional statements don't support limit")
	err = tk.ExecToErr("batch on a limit 10 delete from t limit 10")
	require.EqualError(t, err, "Non-transactional statements don't support limit")
	checkFn()

	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t order by a")
	require.EqualError(t, err, "Non-transactional statements don't support order by")
	err = tk.ExecToErr("batch on a limit 10 insert into t1 select * from t order by a on duplicate key update t1.b=t.b")
	require.EqualError(t, err, "Non-transactional statements don't support order by")
	err = tk.ExecToErr("batch on a limit 10 delete from t order by a")
	require.EqualError(t, err, "Non-transactional statements don't support order by")
	checkFn()

	err = tk.ExecToErr("prepare nt FROM 'batch limit 1 insert into t1 select * from t'")
	require.EqualError(t, err, "[executor:1295]This command is not supported in the prepared statement protocol yet")
	err = tk.ExecToErr("prepare nt FROM 'batch on a limit 10 insert into t1 select * from t on duplicate key update t1.b=t.b'")
	require.EqualError(t, err, "[executor:1295]This command is not supported in the prepared statement protocol yet")
	err = tk.ExecToErr("prepare nt FROM 'batch limit 1 delete from t'")
	require.EqualError(t, err, "[executor:1295]This command is not supported in the prepared statement protocol yet")

	err = tk.ExecToErr("batch limit 1 insert into t select 1, 1")
	require.EqualError(t, err, "table reference is nil")
	err = tk.ExecToErr("batch limit 1 insert into t select * from (select 1, 2) tmp")
	require.EqualError(t, err, "Non-transactional DML, table name not found in join")
}

func TestNonTransactionalDMLRangeModeRejectsUnsupportedShapes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_nontransactional_dml_execution_mode='range'")
	tk.MustExec("create table t(a int primary key clustered, b int, key idx_b(b))")
	tk.MustExec("create table t1(a int primary key clustered, b int)")
	tk.MustExec("create table t_part_pk(a int primary key clustered, b int) partition by hash(a) partitions 2")
	tk.MustExec("create table t_part_rowid(a int, b int) partition by hash(a) partitions 2")
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	err := tk.ExecToErr("batch on a limit 1 insert into t1 select * from t")
	require.ErrorContains(t, err, "range mode supports DELETE and UPDATE only")

	err = tk.ExecToErr("batch on t.b limit 1 delete t from t join t1 on t.a = t1.a")
	require.ErrorContains(t, err, "range mode supports single-table statements only")

	err = tk.ExecToErr("batch on b limit 1 delete from t")
	require.ErrorContains(t, err, "range mode requires _tidb_rowid or a single signed integer clustered primary key")

	err = tk.ExecToErr("batch on a limit 1 update t set a = a + 10")
	require.ErrorContains(t, err, "shard column cannot be updated")

	tk.MustExec("set @upper_bound = 2")
	err = tk.ExecToErr("batch on a limit 1 update t set b = b + 10 where a <= @upper_bound")
	require.ErrorContains(t, err, "range mode doesn't support user variables")
	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows("1 1", "2 2"))

	err = tk.ExecToErr("batch on a limit 1 update t set b = @@sql_mode where a <= 2")
	require.ErrorContains(t, err, "range mode doesn't support user variables")

	err = tk.ExecToErr("batch on a limit 1 update t set b = connection_id() where a <= 2")
	require.ErrorContains(t, err, "session-local functions")

	err = tk.ExecToErr("batch on a limit 1 update t_part_pk set b = b + 1")
	require.ErrorContains(t, err, "range mode doesn't support partitioned tables")

	err = tk.ExecToErr("batch on _tidb_rowid limit 1 update t_part_rowid set b = b + 1")
	require.ErrorContains(t, err, "range mode doesn't support partitioned tables")
}

func TestNonTransactionalDMLRangeModeDeleteAndUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_nontransactional_dml_execution_mode='range'")
	tk.MustExec("set @@tidb_nontransactional_dml_concurrency=2")
	tk.MustExec("create table t(a int primary key clustered, b int)")
	for i := 1; i <= 9; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	updateMetricChecks := []prometheusCounterCheck{
		{metrics.NonTransactionalDMLTaskCounter.WithLabelValues("range", "update", "submitted"), 1},
		{metrics.NonTransactionalDMLTaskCounter.WithLabelValues("range", "update", metrics.LblOK), 1},
		{metrics.NonTransactionalDMLChunkCounter.WithLabelValues("range", "update", "done"), 3},
		{metrics.NonTransactionalDMLRowsCounter.WithLabelValues("range", "update", "scanned"), 6},
		{metrics.NonTransactionalDMLRowsCounter.WithLabelValues("range", "update", "affected"), 6},
	}
	updateBefore := readPrometheusCounters(t, updateMetricChecks)
	tk.MustQuery("batch on a limit 2 update t set b = b + 10 where a <= 6").
		Check(testkit.Rows("3 all succeeded"))
	checkPrometheusCounterDiffs(t, updateMetricChecks, updateBefore)
	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows(
		"1 11", "2 12", "3 13", "4 14", "5 15", "6 16", "7 7", "8 8", "9 9",
	))

	deleteMetricChecks := []prometheusCounterCheck{
		{metrics.NonTransactionalDMLTaskCounter.WithLabelValues("range", "delete", "submitted"), 1},
		{metrics.NonTransactionalDMLTaskCounter.WithLabelValues("range", "delete", metrics.LblOK), 1},
		{metrics.NonTransactionalDMLChunkCounter.WithLabelValues("range", "delete", "done"), 2},
		{metrics.NonTransactionalDMLRowsCounter.WithLabelValues("range", "delete", "scanned"), 4},
		{metrics.NonTransactionalDMLRowsCounter.WithLabelValues("range", "delete", "affected"), 4},
	}
	deleteBefore := readPrometheusCounters(t, deleteMetricChecks)
	tk.MustQuery("batch on a limit 3 delete from t where b >= 13").
		Check(testkit.Rows("2 all succeeded"))
	checkPrometheusCounterDiffs(t, deleteMetricChecks, deleteBefore)
	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows(
		"1 11", "2 12", "7 7", "8 8", "9 9",
	))

	tk.MustQuery("select count(*) from mysql.tidb_nontransactional_dml_checkpoint where current_db = 'test' and table_name = 't' and status = 'done'").
		Check(testkit.Rows("5"))
}

func TestNonTransactionalDMLRangeModeRetriesChunkBeforeCommit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	require.NoError(t, failpoint.Enable(
		"github.com/pingcap/tidb/pkg/session/nonTransactionalDMLRangeChunkRetryableError",
		`1*return(true)->return(false)`,
	))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/session/nonTransactionalDMLRangeChunkRetryableError"))
	}()

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_nontransactional_dml_execution_mode='range'")
	tk.MustExec("create table t(a int primary key clustered, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")

	tk.MustQuery("batch on a limit 3 update t set b = b + 1").
		Check(testkit.Rows("1 all succeeded"))
	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows("1 2", "2 3", "3 4"))
	tk.MustQuery("select status, count(*) from mysql.tidb_nontransactional_dml_checkpoint where current_db = 'test' and table_name = 't' group by status").
		Check(testkit.Rows("done 1"))
}

func TestNonTransactionalDMLDXFModeDeleteAndUpdate(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 0, 16, true)
	// Keep test DXF exec IDs separate from the domain dist-task manager's test ID.
	c.ScaleOutBy(":5100", true)
	c.ScaleOutBy(":5101", false)
	tk := testkit.NewTestKit(t, c.Store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_nontransactional_dml_execution_mode='dxf'")
	tk.MustExec("set @@tidb_nontransactional_dml_concurrency=2")
	tk.MustExec("create table t(a int primary key clustered, b int)")
	for i := 1; i <= 9; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	updateMetricChecks := []prometheusCounterCheck{
		{metrics.NonTransactionalDMLTaskCounter.WithLabelValues("dxf", "update", "submitted"), 1},
		{metrics.NonTransactionalDMLTaskCounter.WithLabelValues("dxf", "update", metrics.LblOK), 1},
	}
	updateBefore := readPrometheusCounters(t, updateMetricChecks)
	tk.MustQuery("batch on a limit 2 update t set b = b + 10 where a <= 6").
		Check(testkit.Rows("2 all succeeded"))
	checkPrometheusCounterDiffs(t, updateMetricChecks, updateBefore)
	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows(
		"1 11", "2 12", "3 13", "4 14", "5 15", "6 16", "7 7", "8 8", "9 9",
	))

	deleteMetricChecks := []prometheusCounterCheck{
		{metrics.NonTransactionalDMLTaskCounter.WithLabelValues("dxf", "delete", "submitted"), 1},
		{metrics.NonTransactionalDMLTaskCounter.WithLabelValues("dxf", "delete", metrics.LblOK), 1},
	}
	deleteBefore := readPrometheusCounters(t, deleteMetricChecks)
	tk.MustQuery("batch on a limit 3 delete from t where b >= 13").
		Check(testkit.Rows("2 all succeeded"))
	checkPrometheusCounterDiffs(t, deleteMetricChecks, deleteBefore)
	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows(
		"1 11", "2 12", "7 7", "8 8", "9 9",
	))

	tk.MustQuery(`select count(*) from (
		select type, state from mysql.tidb_global_task
		union all
		select type, state from mysql.tidb_global_task_history
	) tasks where type = 'NonTransactionalDML' and state = 'succeed'`).Check(testkit.Rows("2"))
	tk.MustQuery(`select count(*) from (
		select t.id from (
			select id, type, state from mysql.tidb_global_task
			union all
			select id, type, state from mysql.tidb_global_task_history
		) t join (
			select task_key, id from mysql.tidb_background_subtask
			union all
			select task_key, id from mysql.tidb_background_subtask_history
		) s on cast(t.id as char) = s.task_key
		where t.type = 'NonTransactionalDML' and t.state = 'succeed'
	) subtasks`).Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from mysql.tidb_nontransactional_dml_checkpoint where current_db = 'test' and table_name = 't' and status <> 'done'").
		Check(testkit.Rows("0"))
}

func TestNonTransactionalDMLDXFModePropagatesResourceGroup(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 0, 16, true)
	c.ScaleOutBy(":5200", true)
	tk := testkit.NewTestKit(t, c.Store)

	tk.MustExec("use test")
	tk.MustExec("create resource group rg_ntdml ru_per_sec=1000")
	tk.MustExec("set resource group rg_ntdml")
	tk.MustExec("set @@tidb_nontransactional_dml_execution_mode='dxf'")
	tk.MustExec("set @@tidb_nontransactional_dml_concurrency=1")
	tk.MustExec("create table t(a int primary key clustered, b int)")
	for i := 1; i <= 4; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	tk.MustQuery("batch on a limit 2 update t set b = b + 1 where a <= 4").
		Check(testkit.Rows("1 all succeeded"))
	tk.MustQuery(`select json_unquote(json_extract(cast(meta as char), '$.resource_group')),
		json_unquote(json_extract(cast(meta as char), '$.stmt_resource_group')) from (
		select meta, type, state from mysql.tidb_global_task
		union all
		select meta, type, state from mysql.tidb_global_task_history
	) tasks where type = 'NonTransactionalDML' and state = 'succeed'`).Check(testkit.Rows("rg_ntdml rg_ntdml"))
	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows(
		"1 2", "2 3", "3 4", "4 5",
	))
}

func TestNonTransactionalDMLDXFModeConcurrencyAndMultiRun(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 0, 16, true)
	c.ScaleOutBy(":5300", true)
	c.ScaleOutBy(":5301", false)
	tk := testkit.NewTestKit(t, c.Store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_nontransactional_dml_execution_mode='dxf'")
	tk.MustExec("set @@tidb_nontransactional_dml_concurrency=2")
	tk.MustExec("create table t(a int primary key clustered, b int)")
	for i := 1; i <= 8; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	tk.MustQuery("batch on a limit 2 update t set b = b + 10 where a <= 8").
		Check(testkit.Rows("2 all succeeded"))

	tk.MustExec("set @@tidb_nontransactional_dml_concurrency=1")
	tk.MustQuery("batch on a limit 4 update t set b = b + 1 where a <= 4").
		Check(testkit.Rows("1 all succeeded"))
	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows(
		"1 12", "2 13", "3 14", "4 15", "5 15", "6 16", "7 17", "8 18",
	))
	tk.MustQuery(`select count(*), count(distinct task_key) from (
		select task_key, type, state from mysql.tidb_global_task
		union all
		select task_key, type, state from mysql.tidb_global_task_history
	) tasks where type = 'NonTransactionalDML' and state = 'succeed'`).Check(testkit.Rows("2 2"))
	tk.MustQuery(`select task_concurrency, count(*) from (
		select t.concurrency as task_concurrency, s.id from (
			select id, type, state, concurrency from mysql.tidb_global_task
			union all
			select id, type, state, concurrency from mysql.tidb_global_task_history
		) t join (
			select task_key, id from mysql.tidb_background_subtask
			union all
			select task_key, id from mysql.tidb_background_subtask_history
		) s on cast(t.id as char) = s.task_key
		where t.type = 'NonTransactionalDML' and t.state = 'succeed'
	) subtasks group by task_concurrency order by task_concurrency`).Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select count(*) from mysql.tidb_nontransactional_dml_checkpoint where current_db = 'test' and table_name = 't' and status <> 'done'").
		Check(testkit.Rows("0"))
}

func TestNonTransactionalDMLWorkWithForeignKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// t1 is the parent table, t2 is the child table, t3 is a helper table.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int, b int, key(a), key(b))")
	tk.MustExec("create table t2(a int, b int, foreign key (a) references t1(a), key(b))")
	tk.MustExec("create table t3(a int, b int, key(a))")

	cleanFn := func() {
		tk.MustExec("truncate t3")
		tk.MustExec("truncate t2")
		// Cannot truncate t1 because it is the parent table
		tk.MustExec("delete from t1")
	}

	// The check should work for INSERT
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
		tk.MustExec(fmt.Sprintf("insert into t3 values (%d, %d)", i, i))
	}
	tk.MustExec("DELETE FROM t1 WHERE a = 55")
	tk.MustContainErrMsg("BATCH ON a LIMIT 10 INSERT INTO t2 SELECT * FROM t3", "Cannot add or update a child row: a foreign key constraint fails")
	// Though it failed, some data is still inserted
	tk.MustQuery("select count(*) from t2").Check(testkit.Rows("50"))
	cleanFn()

	// The check should work for UPDATE
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
	}
	tk.MustExec("DELETE FROM t1 WHERE a = 55")
	for i := 0; i < 100; i++ {
		if i != 55 {
			tk.MustExec(fmt.Sprintf("insert into t2 values (%d, %d)", i, i))
		}
	}
	tk.MustContainErrMsg("BATCH ON b LIMIT 10 UPDATE t2 SET a = a + 1", "Cannot add or update a child row: a foreign key constraint fails")
	tk.MustQuery("select min(a) from t2").Check(testkit.Rows("1"))
	cleanFn()

	// The check should work for DELETE
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
	}
	tk.MustExec("DELETE FROM t1 WHERE a = 55")
	for i := 56; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t2 values (%d, %d)", i, i))
	}
	tk.MustContainErrMsg("BATCH ON b LIMIT 10 DELETE FROM t1", "Cannot delete or update a parent row: a foreign key constraint fails")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("49"))
	cleanFn()
}

func TestNonTransactionalMetrics(t *testing.T) {
	runAndCheck := func(tp string, fn func()) {
		var (
			affectedRowsCounter prometheus.Counter
			stmtNodeCounter     prometheus.Counter
		)
		switch tp {
		case "insert":
			affectedRowsCounter = metrics.AffectedRowsCounterNTDMLInsert
			stmtNodeCounter = metrics.StmtNodeCounter.WithLabelValues("NTDML-Insert", "", "default")
		case "replace":
			affectedRowsCounter = metrics.AffectedRowsCounterNTDMLReplace
			stmtNodeCounter = metrics.StmtNodeCounter.WithLabelValues("NTDML-Replace", "", "default")
		case "delete":
			affectedRowsCounter = metrics.AffectedRowsCounterNTDMLDelete
			stmtNodeCounter = metrics.StmtNodeCounter.WithLabelValues("NTDML-Delete", "", "default")
		case "update":
			affectedRowsCounter = metrics.AffectedRowsCounterNTDMLUpdate
			stmtNodeCounter = metrics.StmtNodeCounter.WithLabelValues("NTDML-Update", "", "default")
		default:
			require.Fail(t, "Unknown type of DML", tp)
		}
		checkMetrics := []prometheusCounterCheck{
			{affectedRowsCounter, 100},
			{stmtNodeCounter, 11}, // 1 Select + 10 split DMLs
			{metrics.AffectedRowsCounterInsert, 0},
			{metrics.AffectedRowsCounterReplace, 0},
			{metrics.AffectedRowsCounterDelete, 0},
			{metrics.AffectedRowsCounterUpdate, 0},
			{metrics.StmtNodeCounter.WithLabelValues("Insert", "", "default"), 0},
			{metrics.StmtNodeCounter.WithLabelValues("Replace", "", "default"), 0},
			{metrics.StmtNodeCounter.WithLabelValues("Delete", "", "default"), 0},
			{metrics.StmtNodeCounter.WithLabelValues("Update", "", "default"), 0},
		}

		before := readPrometheusCounters(t, checkMetrics)
		fn()
		checkPrometheusCounterDiffs(t, checkMetrics, before)
	}

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d)", i))
	}
	runAndCheck("insert", func() {
		tk.MustExec("BATCH LIMIT 10 INSERT INTO t2 SELECT * FROM t1")
	})
	runAndCheck("update", func() {
		tk.MustExec("BATCH LIMIT 10 UPDATE t2 SET a = a + 1")
	})
	runAndCheck("delete", func() {
		tk.MustExec("BATCH LIMIT 10 DELETE FROM t2")
	})
	runAndCheck("replace", func() {
		tk.MustExec("BATCH LIMIT 10 REPLACE INTO t2 SELECT * FROM t1")
	})
}

func TestNonTransactionalDmlIgnoreMaxExecutionTime(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=10")
	tk.MustExec("set @@max_execution_time=1000")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := range 100 {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/CheckMaxExecutionTime", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/CheckMaxExecutionTime")
	tk.MustExec("batch on a limit 10 update t set b = b + 1 where b > 0")
}
