// Copyright 2026 PingCAP, Inc.
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

package session

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestCheckRangeModeStatementShape(t *testing.T) {
	cases := []struct {
		sql        string
		errContain string
	}{
		{
			sql:        "batch on a limit 1 insert into t1 select * from t",
			errContain: "range mode supports DELETE and UPDATE only",
		},
		{
			sql:        "batch on t.a limit 1 delete t from t join t1 on t.a = t1.a",
			errContain: "range mode supports single-table statements only",
		},
		{
			sql: "batch on a limit 1 delete from t where a > 1",
		},
		{
			sql: "batch on a limit 1 update t set b = b + 1",
		},
		{
			sql:        "batch on a limit 1 update t set b = b + 1 where a <= @upper_bound",
			errContain: "range mode doesn't support user variables",
		},
		{
			sql:        "batch on a limit 1 update t set b = @@sql_mode",
			errContain: "range mode doesn't support user variables",
		},
		{
			sql:        "batch on a limit 1 update t set b = connection_id()",
			errContain: "session-local functions",
		},
		{
			sql:        "batch on a limit 1 update t set b = last_insert_id()",
			errContain: "session-local functions",
		},
	}

	for _, tt := range cases {
		t.Run(tt.sql, func(t *testing.T) {
			stmt := parseNonTransactionalDML(t, tt.sql)
			err := checkRangeModeStatementShape(stmt)
			if err == nil {
				err = checkRangeModeConstraint(stmt, nil, nil, nil, []*ast.TableSource{{}})
			}
			if tt.errContain != "" {
				require.ErrorContains(t, err, tt.errContain)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestNonTransactionalDMLRangeRetryableError(t *testing.T) {
	require.False(t, isNonTransactionalDMLRangeRetryableError(nil))
	require.False(t, isNonTransactionalDMLRangeRetryableError(errors.New("check constraint failed")))
	require.True(t, isNonTransactionalDMLRangeRetryableError(errors.New(nonTransactionalDMLRangeInjectedErrMsg)))
	require.True(t, isNonTransactionalDMLRangeRetryableError(kv.ErrTxnRetryable.GenWithStackByArgs()))
}

func TestBuildNonTransactionalDMLRangeChunkSQLAddsHandleBounds(t *testing.T) {
	stmt := parseNonTransactionalDML(t, "batch on a limit 2 update t set b = b + 1 where b < 10")
	rangeCtx := &nonTransactionalDMLRangeContext{
		stmt:              stmt,
		handleColumn:      stmt.ShardColumn,
		handleColumnType:  *types.NewFieldType(mysql.TypeLong),
		originalCondition: stmt.DMLStmt.WhereExpr(),
	}

	start := int64(3)
	sql, err := buildNonTransactionalDMLRangeChunkSQL(rangeCtx, &start, 7)
	require.NoError(t, err)
	require.Contains(t, sql, "UPDATE `t` SET `b`=(`b` + 1)")
	require.Contains(t, sql, "`a` > 3")
	require.Contains(t, sql, "`a` <= 7")
	require.Contains(t, sql, "`b` < 10")

	whereSQL, err := restoreWhereExpression(stmt.DMLStmt.WhereExpr())
	require.NoError(t, err)
	require.Equal(t, "(`b` < 10)", whereSQL)

	sql, err = buildNonTransactionalDMLRangeChunkSQL(rangeCtx, nil, 5)
	require.NoError(t, err)
	require.Contains(t, sql, "`a` <= 5")
	require.NotContains(t, sql, "`a` >")
	require.Contains(t, sql, "`b` < 10")
}

func TestApplyNonTransactionalDMLWorkerResourceGroup(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.SetResourceGroupName("default")
	vars.StmtCtx.ResourceGroupName = "default"

	applyNonTransactionalDMLWorkerResourceGroup(vars, "rg_session", "rg_stmt")
	require.Equal(t, "rg_session", vars.ResourceGroupName)
	require.Equal(t, "rg_stmt", vars.StmtCtx.ResourceGroupName)

	applyNonTransactionalDMLWorkerResourceGroup(vars, "", "")
	require.Equal(t, "rg_session", vars.ResourceGroupName)
	require.Equal(t, "rg_session", vars.StmtCtx.ResourceGroupName)
}

func TestApplyNonTransactionalDMLWorkerIdentityWithoutPrivilegeManager(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() {
		dom.Close()
		require.NoError(t, store.Close())
	}()
	workerSe := CreateSessionAndSetID(t, store)
	privilege.BindPrivilegeManager(workerSe, nil)

	submitterUser := &auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthUsername: "root", AuthHostname: "%"}
	submitterRoles := []*auth.RoleIdentity{{Username: "ntdml_role", Hostname: "%"}}

	require.NotPanics(t, func() {
		applyNonTransactionalDMLWorkerIdentity(workerSe, submitterUser, submitterRoles)
	})
	require.NotNil(t, workerSe.GetSessionVars().User)
	require.Equal(t, "root", workerSe.GetSessionVars().User.Username)
	require.Equal(t, "127.0.0.1", workerSe.GetSessionVars().User.Hostname)
	require.Len(t, workerSe.GetSessionVars().ActiveRoles, 1)
	require.Equal(t, "ntdml_role", workerSe.GetSessionVars().ActiveRoles[0].Username)

	submitterUser.Username = "changed"
	submitterRoles[0].Username = "changed"
	require.Equal(t, "root", workerSe.GetSessionVars().User.Username)
	require.Equal(t, "ntdml_role", workerSe.GetSessionVars().ActiveRoles[0].Username)
}

func TestNonTransactionalDMLDXFRunSubtaskResumesFromCheckpoint(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() {
		dom.Close()
		require.NoError(t, store.Close())
	}()
	se := CreateSessionAndSetID(t, store)

	MustExec(t, se, "use test")
	MustExec(t, se, "create table t(a int primary key clustered, b int)")
	const rowCount = 30
	for i := 1; i <= rowCount; i++ {
		MustExec(t, se, "insert into t values (?, ?)", i, i)
	}

	taskMeta, subtaskMeta := buildTestNonTransactionalDMLTaskMeta(t, se,
		"batch on a limit 1 update t set b = b + 1 where a <= 30", rowCount)
	executor := &nonTransactionalDMLStepExecutor{taskMeta: taskMeta}

	runFirstNonTransactionalDMLDXFChunk(t, taskMeta, subtaskMeta, se)

	checkpoint, err := loadNonTransactionalDMLRangeCheckpoint(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers), se, taskMeta.JobID, subtaskMeta.RangeID)
	require.NoError(t, err)
	require.Equal(t, "done", checkpoint.status)
	require.NotNil(t, checkpoint.checkpoint)
	require.Equal(t, uint64(1), checkpoint.scanned)
	require.Equal(t, uint64(1), checkpoint.affected)

	resumeSe := CreateSessionAndSetID(t, store)
	MustExec(t, resumeSe, "use test")
	var scanned uint64
	var affected uint64
	require.NoError(t, executor.runSubtaskWithSession(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask),
		resumeSe, subtaskMeta, &scanned, &affected))

	rows := mustRows(t, se, "select count(*) from t where b = a + 1")
	require.Equal(t, int64(rowCount), rows[0].GetInt64(0))
	rows = mustRows(t, se, "select scanned, affected, status from mysql.tidb_nontransactional_dml_checkpoint where job_id = ? and range_id = ?",
		taskMeta.JobID, subtaskMeta.RangeID)
	require.Equal(t, int64(rowCount), rows[0].GetInt64(0))
	require.Equal(t, int64(rowCount), rows[0].GetInt64(1))
	require.Equal(t, "done", rows[0].GetString(2))
}

func TestNonTransactionalDMLDXFRunSubtaskIgnoresRetryableFailedCheckpoint(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() {
		dom.Close()
		require.NoError(t, store.Close())
	}()
	se := CreateSessionAndSetID(t, store)

	MustExec(t, se, "use test")
	MustExec(t, se, "create table t(a int primary key clustered, b int)")
	for i := 1; i <= 3; i++ {
		MustExec(t, se, "insert into t values (?, ?)", i, i)
	}

	taskMeta, subtaskMeta := buildTestNonTransactionalDMLTaskMeta(t, se,
		"batch on a limit 3 update t set b = b + 1 where a <= 3", 3)
	rangeCtx, err := buildNonTransactionalDMLRangeContextFromTaskMeta(taskMeta)
	require.NoError(t, err)
	require.NoError(t, writeNonTransactionalDMLRangeCheckpoint(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers),
		rangeCtx,
		se,
		nonTransactionalDMLRangeChunk{
			jobID:   taskMeta.JobID,
			rangeID: subtaskMeta.RangeID,
			end:     3,
			size:    3,
		},
		"failed",
		0,
		errors.New(nonTransactionalDMLRangeInjectedErrMsg),
	))

	resumeSe := CreateSessionAndSetID(t, store)
	var scanned uint64
	var affected uint64
	executor := &nonTransactionalDMLStepExecutor{taskMeta: taskMeta}
	require.NoError(t, executor.runSubtaskWithSession(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask),
		resumeSe, subtaskMeta, &scanned, &affected))

	rows := mustRows(t, se, "select a, b from t order by a")
	require.Equal(t, int64(2), rows[0].GetInt64(1))
	require.Equal(t, int64(3), rows[1].GetInt64(1))
	require.Equal(t, int64(4), rows[2].GetInt64(1))
	rows = mustRows(t, se, "select scanned, affected, status from mysql.tidb_nontransactional_dml_checkpoint where job_id = ? and range_id = ?",
		taskMeta.JobID, subtaskMeta.RangeID)
	require.Equal(t, int64(3), rows[0].GetInt64(0))
	require.Equal(t, int64(3), rows[0].GetInt64(1))
	require.Equal(t, "done", rows[0].GetString(2))
}

func TestNonTransactionalDMLDXFPlanningUsesTaskSessionContext(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() {
		dom.Close()
		require.NoError(t, store.Close())
	}()
	submitterSe := CreateSessionAndSetID(t, store)
	plannerSe := CreateSessionAndSetID(t, store)

	MustExec(t, submitterSe, "use test")
	MustExec(t, submitterSe, "set time_zone = '+00:00'")
	MustExec(t, submitterSe, "create table t(a int primary key clustered, ts timestamp)")
	MustExec(t, submitterSe, "insert into t values (1, '2020-01-01 00:30:00')")
	MustExec(t, plannerSe, "set time_zone = '+02:00'")

	taskMeta, _ := buildTestNonTransactionalDMLTaskMeta(t, submitterSe,
		"batch on a limit 1 update t set a = a where ts >= '2020-01-01 00:00:00' and ts < '2020-01-01 01:00:00'", 1)
	ranges, err := planNonTransactionalDMLDXFRanges(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask),
		testTaskHandle{se: plannerSe},
		taskMeta,
		1,
	)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	require.Nil(t, ranges[0].RangeStart)
	require.NotNil(t, ranges[0].RangeEnd)
	require.Equal(t, int64(1), *ranges[0].RangeEnd)
}

func TestNonTransactionalDMLDXFTaskMetaAndWorkerPreserveSubmitterIdentity(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() {
		dom.Close()
		require.NoError(t, store.Close())
	}()
	submitterSe := CreateSessionAndSetID(t, store)

	MustExec(t, submitterSe, "use test")
	MustExec(t, submitterSe, "create table t(a int primary key clustered, b int)")
	MustExec(t, submitterSe, "insert into t values (1, 1)")
	submitterSe.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%", AuthUsername: "root", AuthHostname: "%"}
	submitterSe.GetSessionVars().ActiveRoles = []*auth.RoleIdentity{{Username: "ntdml_role", Hostname: "%"}}

	taskMeta, subtaskMeta := buildTestNonTransactionalDMLTaskMeta(t, submitterSe,
		"batch on a limit 1 update t set b = b + 1 where a = 1", 1)
	metaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	var metaJSON map[string]any
	require.NoError(t, json.Unmarshal(metaBytes, &metaJSON))
	require.Contains(t, metaJSON, "user")
	require.Contains(t, metaJSON, "active_roles")

	workerSe := CreateSessionAndSetID(t, store)
	taskMetaValue := reflect.ValueOf(taskMeta).Elem()
	require.True(t, taskMetaValue.FieldByName("User").IsValid())
	require.True(t, taskMetaValue.FieldByName("ActiveRoles").IsValid())

	executor := &nonTransactionalDMLStepExecutor{taskMeta: taskMeta}
	var scanned uint64
	var affected uint64
	require.NoError(t, executor.runSubtaskWithSession(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask),
		workerSe, subtaskMeta, &scanned, &affected))

	require.NotNil(t, workerSe.GetSessionVars().User)
	require.Equal(t, "root", workerSe.GetSessionVars().User.Username)
	require.Equal(t, "%", workerSe.GetSessionVars().User.Hostname)
	require.Len(t, workerSe.GetSessionVars().ActiveRoles, 1)
	require.Equal(t, "ntdml_role", workerSe.GetSessionVars().ActiveRoles[0].Username)
	require.Equal(t, "%", workerSe.GetSessionVars().ActiveRoles[0].Hostname)
}

func TestNonTransactionalDMLDXFCheckpointSummaryCleanupAndResultFallback(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() {
		dom.Close()
		require.NoError(t, store.Close())
	}()
	se := CreateSessionAndSetID(t, store)

	MustExec(t, se, "use test")
	MustExec(t, se, "create table t(a int primary key clustered, b int)")
	MustExec(t, se, "insert into t values (1, 1), (2, 2), (3, 3)")
	taskMeta, _ := buildTestNonTransactionalDMLTaskMeta(t, se,
		"batch on a limit 2 update t set b = b + 1 where a <= 3", 3)
	taskMeta.Ranges = []nonTransactionalDMLSubtaskMeta{
		{RangeID: 1},
		{RangeID: 2},
	}
	rangeCtx, err := buildNonTransactionalDMLRangeContextFromTaskMeta(taskMeta)
	require.NoError(t, err)
	require.NoError(t, writeNonTransactionalDMLRangeCheckpoint(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers),
		rangeCtx,
		se,
		nonTransactionalDMLRangeChunk{jobID: taskMeta.JobID, rangeID: 1, end: 2, size: 2},
		"done",
		2,
		nil,
	))
	require.NoError(t, writeNonTransactionalDMLRangeCheckpoint(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers),
		rangeCtx,
		se,
		nonTransactionalDMLRangeChunk{jobID: taskMeta.JobID, rangeID: 2, end: 3, size: 1},
		"done",
		1,
		nil,
	))

	summary, err := summarizeNonTransactionalDMLRangeCheckpoints(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers), se, taskMeta.JobID)
	require.NoError(t, err)
	require.Equal(t, int64(2), summary.total)
	require.Equal(t, int64(2), summary.done)
	require.Equal(t, int64(0), summary.failed)
	require.Equal(t, uint64(3), summary.scanned)
	require.Equal(t, uint64(3), summary.affected)

	metaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	require.NoError(t, cleanupNonTransactionalDMLDXFCheckpoints(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask),
		se,
		&proto.Task{
			TaskBase: proto.TaskBase{State: proto.TaskStateSucceed},
			Meta:     metaBytes,
		},
	))
	rows := mustRows(t, se, "select count(*) from mysql.tidb_nontransactional_dml_checkpoint where job_id = ?", taskMeta.JobID)
	require.Equal(t, int64(0), rows[0].GetInt64(0))

	rs, err := buildNonTransactionalDMLDXFResults(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask), se, taskMeta)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rs.Close())
	}()
	resultRows, err := GetRows4Test(context.Background(), se, rs)
	require.NoError(t, err)
	require.Len(t, resultRows, 1)
	require.Equal(t, int64(2), resultRows[0].GetInt64(0))
	require.Equal(t, "all succeeded", resultRows[0].GetString(1))
}

func TestNonTransactionalDMLDXFCleanupPreservesFailedCheckpoints(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() {
		dom.Close()
		require.NoError(t, store.Close())
	}()
	se := CreateSessionAndSetID(t, store)

	MustExec(t, se, "use test")
	MustExec(t, se, "create table t(a int primary key clustered, b int)")
	MustExec(t, se, "insert into t values (1, 1)")
	taskMeta, _ := buildTestNonTransactionalDMLTaskMeta(t, se,
		"batch on a limit 1 update t set b = b + 1 where a = 1", 1)
	rangeCtx, err := buildNonTransactionalDMLRangeContextFromTaskMeta(taskMeta)
	require.NoError(t, err)
	require.NoError(t, writeNonTransactionalDMLRangeCheckpoint(
		kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers),
		rangeCtx,
		se,
		nonTransactionalDMLRangeChunk{jobID: taskMeta.JobID, rangeID: 1, end: 1, size: 1},
		"failed",
		0,
		errors.New("permanent failure"),
	))

	metaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	require.NoError(t, cleanupNonTransactionalDMLDXFCheckpoints(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask),
		se,
		&proto.Task{
			TaskBase: proto.TaskBase{State: proto.TaskStateFailed},
			Meta:     metaBytes,
		},
	))
	rows := mustRows(t, se, "select status, error from mysql.tidb_nontransactional_dml_checkpoint where job_id = ?", taskMeta.JobID)
	require.Equal(t, "failed", rows[0].GetString(0))
	require.Contains(t, rows[0].GetString(1), "permanent failure")
}

func TestSplitNonTransactionalDMLSignedHandleRange(t *testing.T) {
	ranges := splitNonTransactionalDMLSignedHandleRange(1, 6, 2)
	require.Len(t, ranges, 2)
	require.Nil(t, ranges[0].RangeStart)
	require.Equal(t, int64(3), *ranges[0].RangeEnd)
	require.Equal(t, int64(3), *ranges[1].RangeStart)
	require.Equal(t, int64(6), *ranges[1].RangeEnd)

	ranges = splitNonTransactionalDMLSignedHandleRange(-2, 2, 10)
	require.Len(t, ranges, 5)
	require.Nil(t, ranges[0].RangeStart)
	for i, r := range ranges {
		require.Equal(t, int64(-2+i), *r.RangeEnd)
		if i > 0 {
			require.Equal(t, *ranges[i-1].RangeEnd, *r.RangeStart)
		}
	}

	ranges = splitNonTransactionalDMLSignedHandleRange(math.MinInt64, math.MinInt64+1, 2)
	require.Len(t, ranges, 2)
	require.Nil(t, ranges[0].RangeStart)
	require.Equal(t, int64(math.MinInt64), *ranges[0].RangeEnd)
	require.Equal(t, int64(math.MinInt64), *ranges[1].RangeStart)
	require.Equal(t, int64(math.MinInt64+1), *ranges[1].RangeEnd)
}

type testTaskHandle struct {
	storage.TaskHandle
	se sessionctx.Context
}

func (h testTaskHandle) WithNewSession(fn func(se sessionctx.Context) error) error {
	return fn(h.se)
}

func (h testTaskHandle) WithNewTxn(_ context.Context, fn func(se sessionctx.Context) error) error {
	return fn(h.se)
}

func (h testTaskHandle) GetPreviousSubtaskMetas(int64, proto.Step) ([][]byte, error) {
	return nil, nil
}

func parseNonTransactionalDML(t *testing.T, sql string) *ast.NonTransactionalDMLStmt {
	t.Helper()

	node, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	stmt, ok := node.(*ast.NonTransactionalDMLStmt)
	require.True(t, ok)
	return stmt
}

func buildTestNonTransactionalDMLTaskMeta(t *testing.T, se sessiontypes.Session, sql string, rangeEnd int64) (*nonTransactionalDMLTaskMeta, *nonTransactionalDMLSubtaskMeta) {
	t.Helper()

	stmt := parseNonTransactionalDML(t, sql)
	ctx := context.Background()
	nodeW := resolve.NewNodeW(stmt)
	require.NoError(t, core.Preprocess(ctx, se, nodeW))
	tableName, _, shardColumnInfo, tableSources, err := buildSelectSQL(stmt, nodeW.GetResolveContext(), se)
	require.NoError(t, err)
	require.NotNil(t, tableName)
	tnW := nodeW.GetResolveContext().GetTableName(tableName)
	require.NotNil(t, tnW)
	rangeCtx, err := buildNonTransactionalDMLRangeContext(stmt, se, tnW, tableName, shardColumnInfo, tableSources)
	require.NoError(t, err)
	taskMeta, err := buildNonTransactionalDMLTaskMeta(rangeCtx, se, int(stmt.Limit))
	require.NoError(t, err)
	taskMeta.JobID = "test-dxf-resume"
	return taskMeta, &nonTransactionalDMLSubtaskMeta{
		RangeID:  1,
		RangeEnd: &rangeEnd,
	}
}

func runFirstNonTransactionalDMLDXFChunk(t *testing.T, taskMeta *nonTransactionalDMLTaskMeta,
	subtaskMeta *nonTransactionalDMLSubtaskMeta, se sessiontypes.Session) {
	t.Helper()

	rangeCtx, err := buildNonTransactionalDMLRangeContextFromTaskMeta(taskMeta)
	require.NoError(t, err)
	handles, err := selectNextNonTransactionalDMLRangeHandles(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask), rangeCtx, se, subtaskMeta.RangeStart, subtaskMeta.RangeEnd, taskMeta.BatchSize)
	require.NoError(t, err)
	require.Len(t, handles, 1)
	sql, err := buildNonTransactionalDMLRangeChunkSQL(rangeCtx, subtaskMeta.RangeStart, handles[0])
	require.NoError(t, err)
	result := executeNonTransactionalDMLRangeChunkWithRetry(
		kv.WithInternalSourceType(context.Background(), kv.InternalDistTask), rangeCtx, se, nonTransactionalDMLRangeChunk{
			jobID:   taskMeta.JobID,
			rangeID: subtaskMeta.RangeID,
			end:     handles[0],
			size:    len(handles),
			sql:     sql,
		})
	require.NoError(t, result.err)
	require.Equal(t, uint64(1), result.affected)
}

func mustRows(t *testing.T, se sessiontypes.Session, sql string, args ...any) []chunk.Row {
	t.Helper()
	rs := MustExecToRecodeSet(t, se, sql, args...)
	defer func() {
		require.NoError(t, rs.Close())
	}()
	rows, err := GetRows4Test(context.Background(), se, rs)
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	return rows
}
