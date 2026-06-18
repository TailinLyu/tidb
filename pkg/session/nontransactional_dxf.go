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
	"fmt"
	"math"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

type nonTransactionalDMLColumnNameMeta struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Name   string `json:"name"`
}

type nonTransactionalDMLTaskMeta struct {
	JobID             string                            `json:"job_id"`
	ExecutableDML     string                            `json:"executable_dml"`
	DisplayDML        string                            `json:"display_dml"`
	DMLType           string                            `json:"dml_type,omitempty"`
	CurrentDB         string                            `json:"current_db"`
	DBName            string                            `json:"db_name"`
	TableName         string                            `json:"table_name"`
	TableID           int64                             `json:"table_id"`
	FromSQL           string                            `json:"from_sql"`
	HandleExprSQL     string                            `json:"handle_expr_sql"`
	HandleColumn      nonTransactionalDMLColumnNameMeta `json:"handle_column"`
	OriginalWhereSQL  string                            `json:"original_where_sql"`
	BatchSize         int                               `json:"batch_size"`
	SysVars           map[string]string                 `json:"sys_vars,omitempty"`
	User              *auth.UserIdentity                `json:"user,omitempty"`
	ActiveRoles       []*auth.RoleIdentity              `json:"active_roles,omitempty"`
	ResourceGroup     string                            `json:"resource_group,omitempty"`
	StmtResourceGroup string                            `json:"stmt_resource_group,omitempty"`
	Ranges            []nonTransactionalDMLSubtaskMeta  `json:"ranges,omitempty"`
}

type nonTransactionalDMLSubtaskMeta struct {
	RangeID    int64  `json:"range_id"`
	RangeStart *int64 `json:"range_start,omitempty"`
	RangeEnd   *int64 `json:"range_end,omitempty"`
	Checkpoint *int64 `json:"checkpoint,omitempty"`
	Scanned    uint64 `json:"scanned,omitempty"`
	Affected   uint64 `json:"affected,omitempty"`
}

type nonTransactionalDMLScheduler struct {
	*scheduler.BaseScheduler
}

type nonTransactionalDMLTaskExecutor struct {
	*taskexecutor.BaseTaskExecutor
}

type nonTransactionalDMLStepExecutor struct {
	taskMeta *nonTransactionalDMLTaskMeta
	taskMgr  *storage.TaskManager
	scanned  atomic.Uint64

	taskexecutor.BaseStepExecutor
}

func init() {
	registerNonTransactionalDMLDXFTask()
}

func registerNonTransactionalDMLDXFTask() {
	scheduler.RegisterSchedulerFactory(
		proto.NonTransactionalDML,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			sch := &nonTransactionalDMLScheduler{
				BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
			}
			sch.BaseScheduler.Extension = sch
			return sch
		},
	)
	taskexecutor.RegisterTaskType(
		proto.NonTransactionalDML,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			executor := &nonTransactionalDMLTaskExecutor{
				BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
			}
			executor.BaseTaskExecutor.Extension = executor
			return executor
		},
	)
	scheduler.RegisterSchedulerCleanUpFactory(
		proto.NonTransactionalDML,
		func() scheduler.CleanUpRoutine {
			return &nonTransactionalDMLCleanUp{}
		},
	)
}

func handleNonTransactionalDMLByDXF(ctx context.Context, stmt *ast.NonTransactionalDMLStmt, se sessiontypes.Session,
	resolveCtx *resolve.Context, tableName *ast.TableName, shardColumnInfo *model.ColumnInfo,
	tableSources []*ast.TableSource) (recordSet sqlexec.RecordSet, retErr error) {
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return handleNonTransactionalDMLByRange(ctx, stmt, se, resolveCtx, tableName, shardColumnInfo, tableSources)
	}
	if se.GetSessionVars().NonTransactionalIgnoreError {
		return nil, errors.New("Non-transactional DML DXF mode doesn't support tidb_nontransactional_ignore_error")
	}
	if err := checkRangeModeConstraint(stmt, se, tableName, shardColumnInfo, tableSources); err != nil {
		return nil, err
	}
	if stmt.DryRun != ast.NoDryRun {
		return nil, errors.New("Non-transactional DML DXF mode doesn't support dry run")
	}
	if stmt.Limit == 0 || stmt.Limit > uint64(math.MaxInt64) {
		return nil, errors.New("Non-transactional DML, batch size should be positive")
	}

	tnW := resolveCtx.GetTableName(tableName)
	if tnW == nil {
		return nil, errors.New("Non-transactional DML DXF mode, table not found")
	}
	rangeCtx, err := buildNonTransactionalDMLRangeContext(stmt, se, tnW, tableName, shardColumnInfo, tableSources)
	if err != nil {
		return nil, err
	}
	rangeCtx.executionMode = session_metrics.NonTransactionalDMLModeDXF
	rangeCtx.dmlType = nonTransactionalDMLStmtType(stmt.DMLStmt)
	taskMeta, err := buildNonTransactionalDMLTaskMeta(rangeCtx, se, int(stmt.Limit))
	if err != nil {
		return nil, err
	}
	taskMetaBytes, err := json.Marshal(taskMeta)
	if err != nil {
		return nil, err
	}

	concurrency := se.GetSessionVars().NonTransactionalDMLConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	taskCtx := kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	task, err := handle.SubmitTask(taskCtx, nonTransactionalDMLTaskKey(taskMeta.JobID),
		proto.NonTransactionalDML, concurrency, "", 0, taskMetaBytes)
	if err != nil {
		return nil, err
	}
	session_metrics.NonTransactionalDMLTaskInc(session_metrics.NonTransactionalDMLModeDXF, taskMeta.DMLType, session_metrics.NonTransactionalDMLTaskSubmitted)
	startTime := time.Now()
	defer func() {
		result := metricsResultLabel(retErr)
		session_metrics.NonTransactionalDMLTaskInc(session_metrics.NonTransactionalDMLModeDXF, taskMeta.DMLType, result)
		session_metrics.NonTransactionalDMLDurationObserve(session_metrics.NonTransactionalDMLModeDXF, taskMeta.DMLType, result, time.Since(startTime).Seconds())
	}()
	logutil.Logger(ctx).Info("Non-transactional DML DXF task submitted",
		zap.Int64("task-id", task.ID),
		zap.String("job-id", taskMeta.JobID),
		zap.String("current-db", taskMeta.CurrentDB),
		zap.String("table", taskMeta.TableName),
		zap.Int("concurrency", concurrency),
		zap.String("dml", taskMeta.DisplayDML))
	if err := handle.WaitTaskDoneOrPaused(taskCtx, task.ID); err != nil {
		return nil, err
	}
	finishedTask, err := taskMgr.GetTaskByIDWithHistory(taskCtx, task.ID)
	if err != nil {
		return nil, err
	}
	if finishedTask.State != proto.TaskStateSucceed {
		return nil, errors.Errorf("Non-transactional DML DXF task stopped with state %s", finishedTask.State)
	}
	finishedMeta, err := unmarshalNonTransactionalDMLTaskMeta(finishedTask.Meta)
	if err != nil {
		return nil, err
	}
	summary, summaryErr := summarizeNonTransactionalDMLRangeCheckpoints(taskCtx, se, finishedMeta.JobID)
	if summaryErr != nil {
		return nil, summaryErr
	}
	logutil.Logger(ctx).Info("Non-transactional DML DXF task finished",
		zap.Int64("task-id", finishedTask.ID),
		zap.String("job-id", finishedMeta.JobID),
		zap.Stringer("state", finishedTask.State),
		zap.Int64("checkpoint-total", summary.total),
		zap.Int64("checkpoint-done", summary.done),
		zap.Int64("checkpoint-failed", summary.failed),
		zap.Uint64("scanned", summary.scanned),
		zap.Uint64("affected", summary.affected))
	return buildNonTransactionalDMLDXFResults(taskCtx, se, finishedMeta)
}

func buildNonTransactionalDMLTaskMeta(rangeCtx *nonTransactionalDMLRangeContext,
	se sessiontypes.Session, batchSize int) (*nonTransactionalDMLTaskMeta, error) {
	executableDML, err := restoreNonTransactionalDMLExecutableDML(rangeCtx.stmt.DMLStmt)
	if err != nil {
		return nil, err
	}
	return &nonTransactionalDMLTaskMeta{
		JobID:             rangeCtx.jobID,
		ExecutableDML:     executableDML,
		DisplayDML:        redact.String(se.GetSessionVars().EnableRedactLog, executableDML),
		DMLType:           rangeCtx.dmlType,
		CurrentDB:         rangeCtx.currentDB,
		DBName:            rangeCtx.dbName,
		TableName:         rangeCtx.tableName,
		TableID:           rangeCtx.tableInfo.ID,
		FromSQL:           rangeCtx.fromSQL,
		HandleExprSQL:     rangeCtx.handleExprSQL,
		HandleColumn:      nonTransactionalDMLColumnNameMetaFromAST(rangeCtx.handleColumn),
		OriginalWhereSQL:  rangeCtx.originalWhereSQL,
		BatchSize:         batchSize,
		SysVars:           collectNonTransactionalDMLWorkerSysVars(se.GetSessionVars()),
		User:              cloneNonTransactionalDMLWorkerUser(se.GetSessionVars().User),
		ActiveRoles:       cloneNonTransactionalDMLWorkerActiveRoles(se.GetSessionVars().ActiveRoles),
		ResourceGroup:     se.GetSessionVars().ResourceGroupName,
		StmtResourceGroup: se.GetSessionVars().StmtCtx.ResourceGroupName,
	}, nil
}

func nonTransactionalDMLColumnNameMetaFromAST(name *ast.ColumnName) nonTransactionalDMLColumnNameMeta {
	if name == nil {
		return nonTransactionalDMLColumnNameMeta{}
	}
	return nonTransactionalDMLColumnNameMeta{
		Schema: name.Schema.O,
		Table:  name.Table.O,
		Name:   name.Name.O,
	}
}

func restoreNonTransactionalDMLExecutableDML(stmt ast.StmtNode) (string, error) {
	var sb strings.Builder
	err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|
		format.RestoreNameBackQuotes|
		format.RestoreSpacesAroundBinaryOperation|
		format.RestoreBracketAroundBinaryOperation|
		format.RestoreStringWithoutCharset, &sb))
	if err != nil {
		return "", errors.Annotate(err, "Failed to restore non-transactional DML for DXF")
	}
	return sb.String(), nil
}

func buildNonTransactionalDMLDXFResults(ctx context.Context, se sessiontypes.Session, taskMeta *nonTransactionalDMLTaskMeta) (sqlexec.RecordSet, error) {
	summary, err := summarizeNonTransactionalDMLRangeCheckpoints(ctx, se, taskMeta.JobID)
	if err != nil {
		return nil, err
	}
	jobCount := int(summary.done)
	if summary.total == 0 && len(taskMeta.Ranges) > 0 {
		jobCount = len(taskMeta.Ranges)
	}
	jobs := make([]job, 0, jobCount)
	for i := 1; i <= jobCount; i++ {
		jobs = append(jobs, job{jobID: i})
	}
	return buildExecuteResults(ctx, jobs, se.GetSessionVars().BatchSize.MaxChunkSize, se.GetSessionVars().EnableRedactLog)
}

func nonTransactionalDMLTaskKey(jobID string) string {
	return fmt.Sprintf("ntdml/%s", jobID)
}

func (s *nonTransactionalDMLScheduler) OnTick(context.Context, *proto.Task) {}

func (s *nonTransactionalDMLScheduler) OnNextSubtasksBatch(ctx context.Context, h storage.TaskHandle,
	task *proto.Task, _ []string, step proto.Step) ([][]byte, error) {
	if step == proto.StepDone {
		return nil, nil
	}
	taskMeta, err := unmarshalNonTransactionalDMLTaskMeta(task.Meta)
	if err != nil {
		return nil, err
	}
	if len(taskMeta.Ranges) == 0 {
		ranges, err := planNonTransactionalDMLDXFRanges(ctx, h, taskMeta, task.Concurrency)
		if err != nil {
			return nil, err
		}
		taskMeta.Ranges = ranges
		task.Meta, err = json.Marshal(taskMeta)
		if err != nil {
			return nil, err
		}
	}
	metas := make([][]byte, 0, len(taskMeta.Ranges))
	for _, rangeMeta := range taskMeta.Ranges {
		metaBytes, err := json.Marshal(rangeMeta)
		if err != nil {
			return nil, err
		}
		metas = append(metas, metaBytes)
	}
	return metas, nil
}

func (s *nonTransactionalDMLScheduler) OnDone(context.Context, storage.TaskHandle, *proto.Task) error {
	return nil
}

func (s *nonTransactionalDMLScheduler) GetEligibleInstances(context.Context, *proto.Task) ([]string, error) {
	return nil, nil
}

func (s *nonTransactionalDMLScheduler) IsRetryableErr(err error) bool {
	return isNonTransactionalDMLRangeRetryableError(err)
}

func (s *nonTransactionalDMLScheduler) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.NonTransactionalDMLStepRun
	case proto.NonTransactionalDMLStepRun:
		return proto.StepDone
	default:
		return proto.StepDone
	}
}

func (s *nonTransactionalDMLScheduler) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}

type nonTransactionalDMLCleanUp struct{}

func (*nonTransactionalDMLCleanUp) CleanUp(ctx context.Context, task *proto.Task) error {
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	return taskMgr.WithNewSession(func(ctxSe sessionctx.Context) error {
		se, ok := ctxSe.(sessiontypes.Session)
		if !ok {
			return errors.New("Non-transactional DML DXF cleanup requires a session executor")
		}
		return cleanupNonTransactionalDMLDXFCheckpoints(kv.WithInternalSourceType(ctx, kv.InternalDistTask), se, task)
	})
}

func cleanupNonTransactionalDMLDXFCheckpoints(ctx context.Context, se sessiontypes.Session, task *proto.Task) (retErr error) {
	result := session_metrics.NonTransactionalDMLCleanupSkipped
	defer func() {
		if retErr != nil {
			result = "error"
		}
		session_metrics.NonTransactionalDMLCheckpointCleanupInc(result)
	}()
	taskMeta, err := unmarshalNonTransactionalDMLTaskMeta(task.Meta)
	if err != nil {
		return err
	}
	summary, err := summarizeNonTransactionalDMLRangeCheckpoints(ctx, se, taskMeta.JobID)
	if err != nil {
		return err
	}
	logutil.Logger(ctx).Info("Non-transactional DML DXF checkpoint cleanup inspected task",
		zap.Int64("task-id", task.ID),
		zap.String("job-id", taskMeta.JobID),
		zap.Stringer("state", task.State),
		zap.Int64("checkpoint-total", summary.total),
		zap.Int64("checkpoint-done", summary.done),
		zap.Int64("checkpoint-failed", summary.failed),
		zap.Uint64("scanned", summary.scanned),
		zap.Uint64("affected", summary.affected))
	if task.State != proto.TaskStateSucceed {
		return nil
	}
	if err := deleteNonTransactionalDMLRangeCheckpoints(ctx, se, taskMeta.JobID); err != nil {
		return err
	}
	result = "ok"
	return nil
}

func (e *nonTransactionalDMLTaskExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

func (e *nonTransactionalDMLTaskExecutor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	taskMeta, err := unmarshalNonTransactionalDMLTaskMeta(task.Meta)
	if err != nil {
		return nil, err
	}
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return nil, err
	}
	return &nonTransactionalDMLStepExecutor{
		taskMeta: taskMeta,
		taskMgr:  taskMgr,
	}, nil
}

func (e *nonTransactionalDMLTaskExecutor) IsRetryableError(err error) bool {
	return isNonTransactionalDMLRangeRetryableError(err)
}

func (e *nonTransactionalDMLStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	var subtaskMeta nonTransactionalDMLSubtaskMeta
	if err := json.Unmarshal(subtask.Meta, &subtaskMeta); err != nil {
		return err
	}

	var scanned uint64
	var affected uint64
	err := e.taskMgr.WithNewSession(func(ctxSe sessionctx.Context) error {
		se, ok := ctxSe.(sessiontypes.Session)
		if !ok {
			return errors.New("Non-transactional DML DXF executor requires a session executor")
		}
		return e.runSubtaskWithSession(kv.WithInternalSourceType(ctx, kv.InternalDistTask), se, &subtaskMeta, &scanned, &affected)
	})
	if err != nil {
		return err
	}
	subtaskMeta.Scanned = scanned
	subtaskMeta.Affected = affected
	subtask.Meta, err = json.Marshal(subtaskMeta)
	return err
}

func (e *nonTransactionalDMLStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return &execute.SubtaskSummary{RowCount: int64(e.scanned.Load())}
}

func (e *nonTransactionalDMLStepExecutor) runSubtaskWithSession(ctx context.Context, se sessiontypes.Session,
	subtaskMeta *nonTransactionalDMLSubtaskMeta, scanned *uint64, affected *uint64) error {
	if err := prepareNonTransactionalDMLDXFSession(ctx, se, e.taskMeta); err != nil {
		return err
	}
	rangeCtx, err := buildNonTransactionalDMLRangeContextFromTaskMeta(e.taskMeta)
	if err != nil {
		return err
	}
	checkpoint, err := loadNonTransactionalDMLRangeCheckpoint(ctx, se, e.taskMeta.JobID, subtaskMeta.RangeID)
	if err != nil {
		return err
	}
	logutil.Logger(ctx).Info("Non-transactional DML DXF subtask started",
		zap.String("job-id", e.taskMeta.JobID),
		zap.Int64("range-id", subtaskMeta.RangeID),
		zap.Any("range-start", subtaskMeta.RangeStart),
		zap.Any("range-end", subtaskMeta.RangeEnd))
	if checkpoint.status == "failed" {
		if !isNonTransactionalDMLRangeRetryableError(errors.New(checkpoint.errText)) {
			return errors.Errorf("Non-transactional DML DXF range %d has failed checkpoint: %s", subtaskMeta.RangeID, checkpoint.errText)
		}
		logutil.Logger(ctx).Info("Non-transactional DML DXF ignores retryable failed checkpoint",
			zap.String("job-id", e.taskMeta.JobID),
			zap.Int64("range-id", subtaskMeta.RangeID),
			zap.String("error", checkpoint.errText))
		if err := deleteNonTransactionalDMLRangeCheckpoint(ctx, se, e.taskMeta.JobID, subtaskMeta.RangeID); err != nil {
			return err
		}
		checkpoint = nonTransactionalDMLRangeCheckpoint{}
	}
	start := subtaskMeta.RangeStart
	if checkpoint.status == "done" && checkpoint.checkpoint != nil {
		start = checkpoint.checkpoint
		*scanned = checkpoint.scanned
		*affected = checkpoint.affected
		subtaskMeta.Checkpoint = checkpoint.checkpoint
		e.scanned.Store(*scanned)
		logutil.Logger(ctx).Info("Non-transactional DML DXF subtask resumed from checkpoint",
			zap.String("job-id", e.taskMeta.JobID),
			zap.Int64("range-id", subtaskMeta.RangeID),
			zap.Int64("checkpoint", *checkpoint.checkpoint),
			zap.Uint64("scanned", *scanned),
			zap.Uint64("affected", *affected))
	}
	for {
		handles, err := selectNextNonTransactionalDMLRangeHandles(ctx, rangeCtx, se, start, subtaskMeta.RangeEnd, e.taskMeta.BatchSize)
		if err != nil {
			return err
		}
		if len(handles) == 0 {
			return nil
		}
		end := handles[len(handles)-1]
		sql, err := buildNonTransactionalDMLRangeChunkSQL(rangeCtx, start, end)
		if err != nil {
			return err
		}
		chunkJob := nonTransactionalDMLRangeChunk{
			jobID:          e.taskMeta.JobID,
			rangeID:        subtaskMeta.RangeID,
			start:          cloneInt64Ptr(start),
			end:            end,
			size:           len(handles),
			scannedBefore:  *scanned,
			affectedBefore: *affected,
			sql:            sql,
		}
		result := executeNonTransactionalDMLRangeChunkWithRetry(ctx, rangeCtx, se, chunkJob)
		if result.err != nil {
			return result.err
		}
		*scanned += uint64(len(handles))
		*affected += result.affected
		e.scanned.Store(*scanned)
		subtaskMeta.Checkpoint = &end
		start = &end
		logutil.Logger(ctx).Info("Non-transactional DML DXF subtask checkpoint advanced",
			zap.String("job-id", e.taskMeta.JobID),
			zap.Int64("range-id", subtaskMeta.RangeID),
			zap.Int64("checkpoint", end),
			zap.Int("chunk-size", len(handles)),
			zap.Uint64("chunk-affected", result.affected),
			zap.Uint64("scanned", *scanned),
			zap.Uint64("affected", *affected))
	}
}

func unmarshalNonTransactionalDMLTaskMeta(data []byte) (*nonTransactionalDMLTaskMeta, error) {
	var taskMeta nonTransactionalDMLTaskMeta
	if err := json.Unmarshal(data, &taskMeta); err != nil {
		return nil, err
	}
	if taskMeta.BatchSize <= 0 {
		return nil, errors.New("Non-transactional DML DXF task has invalid batch size")
	}
	return &taskMeta, nil
}

func buildNonTransactionalDMLRangeContextFromTaskMeta(taskMeta *nonTransactionalDMLTaskMeta) (*nonTransactionalDMLRangeContext, error) {
	stmt, err := parseNonTransactionalDMLExecutableDML(taskMeta)
	if err != nil {
		return nil, err
	}
	dmlType := taskMeta.DMLType
	if dmlType == "" {
		dmlType = nonTransactionalDMLStmtType(stmt.DMLStmt)
	}
	return &nonTransactionalDMLRangeContext{
		stmt:              stmt,
		tableInfo:         &model.TableInfo{ID: taskMeta.TableID},
		dbName:            taskMeta.DBName,
		currentDB:         taskMeta.CurrentDB,
		tableName:         taskMeta.TableName,
		fromSQL:           taskMeta.FromSQL,
		handleName:        taskMeta.HandleColumn.Name,
		handleExprSQL:     taskMeta.HandleExprSQL,
		handleColumn:      taskMeta.HandleColumn.toASTColumnName(),
		handleColumnType:  *types.NewFieldType(mysql.TypeLonglong),
		originalCondition: stmt.DMLStmt.WhereExpr(),
		originalWhereSQL:  taskMeta.OriginalWhereSQL,
		jobID:             taskMeta.JobID,
		executionMode:     session_metrics.NonTransactionalDMLModeDXF,
		dmlType:           dmlType,
	}, nil
}

func parseNonTransactionalDMLExecutableDML(taskMeta *nonTransactionalDMLTaskMeta) (*ast.NonTransactionalDMLStmt, error) {
	parsed, err := parser.New().ParseOneStmt(taskMeta.ExecutableDML, "", "")
	if err != nil {
		return nil, err
	}
	dml, ok := parsed.(ast.ShardableDMLStmt)
	if !ok {
		return nil, errors.Errorf("Non-transactional DML DXF task stores unsupported DML type %T", parsed)
	}
	switch parsed.(type) {
	case *ast.DeleteStmt, *ast.UpdateStmt:
	default:
		return nil, errors.Errorf("Non-transactional DML DXF task stores unsupported DML type %T", parsed)
	}
	return &ast.NonTransactionalDMLStmt{
		DMLStmt:     dml,
		ShardColumn: taskMeta.HandleColumn.toASTColumnName(),
		Limit:       uint64(taskMeta.BatchSize),
	}, nil
}

func (m nonTransactionalDMLColumnNameMeta) toASTColumnName() *ast.ColumnName {
	return &ast.ColumnName{
		Schema: pmodel.NewCIStr(m.Schema),
		Table:  pmodel.NewCIStr(m.Table),
		Name:   pmodel.NewCIStr(m.Name),
	}
}

func planNonTransactionalDMLDXFRanges(ctx context.Context, h storage.TaskHandle,
	taskMeta *nonTransactionalDMLTaskMeta, concurrency int) ([]nonTransactionalDMLSubtaskMeta, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	var minHandle int64
	var maxHandle int64
	var hasRows bool
	err := h.WithNewSession(func(se sessionctx.Context) error {
		session, ok := se.(sessiontypes.Session)
		if !ok {
			return errors.New("Non-transactional DML DXF planner requires a session executor")
		}
		if err := prepareNonTransactionalDMLDXFSession(ctx, session, taskMeta); err != nil {
			return err
		}
		rows, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WHERE (%s)",
				taskMeta.HandleExprSQL, taskMeta.HandleExprSQL, taskMeta.FromSQL, taskMeta.OriginalWhereSQL))
		if err != nil {
			return err
		}
		if len(rows) == 0 || rows[0].IsNull(0) || rows[0].IsNull(1) {
			return nil
		}
		minHandle = rows[0].GetInt64(0)
		maxHandle = rows[0].GetInt64(1)
		hasRows = true
		return nil
	})
	if err != nil || !hasRows {
		return nil, err
	}
	return splitNonTransactionalDMLSignedHandleRange(minHandle, maxHandle, concurrency), nil
}

func prepareNonTransactionalDMLDXFSession(ctx context.Context, se sessiontypes.Session, taskMeta *nonTransactionalDMLTaskMeta) error {
	applyNonTransactionalDMLWorkerIdentity(se, taskMeta.User, taskMeta.ActiveRoles)
	applyNonTransactionalDMLWorkerResourceGroup(se.GetSessionVars(), taskMeta.ResourceGroup, taskMeta.StmtResourceGroup)
	if err := applyNonTransactionalDMLWorkerSysVars(se.GetSessionVars(), taskMeta.SysVars); err != nil {
		return err
	}
	if taskMeta.CurrentDB == "" {
		return nil
	}
	return executeInternalNoResult(ctx, se, "USE %n", taskMeta.CurrentDB)
}

func splitNonTransactionalDMLSignedHandleRange(minHandle int64, maxHandle int64, count int) []nonTransactionalDMLSubtaskMeta {
	if count < 1 || minHandle > maxHandle {
		return nil
	}
	minBig := big.NewInt(minHandle)
	maxBig := big.NewInt(maxHandle)
	span := new(big.Int).Sub(maxBig, minBig)
	span.Add(span, big.NewInt(1))
	if span.IsInt64() && span.Int64() < int64(count) {
		count = int(span.Int64())
	}
	countBig := big.NewInt(int64(count))
	width := new(big.Int).Add(span, new(big.Int).Sub(countBig, big.NewInt(1)))
	width.Div(width, countBig)

	ranges := make([]nonTransactionalDMLSubtaskMeta, 0, count)
	var start *int64
	for i := 0; i < count; i++ {
		offset := new(big.Int).Mul(width, big.NewInt(int64(i+1)))
		endBig := new(big.Int).Add(minBig, offset)
		endBig.Sub(endBig, big.NewInt(1))
		if endBig.Cmp(maxBig) > 0 {
			endBig.Set(maxBig)
		}
		end := endBig.Int64()
		endCopy := end
		ranges = append(ranges, nonTransactionalDMLSubtaskMeta{
			RangeID:    int64(len(ranges) + 1),
			RangeStart: cloneInt64Ptr(start),
			RangeEnd:   &endCopy,
		})
		if end == maxHandle {
			break
		}
		startVal := end
		start = &startVal
	}
	return ranges
}
