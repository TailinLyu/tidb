# Parallel Non-Transactional DML Verification Notes

Recorded: 2026-07-06

Worktree: `/Users/tailinlyu/code/tidb/tidb-pr1-review`

Branch: `codex/parallel-ntdml-production`

Base branch: `release-8.5-20260608-v8.5.6`

## PR State

- PR #1 fork head was verified at `39f8270a8ac190cc7849e0cafbbd4d46e2042ea8`.
- PR #2 fork head before this phase was verified at `3595973c9c6cd44abf9d8662e4f7e917bde77fee`.
- Both PRs target `release-8.5-20260608-v8.5.6`.

## Environment

- Local platform: macOS arm64, Docker Desktop.
- Docker system test builds `tidb-server` from this branch with `BUILD_TIDB=1`.
- Docker system test component defaults now align with the base branch patch line:
  - `PD_IMAGE=pingcap/pd:v8.5.6`
  - `TIKV_IMAGE=pingcap/tikv:v8.5.6`
- `docker manifest inspect pingcap/pd:v8.5.6` passed.
- `docker manifest inspect pingcap/tikv:v8.5.6` passed.

## Passed Checks

Session-local guard and binary/common-handle focused tests:

```bash
go test ./pkg/session -run 'TestNonTransactionalDML(SessionLocalExpressionRejected|HandleDescriptorSupportedShapes|BoundaryEncodeDecode)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session 5.906s`.

Range/DXF integration tests touched by this phase:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run 'TestNonTransactionalDML(RangeModeIntAndVarchar|DXFModeIntAndVarchar|RangeModeRejectsSessionLocalExpressions|DXFModeRejectsSessionLocalExpressions)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session/nontransactionaltest 10.026s`.

Updated focused run after stabilizing the DXF checkpoint cleanup wait:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run 'TestNonTransactionalDML(RangeModeIntAndVarchar|DXFModeIntAndVarchar|RangeModeRejectsSessionLocalExpressions|DXFModeRejectsSessionLocalExpressions)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session/nontransactionaltest 6.517s`.

DXF cleanup stabilization repeat:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run 'TestNonTransactionalDMLDXFModeIntAndVarchar' -count=3
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session/nontransactionaltest 9.751s`.

Broader session focused suite:

```bash
go test ./pkg/session -run 'TestNonTransactionalDML(HandleDescriptor|Boundary|RangeCondition|RangeSelectWhere|RangeWorker|RegionRangePlanning|DXFTaskMeta|DXFWait|SessionContext|Checkpoint|Retry|SessionLocal)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session 17.545s`.

Phase 2 checkpoint/retry/cancellation regression suite:

```bash
go test ./pkg/session -run 'TestNonTransactionalDML(AmbiguousCommitCoveredByCheckpoint|DXFReplayFailedCheckpoints|CanceledChunkPreservesFailedCheckpoint|DXFCleanupPreservesFailedTaskCheckpoints)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session 7.128s`.

Updated broader session focused suite including the Phase 2 regressions:

```bash
go test ./pkg/session -run 'TestNonTransactionalDML(HandleDescriptor|Boundary|RangeCondition|RangeSelectWhere|RangeWorker|RegionRangePlanning|DXFTaskMeta|DXFWait|DXFModeRequiresTaskManager|SessionContext|Checkpoint|Retry|SessionLocal|AmbiguousCommit|DXFReplay|CanceledChunk|DXFCleanup)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session 21.490s`.

Sysvar focused suite:

```bash
go test ./pkg/sessionctx/variable -run 'TestNonTransactionalDML(ExecutionMode|Concurrency)SysVar' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/sessionctx/variable 4.134s`.

Bootstrap checkpoint focused suite:

```bash
go test ./pkg/session/bootstraptest -run 'TestBootstrapNonTransactionalDMLCheckpointTable|TestUpgradeVersion239CreatesNonTransactionalDMLCheckpointTable' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session/bootstraptest 11.178s`.

Distributed task and metrics packages:

```bash
go test ./pkg/disttask/framework/proto -count=1
go test ./pkg/metrics -count=1
go test ./pkg/session/metrics -count=1
```

Result: passed.

- `ok github.com/pingcap/tidb/pkg/disttask/framework/proto 0.314s`
- `ok github.com/pingcap/tidb/pkg/metrics 0.869s`
- `? github.com/pingcap/tidb/pkg/session/metrics [no test files]`

Static, dashboard, and image checks:

```bash
bash -n tests/ntdml/run-dxf-system-test.sh
python3 -m json.tool pkg/metrics/grafana/non_transactional_dml.json >/dev/null
go run tools/dashboard-linter/main.go pkg/metrics/grafana/non_transactional_dml.json
docker manifest inspect pingcap/pd:v8.5.6 >/dev/null
docker manifest inspect pingcap/tikv:v8.5.6 >/dev/null
git diff --check HEAD
```

Result: passed.

Docker DXF system test:

```bash
DOCKER_CONFIG=$(mktemp -d) BUILD_TIDB=1 tests/ntdml/run-dxf-system-test.sh
```

Result: passed with final output `parallel non-transactional DML DXF system test passed`.

The system test covered:

- Integer clustered primary key DXF update/delete.
- `VARCHAR(...) COLLATE utf8mb4_bin PRIMARY KEY CLUSTERED` DXF update/delete.
- `VARBINARY(...) PRIMARY KEY CLUSTERED` DXF update/delete.
- Long-running integer DXF restart/failover.
- Long-running `VARCHAR(...) COLLATE utf8mb4_bin PRIMARY KEY CLUSTERED` DXF restart/failover with submitter/owner loss.
- Prometheus and Grafana datasource queries for the NTDML metrics.
- Checkpoint cleanup after successful jobs.

Docker DXF reduced large-mode system test:

```bash
DOCKER_CONFIG=$(mktemp -d) BUILD_TIDB=1 NTDML_LARGE_MODE=1 NTDML_RESTART_ROWS=50000 NTDML_PAYLOAD_BYTES=2048 NTDML_RESTART_SLEEP_SECONDS=0 tests/ntdml/run-dxf-system-test.sh
```

Result: passed with final output `parallel non-transactional DML DXF system test passed`.

This used the same large-mode code path with locally bounded row and payload overrides. It covered the integer and `VARCHAR(...) COLLATE utf8mb4_bin PRIMARY KEY CLUSTERED` restart/failover workloads with TiKV remaining healthy.

Phase 4 performance smoke syntax check:

```bash
bash -n tests/ntdml/run-performance-smoke.sh
```

Result: passed.

Phase 4 local performance smoke found and then verified a range-mode concurrency bug. Before the fix, this command failed with `t_varbinary_range_2 updated 350 rows, expected 400`, exposing concurrent mutation of the shared range-mode statement AST while chunk SQL was restored. After guarding that mutation and rebuilding the TiDB image, the narrow reproducer passed:

```bash
DOCKER_CONFIG=$(mktemp -d) BUILD_TIDB=1 NTDML_PERF_ROWS=400 NTDML_PERF_BATCH_SIZE=80 NTDML_PERF_PAYLOAD_BYTES=128 NTDML_PERF_CONCURRENCY_VALUES='2' NTDML_PERF_HANDLE_TYPES='varbinary' tests/ntdml/run-performance-smoke.sh
```

Result: passed with:

```text
handle,mode,concurrency,rows,batch_size,payload_bytes,duration_ms,affected_rows
varbinary,serial,1,400,80,128,1525,400
varbinary,range,2,400,80,128,1441,400
varbinary,dxf,2,400,80,128,3549,400
```

Bounded local performance sweep:

```bash
DOCKER_CONFIG=$(mktemp -d) NTDML_PERF_ROWS=400 NTDML_PERF_BATCH_SIZE=80 NTDML_PERF_PAYLOAD_BYTES=128 NTDML_PERF_CONCURRENCY_VALUES='1 2 4' NTDML_PERF_HANDLE_TYPES='int varchar varbinary' tests/ntdml/run-performance-smoke.sh
```

Result: passed with:

```text
handle,mode,concurrency,rows,batch_size,payload_bytes,duration_ms,affected_rows
int,serial,1,400,80,128,2020,400
int,range,1,400,80,128,1978,400
int,range,2,400,80,128,2035,400
int,range,4,400,80,128,1961,400
int,dxf,1,400,80,128,5147,400
int,dxf,2,400,80,128,5256,400
int,dxf,4,400,80,128,5097,400
varchar,serial,1,400,80,128,1911,400
varchar,range,1,400,80,128,2092,400
varchar,range,2,400,80,128,2076,400
varchar,range,4,400,80,128,1993,400
varchar,dxf,1,400,80,128,5019,400
varchar,dxf,2,400,80,128,4879,400
varchar,dxf,4,400,80,128,4981,400
varbinary,serial,1,400,80,128,1914,400
varbinary,range,1,400,80,128,1974,400
varbinary,range,2,400,80,128,1989,400
varbinary,range,4,400,80,128,2056,400
varbinary,dxf,1,400,80,128,5346,400
varbinary,dxf,2,400,80,128,4999,400
varbinary,dxf,4,400,80,128,5031,400
```

The smoke validates row counts and successful checkpoint cleanup after every case. On this small local dataset, range mode is similar to serial because setup and chunk overhead dominate; DXF is slower because distributed task framework scheduling dominates. The script defaults to concurrency `1 2 4 8 16` for larger local or CI sweeps.

## Production Readiness Pass

Added the operator runbook:

- `docs/superpowers/runbooks/2026-07-05-parallel-ntdml-production-runbook.md`

The runbook covers mode selection, initial concurrency, safe `DELETE`, idempotent
`UPDATE`, non-idempotent update warnings, preflight checklist, checkpoint
inspection, failed checkpoint cleanup, Prometheus queries, Grafana dashboard
usage, TiDB submitter/owner/executor restart procedures, unsupported shapes,
and serial-mode rollback controls.

Grafana dashboard publication was audited:

- `pkg/metrics/grafana/non_transactional_dml.json` is a tracked dashboard added by this PR.
- `Makefile` runs the dashboard linter for `non_transactional_dml.json` with the other published TiDB dashboards.
- `tests/ntdml/run-dxf-system-test.sh` provisions the dashboard into Grafana as `Test-Cluster-TiDB-Non-Transactional-DML`.
- The dashboard contains panels for every dedicated NTDML metric family: `tidb_session_non_transactional_dml_count`, `tidb_session_non_transactional_dml_task_total`, `tidb_session_non_transactional_dml_chunk_total`, `tidb_session_non_transactional_dml_rows_total`, `tidb_session_non_transactional_dml_retry_total`, and `tidb_session_non_transactional_dml_duration_seconds`.

Non-idempotent update acceptance regression:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run '^TestNonTransactionalDMLRangeModeIntAndVarchar$' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session/nontransactionaltest 3.219s`.

This test now includes `BATCH ON id LIMIT 2 UPDATE t_range_nonidempotent SET c = c + 1 WHERE id >= 1` in explicit `range` mode. It confirms the accepted statement shape remains allowed without claiming exactly-once behavior across failures.

Failed checkpoint retention coverage already exists in the focused session suite and was not duplicated:

- `TestNonTransactionalDMLCanceledChunkPreservesFailedCheckpoint` verifies a canceled chunk leaves a failed checkpoint with `error_class = 'canceled'`.
- `TestNonTransactionalDMLDXFCleanupPreservesFailedTaskCheckpoints` verifies DXF cleanup for a failed task does not delete the retained failed checkpoint.
- `TestNonTransactionalDMLDXFReplayFailedCheckpoints` verifies retryable failed checkpoints can be replayed, while permanent execution failures remain retained with `error_class` and `error_text`.
- `TestNonTransactionalDMLCheckpointReadWriteSummaryAndCleanup` verifies checkpoint read/write summary metadata and explicit cleanup helper behavior.

During focused integration verification, the unanchored focused `intest` slice
initially exposed a successful DXF checkpoint row that could remain after the
SQL submitter had already observed task success. The retained row had
`status = 'done'`, so the root cause was successful-job cleanup timing, not
failed-checkpoint retention. Added a submitter-side cleanup after
`buildNonTransactionalDMLDXFResults` captures the checkpoint summary; the
existing asynchronous DXF cleanup still covers detached or recovered jobs.

The new regression failed before the fix and passed after it:

```bash
go test ./pkg/session -run '^TestNonTransactionalDMLDXFResultsDeleteSuccessfulCheckpoints$' -count=1
```

Result after fix: passed, `ok github.com/pingcap/tidb/pkg/session 3.704s`.

Updated broader session focused suite including the DXF result cleanup
regression:

```bash
go test ./pkg/session -run 'TestNonTransactionalDML(HandleDescriptor|Boundary|RangeCondition|RangeSelectWhere|RangeWorker|RegionRangePlanning|DXFTaskMeta|DXFWait|DXFModeRequiresTaskManager|SessionContext|Checkpoint|DXFResults|Retry|SessionLocal|AmbiguousCommit|DXFReplay|CanceledChunk|DXFCleanup)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session 22.610s`.

Focused integration suite after the cleanup fix:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run 'TestNonTransactionalDML(RangeModeIntAndVarchar|DXFModeIntAndVarchar|RangeModeRejectsSessionLocalExpressions|DXFModeRejectsSessionLocalExpressions)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session/nontransactionaltest 6.176s`.

Timing-sensitive repeat:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run 'TestNonTransactionalDML(RangeModeIntAndVarchar|DXFModeIntAndVarchar|RangeModeRejectsSessionLocalExpressions|DXFModeRejectsSessionLocalExpressions)' -count=2
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session/nontransactionaltest 9.590s`.

Sysvar focused suite:

```bash
go test ./pkg/sessionctx/variable -run 'TestNonTransactionalDML(ExecutionMode|Concurrency)SysVar' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/sessionctx/variable 2.991s`.

Static, dashboard, and dashboard coverage checks:

```bash
bash -n tests/ntdml/run-dxf-system-test.sh
bash -n tests/ntdml/run-performance-smoke.sh
python3 -m json.tool pkg/metrics/grafana/non_transactional_dml.json >/dev/null
go run tools/dashboard-linter/main.go pkg/metrics/grafana/non_transactional_dml.json
python3 - <<'PY'
import json, pathlib
path = pathlib.Path('pkg/metrics/grafana/non_transactional_dml.json')
data = json.loads(path.read_text())
exprs = '\n'.join(t.get('expr','') for p in data['panels'] for t in p.get('targets', []))
metrics = [
    'tidb_session_non_transactional_dml_count',
    'tidb_session_non_transactional_dml_task_total',
    'tidb_session_non_transactional_dml_chunk_total',
    'tidb_session_non_transactional_dml_rows_total',
    'tidb_session_non_transactional_dml_retry_total',
    'tidb_session_non_transactional_dml_duration_seconds_bucket',
    'tidb_session_non_transactional_dml_duration_seconds_sum',
    'tidb_session_non_transactional_dml_duration_seconds_count',
]
missing = [m for m in metrics if m not in exprs]
if missing:
    raise SystemExit('missing dashboard metrics: ' + ', '.join(missing))
print('dashboard metric coverage ok')
PY
git diff --check HEAD
```

Result: passed with `dashboard metric coverage ok`.

## Successful Cleanup Failure Follow-up

The follow-up changed successful range and DXF checkpoint deletion to
best-effort so a cleanup delete failure cannot mask a completed mutation. The
targeted checkpoint/result suite passes:

```bash
go test ./pkg/session -run 'TestNonTransactionalDML(Checkpoint|DXFResults|Cleanup|CanceledChunk|DXFCleanup|RangeModeCleanupFailureReturnsSuccess)' -count=1
```

Result: passed.

The normal range/DXF integration smoke tests still pass and continue to verify
successful checkpoint cleanup when no cleanup error is injected:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run 'TestNonTransactionalDML(RangeModeIntAndVarchar|DXFModeIntAndVarchar)' -count=1
```

Result: passed.

Sysvar, bootstrap, and dashboard validation after the cleanup change:

```bash
go test ./pkg/sessionctx/variable -run 'TestNonTransactionalDML(ExecutionMode|Concurrency)SysVar' -count=1
go test ./pkg/session/bootstraptest -run 'TestBootstrapNonTransactionalDMLCheckpointTable|TestUpgradeVersion239CreatesNonTransactionalDMLCheckpointTable' -count=1
python3 -m json.tool pkg/metrics/grafana/non_transactional_dml.json >/dev/null
go run tools/dashboard-linter/main.go pkg/metrics/grafana/non_transactional_dml.json
```

Result: passed.

The Grafana dashboard still covers every dedicated NTDML metric family:

```bash
python3 - <<'PY'
import json, pathlib
path = pathlib.Path('pkg/metrics/grafana/non_transactional_dml.json')
data = json.loads(path.read_text())
text = json.dumps(data)
metrics = [
    'tidb_session_non_transactional_dml_count',
    'tidb_session_non_transactional_dml_task_total',
    'tidb_session_non_transactional_dml_chunk_total',
    'tidb_session_non_transactional_dml_rows_total',
    'tidb_session_non_transactional_dml_retry_total',
    'tidb_session_non_transactional_dml_duration_seconds_bucket',
    'tidb_session_non_transactional_dml_duration_seconds_sum',
    'tidb_session_non_transactional_dml_duration_seconds_count',
]
missing = [metric for metric in metrics if metric not in text]
if missing:
    raise SystemExit('missing dashboard metrics: ' + ', '.join(missing))
print('dashboard covers all NTDML metric families')
PY
```

Result: passed with `dashboard covers all NTDML metric families`.

## Baseline Failure

The full `pkg/session/nontransactionaltest` NTDML regex still fails because an existing failpoint-based serial error-message test does not inject its expected error under the current local test invocation:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run 'TestNonTransactionalDML' -count=1
```

Local result: failed at `TestNonTransactionalDMLErrorMessage`; the first `require.EqualError` saw `nil`.

This failure was reproduced against the unmodified PR #2 head `3595973c9c6cd44abf9d8662e4f7e917bde77fee` in a detached worktree:

```bash
go test -tags intest ./pkg/session/nontransactionaltest -run '^TestNonTransactionalDMLErrorMessage$' -count=1
```

Baseline result: failed with `An error is expected but got nil` at the same serial `INSERT ... SELECT` failpoint assertion. The new range/DXF tests added in this phase pass when run directly.

The handoff-listed storage package command also is not a clean local gate in this checkout:

```bash
go test ./pkg/disttask/framework/storage -count=1 -timeout=5m
```

Local result: timed out. An isolated run of `TestTaskTable` prints TiDB's guard message that the package should be tested with `--tags=intest`.

With the intended tag, the package completes quickly but fails unrelated disttask storage assertions:

```bash
go test -tags intest ./pkg/disttask/framework/storage -count=1 -timeout=10m
```

Local result: failed in `TestSwitchTaskStepInBatch` and `TestModifyTask`.

The same two failures were reproduced against the base branch `origin/release-8.5-20260608-v8.5.6` in a detached worktree:

```bash
go test -tags intest ./pkg/disttask/framework/storage -run 'Test(SwitchTaskStepInBatch|ModifyTask)$' -count=1 -timeout=3m
```

Baseline result: failed with:

- `TestSwitchTaskStepInBatch`: expected duplicate-entry error in chain, got an empty chain.
- `TestModifyTask`: expected `task changed by other operation` in chain, got an empty chain.

## Not Yet Run

- Broad non-NTDML package suites beyond the focused commands above.
- Resource group throttling behavior under sustained NTDML load.

The default full large-mode command was attempted locally:

```bash
DOCKER_CONFIG=$(mktemp -d) BUILD_TIDB=1 NTDML_LARGE_MODE=1 tests/ntdml/run-dxf-system-test.sh
```

Local result: interrupted after TiKV exited with Docker status `137` during the large failover workload. The default large-mode settings are `NTDML_RESTART_ROWS=250000` and `NTDML_PAYLOAD_BYTES=4096`, which exceeded this local Docker Desktop environment. The reduced large-mode override above passed.

## Residual Risks

- The full `nontransactionaltest` package needs the existing `TestNonTransactionalDMLErrorMessage` failpoint issue resolved or excluded by the maintainers' intended test mode before it can be used as a clean gate.
- Full default large-mode Docker coverage still needs a larger host or CI runner with enough Docker memory for the 250k x 4KB failover workload.
- The dashboard uses only bounded Prometheus labels already emitted by this branch; it cannot show durable checkpoint-row state until that state is exported as a metric.
