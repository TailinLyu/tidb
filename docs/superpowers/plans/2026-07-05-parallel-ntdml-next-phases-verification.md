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

Result: passed, `ok github.com/pingcap/tidb/pkg/session/nontransactionaltest 7.615s`.

Broader session focused suite:

```bash
go test ./pkg/session -run 'TestNonTransactionalDML(HandleDescriptor|Boundary|RangeCondition|RangeSelectWhere|RangeWorker|RegionRangePlanning|DXFTaskMeta|DXFWait|SessionContext|Checkpoint|Retry|SessionLocal)' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session 17.545s`.

Sysvar focused suite:

```bash
go test ./pkg/sessionctx/variable -run 'TestNonTransactionalDML(ExecutionMode|Concurrency)SysVar' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/sessionctx/variable 5.679s`.

Bootstrap checkpoint focused suite:

```bash
go test ./pkg/session/bootstraptest -run 'TestBootstrapNonTransactionalDMLCheckpointTable|TestUpgradeVersion239CreatesNonTransactionalDMLCheckpointTable' -count=1
```

Result: passed, `ok github.com/pingcap/tidb/pkg/session/bootstraptest 12.840s`.

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

## Not Yet Run

- `DOCKER_CONFIG=$(mktemp -d) BUILD_TIDB=1 NTDML_LARGE_MODE=1 tests/ntdml/run-dxf-system-test.sh`
- Broad non-NTDML package suites beyond the focused commands above.
- Performance comparison for serial versus range versus DXF.

## Residual Risks

- The full `nontransactionaltest` package needs the existing `TestNonTransactionalDMLErrorMessage` failpoint issue resolved or excluded by the maintainers' intended test mode before it can be used as a clean gate.
- Large-mode Docker coverage remains to be run for longer restart windows and larger payloads.
- The dashboard uses only bounded Prometheus labels already emitted by this branch; it cannot show durable checkpoint-row state until that state is exported as a metric.
