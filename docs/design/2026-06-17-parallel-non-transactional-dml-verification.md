# Parallel Non-Transactional DML Verification Notes

This note records the focused local verification performed for the parallel
non-transactional DML range and DXF implementation on branch
`codex/parallel-ntdml-p1`.

## Scope

The latest verification covers:

- range-mode statement validation and bounded chunk SQL construction;
- range-mode retry behavior for retryable pre-commit failures;
- DXF-mode `DELETE` and `UPDATE` execution;
- DXF-mode concurrency and repeated task execution;
- DXF worker resource group propagation;
- per-chunk checkpoint resume for a fresh DXF worker session.

## Commands

```bash
go test --tags=intest ./pkg/session \
  -run 'Test(CheckRangeModeStatementShape|NonTransactionalDMLRangeRetryableError|BuildNonTransactionalDMLRangeChunkSQLAddsHandleBounds|ApplyNonTransactionalDMLWorkerResourceGroup|NonTransactionalDMLDXFRunSubtaskResumesFromCheckpoint|SplitNonTransactionalDMLSignedHandleRange)' \
  -count=1
```

This command verifies the package-level building blocks:

- P1 accepts only the supported statement shapes and rejects unsupported forms.
- Range chunks add lower and upper handle bounds without mutating the original
  predicate AST.
- Retry classification recognizes TiDB transaction-retryable errors.
- Signed handle ranges split deterministically.
- Non-transactional DML workers apply the captured resource group and fall back
  statement resource group state consistently.
- A DXF subtask can resume from a durable chunk checkpoint using a fresh session:
  the test executes one committed chunk, reloads the checkpoint, resumes the
  range, and verifies all rows plus final checkpoint counters.

Result:

```text
ok  	github.com/pingcap/tidb/pkg/session	2.987s
```

```bash
go test --tags=intest ./pkg/session/nontransactionaltest \
  -run 'TestNonTransactionalDML(RangeModeRetriesChunkBeforeCommit|RangeModeDeleteAndUpdate|DXFModeDeleteAndUpdate|DXFModePropagatesResourceGroup|DXFModeConcurrencyAndMultiRun)' \
  -count=1
```

This command verifies the session-facing behavior:

- Range mode retries a retryable chunk failure before commit.
- Range mode executes supported single-table `DELETE` and `UPDATE`.
- DXF mode executes supported single-table `DELETE` and `UPDATE`.
- DXF task metadata preserves the session resource group and statement resource
  group so worker sessions execute under the intended resource control context.
- DXF mode handles multiple runs against the same table, uses the requested task
  concurrency, creates the expected number of subtasks, writes independent
  checkpoint job IDs, and leaves the expected row values.

Result:

```text
ok  	github.com/pingcap/tidb/pkg/session/nontransactionaltest	7.448s
```

```bash
git diff --check
```

This command verifies that the patch has no whitespace errors.

Result: passed with no output.

## Notes

The DXF concurrency assertion intentionally joins global tasks to subtasks by
the numeric task id. DXF stores subtasks using the numeric task id in the
subtask `task_key` column, while the global task row also has a human-readable
task key string. Joining by the human-readable key would miss subtasks after
cleanup/history transfer.

The checkpoint resume test avoids failpoint-dependent executor shutdown in a
plain local `go test` process. Instead, it directly simulates the important
failure boundary: a chunk has committed and its checkpoint is durable, then a
new worker session resumes from that checkpoint.
