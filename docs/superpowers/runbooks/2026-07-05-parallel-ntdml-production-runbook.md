# Parallel Non-Transactional DML Production Runbook

Use this runbook for controlled opt-in cleanup jobs that need bounded
transactions over large TiDB tables. The default `serial` mode remains the
compatibility path. Do not enable `range` or `dxf` globally for broad workloads
until the job shape, monitoring, and rollback path are understood.

## Mode Selection

Use `serial` when:

- The table is small enough for existing non-transactional DML behavior.
- You need the lowest operational risk.
- You are disabling new parallel jobs during an incident.

Use `range` when:

- The job can finish while the submitting TiDB session stays alive.
- You want local TiDB worker parallelism without distributed-task scheduling.
- You are doing a small first production trial.

Use `dxf` when:

- The job may outlive the submitting SQL connection.
- TiDB owner or executor restarts are expected during the job.
- You need Distributed Task Framework ownership and restart recovery.

Start with low concurrency. For first production runs, use `2` or `4`; increase
only after TiDB, TiKV, Prometheus, and application latency remain healthy.

```sql
SET SESSION tidb_nontransactional_dml_execution_mode = 'range';
SET SESSION tidb_nontransactional_dml_concurrency = 2;
```

For DXF, make sure distributed task execution is enabled:

```sql
SET GLOBAL tidb_enable_dist_task = 1;
SET SESSION tidb_nontransactional_dml_execution_mode = 'dxf';
SET SESSION tidb_nontransactional_dml_concurrency = 2;
```

## Preflight Checklist

- Confirm a backup, restore, or application-level recovery path exists.
- Confirm the statement is a single-table `DELETE` or `UPDATE`.
- Confirm the chunking handle is `_tidb_rowid` or the single-column clustered
  primary key.
- Confirm string clustered primary keys use binary ordering, such as
  `VARCHAR(...) COLLATE utf8mb4_bin`, `VARBINARY`, or `BINARY`.
- Confirm the statement does not update the chunking handle column.
- Confirm the statement does not contain user variables, system variable
  references, or session-local functions such as `connection_id()`,
  `last_insert_id()`, `current_user()`, `current_resource_group()`,
  `database()`, or `row_count()`.
- Confirm the `WHERE` predicate matches the intended cleanup and is not wider
  than expected.
- Confirm the batch `LIMIT` is a per-chunk row target, not a full-statement
  row limit.
- Confirm concurrency starts low.
- Confirm NTDML Prometheus metrics are visible and the Grafana dashboard is
  provisioned.
- Confirm `mysql.tidb_nontransactional_dml_checkpoint` is empty, or that any
  retained rows are understood and intentionally kept.
- Confirm a maintenance window or load budget is available.

## Safe Examples

Safe `DELETE` over a binary `VARCHAR` clustered primary key:

```sql
SET SESSION tidb_nontransactional_dml_execution_mode = 'dxf';
SET SESSION tidb_nontransactional_dml_concurrency = 4;

BATCH ON id LIMIT 1000
DELETE FROM user_events
WHERE id >= 'tenant:00000000'
  AND created_at < '2025-01-01 00:00:00';
```

Idempotent `UPDATE`, recommended for cleanup marks:

```sql
SET SESSION tidb_nontransactional_dml_execution_mode = 'range';
SET SESSION tidb_nontransactional_dml_concurrency = 2;

BATCH ON id LIMIT 1000
UPDATE user_events
SET deleted_at = '2026-07-06 00:00:00'
WHERE id >= 'tenant:00000000'
  AND deleted_at IS NULL
  AND created_at < '2025-01-01 00:00:00';
```

Non-idempotent updates such as `SET c = c + 1` are accepted, but they are not
exactly-once across the whole job. A chunk can be retried after worker failure,
TiDB restart, or ambiguous commit handling. Use them only when the business
logic can tolerate at-least-once chunk semantics.

## Unsupported Shapes

Explicit `range` and `dxf` modes reject unsupported shapes instead of falling
back to serial. Unsupported shapes include:

- Partitioned tables.
- Multi-table `DELETE` or `UPDATE`.
- `INSERT ... SELECT`.
- Composite clustered primary keys.
- Prefix primary keys.
- Secondary-index chunking columns.
- Unsigned integer clustered primary keys.
- Non-binary string collations.
- Statements that update the chunking handle column.
- Statements with user variables, system variables, or session-local
  functions.

## Monitoring

Prometheus metrics use bounded labels only:

- `tidb_session_non_transactional_dml_count`
- `tidb_session_non_transactional_dml_task_total`
- `tidb_session_non_transactional_dml_chunk_total`
- `tidb_session_non_transactional_dml_rows_total`
- `tidb_session_non_transactional_dml_retry_total`
- `tidb_session_non_transactional_dml_duration_seconds`

Useful Prometheus queries:

```promql
sum by (type) (rate(tidb_session_non_transactional_dml_count[5m]))
sum by (mode, type, result) (rate(tidb_session_non_transactional_dml_task_total[5m]))
sum by (mode, type, result) (rate(tidb_session_non_transactional_dml_chunk_total[5m]))
sum by (mode, type, kind) (rate(tidb_session_non_transactional_dml_rows_total[5m]))
sum by (mode, type) (rate(tidb_session_non_transactional_dml_retry_total[5m]))
histogram_quantile(0.95, sum by (le, mode, type, result) (rate(tidb_session_non_transactional_dml_duration_seconds_bucket[5m])))
```

The Grafana dashboard JSON is
`pkg/metrics/grafana/non_transactional_dml.json`. In the Docker system test it
is provisioned as `Test-Cluster-TiDB-Non-Transactional-DML`.

## SQL Observability

Show active or retained checkpoints:

```sql
SELECT job_id, range_id, mode, dml_type, db_name, table_id,
       physical_table_id, handle_kind, status, retry_count, error_class,
       scanned, affected, updated_at, finished_at
FROM mysql.tidb_nontransactional_dml_checkpoint
ORDER BY updated_at DESC, job_id, range_id
LIMIT 100;
```

Show failed checkpoints grouped by error class:

```sql
SELECT error_class, COUNT(*) AS ranges, SUM(scanned) AS scanned,
       SUM(affected) AS affected, MAX(updated_at) AS latest_update
FROM mysql.tidb_nontransactional_dml_checkpoint
WHERE status = 'failed'
GROUP BY error_class
ORDER BY ranges DESC;
```

Show retry counts and row totals:

```sql
SELECT mode, dml_type, status, SUM(retry_count) AS retries,
       SUM(scanned) AS scanned, SUM(affected) AS affected
FROM mysql.tidb_nontransactional_dml_checkpoint
GROUP BY mode, dml_type, status
ORDER BY mode, dml_type, status;
```

Show old retained checkpoints:

```sql
SELECT job_id, MIN(created_at) AS created_at, MAX(updated_at) AS updated_at,
       COUNT(*) AS ranges, SUM(status = 'failed') AS failed_ranges
FROM mysql.tidb_nontransactional_dml_checkpoint
GROUP BY job_id
ORDER BY updated_at ASC
LIMIT 20;
```

Show per-table checkpoint state:

```sql
SELECT db_name, table_id, physical_table_id, mode, status,
       COUNT(*) AS ranges, SUM(scanned) AS scanned, SUM(affected) AS affected
FROM mysql.tidb_nontransactional_dml_checkpoint
GROUP BY db_name, table_id, physical_table_id, mode, status
ORDER BY db_name, table_id, mode, status;
```

Inspect DXF tasks:

```sql
SELECT id, task_key, state, step, concurrency, create_time, start_time,
       state_update_time, end_time
FROM mysql.tidb_global_task
WHERE type = 'NonTransactionalDML'
ORDER BY id DESC;

SELECT id, task_key, state, step, concurrency, create_time, start_time,
       state_update_time, end_time
FROM mysql.tidb_global_task_history
WHERE type = 'NonTransactionalDML'
ORDER BY id DESC
LIMIT 20;
```

## Job States And Checkpoints

- Completed successful jobs delete their checkpoint rows after the final
  summary.
- Failed jobs retain checkpoint rows with `status = 'failed'`.
- Canceled jobs retain failed checkpoint rows with `error_class = 'canceled'`
  when cancellation interrupts a chunk.
- `error_class = 'retryable'` means the range can be replayed by a worker after
  deleting the failed checkpoint.
- `error_class = 'execution'` means the range hit a permanent execution error
  and should be inspected before retrying.

Retained rows include the job id, range id, mode, DML type, database, table ids,
handle kind, range boundaries, checkpoint value, status, retry count, error
class, error text, scanned rows, affected rows, and timestamps.

## Cleanup Of Failed Checkpoints

Do not delete retained rows until the failure is understood and any needed
diagnostics have been captured. To remove failed checkpoints for a known job:

```sql
DELETE FROM mysql.tidb_nontransactional_dml_checkpoint
WHERE job_id = 'range-123-456'
  AND status = 'failed';
```

To remove all failed checkpoints for a table after review:

```sql
DELETE FROM mysql.tidb_nontransactional_dml_checkpoint
WHERE db_name = 'app_db'
  AND table_id = 12345
  AND status = 'failed';
```

## Restart And Resume Procedure

Submitting TiDB disconnects:

- `range` mode stops with the SQL session; inspect retained checkpoints.
- `dxf` mode is owned by Distributed Task Framework and can continue after the
  submitter disconnects.

DXF owner restarts:

- Inspect `mysql.tidb_global_task` and `mysql.tidb_global_task_history`.
- The task should continue or move to history when a new owner takes over.
- Check checkpoint rows only if the task fails or appears stalled.

Executor TiDB restarts:

- The framework may retry subtasks on another executor.
- Inspect task state and checkpoint retry counts.
- Retryable failed checkpoints are diagnostic state, not proof that all chunks
  are lost.

Job fails with retained checkpoints:

- Inspect `error_class`, `error_text`, `retry_count`, `scanned`, and `affected`.
- Fix the permanent cause before rerunning.
- Rerunning the same SQL creates a new job id. It does not attach to the old
  job id. Keep or delete old failed checkpoint rows deliberately.

## Kill Switch And Fallback

Stop launching new parallel jobs by setting the execution mode back to serial:

```sql
SET GLOBAL tidb_nontransactional_dml_execution_mode = 'serial';
SET SESSION tidb_nontransactional_dml_execution_mode = 'serial';
```

The global setting affects new sessions. The session setting affects the
current session and is the quickest per-connection override. Existing running
DXF jobs should be inspected or canceled separately through the distributed
task controls used by TiDB operators.

## Go/No-Go

This feature is suitable for controlled opt-in jobs when the preflight checklist
passes, metrics are visible, and operators understand checkpoint retention and
at-least-once semantics for non-idempotent updates.

It is not suitable for broad default enablement. Keep `serial` as the default
until broader package suites, sustained resource-group throttling, and larger
CI-scale performance runs are complete.
