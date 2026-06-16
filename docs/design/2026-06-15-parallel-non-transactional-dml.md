# Parallel Range Execution for Non-transactional DML

- Author(s): Proposal draft for TiDB contributors
- Discussion PR: This document is a local RFC draft. The upstream PR is created when the proposal is submitted.
- Tracking Issue: The tracking issue is created after the design direction is accepted.

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Goals](#goals)
* [Non-goals](#non-goals)
* [User Interface](#user-interface)
* [P1 Scope](#p1-scope)
* [Detailed Design](#detailed-design)
  * [Mode Selection](#mode-selection)
  * [Supported Statements](#supported-statements)
  * [Executable Task Metadata](#executable-task-metadata)
  * [Range Planning](#range-planning)
  * [Chunk Scan and Mutation](#chunk-scan-and-mutation)
  * [Durable Checkpoints](#durable-checkpoints)
  * [DXF Integration](#dxf-integration)
  * [Local Fallback](#local-fallback)
  * [Failure Handling and Retry](#failure-handling-and-retry)
  * [Progress and Observability](#progress-and-observability)
  * [Compatibility](#compatibility)
* [Test Design](#test-design)
* [Roadmap](#roadmap)
* [Impacts and Risks](#impacts-and-risks)
* [Investigation and Alternatives](#investigation-and-alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes a new execution mode for TiDB non-transactional DML. The current `BATCH` syntax splits a DML statement by running a full ordered query over the shard column, materializes all split jobs in memory, and executes split DML statements serially in the user session. This proposal keeps the existing non-transactional contract but adds a handle-range executor that can scan and mutate bounded chunks without first materializing every split job.

The first implementation is intentionally narrow. It supports explicit opt-in for single-table `DELETE` and `UPDATE` statements on a single signed integer handle: either `_tidb_rowid` or a single-column signed integer clustered primary key. It preserves the user's `LIMIT` as the target chunk size and adds a concurrency control for the new executor.

## Motivation or Background

For a statement such as:

```sql
BATCH ON id LIMIT 10000 DELETE FROM t WHERE status = 'expired';
```

the existing executor first builds and executes a query similar to:

```sql
SELECT id FROM t
WHERE status = 'expired'
ORDER BY IF(ISNULL(id), 0, 1), id;
```

It then reads the full result, builds shard ranges of about `LIMIT` rows, and executes the resulting DML statements one by one. This avoids unsafe transaction splitting, but it is slow for large tables because execution cannot start until the qualifying-row pre-scan finishes and because split jobs run with parallelism 1.

Users need a native TiDB mechanism that can make progress immediately, use controlled parallelism, keep each chunk's scan and mutation interval bounded, and retry safe transient failures without an external splitter.

## Goals

1. Avoid the full qualifying-row pre-scan before starting DML execution.
2. Execute non-transactional DML over handle ranges with configurable concurrency.
3. Bound each chunk's initial handle scan by the user-specified `LIMIT`, and keep the mutation transaction constrained to that handle interval.
4. Preserve explicit user opt-in through `BATCH` plus an explicit new execution mode.
5. Preserve non-transactional semantics: no atomicity across chunks, no global snapshot isolation, and no rollback across chunks.
6. Support single-table `DELETE` and `UPDATE` in P1.
7. Persist per-chunk checkpoints durably enough to classify ambiguous chunk commits in P1, and to become the failover/resume state for later DXF execution.
8. Keep existing serial non-transactional DML behavior available for unsupported shapes.

## Non-goals

1. This design does not migrate TTL to DXF.
2. This design does not change normal DML semantics. Normal `DELETE` or `UPDATE` statements are never silently rewritten into non-transactional chunks.
3. This design does not provide exactly-once row mutation semantics for arbitrary non-idempotent `UPDATE` statements.
4. P1 does not support multi-table `DELETE` or `UPDATE`.
5. P1 does not support `INSERT INTO SELECT`.
6. P1 does not split on arbitrary secondary indexes.
7. P1 does not support unsigned integer handles, composite clustered primary keys, or non-integer common handles.
8. P1 does not depend on statistics or histogram split points for correctness.
9. P1 does not support adaptive range splitting, although checkpoint records should be designed so that adaptive splitting can be added later.

## User Interface

Existing syntax remains valid and continues to use the existing serial executor by default:

```sql
BATCH [ON shard_column] LIMIT batch_size DELETE FROM ...
BATCH [ON shard_column] LIMIT batch_size UPDATE ...
```

The new executor is selected explicitly. The preferred SQL syntax is:

```sql
BATCH [ON handle_column] LIMIT batch_size CONCURRENCY concurrency DELETE FROM single_table WHERE ...
BATCH [ON handle_column] LIMIT batch_size CONCURRENCY concurrency UPDATE single_table SET ... WHERE ...
```

Before parser support for `CONCURRENCY` lands, the same mode can be enabled with session variables:

```sql
SET @@tidb_nontransactional_dml_execution_mode = 'range';
SET @@tidb_nontransactional_dml_concurrency = 8;
BATCH ON id LIMIT 10000 DELETE FROM t WHERE status = 'expired';

SET @@tidb_nontransactional_dml_execution_mode = 'dxf';
BATCH ON id LIMIT 10000 UPDATE t SET archived = 1 WHERE status = 'expired';
```

The semantics are:

* `LIMIT batch_size` is the target maximum number of handles scanned per mutation chunk. Because the mutation rechecks the original predicate over the scanned handle interval, concurrent changes can make the affected row count differ from the scanned handle count.
* `CONCURRENCY concurrency` is the maximum number of local range workers or DXF distributed subtasks.
* If the user does not explicitly select the range/DXF executor, existing serial behavior is preserved.
* If the range/DXF executor is explicitly selected and the statement shape is unsupported, TiDB returns an error instead of silently changing semantics.
* If the range/DXF executor is not explicitly selected and the statement shape is unsupported by the range executor, TiDB continues to use the existing serial executor.

## P1 Scope

P1 implements the local handle-range executor. It is the foundation for the later DXF executor but does not require DXF to prove the chunking semantics.

P1 supports:

* single-table `DELETE FROM t WHERE ...`;
* single-table `UPDATE t SET ... WHERE ...`;
* `_tidb_rowid` handles;
* a single-column signed integer clustered primary key;
* explicit `BATCH ON` only when it names the handle column;
* local worker concurrency controlled by `tidb_nontransactional_dml_concurrency`;
* durable per-chunk checkpoints stored in a system table.

P1 rejects in range mode:

* multi-table `DELETE`;
* multi-table `UPDATE`;
* `INSERT INTO SELECT`;
* predicates or assignments that reference user variables, system variables, or session-local information functions such as `connection_id()` and `last_insert_id()`;
* non-handle shard columns;
* partitioned tables;
* composite clustered primary keys;
* unsigned integer handles;
* string, binary, decimal, date/time, or other common-handle key types;
* updates that modify the handle column.

## Detailed Design

### Mode Selection

The existing serial executor remains the default for the existing syntax. This avoids breaking valid statements that currently use non-handle indexed shard columns or `INSERT INTO SELECT`.

The range executor is selected only when one of these is true:

1. the SQL statement includes `CONCURRENCY`;
2. `tidb_nontransactional_dml_execution_mode = 'range'`;
3. an internal test hook explicitly selects the range executor.

When the range executor is selected, TiDB validates the stricter P1 statement shape. Unsupported statements return an error that explains that the user can remove the explicit range-mode selection to use the legacy serial executor.

### Supported Statements

P1 supports only single-target, single-table statements. This restriction is required because the chunk scanner bounds work by handles from one table. Multi-table `DELETE` and `UPDATE` can mutate rows in other tables and can affect more rows than the scanned handle chunk. They need a separate rewrite and bounding design.

Supported examples:

```sql
BATCH ON id LIMIT 10000 CONCURRENCY 8
DELETE FROM t WHERE status = 'expired';
```

```sql
BATCH ON id LIMIT 10000 CONCURRENCY 8
UPDATE t SET archived = 1 WHERE status = 'expired';
```

Unsupported in P1:

```sql
BATCH ON t1.id LIMIT 10000 CONCURRENCY 8
DELETE t1, t2 FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.status = 'expired';
```

```sql
BATCH ON t1.id LIMIT 10000 CONCURRENCY 8
UPDATE t1 JOIN t2 ON t1.id = t2.id SET t2.flag = 1 WHERE t1.status = 'expired';
```

`UPDATE` statements are allowed to contain non-idempotent assignments, but the user accepts the non-transactional semantics. For example:

```sql
BATCH ON id LIMIT 10000 CONCURRENCY 8
UPDATE t SET c = c + 1 WHERE status = 'active';
```

P1 does not blindly retry a chunk after an ambiguous commit. It first checks the durable checkpoint. If the checkpoint did not advance, TiDB reports the ambiguous outcome instead of replaying a possibly committed non-idempotent update. Non-idempotent assignments remain part of the user's explicit non-transactional opt-in for failures outside that checkpointed path.

### Executable Task Metadata

The range executor must not use normalized SQL as the executable source of truth. Normalized SQL is suitable for display, digests, logs, and redacted summaries, but it can replace literals and omit details needed for faithful re-execution.

Task metadata is split into:

* executable statement representation: a serialized AST or restored executable SQL with literals preserved;
* redacted display SQL: normalized or redacted SQL for logs and task summaries;
* current database;
* target table ID, physical table IDs, and schema version;
* selected handle column ID and field type;
* original `BATCH` options;
* SQL mode, time zone, charset, collation, foreign-key/check-related flags, and relevant statement variables;
* captured user/security context, including active roles, for privilege-checked internal execution.

If executable SQL with literals is stored in a system table, the privacy and security impact must be documented. Redaction settings must apply to display fields, not to the executable representation.

### Range Planning

P1 plans ranges only on the table record handle keyspace. The planner chooses the handle in this order:

1. If `BATCH ON` is specified, validate that it names the handle column.
2. If the table has a single-column signed integer clustered primary key, use it.
3. If the table has an implicit row ID, use `_tidb_rowid`.
4. Otherwise, reject the statement in range mode.

P1 uses ordered keyset pagination over one logical handle range. It discovers the next chunk by selecting at most `batch_size` qualifying handles after the last in-memory continuation key and then mutates the bounded handle interval for that chunk. This is enough to avoid full pre-scan materialization and to make mutation start immediately.

The chunk uses a handle interval rather than an exact handle set. Under concurrent writes, rows can begin or stop matching the original predicate between chunk discovery and mutation. The original predicate is always rechecked during mutation, but `LIMIT` is therefore a scan target, not a strict affected-row guarantee.

Later DXF and adaptive-planning phases can ask the TiKV Region cache for Region boundaries over the table record keyspace and create multiple independently owned range tasks. Those range boundaries are scheduling hints. Correctness still comes from handle ordering, predicate recheck, and per-chunk checkpoints.

Composite clustered primary keys and common handles are not supported in P1. They require a proof that SQL tuple ordering, encoded key ordering, collations, and boundary sentinels are equivalent for every supported type.

### Chunk Scan and Mutation

Each range task scans handles using keyset pagination. The first scan uses the inclusive range start:

```sql
SELECT handle
FROM t
WHERE handle > checkpoint
  AND original_predicate
ORDER BY handle
LIMIT batch_size;
```

For the first chunk, `checkpoint` is negative infinity. After a successful chunk transaction, the task records the last scanned handle as an exclusive checkpoint. The next scan uses the same keyset predicate with the new checkpoint.

In a later Region/DXF planner, each range task also applies its own upper bound:

```sql
SELECT handle
FROM t
WHERE handle > checkpoint
  AND handle < range_end
  AND original_predicate
ORDER BY handle
LIMIT batch_size;
```

The mutation statement is built from the original DML plus a handle interval and the original predicate. The predicate is rechecked so concurrent changes do not mutate rows that no longer match.

For `DELETE`:

```sql
DELETE FROM t
WHERE handle > checkpoint
  AND handle <= chunk_end
  AND original_predicate
```

For `UPDATE`:

```sql
UPDATE t
SET assignments
WHERE handle > checkpoint
  AND handle <= chunk_end
  AND original_predicate
```

The handle column cannot be updated. This preserves forward progress because the executor uses that column as its ordered checkpoint key.

### Durable Checkpoints

DXF only persists subtask metadata when a subtask finishes. That is not enough for non-transactional DML. A TiDB crash after several committed chunks would replay the whole subtask range and could double-apply non-idempotent updates.

The range executor therefore owns a per-chunk durable checkpoint mechanism. P1 introduces a system table for signed-integer handles:

```sql
CREATE TABLE mysql.tidb_nontransactional_dml_checkpoint (
  job_id varchar(128) NOT NULL,
  range_id bigint NOT NULL,
  table_id bigint NOT NULL,
  current_db varchar(64) NOT NULL,
  table_name varchar(64) NOT NULL,
  checkpoint bigint DEFAULT NULL,
  status varchar(16) NOT NULL,
  scanned bigint NOT NULL DEFAULT 0,
  affected bigint NOT NULL DEFAULT 0,
  error text DEFAULT NULL,
  update_time timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (job_id, range_id)
);
```

Future support for unsigned, composite, and common handles should widen the checkpoint representation to encoded keys or typed tuple values.

Each mutation chunk is a small transaction that contains:

1. the user table mutation;
2. an update to the checkpoint row with the exclusive last scanned handle and progress counters.

This preserves the non-transactional contract across chunks while making each committed chunk and its checkpoint atomic inside one invocation. In P1, the checkpoint is used to classify ambiguous commit results for the active job. Crash/failover resume is not complete until Phase 2 adds stable job identity, ownership, startup checkpoint discovery, and checkpoint lifecycle management.

If the mutation transaction returns an ambiguous commit result, the retry path first reads the checkpoint row. If the checkpoint advanced, the chunk is treated as committed. If it did not advance, TiDB reports the ambiguous outcome instead of replaying a possibly committed non-idempotent update.

### DXF Integration

The DXF task is responsible for ownership, scheduling, failover, and task history. The non-transactional DML executor is responsible for SQL-specific planning, checkpoint persistence, statement construction, and retry classification.

The DXF subtask meta contains immutable range identity and executable task metadata references. It does not serve as the only progress checkpoint. Live progress is persisted in the checkpoint table after every committed chunk. The task meta stores both executable DML text and redacted display DML text, plus current database and captured session variables needed by worker sessions.

The initial task type is `NonTransactionalDML` and has one execution step:

```text
NonTransactionalDMLStepRun
```

The first DXF implementation splits the signed handle interval from `MIN(handle)` to `MAX(handle)` into up to `tidb_nontransactional_dml_concurrency` coarse `(start, end]` subtasks. Each subtask then runs the same bounded chunk scan/mutate loop as the local range executor with the user's `LIMIT` as the per-transaction scan target. This avoids materializing every chunk before execution. Region, statistics, and adaptive split planning remain later phases.

When a DXF subtask starts or restarts, it loads `(checkpoint, range_end)` from `mysql.tidb_nontransactional_dml_checkpoint`. A `done` checkpoint advances the exclusive lower bound and carries cumulative scanned/affected counters. A `failed` checkpoint fails the subtask for user inspection rather than guessing whether a non-idempotent update should be replayed.

If the DXF task manager is not initialized, explicit `dxf` mode falls back to the local range executor. If DXF is initialized, execution uses DXF and task failures are surfaced through DXF task state.

### Local Fallback

The local executor uses the same range planner, chunk scan/mutate loop, and checkpoint table. It runs on the current TiDB node and can use multiple internal sessions capped by `tidb_nontransactional_dml_concurrency`.

This local path is P1. DXF uses the same executor logic with distributed ownership, stable job identity, checkpoint loading, and task history.

### Failure Handling and Retry

Completed chunks stay committed. Failed chunks are reported to the user. TiDB does not roll back previous chunks.

Errors are classified into:

1. retryable errors before commit result is known;
2. ambiguous commit result;
3. permanent errors.

Retryable errors before commit result is known are retried with bounded exponential backoff. P1 keeps this bucket narrow: known TiDB transaction-retryable errors and schema-change retry signals are retried, while deterministic statement errors such as constraint violations are recorded as permanent chunk failures.

For ambiguous commit results, TiDB reads the durable checkpoint row before retrying. If the checkpoint advanced for the active job, TiDB skips to the next chunk. If the checkpoint did not advance, TiDB reports the ambiguous outcome instead of blindly replaying a possibly committed non-idempotent update. Full worker-crash and DXF-failover resume is a Phase 2 requirement.

The existing `tidb_nontransactional_ignore_error` behavior is preserved at chunk level by the serial and local range executors:

* if disabled, the first permanent chunk error fails the task;
* if enabled, TiDB records the failed range or chunk and continues with other ranges where possible.

DXF v1 rejects `tidb_nontransactional_ignore_error=ON`. Supporting it safely needs a separate per-failed-chunk record, because a resumable DXF range currently stores one advancing checkpoint row per range.

### Progress and Observability

The task summary should include:

* task ID;
* redacted display SQL;
* execution mode: local or DXF;
* batch size;
* concurrency;
* planned range count;
* completed range count;
* scanned row count;
* affected row count;
* failed chunk count;
* retry count;
* current range checkpoints.

`DRY RUN` should show the selected executor and why the statement is accepted or rejected by range mode:

```text
execution_mode: range
handle: id
handle_type: int
batch_size: 10000
concurrency: 8
range_strategy: tikv_region
mutation_chunk: DELETE by handle with predicate recheck
```

### Compatibility

The existing serial executor remains available. Existing valid `BATCH` statements do not start failing just because TiDB contains the new range executor.

The range executor keeps the existing constraints:

* autocommit mode only;
* no active user transaction;
* no weak read consistency;
* no `tidb_snapshot`;
* no `ORDER BY` or `LIMIT` inside the original DML;
* handle column cannot be updated.

P1 adds stricter range-mode constraints listed in [P1 Scope](#p1-scope). Unsupported range-mode statements fail with an error that points to the legacy serial executor as the compatibility path.

P1 rejects partitioned tables. A later DXF phase can plan each physical partition independently after specifying physical range identity, ownership, and checkpoint semantics per partition.

Schema changes during execution need explicit handling. P1 captures the schema version used for planning. If a relevant schema change affects the handle column, target table identity, assignment validity, or predicate evaluation, the task fails with a permanent schema-change error. Automatic re-planning is left to a later design.

## Test Design

### Functional Tests

1. Range mode rejects non-handle `BATCH ON` columns.
2. Range mode rejects multi-table `DELETE`.
3. Range mode rejects multi-table `UPDATE`.
4. Range mode rejects `INSERT INTO SELECT`.
5. Range mode rejects composite clustered primary keys.
6. Range mode rejects partitioned tables.
7. Range mode accepts `_tidb_rowid`.
8. Range mode accepts a single-column signed integer clustered primary key.
9. `LIMIT` bounds each chunk's qualifying-handle scan, while concurrent changes can make affected rows differ from the scanned handle count.
10. The checkpoint uses an exclusive continuation key.
11. `UPDATE t SET c = c + 1` documents and exercises retry semantics.
12. A checkpoint and mutation commit atomically in the chunk transaction.

### Scenario Tests

1. A large delete starts mutating before a full qualifying-row pre-scan would finish.
2. An ambiguous commit result for the active job is classified by reading the durable checkpoint row.
3. Permanent error with `tidb_nontransactional_ignore_error = 0` stops the task.
4. Permanent error with `tidb_nontransactional_ignore_error = 1` records failure and continues where safe.
5. Existing serial non-transactional DML still supports indexed shard columns when range mode is not selected.

### Compatibility Tests

1. Existing non-transactional DML tests continue to pass.
2. `DRY RUN` and `DRY RUN QUERY` preserve existing behavior for the serial executor.
3. Privilege checks match normal DML.
4. TiCDC observes normal committed row changes from each chunk.
5. BR backup and restore work with tables modified by range-mode non-transactional DML.

### Benchmark Tests

Benchmarks should compare:

1. current serial non-transactional DML;
2. new local range executor with concurrency 1;
3. new local range executor with concurrency greater than 1;
4. later DXF distributed execution.

Important metrics:

* time to first mutation;
* total runtime;
* rows affected per second;
* transaction size distribution;
* TiDB memory usage;
* TiKV write pressure;
* checkpoint write overhead.

## Roadmap

### Phase 1: Local Range Executor

Implement P1 scope: local handle-range planning, chunk scan/mutate execution, explicit mode selection, and durable checkpoints for active-job ambiguous commit classification.

### Phase 2: DXF Distributed Executor

Add a `NonTransactionalDML` DXF task type. Use DXF for range subtasks, task ownership, failover, progress summaries, and task history. Keep the checkpoint table as the source of resumable progress.

Initial implementation status:

* `tidb_nontransactional_dml_execution_mode = 'dxf'` selects DXF explicitly.
* `NonTransactionalDML` task type and `NonTransactionalDMLStepRun` are registered with DXF.
* the scheduler creates coarse signed-handle `(start, end]` range subtasks without enumerating every mutation chunk;
* each subtask loads the durable checkpoint and resumes from `(checkpoint, range_end)`;
* the statement waits synchronously for the DXF task and returns the existing non-transactional DML result shape;
* local range fallback is used when DXF is not initialized;
* `tidb_nontransactional_ignore_error=ON` is rejected in DXF mode until failed-chunk history is represented separately.

### Phase 3: Composite and Common Handles

Support unsigned integer handles, composite clustered primary keys, and non-integer common handles after specifying and testing the mapping between encoded keys and SQL range predicates.

### Phase 4: Smarter Range Planning

Add optional statistics-based range refinement for handle ranges. Statistics are hints only; correctness remains based on handle boundaries and predicate recheck.

### Phase 5: Adaptive Splitting

Allow a slow range task to split its remaining keyspace and enqueue additional subtasks. Because the checkpoint is an exclusive lower bound, the resumable remaining range is `(checkpoint, range_end)`.

### Phase 6: Secondary Index Sharding and Multi-table DML

Consider supporting `BATCH ON secondary_index_column` and multi-table statements. This requires separate designs for duplicate values, row lookup, collation, index consistency, and bounding affected rows in joined mutations.

## Impacts and Risks

### Impacts

The proposed executor should:

* reduce time to first mutation;
* improve throughput on large DML jobs;
* reduce TiDB memory usage by avoiding full pre-scan job materialization;
* bound each chunk by a `LIMIT`-sized qualifying-handle scan and bounded handle interval;
* provide durable checkpoint evidence for active-job chunk progress.

### Risks

1. Parallel DML can increase write pressure on TiKV and downstream systems.
2. Non-idempotent `UPDATE` can be applied more than once in failure modes not covered by durable chunk checkpoints.
3. Storing executable SQL or serialized AST in system tables has privacy and security implications.
4. P1 rejects session-local variable references and session-local information functions because internal sessions do not yet faithfully inherit every user context field that can affect predicate and assignment evaluation.
5. Schema changes during execution can invalidate planned ranges or statement builders.
6. Checkpoint writes add overhead to every chunk.

## Investigation and Alternatives

### Alternative 1: Parallelize Existing Jobs

TiDB could keep the existing full ordered pre-scan, build all jobs, and execute those jobs with multiple sessions. This is not recommended as the main design because TiDB still has to finish the full qualifying-row pre-scan before the first mutation.

### Alternative 2: One DML per Region Range

TiDB could plan Region ranges and execute one DML statement per range:

```sql
DELETE FROM t
WHERE id >= start
  AND id < end
  AND original_predicate;
```

This is simpler than chunk scan/mutate but does not bound each transaction by affected rows, write amplification, lock conflicts, or downstream pressure. It also cannot be split or checkpointed once the statement starts.

### Alternative 3: Use Statistics Quantiles as the Primary Splitter

Statistics can improve balance when fresh and applicable, but they can be stale, pseudo, missing, or too coarse. They do not fully model arbitrary predicates, update write amplification, or runtime conflicts. Stats should be later hints, not the P1 foundation.

### Alternative 4: Rely on Pipelined DML

Pipelined DML addresses large transaction memory limits while preserving transaction semantics for supported statements. It does not replace non-transactional batch execution because this proposal intentionally accepts partial success, independent chunks, and range execution for maintenance jobs.

## Unresolved Questions

1. Should parser support for `CONCURRENCY` be part of P1, or should P1 use only session variables?
2. What is the exact on-disk representation for executable statement metadata?
3. Should checkpoint records be stored in a new system table or in an extension to DXF subtask checkpoint storage?
4. What retry limit and backoff defaults are appropriate for range-mode DML?
