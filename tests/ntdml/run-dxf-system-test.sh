#!/usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TIDB_BIN="${TIDB_BIN:-$ROOT_DIR/bin/tidb-server}"
BUILD_TIDB="${BUILD_TIDB:-0}"
PD_IMAGE="${PD_IMAGE:-pingcap/pd:v8.5.6}"
TIKV_IMAGE="${TIKV_IMAGE:-pingcap/tikv:v8.5.6}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"
ROWS="${ROWS:-200000}"
BATCH_SIZE="${BATCH_SIZE:-5000}"
CONCURRENCY="${CONCURRENCY:-4}"
SLEEP_SECONDS="${SLEEP_SECONDS:-0.0002}"
SUFFIX="${SUFFIX:-$$}"
PREFIX="ntdml-dxf-system-${SUFFIX}"

PD_PORT="${PD_PORT:-32379}"
PD_PEER_PORT="${PD_PEER_PORT:-32380}"
TIKV_PORT="${TIKV_PORT:-32160}"
TIDB1_PORT="${TIDB1_PORT:-34010}"
TIDB2_PORT="${TIDB2_PORT:-34011}"
TIDB1_STATUS_PORT="${TIDB1_STATUS_PORT:-31090}"
TIDB2_STATUS_PORT="${TIDB2_STATUS_PORT:-31091}"

TIDB1_PID=""
TIDB2_PID=""

log() {
	echo "[$(date '+%H:%M:%S')] $*"
}

find_host_ip() {
	if [[ "$(uname -s)" == "Darwin" ]]; then
		ipconfig getifaddr en0
	else
		hostname -I | awk '{print $1}'
	fi
}

ensure_port_free() {
	local port="$1"
	if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
		echo "port $port is already in use" >&2
		exit 1
	fi
}

cleanup() {
	set +e
	if [[ -n "$TIDB1_PID" ]]; then
		kill "$TIDB1_PID" >/dev/null 2>&1
		wait "$TIDB1_PID" >/dev/null 2>&1
	fi
	if [[ -n "$TIDB2_PID" ]]; then
		kill "$TIDB2_PID" >/dev/null 2>&1
		wait "$TIDB2_PID" >/dev/null 2>&1
	fi
	docker rm -f "${PREFIX}-tikv" "${PREFIX}-pd" >/dev/null 2>&1
	rm -f "/tmp/${PREFIX}-tidb1.log" "/tmp/${PREFIX}-tidb2.log" \
		"/tmp/${PREFIX}-dxf-owner.out" "/tmp/${PREFIX}-dxf-executor.out" "/tmp/${PREFIX}-dxf-rolling.out"
}
trap cleanup EXIT

mysql_cmd() {
	local port="$1"
	shift
	docker run --rm -i --add-host=host.docker.internal:host-gateway "$MYSQL_IMAGE" \
		mysql -hhost.docker.internal -P"$port" -uroot --connect-timeout=5 "$@"
}

wait_for_http() {
	local url="$1"
	for _ in $(seq 1 90); do
		if curl -fsS "$url" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	curl -fsS "$url"
}

wait_for_tidb() {
	local port="$1"
	for _ in $(seq 1 90); do
		if mysql_cmd "$port" -e "select 1" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	mysql_cmd "$port" -e "select 1"
}

query_scalar() {
	local port="$1"
	local sql="$2"
	mysql_cmd "$port" -N -B -e "$sql" | tail -n 1
}

wait_for_task_count() {
	local expected="$1"
	for _ in $(seq 1 120); do
		local count
		count="$(query_scalar "$TIDB2_PORT" "select count(*) from (select type from mysql.tidb_global_task union all select type from mysql.tidb_global_task_history) t where type='NonTransactionalDML'")"
		if [[ "$count" -ge "$expected" ]]; then
			return 0
		fi
		sleep 1
	done
	echo "timed out waiting for NonTransactionalDML task count >= $expected" >&2
	exit 1
}

wait_for_succeeded_tasks() {
	local expected="$1"
	for _ in $(seq 1 180); do
		local row
		row="$(mysql_cmd "$TIDB2_PORT" -N -B -e "select count(*), sum(state='succeed'), sum(state='failed') from (select type,state from mysql.tidb_global_task union all select type,state from mysql.tidb_global_task_history) t where type='NonTransactionalDML'" | tail -n 1)"
		local total succeeded failed
		read -r total succeeded failed <<<"$row"
		succeeded="${succeeded:-0}"
		failed="${failed:-0}"
		if [[ "$failed" != "0" && "$failed" != "NULL" ]]; then
			echo "NonTransactionalDML task failed: $row" >&2
			exit 1
		fi
		if [[ "$total" -ge "$expected" && "$succeeded" -ge "$expected" ]]; then
			return 0
		fi
		sleep 1
	done
	echo "timed out waiting for $expected succeeded NonTransactionalDML tasks" >&2
	exit 1
}

checkpoint_count() {
	query_scalar "$TIDB2_PORT" "select count(*) from mysql.tidb_nontransactional_dml_checkpoint where current_db='ntdml_system' and table_name='t'"
}

wait_for_checkpoint_count_at_least() {
	local expected="$1"
	local label="$2"
	for _ in $(seq 1 120); do
		local count
		count="$(checkpoint_count)"
		if [[ "$count" -ge "$expected" ]]; then
			return 0
		fi
		sleep 1
	done
	echo "timed out waiting for checkpoint progress during ${label}; expected >= ${expected}" >&2
	exit 1
}

start_tidb1() {
	"$TIDB_BIN" -store=tikv -path="${HOST_IP}:${PD_PORT}" -host=127.0.0.1 -P="$TIDB1_PORT" -status="$TIDB1_STATUS_PORT" \
		-lease=1 -log-file="/tmp/${PREFIX}-tidb1.log" >/dev/null 2>&1 &
	TIDB1_PID="$!"
	wait_for_http "http://127.0.0.1:${TIDB1_STATUS_PORT}/status"
	wait_for_tidb "$TIDB1_PORT"
}

start_tidb2() {
	"$TIDB_BIN" -store=tikv -path="${HOST_IP}:${PD_PORT}" -host=127.0.0.1 -P="$TIDB2_PORT" -status="$TIDB2_STATUS_PORT" \
		-lease=1 -log-file="/tmp/${PREFIX}-tidb2.log" >/dev/null 2>&1 &
	TIDB2_PID="$!"
	wait_for_http "http://127.0.0.1:${TIDB2_STATUS_PORT}/status"
	wait_for_tidb "$TIDB2_PORT"
}

stop_tidb1() {
	if [[ -n "$TIDB1_PID" ]]; then
		kill "$TIDB1_PID"
		wait "$TIDB1_PID" || true
		TIDB1_PID=""
	fi
}

stop_tidb2() {
	if [[ -n "$TIDB2_PID" ]]; then
		kill "$TIDB2_PID"
		wait "$TIDB2_PID" || true
		TIDB2_PID=""
	fi
}

if (( ROWS < 200000 )); then
	echo "ROWS must be at least 200000 because the test deletes rows 150001..200000" >&2
	exit 1
fi

if [[ "$BUILD_TIDB" == "1" || ! -x "$TIDB_BIN" ]]; then
	log "building tidb-server at $TIDB_BIN"
	(cd "$ROOT_DIR" && make server)
fi

for port in "$PD_PORT" "$PD_PEER_PORT" "$TIKV_PORT" "$TIDB1_PORT" "$TIDB2_PORT" "$TIDB1_STATUS_PORT" "$TIDB2_STATUS_PORT"; do
	ensure_port_free "$port"
done

HOST_IP="${HOST_IP:-$(find_host_ip)}"
log "using host ip ${HOST_IP}"

docker rm -f "${PREFIX}-tikv" "${PREFIX}-pd" >/dev/null 2>&1 || true

log "starting PD and TiKV"
docker run -d --name "${PREFIX}-pd" -p "${PD_PORT}:2379" -p "${PD_PEER_PORT}:2380" "$PD_IMAGE" \
	--name=pd --data-dir=/tmp/pd \
	--client-urls=http://0.0.0.0:2379 --advertise-client-urls="http://${HOST_IP}:${PD_PORT}" \
	--peer-urls=http://0.0.0.0:2380 --advertise-peer-urls="http://${HOST_IP}:${PD_PEER_PORT}" \
	--initial-cluster="pd=http://${HOST_IP}:${PD_PEER_PORT}" --log-level=info >/dev/null

docker run -d --name "${PREFIX}-tikv" -p "${TIKV_PORT}:20160" "$TIKV_IMAGE" \
	--addr=0.0.0.0:20160 --advertise-addr="${HOST_IP}:${TIKV_PORT}" \
	--pd="${HOST_IP}:${PD_PORT}" --data-dir=/tmp/tikv --log-level=info >/dev/null

wait_for_http "http://127.0.0.1:${PD_PORT}/pd/api/v1/members"
store_up=0
for _ in $(seq 1 90); do
	if curl -fsS "http://127.0.0.1:${PD_PORT}/pd/api/v1/stores" | grep -Eq '"state_name"[[:space:]]*:[[:space:]]*"Up"'; then
		store_up=1
		break
	fi
	sleep 1
done
if [[ "$store_up" != "1" ]]; then
	curl -fsS "http://127.0.0.1:${PD_PORT}/pd/api/v1/stores" || true
	echo "TiKV store did not become Up" >&2
	exit 1
fi

log "starting two TiDB nodes"
start_tidb1
start_tidb2

log "loading ${ROWS} rows"
mysql_cmd "$TIDB1_PORT" <<SQL
SET @@global.tidb_enable_dist_task = ON;
DROP DATABASE IF EXISTS ntdml_system;
CREATE DATABASE ntdml_system;
USE ntdml_system;
CREATE TABLE digits(d INT PRIMARY KEY);
INSERT INTO digits VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE t(a INT PRIMARY KEY CLUSTERED, b INT, pad VARBINARY(256));
INSERT INTO t(a,b,pad)
SELECT n, n % 1000, REPEAT('x', 256)
FROM (
  SELECT d1.d + d2.d*10 + d3.d*100 + d4.d*1000 + d5.d*10000 + d6.d*100000 + 1 AS n
  FROM digits d1 JOIN digits d2 JOIN digits d3 JOIN digits d4 JOIN digits d5 JOIN digits d6
) s
WHERE n BETWEEN 1 AND ${ROWS};
SELECT COUNT(*) AS loaded_rows, SUM(LENGTH(pad)) AS payload_bytes FROM t;
SQL

log "starting DXF update and restarting the submitter/owner candidate"
checkpoint_baseline="$(checkpoint_count)"
set +e
mysql_cmd "$TIDB1_PORT" >/tmp/"${PREFIX}-dxf-owner.out" 2>&1 <<SQL &
USE ntdml_system;
SET @@tidb_nontransactional_dml_execution_mode='dxf';
SET @@tidb_nontransactional_dml_concurrency=${CONCURRENCY};
BATCH ON a LIMIT ${BATCH_SIZE} UPDATE t SET b = 1000 + (a % 1000) + sleep(${SLEEP_SECONDS}) WHERE a BETWEEN 1 AND 50000;
SQL
OWNER_CLIENT_PID="$!"
set -e
wait_for_task_count 1
wait_for_checkpoint_count_at_least "$((checkpoint_baseline + 1))" "submitter/owner restart"
stop_tidb1
start_tidb1
wait "$OWNER_CLIENT_PID" || true
wait_for_succeeded_tasks 1

log "starting DXF delete and restarting the second executor"
checkpoint_baseline="$(checkpoint_count)"
set +e
mysql_cmd "$TIDB1_PORT" >/tmp/"${PREFIX}-dxf-executor.out" 2>&1 <<SQL &
USE ntdml_system;
SET @@tidb_nontransactional_dml_execution_mode='dxf';
SET @@tidb_nontransactional_dml_concurrency=${CONCURRENCY};
BATCH ON a LIMIT ${BATCH_SIZE} DELETE FROM t WHERE a BETWEEN 150001 AND 200000 AND sleep(${SLEEP_SECONDS}) = 0;
SQL
EXEC_CLIENT_PID="$!"
set -e
wait_for_task_count 2
wait_for_checkpoint_count_at_least "$((checkpoint_baseline + 1))" "executor restart"
stop_tidb2
start_tidb2
wait "$EXEC_CLIENT_PID" || true
wait_for_succeeded_tasks 2

log "starting DXF update and rolling restarting both TiDB nodes"
checkpoint_baseline="$(checkpoint_count)"
set +e
mysql_cmd "$TIDB1_PORT" >/tmp/"${PREFIX}-dxf-rolling.out" 2>&1 <<SQL &
USE ntdml_system;
SET @@tidb_nontransactional_dml_execution_mode='dxf';
SET @@tidb_nontransactional_dml_concurrency=${CONCURRENCY};
BATCH ON a LIMIT ${BATCH_SIZE} UPDATE t SET b = 2000 + (a % 1000) + sleep(${SLEEP_SECONDS}) WHERE a BETWEEN 50001 AND 100000;
SQL
ROLLING_CLIENT_PID="$!"
set -e
wait_for_task_count 3
wait_for_checkpoint_count_at_least "$((checkpoint_baseline + 1))" "rolling restart"
stop_tidb1
start_tidb1
stop_tidb2
start_tidb2
wait "$ROLLING_CLIENT_PID" || true
wait_for_succeeded_tasks 3

log "checking data integrity"
mysql_cmd "$TIDB1_PORT" <<SQL
USE ntdml_system;
SELECT COUNT(*) AS updated_rows FROM t WHERE a BETWEEN 1 AND 50000 AND b >= 1000;
SELECT COUNT(*) AS rolling_updated_rows FROM t WHERE a BETWEEN 50001 AND 100000 AND b >= 2000;
SELECT COUNT(*) AS deleted_rows FROM t WHERE a BETWEEN 150001 AND 200000;
SELECT COUNT(*) AS final_rows FROM t;
SELECT status, COUNT(*) AS checkpoints, SUM(scanned) AS scanned, SUM(affected) AS affected
FROM mysql.tidb_nontransactional_dml_checkpoint
WHERE current_db='ntdml_system' AND table_name='t'
GROUP BY status ORDER BY status;
SQL

updated_rows="$(query_scalar "$TIDB1_PORT" "select count(*) from ntdml_system.t where a between 1 and 50000 and b >= 1000")"
rolling_updated_rows="$(query_scalar "$TIDB1_PORT" "select count(*) from ntdml_system.t where a between 50001 and 100000 and b >= 2000")"
deleted_rows="$(query_scalar "$TIDB1_PORT" "select count(*) from ntdml_system.t where a between 150001 and 200000")"
final_rows="$(query_scalar "$TIDB1_PORT" "select count(*) from ntdml_system.t")"
done_statuses="$(query_scalar "$TIDB1_PORT" "select count(*) from mysql.tidb_nontransactional_dml_checkpoint where current_db='ntdml_system' and table_name='t' and status <> 'done'")"

if [[ "$updated_rows" != "50000" ]]; then
	echo "expected 50000 updated rows, got $updated_rows" >&2
	exit 1
fi
if [[ "$rolling_updated_rows" != "50000" ]]; then
	echo "expected 50000 rolling-updated rows, got $rolling_updated_rows" >&2
	exit 1
fi
if [[ "$deleted_rows" != "0" ]]; then
	echo "expected deleted range to be empty, got $deleted_rows" >&2
	exit 1
fi
if [[ "$final_rows" != "$((ROWS - 50000))" ]]; then
	echo "expected final row count $((ROWS - 50000)), got $final_rows" >&2
	exit 1
fi
if [[ "$done_statuses" != "0" ]]; then
	echo "expected only done checkpoints, got $done_statuses non-done rows" >&2
	exit 1
fi

log "dropping test database"
mysql_cmd "$TIDB1_PORT" -e "DROP DATABASE IF EXISTS ntdml_system"

log "DXF system test passed"
