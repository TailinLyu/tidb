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
WORK_DIR="${WORK_DIR:-$(mktemp -d -t tidb-ntdml-perf.XXXXXX)}"
PROJECT="${PROJECT:-tidb-ntdml-perf}"
TIDB_IMAGE="${TIDB_IMAGE:-tidb-ntdml-test:local}"
PD_IMAGE="${PD_IMAGE:-pingcap/pd:v8.5.6}"
TIKV_IMAGE="${TIKV_IMAGE:-pingcap/tikv:v8.5.6}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"

NTDML_PERF_ROWS="${NTDML_PERF_ROWS:-1000}"
NTDML_PERF_BATCH_SIZE="${NTDML_PERF_BATCH_SIZE:-100}"
NTDML_PERF_PAYLOAD_BYTES="${NTDML_PERF_PAYLOAD_BYTES:-256}"
NTDML_PERF_CONCURRENCY_VALUES="${NTDML_PERF_CONCURRENCY_VALUES:-1 2 4 8 16}"
NTDML_PERF_HANDLE_TYPES="${NTDML_PERF_HANDLE_TYPES:-int varchar varbinary}"

if docker compose version >/dev/null 2>&1; then
	COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
	COMPOSE=(docker-compose)
else
	echo "docker compose or docker-compose is required" >&2
	exit 1
fi

cleanup() {
	status=$?
	if [[ "$status" -ne 0 && -f "$WORK_DIR/docker-compose.yml" ]]; then
		"${COMPOSE[@]}" -p "$PROJECT" -f "$WORK_DIR/docker-compose.yml" logs --no-color --tail=300 >&2 || true
		for tidb in tidb0 tidb1; do
			container="${PROJECT}-${tidb}-1"
			echo "==== ${container} /tmp/${tidb}.log ====" >&2
			docker exec "$container" sh -c "tail -n 300 /tmp/${tidb}.log" >&2 || true
		done
	fi
	if [[ -f "$WORK_DIR/docker-compose.yml" ]]; then
		"${COMPOSE[@]}" -p "$PROJECT" -f "$WORK_DIR/docker-compose.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	fi
	if [[ "${KEEP_WORK_DIR:-0}" != "1" ]]; then
		rm -rf "$WORK_DIR"
	fi
}
trap cleanup EXIT

now_ms() {
	if command -v python3 >/dev/null 2>&1; then
		python3 -c 'import time; print(int(time.time() * 1000))'
	else
		printf "%s000\n" "$(date +%s)"
	fi
}

if [[ "${BUILD_TIDB:-0}" == "1" ]]; then
	DOCKER_GOARCH="$(docker version --format '{{.Server.Arch}}')"
	case "$DOCKER_GOARCH" in
		amd64 | arm64) ;;
		aarch64) DOCKER_GOARCH=arm64 ;;
		x86_64) DOCKER_GOARCH=amd64 ;;
		*)
			echo "unsupported Docker server architecture: $DOCKER_GOARCH" >&2
			exit 1
			;;
	esac
	(cd "$ROOT_DIR" && GOOS=linux GOARCH="$DOCKER_GOARCH" CGO_ENABLED=0 GO111MODULE=on go build -tags codes -o "$WORK_DIR/tidb-server" ./cmd/tidb-server)
	cat > "$WORK_DIR/tidb.Dockerfile" <<'DOCKERFILE'
FROM rockylinux:9-minimal
COPY tidb-server /tidb-server
WORKDIR /
EXPOSE 4000
ENTRYPOINT ["/tidb-server"]
DOCKERFILE
	docker build -t "$TIDB_IMAGE" -f "$WORK_DIR/tidb.Dockerfile" "$WORK_DIR"
else
	echo "Using TIDB_IMAGE=$TIDB_IMAGE. Set BUILD_TIDB=1 to build this branch before running." >&2
fi

cat > "$WORK_DIR/docker-compose.yml" <<YAML
services:
  pd:
    image: ${PD_IMAGE}
    command:
      - --name=pd
      - --data-dir=/data/pd
      - --client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://pd:2379
      - --peer-urls=http://0.0.0.0:2380
      - --advertise-peer-urls=http://pd:2380
      - --initial-cluster=pd=http://pd:2380
    healthcheck:
      test: ["CMD", "wget", "-q", "-O", "-", "http://127.0.0.1:2379/pd/api/v1/version"]
      interval: 2s
      timeout: 2s
      retries: 60
  tikv:
    image: ${TIKV_IMAGE}
    command:
      - --addr=0.0.0.0:20160
      - --advertise-addr=tikv:20160
      - --pd=pd:2379
      - --data-dir=/data/tikv
    depends_on:
      pd:
        condition: service_healthy
  tidb0:
    image: ${TIDB_IMAGE}
    command:
      - --store=tikv
      - --path=pd:2379
      - --host=0.0.0.0
      - --advertise-address=tidb0
      - -P
      - "4000"
      - --status
      - "10080"
      - --log-file=/tmp/tidb0.log
    depends_on:
      - tikv
  tidb1:
    image: ${TIDB_IMAGE}
    command:
      - --store=tikv
      - --path=pd:2379
      - --host=0.0.0.0
      - --advertise-address=tidb1
      - -P
      - "4000"
      - --status
      - "10080"
      - --log-file=/tmp/tidb1.log
    depends_on:
      - tikv
YAML

"${COMPOSE[@]}" -p "$PROJECT" -f "$WORK_DIR/docker-compose.yml" up -d
NETWORK="${PROJECT}_default"

sql_on() {
	host="$1"
	shift
	docker run --rm -i --network "$NETWORK" "$MYSQL_IMAGE" \
		mysql --protocol=tcp -uroot -h"$host" -P4000 --default-character-set=utf8mb4 "$@"
}

sql() {
	sql_on tidb0 "$@"
}

scalar() {
	query="$1"
	sql_on tidb0 -N -B -e "$query" | tail -n 1
}

wait_sql() {
	host="$1"
	for _ in $(seq 1 90); do
		if sql_on "$host" -e "select 1" >/dev/null 2>&1; then
			return
		fi
		sleep 2
	done
	sql_on "$host" -e "select 1" >/dev/null
}

wait_scalar_equals() {
	query="$1"
	expected="$2"
	label="$3"
	for _ in $(seq 1 180); do
		value="$(scalar "$query" 2>/dev/null || true)"
		if [[ "$value" == "$expected" ]]; then
			return
		fi
		sleep 1
	done
	echo "timed out waiting for ${label}; expected ${expected}, got ${value:-<empty>}" >&2
	exit 1
}

checkpoint_cleanup() {
	wait_scalar_equals "SELECT COUNT(*) FROM mysql.tidb_nontransactional_dml_checkpoint" "0" "checkpoint cleanup"
}

row_key() {
	handle="$1"
	i="$2"
	case "$handle" in
		int) printf "%d" "$i" ;;
		varchar) printf "'perf:%08d'" "$i" ;;
		varbinary) printf "'vb:%08d'" "$i" ;;
		*)
			echo "unknown handle type: $handle" >&2
			exit 1
			;;
	esac
}

create_table_sql() {
	handle="$1"
	table="$2"
	case "$handle" in
		int)
			echo "CREATE TABLE ${table}(id BIGINT PRIMARY KEY CLUSTERED, payload TEXT NOT NULL, marker INT NOT NULL DEFAULT 0);"
			;;
		varchar)
			echo "CREATE TABLE ${table}(id VARCHAR(96) COLLATE utf8mb4_bin PRIMARY KEY CLUSTERED, payload TEXT NOT NULL, marker INT NOT NULL DEFAULT 0);"
			;;
		varbinary)
			echo "CREATE TABLE ${table}(id VARBINARY(96) PRIMARY KEY CLUSTERED, payload TEXT NOT NULL, marker INT NOT NULL DEFAULT 0);"
			;;
		*)
			echo "unknown handle type: $handle" >&2
			exit 1
			;;
	esac
}

insert_rows_sql() {
	handle="$1"
	table="$2"
	payload="$3"
	for start in $(seq 0 100 $((NTDML_PERF_ROWS - 1))); do
		values=()
		end=$((start + 99))
		if (( end >= NTDML_PERF_ROWS )); then
			end=$((NTDML_PERF_ROWS - 1))
		fi
		for i in $(seq "$start" "$end"); do
			values+=("($(row_key "$handle" "$i"),'$payload',0)")
		done
		IFS=,
		echo "INSERT INTO ${table} VALUES ${values[*]};"
		unset IFS
	done
}

split_table_sql() {
	handle="$1"
	table="$2"
	split_points=()
	for divisor in 1 2 3 4 5 6 7; do
		point=$((NTDML_PERF_ROWS * divisor / 8))
		if (( point > 0 && point < NTDML_PERF_ROWS )); then
			split_points+=("($(row_key "$handle" "$point"))")
		fi
	done
	if (( ${#split_points[@]} > 0 )); then
		IFS=,
		echo "SPLIT TABLE ${table} BY ${split_points[*]};"
		unset IFS
	fi
}

lower_bound_predicate() {
	handle="$1"
	case "$handle" in
		int) echo "id >= 0" ;;
		varchar) echo "id >= 'perf:00000000'" ;;
		varbinary) echo "id >= 'vb:00000000'" ;;
	esac
}

run_case() {
	handle="$1"
	mode="$2"
	concurrency="$3"
	table="t_${handle}_${mode}_${concurrency}"
	sql_file="$WORK_DIR/${table}.sql"
	payload="$(printf '%*s' "$NTDML_PERF_PAYLOAD_BYTES" '' | tr ' ' 'x')"

	{
		echo "USE ntdml_perf;"
		echo "DROP TABLE IF EXISTS ${table};"
		create_table_sql "$handle" "$table"
		insert_rows_sql "$handle" "$table" "$payload"
		split_table_sql "$handle" "$table"
		echo "SET tidb_nontransactional_dml_execution_mode = '${mode}';"
		echo "SET tidb_nontransactional_dml_concurrency = ${concurrency};"
		echo "BATCH ON id LIMIT ${NTDML_PERF_BATCH_SIZE} UPDATE ${table} SET marker = 1 WHERE $(lower_bound_predicate "$handle");"
	} > "$sql_file"

	start_ms="$(now_ms)"
	sql < "$sql_file" >/dev/null
	end_ms="$(now_ms)"
	updated_rows="$(scalar "SELECT COUNT(*) FROM ntdml_perf.${table} WHERE marker = 1")"
	if [[ "$updated_rows" != "$NTDML_PERF_ROWS" ]]; then
		echo "${table} updated ${updated_rows} rows, expected ${NTDML_PERF_ROWS}" >&2
		exit 1
	fi
	checkpoint_cleanup
	printf "%s,%s,%s,%s,%s,%s,%s,%s\n" \
		"$handle" \
		"$mode" \
		"$concurrency" \
		"$NTDML_PERF_ROWS" \
		"$NTDML_PERF_BATCH_SIZE" \
		"$NTDML_PERF_PAYLOAD_BYTES" \
		"$((end_ms - start_ms))" \
		"$NTDML_PERF_ROWS"
}

wait_sql tidb0
wait_sql tidb1

sql <<'SQL' >/dev/null
SET GLOBAL tidb_enable_dist_task = 1;
DROP DATABASE IF EXISTS ntdml_perf;
CREATE DATABASE ntdml_perf;
SQL

echo "handle,mode,concurrency,rows,batch_size,payload_bytes,duration_ms,affected_rows"
for handle in $NTDML_PERF_HANDLE_TYPES; do
	run_case "$handle" serial 1
	for concurrency in $NTDML_PERF_CONCURRENCY_VALUES; do
		run_case "$handle" range "$concurrency"
	done
	for concurrency in $NTDML_PERF_CONCURRENCY_VALUES; do
		run_case "$handle" dxf "$concurrency"
	done
done

echo "parallel non-transactional DML performance smoke passed" >&2
