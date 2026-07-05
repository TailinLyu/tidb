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
WORK_DIR="${WORK_DIR:-$(mktemp -d -t tidb-ntdml-dxf.XXXXXX)}"
PROJECT="${PROJECT:-tidb-ntdml-dxf}"
TIDB_IMAGE="${TIDB_IMAGE:-tidb-ntdml-test:local}"
PD_IMAGE="${PD_IMAGE:-pingcap/pd:v8.5.0}"
TIKV_IMAGE="${TIKV_IMAGE:-pingcap/tikv:v8.5.0}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"
CURL_IMAGE="${CURL_IMAGE:-curlimages/curl:8.8.0}"
PROMETHEUS_IMAGE="${PROMETHEUS_IMAGE:-prom/prometheus:v2.53.0}"
GRAFANA_IMAGE="${GRAFANA_IMAGE:-grafana/grafana:10.4.5}"

# Default data sizes are kept laptop-friendly. For a larger/GB-class run, set
# NTDML_LARGE_MODE=1 or override these directly, for example:
#   NTDML_RESTART_ROWS=250000 NTDML_PAYLOAD_BYTES=4096 NTDML_RESTART_SLEEP_SECONDS=0
if [[ "${NTDML_LARGE_MODE:-0}" == "1" ]]; then
	: "${NTDML_RESTART_ROWS:=250000}"
	: "${NTDML_PAYLOAD_BYTES:=4096}"
	: "${NTDML_RESTART_SLEEP_SECONDS:=0}"
else
	: "${NTDML_RESTART_ROWS:=800}"
	: "${NTDML_PAYLOAD_BYTES:=1024}"
	: "${NTDML_RESTART_SLEEP_SECONDS:=0.02}"
fi

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

mkdir -p "$WORK_DIR/grafana/provisioning/datasources" "$WORK_DIR/grafana/provisioning/dashboards"

cat > "$WORK_DIR/prometheus.yml" <<'YAML'
global:
  scrape_interval: 2s
  evaluation_interval: 2s
scrape_configs:
  - job_name: tidb
    metrics_path: /metrics
    static_configs:
      - targets:
          - tidb0:10080
          - tidb1:10080
YAML

cat > "$WORK_DIR/grafana/provisioning/datasources/prometheus.yml" <<'YAML'
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
YAML

cat > "$WORK_DIR/grafana/provisioning/dashboards/tidb.yml" <<'YAML'
apiVersion: 1
providers:
  - name: tidb-ntdml
    orgId: 1
    folder: TiDB
    type: file
    disableDeletion: false
    updateIntervalSeconds: 5
    allowUiUpdates: false
    options:
      path: /var/lib/grafana/dashboards
YAML

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
  prometheus:
    image: ${PROMETHEUS_IMAGE}
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.retention.time=1h
    volumes:
      - "${WORK_DIR}/prometheus.yml:/etc/prometheus/prometheus.yml:ro"
    depends_on:
      - tidb0
      - tidb1
  grafana:
    image: ${GRAFANA_IMAGE}
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
      GF_LOG_LEVEL: warn
    volumes:
      - "${WORK_DIR}/grafana/provisioning:/etc/grafana/provisioning:ro"
      - "${ROOT_DIR}/pkg/metrics/grafana/non_transactional_dml.json:/var/lib/grafana/dashboards/non_transactional_dml.json:ro"
    depends_on:
      - prometheus
YAML

"${COMPOSE[@]}" -p "$PROJECT" -f "$WORK_DIR/docker-compose.yml" up -d
NETWORK="${PROJECT}_default"

tidb_container() {
	echo "${PROJECT}-$1-1"
}

sql_on() {
	host="$1"
	shift
	docker run --rm -i --network "$NETWORK" "$MYSQL_IMAGE" \
		mysql --protocol=tcp -uroot -h"$host" -P4000 --default-character-set=utf8mb4 "$@"
}

sql() {
	sql_on tidb0 "$@"
}

scalar_on() {
	host="$1"
	query="$2"
	sql_on "$host" -N -B -e "$query" | tail -n 1
}

scalar() {
	scalar_on tidb0 "$1"
}

http_get() {
	docker run --rm --network "$NETWORK" "$CURL_IMAGE" -fsS "$@"
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

wait_status() {
	host="$1"
	for _ in $(seq 1 60); do
		if http_get "http://${host}:10080/status" >/dev/null 2>&1; then
			return
		fi
		sleep 2
	done
	http_get "http://${host}:10080/status" >/dev/null
}

wait_http() {
	url="$1"
	label="$2"
	for _ in $(seq 1 90); do
		if http_get "$url" >/dev/null 2>&1; then
			return
		fi
		sleep 2
	done
	echo "timed out waiting for ${label}: ${url}" >&2
	http_get "$url" >/dev/null
}

query_metrics() {
	source="$1"
	query="$2"
	case "$source" in
		prometheus)
			http_get --get --data-urlencode "query=$query" "http://prometheus:9090/api/v1/query"
			;;
		grafana)
			http_get -u admin:admin --get --data-urlencode "query=$query" \
				"http://grafana:3000/api/datasources/proxy/uid/prometheus/api/v1/query"
			;;
		*)
			echo "unknown metrics query source: $source" >&2
			exit 1
			;;
	esac
}

wait_metrics_query_nonempty() {
	source="$1"
	query="$2"
	label="$3"
	response=""
	for _ in $(seq 1 120); do
		response="$(query_metrics "$source" "$query" 2>/dev/null || true)"
		if printf "%s" "$response" | grep -q '"status":"success"' &&
			printf "%s" "$response" | grep -q '"result":\[{' ; then
			return
		fi
		sleep 2
	done
	echo "timed out waiting for ${source} query result: ${label}" >&2
	echo "query: ${query}" >&2
	echo "last response: ${response:-<empty>}" >&2
	exit 1
}

wait_grafana_provisioning() {
	wait_http "http://grafana:3000/api/health" "Grafana health"

	response=""
	for _ in $(seq 1 90); do
		response="$(http_get -u admin:admin "http://grafana:3000/api/datasources/uid/prometheus" 2>/dev/null || true)"
		if printf "%s" "$response" | grep -q '"type":"prometheus"'; then
			break
		fi
		sleep 2
	done
	if ! printf "%s" "$response" | grep -q '"type":"prometheus"'; then
		echo "timed out waiting for Grafana Prometheus datasource" >&2
		echo "last response: ${response:-<empty>}" >&2
		exit 1
	fi

	response=""
	for _ in $(seq 1 90); do
		response="$(http_get -u admin:admin "http://grafana:3000/api/dashboards/uid/tidb-non-transactional-dml" 2>/dev/null || true)"
		if printf "%s" "$response" | grep -q '"title":"Test-Cluster-TiDB-Non-Transactional-DML"'; then
			return
		fi
		sleep 2
	done
	echo "timed out waiting for Grafana NTDML dashboard provisioning" >&2
	echo "last response: ${response:-<empty>}" >&2
	exit 1
}

wait_ntdml_metrics_captured() {
	source="$1"
	wait_metrics_query_nonempty "$source" \
		'sum(increase(tidb_session_non_transactional_dml_count[10m])) > 0' \
		"non-transactional DML statement count"
	wait_metrics_query_nonempty "$source" \
		'sum(increase(tidb_session_non_transactional_dml_task_total[10m])) > 0' \
		"non-transactional DML task counter"
	wait_metrics_query_nonempty "$source" \
		'sum(increase(tidb_session_non_transactional_dml_chunk_total[10m])) > 0' \
		"non-transactional DML chunk counter"
	wait_metrics_query_nonempty "$source" \
		'sum(increase(tidb_session_non_transactional_dml_rows_total[10m])) > 0' \
		"non-transactional DML rows counter"
	wait_metrics_query_nonempty "$source" \
		'sum(increase(tidb_session_non_transactional_dml_duration_seconds_count[10m])) > 0' \
		"non-transactional DML duration histogram"
}

wait_scalar_equals() {
	host="$1"
	query="$2"
	expected="$3"
	label="$4"
	for _ in $(seq 1 180); do
		value="$(scalar_on "$host" "$query" 2>/dev/null || true)"
		if [[ "$value" == "$expected" ]]; then
			return
		fi
		sleep 1
	done
	echo "timed out waiting for ${label}; expected ${expected}, got ${value:-<empty>}" >&2
	exit 1
}

wait_positive_scalar() {
	host="$1"
	query="$2"
	label="$3"
	for _ in $(seq 1 180); do
		value="$(scalar_on "$host" "$query" 2>/dev/null || true)"
		if [[ "$value" =~ ^[0-9]+$ ]] && (( value > 0 )); then
			echo "$value"
			return
		fi
		sleep 1
	done
	echo "timed out waiting for ${label}; got ${value:-<empty>}" >&2
	exit 1
}

wait_checkpoint_cleanup() {
	host="$1"
	wait_scalar_equals "$host" "SELECT COUNT(*) FROM mysql.tidb_nontransactional_dml_checkpoint" "0" "checkpoint cleanup"
}

ensure_failover_running() {
	label="$1"
	if ! kill -0 "$failover_pid" 2>/dev/null; then
		echo "failover workload finished before ${label}" >&2
		cat "$WORK_DIR/failover.out" >&2 || true
		cat "$WORK_DIR/failover.err" >&2 || true
		exit 1
	fi
}

wait_sql tidb0
wait_sql tidb1
wait_http "http://prometheus:9090/-/ready" "Prometheus readiness"
wait_grafana_provisioning
wait_metrics_query_nonempty prometheus 'count(up{job="tidb"} == 1) == 2' "Prometheus TiDB scrape targets"
wait_metrics_query_nonempty grafana 'count(up{job="tidb"} == 1) == 2' "Grafana Prometheus datasource query"

{
	cat <<'SQL'
SET GLOBAL tidb_enable_dist_task = 1;
DROP DATABASE IF EXISTS ntdml_system;
CREATE DATABASE ntdml_system;
USE ntdml_system;
SET tidb_nontransactional_dml_execution_mode = 'dxf';
SET tidb_nontransactional_dml_concurrency = 4;
CREATE TABLE t_int(id BIGINT PRIMARY KEY CLUSTERED, v INT NOT NULL);
SQL
	for start in $(seq 0 20 180); do
		values=()
		for i in $(seq "$start" $((start + 19))); do
			values+=("($i,$i)")
		done
		IFS=,
		echo "INSERT INTO t_int VALUES ${values[*]};"
		unset IFS
	done
	cat <<'SQL'
SPLIT TABLE t_int BY (25), (50), (75), (100), (125), (150), (175);
BATCH ON id LIMIT 25 UPDATE t_int SET v = 100 WHERE id >= 0 AND id < 200;
SELECT IF(COUNT(*) = 200, 'ok', CONCAT('bad_int_count=', COUNT(*))) AS int_rows FROM t_int WHERE v = 100;
SQL
} > "$WORK_DIR/int.sql"
sql < "$WORK_DIR/int.sql"

docker restart "$(tidb_container tidb1)" >/dev/null
wait_status tidb1
wait_sql tidb1

{
	cat <<'SQL'
USE ntdml_system;
SET tidb_nontransactional_dml_execution_mode = 'dxf';
SET tidb_nontransactional_dml_concurrency = 4;
CREATE TABLE t_varchar(
  id VARCHAR(64) COLLATE utf8mb4_bin PRIMARY KEY CLUSTERED,
  payload VARCHAR(64),
  marker JSON
);
SQL
	for start in $(seq 0 20 180); do
		values=()
		for i in $(seq "$start" $((start + 19))); do
			key=$(printf "v1:pacer_largepayload%04d" "$i")
			values+=("('$key','payload-$i',JSON_OBJECT('deleted_at','live'))")
		done
		IFS=,
		echo "INSERT INTO t_varchar VALUES ${values[*]};"
		unset IFS
	done
	cat <<'SQL'
SPLIT TABLE t_varchar BY
  ('v1:pacer_largepayload0050'),
  ('v1:pacer_largepayload0100'),
  ('v1:pacer_largepayload0150');
BATCH ON id LIMIT 30 UPDATE t_varchar
  SET marker = JSON_SET(marker, '$.deleted_at', CAST('null' AS JSON))
  WHERE id >= 'v1:pacer_largepayload0000' AND id < 'v1:pacer_largepayload0200';
SELECT IF(COUNT(*) = 200, 'ok', CONCAT('bad_varchar_count=', COUNT(*))) AS varchar_rows
  FROM t_varchar
  WHERE JSON_TYPE(JSON_EXTRACT(marker, '$.deleted_at')) = 'NULL';
BATCH ON id LIMIT 35 DELETE FROM t_varchar
  WHERE id >= 'v1:pacer_largepayload0040' AND id < 'v1:pacer_largepayload0160';
SELECT IF(COUNT(*) = 80, 'ok', CONCAT('bad_delete_remaining=', COUNT(*))) AS delete_rows FROM t_varchar;
SQL
} > "$WORK_DIR/varchar.sql"
sql < "$WORK_DIR/varchar.sql"

wait_checkpoint_cleanup tidb0

payload="$(printf '%*s' "$NTDML_PAYLOAD_BYTES" '' | tr ' ' 'x')"
{
	cat <<'SQL'
USE ntdml_system;
SET tidb_nontransactional_dml_execution_mode = 'dxf';
SET tidb_nontransactional_dml_concurrency = 4;
DROP TABLE IF EXISTS t_failover;
CREATE TABLE t_failover(
  id BIGINT PRIMARY KEY CLUSTERED,
  payload TEXT NOT NULL,
  marker VARCHAR(16) NOT NULL
);
SQL
	for start in $(seq 0 20 $((NTDML_RESTART_ROWS - 1))); do
		values=()
		end=$((start + 19))
		if (( end >= NTDML_RESTART_ROWS )); then
			end=$((NTDML_RESTART_ROWS - 1))
		fi
		for i in $(seq "$start" "$end"); do
			values+=("($i,'$payload','todo')")
		done
		IFS=,
		echo "INSERT INTO t_failover VALUES ${values[*]};"
		unset IFS
	done
	split_points=()
	for divisor in 1 2 3 4 5 6 7; do
		point=$((NTDML_RESTART_ROWS * divisor / 8))
		if (( point > 0 && point < NTDML_RESTART_ROWS )); then
			split_points+=("($point)")
		fi
	done
	if (( ${#split_points[@]} > 0 )); then
		IFS=,
		echo "SPLIT TABLE t_failover BY ${split_points[*]};"
		unset IFS
	fi
} > "$WORK_DIR/failover_setup.sql"
sql < "$WORK_DIR/failover_setup.sql"

cat > "$WORK_DIR/failover_run.sql" <<SQL
USE ntdml_system;
SET tidb_nontransactional_dml_execution_mode = 'dxf';
SET tidb_nontransactional_dml_concurrency = 4;
BATCH ON id LIMIT 20 UPDATE t_failover
  SET marker = 'done'
  WHERE id >= 0 AND SLEEP(${NTDML_RESTART_SLEEP_SECONDS}) = 0;
SQL

sql < "$WORK_DIR/failover_run.sql" > "$WORK_DIR/failover.out" 2> "$WORK_DIR/failover.err" &
failover_pid=$!

wait_positive_scalar tidb1 \
	"SELECT COUNT(*) FROM mysql.tidb_nontransactional_dml_checkpoint WHERE db_name='ntdml_system' AND status='done'" \
	"in-flight checkpoint progress" >/dev/null

ensure_failover_running "Prometheus/Grafana metrics verification started"
http_get http://tidb0:10080/metrics \
	| grep -E 'tidb_session_non_transactional_dml_(task|chunk|rows)_total' >/dev/null
wait_ntdml_metrics_captured prometheus
wait_ntdml_metrics_captured grafana
ensure_failover_running "Prometheus/Grafana metrics verification completed"

docker restart "$(tidb_container tidb1)" >/dev/null
wait_status tidb1
wait_sql tidb1

ensure_failover_running "restart coverage completed"

docker kill "$(tidb_container tidb0)" >/dev/null
wait "$failover_pid" >/dev/null 2>&1 || true

wait_scalar_equals tidb1 \
	"SELECT COUNT(*) FROM ntdml_system.t_failover WHERE marker='done'" \
	"$NTDML_RESTART_ROWS" \
	"DXF completion after submitter/owner loss"

docker start "$(tidb_container tidb0)" >/dev/null
wait_status tidb0
wait_sql tidb0
wait_checkpoint_cleanup tidb1

{
	cat <<'SQL'
USE ntdml_system;
SET tidb_nontransactional_dml_execution_mode = 'dxf';
SET tidb_nontransactional_dml_concurrency = 2;
SET sql_mode = 'STRICT_TRANS_TABLES';
DROP TABLE IF EXISTS t_failed;
CREATE TABLE t_failed(
  id BIGINT PRIMARY KEY CLUSTERED,
  v INT NOT NULL
);
INSERT INTO t_failed VALUES (1, 1), (2, 2), (3, 3), (4, 4);
BATCH ON id LIMIT 2 UPDATE t_failed SET v = NULL WHERE id >= 1;
SQL
} > "$WORK_DIR/failed_checkpoint.sql"

if sql < "$WORK_DIR/failed_checkpoint.sql" > "$WORK_DIR/failed_checkpoint.out" 2> "$WORK_DIR/failed_checkpoint.err"; then
	echo "expected failed DXF job to preserve diagnostic checkpoint rows" >&2
	exit 1
fi

failed_checkpoints="$(scalar "SELECT COUNT(*) FROM mysql.tidb_nontransactional_dml_checkpoint WHERE status='failed' AND error_text IS NOT NULL")"
if [[ "$failed_checkpoints" == "0" ]]; then
	echo "failed DXF job did not retain diagnostic checkpoint rows" >&2
	cat "$WORK_DIR/failed_checkpoint.out" >&2 || true
	cat "$WORK_DIR/failed_checkpoint.err" >&2 || true
	exit 1
fi
sql -e "DELETE FROM mysql.tidb_nontransactional_dml_checkpoint WHERE status='failed'" >/dev/null

http_get http://tidb0:10080/metrics \
	| grep -E 'tidb_session_non_transactional_dml_(task|chunk|rows)_total' >/dev/null
wait_metrics_query_nonempty prometheus 'count(up{job="tidb"} == 1) == 2' "Prometheus TiDB scrape targets after restart"
wait_metrics_query_nonempty grafana 'count(up{job="tidb"} == 1) == 2' "Grafana Prometheus datasource query after restart"
wait_ntdml_metrics_captured prometheus
wait_ntdml_metrics_captured grafana

echo "parallel non-transactional DML DXF system test passed"
