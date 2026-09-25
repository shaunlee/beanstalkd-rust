#!/usr/bin/env bash
# Benchmark matrix: runs bstk-bench against the reference beanstalkd and
# beanstalkd-rs for every (scenario, conns, body size, pipeline) cell,
# RUNS times each, alternating servers, with a fresh server process per
# run. Appends one CSV row per run to $OUT_CSV; summarize with
# bench/summarize.py.
#
# Environment overrides (defaults in brackets):
#   REF_BIN     reference binary      [.ref/beanstalkd/beanstalkd]
#   RS_BIN      beanstalkd-rs binary  [$CARGO_TARGET_DIR/release/beanstalkd-rs]
#   BENCH_BIN   bstk-bench binary     [$CARGO_TARGET_DIR/release/bstk-bench]
#   SCENARIOS   ["put-reserve-delete producers-consumers put-only"]
#   CONNS       ["1 10 100"]  (producers-consumers uses 2 instead of 1)
#   BODIES      ["16 4096"]
#   PIPELINES   ["1"]
#   RUNS        [3]
#   DURATION    seconds per run [5]
#   OUT_CSV     [bench-results.csv]
#   BENCH_ARGS  extra bstk-bench arguments (e.g. "--threads 4")
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
REF_BIN="${REF_BIN:-$ROOT/.ref/beanstalkd/beanstalkd}"
RS_BIN="${RS_BIN:-$TARGET/release/beanstalkd-rs}"
BENCH_BIN="${BENCH_BIN:-$TARGET/release/bstk-bench}"
SCENARIOS="${SCENARIOS:-put-reserve-delete producers-consumers put-only}"
CONNS="${CONNS:-1 10 100}"
BODIES="${BODIES:-16 4096}"
PIPELINES="${PIPELINES:-1}"
RUNS="${RUNS:-3}"
DURATION="${DURATION:-5}"
OUT_CSV="${OUT_CSV:-bench-results.csv}"
BENCH_ARGS="${BENCH_ARGS:-}"

for b in "$REF_BIN" "$RS_BIN" "$BENCH_BIN"; do
  [ -x "$b" ] || { echo "run-matrix: missing binary $b" >&2; exit 1; }
done

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

wait_port() {
  local i
  for i in $(seq 1 100); do
    python3 -c 'import socket,sys; socket.create_connection(("127.0.0.1", int(sys.argv[1])), 0.2).close()' "$1" 2>/dev/null && return 0
    sleep 0.05
  done
  echo "run-matrix: server on port $1 did not come up" >&2
  return 1
}

SERVER_PID=""
trap '[ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true' EXIT

[ -s "$OUT_CSV" ] || echo "server,scenario,conns,body_size,pipeline,run,ops_per_sec,server_cpu_pct,put_p50_us,put_p99_us,put_p999_us,reserve_p50_us,reserve_p99_us,reserve_p999_us,delete_p50_us,delete_p99_us,delete_p999_us,load_avg" >"$OUT_CSV"

# Extracts a numeric field from the bench's JSON line ("" if absent).
field() {
  python3 -c 'import json,sys; d=json.loads(sys.argv[1]); v=d.get(sys.argv[2]); print("" if v is None else v)' "$1" "$2"
}

failures=0
for scenario in $SCENARIOS; do
  for conns in $CONNS; do
    if [ "$scenario" = producers-consumers ] && [ "$conns" -lt 2 ]; then conns=2; fi
    for body in $BODIES; do
      for pipeline in $PIPELINES; do
        for run in $(seq 1 "$RUNS"); do
          for server in ref rs; do
            if [ "$server" = ref ]; then bin="$REF_BIN"; else bin="$RS_BIN"; fi
            port="$(free_port)"
            "$bin" -l 127.0.0.1 -p "$port" >/dev/null 2>&1 &
            SERVER_PID=$!
            wait_port "$port"
            load="$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || echo "")"
            # shellcheck disable=SC2086
            if out="$("$BENCH_BIN" --addr "127.0.0.1:$port" --conns "$conns" --duration "$DURATION" \
                --scenario "$scenario" --body-size "$body" --pipeline "$pipeline" --json $BENCH_ARGS 2>&1)"; then
              json="$(printf '%s\n' "$out" | sed -n 's/^JSON //p')"
              row="$server,$scenario,$conns,$body,$pipeline,$run"
              for f in ops_per_sec server_cpu_pct put_p50_us put_p99_us put_p999_us \
                       reserve_p50_us reserve_p99_us reserve_p999_us delete_p50_us delete_p99_us delete_p999_us; do
                row="$row,$(field "$json" "$f")"
              done
              echo "$row,$load" >>"$OUT_CSV"
              printf '%-4s %-20s conns=%-3s body=%-5s pipe=%-3s run=%s  %s ops/s  cpu=%s%%\n' \
                "$server" "$scenario" "$conns" "$body" "$pipeline" "$run" \
                "$(field "$json" ops_per_sec)" "$(field "$json" server_cpu_pct)"
            else
              echo "FAILED: $server $scenario conns=$conns body=$body pipeline=$pipeline run=$run" >&2
              printf '%s\n' "$out" >&2
              failures=$((failures + 1))
            fi
            kill "$SERVER_PID" 2>/dev/null || true
            wait "$SERVER_PID" 2>/dev/null || true
            SERVER_PID=""
          done
        done
      done
    done
  done
done
echo "results: $OUT_CSV (failures: $failures)"
[ "$failures" -eq 0 ]
