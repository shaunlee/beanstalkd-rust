#!/usr/bin/env bash
# Benchmark matrix: runs bstk-bench against the reference beanstalkd and
# beanstalkd-rs for every (scenario, conns, body size, pipeline) cell,
# RUNS times each, alternating servers, with a fresh server process per
# run. Appends one CSV row per run to $OUT_CSV; summarize with
# bench/summarize.py.
#
# Environment overrides (defaults in brackets):
#   REF_BIN     reference binary      [.ref/beanstalkd-opt/beanstalkd, the -O2
#               build from `scripts/build-ref.sh --optimized`]
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
#   IDLE_CONNS     ["0"]  values for --idle-conns (scaling scenario)
#   DELAYED_TUBES  ["0"]  --delayed-tubes, paired with each IDLE_CONNS value
#                  by position (e.g. IDLE_CONNS="0 10000" DELAYED_TUBES="0 10000")
#   SERVER_MODES   ["none"]  server persistence modes, applied to both servers:
#                  none     no binlog
#                  F        -b <fresh dir> -F      (never fsync)
#                  default  -b <fresh dir>         (fsync at most every 50 ms)
#                  f0       -b <fresh dir> -f0     (fsync every write)
#                  Every run gets a fresh, empty binlog directory under
#                  BINLOG_ROOT, removed after the run.
#   BINLOG_ROOT    parent of the per-run binlog directories [a mktemp -d dir]
#   SERVER_ARGS    extra arguments for both servers (e.g. "-s 1048576")
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
REF_BIN="${REF_BIN:-$ROOT/.ref/beanstalkd-opt/beanstalkd}"
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
IDLE_CONNS="${IDLE_CONNS:-0}"
DELAYED_TUBES="${DELAYED_TUBES:-0}"
SERVER_MODES="${SERVER_MODES:-none}"
SERVER_ARGS="${SERVER_ARGS:-}"
for m in $SERVER_MODES; do
  case "$m" in none|F|default|f0) ;; *) echo "run-matrix: unknown server mode $m" >&2; exit 1 ;; esac
done
BINLOG_ROOT="${BINLOG_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/bstk-bench-binlog.XXXXXX")}"
mkdir -p "$BINLOG_ROOT"
read -r -a idle_list <<<"$IDLE_CONNS"
read -r -a delayed_list <<<"$DELAYED_TUBES"
[ "${#idle_list[@]}" -eq "${#delayed_list[@]}" ] || {
  echo "run-matrix: IDLE_CONNS and DELAYED_TUBES need the same number of values" >&2; exit 1; }

for b in "$REF_BIN" "$RS_BIN" "$BENCH_BIN"; do
  [ -x "$b" ] || { echo "run-matrix: missing binary $b (reference: scripts/build-ref.sh --optimized; ours: cargo build --release -p bstk-server -p bstk-bench)" >&2; exit 1; }
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
BINLOG_DIR=""
cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  [ -n "$BINLOG_DIR" ] && rm -rf "$BINLOG_DIR" || true
}
trap cleanup EXIT

# Server arguments for a mode; creates a fresh binlog directory (BINLOG_DIR).
mode_args() {
  MODE_ARGS=()
  BINLOG_DIR=""
  [ "$1" = none ] && return 0
  BINLOG_DIR="$(mktemp -d "$BINLOG_ROOT/run.XXXXXX")"
  MODE_ARGS=(-b "$BINLOG_DIR")
  case "$1" in
    F) MODE_ARGS+=(-F) ;;
    f0) MODE_ARGS+=(-f0) ;;
  esac
}

[ -s "$OUT_CSV" ] || echo "server,scenario,conns,body_size,pipeline,run,ops_per_sec,server_cpu_pct,put_p50_us,put_p99_us,put_p999_us,reserve_p50_us,reserve_p99_us,reserve_p999_us,delete_p50_us,delete_p99_us,delete_p999_us,load_avg,idle_conns,delayed_tubes,server_mode" >"$OUT_CSV"

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
       for li in "${!idle_list[@]}"; do
        idle="${idle_list[$li]}"
        delayed="${delayed_list[$li]}"
        for run in $(seq 1 "$RUNS"); do
         for mode in $SERVER_MODES; do
          for server in ref rs; do
            if [ "$server" = ref ]; then bin="$REF_BIN"; else bin="$RS_BIN"; fi
            port="$(free_port)"
            mode_args "$mode"
            # shellcheck disable=SC2086
            "$bin" -l 127.0.0.1 -p "$port" ${MODE_ARGS[@]+"${MODE_ARGS[@]}"} $SERVER_ARGS >/dev/null 2>&1 &
            SERVER_PID=$!
            wait_port "$port"
            load="$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || echo "")"
            # shellcheck disable=SC2086
            if out="$("$BENCH_BIN" --addr "127.0.0.1:$port" --conns "$conns" --duration "$DURATION" \
                --scenario "$scenario" --body-size "$body" --pipeline "$pipeline" --idle-conns "$idle" --delayed-tubes "$delayed" \
                --json $BENCH_ARGS 2>&1)"; then
              json="$(printf '%s\n' "$out" | sed -n 's/^JSON //p')"
              row="$server,$scenario,$conns,$body,$pipeline,$run"
              for f in ops_per_sec server_cpu_pct put_p50_us put_p99_us put_p999_us \
                       reserve_p50_us reserve_p99_us reserve_p999_us delete_p50_us delete_p99_us delete_p999_us; do
                row="$row,$(field "$json" "$f")"
              done
              echo "$row,$load,$idle,$delayed,$mode" >>"$OUT_CSV"
              printf '%-4s %-7s %-20s conns=%-3s body=%-5s pipe=%-3s idle=%-5s delayed=%-5s run=%s  %s ops/s  cpu=%s%%\n' \
                "$server" "$mode" "$scenario" "$conns" "$body" "$pipeline" "$idle" "$delayed" "$run" \
                "$(field "$json" ops_per_sec)" "$(field "$json" server_cpu_pct)"
            else
              echo "FAILED: $server mode=$mode $scenario conns=$conns body=$body pipeline=$pipeline idle=$idle delayed=$delayed run=$run" >&2
              printf '%s\n' "$out" >&2
              failures=$((failures + 1))
            fi
            kill "$SERVER_PID" 2>/dev/null || true
            wait "$SERVER_PID" 2>/dev/null || true
            SERVER_PID=""
            [ -n "$BINLOG_DIR" ] && rm -rf "$BINLOG_DIR"
            BINLOG_DIR=""
          done
         done
        done
       done
      done
    done
  done
done
rmdir "$BINLOG_ROOT" 2>/dev/null || true
echo "results: $OUT_CSV (failures: $failures)"
[ "$failures" -eq 0 ]
