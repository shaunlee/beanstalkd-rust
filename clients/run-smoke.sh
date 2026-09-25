#!/usr/bin/env bash
# Real-client smoke tests: runs each client (Python greenstalk, Go
# go-beanstalk) against a fresh reference beanstalkd and a fresh
# beanstalkd-rs, normalizes both transcripts (clients/normalize.py) and
# diffs them. Exits non-zero on any client assertion failure, hang
# (watchdog), or transcript difference.
#
# Environment overrides:
#   BSTK_REF_BIN   reference binary   (default: .ref/beanstalkd/beanstalkd)
#   BSTK_RS_BIN    beanstalkd-rs      (default: built via cargo --release)
#   SMOKE_VENV     Python venv dir    (default: clients/python/.venv)
#   SMOKE_CLIENTS  clients to run     (default: "python go")
#   SMOKE_TIMEOUT  per-client watchdog in seconds (default: 120)
#   SMOKE_OUT      where transcripts are kept (default: a temp dir)
#   SMOKE_BINLOG   1: run both servers with -b <fresh dir under SMOKE_OUT>
#                  and mask the binlog layout fields (docs/COMPAT.md D8)
#   SMOKE_RESTART  1 (implies SMOKE_BINLOG=1): the client leaves jobs in
#                  known states and holds two reservations, both servers
#                  are killed with SIGKILL and restarted on the same binlog
#                  dir, and the client dumps the recovered state
#   SMOKE_SERVER_ARGS  extra server arguments for both servers, e.g. "-f0"
#                  or "-F" (split on whitespace)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLIENTS_DIR="$ROOT/clients"
REF_BIN="${BSTK_REF_BIN:-$ROOT/.ref/beanstalkd/beanstalkd}"
SMOKE_VENV="${SMOKE_VENV:-$CLIENTS_DIR/python/.venv}"
SMOKE_CLIENTS="${SMOKE_CLIENTS:-python go}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-120}"
OUT="${SMOKE_OUT:-$(mktemp -d "${TMPDIR:-/tmp}/bstk-smoke.XXXXXX")}"
mkdir -p "$OUT"
SMOKE_RESTART="${SMOKE_RESTART:-0}"
SMOKE_BINLOG="${SMOKE_BINLOG:-0}"
[ "$SMOKE_RESTART" = 1 ] && SMOKE_BINLOG=1
read -r -a SERVER_ARGS <<<"${SMOKE_SERVER_ARGS:-}"
NORMALIZE_ARGS=()
[ "$SMOKE_BINLOG" = 1 ] && NORMALIZE_ARGS=(--binlog)

die() { echo "run-smoke: $*" >&2; exit 1; }

if [ ! -x "$REF_BIN" ]; then
  die "reference binary not found at $REF_BIN (run scripts/build-ref.sh or set BSTK_REF_BIN)"
fi
if [ -z "${BSTK_RS_BIN:-}" ]; then
  echo "run-smoke: building beanstalkd-rs (release)..." >&2
  (cd "$ROOT" && cargo build --release -q -p bstk-server)
  RS_BIN="${CARGO_TARGET_DIR:-$ROOT/target}/release/beanstalkd-rs"
else
  RS_BIN="$BSTK_RS_BIN"
fi
[ -x "$RS_BIN" ] || die "beanstalkd-rs binary not found at $RS_BIN"

# Runs "$@" with a wall-clock limit (macOS has no timeout(1)).
with_timeout() {
  local secs="$1"; shift
  perl -e 'alarm shift @ARGV; exec @ARGV or die "exec: $!"' "$secs" "$@"
}

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

wait_port() {
  local port="$1" i
  for i in $(seq 1 100); do
    if python3 -c 'import socket,sys; socket.create_connection(("127.0.0.1", int(sys.argv[1])), 0.2).close()' "$port" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  die "server on port $port did not come up"
}

PIDS=()
cleanup() {
  local p
  for p in "${PIDS[@]:-}"; do
    [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
  done
  # Only remove what this script created.
  rm -rf "$OUT/binlog" "$OUT/hold.fifo"
}
trap cleanup EXIT

# start_server NAME BIN [BINLOG_DIR] -> sets SERVER_PORT, SERVER_PID.
# Always a fresh port: a restarted server must not depend on SO_REUSEADDR.
start_server() {
  local name="$1" bin="$2" dir="${3:-}" args=()
  [ -n "$dir" ] && args=(-b "$dir")
  SERVER_PORT="$(free_port)"
  "$bin" -l 127.0.0.1 -p "$SERVER_PORT" ${args[@]:+"${args[@]}"} \
    ${SERVER_ARGS[@]:+"${SERVER_ARGS[@]}"} >>"$OUT/$name.server.log" 2>&1 &
  SERVER_PID=$!
  PIDS+=("$SERVER_PID")
  wait_port "$SERVER_PORT"
}

stop_server() {
  kill "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}

# SIGKILL for both servers: the reference has no SIGTERM handler anyway.
crash_server() {
  kill -9 "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}

# run_client NAME RAW [ARGS...]: runs the client under the watchdog,
# appending to RAW; returns its status.
run_client() {
  local name="$1" raw="$2"; shift 2
  with_timeout "$SMOKE_TIMEOUT" "${CLIENT_CMD[@]}" "$@" "127.0.0.1:$SERVER_PORT" \
    >>"$raw" 2>>"$OUT/$name.err"
}

# run_restart NAME BIN DIR RAW: phase 1 (full flow + leftover jobs, client
# holds its reservations), SIGKILL the server, restart it on the same binlog
# dir, phase 2 (dump the recovered state). Returns non-zero on any failure.
run_restart() {
  local name="$1" bin="$2" dir="$3" raw="$4" fifo="$OUT/hold.fifo" cpid rc=0 i
  rm -f "$fifo"; mkfifo "$fifo"
  with_timeout "$SMOKE_TIMEOUT" "${CLIENT_CMD[@]}" --leave-jobs "127.0.0.1:$SERVER_PORT" \
    <"$fifo" >>"$raw" 2>>"$OUT/$name.err" &
  cpid=$!
  exec 3>"$fifo" # opening the write end unblocks the client's stdin
  for i in $(seq 1 $((SMOKE_TIMEOUT * 10))); do
    grep -q '^HOLDING$' "$raw" && break
    kill -0 "$cpid" 2>/dev/null || break
    sleep 0.1
  done
  grep -q '^HOLDING$' "$raw" || rc=1
  crash_server "$SERVER_PID"
  exec 3>&-
  wait "$cpid" || rc=1
  rm -f "$fifo"
  [ "$rc" = 0 ] || return 1
  echo "--- server killed (SIGKILL) and restarted on the same binlog ---" >>"$raw"
  start_server "$name" "$bin" "$dir"
  run_client "$name" "$raw" --after-restart
}

# Client command lines (the address is appended).
prepare_python() {
  if [ ! -x "$SMOKE_VENV/bin/python" ]; then
    echo "run-smoke: creating venv at $SMOKE_VENV" >&2
    python3 -m venv "$SMOKE_VENV"
  fi
  if ! "$SMOKE_VENV/bin/python" -c 'import greenstalk' 2>/dev/null; then
    "$SMOKE_VENV/bin/pip" install -q greenstalk
  fi
  CLIENT_CMD=("$SMOKE_VENV/bin/python" "$CLIENTS_DIR/python/smoke.py")
}

prepare_go() {
  command -v go >/dev/null || die "go toolchain not found (set SMOKE_CLIENTS=python to skip the Go client)"
  (cd "$CLIENTS_DIR/go" && go build -o "$OUT/smoke-go" .)
  CLIENT_CMD=("$OUT/smoke-go")
}

failed=0
for client in $SMOKE_CLIENTS; do
  case "$client" in
    python) prepare_python ;;
    go) prepare_go ;;
    *) die "unknown client: $client" ;;
  esac

  status=()
  for server in ref rs; do
    if [ "$server" = ref ]; then bin="$REF_BIN"; else bin="$RS_BIN"; fi
    name="$client-$server"
    raw="$OUT/$name.raw.txt"
    : >"$raw"; : >"$OUT/$name.err"; : >"$OUT/$name.server.log"
    dir=""
    if [ "$SMOKE_BINLOG" = 1 ]; then
      dir="$OUT/binlog/$name"
      rm -rf "$dir"; mkdir -p "$dir"
    fi
    # A fresh server (and binlog dir) per (client, server) so counters
    # start from zero.
    start_server "$name" "$bin" "$dir"
    if [ "$SMOKE_RESTART" = 1 ]; then
      run_restart "$name" "$bin" "$dir" "$raw" && rc=0 || rc=1
    else
      run_client "$name" "$raw" && rc=0 || rc=1
    fi
    if [ "$rc" = 0 ]; then
      status+=(0)
    else
      status+=(1)
      echo "FAIL: $client client against $server (see $OUT/$client-$server.err):" >&2
      tail -n 5 "$OUT/$client-$server.err" >&2 || true
      failed=1
    fi
    stop_server "$SERVER_PID"
    [ -n "$dir" ] && rm -rf "$dir"
    python3 "$CLIENTS_DIR/normalize.py" ${NORMALIZE_ARGS[@]:+"${NORMALIZE_ARGS[@]}"} \
      <"$raw" >"$OUT/$name.txt"
  done

  if diff -u "$OUT/$client-ref.txt" "$OUT/$client-rs.txt" >"$OUT/$client.diff"; then
    lines=$(wc -l <"$OUT/$client-rs.txt" | tr -d ' ')
    if [ "${status[0]}" = 0 ] && [ "${status[1]}" = 0 ]; then
      echo "PASS: $client: transcripts identical ($lines lines)"
    fi
  else
    echo "FAIL: $client: transcripts differ (reference vs beanstalkd-rs):" >&2
    cat "$OUT/$client.diff" >&2
    failed=1
  fi
done

mode="default"
[ "$SMOKE_BINLOG" = 1 ] && mode="binlog"
[ "$SMOKE_RESTART" = 1 ] && mode="binlog+restart"
echo "mode: $mode; server args: ${SMOKE_SERVER_ARGS:-(none)}"
echo "transcripts: $OUT"
exit "$failed"
