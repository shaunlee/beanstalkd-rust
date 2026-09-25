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
#   SMOKE_TLS      1: beanstalkd-rs is started with a generated --config
#                  holding one TLS listener (auth none) and the clients
#                  connect to it over TLS, verifying a generated CA; the
#                  reference stays plaintext, and the transcripts must
#                  still be identical
#   SMOKE_MTLS     1 (implies SMOKE_TLS=1): the listener uses auth = "mtls"
#                  and the clients present a client certificate; also
#                  checks that a client without a certificate, and one with
#                  a certificate from an untrusted CA, are rejected
#   SMOKE_TOKEN    1: token authentication checks (clients/python/checks.py
#                  token) against a beanstalkd-rs with an auth = "token"
#                  TLS listener
#   SMOKE_HTTP     1: HTTP endpoint checks (clients/python/checks.py http):
#                  /healthz, /readyz, /metrics and /admin against `stats`
#                  and `stats-tube`, on a beanstalkd-rs with [http] and the
#                  listener of the current mode (plaintext, TLS or mTLS;
#                  with -b in binlog modes)
#   SMOKE_CLIENTS=""   skips the client transcript comparison (e.g. to run
#                  only the SMOKE_TOKEN / SMOKE_HTTP checks)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLIENTS_DIR="$ROOT/clients"
REF_BIN="${BSTK_REF_BIN:-$ROOT/.ref/beanstalkd/beanstalkd}"
SMOKE_VENV="${SMOKE_VENV:-$CLIENTS_DIR/python/.venv}"
SMOKE_CLIENTS="${SMOKE_CLIENTS-python go}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-120}"
OUT="${SMOKE_OUT:-$(mktemp -d "${TMPDIR:-/tmp}/bstk-smoke.XXXXXX")}"
mkdir -p "$OUT"
SMOKE_RESTART="${SMOKE_RESTART:-0}"
SMOKE_BINLOG="${SMOKE_BINLOG:-0}"
[ "$SMOKE_RESTART" = 1 ] && SMOKE_BINLOG=1
SMOKE_MTLS="${SMOKE_MTLS:-0}"
SMOKE_TLS="${SMOKE_TLS:-0}"
[ "$SMOKE_MTLS" = 1 ] && SMOKE_TLS=1
SMOKE_TOKEN="${SMOKE_TOKEN:-0}"
SMOKE_HTTP="${SMOKE_HTTP:-0}"
CERTS="$OUT/certs"
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

# wait_port PORT [CA [CERT KEY]]: waits until PORT accepts a connection;
# with CA, the probe completes a TLS handshake (with a client certificate
# if given). The probe is a full connection either way, so that the
# servers' total-connections agree (beanstalkd-rs counts a TLS connection
# once its handshake completes).
wait_port() {
  local port="$1" i
  for i in $(seq 1 100); do
    if python3 - "$@" 2>/dev/null <<'PY'
import socket, ssl, sys
port, *tls = sys.argv[1:]
s = socket.create_connection(("127.0.0.1", int(port)), 0.5)
if tls:
    ctx = ssl.create_default_context(cafile=tls[0])
    if len(tls) == 3:
        ctx.load_cert_chain(tls[1], tls[2])
    s = ctx.wrap_socket(s, server_hostname="127.0.0.1")
    s.unwrap()  # close_notify; the server has seen our Finished before it
s.close()
PY
    then
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
  rm -rf "$OUT/binlog" "$OUT/hold.fifo" "$CERTS" "$OUT"/*.toml
}
trap cleanup EXIT

# The listener's auth mode for beanstalkd-rs in TLS modes.
LISTENER_AUTH=none
[ "$SMOKE_MTLS" = 1 ] && LISTENER_AUTH=mtls
# Token for SMOKE_TOKEN (never printed by the checks).
TOKEN="smoke-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"

# write_config FILE PORT AUTH TLS [HTTP_PORT]: a beanstalkd-rs config with
# one listener on 127.0.0.1:PORT (plus [http] on HTTP_PORT if given).
write_config() {
  local file="$1" port="$2" auth="$3" tls="$4" http="${5:-}"
  {
    printf '[[listener]]\naddr = "127.0.0.1:%s"\ntls = %s\nauth = "%s"\n' "$port" "$tls" "$auth"
    if [ "$tls" = true ]; then
      printf '[tls]\ncert = "%s"\nkey = "%s"\n' "$CERTS/server.pem" "$CERTS/server.key"
      if [ "$auth" = mtls ]; then printf 'client_ca = "%s"\n' "$CERTS/ca.pem"; fi
    fi
    if [ "$auth" = token ]; then printf '[auth]\ntokens = ["%s"]\ntimeout = "1s"\n' "$TOKEN"; fi
    # No snapshot cache: /metrics and /admin must reflect the `stats` the
    # checks ran just before.
    if [ -n "$http" ]; then printf '[http]\naddr = "127.0.0.1:%s"\nsnapshot_min_interval = "0s"\n' "$http"; fi
  } >"$file"
}

# start_server NAME BIN [BINLOG_DIR] -> sets SERVER_PORT, SERVER_PID.
# Always a fresh port: a restarted server must not depend on SO_REUSEADDR.
# beanstalkd-rs runs from a generated --config in TLS modes (and when
# SERVER_AUTH / SERVER_HTTP_PORT are set, for the extra checks).
start_server() {
  local name="$1" bin="$2" dir="${3:-}" args=() tls=false auth="${SERVER_AUTH:-none}" probe=()
  [ -n "$dir" ] && args=(-b "$dir")
  SERVER_PORT="$(free_port)"
  if [ "$bin" = "$RS_BIN" ] && { [ "$SMOKE_TLS" = 1 ] || [ -n "${SERVER_AUTH:-}" ] || [ -n "${SERVER_HTTP_PORT:-}" ]; }; then
    if [ "$SMOKE_TLS" = 1 ]; then
      tls=true
      if [ -z "${SERVER_AUTH:-}" ]; then auth="$LISTENER_AUTH"; fi
    fi
    if [ "$auth" = token ]; then tls=true; fi
    # A token listener is probed with plain TCP (nothing to answer
    # without `auth`).
    case "$auth" in
      none) [ "$tls" = true ] && probe=("$CERTS/ca.pem") ;;
      mtls) probe=("$CERTS/ca.pem" "$CERTS/client.pem" "$CERTS/client.key") ;;
    esac
    write_config "$OUT/$name.toml" "$SERVER_PORT" "$auth" "$tls" "${SERVER_HTTP_PORT:-}"
    "$bin" --config "$OUT/$name.toml" --check-config >/dev/null 2>>"$OUT/$name.server.log" ||
      die "invalid generated config $OUT/$name.toml (see $OUT/$name.server.log)"
    args+=(--config "$OUT/$name.toml")
  else
    args+=(-l 127.0.0.1 -p "$SERVER_PORT")
  fi
  "$bin" ${args[@]:+"${args[@]}"} \
    ${SERVER_ARGS[@]:+"${SERVER_ARGS[@]}"} >>"$OUT/$name.server.log" 2>&1 &
  SERVER_PID=$!
  PIDS+=("$SERVER_PID")
  wait_port "$SERVER_PORT" ${probe[@]:+"${probe[@]}"}
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
  with_timeout "$SMOKE_TIMEOUT" "${CLIENT_ENV[@]}" "${CLIENT_CMD[@]}" "$@" "127.0.0.1:$SERVER_PORT" \
    >>"$raw" 2>>"$OUT/$name.err"
}

# run_restart NAME BIN DIR RAW: phase 1 (full flow + leftover jobs, client
# holds its reservations), SIGKILL the server, restart it on the same binlog
# dir, phase 2 (dump the recovered state). Returns non-zero on any failure.
run_restart() {
  local name="$1" bin="$2" dir="$3" raw="$4" fifo="$OUT/hold.fifo" cpid rc=0 i
  rm -f "$fifo"; mkfifo "$fifo"
  with_timeout "$SMOKE_TIMEOUT" "${CLIENT_ENV[@]}" "${CLIENT_CMD[@]}" --leave-jobs "127.0.0.1:$SERVER_PORT" \
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

if [ "$SMOKE_TLS" = 1 ] || [ "$SMOKE_TOKEN" = 1 ]; then
  "$CLIENTS_DIR/mkcerts.sh" "$CERTS"
fi
# Environment of the client processes: TLS settings only against
# beanstalkd-rs in TLS modes (the reference is always plaintext).
CLEAN_ENV=(env -u SMOKE_TLS_CA -u SMOKE_TLS_CERT -u SMOKE_TLS_KEY)
RS_ENV=("${CLEAN_ENV[@]}")
if [ "$SMOKE_TLS" = 1 ]; then
  RS_ENV+=("SMOKE_TLS_CA=$CERTS/ca.pem")
  [ "$SMOKE_MTLS" = 1 ] && RS_ENV+=("SMOKE_TLS_CERT=$CERTS/client.pem" "SMOKE_TLS_KEY=$CERTS/client.key")
fi

failed=0
for client in $SMOKE_CLIENTS; do
  case "$client" in
    python) prepare_python ;;
    go) prepare_go ;;
    *) die "unknown client: $client" ;;
  esac

  status=()
  for server in ref rs; do
    if [ "$server" = ref ]; then
      bin="$REF_BIN"; CLIENT_ENV=("${CLEAN_ENV[@]}")
    else
      bin="$RS_BIN"; CLIENT_ENV=("${RS_ENV[@]}")
    fi
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

# check NAME CMD...: runs one of the extra checks, reporting PASS/FAIL.
check() {
  local name="$1"; shift
  if with_timeout "$SMOKE_TIMEOUT" "$@" >"$OUT/$name.txt" 2>"$OUT/$name.err"; then
    echo "PASS: $name ($(grep -c '^ok:' "$OUT/$name.txt") checks)"
  else
    echo "FAIL: $name (see $OUT/$name.txt, $OUT/$name.err):" >&2
    tail -n 5 "$OUT/$name.txt" "$OUT/$name.err" >&2 || true
    failed=1
  fi
}

# extra_server NAME: a fresh beanstalkd-rs for one extra check (with a
# fresh binlog dir in binlog modes).
extra_server() {
  local dir=""
  : >"$OUT/$1.server.log"
  if [ "$SMOKE_BINLOG" = 1 ]; then
    dir="$OUT/binlog/$1"; rm -rf "$dir"; mkdir -p "$dir"
  fi
  start_server "$1" "$RS_BIN" "$dir"
}

PY="python3"
[ -x "$SMOKE_VENV/bin/python" ] && PY="$SMOKE_VENV/bin/python"
CHECKS="$CLIENTS_DIR/python/checks.py"

if [ "$SMOKE_MTLS" = 1 ]; then
  extra_server mtls-reject
  check mtls-reject "$PY" "$CHECKS" mtls-reject --ca "$CERTS/ca.pem" \
    --rogue-cert "$CERTS/rogue-client.pem" --rogue-key "$CERTS/rogue-client.key" "127.0.0.1:$SERVER_PORT"
  stop_server "$SERVER_PID"
fi

if [ "$SMOKE_TOKEN" = 1 ]; then
  SERVER_AUTH=token extra_server token
  check token "$PY" "$CHECKS" token --ca "$CERTS/ca.pem" --token "$TOKEN" "127.0.0.1:$SERVER_PORT"
  stop_server "$SERVER_PID"
fi

if [ "$SMOKE_HTTP" = 1 ]; then
  command -v curl >/dev/null || die "curl not found (needed for SMOKE_HTTP=1)"
  http_port="$(free_port)"
  SERVER_HTTP_PORT="$http_port" extra_server http
  wait_port "$http_port"
  tls_args=()
  [ "$SMOKE_TLS" = 1 ] && tls_args=(--ca "$CERTS/ca.pem")
  [ "$SMOKE_MTLS" = 1 ] && tls_args+=(--cert "$CERTS/client.pem" --key "$CERTS/client.key")
  check http "$PY" "$CHECKS" http --http "127.0.0.1:$http_port" \
    ${tls_args[@]:+"${tls_args[@]}"} "127.0.0.1:$SERVER_PORT"
  stop_server "$SERVER_PID"
fi

mode="default"
[ "$SMOKE_BINLOG" = 1 ] && mode="binlog"
[ "$SMOKE_RESTART" = 1 ] && mode="binlog+restart"
[ "$SMOKE_TLS" = 1 ] && mode="$mode+tls"
[ "$SMOKE_MTLS" = 1 ] && mode="$mode+mtls"
[ "$SMOKE_TOKEN" = 1 ] && mode="$mode+token"
[ "$SMOKE_HTTP" = 1 ] && mode="$mode+http"
echo "mode: $mode; server args: ${SMOKE_SERVER_ARGS:-(none)}"
echo "transcripts: $OUT"
exit "$failed"
