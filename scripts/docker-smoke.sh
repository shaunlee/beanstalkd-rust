#!/usr/bin/env bash
# Smoke test for the container image (Dockerfile): the default command,
# then a binlog on a named volume that must survive a container restart,
# with a graceful stop (exit status 0) in between.
#
# Usage: scripts/docker-smoke.sh IMAGE
# Environment: SMOKE_NAME (prefix for the containers and the volume it
# creates and removes; default bstk-docker-smoke), SMOKE_PORT (host port,
# default 11399).
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: $0 IMAGE" >&2; exit 2; }
IMAGE="$1"
NAME="${SMOKE_NAME:-bstk-docker-smoke}"
PORT="${SMOKE_PORT:-11399}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SMOKE=(python3 "$ROOT/scripts/pkg-smoke.py" --port "$PORT" --wait 20)
VERSION="$(docker run --rm "$IMAGE" --version | awk '{print $2}')"

cleanup() {
  docker rm -f "$NAME-mem" "$NAME-binlog" >/dev/null 2>&1 || true
  docker volume rm "$NAME-data" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

echo "== $IMAGE: version $VERSION, user $(docker run --rm --entrypoint id "$IMAGE" -u)"
[ "$(docker run --rm --entrypoint id "$IMAGE" -u)" != 0 ] || { echo "image runs as root" >&2; exit 1; }

echo "== default command (in memory)"
docker run -d --name "$NAME-mem" -p "127.0.0.1:$PORT:11300" "$IMAGE" >/dev/null
"${SMOKE[@]}" --version "$VERSION"
# The HEALTHCHECK must turn healthy with the default command.
for _ in $(seq 1 30); do
  health="$(docker inspect -f '{{.State.Health.Status}}' "$NAME-mem")"
  [ "$health" = healthy ] && break
  sleep 1
done
echo "health: $health"
[ "$health" = healthy ]
docker stop -t 10 "$NAME-mem" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}}' "$NAME-mem")" = 0 ]
docker rm "$NAME-mem" >/dev/null

echo "== binlog on a volume, across a restart"
docker volume create "$NAME-data" >/dev/null
docker run -d --name "$NAME-binlog" -p "127.0.0.1:$PORT:11300" -v "$NAME-data:/data" \
  "$IMAGE" -l 0.0.0.0 -p 11300 -b /data >/dev/null
"${SMOKE[@]}" --version "$VERSION" --persist-put 5
docker stop -t 10 "$NAME-binlog" >/dev/null
code="$(docker inspect -f '{{.State.ExitCode}}' "$NAME-binlog")"
echo "graceful stop exit status: $code"
[ "$code" = 0 ]
docker start "$NAME-binlog" >/dev/null
"${SMOKE[@]}" --persist-expect 5
"${SMOKE[@]}" --persist-expect 0
docker logs "$NAME-binlog" 2>&1 | tail -n 20
echo "docker-smoke: ok"
