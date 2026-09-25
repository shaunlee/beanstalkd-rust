#!/usr/bin/env bash
# Build the reference C beanstalkd (pinned commit) into .ref/ for differential testing.
#
# Usage:
#   scripts/build-ref.sh              debug build (-g, no -O) at .ref/beanstalkd/beanstalkd;
#                                     used by the differential tests and smoke tests.
#   scripts/build-ref.sh --optimized  (or REF_OPT=1) additionally builds an -O2 binary at
#                                     .ref/beanstalkd-opt/beanstalkd, for benchmarks only.
#                                     The debug build above is left untouched.
#
# The reference Makefile appends its own flags with `override CFLAGS+=-Wall
# -Werror -Wformat=2 -g`, so passing CFLAGS=-O2 on the command line keeps
# them (the compile line becomes `-O2 -Wall -Werror -Wformat=2 -g`).
set -euo pipefail
REF_COMMIT=25085c5f090031fd7110613a77fd6f816681d801
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/.ref/beanstalkd"
OPT_DIR="$ROOT/.ref/beanstalkd-opt"
OPTIMIZED="${REF_OPT:-0}"
for arg in "$@"; do
  case "$arg" in
    --optimized) OPTIMIZED=1 ;;
    *) echo "build-ref: unknown argument $arg" >&2; exit 2 ;;
  esac
done
JOBS="$(getconf _NPROCESSORS_ONLN)"

if [ ! -d "$DIR/.git" ]; then
  git clone -q https://github.com/beanstalkd/beanstalkd.git "$DIR"
fi
git -C "$DIR" fetch -q --depth 1 origin "$REF_COMMIT" 2>/dev/null || true
git -C "$DIR" checkout -q "$REF_COMMIT"
make -C "$DIR" -j"$JOBS" beanstalkd >/dev/null
echo "$DIR/beanstalkd"

if [ "$OPTIMIZED" = 1 ]; then
  # A separate source copy (with .git, so vers.sh reports the same version)
  # keeps the -O2 objects apart from the debug ones.
  mkdir -p "$OPT_DIR"
  rsync -a --delete --exclude '*.o' --exclude /beanstalkd --exclude /vers.c "$DIR/" "$OPT_DIR/"
  make -C "$OPT_DIR" -j"$JOBS" CFLAGS=-O2 beanstalkd >/dev/null
  echo "$OPT_DIR/beanstalkd"
fi
