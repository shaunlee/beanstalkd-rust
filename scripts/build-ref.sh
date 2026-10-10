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
#
# Two deviations from the pinned source, applied to every tree built:
#   - prot.c conn_timeout: `deadline_at >= nanoseconds()` becomes `>`
#     (docs/COMPAT.md D14: a reference bug that loses a TTR timer on Linux).
#   - -Wno-error=stringop-truncation and -Wno-error=discarded-qualifiers when
#     the compiler knows them (gcc 14 trips on tube.c and gcc 16 on prot.c's
#     memchr under -Werror; clang rejects unknown flags).
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

# -Werror in the probe turns clang's "unknown warning option" into a failure.
EXTRA_CFLAGS=""
for flag in -Wno-error=stringop-truncation -Wno-error=discarded-qualifiers; do
  if echo 'int x;' | "${CC:-cc}" -Werror "$flag" -x c -fsyntax-only - 2>/dev/null; then
    EXTRA_CFLAGS="${EXTRA_CFLAGS:+$EXTRA_CFLAGS }$flag"
  fi
done

patch_ref_source() {
  local file="$1/prot.c"
  if grep -q 'j->r.deadline_at > nanoseconds()' "$file"; then
    return 0
  fi
  if [ "$(grep -c 'j->r.deadline_at >= nanoseconds()' "$file")" != 1 ]; then
    echo "build-ref: conn_timeout pattern not found exactly once in $file; the pinned source changed?" >&2
    exit 1
  fi
  sed 's/j->r\.deadline_at >= nanoseconds()/j->r.deadline_at > nanoseconds()/' "$file" > "$file.patched"
  mv "$file.patched" "$file"
}

if [ ! -d "$DIR/.git" ]; then
  git clone -q https://github.com/beanstalkd/beanstalkd.git "$DIR"
fi
git -C "$DIR" fetch -q --depth 1 origin "$REF_COMMIT" 2>/dev/null || true
git -C "$DIR" checkout -q "$REF_COMMIT"
patch_ref_source "$DIR"
make -C "$DIR" -j"$JOBS" CFLAGS="$EXTRA_CFLAGS" beanstalkd >/dev/null
echo "$DIR/beanstalkd"

if [ "$OPTIMIZED" = 1 ]; then
  # A separate source copy (with .git, so vers.sh reports the same version)
  # keeps the -O2 objects apart from the debug ones.
  mkdir -p "$OPT_DIR"
  rsync -a --delete --exclude '*.o' --exclude /beanstalkd --exclude /vers.c "$DIR/" "$OPT_DIR/"
  patch_ref_source "$OPT_DIR"
  make -C "$OPT_DIR" -j"$JOBS" CFLAGS="-O2 $EXTRA_CFLAGS" beanstalkd >/dev/null
  echo "$OPT_DIR/beanstalkd"
fi
