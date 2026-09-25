#!/usr/bin/env bash
# Build the reference C beanstalkd (pinned commit) into .ref/ for differential testing.
set -euo pipefail
REF_COMMIT=25085c5f090031fd7110613a77fd6f816681d801
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/.ref/beanstalkd"
if [ ! -d "$DIR/.git" ]; then
  git clone -q https://github.com/beanstalkd/beanstalkd.git "$DIR"
fi
git -C "$DIR" fetch -q --depth 1 origin "$REF_COMMIT" 2>/dev/null || true
git -C "$DIR" checkout -q "$REF_COMMIT"
make -C "$DIR" -j"$(getconf _NPROCESSORS_ONLN)" beanstalkd >/dev/null
echo "$DIR/beanstalkd"
