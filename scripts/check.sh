#!/usr/bin/env bash
# Quality gate: every task must pass this before hand-off.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace "$@"
