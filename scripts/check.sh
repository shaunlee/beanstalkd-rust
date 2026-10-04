#!/usr/bin/env bash
# Quality gate: every task must pass this before hand-off.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
# The test-only membership hook (never in a shipped build) and its tests.
cargo clippy -p bstk-server --all-targets --features test-hooks -- -D warnings
cargo build --workspace
cargo test --workspace "$@"
cargo test -p bstk-server --features test-hooks --test cluster "$@" -- membership::
# Leave the shipped (feature-less) binary in target/ for later runs.
cargo build -p bstk-server
