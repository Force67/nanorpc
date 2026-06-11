#!/usr/bin/env bash
# Runs every test suite: Rust workspace plus the Python interop suite.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> cargo test"
cargo test --quiet

echo "==> cargo clippy"
cargo clippy --quiet --all-targets

echo "==> cargo fmt --check"
cargo fmt --check

echo "==> runtimes/python/run_tests.sh"
bash runtimes/python/run_tests.sh

echo "all suites passed"
