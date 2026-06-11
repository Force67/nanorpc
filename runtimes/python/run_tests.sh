#!/usr/bin/env bash
# Generates Python bindings for the interop schema (using the sibling
# nanobuf checkout), builds the Rust reference server, and runs the
# Python interop suite against it.
set -euo pipefail

cd "$(dirname "$0")/../.."

nanobuf="../nanobuf"
if [ ! -f "$nanobuf/Cargo.toml" ]; then
    echo "error: expected the nanobuf repo at $nanobuf (see README)" >&2
    exit 1
fi

gen_dir="$(mktemp -d)"
trap 'rm -rf "$gen_dir"' EXIT

cargo run -q --manifest-path "$nanobuf/Cargo.toml" -p nanoc -- \
    build interop/schema/echo.nb --lang python --out "$gen_dir"

cargo build -q -p nanorpc-interop --bin interop-server

PYTHONPATH="$PWD/runtimes/python:$nanobuf/runtimes/python:$gen_dir" \
    python3 -m pytest runtimes/python/tests -q
