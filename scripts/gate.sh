#!/usr/bin/env bash
# Compatibility launcher; the Rust gate owns argument parsing and check selection.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
exec cargo run --locked -p eventlog-postgres --example gate -- "$@"
