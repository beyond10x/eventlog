#!/usr/bin/env bash
# The repository gate: locked workspace tests, formatting, and clippy.
# Green here is the bar for main. Mirrors what the monorepo gate ran for
# this component before extraction.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
printf 'gate: cargo test --workspace --locked\n'
cargo test --workspace --locked
printf 'gate: cargo fmt --all --check\n'
cargo fmt --all --check
printf 'gate: cargo clippy --workspace --all-targets --locked -- -D warnings\n'
cargo clippy --workspace --all-targets --locked -- -D warnings
printf 'gate: bash scripts/check-brand.sh\n'
bash scripts/check-brand.sh
printf 'gate: green\n'
