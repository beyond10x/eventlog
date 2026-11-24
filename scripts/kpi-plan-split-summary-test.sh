#!/bin/sh
# Self-check of scripts/kpi-plan-split-summary.sh on fixed input: no network, no PostgreSQL.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
data=$(mktemp)
trap 'rm -f "$data"' EXIT

cat >"$data" <<'EOF'
0|base|1|32|32|1.6|4.0|0.4|1.0|100|100.0
0|head|1|32|32|3.2|8.0|0.8|2.0|120|200.0
1|head|1|32|32|0.8|16.0|0.2|4.0|120|220.0
1|base|1|32|32|0.4|8.0|0.1|2.0|100|110.0
2|base|1|32|32|1.2|12.0|0.3|3.0|100|120.0
2|head|1|32|32|2.4|24.0|0.6|6.0|120|240.0
3|head|1|32|32|1.6|32.0|0.4|8.0|120|260.0
3|base|1|32|32|0.8|16.0|0.2|4.0|100|130.0
EOF

failures=0

expect() {
    if [ "$2" != "$3" ]; then
        echo "kpi-plan-split-summary-test: $1: expected [$3] got [$2]" >&2
        failures=$((failures + 1))
    fi
}

out=$(sh "$here/kpi-plan-split-summary.sh" "$data")
side() {
    printf '%s\n' "$out" | awk -v side="$1" '$1 == side && NF == 10 { print $2, $3, $4, $5, $6, $7, $8, $9, $10 }'
}
expect base "$(side base)" '4 0.200000 150.0 2.000000 150.0 2.200000 32.000000 110.000000 27.3'
expect head "$(side head)" '4 0.400000 150.0 4.000000 150.0 4.400000 32.000000 220.000000 27.3'
expect ratio "$(printf '%s\n' "$out" | grep '^ratio')" 'ratio head/base: plan 2.000  exec 2.000  sum 2.000  runner_find_p50 2.000'

sed 's/|[0-9.]*$/|NA/' "$data" >"$data.na"
out=$(sh "$here/kpi-plan-split-summary.sh" "$data.na")
rm -f "$data.na"
expect ratio-na "$(printf '%s\n' "$out" | grep '^ratio')" 'ratio head/base: plan 2.000  exec 2.000  sum 2.000  runner_find_p50 NA'

if [ "$failures" -ne 0 ]; then
    exit 1
fi
echo 'kpi-plan-split-summary-test: ok'
