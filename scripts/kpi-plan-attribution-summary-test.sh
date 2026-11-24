#!/bin/sh
# Self-check of scripts/kpi-plan-attribution-summary.sh on fixed input: no network, no PostgreSQL.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
data=$(mktemp)
trap 'rm -f "$data" "$data.na"' EXIT

cat >"$data" <<'EOF'
0|V0|100|100|0.075|0.045|10.00
0|V1|100|100|0.095|0.055|10.00
0|V2|100|100|0.085|0.035|10.00
0|V3|100|100|0.091|0.045|10.00
0|V4|100|100|0.079|0.055|10.00
0|V5|100|100|0.065|0.075|10.00
1|V5|100|100|0.075|0.085|10.00
1|V4|100|100|0.084|0.050|10.00
1|V3|100|100|0.096|0.050|10.00
1|V2|100|100|0.090|0.040|10.00
1|V1|100|100|0.100|0.050|10.00
1|V0|100|100|0.080|0.050|10.00
2|V0|100|100|0.085|0.055|10.00
2|V1|100|100|0.105|0.045|10.00
2|V2|100|100|0.095|0.045|10.00
2|V3|100|100|0.101|0.055|10.00
2|V4|100|100|0.089|0.045|10.00
2|V5|100|100|0.070|0.080|10.00
EOF

failures=0

expect() {
    if [ "$2" != "$3" ]; then
        echo "kpi-plan-attribution-summary-test: $1: expected [$3] got [$2]" >&2
        failures=$((failures + 1))
    fi
}

out=$(sh "$here/kpi-plan-attribution-summary.sh" "$data")
row() {
    printf '%s\n' "$out" | awk -v variant="$1" '$1 == variant && NF == 10 && $2 ~ /^[0-9]+$/ { print $2, $3, $4, $5, $6, $7, $8, $9, $10 }'
}
expect V0 "$(row V0)" '3 0.080000 12.5 0.050000 20.0 0.130000 1.000 1.000 0.0'
expect V1 "$(row V1)" '3 0.100000 10.0 0.050000 20.0 0.150000 1.250 1.000 20.0'
expect V2 "$(row V2)" '3 0.090000 11.1 0.040000 25.0 0.130000 1.125 0.800 10.0'
expect V3 "$(row V3)" '3 0.096000 10.4 0.050000 20.0 0.146000 1.200 1.000 16.0'
expect V4 "$(row V4)" '3 0.084000 11.9 0.050000 20.0 0.134000 1.050 1.000 4.0'
expect V5 "$(row V5)" '3 0.070000 14.3 0.080000 12.5 0.150000 0.875 1.600 -10.0'
delta() {
    printf '%s\n' "$out" | awk -v name="$1" '$1 == name { print $3 }'
}
expect head_vs_base "$(delta head_vs_base)" '20.0'
expect key_c_with_function "$(delta key_c_with_function)" '10.0'
expect key_c_with_parameter "$(delta key_c_with_parameter)" '12.0'
expect function_with_key_c "$(delta function_with_key_c)" '4.0'
expect function_without_key_c "$(delta function_without_key_c)" '6.0'
expect order_by_limit "$(delta order_by_limit)" '30.0'
expect residual "$(delta residual)" '4.0'

grep -v '|V5|' "$data" >"$data.na"
out=$(sh "$here/kpi-plan-attribution-summary.sh" "$data.na")
expect V5-missing "$(row V5)" '0 NA NA NA NA NA NA NA NA'
expect order_by_limit-missing "$(delta order_by_limit)" 'NA'

if [ "$failures" -ne 0 ]; then
    exit 1
fi
echo 'kpi-plan-attribution-summary-test: ok'
