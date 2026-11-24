#!/bin/sh
# Self-check of scripts/kpi-plan-spike-summary.sh on fixed input: no network, no PostgreSQL.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
data=$(mktemp)
trap 'rm -f "$data" "$data.na"' EXIT

cat >"$data" <<'EOF'
0|E0|1000|1000|0.030|0.010|10.00|5.00|9000|0.100|0.100|NA|NA
0|E1|1000|1000|0.030|0.010|10.00|5.00|9000|0.120|0.120|NA|NA
0|P0|1000|1000|0.030|0.010|10.00|5.00|9000|0.080|0.080|NA|NA
0|P1|1000|1000|0.030|0.010|10.00|5.00|9000|0.095|0.095|NA|NA
0|P2|1000|6|0.030|0.010|10.00|5.00|9000|0.090|0.090|994|6
1|P2|1000|6|0.030|0.010|10.00|5.00|9000|0.095|0.095|994|6
1|P1|1000|1000|0.030|0.010|10.00|5.00|9000|0.100|0.100|NA|NA
1|P0|1000|1000|0.030|0.010|10.00|5.00|9000|0.090|0.090|NA|NA
1|E1|1000|1000|0.030|0.010|10.00|5.00|9000|0.125|0.125|NA|NA
1|E0|1000|1000|0.030|0.010|10.00|5.00|9000|0.110|0.110|NA|NA
2|E0|1000|1000|0.030|0.010|10.00|5.00|9000|0.105|0.105|NA|NA
2|E1|1000|1000|0.030|0.010|10.00|5.00|9000|0.130|0.130|NA|NA
2|P0|1000|1000|0.030|0.010|10.00|5.00|9000|0.085|0.085|NA|NA
2|P1|1000|1000|0.030|0.010|10.00|5.00|9000|0.105|0.105|NA|NA
2|P2|1000|6|0.030|0.010|10.00|5.00|9000|0.100|0.100|994|6
0|S2c|1000|1000|0.040|0.020|5.00|8.00|9000|0.110|0.108|0|1000
1|S2c|1000|1000|0.040|0.022|5.00|8.00|9000|0.110|0.110|0|1000
2|S2c|1000|1000|0.040|0.021|5.00|8.00|9000|0.110|0.109|0|1000
0|S2a|1000|6|0.001|0.010|5.00|5.00|9000|0.110|0.108|994|6
1|S2a|1000|6|0.001|0.012|5.00|5.00|9000|0.110|0.110|994|6
2|S2a|1000|6|0.001|0.011|5.00|5.00|9000|0.110|0.109|994|6
0|S2g|100|1|0.000|0.500|5.00|400.00|1000|1.000|1.000|99|0
1|S2g|100|1|0.000|0.520|5.00|400.00|1000|1.000|1.000|99|0
2|S2g|100|1|0.000|0.510|5.00|400.00|1000|1.000|1.000|99|0
EOF

failures=0

expect() {
    if [ "$2" != "$3" ]; then
        echo "kpi-plan-spike-summary-test: $1: expected [$3] got [$2]" >&2
        failures=$((failures + 1))
    fi
}

out=$(sh "$here/kpi-plan-spike-summary.sh" "$data")
row() {
    printf '%s\n' "$out" | awk -v variant="$1" -v cols="$2" '
        $1 == variant && NF == 12 && $2 ~ /^[0-9]+$/ {
            n = split(cols, c, ",")
            line = ""
            for (i = 1; i <= n; i++) line = line (i > 1 ? " " : "") $(c[i])
            print line
        }'
}
ratio() {
    printf '%s\n' "$out" | awk -v name="$1" '$1 == name { print $2 }'
}
paired() {
    printf '%s\n' "$out" | awk -v name="$1" '$1 == "paired" && $2 == name { print $4, $6, $8 }'
}

expect E0 "$(row E0 2,3,4)" '3 0.105000 9.5'
expect E1 "$(row E1 2,3,4)" '3 0.125000 8.0'
expect P0 "$(row P0 2,3,4)" '3 0.085000 11.8'
expect P1 "$(row P1 2,3,4)" '3 0.100000 10.0'
expect P2 "$(row P2 2,3,4,11,12)" '3 0.095000 10.5 6 994'
expect E1/E0 "$(ratio E1/E0)" '1.190'
expect P1/P0 "$(ratio P1/P0)" '1.176'
expect P2/P0 "$(ratio P2/P0)" '1.118'
expect P1/E0 "$(ratio P1/E0)" '0.952'
expect P2/E0 "$(ratio P2/E0)" '0.905'
expect P0/E0 "$(ratio P0/E0)" '0.810'
expect P1/E1 "$(ratio P1/E1)" '0.800'
expect paired-P1/E0 "$(paired P1/E0)" '0.950 0.909 1.000'
expect verdict "$(printf '%s\n' "$out" | awk '$1 == "verdict" { print $3, $5, $7, $9 }')" 'yes 9.8 10.0 9.5'
expect S2c "$(row S2c 2,3,6,8,9,10,11,12)" '3 0.021000 8.00 1000 1000 0 1.000 1.000'
expect S2a "$(row S2a 2,3,6,9,10,11,12)" '3 0.011000 5.00 6 994 0.524 0.625'
expect S2g "$(row S2g 2,3,6,8,9,10,11,12)" '3 0.510000 400.00 100 1 99 24.286 50.000'
expect S1c "$(row S1c 2,3,11)" '0 NA NA'

grep -v '|P1|' "$data" >"$data.na"
out=$(sh "$here/kpi-plan-spike-summary.sh" "$data.na")
expect P1-missing "$(row P1 2,3)" '0 NA'
expect P1/E0-missing "$(ratio P1/E0)" 'NA'
expect verdict-missing "$(printf '%s\n' "$out" | awk '$1 == "verdict" { print $3, $5 }')" 'NA NA'

if [ "$failures" -ne 0 ]; then
    exit 1
fi
echo 'kpi-plan-spike-summary-test: ok'
