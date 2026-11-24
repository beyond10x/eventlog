#!/bin/sh
# Summary of scripts/kpi-plan-split.sh: one line per round and side in, medians per side out.
#
# Usage: kpi-plan-split-summary.sh ROUNDS_FILE
#
# Input columns, `|` separated: round side statements calls plans total_plan_ms total_exec_ms
# mean_plan_ms mean_exec_ms shared_blks_hit runner_find_p50_us (`NA` when the runner printed none).
# Medians are nearest-rank, as `percentile` in crates/eventlog-kpi/src/timing.rs: the value at index
# ceil(50 * n / 100) of the sorted values. The spread is (max - min) / median in percent.
set -eu

file=${1:?rounds file}

# column SIDE N: the numeric values of column N for one side.
column() {
    awk -F'|' -v side="$1" -v n="$2" '$2 == side && $n ~ /^[0-9.eE+-]+$/ { print $n }' "$file"
}

# stat SIDE N: "median spread" of column N for one side.
stat() {
    column "$1" "$2" | sort -g | awk '
        { v[NR] = $1 }
        END {
            if (NR == 0) { print "NA NA"; exit }
            i = int((50 * NR + 99) / 100)
            if (i < 1) i = 1
            if (i > NR) i = NR
            m = v[i]
            s = (m > 0) ? (v[NR] - v[1]) / m * 100 : 0
            printf "%.6f %.1f\n", m, s
        }'
}

ratio() {
    awk -v a="$1" -v b="$2" 'BEGIN { if (a == "NA" || b == "NA" || b + 0 == 0) print "NA"; else printf "%.3f", a / b }'
}

sum() {
    awk -v a="$1" -v b="$2" 'BEGIN { if (a == "NA" || b == "NA") print "NA"; else printf "%.6f", a + b }'
}

echo 'per round and side (sorted by round, side)'
printf '%5s %-5s %5s %6s %6s %13s %13s %13s %13s %11s %12s\n' \
    round side stmts calls plans total_plan_ms total_exec_ms mean_plan_ms mean_exec_ms blks_hit find_p50_us
sort -t'|' -k1,1n -k2,2 "$file" | awk -F'|' '
    { printf "%5s %-5s %5s %6s %6s %13s %13s %13s %13s %11s %12s\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11 }'

echo
echo 'median over rounds, milliseconds per statement, spread = (max - min) / median'
printf '%-5s %6s %12s %10s %12s %10s %12s %11s %12s %10s\n' \
    side rounds plan_ms plan_spr% exec_ms exec_spr% sum_ms calls/round find_p50_us p50_spr%
for side in base head; do
    rounds=$(awk -F'|' -v side="$side" '$2 == side' "$file" | wc -l | tr -d ' ')
    set -- $(stat "$side" 8)
    plan=$1
    plan_spread=$2
    set -- $(stat "$side" 9)
    execms=$1
    exec_spread=$2
    set -- $(stat "$side" 4)
    calls=$1
    set -- $(stat "$side" 11)
    p50=$1
    p50_spread=$2
    total=$(sum "$plan" "$execms")
    printf '%-5s %6s %12s %10s %12s %10s %12s %11s %12s %10s\n' \
        "$side" "$rounds" "$plan" "$plan_spread" "$execms" "$exec_spread" "$total" "$calls" "$p50" "$p50_spread"
    eval "${side}_plan=$plan ${side}_exec=$execms ${side}_total=$total ${side}_p50=$p50"
done

echo
echo "ratio head/base: plan $(ratio "$head_plan" "$base_plan")  exec $(ratio "$head_exec" "$base_exec")  sum $(ratio "$head_total" "$base_total")  runner_find_p50 $(ratio "$head_p50" "$base_p50")"
