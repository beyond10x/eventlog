#!/bin/sh
# Summary of scripts/kpi-plan-attribution.sh: one line per round and variant in, one table out.
#
# Usage: kpi-plan-attribution-summary.sh ROUNDS_FILE
#
# Input columns, `|` separated: round variant calls plans mean_plan_ms mean_exec_ms rows_per_call.
# Medians are nearest-rank, as `percentile` in crates/eventlog-kpi/src/timing.rs: the value at index
# ceil(50 * n / 100) of the sorted values, taken over the per-round means. The spread is
# (max - min) / median in percent.
set -eu

file=${1:?rounds file}

# column VARIANT N: the numeric values of column N for one variant.
column() {
    awk -F'|' -v variant="$1" -v n="$2" '$2 == variant && $n ~ /^[0-9.eE+-]+$/ { print $n }' "$file"
}

# stat VARIANT N: "median spread" of column N for one variant.
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

# diff_us A B: (A - B) milliseconds as microseconds.
diff_us() {
    awk -v a="$1" -v b="$2" 'BEGIN { if (a == "NA" || b == "NA") print "NA"; else printf "%.1f", (a - b) * 1000 }'
}

echo 'variants'
echo '  V0 base DDL (index (tenant_id, idx_0)), base statement'
echo '  V1 head DDL (key_c, digest column and index), head statement'
echo '  V2 head DDL without the key_c index, head statement'
echo '  V3 head DDL, the digest compared to a pre-computed bigint parameter'
echo '  V4 head DDL without the key_c index, the V3 statement'
echo '  V5 head DDL, head statement without ORDER BY row_key LIMIT'
echo
echo 'per round and variant (sorted by round, variant)'
printf '%5s %-4s %7s %7s %13s %13s %9s\n' round var calls plans mean_plan_ms mean_exec_ms rows/call
sort -t'|' -k1,1n -k2,2 "$file" | awk -F'|' '
    { printf "%5s %-4s %7s %7s %13s %13s %9s\n", $1, $2, $3, $4, $5, $6, $7 }'

echo
echo 'median over rounds of the per-round means, milliseconds per statement, spread = (max - min) / median'
printf '%-4s %6s %12s %10s %12s %10s %12s %9s %9s %11s\n' \
    var rounds plan_ms plan_spr% exec_ms exec_spr% sum_ms plan_xV0 exec_xV0 plan_dV0_us
for variant in V0 V1 V2 V3 V4 V5; do
    rounds=$(awk -F'|' -v variant="$variant" '$2 == variant' "$file" | wc -l | tr -d ' ')
    set -- $(stat "$variant" 5)
    plan=$1
    plan_spread=$2
    set -- $(stat "$variant" 6)
    execms=$1
    exec_spread=$2
    total=$(sum "$plan" "$execms")
    eval "${variant}_plan=$plan ${variant}_exec=$execms"
    if [ "$variant" = V0 ]; then
        v0_plan=$plan
        v0_exec=$execms
    fi
    printf '%-4s %6s %12s %10s %12s %10s %12s %9s %9s %11s\n' \
        "$variant" "$rounds" "$plan" "$plan_spread" "$execms" "$exec_spread" "$total" \
        "$(ratio "$plan" "$v0_plan")" "$(ratio "$execms" "$v0_exec")" "$(diff_us "$plan" "$v0_plan")"
done

echo
echo 'planning-time differences in microseconds (medians)'
printf 'head_vs_base V1-V0 %s\n' "$(diff_us "$V1_plan" "$V0_plan")"
printf 'key_c_with_function V1-V2 %s\n' "$(diff_us "$V1_plan" "$V2_plan")"
printf 'key_c_with_parameter V3-V4 %s\n' "$(diff_us "$V3_plan" "$V4_plan")"
printf 'function_with_key_c V1-V3 %s\n' "$(diff_us "$V1_plan" "$V3_plan")"
printf 'function_without_key_c V2-V4 %s\n' "$(diff_us "$V2_plan" "$V4_plan")"
printf 'order_by_limit V1-V5 %s\n' "$(diff_us "$V1_plan" "$V5_plan")"
printf 'residual V4-V0 %s\n' "$(diff_us "$V4_plan" "$V0_plan")"
