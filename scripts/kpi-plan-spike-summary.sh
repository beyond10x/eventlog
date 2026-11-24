#!/bin/sh
# Summary of scripts/kpi-plan-spike.sh: one line per round and variant in, tables and a verdict out.
#
# Usage: kpi-plan-spike-summary.sh ROUNDS_FILE
#
# Input columns, `|` separated: 1 round, 2 variant, 3 calls, 4 plans, 5 mean_plan_ms, 6 mean_exec_ms,
# 7 rows_per_call, 8 shared_blks_hit_per_call, 9 tps, 10 latency_average_ms, 11 stmt_latency_ms (the
# `pgbench -r` line of the one statement), 12 generic_plan_calls, 13 custom_plan_calls.
# Medians are nearest-rank, as `percentile` in crates/eventlog-kpi/src/timing.rs: the value at index
# ceil(50 * n / 100) of the sorted values, taken over the per-round values. The spread is
# (max - min) / median in percent.
set -eu

file=${1:?rounds file}

column() {
    awk -F'|' -v variant="$1" -v n="$2" '$2 == variant && $n ~ /^[0-9.eE+-]+$/ { print $n }' "$file"
}

# stat VARIANT N [FORMAT]: "median spread" of column N for one variant; the median in FORMAT.
stat() {
    column "$1" "$2" | sort -g | awk -v f="${3:-%.6f}" '
        { v[NR] = $1 }
        END {
            if (NR == 0) { print "NA NA"; exit }
            i = int((50 * NR + 99) / 100)
            if (i < 1) i = 1
            if (i > NR) i = NR
            m = v[i]
            s = (m > 0) ? (v[NR] - v[1]) / m * 100 : 0
            fmt = f " %.1f\n"
            printf fmt, m, s
        }'
}

count() {
    awk -F'|' -v variant="$1" '$2 == variant' "$file" | wc -l | tr -d ' '
}

ratio() {
    awk -v a="$1" -v b="$2" 'BEGIN { if (a == "NA" || b == "NA" || b + 0 == 0) print "NA"; else printf "%.3f", a / b }'
}

# paired A B: "median min max" over the rounds of the per-round ratio of the statement latency of A
# to that of B.
paired() {
    awk -F'|' -v a="$1" -v b="$2" '
        $2 == a && $11 ~ /^[0-9.]+$/ { x[$1] = $11 }
        $2 == b && $11 ~ /^[0-9.]+$/ { y[$1] = $11 }
        END { for (r in x) if ((r in y) && y[r] > 0) printf "%.6f\n", x[r] / y[r] }' "$file" |
        sort -g | awk '
            { v[NR] = $1 }
            END {
                if (NR == 0) { print "NA NA NA"; exit }
                i = int((50 * NR + 99) / 100)
                if (i < 1) i = 1
                if (i > NR) i = NR
                printf "%.3f %.3f %.3f\n", v[i], v[1], v[NR]
            }'
}

show_ratio() {
    eval "a=\${$2_lat} b=\${$3_lat}"
    printf '%-6s %8s  %s\n' "$1" "$(ratio "$a" "$b")" "$4"
}

show_paired() {
    set -- "$1" $(paired "$2" "$3")
    printf 'paired %s median %s min %s max %s\n' "$1" "$2" "$3" "$4"
}

echo 'variants (pgbench -n -r -c 1, plan_cache_mode through PGOPTIONS)'
echo '  E0 base DDL, base statement, -M extended'
echo '  E1 head DDL, head statement, -M extended'
echo '  P0 base DDL, base statement, -M prepared, force_custom_plan'
echo '  P1 head DDL, head statement, -M prepared, force_custom_plan'
echo '  P2 head DDL, head statement, -M prepared, auto'
echo '  S1 (t00, rare), S2 (big, rare), S3 (big, common) on the skewed head table, -M prepared:'
echo '     c force_custom_plan, a auto, g force_generic_plan'
echo
echo 'per round and variant (sorted by variant, round): stmt latency ms, latency average ms, plan ms, exec ms, shared_blks_hit per call, calls, plans'
printf '%-5s %5s %11s %11s %9s %9s %9s %8s %7s\n' var round stmt_lat_ms lat_avg_ms plan_ms exec_ms hit/call calls plans
sort -t'|' -k2,2 -k1,1n "$file" | awk -F'|' '
    { printf "%-5s %5s %11s %11s %9s %9s %9s %8s %7s\n", $2, $1, $11, $10, $5, $6, $8, $3, $4 }'

echo
echo 'median over rounds of the per-round values; latency = pgbench -r statement latency in ms (client side, a loopback round trip included); plan and exec = pg_stat_statements means; spread = (max - min) / median in percent'
printf '%-4s %6s %10s %8s %10s %9s %9s %8s %9s %8s %7s %8s\n' \
    var rounds latency_ms lat_spr% lat_avg_ms plan_ms exec_ms exec_spr% tps calls plans generic
for v in E0 E1 P0 P1 P2; do
    n=$(count "$v")
    set -- $(stat "$v" 11)
    lat=$1
    lat_spr=$2
    set -- $(stat "$v" 10)
    lat_avg=$1
    set -- $(stat "$v" 5)
    plan=$1
    set -- $(stat "$v" 6)
    execms=$1
    exec_spr=$2
    set -- $(stat "$v" 9 '%.1f')
    tps=$1
    set -- $(stat "$v" 3 '%.0f')
    calls=$1
    set -- $(stat "$v" 4 '%.0f')
    plans=$1
    set -- $(stat "$v" 12 '%.0f')
    generic=$1
    eval "${v}_lat=$lat ${v}_spr=$lat_spr"
    printf '%-4s %6s %10s %8s %10s %9s %9s %8s %9s %8s %7s %8s\n' \
        "$v" "$n" "$lat" "$lat_spr" "$lat_avg" "$plan" "$execms" "$exec_spr" "$tps" "$calls" "$plans" "$generic"
done

echo
echo 'ratios of the median statement latency'
show_ratio E1/E0 E1 E0 'head over base, both unprepared (-M extended)'
show_ratio P1/P0 P1 P0 'head over base, both prepared with force_custom_plan'
show_ratio P2/P0 P2 P0 'head prepared with auto over base prepared with force_custom_plan'
show_ratio P1/E0 P1 E0 'head prepared with force_custom_plan over base UNprepared (0.10.0 does not prepare)'
show_ratio P2/E0 P2 E0 'head prepared with auto over base unprepared'
show_ratio P0/E0 P0 E0 'what preparing saves on the base'
show_ratio P1/E1 P1 E1 'what preparing saves on the head'
echo
echo 'the same ratios per round, then the median, min and max over rounds'
show_paired P1/E0 P1 E0
show_paired E1/E0 E1 E0
show_paired P1/P0 P1 P0
show_paired P2/P0 P2 P0

verdict=$(awk -v a="$P1_lat" -v b="$E0_lat" 'BEGIN {
    if (a == "NA" || b == "NA" || b + 0 == 0) {
        print "NA NA"
    } else {
        r = a / b
        printf "%s %.1f\n", (r <= 1.05) ? "yes" : "no", (1.05 - r) * 100
    }
}')
set -- $verdict
echo
printf 'verdict P1_le_1.05xE0 %s margin_points %s spread_P1_pct %s spread_E0_pct %s\n' "$1" "$2" "$P1_spr" "$E0_spr"

echo
echo 'skewed fixture: median over rounds; exec_xcust and hit_xcust = the variant against the force_custom_plan variant of the same statement'
printf '%-5s %6s %10s %9s %9s %10s %10s %8s %7s %8s %10s %9s\n' \
    var rounds exec_ms exec_spr% plan_ms hit/call lat_ms calls plans generic exec_xcust hit_xcust
for scen in S1 S2 S3; do
    case $scen in
        S1) echo "$scen: find by (small tenant t00, rare), LIMIT 10" ;;
        S2) echo "$scen: find by (large tenant big, rare), LIMIT 10" ;;
        S3) echo "$scen: find by (large tenant big, common), LIMIT 10" ;;
    esac
    set -- $(stat "${scen}c" 6)
    cexec=$1
    set -- $(stat "${scen}c" 8 '%.2f')
    chit=$1
    for m in c a g; do
        v=$scen$m
        n=$(count "$v")
        set -- $(stat "$v" 6)
        execms=$1
        exec_spr=$2
        set -- $(stat "$v" 5)
        plan=$1
        set -- $(stat "$v" 8 '%.2f')
        hit=$1
        set -- $(stat "$v" 11)
        lat=$1
        set -- $(stat "$v" 3 '%.0f')
        calls=$1
        set -- $(stat "$v" 4 '%.0f')
        plans=$1
        set -- $(stat "$v" 12 '%.0f')
        generic=$1
        printf '%-5s %6s %10s %9s %9s %10s %10s %8s %7s %8s %10s %9s\n' \
            "$v" "$n" "$execms" "$exec_spr" "$plan" "$hit" "$lat" "$calls" "$plans" "$generic" \
            "$(ratio "$execms" "$cexec")" "$(ratio "$hit" "$chit")"
    done
done
