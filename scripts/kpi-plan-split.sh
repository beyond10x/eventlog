#!/bin/sh
# CI-only measurement: the PostgreSQL projection find split into planning and execution time, base
# against head, from pg_stat_statements. Never run on a workstation: .github/workflows/kpi.yml runs it
# by workflow_dispatch with `split=true`.
#
# Usage:
#   kpi-plan-split.sh start              start the PostgreSQL container with pg_stat_statements
#   kpi-plan-split.sh build REV LABEL    build the KPI runner against REV into its own target directory
#   kpi-plan-split.sh rounds N           N interleaved rounds, one line per round and side in
#                                        $SPLIT_DIR/rounds.tsv
#   kpi-plan-split.sh stop               remove the container
set -eu

SPLIT_DIR=${SPLIT_DIR:-${RUNNER_TEMP:?RUNNER_TEMP or SPLIT_DIR must name a scratch directory}/split}
CONTAINER=eventlog-split-pg
IMAGE='postgres:17.6-alpine3.22@sha256:ef257d85f76e48da1c64832459b59fcaba1a4dac97bf5d7450c77753542eee94'

# The statement of the projection find on both revisions: it starts with `SELECT body`, reads the
# projection registry in its NOT EXISTS and ends `ORDER BY row_key LIMIT $3`. The in-transaction
# find of the projection store has no registry read and does not match. `_` stands for the `$`.
MATCH="query LIKE 'SELECT body%' AND query LIKE '%_projection_registry%' AND query LIKE '%NOT EXISTS%' AND query LIKE '%ORDER BY row_key LIMIT _3%'"

psql() {
    docker exec "$CONTAINER" psql -U postgres -d postgres -At -F'|' -v ON_ERROR_STOP=1 "$@"
}

stamp() {
    printf '%s load %s\n' "$1" "$(cut -d' ' -f1-3 /proc/loadavg)"
}

start() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    docker run -d --name "$CONTAINER" --cpus 2 --memory 1g \
        -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:5432:5432 "$IMAGE" \
        -c shared_preload_libraries=pg_stat_statements \
        -c pg_stat_statements.track=all \
        -c pg_stat_statements.track_planning=on \
        -c pg_stat_statements.max=10000 \
        -c track_activity_query_size=8192
    tries=0
    until docker exec "$CONTAINER" pg_isready -U postgres -h 127.0.0.1 >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [ "$tries" -gt 60 ]; then
            docker logs "$CONTAINER" || true
            echo 'kpi-plan-split: PostgreSQL did not become ready' >&2
            exit 1
        fi
        sleep 1
    done
    psql -c 'CREATE EXTENSION pg_stat_statements'
    psql -c 'SELECT version()'
    psql -c 'SHOW shared_preload_libraries'
    psql -c 'SHOW pg_stat_statements.track'
    psql -c 'SHOW pg_stat_statements.track_planning'
}

build() {
    rev=$1
    label=$2
    sha=$(git rev-parse "$rev^{commit}")
    source_dir=$SPLIT_DIR/src-$label
    target_dir=$SPLIT_DIR/target-$label
    rm -rf "$source_dir" "$target_dir"
    mkdir -p "$source_dir" "$SPLIT_DIR/bin"
    git archive --format=tar "$sha" | tar -x -m -C "$source_dir"
    rm -rf "$source_dir/crates/eventlog-kpi"
    cp -R crates/eventlog-kpi "$source_dir/crates/eventlog-kpi"
    grep -v '/kpi-counters' "$source_dir/crates/eventlog-kpi/Cargo.toml" >"$source_dir/runner-manifest"
    mv "$source_dir/runner-manifest" "$source_dir/crates/eventlog-kpi/Cargo.toml"
    if ! grep -q '"crates/eventlog-kpi"' "$source_dir/Cargo.toml"; then
        sed -i '0,/members = \[/s//members = [\n    "crates\/eventlog-kpi",/' "$source_dir/Cargo.toml"
    fi
    echo "kpi-plan-split: building $label $sha"
    rustc --version --verbose
    (cd "$source_dir" && CARGO_TARGET_DIR=$target_dir cargo build --release --locked -p eventlog-kpi)
    cp "$target_dir/release/eventlog-kpi" "$SPLIT_DIR/bin/eventlog-kpi-$label"
    rm -rf "$source_dir" "$target_dir"
}

stats() {
    psql -c "SELECT count(*),
        coalesce(sum(calls), 0),
        coalesce(sum(plans), 0),
        round(coalesce(sum(total_plan_time), 0)::numeric, 6),
        round(coalesce(sum(total_exec_time), 0)::numeric, 6),
        round(coalesce(sum(total_plan_time) / nullif(sum(plans), 0), 0)::numeric, 6),
        round(coalesce(sum(total_exec_time) / nullif(sum(calls), 0), 0)::numeric, 6),
        coalesce(sum(shared_blks_hit), 0)
        FROM pg_stat_statements WHERE $MATCH"
}

show_matched() {
    echo "kpi-plan-split: statements matched on $1, round $2"
    psql -c "SELECT '--- calls=' || calls || ' plans=' || plans || ' mean_plan_ms=' || round(mean_plan_time::numeric, 6)
        || ' mean_exec_ms=' || round(mean_exec_time::numeric, 6) || E'\n' || query
        FROM pg_stat_statements WHERE $MATCH ORDER BY calls DESC"
    echo '--- end'
}

measure() {
    round=$1
    side=$2
    binary=$SPLIT_DIR/bin/eventlog-kpi-$side
    psql -c 'SELECT pg_stat_statements_reset()' >/dev/null
    if ! "$binary" --work "$SPLIT_DIR/work" timings --json >"$SPLIT_DIR/timings-$side.json" 2>"$SPLIT_DIR/timings-$side.err"; then
        cat "$SPLIT_DIR/timings-$side.err" >&2
        echo "kpi-plan-split: $side timings failed in round $round" >&2
        exit 1
    fi
    found=$(psql -c "SELECT count(*) FROM pg_stat_statements WHERE $MATCH")
    if [ "$found" -eq 0 ]; then
        echo "kpi-plan-split: no statement matched on $side in round $round" >&2
        psql -c "SELECT calls, left(query, 300) FROM pg_stat_statements ORDER BY calls DESC LIMIT 20" >&2
        exit 1
    fi
    if [ "$round" -eq 0 ]; then
        show_matched "$side" "$round"
    fi
    p50=$(jq -r '."postgres.projection_find.latency".p50 // "NA"' "$SPLIT_DIR/timings-$side.json")
    line="$round|$side|$(stats)|$p50"
    echo "$line" >>"$SPLIT_DIR/rounds.tsv"
    echo "round $round $side: $line"
}

rounds() {
    count=$1
    mkdir -p "$SPLIT_DIR/work"
    : >"$SPLIT_DIR/rounds.tsv"
    export EVENTLOG_TEST_POSTGRES_URL=postgresql://postgres@127.0.0.1:5432/postgres
    echo "rustc commit-hash: $(rustc --version --verbose | sed -n 's/^commit-hash: //p')"
    echo "nproc: $(nproc)"
    stamp 'before the rounds'
    echo 'columns: round|side|statements|calls|plans|total_plan_ms|total_exec_ms|mean_plan_ms|mean_exec_ms|shared_blks_hit|runner_find_p50_us'
    round=0
    while [ "$round" -lt "$count" ]; do
        if [ $((round % 2)) -eq 0 ]; then
            order='base head'
        else
            order='head base'
        fi
        for side in $order; do
            measure "$round" "$side"
        done
        round=$((round + 1))
    done
    stamp 'after the rounds'
}

stop() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}

case ${1:-} in
    start) start ;;
    build) build "${2:?revision}" "${3:?label}" ;;
    rounds) rounds "${2:?rounds}" ;;
    stop) stop ;;
    *)
        echo 'usage: kpi-plan-split.sh start | build REV LABEL | rounds N | stop' >&2
        exit 2
        ;;
esac
