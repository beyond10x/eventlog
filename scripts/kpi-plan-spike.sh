#!/bin/sh
# CI-only spike: does a prepared statement per connection bring the head's indexed projection find
# to the end-to-end latency of 0.10.0? Plain SQL against one PostgreSQL container, measured by
# `pgbench -r` (client latency, a loopback round trip included) and pg_stat_statements. Never run on
# a workstation: .github/workflows/kpi.yml runs it by workflow_dispatch with `spike=true`.
#
# The DDL, the registry row, the statements and the 1,001-row fixture are those of
# scripts/kpi-plan-attribution.sh (0.10.0 `ba`, head `hd`). The skewed fixture `sk` is the head DDL
# with one tenant `big` of 25,000 rows and 63 tenants `t00`..`t62` of 75 rows each (29,725 rows, below
# the 30,000 that one ANALYZE reads in full); the value `rare` is held by 5 rows at the end of the
# key order of `big` and 5 at the end of `t00`, the other rows are `common` (3 of 4) or `mid`.
#
# Usage:
#   kpi-plan-spike.sh start         start the PostgreSQL container with pg_stat_statements
#   kpi-plan-spike.sh setup         create and load the three table sets, ANALYZE, write the statements
#   kpi-plan-spike.sh explain       EXPLAIN of the three skewed finds, custom and generic, and the
#                                   estimated against actual rows of (big, rare)
#   kpi-plan-spike.sh rounds N      N rounds over E0 E1 P0 P1 P2, then SPIKE_SKEW_ROUNDS rounds over
#                                   the nine skewed variants, order rotated per round, one line per
#                                   round and variant in $SPIKE_DIR/rounds.tsv
#   kpi-plan-spike.sh stop          remove the container
#
# Variants (each `pgbench -n -r -c 1 -T 10`, plan_cache_mode set through PGOPTIONS):
#   E0 base DDL and statement, -M extended          E1 head DDL and statement, -M extended
#   P0 base, -M prepared, force_custom_plan         P1 head, -M prepared, force_custom_plan
#   P2 head, -M prepared, auto
#   S1 (t00, rare)  S2 (big, rare)  S3 (big, common), head statement on `sk`, -M prepared, each as
#   c force_custom_plan, a auto, g force_generic_plan: S1c S1a S1g S2c S2a S2g S3c S3a S3g
set -eu

SPIKE_DIR=${SPIKE_DIR:-${RUNNER_TEMP:?RUNNER_TEMP or SPIKE_DIR must name a scratch directory}/spike}
CONTAINER=eventlog-spike-pg
IMAGE='postgres:17.6-alpine3.22@sha256:ef257d85f76e48da1c64832459b59fcaba1a4dac97bf5d7450c77753542eee94'
SECONDS_PER_RUN=${SPIKE_SECONDS:-10}
SKEW_ROUNDS=${SPIKE_SKEW_ROUNDS:-3}
EP_VARIANTS='E0 E1 P0 P1 P2'
SK_VARIANTS='S1c S1a S1g S2c S2a S2g S3c S3a S3g'
PLAN_COLS="'NA', 'NA'"

psql() {
    docker exec "$CONTAINER" psql -h 127.0.0.1 -U postgres -d postgres -At -F'|' -v ON_ERROR_STOP=1 "$@"
}

psql_show() {
    docker exec "$CONTAINER" psql -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}

psql_file() {
    docker exec -i "$CONTAINER" psql -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 <"$1"
}

stamp() {
    printf '%s load %s\n' "$1" "$(cut -d' ' -f1-3 /proc/loadavg)"
}

start() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    docker run -d --name "$CONTAINER" --cpus 2 --memory 1g \
        -e POSTGRES_HOST_AUTH_METHOD=trust "$IMAGE" \
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
            echo 'kpi-plan-spike: PostgreSQL did not become ready' >&2
            exit 1
        fi
        sleep 1
    done
    psql -c 'CREATE EXTENSION pg_stat_statements'
    psql -c 'SELECT version()'
    psql -c 'SHOW pg_stat_statements.track_planning'
    psql -c 'SHOW plan_cache_mode'
    psql -c 'SHOW default_statistics_target'
    psql -c "SELECT 'pg_stat_statements plan columns: ' || coalesce(string_agg(column_name, ',' ORDER BY column_name), 'none') FROM information_schema.columns WHERE table_name = 'pg_stat_statements' AND (column_name LIKE '%plan%')"
    docker exec "$CONTAINER" pgbench --version
}

registry_sql() {
    printf '%s' "CREATE TABLE $1_projection_registry(projection_name TEXT PRIMARY KEY,indexed_fields JSONB NOT NULL); INSERT INTO $1_projection_registry(projection_name,indexed_fields) VALUES('kpi_rows','[\"kind\"]'::jsonb);"
}

base_ddl() {
    printf '%s' "CREATE TABLE $1_p_kpi_rows (tenant_id text NOT NULL,row_key text NOT NULL,body jsonb NOT NULL,idx_0 text,PRIMARY KEY(tenant_id,row_key)); CREATE INDEX $1_p_kpi_rows_idx_0 ON $1_p_kpi_rows (tenant_id, idx_0);"
}

head_ddl() {
    table=$1_p_kpi_rows
    printf '%s' "CREATE TABLE $table (tenant_id text NOT NULL,row_key text NOT NULL,body jsonb NOT NULL,idx_0 text,idx_0_digest bigint GENERATED ALWAYS AS (hashtextextended(idx_0, (0)::bigint)) STORED,PRIMARY KEY(tenant_id,row_key));"
    printf '%s' " CREATE INDEX ${table}_key_c ON $table USING btree (tenant_id, row_key COLLATE \"C\");"
    printf '%s' " CREATE INDEX ${table}_idx_0 ON $table USING btree (tenant_id, idx_0_digest);"
}

load_sql() {
    printf '%s' "INSERT INTO $1_p_kpi_rows (tenant_id,row_key,body,idx_0) SELECT 'owner','key-' || lpad(i::text,5,'0'),jsonb_build_object('kind',k,'n',i),k FROM (SELECT i, CASE WHEN i + 10 >= 1001 THEN 'item' ELSE 'other' END AS k FROM generate_series(0,1000) AS i) AS s;"
}

skew_load_sql() {
    printf '%s' "INSERT INTO sk_p_kpi_rows (tenant_id,row_key,body,idx_0) SELECT s.t,'key-' || lpad(s.i::text,5,'0'),jsonb_build_object('kind',s.k,'n',s.i),s.k FROM (SELECT tn.t, g.i, CASE WHEN g.i >= tn.n - 5 AND tn.t IN ('big','t00') THEN 'rare' WHEN g.i % 4 = 0 THEN 'mid' ELSE 'common' END AS k FROM (SELECT 'big'::text AS t, 25000 AS n UNION ALL SELECT 't' || lpad(x::text,2,'0'), 75 FROM generate_series(0,62) AS x) AS tn CROSS JOIN LATERAL generate_series(0, tn.n - 1) AS g(i)) AS s;"
}

setup() {
    psql -c "$(base_ddl ba) $(registry_sql ba)"
    psql -c "$(head_ddl hd) $(registry_sql hd)"
    psql -c "$(head_ddl sk) $(registry_sql sk)"
    psql -c "$(load_sql ba)"
    psql -c "$(load_sql hd)"
    psql -c "$(skew_load_sql)"
    psql -c 'ANALYZE ba_p_kpi_rows; ANALYZE ba_projection_registry; ANALYZE hd_p_kpi_rows; ANALYZE hd_projection_registry; ANALYZE sk_p_kpi_rows; ANALYZE sk_projection_registry'
    for set in ba hd sk; do
        echo "--- ${set}_p_kpi_rows"
        psql_show -c "\\d ${set}_p_kpi_rows"
    done
    psql -c "SELECT 'sk rows ' || count(*) || ' tenants ' || count(DISTINCT tenant_id) FROM sk_p_kpi_rows"
    psql -c "SELECT 'sk value ' || idx_0 || ' rows ' || count(*) || ' in big ' || count(*) FILTER (WHERE tenant_id = 'big') || ' in t00 ' || count(*) FILTER (WHERE tenant_id = 't00') FROM sk_p_kpi_rows GROUP BY idx_0 ORDER BY idx_0"
    psql -c "SELECT 'sk stats ' || attname || ' n_distinct ' || n_distinct || ' mcv ' || coalesce(most_common_vals::text, '') || ' freqs ' || coalesce(most_common_freqs::text, '') FROM pg_stats WHERE tablename = 'sk_p_kpi_rows' AND attname IN ('tenant_id', 'idx_0_digest')"
    psql -c "SELECT relname, relpages, reltuples FROM pg_class WHERE relname LIKE '%_p_kpi_rows%' ORDER BY relname"
    prepare_sql_files
}

guard() {
    printf '%s' "NOT EXISTS (SELECT FROM $1_projection_registry WHERE projection_name = :name AND indexed_fields <> :fields)"
}

prepare_sql_files() {
    rm -rf "$SPIKE_DIR/sql"
    mkdir -p "$SPIKE_DIR/sql"
    printf '%s\n' "SELECT body FROM ba_p_kpi_rows WHERE tenant_id = :tenant AND idx_0 = :value AND $(guard ba) ORDER BY row_key LIMIT :lim;" >"$SPIKE_DIR/sql/base.sql"
    printf '%s\n' "SELECT body, idx_0 FROM hd_p_kpi_rows WHERE tenant_id = :tenant AND idx_0_digest = hashtextextended(:value::text, (0)::bigint) AND $(guard hd) ORDER BY row_key LIMIT :lim;" >"$SPIKE_DIR/sql/head.sql"
    printf '%s\n' "SELECT body, idx_0 FROM sk_p_kpi_rows WHERE tenant_id = :tenant AND idx_0_digest = hashtextextended(:value::text, (0)::bigint) AND $(guard sk) ORDER BY row_key LIMIT :lim;" >"$SPIKE_DIR/sql/skew.sql"
    docker exec "$CONTAINER" rm -rf /tmp/spike-sql
    docker cp "$SPIKE_DIR/sql" "$CONTAINER:/tmp/spike-sql"
    for name in base head skew; do
        echo "--- $name.sql"
        cat "$SPIKE_DIR/sql/$name.sql"
    done
}

# Sets V_FILE (statement file), V_TBL (table set), V_PROTO (pgbench -M), V_MODE (plan_cache_mode),
# V_TENANT and V_VALUE (the parameters) for one variant.
variant_def() {
    V_TENANT=owner
    V_VALUE=item
    case $1 in
        E0) V_FILE=base V_TBL=ba V_PROTO=extended V_MODE=auto ;;
        E1) V_FILE=head V_TBL=hd V_PROTO=extended V_MODE=auto ;;
        P0) V_FILE=base V_TBL=ba V_PROTO=prepared V_MODE=force_custom_plan ;;
        P1) V_FILE=head V_TBL=hd V_PROTO=prepared V_MODE=force_custom_plan ;;
        P2) V_FILE=head V_TBL=hd V_PROTO=prepared V_MODE=auto ;;
        S[123][cag])
            V_FILE=skew
            V_TBL=sk
            V_PROTO=prepared
            case $1 in
                S1*) V_TENANT=t00 V_VALUE=rare ;;
                S2*) V_TENANT=big V_VALUE=rare ;;
                S3*) V_TENANT=big V_VALUE=common ;;
            esac
            case $1 in
                *c) V_MODE=force_custom_plan ;;
                *a) V_MODE=auto ;;
                *g) V_MODE=force_generic_plan ;;
            esac
            ;;
        *) echo "kpi-plan-spike: unknown variant $1" >&2; exit 2 ;;
    esac
}

explain_call() {
    printf "EXECUTE sk('%s','%s',10,'kpi_rows','[\"kind\"]')" "$1" "$2"
}

explain() {
    file=$SPIKE_DIR/explain-skew.sql
    {
        printf '%s\n' "PREPARE sk(text,text,bigint,text,jsonb) AS SELECT body, idx_0 FROM sk_p_kpi_rows WHERE tenant_id = \$1 AND idx_0_digest = hashtextextended(\$2::text, (0)::bigint) AND NOT EXISTS (SELECT FROM sk_projection_registry WHERE projection_name = \$4 AND indexed_fields <> \$5) ORDER BY row_key LIMIT \$3;"
        for mode in force_custom_plan force_generic_plan; do
            printf '%s\n' "SET plan_cache_mode = $mode;"
            printf '%s\n' "\\echo '=== EXPLAIN, $mode'"
            for pair in 't00:rare' 'big:rare' 'big:common'; do
                tenant=${pair%%:*}
                value=${pair##*:}
                printf '%s\n' "\\echo '--- ($tenant, $value) $mode'"
                printf '%s\n' "EXPLAIN $(explain_call "$tenant" "$value");"
            done
        done
        for mode in force_custom_plan force_generic_plan; do
            printf '%s\n' "SET plan_cache_mode = $mode;"
            printf '%s\n' "\\echo '=== EXPLAIN (ANALYZE, BUFFERS) of (big, rare), $mode: estimated against actual rows'"
            printf '%s\n' "EXPLAIN (ANALYZE, BUFFERS, SUMMARY) $(explain_call big rare);"
            printf '%s\n' "\\echo '=== EXPLAIN (ANALYZE, BUFFERS) of (big, common), $mode'"
            printf '%s\n' "EXPLAIN (ANALYZE, BUFFERS, SUMMARY) $(explain_call big common);"
        done
    } >"$file"
    cat "$file"
    echo '--- output'
    psql_file "$file"
}

plan_cols() {
    cols=$(psql -c "SELECT coalesce(string_agg(column_name, ',' ORDER BY column_name), '') FROM information_schema.columns WHERE table_name = 'pg_stat_statements' AND column_name IN ('generic_plan_calls', 'custom_plan_calls', 'generic_plans', 'custom_plans')")
    case $cols in
        *custom_plan_calls*) PLAN_COLS='generic_plan_calls, custom_plan_calls' ;;
        *custom_plans*) PLAN_COLS='generic_plans, custom_plans' ;;
        *) PLAN_COLS="'NA', 'NA'" ;;
    esac
    echo "kpi-plan-spike: pg_stat_statements plan counters: $cols -> $PLAN_COLS"
}

wait_backends() {
    tries=0
    while [ "$(psql -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND application_name = 'pgbench'")" != 0 ]; do
        tries=$((tries + 1))
        if [ "$tries" -gt 50 ]; then
            echo 'kpi-plan-spike: a pgbench backend is still connected' >&2
            break
        fi
        sleep 0.2
    done
}

measure() {
    round=$1
    variant=$2
    variant_def "$variant"
    out=$SPIKE_DIR/pgbench-$variant.out
    if [ "$round" -eq 0 ]; then
        echo "kpi-plan-spike: $variant uses $(docker exec -e "PGOPTIONS=-c plan_cache_mode=$V_MODE" "$CONTAINER" psql -h 127.0.0.1 -U postgres -d postgres -At -c 'SHOW plan_cache_mode') with -M $V_PROTO on $V_TBL ($V_TENANT, $V_VALUE)"
    fi
    psql -c 'SELECT pg_stat_statements_reset()' >/dev/null
    docker exec -e "PGOPTIONS=-c plan_cache_mode=$V_MODE" "$CONTAINER" \
        pgbench -n -r -M "$V_PROTO" -c 1 -T "$SECONDS_PER_RUN" \
        -D "tenant=$V_TENANT" -D "value=$V_VALUE" -D lim=10 -D name=kpi_rows -D 'fields=["kind"]' \
        -f "/tmp/spike-sql/$V_FILE.sql" -h 127.0.0.1 -U postgres postgres >"$out" 2>&1 || {
        cat "$out" >&2
        echo "kpi-plan-spike: pgbench failed for $variant in round $round" >&2
        exit 1
    }
    wait_backends
    stats=$(psql -c "SELECT calls, plans, round(mean_plan_time::numeric, 6), round(mean_exec_time::numeric, 6), round(rows::numeric / calls, 2), round(shared_blks_hit::numeric / calls, 2), $PLAN_COLS
        FROM pg_stat_statements
        WHERE query LIKE '%${V_TBL}_p_kpi_rows%' AND query LIKE '%projection_registry%' AND query NOT LIKE '%pg_stat_statements%'
        ORDER BY calls DESC LIMIT 1")
    if [ -z "$stats" ]; then
        echo "kpi-plan-spike: no statement matched for $variant in round $round" >&2
        psql -c "SELECT calls, left(query, 300) FROM pg_stat_statements ORDER BY calls DESC LIMIT 10" >&2
        exit 1
    fi
    if [ "$round" -eq 0 ]; then
        echo "kpi-plan-spike: pgbench output of $variant"
        cat "$out"
        echo "kpi-plan-spike: statement of $variant as pg_stat_statements has it"
        psql -c "SELECT query FROM pg_stat_statements WHERE query LIKE '%${V_TBL}_p_kpi_rows%' AND query LIKE '%projection_registry%' AND query NOT LIKE '%pg_stat_statements%' ORDER BY calls DESC LIMIT 1"
    fi
    base=$(echo "$stats" | cut -d'|' -f1-6)
    generic=$(echo "$stats" | cut -d'|' -f7-8)
    tps=$(awk '/^tps = / { print $3; exit }' "$out")
    lat=$(awk '/^latency average = / { print $4; exit }' "$out")
    stmt=$(awk '/^statement latencies/ { f = 1; next } f && $1 ~ /^[0-9.]+$/ { print $1; exit }' "$out")
    echo "$round|$variant|$base|${tps:-NA}|${lat:-NA}|${stmt:-NA}|$generic" >>"$SPIKE_DIR/rounds.tsv"
    echo "round $round $variant: $base tps=${tps:-NA} latency_avg=${lat:-NA} stmt_latency=${stmt:-NA} plan_counters=$generic"
}

rotate() {
    echo "$1" | awk -v k="$2" '{ for (i = 0; i < NF; i++) printf "%s ", $(((k + i) % NF + 1)) }'
}

rounds() {
    count=$1
    mkdir -p "$SPIKE_DIR"
    : >"$SPIKE_DIR/rounds.tsv"
    plan_cols
    echo "nproc: $(nproc)"
    stamp 'before the rounds'
    echo 'columns: round|variant|calls|plans|mean_plan_ms|mean_exec_ms|rows_per_call|shared_blks_hit_per_call|tps|latency_average_ms|stmt_latency_ms|generic_plan_calls|custom_plan_calls'
    round=0
    while [ "$round" -lt "$count" ]; do
        for variant in $(rotate "$EP_VARIANTS" "$round"); do
            measure "$round" "$variant"
        done
        round=$((round + 1))
    done
    stamp 'after the find rounds'
    round=0
    while [ "$round" -lt "$SKEW_ROUNDS" ]; do
        for variant in $(rotate "$SK_VARIANTS" "$round"); do
            measure "$round" "$variant"
        done
        round=$((round + 1))
    done
    stamp 'after the skewed rounds'
}

stop() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}

mkdir -p "$SPIKE_DIR"
case ${1:-} in
    start) start ;;
    setup) setup ;;
    explain) explain ;;
    rounds) rounds "${2:?rounds}" ;;
    stop) stop ;;
    *)
        echo 'usage: kpi-plan-spike.sh start | setup | explain | rounds N | stop' >&2
        exit 2
        ;;
esac
