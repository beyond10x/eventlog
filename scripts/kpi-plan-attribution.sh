#!/bin/sh
# CI-only measurement: what the +10 us of PostgreSQL planning of the indexed projection find costs
# between 0.10.0 and head, in plain SQL against one PostgreSQL container. Never run on a workstation:
# .github/workflows/kpi.yml runs it by workflow_dispatch with `attribute=true`.
#
# The projection table, its indexes, the registry table and its row are the DDL of
# crates/eventlog-postgres/src/schema.rs at 0.10.0 (c0f961dc) and at main 6e72772a
# (`create_projection`, `projection_columns`, `projection_indexes`); the rows are the ones the KPI
# runner seeds for the find (crates/eventlog-kpi/src/fixture.rs: tenant `owner`, 1,001 rows
# `key-NNNNN`, the last ten with kind `item`). Each statement is run by `pgbench -M extended`, one
# unnamed statement per call, planned with its parameters, as the adapter's `query_typed` does.
#
# Usage:
#   kpi-plan-attribution.sh start         start the PostgreSQL container with pg_stat_statements
#   kpi-plan-attribution.sh setup         create and load the three table sets, ANALYZE, print them
#   kpi-plan-attribution.sh explain       EXPLAIN (SETTINGS, SUMMARY) of each variant
#   kpi-plan-attribution.sh rounds N      N rounds over the variants, order rotated per round, one
#                                         line per round and variant in $ATTR_DIR/rounds.tsv
#   kpi-plan-attribution.sh stop          remove the container
#
# Variants: V0 base DDL and statement; V1 head DDL and statement; V2 head DDL without the `key_c`
# index; V3 head DDL, the digest a pre-computed bigint parameter; V4 V3 without `key_c`; V5 V1
# without ORDER BY and LIMIT.
set -eu

ATTR_DIR=${ATTR_DIR:-${RUNNER_TEMP:?RUNNER_TEMP or ATTR_DIR must name a scratch directory}/attribution}
CONTAINER=eventlog-attr-pg
IMAGE='postgres:17.6-alpine3.22@sha256:ef257d85f76e48da1c64832459b59fcaba1a4dac97bf5d7450c77753542eee94'
SECONDS_PER_RUN=${ATTR_SECONDS:-10}
ANALYZE=${ATTR_ANALYZE:-1}
VARIANTS='V0 V1 V2 V3 V4 V5'

psql() {
    docker exec "$CONTAINER" psql -h 127.0.0.1 -U postgres -d postgres -At -F'|' -v ON_ERROR_STOP=1 "$@"
}

psql_show() {
    docker exec "$CONTAINER" psql -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
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
            echo 'kpi-plan-attribution: PostgreSQL did not become ready' >&2
            exit 1
        fi
        sleep 1
    done
    psql -c 'CREATE EXTENSION pg_stat_statements'
    psql -c 'SELECT version()'
    psql -c 'SHOW shared_preload_libraries'
    psql -c 'SHOW pg_stat_statements.track'
    psql -c 'SHOW pg_stat_statements.track_planning'
    psql -c "SELECT 'collate=' || datcollate || ' ctype=' || datctype || ' provider=' || datlocprovider FROM pg_database WHERE datname = 'postgres'"
    psql -c 'SHOW plan_cache_mode'
    docker exec "$CONTAINER" pgbench --version
}

# The registry table of both revisions and its row, as `migrate` creates it and `register_projection`
# fills it.
registry_sql() {
    printf '%s' "CREATE TABLE $1_projection_registry(projection_name TEXT PRIMARY KEY,indexed_fields JSONB NOT NULL); INSERT INTO $1_projection_registry(projection_name,indexed_fields) VALUES('kpi_rows','[\"kind\"]'::jsonb);"
}

# 0.10.0: the value columns only, one index `(tenant_id, idx_0)`.
base_ddl() {
    printf '%s' "CREATE TABLE $1_p_kpi_rows (tenant_id text NOT NULL,row_key text NOT NULL,body jsonb NOT NULL,idx_0 text,PRIMARY KEY(tenant_id,row_key)); CREATE INDEX $1_p_kpi_rows_idx_0 ON $1_p_kpi_rows (tenant_id, idx_0);"
}

# Head: the stored generated digest after the value, the `key_c` page index first, then the digest
# index. The second argument `nokeyc` leaves the `key_c` index out.
head_ddl() {
    table=$1_p_kpi_rows
    printf '%s' "CREATE TABLE $table (tenant_id text NOT NULL,row_key text NOT NULL,body jsonb NOT NULL,idx_0 text,idx_0_digest bigint GENERATED ALWAYS AS (hashtextextended(idx_0, (0)::bigint)) STORED,PRIMARY KEY(tenant_id,row_key));"
    if [ "${2:-}" != nokeyc ]; then
        printf '%s' " CREATE INDEX ${table}_key_c ON $table USING btree (tenant_id, row_key COLLATE \"C\");"
    fi
    printf '%s' " CREATE INDEX ${table}_idx_0 ON $table USING btree (tenant_id, idx_0_digest);"
}

# The seeded rows: 1,001 of tenant `owner`, the last ten of kind `item`, the rest `other`.
load_sql() {
    printf '%s' "INSERT INTO $1_p_kpi_rows (tenant_id,row_key,body,idx_0) SELECT 'owner','key-' || lpad(i::text,5,'0'),jsonb_build_object('kind',k,'n',i),k FROM (SELECT i, CASE WHEN i + 10 >= 1001 THEN 'item' ELSE 'other' END AS k FROM generate_series(0,1000) AS i) AS s;"
}

setup() {
    psql -c "$(base_ddl ba) $(registry_sql ba)"
    psql -c "$(head_ddl hd) $(registry_sql hd)"
    psql -c "$(head_ddl hn nokeyc) $(registry_sql hn)"
    for set in ba hd hn; do
        psql -c "$(load_sql $set)"
        if [ "$ANALYZE" = 1 ]; then
            psql -c "ANALYZE ${set}_p_kpi_rows; ANALYZE ${set}_projection_registry"
        fi
    done
    echo "kpi-plan-attribution: ANALYZE=$ANALYZE"
    for set in ba hd hn; do
        echo "--- ${set}_p_kpi_rows"
        psql_show -c "\\d ${set}_p_kpi_rows"
        psql -c "SELECT count(*), count(*) FILTER (WHERE idx_0 = 'item') FROM ${set}_p_kpi_rows"
    done
    psql -c "SELECT relname, relpages, reltuples FROM pg_class WHERE relname LIKE '%_p_kpi_rows%' ORDER BY relname"
    prepare_sql_files
}

guard() {
    printf '%s' "NOT EXISTS (SELECT FROM $1_projection_registry WHERE projection_name = :name AND indexed_fields <> :fields)"
}

# One file per variant for pgbench: `:name` becomes a parameter, as the adapter's `$n` are.
variant_sql() {
    case $1 in
        V0) printf '%s\n' "SELECT body FROM ba_p_kpi_rows WHERE tenant_id = :tenant AND idx_0 = :value AND $(guard ba) ORDER BY row_key LIMIT :lim;" ;;
        V1) printf '%s\n' "SELECT body, idx_0 FROM hd_p_kpi_rows WHERE tenant_id = :tenant AND idx_0_digest = hashtextextended(:value::text, (0)::bigint) AND $(guard hd) ORDER BY row_key LIMIT :lim;" ;;
        V2) printf '%s\n' "SELECT body, idx_0 FROM hn_p_kpi_rows WHERE tenant_id = :tenant AND idx_0_digest = hashtextextended(:value::text, (0)::bigint) AND $(guard hn) ORDER BY row_key LIMIT :lim;" ;;
        V3) printf '%s\n' "SELECT body, idx_0 FROM hd_p_kpi_rows WHERE tenant_id = :tenant AND idx_0_digest = :digest AND $(guard hd) ORDER BY row_key LIMIT :lim;" ;;
        V4) printf '%s\n' "SELECT body, idx_0 FROM hn_p_kpi_rows WHERE tenant_id = :tenant AND idx_0_digest = :digest AND $(guard hn) ORDER BY row_key LIMIT :lim;" ;;
        V5) printf '%s\n' "SELECT body, idx_0 FROM hd_p_kpi_rows WHERE tenant_id = :tenant AND idx_0_digest = hashtextextended(:value::text, (0)::bigint) AND $(guard hd);" ;;
        *) echo "kpi-plan-attribution: unknown variant $1" >&2; exit 2 ;;
    esac
}

variant_set() {
    case $1 in
        V0) echo ba ;;
        V1 | V3 | V5) echo hd ;;
        V2 | V4) echo hn ;;
    esac
}

prepare_sql_files() {
    rm -rf "$ATTR_DIR/sql"
    mkdir -p "$ATTR_DIR/sql"
    for variant in $VARIANTS; do
        variant_sql "$variant" >"$ATTR_DIR/sql/$variant.sql"
    done
    docker exec "$CONTAINER" rm -rf /tmp/attr-sql
    docker cp "$ATTR_DIR/sql" "$CONTAINER:/tmp/attr-sql"
    for variant in $VARIANTS; do
        echo "--- $variant ($(variant_set "$variant"))"
        cat "$ATTR_DIR/sql/$variant.sql"
    done
}

# The statement of each variant as a prepared statement with the adapter's five typed parameters
# (tenant, value, limit, name, fields), for EXPLAIN.
explain_statement() {
    guard_sql="NOT EXISTS (SELECT FROM $2_projection_registry WHERE projection_name = \$4 AND indexed_fields <> \$5)"
    case $1 in
        V0) printf '%s' "PREPARE v0(text,text,bigint,text,jsonb) AS SELECT body FROM ba_p_kpi_rows WHERE tenant_id = \$1 AND idx_0 = \$2 AND $guard_sql ORDER BY row_key LIMIT \$3;" ;;
        V1) printf '%s' "PREPARE v1(text,text,bigint,text,jsonb) AS SELECT body, idx_0 FROM hd_p_kpi_rows WHERE tenant_id = \$1 AND idx_0_digest = hashtextextended(\$2::text, (0)::bigint) AND $guard_sql ORDER BY row_key LIMIT \$3;" ;;
        V2) printf '%s' "PREPARE v2(text,text,bigint,text,jsonb) AS SELECT body, idx_0 FROM hn_p_kpi_rows WHERE tenant_id = \$1 AND idx_0_digest = hashtextextended(\$2::text, (0)::bigint) AND $guard_sql ORDER BY row_key LIMIT \$3;" ;;
        V3) printf '%s' "PREPARE v3(text,bigint,bigint,text,jsonb) AS SELECT body, idx_0 FROM hd_p_kpi_rows WHERE tenant_id = \$1 AND idx_0_digest = \$2 AND $guard_sql ORDER BY row_key LIMIT \$3;" ;;
        V4) printf '%s' "PREPARE v4(text,bigint,bigint,text,jsonb) AS SELECT body, idx_0 FROM hn_p_kpi_rows WHERE tenant_id = \$1 AND idx_0_digest = \$2 AND $guard_sql ORDER BY row_key LIMIT \$3;" ;;
        V5) printf '%s' "PREPARE v5(text,text,text,jsonb) AS SELECT body, idx_0 FROM hd_p_kpi_rows WHERE tenant_id = \$1 AND idx_0_digest = hashtextextended(\$2::text, (0)::bigint) AND NOT EXISTS (SELECT FROM hd_projection_registry WHERE projection_name = \$3 AND indexed_fields <> \$4);" ;;
    esac
}

explain_call() {
    digest=$(psql -c "SELECT hashtextextended('item', 0)")
    case $1 in
        V0 | V1 | V2) printf '%s' "EXECUTE $(echo "$1" | tr 'V' 'v')('owner','item',10,'kpi_rows','[\"kind\"]')" ;;
        V3 | V4) printf '%s' "EXECUTE $(echo "$1" | tr 'V' 'v')('owner',$digest,10,'kpi_rows','[\"kind\"]')" ;;
        V5) printf '%s' "EXECUTE v5('owner','item','kpi_rows','[\"kind\"]')" ;;
    esac
}

explain() {
    for variant in $VARIANTS; do
        tbl=$(variant_set "$variant")
        file=$ATTR_DIR/explain-$variant.sql
        {
            echo 'SET plan_cache_mode = force_custom_plan;'
            explain_statement "$variant" "$tbl"
            echo
            call=$(explain_call "$variant")
            echo "EXPLAIN (SETTINGS, SUMMARY) $call;"
            echo "EXPLAIN (SETTINGS, SUMMARY) $call;"
            echo "EXPLAIN (SETTINGS, SUMMARY) $call;"
        } >"$file"
        echo "--- $variant ($tbl): EXPLAIN (SETTINGS, SUMMARY), three runs in one session"
        docker exec -i "$CONTAINER" psql -h 127.0.0.1 -U postgres -d postgres -v ON_ERROR_STOP=1 <"$file" |
            awk '/Planning Time/ { n++ } n == 0 || /Planning Time/ { print }' |
            sed '/^SET$/d;/^PREPARE$/d'
    done
}

measure() {
    round=$1
    variant=$2
    tbl=$(variant_set "$variant")
    psql -c 'SELECT pg_stat_statements_reset()' >/dev/null
    digest=$(psql -c "SELECT hashtextextended('item', 0)")
    docker exec "$CONTAINER" pgbench -n -M extended -c 1 -T "$SECONDS_PER_RUN" \
        -D tenant=owner -D value=item -D lim=10 -D name=kpi_rows -D 'fields=["kind"]' -D "digest=$digest" \
        -f "/tmp/attr-sql/$variant.sql" -h 127.0.0.1 -U postgres postgres >"$ATTR_DIR/pgbench-$variant.out" 2>&1 || {
        cat "$ATTR_DIR/pgbench-$variant.out" >&2
        echo "kpi-plan-attribution: pgbench failed for $variant in round $round" >&2
        exit 1
    }
    stats=$(psql -c "SELECT calls, plans, round(mean_plan_time::numeric, 6), round(mean_exec_time::numeric, 6), round(rows::numeric / calls, 2)
        FROM pg_stat_statements
        WHERE query LIKE '%${tbl}_p_kpi_rows%' AND query LIKE '%projection_registry%' AND query NOT LIKE '%pg_stat_statements%'
        ORDER BY calls DESC LIMIT 1")
    if [ -z "$stats" ]; then
        echo "kpi-plan-attribution: no statement matched for $variant in round $round" >&2
        psql -c "SELECT calls, left(query, 300) FROM pg_stat_statements ORDER BY calls DESC LIMIT 10" >&2
        exit 1
    fi
    if [ "$round" -eq 0 ]; then
        echo "kpi-plan-attribution: statement of $variant as pg_stat_statements has it"
        psql -c "SELECT query FROM pg_stat_statements WHERE query LIKE '%${tbl}_p_kpi_rows%' AND query LIKE '%projection_registry%' AND query NOT LIKE '%pg_stat_statements%' ORDER BY calls DESC LIMIT 1"
    fi
    echo "$round|$variant|$stats" >>"$ATTR_DIR/rounds.tsv"
    echo "round $round $variant: $stats $(grep -h 'latency average\|^tps' "$ATTR_DIR/pgbench-$variant.out" | tr '\n' ' ')"
}

rotate() {
    echo "$VARIANTS" | awk -v k="$1" '{ for (i = 0; i < NF; i++) printf "%s ", $(((k + i) % NF + 1)) }'
}

rounds() {
    count=$1
    mkdir -p "$ATTR_DIR"
    : >"$ATTR_DIR/rounds.tsv"
    echo "nproc: $(nproc)"
    stamp 'before the rounds'
    echo 'columns: round|variant|calls|plans|mean_plan_ms|mean_exec_ms|rows_per_call'
    round=0
    while [ "$round" -lt "$count" ]; do
        for variant in $(rotate "$round"); do
            measure "$round" "$variant"
        done
        round=$((round + 1))
    done
    stamp 'after the rounds'
}

stop() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}

mkdir -p "$ATTR_DIR"
case ${1:-} in
    start) start ;;
    setup) setup ;;
    explain) explain ;;
    rounds) rounds "${2:?rounds}" ;;
    stop) stop ;;
    *)
        echo 'usage: kpi-plan-attribution.sh start | setup | explain | rounds N | stop' >&2
        exit 2
        ;;
esac
