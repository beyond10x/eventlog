# Adversary report: Eventlog PostgreSQL indexed find design

Document reviewed: design v1 of `docs/design/projection-find-planning.md` (v1 is not kept in the
repository; the file now holds v2, whose section 8.2 says which findings were taken). Code:
`origin/main` 6e72772a, read with `git show` and `tokio-postgres-0.7.18` from the cargo registry.
Nothing built, run or timed. Independent review; verdict approve with changes (end of file). Section and
decision numbers in this review (0.6, 8.1, 9, D1, D2) are v1's.
Labels: **verified** = read in code or driver source; **inferred** = PostgreSQL behaviour from
`plancache.c` / `postgres.c` knowledge and the docs, not run here.

Verdict at the end. Severity: blocks / must-change / note.

## F1. Central claim and whether the plan reaches 1.05x (design section 0, 8.1, 9)

Severity: must-change (framing and guard), no block.

- The claim holds. **inferred**: a named statement under `force_custom_plan` keeps the parsed and
  rewritten tree in the plan source and calls the planner on a copy at every Bind. Planning time
  (`total_plan_time` 77 -> 87 us) does not move. The unnamed statement of `query_typed`
  (`tokio-postgres query.rs:62-84`: Parse "", Bind "", Describe S, Execute, Sync) pays parse,
  analysis and rewrite on every call; a prepared custom plan pays none of them. So the saving is
  real server CPU, not a relabelled number.
- Will it reach <= 1.05x? Probably, with room. Parse plus analysis of this statement (5 parameters,
  a sub-select on the registry, a function call, a cast) is plausibly 20-50 us of server time; the
  shortfall to close is 8 us at the median ratio and about 23 us at the worst of the 4 runs
  (1.074 x 311 us). **inferred**, unmeasured. Which of the +10 us planning is `key_c` is also
  inferred: the planner opens every index of the table and builds paths for each, so a third index
  on a `tenant_id = $1` query is the likely main cost. That makes the gap structural, as the design
  says in section 0.6(b).
- What the design under-states: after the change head is faster than 0.10.0 per call by a different
  mechanism, while its planning still costs +10 us. The bound passes because the baseline is
  unnamed and head is prepared. This is legitimate work removed, but the owner's phrase is "no
  shortcut that only moves the number"; the PR text and CHANGELOG must say plainly that the +10 us
  planning stays and is paid for by the removed parse. Do not describe it as "find is not slower".
- Nothing guards the gain. The 1.05x bound lives only in `docs/design/projection-value-digest.md:15`
  and in manual dispatch runs; `kpis.json` leaves `ab_max_ratio` null (1.5) and the timing job is
  `continue-on-error` (`.github/workflows/kpi.yml`, timing job). If the cache silently stops being
  used (`refused = true`, a stale-evict loop, a future refactor), find returns to about 1.06x and no
  gate goes red. Smallest fix: a counter `postgres.projection_find.parses` (Parse messages per warm
  find, expected 0) from the counting proxy; `Framing::feed` (`crates/eventlog-kpi/src/pg.rs:190-215`)
  already splits frames and sees the type byte `P`. Deterministic, no timing, same file as the
  planned round-trip counter.
- Measure first: before V1, one CI `pgbench`-style run (CI is where such tools are allowed,
  AGENTS.md "The KPI gate") of the exact find on a 0.10.0-shaped table and a head-shaped table,
  `-M extended` against `-M prepared` with `PGOPTIONS='-c plan_cache_mode=force_custom_plan'`. It
  splits server cost into parse+analysis, plan and execute per layout, tells which of the 10 us is
  the index count, and bounds what V1 can save before any product code. Then V1 as designed. The
  1.03 pre-build threshold in section 9 is sound given 1.049-1.074 run-to-run spread.
- Alternative the design should name and reject: a generic plan behind an optimisation fence
  (sub-select with `OFFSET 0`, sort above it) skips planning entirely and removes the pk walk, but
  it fails for a hot value (reads and sorts every match). The design's refusal of generic plans is
  right; say why this variant is not used.

## F2. Session-wide `force_custom_plan` (section 3, option iv)

Severity: must-change (the proposed A/B cannot see the effect); the override question is a note.

- Statements that change: unnamed `query_typed` and one-shot `client.query(&str)` do not (one
  execution per plan source, custom today). **inferred**, correct. Only statements prepared once
  and executed more than 5 times change, from the 6th execution on: `atomic_group.rs:134, 245, 333`,
  `inline_admin.rs:240`, `lib.rs:2303, 2368`, each executed once per `STATEMENT_BATCH` = 1,000 items
  (`lib.rs:73`). So only an operation over 5,000 items changes. **verified** for the list.
- The design's measurement does not see it. Section 3 and 9 use `postgres.batch_append.throughput`
  and `postgres.group_blobs.latency` as the A/B. Their scenarios are 4 appends of 32 events and 16
  groups of 2 streams and 2 blobs (`kpis.json`; `crates/eventlog-kpi/src/timing.rs`, `provider_round`):
  one chunk each, so the 6th execution never happens. The gate would be green by construction.
  Smallest fix: one in-crate test or KPI case with a group of more than 6,000 events (it already has
  a 20,000-event group in the repo's own story text, `lib.rs:68-72`) run under `auto` and under
  `force_custom_plan`, comparing wall time and `pg_prepared_statements.generic_plans`; or state the
  reasoning (custom is what executions 1-5 already do, one plan per 1,000-item chunk is about
  0.1 ms against a chunk's work) and drop the A/B claim. Either way the section must stop calling
  the batch KPIs "the D2 gate".
- No plan changes inside the database: `grep` of `schema.rs`, `lib.rs`, `atomic_group.rs` finds no
  `CREATE FUNCTION`, plpgsql or trigger, so no cached PL plans are touched. **verified**.
- Overriding `ALTER ROLE ... SET plan_cache_mode` on a caller-owned connection: acceptable. The
  same `session_configuration` (`pool.rs:116-125`) already overrides `search_path`,
  `default_transaction_isolation`, `default_transaction_read_only`, `statement_timeout`,
  `lock_timeout` and `idle_in_transaction_session_timeout` on every pool session, caller-owned
  ones included (`pool.rs:628-640`). Document it in the `caller_owned` rustdoc next to those.
  `plan_cache_mode` exists since PostgreSQL 12 (the code's only version note, `pool.rs:19-23`, is
  about `client_connection_check_interval`, not this); no tolerance branch needed. The design's
  parenthesis about `pool.rs:19-23` is irrelevant and can go.
- Fragility, note: the safety of the cached statement rests on a session setting that `RESET ALL`
  or `DISCARD ALL` clears silently, after which the mode is the role default (`auto`) and the
  statement may go generic after 5 uses. Correctness is unaffected, speed is. F1's Parse counter
  does not catch it; `generic_plans` only in tests does. Document; no detection needed.

## F3. Stale-statement handling (section 2 and 4)

Severity: must-change (one item); the rest verified or note.

- SQLSTATE list, **inferred**, all plausible and right for the extended protocol:
  - 0A000 `cached plan must not change result type`: raised in plan revalidation, at Bind.
  - 42703 / 42P01: raised when revalidation re-analyses the statement after DDL, at Bind.
  - 26000 `invalid_sql_statement_name`: Bind or Describe of a statement the session no longer has.
  - 42P05 `duplicate_prepared_statement`: Parse of a name already present (not Bind).
  - 08P01 is pooler-specific noise; harmless in the allowlist because the fallback is the SQL that
    runs today.
- Where it surfaces: the error is the first response of the Bind/Execute/Sync batch. In
  tokio-postgres it comes out of `responses.next()` in `query::start` / `RowStream` poll as
  `Error::db` with `code()`; the server skips to Sync, so the connection is idle again. For
  `prepare_typed` Parse errors come at the first `responses.next()` (`prepare.rs:57-60`). The
  design's "classify on `tokio_postgres::Error` before `backend()` flattens it" is correct
  (`lib.rs:2558-2570` keeps the code only as text). **verified** for the driver, **inferred** for
  the server.
- Masking a real fault: no. The failed first attempt read nothing (it failed at Bind), so the retry
  is a fresh statement in a fresh read-committed snapshot, not a second view of a half-done read.
  The `NOT EXISTS` registry guard is part of the SQL text and is evaluated by the retry with the
  same parameters ($4, $5), so it is preserved. A genuine wrong-schema 42703/42P01 fails again
  and returns its own error through `backend()`. Note: the stale-eviction retry must wrap both
  statements of a find (digest and checked variant); with a collision the find can run 4
  executions, not "at most two". Say "per statement".
- Must-change: a multiplexing pooler thrashes silently. Prepare succeeds on server session X,
  Bind lands on Y and gives 26000, classified stale, evicted, retried unprepared; the next two
  finds repeat prepare + failed Bind + retry: 3 round trips per 2 finds, forever, counted but
  visible nowhere. Section 4 says "there is no loop", true, but there is a steady-state cost
  regression. Smallest fix: an execution-time 26000 or 08P01 on a statement prepared on this same
  connection (not one that outlived a `DISCARD`) sets `refused` like the prepare-time list. One
  line plus test 7's second case.
- Hazard, note, stronger than the design says: `tokio-postgres-0.7.18/src/prepare.rs:55-56` names
  statements from a process-global `NEXT_ID` counter: unique inside a process, identical across
  processes and replicas (`s0`, `s1`, ...). Behind a transaction-mode pooler two replicas can
  collide on a name; with equal parameter counts Bind then runs another client's statement and
  returns wrong rows, not an error. Test 5's "names differ" is true only in one process. That
  topology is already unsupported (F4), but it turns an error class into silent wrongness, so the
  rustdoc requirement "dedicated session" is mandatory, not optional.

## F4. Poolers, `DISCARD ALL`, dedicated session (section 4, attack question 4)

Severity: note.

- The adapter already requires a dedicated session. **verified**: `search_path`, timeouts and
  isolation are set once per physical session in `batch_execute(&configuration)`
  (`pool.rs:628-640`); the hosted principal is checked once per new physical session
  (`pool.rs:638-649`); tables are unqualified (`lib.rs:2007`). No document names a pooler.
  Nothing detects a lost `SET` today.
- The premise of attack question 4 is weaker than written: a session-mode pooler runs its reset
  query when the client connection is released, not between statements of one held connection, and
  the adapter holds its connection for the pool's life. A `DISCARD ALL` between two finds only
  happens in transaction-mode or per-statement multiplexing, which already breaks the schema
  resolution. After a real `DISCARD ALL`: prepared statements die (26000, handled by F3's rule) and
  `plan_cache_mode` reverts (speed only, F2). The change adds no correctness exposure beyond the
  name collision in F3; the owner of "dedicated session" is the pool's rustdoc, which the design
  already schedules.
- Hosted roles: `PREPARE` needs no privilege beyond the statement's own (PostgreSQL docs, SQL
  PREPARE). Proxies that pin sessions on named statements (managed connection proxies) would pin;
  state it in the rustdoc.

## F5. Skewed-tenant KPI (section 5)

Severity: must-change. Items 1-3 decide whether the case proves anything; without them the
acceptance step 4 can fail for the wrong reason or pass for the wrong reason.

1. The "good plan reads about 10 tuples" claim is unproven and probably false on the fixture as
   written. Column statistics are table-wide and the planner multiplies selectivities as if
   independent. The `rare` value is 640 of 72,600 rows (10 per tenant x 64); with the large tenant
   at 83 % of the table the custom-plan estimate for (large tenant, `rare`) is about 530 rows, not
   10. For that estimate a primary-key walk with `LIMIT 10` (stop after 10/530 of the walk) is
   estimated far cheaper than the digest index plus sort, so the custom plan itself can pick the
   60,000-tuple walk. Then head's baseline records about 60,000, G and U do not differ, and the
   case is green on every variant. **inferred** from cost arithmetic; decide with `EXPLAIN` on CI
   before any counter is recorded. Fix the fixture so the skew is visible in single-column stats:
   `rare` must be rare in the whole table (about 10-20 rows in total, in the large tenant) so the
   custom estimate is a handful of rows.
2. Variant U may never flip. Under `auto` the generic plan is used only when its estimated cost
   is below the average estimated cost of the first five custom plans. A sequence of only `rare`
   values gives the cheapest custom costs, which is the least likely to flip. The existing repo
   note (`projection-value-digest.md:20`, generic walked the primary key) suggests it flips on the
   1,001-row `projected` fixture, which is the better place to see U: run V2 and V3 on the existing
   `postgres.projection_find.latency` first, before building a new fixture. For the new fixture,
   assert the mechanism, not the timing: `SET plan_cache_mode = force_generic_plan; EXPLAIN
   (ANALYZE, BUFFERS) EXECUTE` of a prepared copy of the statement (or `EXPLAIN (GENERIC_PLAN)` on
   PostgreSQL 16+; CI runs 17.6) must show the pk walk, and the custom EXPLAIN must show the digest
   index. Deterministic, no stats flush, no timing; the repo already has the `EXPLAIN ANALYZE`
   counter pattern (`postgres.feed_one_row.buffers`, `kpi/baseline.json:33`). Keep the real-path
   stats counter for the real change.
3. ANALYZE on more than 30,000 rows samples randomly (300 x default statistics target), so
   `n_distinct`, MCV lists and the plan can differ between runs, and a counter that depends on the
   plan then flaps. Keep the fixture at 30,000 rows or fewer, or `ALTER TABLE ... ALTER COLUMN
   ... SET STATISTICS 1000` so ANALYZE reads every row.
4. Stats flush. After `Store::close` the backends exit asynchronously; "poll until two reads
   agree" can agree on two pre-flush reads. Wait until the pool's backends are gone from
   `pg_stat_activity` (the repo's pattern, `tests/retired_connection.rs:73` and `await_gone`) and
   then read once. Use deltas of `idx_tup_read` and `seq_tup_read` taken before and after;
   `pg_stat_reset_single_table_counters` resets only the one relation, not its indexes, and needs
   a privilege the hosted-role runs may lack. The CI service has `--cpus 2`; the flush timer
   (about 1 s) is not the problem once the backends have exited.
5. Autovacuum on a 2-CPU container: it does not change `idx_tup_read` or `seq_tup_read` (index
   vacuum is bulk delete; a manual ANALYZE resets the autoanalyze trigger), so the counter is
   stable. The timing is not: 72,600 freshly inserted rows trigger an insert-based autovacuum
   inside the measured window on 2 CPUs. Use `VACUUM (ANALYZE)` instead of `ANALYZE` in the
   fixture, or set `autovacuum_enabled = false` on the fixture table.
6. Only p50 is judged by `ab` (`kpi_ab.rs:204-228` uses the median of per-round p50s); the
   "p95" of the new ids is displayed, not gated. Say so or drop p95 from the ids.
7. Recording. `Verdict::Unrecorded` fails the gate (`compare.rs`), and `record` rewrites the whole
   baseline including timings. Risk 4 is real and blocks the counters job on the PR itself. Decide
   now: a `workflow_dispatch` input `record` on the counters job that runs `record` on the CI
   runner and uploads `kpi/baseline.json`, committed by a human (the rule is "no `record` on the
   host `next`", not "never on CI"; counters are machine-independent). Without it the plan cannot
   close.

## F6. Cost and test claims (sections 6, 9)

Severity: must-change (test placement), notes for the rest.

- Must-change: tests 1, 2, 5, 7, 8 read session-local or crate-private state
  (`pg_prepared_statements`, `SHOW plan_cache_mode`, the `Pool` counters, a `prepare_typed` hook).
  They cannot live in `tests/` (an external binary): an outside session cannot see another
  session's prepared statements, the `Pool` and `Lease` are `pub(crate)` (`pool.rs:831`), and D1 A
  makes the counters private. The existing in-crate tests (`pool.rs:1089, 1198, 1279`, hooks at
  `pool.rs:488, 550`) show the place: `#[cfg(test)]` modules inside `src/`. Fix: put them in
  `pool.rs`/`lib.rs` test modules (no new integration binary, which also keeps the one-binary
  rule) and say so in the table of section 9.
- Test 2's count: finds 2..30 are the 29 prepared executions, so `custom_plans` reads 29, not 28
  (find 1 is unprepared). Off by one in the expected value.
- The `Seen`/`Prepared`/`refused`/64-entry machinery plus three `AtomicU64` is about 220 lines of
  product code whose counters are read only by tests. Gate the counters with `cfg(test)` or expose
  them; unread production counters are the tech debt the owner excluded. `FindStatements` inside
  `pool.rs` also puts a projection concept in a generic pool module; a neutral `StatementCache`
  keyed by SQL is the same size and keeps the layering.
- Round trips: warm find is Bind, Execute, Sync = 1.00 **verified** from the driver; `Statement`
  drop sends Close + Sync (counted, only on eviction). Cold costs: find 1 is 1 RT, find 2 is 2 RT.
  In the KPI each round opens a fresh store and runs 32 finds, so two samples are slow and the p50
  of 32 is unaffected; p95 (rank 31 of 32) takes one of them but is not gated (F5.6).
- Budgets: the new tests are DB tests under 5 s each; no model, no network beyond the fixture.
  The KPI lane adds seeding 72,600 rows per round per side (about 1-2 s with three indexes and a
  generated column) plus the 80 finds; 20 rounds x 2 sides stays far under the 60-minute job
  timeout. Not measured; time them before adding (folder rule).
- Line count 870 is plausible; most of it is KPI and tests.

## F7. Missed items (attack question and section 8)

Severity: note unless stated.

- Concurrency: one owner per `Connection` at a time (`pool.rs:667` pops, `884` pushes), so no
  lock is needed. **verified**. Idle reuse is LIFO (`Vec::pop`), so a hot connection warms first.
- Cancel-on-drop: a deadline in the middle of `prepare_typed` (Parse + Describe + Sync) leaves the
  lease unsettled; `Lease::drop` cancels server-side and severs (`pool.rs:861-927`); the `Statement`
  dropped in flight sends Close into a dead channel, ignored. No leak. Same for the retry path.
- Memory: 64 entries x (SQL text about 400 bytes + client `Statement`) is negligible client-side;
  server-side each named statement keeps its parsed tree, roughly tens of KB, so about 1-2 MB per
  backend at the cap. The cap without eviction means a store with more than 64 distinct (projection,
  field, variant) texts silently never caches the later ones: a performance cliff with no signal.
  Count it (the F1 Parse counter shows it) or use LRU with the Close piggybacking the next call.
- FIPS and the digest: unaffected. `hashtextextended` runs no OpenSSL digest
  (`schema.rs:1198-1205`); prepared statements add no digest. Not a finding.
- The collision rerun uses `projection_find_checked_filter`, a second SQL text, so it takes its
  own slot and its own warm-up; a rerun happens on a hash collision only, so it is cold by design.
- `PoolStatus` and `PoolOptions` stay unchanged: agreed (public fields, not `non_exhaustive`).
- The transaction-scoped `ProjectionStore::find` (`lib.rs:1896-1950`) still prepares and closes
  per call (three Syncs); the design defers it. Acceptable if the story text names it, since a
  caller that finds inside an append pays 3x and the bound does not cover it.

## Verdict: approve with changes

Must change before build:

1. F5.1-3: re-derive the skewed fixture from `EXPLAIN` on CI (independence assumption makes the
   custom plan walk too; U may never flip); keep it at or under 30,000 rows or raise statistics.
2. F5.4, F5.5, F5.7: wait for backend exit before reading stats, use deltas, `VACUUM (ANALYZE)`, and
   decide the CI `record` path now.
3. F6: tests 1, 2, 5, 7, 8 cannot be in `tests/`; move to in-crate `#[cfg(test)]`, fix 29 vs 28.
4. F2: the batch KPIs cannot show the sixth-execution effect (1 chunk); add a more-than-6,000-item
   case or drop the A/B claim.
5. F3: treat Bind-time 26000/08P01 on a statement prepared on the same connection as `refused`
   (multiplexing pooler thrash).
6. F1: add a Parse-count counter so the prepared path is guarded, state in the PR that planning +10 us
   stays, and run the CI pgbench split plus V1 first.
