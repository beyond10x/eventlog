# Eventlog PostgreSQL indexed find: a prepared find, gated on a measurement

Owner: story:projection-find-holds-its-bound-with-a-prepared-statement. It continues
story:pg-projection-find-back-within-1-05 and `docs/design/projection-value-digest.md`, whose bound
it keeps. The independent review of the first version is
`docs/design/projection-find-planning-review.md` (finding ids F1-F7 below).

Status: design v3, the product change built (section 10, step 5; build notes in section 11).
Design v2 was written 2026-11-24 10:45 CET. The CI spike for the build gate G1 (section 0.4) was
run 45368249583 on branch `ci/plan-prepared-spike` (**reported**), prepared + `force_custom_plan`
against the unprepared 0.10.0 shape P1/E0 0.895, so G1 was met at pgbench level and provisional
until variant V1 (section 10, step 4b). V1 passed: median 0.973 of the `postgres.projection_find`
p50 against 0.10.0 over 4 CI runs (limit 1.03), so the product change was built.
Supersedes design v1, which is not kept in the repository. Source: `origin/main` 6e72772a, read with
`git show origin/main:<path>` (no repository edit, no build, no run). Paths are relative to the
repository root. Inputs: v1, the adversary review of v1, and the CI attribution run 45365255371
(branch `ci/plan-attribution`, head f885cdf3; section 0.2).

Owner's order: do it the right, performant, efficient way; no tech debt; the bound stays:
`postgres.projection_find` p50 of head within 1.05x of 0.10.0 (`docs/design/projection-value-digest.md:15`),
judged as the median of 4 dispatched 20-round `KPI / KPI timing` runs on the CI runner. No change
that only moves the number.

Labels: **verified** = read in the source, the driver source or a quoted document. **inferred** =
follows from PostgreSQL behaviour not run here. **reported** = a CI number from a run named in the text,
not re-run here. **to measure** = decided by a CI run, section 10.

Changes against v1, by finding id (details in the sections named):

| Id | Change | Section |
|---|---|---|
| attribution | the shape-only outcome is closed; the build is gated on the spike (G1); two fallbacks are costed for the owner | 0.2, 0.4, 0.5 |
| F1 | honest framing, PR and CHANGELOG wording, a Parse-per-find counter | 0.3, 5.4, 10 |
| F2 | batch A/B re-built over more than 5,000 items; the override and `RESET ALL` notes | 3 |
| F3 | Bind-time 26000/08P01 sets `refused`; retry is per statement; name hazard is mandatory rustdoc | 2, 4, 7 |
| F5.1-3 | skewed fixture re-derived: rare value rare table-wide, at most 30,000 rows, EXPLAIN-derived on CI | 5.2 |
| F5.4, 5.5, 5.7 | counter read after backend exit, deltas, `VACUUM (ANALYZE)`, a decided CI record path | 5.3, 10 |
| F5.6 | only p50 is gated; the p95 ids are dropped | 5.3 |
| F6 | tests that need session-local or crate-private state move in-crate; 29 custom plans; neutral `StatementCache`; counters `cfg(test)` | 1, 6, 9 |

## 0. Where the time is, and what a prepared statement can and cannot save

### 0.1 The find

1. The find the KPI times is `projection_find`, `crates/eventlog-postgres/src/lib.rs:1567`. It sends
   Parse, Bind, Describe, Execute, Sync as one batch with the unnamed statement (`lib.rs:1615`; the
   driver's `query_typed`, `tokio-postgres-0.7.18/src/query.rs:62-84`: `frontend::parse("", ..)`,
   `encode_bind_raw("", ..)`, `describe`, `execute`, one `sync`). **verified.** One Sync is one round
   trip in the KPI's counting proxy (`crates/eventlog-kpi/src/pg.rs`, `Framing::feed`: type bytes
   `S` and `Q` count; `P` is read and ignored). **verified.** So every find costs the server a parse,
   an analysis, a rewrite and a plan. The four stages are PostgreSQL behaviour, **inferred.**
2. 0.10.0 filters `idx_N = $2` on an index `(tenant_id, idx_N)` and selects `body`
   (`git show c0f961dc:crates/eventlog-postgres/src/lib.rs`, lines 1600-1607). Head filters
   `idx_N_digest = hashtextextended($2::text, (0)::bigint)` (`schema.rs:1216-1221, 1203-1205`),
   selects `body, idx_N` (`lib.rs:1596`), and the table has one more index, `key_c`
   `(tenant_id, row_key COLLATE "C")` (`schema.rs:1154-1166`; 0.10.0 had only `(tenant_id, idx_N)`).
   Both statements end `ORDER BY row_key LIMIT $3` (**verified**, same text in both revisions).
   Head also compares each returned row's `idx_N` in Rust (`every_candidate_holds`, `lib.rs:1971`).
   **verified.**
3. The KPI scenario is 32 finds of 10 over 1,001 projected rows, one store, sequential
   (`crates/eventlog-kpi/src/timing.rs`, `provider_round`); the A/B collects each round's p50 and
   compares the medians over rounds (`ab.rs:189, 213`). **verified.**

### 0.2 Attribution (reported, CI run 45365255371, branch `ci/plan-attribution` head f885cdf3: SQL only, pgbench extended, 8 rounds, PostgreSQL 17.6)

| | base (0.10.0 shape) | head | delta |
|---|---|---|---|
| planning per find | 37.0 us | 44.5 us | +7.5 us (1.20x) |
| execution per find | 12.9 us | 14.0 us | +1.1 us |

Of the +7.5 us planning: the extra index `key_c` alone +4.0 us; `hashtextextended()` on the
parameter alone +3.1 us; with both removed +0.4 us. The ordered-index path (`ORDER BY row_key
LIMIT`) is 10.8 us of head's planning; it is a level, not a delta (same text in both revisions,
section 0.1 item 2). What follows, each step labelled:

- **The shape-only outcome of v1 (its 0.6 (a): fix the statement text, build no cache, stop) is
  closed.** 7.1 of the 7.5 us are structural: the index count (the planner builds paths over every
  index of the table; **inferred**) and a server-side call on the parameter. No text of this
  statement removes either. Only the remaining 0.4 us is text.
- The SQL-only gap is about 8.6 us (7.5 + 1.1). The end-to-end gap is larger: the four earlier
  runs gave 1.049-1.074 on a base of about 311 us (`projection-value-digest.md:19`), that is
  +15 to +23 us. **inferred** from the two documents. Parse, analysis and rewrite are not in the
  attribution table. The remaining 6-14 us are either the server's parse and analysis of the
  longer head statement or client work (the extra selected column and the Rust candidate check);
  the table cannot say which. The spike (G1) measures the end-to-end number and so covers both.
- Neither structural cause is removed by a prepared statement. A prepared statement removes
  parse, analysis and rewrite from every call after the first; it keeps planning (0.3).

### 0.3 What a prepared statement saves, and the framing that follows (F1)

PostgreSQL's text: "Custom plans are made afresh for each execution using its specific set of
parameter values, while generic plans do not rely on the parameter values and can be re-used
across executions. Thus, use of a generic plan saves planning time, but if the ideal plan depends
strongly on the parameter values then a generic plan may be inefficient."
(<https://www.postgresql.org/docs/17/runtime-config-query.html>). **verified.**

- A named statement under `force_custom_plan` keeps the parsed tree and plans on a copy at every
  Bind. Planning time does not move (**inferred**). It removes parse, analysis and rewrite. That is
  server CPU removed per call, 0.10.0 pays it too and head would no longer pay it.
- A generic plan would remove planning as well and is unsafe here (section 5).
- After the change, head is faster than 0.10.0 per call by a different mechanism, while its
  planning still costs +7 to +10 us per find (+7.5 us SQL-only; +10 us in the earlier
  `plan_time` numbers, 77 to 87 us). The bound passes because the base is unnamed and head is prepared. That is
  work removed, not a relabelled number, and the text must say so. **Wording required in the pull
  request and the CHANGELOG** (the numbers are filled from the acceptance runs, section 10):
  "`projection_find` reuses a prepared statement on each pooled connection from its second use.
  The server plans the statement on every call as before and no longer parses and analyses it.
  The planning cost of the new projection indexes (+7 to +10 us per find: the page index and the
  server-side digest call) stays; it is paid for by the work the prepared statement removes.
  Median latency against 0.10.0: <ratio> over 4 CI runs." Do not write "find is not slower" or
  "planning is unchanged".
- The gain is guarded (F1): the counter `postgres.projection_find.parses` (section 5.4) fails when
  the cache silently stops being used (`refused`, an evict loop, a future refactor); without it
  the find would return to about 1.06x and no gate would go red (`ab_max_ratio` is null and the
  timing job is `continue-on-error`, **verified**, `kpi/kpis.json`, `.github/workflows/kpi.yml`).

Alternative named and rejected (F1): a generic plan behind an optimisation fence (the filter in a
sub-select with `OFFSET 0`, the sort above it). It skips planning and removes the primary-key walk
of section 5, but for a hot value it reads and sorts every match of the tenant; it trades the
rare-value failure for the hot-value one. **inferred.**

### 0.4 The build gate and the decision rule

- **G1 (build gate).** Build only if the prepared statement under `force_custom_plan`, on head's
  statement, measured end to end, is at most 1.03x of the unprepared base (0.10.0 shape,
  `-M extended`), the margin the original design set (the four runs vary 1.049-1.074, a median at
  1.05 would fail half of the acceptance runs). Reading rules:
  - If the spike runs the Rust find path (the KPI runner), its median is final.
  - If the spike is a pgbench-level end-to-end run, it cannot see the client's work (decode, the
    candidate check; **inferred**). Then G1 is provisional: it also needs one scratch branch V1
    (the prepared find, nothing else) dispatched 4 times through `KPI / KPI timing`, `base` = the
    0.10.0 tag, median at most 1.03, before the story is accepted.
- **G2 (fixture gate)**, from the same spike (the skewed fixture under `auto` and
  `force_generic_plan`): the fixture is accepted only if it meets E1 and E2 of section 5.2. E3
  (does `auto` flip) is recorded, it decides only the wording of the variant-U proof.
- Outcomes:
  - G1 passes: build sections 1-9 and follow section 10.
  - G1 fails: nothing of sections 1-4 is built. The two honest ways forward are in 0.5; the choice
    is the owner's (D5). The bound is not touched.
  - G2 fails (a generic plan is not bad on any fixture of at most 30,000 rows): the setting is not
    dropped on that evidence, because the repository already records that a cached statement went
    generic and walked the primary key on the 1,001-row fixture (`projection-value-digest.md:20`,
    **verified**). The skewed case then reduces to the two deterministic counters of 5.4 and test 2,
    and the finding goes to the owner (D3).

### 0.5 If G1 fails: the two fallbacks (decision for the owner, none taken here)

Both attack a structural cause of 0.2. Neither is built until the owner picks.

**FA. A digest computed by the client, sent as a `bigint` parameter** (the filter becomes
`idx_N_digest = $2`).

- Gain: removes the server-side call, about 3.1 us of planning (reported), about 1 percent of a
  330 us find. It still plans every call; alone it does not reach 1.03 (arithmetic from 0.2,
  **inferred**).
- Needs a Rust implementation identical to PostgreSQL's `hashtextextended(text, 0)`. For a
  deterministic collation the server hashes the stored bytes with its own non-cryptographic hash
  (`hash_any_extended`; the source of that function was not read here, **inferred**). It must be
  exact for every input (empty, multibyte, long) and for every supported major (13 and later),
  pinned by a CI test against a live server and a property test.
- Raises an encoding question: the design document rejected a client digest because it "misses
  rows on a database whose encoding is not UTF-8" (`projection-value-digest.md:21-22`,
  **verified**). The client hashes the bytes it sends (UTF-8); the server hashes the stored bytes
  in the database encoding. So FA needs an admission that refuses a non-UTF-8 database (no such
  admission exists today: `schema.rs` has no encoding check, **verified** by search), a new
  refusal for hosts.
- Raises an on-disk digest-format question: the stored column stays server-generated
  (`GENERATED ALWAYS AS (hashtextextended(idx_N, 0)) STORED`), so the bytes on disk do not change.
  What changes is a contract: the Rust function must equal the server function for ever. A drift
  produces silent misses, which the candidate check cannot see (the design document's "silent
  miss" argument, **verified**). A guard would be one probe at `open` comparing the server's
  `hashtextextended('probe', 0)` with the Rust value (one more round trip or a piggy-back on an
  existing admission query; `postgres.open.round_trips` would move).
- Raises a FIPS question: the server function runs no OpenSSL digest (`schema.rs:1198-1205`,
  **verified**) and a Rust copy of a non-cryptographic hash would run none either. Whether a
  hand-written hash in the product is acceptable under the FIPS posture the digest design set is
  the owner's call; the repository does not say.

**FB. A change of the paging-index shape** (drop `key_c`).

- Gain: removes about 4.0 us of planning (reported), about 1.2 percent, and one btree per table
  (write amplification and size). Alone it does not reach 1.03.
- Shape FB1: declare `row_key` with `COLLATE "C"` on the projection table. The primary key then
  orders bytewise; it serves `projection_page` (`lib.rs:2017-2024`) and `projection_find`
  (`ORDER BY row_key`), and `key_c` goes.
- Cost: (a) `projection_find` order on a non-C database changes from the database collation to
  byte order; the trait documents no order for it (`crates/eventlog-core/src/lib.rs:1207-1211`,
  **verified**), the other providers' order was not read here. (b) `migrate` rewrites the table
  and rebuilds the primary key under `ACCESS EXCLUSIVE` (it already rewrites for the digest
  column; the two could share one rewrite, **inferred**). (c) A new ledger checksum and edition
  row; the admission shape comparison reads `attcollation` (`schema.rs:310`, **verified**) and the
  capture admission lists the columns, so both change. (d) The editions on `main` since 0.10.0 are
  unreleased (CHANGELOG `[Unreleased]`, **verified**), so only the 0.10.0 path must migrate
  unless the owner supports databases built from `main`.
- Rejected inside FB: no paging index and sort each page (the reason the index exists,
  `schema.rs:1149-1152`); build `key_c` lazily on first `projection_page` (DDL on a read path; a hosted
  role must not own DDL, `HostedRole::admit`, `pool.rs:92-107`, **verified**).

**FA + FB** together remove about 7.1 us SQL-only (about 2 percent), which moves a 1.06 median to
about 1.04 (**inferred**): it can reach the 1.05 bound, not the 1.03 margin. A fallback is judged
against the 1.05 bound with the same 4-run median; whether that margin is enough is the owner's.

## 1. Where the prepared statement lives, and how it dies

**Decision: one cache per physical connection, inside `Connection`, keyed by the exact SQL text,
implemented as a neutral `StatementCache` (F6: no projection concept in the generic pool).**

- `Connection` (`pool.rs:962-968`) holds `client`, `driver`, `transport`, `cancel`. It moves between
  `PoolState.idle` (`pool.rs:496`) and a `Lease` (`pool.rs:831`, `client: Option<Connection>`) by
  value (`checkout` pops it, `pool.rs:667`; `Lease::drop` pushes it back, `pool.rs:884`). A field of
  `Connection` follows the session exactly: same lifetime as the server session that owns the
  statements. **verified.**
- A prepared statement is session state: "Prepared statements only last for the duration of the
  current database session. When the session ends, the prepared statement is forgotten ... a single
  prepared statement cannot be used by multiple simultaneous database clients"
  (<https://www.postgresql.org/docs/17/sql-prepare.html>). **verified.** A pool-level map would hand
  a statement name to a connection that never prepared it; rejected. Not per `Lease`: a lease lives
  for one operation (`lib.rs:1580-1651`) and would prepare on every find.
- Key: the SQL text. It contains the table name, `{prefix}_p_{name}` (`lib.rs:2007-2009`), the
  field position `idx_{position}` and the filter variant (digest or checked, `schema.rs:1216,1225`).
  Schema is not in the text because every statement is unqualified and `search_path` is set once
  per physical session (`pool.rs:116-120`, applied `pool.rs:628-640`); one `Pool` has one
  `PostgresConfig` schema (`pool.rs:260-266`). PostgreSQL re-parses a statement whose `search_path`
  changed ("if the value of search_path changes from one use to the next, the statement will be
  re-parsed", sql-prepare page). Parameter types are passed explicitly as today (`TEXT, TEXT, INT8,
  TEXT, JSONB`, `lib.rs:1607-1613`) through `prepare_typed`. **verified** (code and quote).
- Shape (crate-private, new private module `statement_cache.rs`, no projection or pool types in
  it): `struct StatementCache { entries: HashMap<String, Entry>, refused: bool }`,
  `enum Entry { Seen, Prepared(Statement) }`. A pure state machine with one entry point that, given
  the SQL text, says: run unprepared and record `Seen`; prepare now; use this statement; or the
  connection is refused. The driver calls stay in `lib.rs`. No lock: a `Connection` has exactly one
  owner at a time (`pool.rs:496, 831`). The state machine has unit tests without a database (6).
- Warm-up rule: the first find of a text on a connection runs exactly as today (`query_typed`, one
  round trip) and records `Seen`. The **second** find of that text prepares (`prepare_typed`, one
  round trip) and executes. A connection that serves one find never pays the extra prepare round
  trip; cold paths stay byte-identical to today. The prepare cannot be pipelined with the execute
  through the driver's public API (`prepare.rs:57-92`: `prepare` returns after its own response).
  Steady state is Bind, Execute, Sync: one round trip (**verified** from the driver's
  `query(&Statement)`; the 1.00 per find is a counter, 5.4).
- Bound: at most 64 entries per connection (a constant, not a knob; section 11 for texts seen once). A 65th distinct text is not
  cached and runs through `query_typed` (no eviction, so no `Close` churn). Distinct texts are
  projections x indexed fields x 2 variants. The cap is a silent performance cliff for a store
  with more than 64 distinct texts (F7). Taken: it is documented in the rustdoc and pinned by a
  test (6, test 9). Not taken: production counters or an LRU (disposition table, 8.2).
- Memory (F7, **inferred**): each named statement keeps its parsed tree server-side, tens of KB;
  about 1-2 MB per backend at the cap. Client side the cache is negligible.
- Statements are never dropped on a live connection in steady state: `Statement::drop` sends
  `Close` plus `Sync` (`statement.rs:13-25`), which lands on the caller's next operation and counts
  there; the code documents this effect (`lib.rs:2741-2745`). **verified.**
- Statement names are `s<N>` from a process-global counter (`prepare.rs:59, 66`, **verified**):
  unique within one process, identical across processes. See 4 and 7.

How it dies, each case with its path:

| End of the connection | Path | Cache |
|---|---|---|
| Normal retirement of an unsettled lease (cancelled, timed out, errored operation) | `Lease::drop` `pool.rs:861-927`: `reusable == false` -> `connection.sever()` `pool.rs:915`, then `close()` `pool.rs:974-981` | dropped with the `Connection`; the server frees the statements with the session |
| Cancelled transaction or a deadline inside `prepare_typed` | the future is dropped inside `bounded` (`lib.rs:345-356`) before `client.settled()`; the lease is unsettled, the connection never returns to `idle` (module doc `pool.rs:1`); a `Statement` dropped in flight sends `Close` into a severed channel and is ignored (F7) | same |
| Closed client found at checkout | `pool.rs:606-621`: `connection.close().await`, replacement popped or opened | same |
| Pool shutdown | `pool.rs:780-805`: idle connections are wrapped in a `Lease` and dropped with the pool closed | same |
| Driver close | `Connection::drop` `pool.rs:983-990` severs and aborts the driver | same |

Field order: the cache after `client`, so the client drops first; a `Statement` then holds a
`Weak<InnerClient>` that no longer upgrades and sends nothing (`statement.rs:13-25`). **verified.**
A settled lease on a live connection keeps its cache.

## 2. Schema change, stale statements, refusal

What changes the physical table after a connection prepared a find: `migrate` (drops retired
indexes, `schema.rs:1173-1186`; `ALTER TABLE ... ADD COLUMN idx_N_digest ... STORED`, a rewrite;
creates the digest and page indexes, `schema.rs:1364-1399`; one transaction behind an advisory
lock that holds the table `ACCESS EXCLUSIVE` until commit, `schema.rs:268-270`), `drop_tables`
(`lib.rs:386-406`), a DBA's `DROP INDEX`, `ANALYZE`, `REINDEX`, a later edition's migration.

What PostgreSQL does: "PostgreSQL will force re-analysis and re-planning of the statement before
using it whenever database objects used in the statement have undergone definitional (DDL) changes
or their planner statistics have been updated" (sql-prepare page, **verified**). An added column, a
dropped or created index, a rewrite and `ANALYZE` are handled by the server with no error. The
find selects named columns, never `*` (`lib.rs:1596`), so the digest column cannot change its
result type.

Errors, **inferred** from the plan cache (`plancache.c`), all raised at Bind (F3, **inferred**):
0A000 `cached plan must not change result type`; 42703 and 42P01 after DDL; 26000 for a statement
the session no longer has; 42P05 at Parse for a name already present; 08P01 is pooler noise.
Section 6 forces each code on CI before the lists are final.

Rule, **per statement**: classify on the `tokio_postgres::Error` before `backend()` flattens it to
text (`lib.rs:2558-2570`).

1. Execute the cached statement. On success: done.
2. SQLSTATE 0A000, 42703 or 42P01: **stale**. Remove the entry (`Statement` drop queues one
   `Close`), run the **same SQL through `query_typed`** exactly once, return its result or its
   error through `backend()`. The next find of that text starts again at `Seen`. At most two
   executions per statement (a find that meets a digest collision runs a second statement, the
   checked variant, with its own slot and its own cold start: at most four per find, F3).
3. SQLSTATE 26000 or 08P01 at execution of a statement this connection prepared (F3): the session
   or a pooler in front of it does not keep named statements. Set `refused` for the life of this
   `Connection`, drop the cache, run the same SQL through `query_typed` once, and from then on
   every find on this connection goes straight to `query_typed`. A real `DISCARD ALL` is treated
   the same (indistinguishable from a multiplexing pooler; correctness is unchanged, only the
   speed-up is lost on that connection). This closes v1's steady-state thrash (prepare, failed Bind,
   retry, forever).
4. Any other error is returned as today through `backend()`. It leaves the lease unsettled, which
   retires the connection (`lib.rs:1617` returns before `settled()`) and the cache with it.
   Never retried, never swallowed.
5. `client.settled()` is called as today: an error in a single-Sync batch ends at `ReadyForQuery`,
   so a classified error leaves the connection idle; if the retry fails the lease stays unsettled
   exactly as on the unprepared path.

Masking a real fault (F3, **verified** reasoning): the failed first attempt failed at Bind and read
nothing, so the retry is a fresh statement in a fresh read-committed snapshot. The `NOT EXISTS`
registry guard is part of the SQL text and is evaluated again with the same `$4`, `$5`. A genuine
wrong-schema 42703/42P01 fails again and returns its own error.

The find runs in autocommit (`lib.rs:1580-1651`), so a stale-plan error aborts no transaction. The
transaction-scoped `ProjectionStore::find` (`lib.rs:1896-1950`) is **not** cached: a failed statement
there aborts the transaction (25P02 afterwards) and a retry is impossible. It keeps its per-call
`query(&str)` (three Syncs per find, not covered by the bound; the story text names it, F7).

Two processes at different versions on one database:

- A 0.10.0 process has no cache. After `migrate` it keeps running its `idx_N = $2` statement (a
  sequential scan, its index is dropped) but the changed ledger checksum makes every older binary
  refuse `open` and `migrate` (`schema.rs:927-960`, `docs/design/projection-value-digest.md`,
  "Edition, fence and checksum"); the fence is checked at `open` (`schema.rs:975-985`), not per
  statement. **verified.**
- The new binary cannot open before `migrate` ran (`validate`, `schema.rs:975`), so none of its
  connections holds a statement over the pre-migration table. Staleness comes from later events;
  the rule covers them and costs nothing when none occurs. A running new-binary process under a
  later edition's `migrate` is not fenced per statement either; the unprepared path has the same
  exposure today.

## 3. Plan mode

PostgreSQL has no per-statement mode. The setting applies "when a cached plan is to be executed,
not when it is prepared" and takes `auto` (default), `force_custom_plan`, `force_generic_plan`
(runtime-config-query page, **verified**). Under `auto`: "the first five executions are done with
custom plans and the average estimated cost of those plans is calculated. Then a generic plan is
created and its estimated cost is compared ... Subsequent executions use the generic plan if its
cost is not so much higher than the average custom-plan cost" (sql-prepare page, **verified**).

- **(i) `auto`, no setting.** No extra round trip; after five executions a connection may switch to
  a generic plan, decided by the first five parameter values that connection happened to see:
  nondeterministic per-connection performance. The repository records a cached statement going
  generic and walking the primary key (`projection-value-digest.md:20`). Reject.
- **(ii) `SET LOCAL` in a transaction around the find.** BEGIN, SET LOCAL, find, COMMIT: three more
  Syncs (the counter would read 4, `pg.rs`). Breaks the 1.00 rule. Reject.
- **(iii) session-wide `force_generic_plan`.** The unsafe mode of section 5, for every statement of
  the connection. Reject.
- **(iv) session-wide `force_custom_plan`, added to `session_configuration` (`pool.rs:116-127`).**
  The same batch that already runs at connect (`batch_execute`, `pool.rs:628-640`): no extra round
  trip. Effect on the rest of the adapter (F2, **verified** list, **inferred** mechanism):
  - unnamed typed statements (`lib.rs:602, 775, 1615, 1620`, `capture.rs:110`) and one-shot
    `client.query(&str)`: one execution per plan source, custom today. No change.
  - statements prepared once and executed repeatedly: `atomic_group.rs:134, 245, 333`,
    `inline_admin.rs:240`, `lib.rs:2303, 2368`; each runs once per `STATEMENT_BATCH` = 1,000 items
    (`lib.rs:73`; chunk loops `atomic_group.rs:153, 252, 344`, `lib.rs:2321, 2377`). Only the sixth
    and later execution changes, so only an operation over 5,000 items: it is re-planned with its
    parameters per chunk instead of possibly switching to a generic plan. Results never depend on
    the plan.
  - No plan inside the database is touched: no `CREATE FUNCTION`, plpgsql or trigger in
    `schema.rs`, `lib.rs`, `atomic_group.rs` (F2, **verified** by search).
  - **The v1 measurement could not see this** (F2, **verified**): `postgres.batch_append.throughput`
    and `postgres.group_blobs.latency` run 4 appends of 32 events and 16 groups of 2 streams and 2
    blobs (`kpis.json`, `timing.rs`): one chunk each, the sixth execution never happens. v1 called
    them the gate; they are not. v2 adds a case that does reach it (sections 9 and 10): timing
    `postgres.atomic_group_large.latency`, one guarded atomic group of 6,500
    single-event streams, so each chunked statement runs 7 times (scenario size to be checked
    against the group's admitted limits when the story is written). A/B of head-before vs
    head-after, median over 4 runs, ratio at most 1.05, is the D3 gate. Expected cost: one plan per
    further chunk, about 0.1 ms against a chunk's work (**inferred**, not measured).
  - The unit test `submillisecond_timeouts_keep_their_server_side_limits` (`pool.rs:1089-1106`) pins
    the exact configuration string and changes.
- **(v) a generic plan behind an optimisation fence**: rejected in 0.3.

**Recommendation: (iv).** It is the only option that keeps a prepared find value-aware, adds no
round trip and has a closed list of other effects. If the large-group A/B regresses, the choices
are the owner's (D3): accept the measured cost and document it, or the fallback of 0.5; (i) and
(iii) are not on the list.

Notes (F2):

- A host that sets `plan_cache_mode` on its role (`ALTER ROLE ... SET`) is overridden for this
  pool's sessions, including caller-owned connections. Acceptable: `session_configuration` already
  overrides `search_path`, `default_transaction_isolation`, `default_transaction_read_only`,
  `statement_timeout`, `lock_timeout` and `idle_in_transaction_session_timeout` on every pool
  session, caller-owned ones included (`pool.rs:116-125, 628-640`, **verified**). The
  `PostgresConfig::caller_owned` rustdoc lists the new setting beside them.
- `plan_cache_mode` exists since PostgreSQL 12 and the code supports 13 and later; no tolerance
  branch is needed (the `pool.rs:19-23` note concerns another setting; v1's remark is dropped).
- The safety rests on a session setting that `RESET ALL` or `DISCARD ALL` clears silently; the mode
  is then the role default and the statement may go generic after five uses. Correctness is
  unaffected, speed is. Documented; test 2 sees it only under `generic_plans`; no runtime
  detection (disposition table).

## 4. Hosted and caller-owned connections

- `AuthorizedConnection` carries one `tokio_postgres::Client` and its driver (`pool.rs:137-141`);
  the pool owns the driver and sets `search_path`, read-only, statement, lock and transaction
  timeouts "before a lease is returned" (`docs/design/caller-owned-postgres-connections.md`,
  Ownership item 4; code `pool.rs:116-127, 628-640`). A hosted pool checks each new session is the
  admitted database, role and schema (`pool.rs:638-649`, `HostedRole::admit` `pool.rs:92-107`).
- The adapter already requires a **dedicated session**: tables are unqualified (`lib.rs:2007`) and
  resolved through a `search_path` set once per physical session; the principal is checked once per
  new session. **verified.** No document names transaction-mode pooling as supported (search of
  `origin/main` for pgbouncer, pooler, "transaction pool": no hit outside lock-file noise;
  `prepared statement` appears only at `lib.rs:2741` and `projection-value-digest.md:20`).
  **verified.** Nothing detects a lost `SET` today.
- Premise corrected (F4): a session-mode pooler runs its reset when the client connection is
  released, not between statements of a connection the adapter holds for the pool's life. A
  `DISCARD ALL` between two finds happens only under transaction-mode or per-statement
  multiplexing, which already breaks schema resolution. After a real `DISCARD ALL`: statements die
  (26000, rule 3 of section 2: `refused`) and `plan_cache_mode` reverts (speed only). The change
  adds no correctness exposure beyond the name collision below.
- Name collision (F3, **verified** for the counter): `tokio-postgres` names statements from a
  process-global `NEXT_ID` (`prepare.rs:59, 66`): `s0`, `s1`, ... identical across processes and
  replicas. Behind a transaction-mode pooler two replicas can collide on a name; with equal
  parameter counts Bind then runs another client's statement and returns wrong rows, not an error.
  That topology is already unsupported, but this turns an error class into silent wrongness, so the
  rustdoc "each returned connection must be a dedicated session, not a transaction-mode pooled one"
  on `PostgresConnectionAuthority::connect` and `PostgresConfig::caller_owned` is mandatory, not
  optional. Managed connection proxies that pin a session on a named statement would pin; the
  rustdoc says so. A role needs no privilege beyond the statement's own to prepare it (**inferred**).
- Refusal on prepare (allowlist, F3): 0A000, 08P01, 42P05 (name exists on a multiplexed server
  session), 26000. On one of them: `refused = true` for this `Connection`, run the find through
  `query_typed` now and later, no retry of the prepare. Any other `prepare_typed` error (57014
  cancel or timeout, 53300, a closed connection) is returned as today through `backend()`, never
  swallowed, and disables nothing.
- Observable? Not in production by default: the three counts (prepared, evicted, refused) exist
  under `#[cfg(test)]` only, because a production counter nothing reads is the tech debt the owner
  excluded (F6). The KPI Parse counter (5.4) is the standing guard. See D2.

## 5. Skewed tenants: why a generic plan is wrong here, and the KPI that shows it

### 5.1 Mechanism (**inferred**, with the record at `projection-value-digest.md:20`)

- Statistics are per table and per column, not per tenant. A custom plan sees
  `idx_N_digest = <constant>`; the constant folds at plan time because `hashtextextended` is
  immutable (`schema.rs:1198-1205`), so the planner uses the digest column's most-common-value
  list. A generic plan sees `hashtextextended($2, 0)` with `$2` unknown and can only use
  `1 / n_distinct` for the digest and for `tenant_id = $1`.
- The planner multiplies the tenant and digest selectivities as if independent. **This broke v1's
  fixture** (F5.1): a `rare` value of 640 rows in 72,600 with the large tenant at 83 percent of
  the table gives a custom estimate of about 530 rows, not 10; for that estimate a primary-key
  walk with `LIMIT 10` is estimated cheaper than the digest index plus sort, so the custom plan
  itself could pick the 60,000-tuple walk, head's baseline would record it, and variants G and U
  would not differ. The case would be green on every variant.
- Under `auto`, the generic plan is adopted only when its estimated cost is not much above the
  average of the first five custom plans. A sequence of only rare values gives the cheapest custom
  costs, which is the least likely to flip (F5.2). So variant U may never flip on a skewed fixture;
  the 1,001-row fixture is where the repository saw it flip.
- The opposite failure (a hot value on a field with many distinct values, index plan chosen because
  the estimate says rare) comes from the same cause: a generic plan cannot see the value.
- Not fixable by an index in this design: even `(tenant_id, idx_N_digest, row_key)` leaves the
  primary-key walk as a competing path whose estimated cost can win under generic estimates.

### 5.2 The fixture, re-derived (F5.1-3), derived on CI

Shape (starting point, the numbers are fixed from the CI `EXPLAIN` output, not from this
arithmetic):

- Direct SQL through `pg::client` (`pg.rs`), not through the projector: create the projection
  through the store, then `INSERT INTO <table> (tenant_id, row_key, body, idx_0) SELECT ...
  generate_series` (four value columns only, so the same runner works on the 0.10.0 table and the
  head table; the generated digest column is filled by the server).
- **At most 30,000 rows**: `ANALYZE` reads every row up to 300 x the default statistics target and
  samples randomly above it (F5.3, **inferred**), so `n_distinct`, the MCV list and the plan would
  differ between runs. Starting numbers: one tenant of 19,000 rows and 20 tenants of 500 rows,
  29,000 rows. The default statistics target is kept (raising it would hide what a default
  installation does).
- `kind` is `open` or `closed` alternately; **`rare` is rare table-wide**: 12 rows in all, the last
  12 keys of the large tenant. With three distinct values all three sit in the MCV list, so the
  custom estimate is about 29,000 x 0.66 x 0.0004 = 8 rows (**inferred**), the digest index is
  cheap, and the generic estimate (1/21 for the tenant, 1/3 for the digest, about 460 rows) makes
  the walk look cheaper (**inferred**).
- v1's small-tenant half is dropped: with `rare` rare table-wide a find in a small tenant returns no
  row, and with `rare` common table-wide the custom plan walks too. The generic plan costs a small
  tenant a few hundred rows, a few times the work; it hides in a p50 over many tenants and adds
  nothing here.
- `VACUUM (ANALYZE) <table>` after seeding, not `ANALYZE` (F5.5): 29,000 freshly inserted rows
  would otherwise trigger an insert-based autovacuum inside the timed window on the 2-CPU
  service container. Autovacuum does not change `idx_tup_read` or `seq_tup_read`, so the counter is
  stable either way; the timing is not. (`autovacuum_enabled = false` on the table would also work;
  not taken, disposition table.)

Acceptance of the fixture, on CI (PostgreSQL 17.6), read from `EXPLAIN (ANALYZE, BUFFERS)` of a
prepared copy of the statement (F5.2: assert the mechanism, not a timing; deterministic, no
statistics flush; the repository already has the `EXPLAIN ANALYZE` counter pattern,
`postgres.feed_one_row.buffers`, `kpi/baseline.json:33`):

- **E1**: under `force_custom_plan` with `rare` in the large tenant: an index scan on the digest
  index, at most 50 tuples read in total.
- **E2**: under `force_generic_plan` (or `EXPLAIN (GENERIC_PLAN)` for the shape, PostgreSQL 16+):
  the primary-key walk, at least 100x E1's tuples (about 19,000).
- **E3**: under `auto`, six executions in one session with `rare` values: whether the sixth is
  generic (`pg_prepared_statements.generic_plans`, PostgreSQL 14+). Recorded, not required.
- If E1 or E2 fails the shape is changed (size of the large tenant, count of `rare`, number of
  distinct `kind` values) and the CI run repeated; the fixture is not accepted on arithmetic. The
  case runs E1 and E2 itself on every run and errors (not a regression verdict) when they do not
  hold, so the fixture proves each time that it can see the failure. The statement copy it uses
  follows the repository rule that a change to the library's statement changes the copy in the
  same commit (`AGENTS.md`, "The KPI gate"). The real-path counter below measures the product, not
  the copy.

### 5.3 KPI cases

- Timing `postgres.projection_find_skewed.latency`: finds of `rare` limit 10 in the large tenant,
  16 per round, through `projection_find` (loop beside `timing.rs`' `projection_find` block),
  interleaved A/B (`ab.rs:181-190`, order unchanged). **Only p50 is judged** (`ab.rs:189, 213`
  collect and compare medians of the per-round p50: F5.6, **verified**); no p95 id is defined.
  v1's second id (`..._skewed_large`) is merged into this one.
- Counter `postgres.projection_find_skewed.tuples_read` (kind `counter`, direction lower), per
  find, through the real store path (not a copy of the statement), computed from deltas of
  `pg_stat_user_tables.seq_tup_read` plus the sum of `pg_stat_user_indexes.idx_tup_read` over the
  table's indexes. Procedure (F5.4, F5.5):
  1. seed and `VACUUM (ANALYZE)` on a setup session; close the setup store;
  2. wait until no backend of the setup holds the database (`pg_stat_activity` by
     `application_name`, the pattern of `tests/retired_connection.rs:60-80`, `await_gone`); read
     `R0` once from a fresh session;
  3. open a store on the same place (reopening is supported, `timing.rs` reopens a place), run the
     16 finds, close it, wait until its backends are gone, read `R1` once;
  4. the counter is `(R1 - R0) / 16`.
  Backends flush their statistics at exit, so a read after exit is complete; v1's "poll until two
  reads agree" could agree on two pre-flush reads. No `pg_stat_reset_single_table_counters` (it
  resets only the relation, not its indexes, and needs a privilege a hosted role may lack).
  Good plan: about 12 (the `rare` rows) plus the registry row. Walk: about 19,000. Values are set
  from the run.
- Both ids run in the existing machinery: the counter in the always-on `KPI counters` job
  (`.github/workflows/kpi.yml`), the timing in the dispatched A/B.

### 5.4 Counters that guard the prepared path (F1)

- `postgres.projection_find.round_trips` (counter, tolerance 0, direction lower): the counting proxy
  over the `projected` fixture, two warm-up finds ignored (the first runs unprepared and records
  the text, the second prepares: section 1), then 8 finds; the value is the average x 100 rounded
  (100 = 1.00). The cold path is 100 for the first find and 200 for the one that prepares; the
  counter is defined for the warm state only. v2 said one warm-up find, which put the prepare
  inside the window (variant V1 read 113 and 1, CI run 45434221837); section 11 gives the reason
  for two and why the cache keeps its second-use rule.
- `postgres.projection_find.parses` (counter, tolerance 0, direction lower): the number of Parse
  messages in the same 8 warm finds. Expected 0. `Framing::feed` already splits frames and sees the
  type byte (`pg.rs`); it returns a second count for `P`. The existing unit test
  (`framing_counts_syncs_and_queries_across_arbitrary_splits`) gains a Parse assertion over all
  splits. Deterministic, no timing, same file as the round-trip counter. Baseline: 8 on head
  before the change (one Parse per find), **re-recorded to 0 in baseline step 3b**, a separate commit
  after the product commit (the repository's rule is that an improvement beyond tolerance is
  re-recorded with the change, `AGENTS.md` "The KPI gate"; the baseline file is written only by the
  CI record path, so the re-recording is its own commit, as in section 10). The guard exists from
  step 3b on; a refused or disabled cache
  returns the counter to 8 and the gate goes red.

### 5.5 Red proof before the change, CI only, never on the shared workstation

- Commit 1 holds the cases against head's unchanged adapter; they record the good-plan numbers.
- Variant G (deliberately bad, scratch branch, never merged): head's find through `prepare_typed` +
  `query(&Statement)` on every call from the first, and `SET plan_cache_mode = force_generic_plan`
  appended to `session_configuration`. Expected: E2's walk, `tuples_read` from about 12 to about
  19,000 per find, the timing p50 far above the 1.5 default.
- Variant U: the same cache with no `plan_cache_mode` line (`auto`). Expected to flip only if E3
  says so; if E3 shows no flip, U is not a red proof and is not claimed as one: G and E2 are the
  proof that the fixture sees the failure, and test 2 is the proof that the product never goes
  generic.
- Run: a draft pull request from each variant so `KPI counters` runs `gate.sh --kpi` against the
  committed baseline and must go red on `tuples_read`; and `workflow_dispatch` of `KPI / KPI timing`
  with `ref` = the feature branch, `base` = the commit-1 sha, `head` = the variant, `rounds=20`,
  which must print REGRESSED for the skewed id (`ab.rs:228`). The dispatched ref's runner measures
  both sides (`ab.rs:1-8`, **verified**).
- Run on 0.10.0: one A/A dispatch with `base` = `head` = the 0.10.0 tag proves the new scenarios
  run there at all; a scenario that errors on 0.10.0 would fail the whole `timings` run
  (`ab.rs`, `timings()` returns an error on a non-zero exit, **verified**).

Budgets: the cases run in the KPI lane (`gate.rs:12-14`), not the test step (60 s per test, 120 s
step, `lanes.rs:66-68`). Seeding 29,000 rows with `VACUUM (ANALYZE)`, 16 finds, the large group
and the reopen are not measured; they are timed before they are added (folder rule), and the
per-round time is read from the first dispatch (the timing job's limit is 60 minutes).

## 6. Tests

All tests that read session-local or crate-private state live in-crate, under `#[cfg(test)]`
(F6, **verified** reasoning): an external `tests/` binary cannot see another session's
`pg_prepared_statements`, `Pool` and `Lease` are `pub(crate)` (`pool.rs:471, 831`), and the
counters are `cfg(test)`. The pattern exists: `lib.rs:36-47` declares `#[cfg(test)] mod
projection_indexes;` and its siblings, `pool.rs:1089, 1198` hold in-crate DB tests with the
`EVENTLOG_TEST_POSTGRES_URL` skip, hooks at `pool.rs:479-487, 826`. Add `#[cfg(test)] mod
find_statements;` (a new in-crate module, no new integration binary) for the DB tests, and unit
tests inside `statement_cache.rs`.

Existing tests that change:

- `pool.rs:1089` `submillisecond_timeouts_keep_their_server_side_limits`: the expected string gains
  `SET plan_cache_mode = force_custom_plan`. Mutation: setting omitted -> fails.
- `projection_indexes.rs` tests (`:619, :960` and the other `projection_find` tests) keep their
  assertions and now also run through the cache on their second call.
- `crates/eventlog-kpi/src/pg.rs` `framing_counts_syncs_and_queries_across_arbitrary_splits`:
  extended for Parse counts.
- `kpi/kpis.json`, `kpi/baseline.json`, `crates/eventlog-kpi/src/{counter,timing,fixture,pg}.rs`:
  new ids, section 5; the baseline is written only by the CI record path (10).

New tests (each expected under 5 s and timed before it is added; no model call, no network beyond
the fixture server). Pure unit tests, no database:

- U1 `warm_up_then_prepare_then_use`: the state machine goes `Seen`, prepare, use. Mutation: prepare
  on first sight -> fails.
- U2 `the_65th_text_is_not_cached_and_nothing_is_evicted`. Mutation: eviction -> fails.
- U3 `refused_is_final_for_the_connection`: after `refused`, no prepare is ever asked again.

DB tests, in-crate:

1. `a_find_is_prepared_on_its_second_use_and_reused`: after find 1, `pg_prepared_statements` of the
   leased session holds 0 rows; after find 2, 1; after 20, still 1. Mutation: cache never stores ->
   0 after find 2; prepare every call -> the count grows.
2. `a_prepared_find_never_switches_to_a_generic_plan`: 30 finds over the 5.2 fixture, then
   `generic_plans` and `custom_plans` from `pg_prepared_statements` (PostgreSQL 14+) must read 0
   and **29** (finds 2..30 are the prepared executions; find 1 is unprepared; v1 said 28, F6), and
   `SHOW plan_cache_mode` must read `force_custom_plan`. Mutation: setting removed -> `SHOW` fails
   at once; the generic count catches it only if the planner flips on the fixture (E3).
3. `a_find_survives_ddl_that_changes_the_cached_result_type`: warm the cache, replace the table
   through a second connection with `body JSON` (result type changes, expected 0A000), find: rows
   correct, one stale-evict counted, entry gone, the following find back at `Seen`. Mutation: no
   eviction -> the following find errors; a loop retrying the cached statement -> the
   one-extra-execution assertion fails.
4. `a_find_after_a_dropped_digest_column_fails_once_and_the_same_way_as_unprepared`: drop the digest
   column, find: the error equals the unprepared path's (`Backend("... SQLSTATE 42703 ...")`), at
   most two executions of that statement, the connection retired. Mutation: error swallowed or
   retried in a loop.
5. `a_connection_retired_by_a_deadline_takes_its_statements_with_it`: time out a find (pattern of
   `tests/retired_connection.rs:152-248`, backends listed through a second session, `:73`), then
   find again: the replacement session shows 0 prepared statements, then 1 after its second use, no
   stale or refused count, the retired backend is gone. Mutation: cache kept on `Pool` -> the
   replacement reuses a name it never prepared, 26000 -> fails. (Statement names are distinct only
   within one process, `prepare.rs:59`; the test does not rely on more.)
6. `two_projections_with_the_same_layout_keep_their_own_statements`: two projections with field
   `kind` and different rows, finds alternating at least 3 times each, a second store with another
   prefix on the same server; each returns its own rows. Mutation: key by (position, variant) ->
   rows from the wrong table.
7. `a_session_that_refuses_named_statements_still_finds`, three cases: (a) a `cfg(test)` hook (the
   `fail_shutdown` pattern, `pool.rs:826`) makes `prepare_typed` return 42P05 once: correct rows,
   `refused`, later finds attempt no prepare (attempt count stays 1); (b) injected 57014 on prepare
   propagates as `Deadline` and disables nothing; (c) **no injection**: warm a statement, run
   `DEALLOCATE ALL` on the leased session, find: the Bind raises a real 26000, the find returns
   correct rows, `refused` is set, later finds prepare nothing (`pg_prepared_statements` stays 0).
   Mutations: no fallback -> the find errors; retry the prepare every time -> attempts grow;
   swallow-all -> (b) fails; evict-and-retry without `refused` on 26000 -> (c) sees a prepare on the
   next find (the v1 thrash).
8. `a_caller_owned_connection_gets_the_same_session_settings`: through `PostgresConfig::caller_owned`
   with the existing test authority (`pool.rs:1112-1148`), `SHOW plan_cache_mode` equals
   `force_custom_plan`. Mutation: setting applied only on the built-in path -> fails.
9. `a_store_with_more_than_64_find_texts_keeps_finding`: 65 distinct texts (projection x field x
   variant) on one connection; the 65th runs unprepared and returns correct rows; no `Close` churn
   (prepared count stays 64).

Test 7(b) and 3 force the codes the lists depend on; any code that does not appear on CI is removed
from its list before the list is final. PostgreSQL majors other than 17.6 stay **inferred**.

## 7. Public API

Default option changes nothing in the public surface:

- Unchanged: `PoolStatus` (`pool.rs:386-393`, public fields, not `#[non_exhaustive]`: a new field
  breaks struct-literal construction), `PoolOptions` (`pool.rs:211-220`, same reason), `PoolChurn`
  (`pool.rs:412-420`, `#[non_exhaustive]`, held to the ESS model `ess/pool/domains/pool.yaml` and
  its generated schema, and named churn: connection opens and retirements),
  `PostgresConnectionAuthority` (`pool.rs:169`), the errors (the same `EventLogError` values; a
  classified stale error is retried before it is reported).
- Changed behaviour visible to a host: each pooled session sets `plan_cache_mode =
  force_custom_plan` (overrides a role-level value); each connection may hold up to 64 named
  statements `s<N>` (visible in `pg_prepared_statements`, tens of KB each server-side).
- Docs to change: rustdoc of `PostgresConnectionAuthority::connect` and
  `PostgresConfig::caller_owned` (dedicated session, mandatory; the new setting beside the others;
  the 64-text cap); `docs/design/projection-value-digest.md` (line 20 and the Lookup section
  describe the unnamed statement; the framing of 0.3); `AGENTS.md` "The KPI gate" (new ids, the
  stat-based counter, the CI record path); `CHANGELOG.md` (the 0.3 wording).

Forks. Each has a default; the one that is not reversible or not covered by the default rule waits.

- **D1, public API.** A (default): no new public item. B: an opt-out switch for prepared finds on
  the config. B costs a public knob for ever and a path that runs unbenchmarked; no evidence asks
  for it. Reversible either way (additive later).
- **D2, fallback visibility (F3, F6).** A (default): not public; the refused, prepared and evicted
  counts exist under `#[cfg(test)]` only; a refused session loses the speed-up, results are
  identical to the unprepared path; the KPI Parse counter (5.4) guards the regression. B: a field on
  `PoolChurn` (`#[non_exhaustive]`, additive in Rust) with an ESS change in
  `ess/pool/domains/pool.yaml` and regenerated `ess/pool-generated`; it is not churn, the model
  stretches. C: a new `#[non_exhaustive]` struct and an accessor on `PostgresEventStore` beside
  `pool_churn` (`lib.rs:286`) with the three counts, public for ever. A is changed from v1 by the
  rule that a production counter nothing reads is debt (F6).
- **D3, plan-mode scope.** A (default): session-wide `force_custom_plan`, gated on the
  `postgres.atomic_group_large.latency` A/B of 3 (head-after within 1.05 of head-before). The
  attribution removed v1's B ("shape-only, chosen automatically"), so what remains if the A/B fails
  is: the owner accepts the measured cost, or the fallback of 0.5 (D5). (i) and (iii) are not
  options. Also decides what happens if G2 fails (0.4).
- **D4, where the bound lives.** A (default): the acceptance is the dispatched 20-round runs; the
  PR timing job stays informational; `ab_max_ratio` stays null (the 1.5 default). The deterministic
  guard is now the Parse and round-trip counters, which v1 lacked. B: `ab_max_ratio: 1.05` on
  `postgres.projection_find.latency`, which makes a 5-round PR job fail on the noise the procedure
  already shows (1.049-1.074, spread 2.5 percent) and so teaches reviewers to ignore it. Reversible.
- **D5, the fallback if G1 fails (new, owner's, not taken).** FA, FB or FA+FB of 0.5, each with its
  cost there. FA changes the admission (UTF-8 only) and adds a contract between a Rust function and
  a server function; FB changes an edition, the ledger and the order of `projection_find` on a
  non-C database. Both are architecture and on-disk questions: not covered by "a default proceeds";
  no default, work not blocked by it continues (the story, the guards of 5.4 that do not depend on
  a prepared statement, the attribution record).

## 8. Risks I could not close, and what the adversary said

### 8.1 Unclosed

1. The end-to-end saving of "no parse, no analysis" is not measured; the spike decides it (G1). The
   need is about 7-14 us per find (0.2, **inferred**).
2. The skewed fixture's E1-E3 are not run. The numbers of 5.2 are starting points.
3. Session-wide `force_custom_plan` on operations over 5,000 items is not measured; the large-group
   A/B decides it.
4. The CI record path (10, step 3) is new workflow and runner code; it has to be proven on CI.
5. SQLSTATE behaviour (0A000 text, 26000 at Bind, 42P05) is from PostgreSQL behaviour, not a fetched
   document; tests 3 and 7 force each code on 17.6 before the lists are final. Other majors
   **inferred**.
6. `pg_prepared_statements.generic_plans` needs PostgreSQL 14; the code supports 13. Test 2's count
   assertion skips on 13 and relies on `SHOW`.
7. The transaction-scoped `ProjectionStore::find` (`lib.rs:1932`) still prepares and closes per call
   (three Syncs); out of scope; named in the story text because a caller that finds inside an append
   pays 3x and the bound does not cover it (F7).
8. A 4-connection pool warms in at most 8 finds; the round-trip and Parse counters are defined for
   the warm state only.
9. `RESET ALL` or `DISCARD ALL` silently reverts the plan mode (3, F2).
10. New KPI scenarios must run on 0.10.0 (5.5, A/A on the tag).
11. The 64-text cap is a silent cliff in production.

### 8.2 Disposition of the adversary report (F1-F7, every finding)

| Finding | Taken | Where |
|---|---|---|
| F1 framing, PR and CHANGELOG wording | yes | 0.3 |
| F1 Parse counter | yes | 5.4 |
| F1 pgbench split before V1 | done as CI run 45365255371 (attribution) and folded in | 0.2 |
| F1 fenced generic plan named and rejected | yes | 0.3 |
| F2 batch KPIs cannot see the effect; case over 6,000 items | yes (timing case) | 3, 9, 10 |
| F2 in-crate test comparing `generic_plans` instead | no: the chunked statements are prepared inside functions, their handles are not reachable after return, so `generic_plans` cannot be read; the timing A/B is the evidence | 3 |
| F2 override of role setting acceptable, document it; drop the `pool.rs:19-23` remark | yes | 3, 7 |
| F2 `RESET ALL` fragility | documented, no detection (a detector would be a production check for a speed-only effect) | 3 |
| F3 Bind-time 26000/08P01 sets `refused` | yes | 2, 4, 6 (7c) |
| F3 retry per statement, up to four executions per find | yes | 2 |
| F3 name collision across processes, rustdoc mandatory | yes (rustdoc only; the driver gives no way to name statements) | 4, 7 |
| F4 premise of attack question 4 weaker; hosted pin note | yes | 4 |
| F5.1-3 fixture re-derived, at most 30,000 rows, EXPLAIN on CI | yes | 5.2 |
| F5.3 alternative `SET STATISTICS 1000` | no: it hides what a default installation does | 5.2 |
| F5.4 read after backend exit, deltas, no single-table reset | yes | 5.3 |
| F5.5 `VACUUM (ANALYZE)` | yes | 5.2 |
| F5.5 alternative `autovacuum_enabled = false` | no: one mechanism is enough and `VACUUM (ANALYZE)` leaves the table as a host has it | 5.2 |
| F5.6 only p50 gated | yes, p95 ids dropped | 5.3 |
| F5.7 decide the CI record path | yes | 10 step 3 |
| F6 tests in-crate, `StatementCache`, `cfg(test)` counters, 29 not 28 | yes | 1, 6, 9 |
| F6 time the new tests and seeding | yes, as a rule | 5.5, 6 |
| F7 cap cliff: count it or LRU | partly: documented and tested (test 9); no production counter (debt) and no LRU (a `Close` lands on an unrelated next operation and adds a counted round trip) | 1 |
| F7 cancel-on-drop, memory, concurrency, FIPS, collision rerun slot | yes, as notes; FIPS is not a finding for the prepared path | 1, 2 |
| F7 transaction-scoped find named in the story | yes | 2, 8.1 |

### 8.3 Questions for the next review

1. If the spike's ratio passes G1 only provisionally (pgbench), does the V1 KPI dispatch contradict it?
2. Does any statement of the adapter other than the six chunked ones change plan under
   `force_custom_plan`, and does the large-group A/B cover the slowest (the head-lock statement)?
3. Can the Parse counter pass while the cache is refused (a refused connection that never reaches
   the warm state in the counter's 8 finds)? The counter's two warm-up finds, the second of which
   prepares, come before the 8, so a refused connection reads 8 parses and the gate goes red.

## 9. Cost

Files and approximate lines (default options, **inferred**):

| File | Change | Lines |
|---|---|---|
| `crates/eventlog-postgres/src/statement_cache.rs` (new, private) | the state machine and its unit tests U1-U3 | +140 |
| `crates/eventlog-postgres/src/pool.rs` | `Connection` field, session setting, `cfg(test)` counters and hook | +60 |
| `crates/eventlog-postgres/src/lib.rs` | `projection_find` through one helper that picks cached or `query_typed`, stale classification before `backend()`, `mod find_statements` | +80, -25 |
| `crates/eventlog-postgres/src/find_statements.rs` (new, `#[cfg(test)]`) | DB tests 1-9 | +320 |
| `crates/eventlog-kpi/src/{counter,timing,fixture,pg,main}.rs` | skewed fixture and E1/E2 check, large group, stat reader, Parse counter, `record --counters` | +300 |
| `.github/workflows/kpi.yml` | a `record` dispatch input and an artifact upload step | +30 |
| `kpi/kpis.json`, `kpi/baseline.json` | 5 ids (skewed timing, skewed counter, round trips, parses, large group) | +20 |
| docs, rustdoc, `CHANGELOG.md`, `AGENTS.md` | | +70 |
| ESS | none (D2 A) | 0 |

About 1,000 lines added. No schema, ledger or migration change; no new dependency; `unsafe` not
needed. All tests are DB or unit tests under 5 s each, no model call, no network beyond the
fixture server; the step budget (60 s per test, 120 s step, ten slowest printed) is the folder's.

## 10. Acceptance and rollout

All proof on CI. No KPI round, `pgbench`, `ab`, `timings` or `record` on the shared workstation.

| # | Step | CI-only proof |
|---|---|---|
| 1 | **Spike result.** Read the spike, CI run 45368249583 on branch `ci/plan-prepared-spike` (open when this section was written; its numbers are now **reported** in the product story: P1/E0 0.895 at pgbench level, so G1 is provisional) (prepared + `force_custom_plan` vs extended end to end; the skewed fixture under `auto` and `force_generic_plan`). Decide G1 and G2 (0.4). The spike is pgbench-level, so G1 is provisional until V1 (step 4b). | The run URLs and the numbers in the story. G1 pass: median at most 1.03 (V1, step 4b). G1 fail: D5 goes to the owner and only steps 5 to 7 (the product change, its A/B and the release) do not start; the kit (steps 3 and 4) stays valid. |
| 2 | **Stories.** Two, split at the gate: `story:projection-find-measurement-kit` (steps 3 and 4, baseline step 1b; valid whether G1 passes or fails) and `story:projection-find-holds-its-bound-with-a-prepared-statement` (steps 5 to 7, baseline step 3b; depends on the kit; blocked by the D5 decision-blocker only). One observable per acceptance line, the 0.3 wording, the transaction-scoped find named as out of scope. Plan critics approve them, implementation starts as soon as they do. | The story artifacts in `.engineering/planning/` validated; critic results recorded. |
| 3 | **Commit 1: KPI cases and the CI record path, against unchanged head.** The skewed fixture with E1/E2 asserted, `tuples_read`, `round_trips`, `parses` (8), `postgres.atomic_group_large.latency`, `record --counters` (counters only: it rewrites no timing of the recording machine, F5.7) and a `record` dispatch input that runs it on the CI runner and uploads `kpi/baseline.json`; a human commits the file (the workflow keeps `contents: read`). | `KPI counters` green on the branch after baseline step 1b, a separate commit of the uploaded `kpi/baseline.json` (commit 1 itself is red `UNRECORDED` by design: `compare.rs:161`, `:21`); E1/E2 pass; one A/A on 0.10.0 passes (new scenarios run there); one A/A head vs head gives the noise floor of the new timings. |
| 4 | **Red-capable proof.** Variants G and U (V1 is step 4b) as scratch branches, draft pull requests, never merged (5.5); variant U's cache is a throwaway on its scratch branch. | G: `KPI counters` red on `tuples_read`, dispatched A/B REGRESSED on the skewed id. U: the result recorded and worded as 5.5 says. The run URLs go into the pull request. |
| 4b | **Build gate V1.** The third throwaway of the family on a scratch branch, never merged: head's find prepared per connection under `force_custom_plan`, nothing else. Dispatch 4 times (`base` = 0.10.0 tag, `head` = V1, `rounds=20`). Owned by the kit story. | Median over the 4 runs of the ratio of `postgres.projection_find.latency` p50 at most 1.03; the run URLs go into the pull request. Fail: D5 to the owner, steps 5 to 7 do not start. |
| 5 | **Commit 3: the implementation** (sections 1-4, tests 1-9, U1-U3, docs). Re-record `parses` to 0 through the CI record path as baseline step 3b, a separate commit after the product commit. | In-crate tests green in the nextest step (each under 60 s, step under 120 s, ten slowest printed); `KPI counters`: `parses` 0, `round_trips` 100, `tuples_read` at commit 1's value. |
| 6 | **Acceptance A/B.** 8 `workflow_dispatch` runs of `KPI / KPI timing` in two groups of 4, `head` = the product commit, `rounds` = 20: group A with `base` = 0.10.0 tag, group B with `base` = commit 1 (the kit commit; the product/kit ratio of `postgres.projection_find.latency` at most 0.95 so the prepared statement is what moves the number, and the ids below). | Median over the 4 runs of the head/0.10.0 ratio of `postgres.projection_find.latency` p50 at most **1.05**; the skewed id and `postgres.atomic_group_large.latency` against commit 1 (`base` = commit 1) within 1.05 (the D3 gate), and `postgres.batch_append.throughput`, `postgres.group_blobs.latency`, `postgres.read_stream.latency`, `postgres.read_feed_page.latency` not REGRESSED against commit 1. Fail: the bound is not touched; back to D5. |
| 7 | **Release 0.11.0.** CHANGELOG under the 0.3 wording with the measured ratio filled in; pull request body names the +7 to +10 us planning that stays. | Required checks green, the run URLs of steps 4 and 6 in the pull request. The tag is a publication: it waits for the owner's word, not taken on default. |

Order of commits on the branch: (1) KPI cases and record path against unchanged head, red
`UNRECORDED` by design; (1b) the recorded baseline, a separate commit of the file the `record`
dispatch uploaded, after which commit 1 is shown green; (2) the variants G, U and V1 (steps 4 and
4b), on scratch branches; (3) the product change; (3b) the baseline re-recorded for `parses`, a
separate commit. Commit 1 is red `UNRECORDED` until 1b. The variants G, U and V1 are red or
throwaway by design on scratch branches and are never merged (G is red on `KPI counters` and on the
A/B by design). Every other commit on the branch builds and gates on its own. Commits 1 and 1b and
the variants belong to the kit story, 3 and 3b to the product story.

Every implementation brief for this work names: the 60 s per-test bound and the 120 s test-step
budget; the 10-minute job limit (`slot --timeout` up to 30 minutes for a job that needs it); no new
Python in anything that stays in the repository (the record path and CI steps are Rust or POSIX
sh); scripts that measure or wait start without the `slot` prefix; and that nothing runs on the
shared workstation for timing.

## 11. Build notes (v3)

What the code does where this document left a choice, and the one statement of v2 it changes.

- **Warm-up of the protocol counters: two finds, the cache keeps its second-use rule.** V1 showed
  that one warm-up find is not enough: the second find prepares, so it ran inside the 8-find
  window and the counters read `round_trips` 113 and `parses` 1. Two ways to fix it. (a) Prepare on
  first use. Rejected: it adds a prepare round trip to the first find on every connection, and a
  connection that serves one find (a short-lived process, a pool connection that sees one find)
  pays 2 round trips where 0.10.0 pays 1; the only gain is a counter that needs one fewer
  warm-up find. (b) Warm the connection before the window with the number of finds that reaches
  the warm state, which is what "defined for the warm state only" (5.4) already says. Taken:
  `WARMUP_FINDS = 2` in `crates/eventlog-kpi/src/counter.rs`. The counter is not weakened: the
  window still holds 8 finds, each must be one round trip and no parse, so a refused cache, an
  evict loop or a cache that never prepares reads 8 parses and fails the gate. On a library that
  prepares nothing the two extra warm-up finds change no count.
- **The sequence of one find.** `find_rows` in `lib.rs` asks the connection's cache what to do
  with the statement text (`Step`), runs it, and classifies a failed prepared execution on the
  SQLSTATE before `backend()` flattens it: 0A000, 42703 and 42P01 forget the statement
  (`stale`), 26000 and 08P01 refuse the connection, anything else is returned as the unprepared
  path returns it. A failed prepare refuses the connection only on 0A000, 08P01, 42P05 and 26000;
  57014, 55P03, 53300, a closed connection and a missing table are returned and refuse nothing.
  The classification is two pure functions in `statement_cache.rs`, tested without a database.
- **The cache is generic over its statement type.** `StatementCache<S>` holds an
  `Entry<S>` per text; the pool instantiates it with `tokio_postgres::Statement`, and the unit
  tests U1-U3 use an integer, because a `Statement` cannot be built without a connection.
- **Capacity.** At most 64 remembered texts per connection. A prepared statement is never evicted: a
  text that arrives when all 64 are prepared is not recorded, runs unprepared every time and
  closes nothing (test 9). A text seen once holds no server state, so a new text replaces one of
  those when the cache is full; the adversary read of the build found that counting them against
  the cap let 64 one-off texts keep every later hot text from ever being prepared.
- **Refusal clears the cache.** `refuse` drops every entry; the dropped statements send their
  `Close` on the connection's next operation, which is a no-op for a session that lost them.
- **Test seam for a code the server does not raise on demand.** SQLSTATE 42P05 at prepare cannot
  be provoked on a session the driver names statements for, and `tokio_postgres::Error` cannot be
  built outside the driver. A `cfg(test)` hook on the pool (`fail_next_prepare`) makes the next
  prepare fail as if the server had answered the code; the classification and the refusal then run
  as for a real error. Every other case of test 7 is real: a prepare that waits for a lock past
  `lock_timeout` (55P03, a deadline) and `DEALLOCATE ALL` (26000 at Bind).
- **Counts exist only under `cfg(test)`** (D2 A): prepare attempts, prepared, evicted, refused.
