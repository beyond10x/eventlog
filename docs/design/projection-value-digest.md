# PostgreSQL projection lookups through a stored value digest

Owner: story:pg-projection-find-back-within-1-05. Option B of that story, decided on Timo's rule
"pick the most correct way here, even if it delays some things, we dont build workarounds here".
This design replaces the index expression the unreleased edition of pull request 72 builds; it
does not change the event log, a blob, a snapshot or any other owner table.

## Required result

- `projection_find` and a guard's `find` return exactly the rows `idx_N = value`, ordered by
  `row_key`, at most `limit` of them, for a value of any length (no 2,048-byte refusal, no
  B-tree entry limit).
- Every row returned holds the value asked for: a digest only selects candidates.
- The digest is computed by a function a PostgreSQL server in FIPS mode allows (no `md5()`).
- `postgres.projection_find` p50 within 1.05x of Eventlog 0.10.0 on `eventlog-kpi ab`.

Measured before this design (pull request 72, story evidence): 0.10.0 311 us, the
`hashtextextended(idx_N, 0)` expression index 369 us (1.19x) through the store; at the database
the expression and a stored column are about 2 us apart, and the value comparison in the `WHERE`
clause costs about 14 % on its own. A cached prepared statement is slower (its generic plan walks
the primary key). A digest computed by the client misses rows on a database whose encoding is not
UTF-8, so the server computes it.

## Physical shape

A projection table with declared fields `0..n` has, in this order:

```sql
tenant_id TEXT NOT NULL,
row_key   TEXT NOT NULL,
body      JSONB NOT NULL,
idx_0 TEXT, ..., idx_{n-1} TEXT,
idx_0_digest BIGINT GENERATED ALWAYS AS (hashtextextended(idx_0, 0)) STORED,
...
idx_{n-1}_digest BIGINT GENERATED ALWAYS AS (hashtextextended(idx_{n-1}, 0)) STORED,
PRIMARY KEY (tenant_id, row_key)
```

and the secondary indexes:

- `<table>_key_c` on `(tenant_id, row_key COLLATE "C")`, unchanged (the bytewise page index);
- `<table>_idx_N` (the bounded name `projection_index` already gives) on
  `(tenant_id, idx_N_digest)` for each declared field.

The digest columns follow every value column, so a table `migrate` extends with
`ALTER TABLE ... ADD COLUMN` has the same column order as a table created fresh. A NULL value has
a NULL digest and is never found, as before (a lookup value is never NULL).

`hashtextextended` is PostgreSQL's own 64-bit text hash, the one hash partitioning stores:
immutable, as a generated column requires, stable across releases for that reason, and it runs
no OpenSSL digest. Admission requires a deterministic collation on every column, under which it
hashes the stored bytes.

### Why a generated column and not one the kit writes

A plain `idx_N_digest` column filled by the upsert would be one more value a writer can get wrong.
A row whose stored digest does not match its value is never a candidate, and no candidate check
can find a row that was never returned: that is a silent miss. `GENERATED ALWAYS ... STORED`
makes the server compute it from the stored value on every insert and update; PostgreSQL refuses
an explicit value for it in `INSERT` and `UPDATE`, from the application role and from anyone
else. The upsert keeps listing only `tenant_id, row_key, body, idx_N`.

## Lookup

```sql
SELECT body, idx_N FROM <table>
WHERE tenant_id = $1 AND idx_N_digest = hashtextextended($2::text, 0)
  [AND NOT EXISTS (registry row with another declaration)]   -- projection_find only
ORDER BY row_key LIMIT $3
```

The statement is sent unnamed with typed parameters, as now, so it is planned with the
parameter values: `hashtextextended($2::text, 0)` folds to a constant and the planner sees a
plain `bigint` equality on an index column, with no expression to match.

The candidate check runs in Rust: each returned row's `idx_N` is compared with the value asked
for. The client receives text in UTF-8; the server's conversion into UTF-8 is one-to-one, and a
deterministic collation makes `=` byte equality, so the Rust comparison keeps exactly the rows
the server's `idx_N = $2` keeps.

- Every candidate holds the value: the rows are the answer.
- A candidate holds another value (a digest collision): the answer may be short, because the
  other row took one of the `LIMIT` places. The find then runs once more with `AND idx_N = $2`
  added to the `WHERE` clause and returns that statement's rows, all of them checked again. The
  answer is always the result of one statement over one snapshot; two statements' rows are never
  merged.

A 64-bit collision between two values of one tenant's field is rare by chance (about n^2 / 2^65
for n distinct values) but can be built deliberately (about 2^32 hash evaluations; the test pair
was found that way). The second statement is therefore part of the contract, not an
optimisation, and it costs one more round trip only on a find that met a collision.

`ProjectionStore::find` inside an append's transaction (a guard, a projector) uses the same two
statements without the registry clause.

## Writes and copies

- The upsert is unchanged: the generated columns are recomputed by the server.
- A rebuild (catch-up and inline) folds into a `LIKE <active> INCLUDING ALL` temporary table,
  which copies the generation expressions, and then copied rows with
  `INSERT INTO <active> SELECT * FROM <shadow>`. A generated column refuses a copied value, so the
  copy names the value columns, `tenant_id, row_key, body, idx_0, ...`, on both sides.
- Erasure deletes rows and is unchanged.

## Admission

Owner-table admission (`schema::shapes`) refuses any generated column today, in one clause with
triggers, policies and identity columns. That refusal stays for every owner table. A projection
table is admitted only when its shape equals the shape of a temporary table built from the same
declaration (`compare_projection`); that comparison admits a generated column, and each
column's description gains its `attgenerated` kind beside the expression it already carries. A
plain column with a `DEFAULT hashtextextended(...)` therefore differs from the generated column
and is refused, as is a generated column with another expression, another type or another
position.

Read-only capture admission (`capture::admit_projection`) compares the catalog with
`schema::projection_columns`, which now lists the digest columns with their generation
expression as `pg_get_expr` prints it (`hashtextextended(idx_N, (0)::bigint)`). It accepts a
generated column only where the list has one, with that expression and kind `s`, and refuses an
identity column, a default, or a generated column anywhere else. The DDL, both admissions and the
ledger checksum read the one list, so none can change alone.

## Edition, fence and checksum

The ledger checksum covers the owner DDL and `projection_edition()`, which now holds the column
declarations of a one-field projection table as well as its index columns. This is a new
checksum. `migrate` accepts the ledger of each predecessor that differs from it only in its
projection tables:

| Edition | Projection tables | Ledger |
|---|---|---|
| 0.10.0 (released) | `(tenant_id, idx_N)` value index, no page index | `BATCH_CHECKSUM` |
| `main` after pull request 70 | MD5 expression index and page index | `MD5_DIGEST_CHECKSUM` |
| pull request 72 | `hashtextextended(idx_N, 0)` expression index and page index | `HASH_EXPRESSION_CHECKSUM` (new constant) |

and the older editions it accepted before (blob integrity and earlier), which reach the same
projection upgrade. Every binary of those editions requires its own checksum at `open` and at
`migrate`, so after this `migrate` commits they refuse the schema with "unknown PostgreSQL schema
version/checksum": the cutover is fenced. This release's `open` refuses each of those ledgers
until `migrate` runs. A test pins the new constant to the checksum pull request 72's code
computes.

## Migrate

Inside the one migration transaction, for every projection table in the registry (the ones the
call names and the ones it does not) and then for every projection the call registers:

1. Drop each retired index the table has, matched by definition: the value index
   `(tenant_id, idx_N)`, the MD5 expression index and the `hashtextextended` expression index.
2. Add every missing `idx_N_digest` column in one `ALTER TABLE` (one rewrite of the table).
3. Create every missing index of `projection_indexes`, under its bounded name.
4. Compare the table with the declared shape; a difference refuses the whole migration.

The rewrite holds an `ACCESS EXCLUSIVE` lock on the table until the migration commits, and its
time grows with the table. Operators: stop every binary of the earlier edition for the owner, run
`PostgresEventStore::migrate` with the migration role, then start this release. No new grants:
the application role keeps `SELECT, INSERT, UPDATE, DELETE` on the projection tables.

## Tests and mutations

Each mutation must fail a test:

| Mutation | Test that fails |
|---|---|
| The Rust candidate check removed (every candidate returned) | `values_sharing_a_digest_are_told_apart_by_the_candidate_check`, in `projection_find` and a guard's `find` |
| The second statement skipped (filtered first page returned) | a collision test whose colliding rows sort before the asked rows and fill the `LIMIT` |
| The digest column missing from the DDL (index on the expression again) | the fresh-table shape assertion and the edition tests |
| A `DEFAULT` column admitted in place of the generated one | a capture and a projection admission test |
| `migrate` skipping a registered table it was not given | the three edition tests (0.10.0, MD5, pull request 72) |
| `migrate` keeping the expression index of pull request 72 | the pull request 72 edition test |
| The rebuild copy back to `SELECT *` | the existing rebuild and inline rebuild conformance cases |

The 8,000-byte value is written and found after each edition's migration; the FIPS test asserts
the DDL and the lookup call no function but `hashtextextended`.

## Measurement and stop rule

A find bench through the store (the KPI shape: 1,001 rows, 10 matching, 1,000 finds per round)
and `eventlog-kpi ab --base 0.10.0`, at least 6 interleaved rounds, with the load average stated.
A result above 1.08x of 0.10.0 stops the work after measuring: no further change ships on this
story without a new decision.
