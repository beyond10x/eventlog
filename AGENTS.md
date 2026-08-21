# Working on daemonloom/eventlog

This component is the shared persistence kit. The root [`AGENTS.md`](../../AGENTS.md) applies
throughout; this file adds component rules. Read `README.md`, then
[RFC 0020](../../architecture/rfcs/0020-state-is-a-fold-over-an-event-log.md), before changing
anything here.

## What this component may hold

- No domain type, no product concept, no policy. Every component may build-depend on these crates,
  which is only safe while that stays true.
- No third backend. In-memory is SQLite `:memory:`; a hand-written one is the defect class recorded
  in [M-022](../../model/modules/docs/stories/M-022-one-replay-rule-behind-one-port.md).
- No crate named `common`, `shared`, `utils`, `misc`, or `helpers`.

## Rules that are not negotiable in review

- **The conformance exercise is the definition of correct behaviour.** A backend change that needs
  an exercise change is a design change; say so in the commit rather than editing the assertion.
- **A new test must fail without the fix.** The watermark test earns its place by failing when
  `WATERMARK` is replaced with `true`; every regression test here is held to that.
- **DDL changes are additive only.** A kit release that changes a column is a migration in every
  owner at once, so a kit major version never forces one.
- **`redact` is the only `UPDATE` this kit issues against an events table**, and it deletes the
  snapshots at or after the redacted version in the same transaction.
- **No payload bytes and no free personal text in the log.** Blobs are content-addressed elsewhere
  and referenced by digest; identities are opaque ids resolved through the identity directory.

## The watermark couples feed latency across owners

A feed reader stops at `pg_snapshot_xmin(pg_current_snapshot())`, and that snapshot is
cluster-wide. A long-running transaction belonging to *any* owner in the same PostgreSQL instance
holds every catch-up projection in that instance still, including ones in unrelated schemas.
Nothing is skipped and inline projections are unaffected, but do not treat a shared instance as
isolation for feed latency. Measured on PostgreSQL 17.6 while implementing EL-003; the conformance
exercise drains with a bounded retry for exactly this reason.

## Validation

Run the gate in `README.md` from this directory. The PostgreSQL exercise needs
`EVENTLOG_TEST_POSTGRES_URL`; without it, it reports itself as not run rather than passing quietly.
