---
format: aep.planning-md/1
id: story:resumed-snapshot-clearing-has-no-reachable-cause
kind: story
status: draft
title: The resumed path clears snapshots for a cause the crate cannot produce
summary: lib.rs:548 survives deletion against 82 cases; redact mints a new epoch, so the branch is never consulted
owner: eventlog
relations:
- informed_by: review-result:verify-once-review-2
revision: 1
---
## Outcome

`crates/eventlog-file/src/lib.rs:548` either has a reachable cause and a case that reaches it, or
it goes. Today it has neither.

## Why

Independent pass 2 over 9f234c5 (review-result:verify-once-review-2), read from the source and
confirmed by mutation M10: deleting `if advanced { tx.clear_snapshots()?; }` leaves every one of
the unit's 65 cases green, and leaves the pass's own 17 green as well.

The branch is justified in the code by "another writer's frames can retire a generation this handle
still has cached". The reviewer traced it:

- the only `Op::Generation` in the crate that **replaces** an existing generation is recorded by
  `redact` (`lib.rs:1033`), which sets `tx.privacy = true` two lines later and mints a new epoch;
- a new epoch fails `Journal::resume`'s epoch clause, so the handle goes to the complete opener,
  which calls `clear_snapshots` itself — before `advanced` is ever consulted;
- `Transaction::generation` (`lib.rs:366`) mints a generation only for a stream that has none,
  which retires no cached snapshot.

So the stated cause cannot arise on the resumed path. The branch is either dead, or it is live for
a reason nobody has written down — and a guard nobody can reach is indistinguishable from one that
does not work.

## Acceptance

- Either a case reaches the branch — a store state, reachable through the public API, in which a
  resumed handle would serve a retired generation without it — and the case is red with the branch
  deleted; or the branch is removed and the suite stays green, with the removal named in the
  CHANGELOG.
- Whichever way it goes, the comment at `lib.rs:548` states what is true afterwards.
- No other snapshot-clearing path changes. `clear_snapshots` on the complete opener and after a
  commit stay as they are.
