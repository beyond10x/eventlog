---
format: aep.planning-md/3
id: story:tree-publication-counter-and-reopen-contract
kind: story
status: draft
title: A Tree append skips the group listing on an unchanged publication counter, with a reopen contract
summary: Review L17 = C; one append lists all 64 group files under the writer lock
owner: eventlog
revision: 1
---
## Outcome

A Tree append skips the listing of every group file while a publication counter, bumped by every
write, is unchanged. A change from an outside checkout (git pull or merge while a handle is open)
then needs a reopen or an explicit `reload()`, and that contract is documented.

## Finding

Review L17, eventlog 0.9.4 (`b8582219`), `round3/eventlog.txt:115` in the review folder: a Tree
append enumerates earlier group files under the writer lock. Before appending, Tree walks all
tenants' group directories and compares every file name with its remembered set. One new event
enumerated all 64 existing group files while the writer lock was held; an idle single-writer store
pays this too. Source: `crates/eventlog-tree/src/lib.rs:281,499`,
`crates/eventlog-tree/src/history.rs:409`; measurement `provider-pass-two/dirs.txt:2`.

## Decision

Assessed by agent el-a. No cheap signal catches files git adds to a nested directory while a handle
is open (adding a file changes only that leaf directory's mtime). Options it put:

- A: a publication counter every write bumps; skip the listing while it is unchanged. Outside
  checkout changes then need a reopen or a new explicit `reload()`. Measured win, contract change:
  today writes notice such changes, and missing one forks the stream silently.
- B: compare directory timestamps. Bounded by directories, no gain at 64 groups.
- C (default): keep today's listing and decide A later.

Timo, 2026-11-23 about 01:20 CET via `host-operator`: L17 = C, plus this draft story (publication
counter and reopen contract). The code stays as built.

## Acceptance

On a Tree store with 64 group files, one append on a handle that saw no outside change lists no
group directory (counted, as in `provider-pass-two/dirs.txt`), and an append after a group file was
added by an outside checkout is refused with an error that names the reopen, never appended on a
forked stream.
