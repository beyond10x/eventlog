---
format: aep.planning-md/3
id: story:hosted-open-counts-extra-connections
kind: story
status: draft
title: A hosted open admits a role limit with the caller's extra connections
revision: 1
---
## Outcome

A hosted PostgreSQL open can declare connections the caller holds under the serving role beside
the pools (for example one session that holds advisory locks), and the role's connection limit is
admitted when it is at most `replicas * max_connections + extra`, where it is refused above
`replicas * max_connections` today (`crates/eventlog-postgres/src/lib.rs`, the
`rolconnlimit` check of the hosted open).

## Why

A consumer that runs several processes of one component on one store holds one more session per
process under the serving role, for a lease (advisory lock) that fences a stale writer. With the
role limited to exactly the pools' share, that session is refused; with the limit raised by one,
the hosted open refuses the role. The consumer therefore takes the lease only when its operator
asks for it; this story lets it take the lease by default.

## Acceptance

- A hosted open declaring 1 extra connection admits a role whose limit is the pools' share plus 1,
  and still refuses a limit above that.
- A hosted open declaring none behaves as today.
