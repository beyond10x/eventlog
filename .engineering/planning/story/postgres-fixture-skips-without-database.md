---
format: aep.planning-md/1
id: story:postgres-fixture-skips-without-database
kind: story
status: draft
title: The PostgreSQL group-migration test skips without EVENTLOG_TEST_POSTGRES_URL
summary: crates/eventlog-postgres/src/schema.rs:694 expects the fixture and panics without a database, so scripts/gate.sh exits 1 on a machine without PostgreSQL while README.md:51-52 promises a skip. Observed 2026-09-15 at 77cda08.
revision: 1
---
# The PostgreSQL group-migration test skips without EVENTLOG_TEST_POSTGRES_URL
