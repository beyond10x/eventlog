---
title: Release highlights
---

## 0.3.0 — September 22, 2026

- File joins SQLite and PostgreSQL as a durable provider.
- Native atomic blob/event publication is available across all three providers. File additionally
  supports grouped blob binding under one durability barrier.
- Existing-only open, strict File/SQLite inspection, complete tenant capture and inline projection
  administration make provisioning and observation boundaries explicit.
- File reuses verified history while checking the committed prefix for damage. Deferred capture
  verifies content when read.
- SQL blob reads verify integrity; differing bytes under an existing binding refuse.

Read the [authoritative 0.3.0 release notes](https://github.com/beyond10x/eventlog/releases/tag/0.3.0)
for exact changes and the File method-name collision. The
[full changelog](https://github.com/beyond10x/eventlog/blob/0.3.0/CHANGELOG.md)
retains earlier releases as history.
