---
format: aep.planning-md/3
id: story:tree-long-stream-and-blob-names
kind: story
status: draft
title: A Tree store accepts stream types, stream ids and blob digests of the longest accepted length
summary: The 512-byte tenant fix covers tenants only; a 512-byte stream id or blob digest can still exceed 255 bytes as a file name
revision: 1
---
## Outcome

A Tree store accepts a stream type, stream id or blob digest of the longest accepted length in
both layouts, the way it now accepts a 512-byte tenant (`tenants/%%<sha256>/` with a
`tenant.json` naming it), instead of failing with `File name too long (os error 36)`.

## Evidence (2026-11-23, e1-tree hand-back)

- story:tree-accepts-a-512-byte-tenant fixed the tenant directory only.
- A stream type, stream id or blob digest of 512 bytes can still escape to a file or directory
  name over 255 bytes in the nested layout or the blob path.

## Acceptance

- One test per name kind (stream type, stream id, blob digest) at the longest accepted length,
  hashed and nested layouts: written, reopened cold, read back, `verify` clean.
- A store written by the previous release reads unchanged; a new on-disk form is fenced like
  group/2 and named in the CHANGELOG.

## Decision taken

D4 of e1-tree, option A (default): file it.
