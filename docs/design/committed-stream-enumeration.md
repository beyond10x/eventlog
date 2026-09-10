# Committed stream inventory

`EventStore::list_streams(tenant, stream_type, after_id, limit)` returns stream identities
visible in one committed storage read. The exact tenant and stream type bound the query;
identities are unique and ordered by their UTF-8 bytes. `after_id` is an exclusive lower bound,
and the existing bounded-read limit applies. A short or empty page ends enumeration.

This is a distinct capability from the resumable feed. PostgreSQL's feed retains its publication
gate, committed-XID watermark and contiguous-prefix rule. An unrelated older transaction can
withhold that feed after a caller's append succeeds. An immediate list must use committed
inventory or an inline projection, never poll the feed and call its current tail complete.
The inventory reads existing event coordinates, using the SQL stream index; no DDL, persisted
layout, domain index or payload filter is added. The file provider reads the same coordinates
under its ordinary validated transaction boundary.

Inventory pages do not share a snapshot. A stream inserted before the last returned id requires
a fresh scan; callers cannot use this cursor as a subscription offset. Redaction retains stream
existence, even when every event is redacted. Tenant erasure removes those identities. A provider
that has not implemented this optional capability explicitly refuses instead of returning a
potentially lagging feed-derived inventory.

The shared `run_stream_inventory` exercise covers ordered pages, duplicate versions, tenant/type
isolation, invalid coordinates, redaction and erasure. PostgreSQL's independent regression holds
an unrelated assigned transaction open, proves that it withholds the feed, and requires immediate
inventory visibility. Adding the feed watermark to the inventory query must fail that assertion.
