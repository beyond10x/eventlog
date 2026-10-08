//! Blob integrity decisions one handle has already made.
//!
//! `docs/design/sql-blob-read-integrity.md` requires every byte-returning SQL path to check the
//! stored length, edition and SHA-256 before returning bytes. That check is a pure function of
//! the row it is given. This module is where every such path asks it.
//!
//! **It is asked once per distinct content, not once per read.** The memory, its hit rule, its
//! budget and its clearing are `eventlog_core::VerifiedContent`, which the File and PostgreSQL
//! providers use too; what is SQLite's own is only the memory-database exception below. A row
//! whose bytes, hash, length or edition changed after a verified read is not a hit and is checked
//! in full, which is what keeps the same-handle tamper cases in `tests/conformance.rs` and
//! `tests/guarded_blob_batch.rs` refusing.
//!
//! **What is remembered, and for how long.** Entries come only from rows this handle read and
//! verified, or wrote and hashed itself. They are bounded by `eventlog_core::VERIFIED_CONTENT_BUDGET`, and all of them are
//! dropped when this handle deletes a blob, erases a tenant, or returns an error from a write that
//! bound blobs (`put_blob`, either blob-bearing group), so content of a write that returned an
//! error is not kept and a deletion or erasure through this handle leaves none of its bytes here.
//! A guard or inline projector that **panics** after the batch was bound unwinds past that
//! clearing, and the batch's own bytes may stay remembered until the budget, a later clearing, or
//! the handle is dropped. A deletion or
//! erasure through **another** handle or process does not reach this one: its entries stay in
//! this process's memory until the budget, this handle's own deletion, erasure or failed write,
//! or the handle itself is dropped. Such an entry is never returned — a hit requires the stored
//! row to carry identical bytes — but it is still a copy in memory.

use eventlog_core::{EventLogError, VerifiedContent};
use rusqlite::Connection;

/// The most content bytes one handle remembers. Past it the memory is cleared, not grown.
#[cfg(test)]
pub(crate) const BUDGET: usize = eventlog_core::VERIFIED_CONTENT_BUDGET;

/// What one handle has verified. Each [`VerifiedBlobs::check`] is the complete validator.
#[derive(Default)]
pub(crate) struct VerifiedBlobs {
    content: VerifiedContent,
}

impl VerifiedBlobs {
    /// Validate one stored row and return its bytes.
    ///
    /// # Errors
    /// [`EventLogError::Backend`] for corrupt persisted metadata or bytes.
    pub(crate) fn check(
        &self,
        connection: &Connection,
        bytes: Vec<u8>,
        byte_count: i64,
        integrity_sha256: Option<String>,
        integrity_v1: i64,
    ) -> Result<Vec<u8>, EventLogError> {
        // A memory database is only ever written through this handle's statements, and
        // `checked_blob` already skips the hash for it; nothing to remember.
        if connection.path().is_none_or(str::is_empty) {
            return crate::checked_blob(
                connection,
                bytes,
                byte_count,
                integrity_sha256,
                integrity_v1,
            );
        }
        self.content
            .check_stored(bytes, byte_count, integrity_sha256, integrity_v1)
    }

    /// Record that this handle computed `integrity_sha256` over `bytes` itself, at write time.
    ///
    /// Only a hash this handle computed from these exact bytes belongs here. The pair is true
    /// whether or not the write commits; a write that does not commit calls [`Self::forget`] so
    /// its bytes are not kept.
    pub(crate) fn remember(&self, integrity_sha256: &str, bytes: &[u8]) {
        self.content.remember(integrity_sha256, bytes);
    }

    /// Drop everything remembered, so deleted or erased content is not kept in memory.
    pub(crate) fn forget(&self) {
        self.content.forget();
    }

    /// SHA-256 computations the read-side validator has run on this handle so far.
    #[cfg(test)]
    pub(crate) fn hashed(&self) -> u64 {
        self.content.hashed()
    }

    /// Content bytes this handle remembers now.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.content.held()
    }

    /// Distinct contents this handle remembers now.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> usize {
        self.content.entries()
    }
}

#[cfg(test)]
mod tests {
    use crate::SqliteEventStore;
    use eventlog_core::{
        CaptureLimits, ConsistentTenantCapture, EventLogError, EventStore, Expected, StreamId,
        TenantId,
    };
    use std::sync::Arc;

    fn tenant() -> TenantId {
        TenantId::new("verify-once").unwrap()
    }

    fn content() -> Vec<u8> {
        (0..4096u32).map(|index| (index % 251) as u8).collect()
    }

    fn limits() -> CaptureLimits {
        CaptureLimits {
            max_events: 16,
            max_blobs: 16,
            max_projection_rows: 16,
            max_payload_bytes: 1 << 20,
        }
    }

    async fn seeded(path: &str) -> SqliteEventStore {
        let store = SqliteEventStore::open(path, "verify_once").await.unwrap();
        store.stream_identity(&tenant()).await.unwrap();
        store.put_blob(&tenant(), "d", &content()).await.unwrap();
        store
            .append(
                &StreamId::new(tenant(), "item", "one").unwrap(),
                Expected::NoStream,
                &[eventlog_conformance::event("item.created", 1)],
                &eventlog_conformance::meta("seed", &serde_json::json!({})),
            )
            .await
            .unwrap();
        store
    }

    async fn read_fifty_and_capture_five(store: &SqliteEventStore) {
        for _ in 0..50 {
            assert_eq!(
                store.get_blob(&tenant(), "d").await.unwrap(),
                Some(content())
            );
        }
        for _ in 0..5 {
            let capture = store
                .capture_tenant(&tenant(), &[], limits())
                .await
                .unwrap();
            assert_eq!(capture.blobs.len(), 1);
        }
    }

    /// The handle that wrote the content hashed it while writing, and no read hashes it again.
    #[tokio::test]
    async fn content_this_handle_wrote_is_not_hashed_again_by_any_read() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        let store = seeded(path.to_str().unwrap()).await;
        read_fifty_and_capture_five(&store).await;
        assert_eq!(
            store.inner.verified.hashed(),
            0,
            "put_blob's readback, 50 reads and 5 captures of content this handle hashed at write"
        );
    }

    /// A fresh handle hashes each stored row once, on the first read that returns it.
    #[tokio::test]
    async fn a_reopened_handle_hashes_unchanged_content_once() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        drop(seeded(path.to_str().unwrap()).await);
        let reopened = SqliteEventStore::open(path.to_str().unwrap(), "verify_once")
            .await
            .unwrap();
        read_fifty_and_capture_five(&reopened).await;
        assert_eq!(
            reopened.inner.verified.hashed(),
            1,
            "50 reads and 5 captures of one unchanged row on a fresh handle"
        );
    }

    /// A guard that reads one digest twenty times, as a batch guard reads its batch per member.
    struct ReadsRepeatedly;

    impl eventlog_core::Guard for ReadsRepeatedly {
        fn check<'a>(
            &'a self,
            store: &'a mut dyn eventlog_core::ProjectionStore,
        ) -> eventlog_core::BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                for _ in 0..20 {
                    assert_eq!(store.get_blob("d").await?, Some(content()));
                }
                Ok(())
            })
        }
    }

    /// The shared acceptance, held on SQLite as on File and PostgreSQL: one guarded append that
    /// reads one blob N times through `ProjectionStore::get_blob` pays one SHA-256.
    #[tokio::test]
    async fn one_guarded_append_reading_one_blob_n_times_hashes_it_once() {
        use eventlog_core::{AppendGroup, AtomicEventStore, StreamAppend};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        drop(seeded(path.to_str().unwrap()).await);
        let reopened = SqliteEventStore::open(path.to_str().unwrap(), "verify_once")
            .await
            .unwrap();
        reopened
            .append_group_guarded(
                &AppendGroup {
                    tenant: tenant(),
                    meta: eventlog_conformance::meta("reads", &serde_json::json!({})),
                    appends: vec![StreamAppend {
                        stream: StreamId::new(tenant(), "item", "two").unwrap(),
                        expected: Expected::NoStream,
                        events: vec![eventlog_conformance::event("item.created", 1)],
                    }],
                },
                Arc::new(ReadsRepeatedly),
            )
            .await
            .unwrap();
        assert_eq!(reopened.inner.verified.hashed(), 1);
    }

    /// A row that no longer matches what was verified is hashed on every read and refused on
    /// every read: a refusal is never remembered.
    #[tokio::test]
    async fn a_changed_row_is_hashed_and_refused_on_every_read() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        let store = seeded(path.to_str().unwrap()).await;
        read_fifty_and_capture_five(&store).await;
        let mut changed = content();
        changed[4095] ^= 1;
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE verify_once_blobs SET bytes=?1 WHERE digest='d'",
                rusqlite::params![changed],
            )
            .unwrap();
        let before = store.inner.verified.hashed();
        for _ in 0..3 {
            assert!(matches!(
                store.get_blob(&tenant(), "d").await,
                Err(EventLogError::Backend(_))
            ));
        }
        assert_eq!(store.inner.verified.hashed() - before, 3);
    }

    /// Remembered content is bounded, and deleting or erasing drops it.
    #[test]
    fn remembered_content_stays_within_its_budget_and_is_dropped_on_deletion() {
        let verified = super::VerifiedBlobs::default();
        let chunk = vec![7u8; super::BUDGET / 3 + 1];
        for index in 0..8u8 {
            let mut bytes = chunk.clone();
            bytes[0] = index;
            verified.remember(&eventlog_core::blob_integrity_sha256(&bytes), &bytes);
            let held = verified.held();
            assert!(held <= super::BUDGET, "{held} bytes held after {index}");
        }
        let oversized = vec![1u8; super::BUDGET + 1];
        verified.remember(
            &eventlog_core::blob_integrity_sha256(&oversized),
            &oversized,
        );
        assert!(verified.held() <= super::BUDGET);
        verified.forget();
        assert_eq!((verified.held(), verified.entries()), (0, 0));
    }

    /// A write that binds blobs and then does not commit leaves none of its bytes remembered.
    #[tokio::test]
    async fn a_write_that_does_not_commit_leaves_nothing_remembered() {
        use eventlog_core::{
            AppendGroup, AtomicBlobEventStore, AtomicEventStore, BlobAppendGroup, BlobWrite,
            NoGuard, StreamAppend,
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        let store = seeded(path.to_str().unwrap()).await;
        let held = |store: &SqliteEventStore| store.inner.verified.held();
        let conflicting = AppendGroup {
            tenant: tenant(),
            meta: eventlog_conformance::meta("conflicts", &serde_json::json!({})),
            appends: vec![StreamAppend {
                stream: StreamId::new(tenant(), "item", "one").unwrap(),
                expected: Expected::NoStream,
                events: vec![eventlog_conformance::event("item.created", 1)],
            }],
        };
        let rolled_back = vec![("new".to_owned(), b"never committed".to_vec())];

        assert!(held(&store) > 0, "the seed put is remembered");
        store
            .append_group_guarded_with_blobs(&conflicting, Arc::new(NoGuard), &rolled_back)
            .await
            .expect_err("the member conflicts after the batch was bound");
        assert_eq!(held(&store), 0, "guarded group rolled back");

        store.get_blob(&tenant(), "d").await.unwrap();
        store
            .append_group_with_blobs(&BlobAppendGroup {
                group: conflicting.clone(),
                blobs: vec![BlobWrite {
                    digest: "new".into(),
                    bytes: b"never committed".to_vec(),
                }],
            })
            .await
            .expect_err("the member conflicts after the batch was bound");
        assert_eq!(held(&store), 0, "atomic blob group rolled back");

        store.get_blob(&tenant(), "d").await.unwrap();
        store
            .put_blob(&tenant(), "d", b"other bytes")
            .await
            .expect_err("a different binding");
        assert_eq!(held(&store), 0, "refused put");
    }

    #[tokio::test]
    async fn deleting_a_blob_or_erasing_a_tenant_drops_remembered_content() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store.sqlite3");
        let store = seeded(path.to_str().unwrap()).await;
        store.put_blob(&tenant(), "e", b"other").await.unwrap();
        store.delete_blob(&tenant(), "e").await.unwrap();
        assert_eq!(store.inner.verified.held(), 0);
        store.get_blob(&tenant(), "d").await.unwrap();
        assert!(store.inner.verified.held() > 0);
        store.forget_tenant(&tenant()).await.unwrap();
        assert_eq!(store.inner.verified.held(), 0);
    }
}
