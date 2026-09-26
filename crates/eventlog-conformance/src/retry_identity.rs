//! One retry identity across the two blob-bearing group methods.
//!
//! A group and batch committed through [`AtomicBlobEventStore::append_group_with_blobs`] is the
//! same request as that group and batch retried through
//! [`AtomicEventStore::append_group_guarded_with_blobs`]: the first receipt's fingerprint already
//! binds the group, the sorted digests and the SHA-256 of the bytes. `eventlog_tree::copy` writes
//! every blob group the first way, and a caller holding a trait object retries the second way.
use crate::{event, meta};
use eventlog_core::{
    AppendGroup, AtomicBlobEventStore, BlobAppendGroup, BlobWrite, BoxFuture, EventLogError,
    Expected, Guard, NoGuard, ProjectionStore, StreamAppend, StreamId, TenantId,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Counted(Arc<AtomicUsize>);

impl Guard for Counted {
    fn check<'a>(
        &'a self,
        _store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

/// # Panics
/// When a guarded retry of a group committed through `append_group_with_blobs` is refused or
/// appends again, when a guarded retry carrying other bytes or digests under that key is not
/// refused, or when the reverse direction stops refusing as documented.
pub async fn run_blob_group_retry_identity(store: &dyn AtomicBlobEventStore) {
    let tenant = TenantId::new("retry-identity").unwrap();
    let group = |key: &str| AppendGroup {
        tenant: tenant.clone(),
        meta: meta(key, &serde_json::json!({})),
        appends: vec![StreamAppend {
            stream: StreamId::new(tenant.clone(), "item", key).unwrap(),
            expected: Expected::NoStream,
            events: vec![event("item.created", 1)],
        }],
    };
    let batch = |second: &str| {
        vec![
            ("r-1".to_owned(), b"one".to_vec()),
            ("r-2".to_owned(), second.as_bytes().to_vec()),
        ]
    };
    let request = |key: &str| BlobAppendGroup {
        group: group(key),
        blobs: batch("two")
            .into_iter()
            .map(|(digest, bytes)| BlobWrite { digest, bytes })
            .collect(),
    };

    let first = store
        .append_group_with_blobs(&request("atomic"))
        .await
        .unwrap();
    assert!(!first.deduplicated);
    let mut reordered = batch("two");
    reordered.reverse();
    reordered.push(("r-1".to_owned(), b"one".to_vec()));
    let calls = Arc::new(AtomicUsize::new(0));
    let retry = store
        .append_group_guarded_with_blobs(
            &group("atomic"),
            Arc::new(Counted(calls.clone())),
            &reordered,
        )
        .await
        .expect("the same group and batch, retried through the guarded method");
    assert!(
        retry.deduplicated,
        "a retry of a committed request writes nothing"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a retry carrying a batch is admitted again, whichever method committed it"
    );
    assert_eq!(
        retry.appends.iter().map(|a| &a.events).collect::<Vec<_>>(),
        first.appends.iter().map(|a| &a.events).collect::<Vec<_>>(),
        "the original event coordinates"
    );
    assert_eq!(
        store
            .stream_version(&StreamId::new(tenant.clone(), "item", "atomic").unwrap())
            .await
            .unwrap(),
        Some(1)
    );
    for other in [batch("TWO"), batch("two")[..1].to_vec()] {
        let refused = store
            .append_group_guarded_with_blobs(&group("atomic"), Arc::new(NoGuard), &other)
            .await
            .expect_err("other bytes or other digests are another request");
        assert!(
            matches!(refused, EventLogError::IdempotencyMismatch { ref key } if key == "atomic"),
            "{refused:?}"
        );
    }

    // The reverse is not an identity: the guarded receipt does not prove the bytes, and an
    // `AtomicBlobEventStore` retry runs no admission that could be asked first.
    store
        .append_group_guarded_with_blobs(&group("guarded"), Arc::new(NoGuard), &batch("two"))
        .await
        .unwrap();
    let reverse = store
        .append_group_with_blobs(&request("guarded"))
        .await
        .expect_err("new versus legacy use of one key conflicts");
    assert!(
        matches!(reverse, EventLogError::IdempotencyMismatch { ref key } if key == "guarded"),
        "{reverse:?}"
    );
}
