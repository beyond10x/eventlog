//! Optional caller-scoped transactions; callbacks never own commit or a database connection.
use crate::{
    AppendGroup, AppendGroupResult, AtomicEventStore, BoxFuture, EventLogError, ProjectionStore,
    SnapshotGeneration, StreamId, StreamSlice, TenantId,
};

/// One tenant's view of an open storage transaction, usable only inside its callback.
///
/// Calls observe preceding writes. The caller owns lock ordering across calls and must not
/// re-enter the outer store. Dropping a started operation poisons the transaction; callback
/// success cannot publish partial work from a cancelled operation. Ordinary operation errors
/// abort the session; failed groups and sequence reservations restore their own savepoint.
pub trait TransactionSession: Send {
    /// The sole tenant this transaction may access.
    fn tenant(&self) -> &TenantId;
    /// Inline reads, blob reads and projection locks share this transaction and its restrictions.
    fn projections(&mut self) -> &mut dyn ProjectionStore;
    /// Read a bounded stream page without a feed-watermark dependency.
    fn read_stream<'a>(
        &'a mut self,
        stream: &'a StreamId,
        after_version: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<StreamSlice, EventLogError>>;
    /// Read this transaction's current stream head.
    fn stream_version<'a>(
        &'a mut self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<u64>, EventLogError>>;
    /// Enumerate bounded committed/staged coordinates in the session's tenant.
    fn list_streams<'a>(
        &'a mut self,
        stream_type: &'a str,
        after_id: Option<&'a str>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<StreamId>, EventLogError>>;
    /// Capture history generation and protect it against redaction until transaction end.
    fn snapshot_generation<'a>(
        &'a mut self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Option<SnapshotGeneration>, EventLogError>>;
    /// Lock the same stream identity used by ordinary and grouped appends, even if absent.
    fn lock_stream<'a>(
        &'a mut self,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<(), EventLogError>>;
    /// Serialize a tenant-scoped logical identity that may have no row yet.
    fn lock_identity<'a>(
        &'a mut self,
        namespace: &'a str,
        identity: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>>;
    /// Reserve positive consecutive values, returning the value just before the range.
    /// Reservations roll back with the outer transaction and cannot address admission counters.
    fn reserve_sequence<'a>(
        &'a mut self,
        namespace: &'a str,
        count: u64,
    ) -> BoxFuture<'a, Result<u64, EventLogError>>;
    /// Stage a complete group with its original durable identity, without committing the session.
    /// Caught failures roll back the group's prefix and receipt, leaving the session usable.
    fn append_group<'a>(
        &'a mut self,
        group: &'a AppendGroup,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>>;
}

/// An optional native capability, never emulated by independently committed operations.
///
/// The callback runs once, returning its value only after a successful commit. This generic
/// capability is not object-safe; the callback receives an object-safe session. Capture owned
/// callback inputs. An unknown commit is not permission to replay the callback: resolve original
/// group identities first. Sequence-only transactions have no automatic deduplication receipt.
pub trait TransactionalEventStore: AtomicEventStore {
    /// Commit a successful callback or roll back all its work. No callback replay is performed.
    fn with_transaction<'a, T, F>(
        &'a self,
        tenant: &'a TenantId,
        work: F,
    ) -> BoxFuture<'a, Result<T, EventLogError>>
    where
        T: Send + 'a,
        F: for<'session> FnOnce(
                &'session mut dyn TransactionSession,
            ) -> BoxFuture<'session, Result<T, EventLogError>>
            + Send
            + 'a;
}
