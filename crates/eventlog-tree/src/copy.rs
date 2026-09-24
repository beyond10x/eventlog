//! Linearizing a tree store into a linear SQL store (design § 9.6).
//!
//! A linear store holds gapless, unique `(stream, version)` and never forks. The tree's replay
//! order already is such a linearization — § 6.3 positions, and each stream's versions its
//! events' places in that order — so a copy appends the committed groups into the target in that
//! same order, with the ids and instants they were first recorded with, and the target numbers
//! each stream gaplessly as it goes.
//!
//! What the linear store has no column for is the chain: each event's tree digest and parents.
//! Those, with the version the tree served the event at, go into the target's origin map
//! ([`SqliteEventStore::origins`]), a table beside the event rows. Not into `data`, because a
//! redaction replaces `data` and would take the origin with it.

use crate::history::{self, Loaded};
use crate::{TreeEventStore, refresh, replay};
use eventlog_core::{EventLogError, EventStore, MAX_READ_LIMIT, StreamId, TenantId};
use eventlog_sqlite::SqliteEventStore;
use std::collections::{BTreeMap, BTreeSet};

/// What a [`copy`] wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CopyReport {
    /// Tenants the tree holds, whether or not they have events.
    pub tenants: usize,
    /// Committed groups appended, one per command the tree recorded.
    pub groups: usize,
    pub events: usize,
    /// Blob files copied, bound to a group or stored alone.
    pub blobs: usize,
    /// Events whose body the tree had redacted, redacted again in the copy.
    pub redactions: usize,
}

/// Copy every tenant of `from` into `to`, linearized: identities, blobs, every committed group in
/// § 6.3 position order with its receipt, and every redaction. Each copied event's tree
/// `{version, digest, parents}` is recorded in `to`'s origin map.
///
/// The tree is held under its writer lock for the whole copy, so no write lands halfway. `to` must
/// not be written by anyone else while the copy runs: it takes each event's original id and
/// instant, which another writer's append could take instead.
///
/// # Errors
/// Returns [`EventLogError::Invalid`], before anything is written, when `to` already holds events
/// for one of the tree's tenants, gave one a different stream identity, binds one of its blob
/// digests to other bytes, or holds queued restored identities. Returns [`EventLogError::Closed`]
/// for a tree that can no longer serve, and whatever the tree's files or the target refuse.
pub async fn copy(
    from: &TreeEventStore,
    to: &SqliteEventStore,
) -> Result<CopyReport, EventLogError> {
    let mut state = from.state.lock().await;
    if state.poisoned {
        return Err(EventLogError::Closed);
    }
    let _lock = from.writer_lock()?;
    refresh(&from.root, &mut state).await?;
    let loaded = history::load(&from.root)?;
    let versions = served_versions(&state.engine, &loaded).await?;
    drop(state);

    refuse_before_writing(&loaded, to).await?;

    let mut report = CopyReport::default();
    for (name, history) in &loaded.tenants {
        let tenant = TenantId::new(name.clone())?;
        report.tenants += 1;
        if let Some(identity) = &history.identity {
            to.restore_stream_identity(&tenant, identity).await?;
        }
        for (digest, bytes) in &history.blobs {
            to.put_blob(&tenant, digest, bytes).await?;
            report.blobs += 1;
        }
        let mut redactions = Vec::new();
        for committed in &history.groups {
            let written = replay(to, &tenant, committed, Some(&versions)).await?;
            report.groups += 1;
            for (record, version) in committed.events.iter().flatten().zip(written) {
                report.events += 1;
                if let Some(reason) = &record.redacted {
                    redactions.push((record.stream()?, version, reason.clone()));
                }
            }
        }
        // After every group, as the tree's own replay does: a redaction is not a group, and the
        // body it replaces had to be appended first.
        for (stream, version, reason) in redactions {
            to.redact(&stream, version, &reason).await?;
            report.redactions += 1;
        }
    }
    Ok(report)
}

/// Every refusal the target's state can already decide, taken before the copy writes anything,
/// so a refused copy leaves the target as it found it and a corrected one can be retried:
///
/// - a tenant with events in the target: appending onto them would give the tree's events
///   versions it never served, and interleave two histories;
/// - a tenant whose identity in the target differs from the tree's: `restore_stream_identity`
///   refuses it;
/// - a blob digest the target binds to other bytes: `put_blob` refuses it;
/// - restored identities another caller queued and no append took: `restore_events` refuses them.
///
/// What remains is a failure of the target itself mid-copy, which no check beforehand can rule out.
async fn refuse_before_writing(
    loaded: &Loaded,
    to: &SqliteEventStore,
) -> Result<(), EventLogError> {
    if to.restored_pending()? != 0 {
        return Err(EventLogError::Invalid(
            "the copy target holds restored identities no append has taken".into(),
        ));
    }
    for (name, history) in &loaded.tenants {
        let tenant = TenantId::new(name.clone())?;
        if !to.read_feed(&tenant, 0, 1).await?.events.is_empty() {
            return Err(EventLogError::Invalid(format!(
                "the copy target already holds history for tenant {name}"
            )));
        }
        if let (Some(ours), Some(theirs)) =
            (&history.identity, to.stored_stream_identity(&tenant).await?)
            && *ours != theirs
        {
            return Err(EventLogError::Invalid(format!(
                "the copy target gave tenant {name} a stream identity of its own"
            )));
        }
        for (digest, bytes) in &history.blobs {
            if let Some(held) = to.get_blob(&tenant, digest).await?
                && held != *bytes
            {
                return Err(EventLogError::Invalid(format!(
                    "the copy target binds blob {digest} of tenant {name} to other bytes"
                )));
            }
        }
    }
    Ok(())
}

/// The version the tree serves each committed event at, by event id, read from its own engine
/// rather than assumed from the replay order the copy is about to repeat.
async fn served_versions(
    engine: &SqliteEventStore,
    loaded: &Loaded,
) -> Result<BTreeMap<String, u64>, EventLogError> {
    let mut streams = BTreeSet::new();
    for history in loaded.tenants.values() {
        for committed in &history.groups {
            for record in committed.events.iter().flatten() {
                streams.insert((
                    record.tenant.clone(),
                    record.stream_type.clone(),
                    record.stream_id.clone(),
                ));
            }
        }
    }
    let mut versions = BTreeMap::new();
    for (tenant, stream_type, stream_id) in streams {
        let stream = StreamId::new(TenantId::new(tenant)?, stream_type, stream_id)?;
        let mut after = 0;
        loop {
            let slice = engine.read_stream(&stream, after, MAX_READ_LIMIT).await?;
            for event in &slice.events {
                versions.insert(event.event_id.clone(), event.version);
            }
            match slice.events.last() {
                Some(last) if !slice.end_of_stream => after = last.version,
                _ => break,
            }
        }
    }
    Ok(versions)
}
