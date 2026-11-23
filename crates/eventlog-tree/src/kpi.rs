//! Work counters read by the `eventlog-kpi` runner. Compiled only with the non-default
//! `kpi-counters` feature, so no product build carries them and no product API changes.
//!
//! Each counter is process-wide: the runner drives one scenario at a time and takes the counters
//! before and after it.

use std::sync::atomic::{AtomicU64, Ordering};

static GROUP_FILES_LISTED: AtomicU64 = AtomicU64::new(0);
static EVENT_DIGESTS: AtomicU64 = AtomicU64::new(0);

/// The counters since the last [`take`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Group file names the writer's change check enumerated (review L17).
    pub group_files_listed: u64,
    /// Calls of the event envelope digest (review L20).
    pub event_digests: u64,
}

/// Read every counter and reset it to zero.
pub fn take() -> Counters {
    Counters {
        group_files_listed: GROUP_FILES_LISTED.swap(0, Ordering::Relaxed),
        event_digests: EVENT_DIGESTS.swap(0, Ordering::Relaxed),
    }
}

pub(crate) fn group_files_listed(count: usize) {
    GROUP_FILES_LISTED.fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
}

pub(crate) fn event_digest() {
    EVENT_DIGESTS.fetch_add(1, Ordering::Relaxed);
}
