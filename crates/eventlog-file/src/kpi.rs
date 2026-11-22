//! Work counters read by the `eventlog-kpi` runner. Compiled only with the non-default
//! `kpi-counters` feature, so no product build carries them and no product API changes.
//!
//! Each counter is process-wide: the runner drives one scenario at a time and takes the counters
//! before and after it.

use std::sync::atomic::{AtomicU64, Ordering};

static FEED_VISITS: AtomicU64 = AtomicU64::new(0);
static BLOB_DIR_ENTRIES: AtomicU64 = AtomicU64::new(0);
static BLOB_DIR_SYNCS: AtomicU64 = AtomicU64::new(0);

/// The counters since the last [`take`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Event-map values `read_feed` inspected, returned or not (review P13).
    pub feed_visits: u64,
    /// Blob-directory entries the commit-time sweep listed (review L07).
    pub blob_dir_entries: u64,
    /// Synchronizations of the blob directory the commit-time sweep issued (review L07).
    pub blob_dir_syncs: u64,
}

/// Read every counter and reset it to zero.
pub fn take() -> Counters {
    Counters {
        feed_visits: FEED_VISITS.swap(0, Ordering::Relaxed),
        blob_dir_entries: BLOB_DIR_ENTRIES.swap(0, Ordering::Relaxed),
        blob_dir_syncs: BLOB_DIR_SYNCS.swap(0, Ordering::Relaxed),
    }
}

pub(crate) fn feed_visit() {
    FEED_VISITS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn blob_dir_entry() {
    BLOB_DIR_ENTRIES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn blob_dir_sync() {
    BLOB_DIR_SYNCS.fetch_add(1, Ordering::Relaxed);
}
