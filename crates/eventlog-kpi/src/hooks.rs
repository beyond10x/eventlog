//! The providers' `kpi` counters, read the same way whether or not this build carries them.
//!
//! With the `counters` feature each function reads (and resets) the provider's counters; without
//! it each returns `None` and the scenario reports the KPI as skipped.

pub const ABSENT: &str = "built without the `counters` feature";

#[derive(Clone, Copy, Debug, Default)]
#[cfg_attr(not(feature = "counters"), allow(dead_code))]
pub struct File {
    pub feed_visits: u64,
    pub blob_dir_entries: u64,
    pub blob_dir_syncs: u64,
}

#[derive(Clone, Copy, Debug, Default)]
#[cfg_attr(not(feature = "counters"), allow(dead_code))]
pub struct Sqlite {
    pub vm_steps: u64,
    pub rebuild_retained_events: u64,
}

#[derive(Clone, Copy, Debug, Default)]
#[cfg_attr(not(feature = "counters"), allow(dead_code))]
pub struct Tree {
    pub group_files_listed: u64,
    pub event_digests: u64,
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "one signature with and without the feature"
)]
#[cfg(feature = "counters")]
pub fn file() -> Option<File> {
    let counters = eventlog_file::kpi::take();
    Some(File {
        feed_visits: counters.feed_visits,
        blob_dir_entries: counters.blob_dir_entries,
        blob_dir_syncs: counters.blob_dir_syncs,
    })
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "one signature with and without the feature"
)]
#[cfg(feature = "counters")]
pub fn sqlite() -> Option<Sqlite> {
    let counters = eventlog_sqlite::kpi::take();
    Some(Sqlite {
        vm_steps: counters.vm_steps,
        rebuild_retained_events: counters.rebuild_retained_events,
    })
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "one signature with and without the feature"
)]
#[cfg(feature = "counters")]
pub fn tree() -> Option<Tree> {
    let counters = eventlog_tree::kpi::take();
    Some(Tree {
        group_files_listed: counters.group_files_listed,
        event_digests: counters.event_digests,
    })
}

#[cfg(not(feature = "counters"))]
pub fn file() -> Option<File> {
    None
}

#[cfg(not(feature = "counters"))]
pub fn sqlite() -> Option<Sqlite> {
    None
}

#[cfg(not(feature = "counters"))]
pub fn tree() -> Option<Tree> {
    None
}
