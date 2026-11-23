//! Work counters read by the `eventlog-kpi` runner. Compiled only with the non-default
//! `kpi-counters` feature, so no product build carries them and no product API changes.
//!
//! Each counter is process-wide: the runner drives one scenario at a time and takes the counters
//! before and after it.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use rusqlite::{
    Connection, StatementStatus,
    trace::{TraceEvent, TraceEventCodes},
};

static VM_STEPS: AtomicU64 = AtomicU64::new(0);
static REBUILD_RETAINED_EVENTS: AtomicU64 = AtomicU64::new(0);

/// The counters since the last [`take`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// SQLite virtual-machine steps of every statement run on a store connection (review L08,
    /// P10).
    pub vm_steps: u64,
    /// The most decoded events an inline rebuild held at once (review L16).
    pub rebuild_retained_events: u64,
}

/// Read every counter and reset it to zero.
pub fn take() -> Counters {
    Counters {
        vm_steps: VM_STEPS.swap(0, Ordering::Relaxed),
        rebuild_retained_events: REBUILD_RETAINED_EVENTS.swap(0, Ordering::Relaxed),
    }
}

pub(crate) fn rebuild_retained(events: usize) {
    REBUILD_RETAINED_EVENTS.fetch_max(u64::try_from(events).unwrap_or(u64::MAX), Ordering::Relaxed);
}

/// Count the VM steps of every statement this connection runs.
///
/// A statement's counter is cumulative over its life, and a cached statement runs many times, so
/// each run is charged the difference between its start and its end. A trigger subprogram's start
/// event carries a `--` comment and is not a new run.
pub(crate) fn instrument(connection: &mut Connection) {
    let mut started: HashMap<String, i32> = HashMap::new();
    let installed = connection.trace_v2(
        TraceEventCodes::SQLITE_TRACE_STMT | TraceEventCodes::SQLITE_TRACE_PROFILE,
        Some(move |event: TraceEvent<'_>| match event {
            TraceEvent::Stmt(statement, text) if !text.starts_with("--") => {
                started.insert(
                    statement.sql().into_owned(),
                    statement.get_status(StatementStatus::VmStep),
                );
            }
            TraceEvent::Profile(statement, _) => {
                let now = statement.get_status(StatementStatus::VmStep);
                let start = started.remove(statement.sql().as_ref()).unwrap_or(0);
                let steps = u64::try_from(now.saturating_sub(start)).unwrap_or(0);
                VM_STEPS.fetch_add(steps, Ordering::Relaxed);
            }
            _ => {}
        }),
    );
    debug_assert!(installed.is_ok(), "the KPI trace callback installs");
}
