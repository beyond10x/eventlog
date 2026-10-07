use crate::journal::backend;
use eventlog_core::{EventLogError, GroupRange, RecordedEvent, StreamId, TenantId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Op {
    Watermark {
        position: u64,
    },
    Counter {
        tenant: Option<TenantId>,
        coordinate: String,
        value: i64,
    },
    Event {
        // Boxed so one large variant does not size every operation; serde writes it unchanged.
        event: Box<RecordedEvent>,
    },
    Command {
        stream: StreamId,
        key: String,
        digest: String,
        first: u64,
        last: u64,
    },
    Claim {
        tenant: TenantId,
        scope: String,
        key: String,
        digest: String,
        range: GroupRange,
    },
    Group {
        tenant: TenantId,
        key: String,
        digest: String,
        ranges: Vec<GroupRange>,
        /// The blob digests this group committed, sorted and without repeats.
        ///
        /// What makes a retry of a blob-bearing group *the same request*. The group's fingerprint
        /// covers the tenant, the members and the command meta and deliberately not the batch, so
        /// the batch has to be recorded to be compared; and it is compared against what this
        /// record says the commit carried, never against which blobs happen to be bound now.
        /// Whether a blob still exists is a separate question with a separate answer —
        /// `delete_blob` is its own act and does not retroactively make a committed group belong
        /// to a different request.
        ///
        /// Absent in a record written before groups could carry blobs, and absent from the wire
        /// whenever it is empty, so every frame a group without blobs writes is byte-identical to
        /// what it wrote before this field existed.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        blobs: Vec<String>,
    },
    Identity {
        tenant: TenantId,
        id: String,
    },
    Generation {
        stream: StreamId,
        id: String,
    },
    Projection {
        name: String,
        indexed: Vec<String>,
    },
    DirtyView {
        tenant: TenantId,
        name: String,
        dirty: bool,
    },
    Row {
        tenant: TenantId,
        name: String,
        key: String,
        body: Option<Value>,
    },
    ClearRows {
        tenant: TenantId,
        name: String,
    },
    Cursor {
        tenant: TenantId,
        name: String,
        position: u64,
    },
    Blob {
        tenant: TenantId,
        digest: String,
        object: Option<Blob>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Blob {
    pub id: String,
    pub hash: String,
}

/// Every committed event, by global position and by stream.
///
/// The stream index is what lets a per-stream read cost that stream rather than the store:
/// [`State::head`], [`Events::of_stream`] and the order check in [`State::apply`] read it, and
/// only the tenant-wide reads walk every event. Both maps are private and [`Events::insert`] is the
/// only write to either, so the index cannot drift from the events it indexes: a state replayed
/// from the journal and one folded onto a cached view run the same inserts for the same events.
/// Redaction and erasure change no event in place; they rewrite the journal and replay it into a
/// new `State`. Nothing here is serialized, so the journal's bytes do not depend on it.
#[derive(Default)]
pub(crate) struct Events {
    by_position: BTreeMap<u64, RecordedEvent>,
    /// [`stream_key`] → version → global position.
    by_stream: BTreeMap<String, BTreeMap<u64, u64>>,
}

impl Events {
    /// Called only by [`State::apply`], after its order check, so no version and no position is
    /// ever written twice.
    fn insert(&mut self, stream: String, event: RecordedEvent) {
        self.by_stream
            .entry(stream)
            .or_default()
            .insert(event.version, event.global_seq);
        self.by_position.insert(event.global_seq, event);
    }
    /// The highest version the stream under `stream` (a [`stream_key`]) holds, or 0. Reads the
    /// index and no event.
    fn head(&self, stream: &str) -> u64 {
        self.by_stream
            .get(stream)
            .and_then(BTreeMap::last_key_value)
            .map_or(0, |(version, _)| *version)
    }
    /// The events of `stream` whose versions lie in `versions`, in version order. Visits only the
    /// events it yields.
    ///
    /// The index is entered at the lower bound alone and the upper bound ends the walk, so a range
    /// whose start lies past its end yields nothing rather than reaching `BTreeMap::range`'s panic.
    pub fn of_stream<'a, R: RangeBounds<u64>>(
        &'a self,
        stream: &StreamId,
        versions: R,
    ) -> impl Iterator<Item = &'a RecordedEvent> + use<'a, R> {
        let from = (versions.start_bound().cloned(), Bound::Unbounded);
        self.by_stream
            .get(&stream_key(stream))
            .map(move |index| {
                index
                    .range(from)
                    .take_while(move |(version, _)| versions.contains(*version))
            })
            .into_iter()
            .flatten()
            .filter_map(|(_, position)| self.by_position.get(position))
            .map(visit)
    }
    /// Every event in global order: the tenant-wide reads, which pay for the whole store.
    pub fn values(&self) -> impl DoubleEndedIterator<Item = &RecordedEvent> + ExactSizeIterator {
        self.by_position.values().map(visit)
    }
    pub fn into_values(self) -> impl DoubleEndedIterator<Item = RecordedEvent> + ExactSizeIterator {
        self.by_position.into_values().inspect(|event| {
            visit(event);
        })
    }
}

/// One event read out of [`Events`]. Every read goes through here, and a test build charges it.
fn visit(event: &RecordedEvent) -> &RecordedEvent {
    #[cfg(test)]
    visits::charge(&event.tenant);
    event
}

/// Events read out of [`Events`], tallied by the tenant that owns each one. Test-only.
///
/// What a per-stream read is measured in: the events it *visited*, not the events it returned.
/// [`visit`] is on every path out of [`Events`], so a read that scanned the store to find one
/// stream is charged for the store. Keyed by tenant for the reason `cost` is keyed by store root:
/// these cases share one process with every other case in the crate, and a tenant no other case
/// names is a tally no other case moves.
#[cfg(test)]
mod visits {
    use eventlog_core::TenantId;
    use std::{
        collections::HashMap,
        sync::{Mutex, OnceLock},
    };

    fn table() -> &'static Mutex<HashMap<String, u64>> {
        static TABLE: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
        TABLE.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(super) fn charge(tenant: &TenantId) {
        let mut table = table().lock().expect("visit table");
        if let Some(count) = table.get_mut(tenant.as_str()) {
            *count += 1;
        } else {
            table.insert(tenant.as_str().to_owned(), 1);
        }
    }

    /// One tenant's tally so far. A case compares two of these; an absolute count means nothing.
    pub(super) fn of(tenant: &TenantId) -> u64 {
        table()
            .lock()
            .expect("visit table")
            .get(tenant.as_str())
            .copied()
            .unwrap_or_default()
    }
}

#[derive(Default)]
pub(crate) struct State {
    pub events: Events,
    pub commands: BTreeMap<String, (String, GroupRange)>,
    pub claims: BTreeMap<String, (String, GroupRange)>,
    pub groups: BTreeMap<String, (String, Vec<GroupRange>, Vec<String>)>,
    pub identities: BTreeMap<String, String>,
    pub generations: BTreeMap<String, String>,
    pub projections: BTreeMap<String, Vec<String>>,
    pub dirty_views: std::collections::BTreeSet<(String, String)>,
    pub rows: BTreeMap<(String, String, String), Value>,
    pub cursors: BTreeMap<(String, String), u64>,
    pub blobs: BTreeMap<(String, String), Blob>,
    pub next_position: u64,
    pub counters: BTreeMap<String, i64>,
}

pub(crate) fn key(parts: &[&str]) -> String {
    serde_json::to_string(parts).expect("string array serialization is infallible")
}
pub(crate) fn stream_key(stream: &StreamId) -> String {
    key(&[
        stream.tenant().as_str(),
        stream.stream_type(),
        stream.stream_id(),
    ])
}
impl State {
    pub fn replay(transactions: &[Value]) -> Result<Self, EventLogError> {
        let mut state = Self::default();
        for transaction in transactions {
            state.fold(transaction)?;
        }
        Ok(state)
    }
    /// Apply one committed transaction and return the blob bindings it created, so a handle that
    /// folds frames it has not seen before can verify exactly the objects they bind.
    pub fn fold(&mut self, transaction: &Value) -> Result<Vec<(TenantId, String)>, EventLogError> {
        let mut bound = Vec::new();
        for op in serde_json::from_value::<Vec<Op>>(transaction.clone()).map_err(backend)? {
            if let Op::Blob {
                tenant,
                digest,
                object: Some(_),
            } = &op
            {
                bound.push((tenant.clone(), digest.clone()));
            }
            self.apply(op)?;
        }
        Ok(bound)
    }
    pub fn head(&self, stream: &StreamId) -> u64 {
        self.events.head(&stream_key(stream))
    }
    pub fn apply(&mut self, op: Op) -> Result<(), EventLogError> {
        match op {
            Op::DirtyView {
                tenant,
                name,
                dirty,
            } => {
                let coordinate = (tenant.as_str().into(), name);
                if dirty {
                    self.dirty_views.insert(coordinate);
                } else {
                    self.dirty_views.remove(&coordinate);
                }
            }
            Op::Watermark { position } => {
                self.next_position = self.next_position.max(position);
            }
            Op::Counter {
                coordinate, value, ..
            } => {
                self.counters.insert(coordinate, value);
            }
            Op::Event { event } => {
                // Sequence gaps after erasure are legal; a duplicated identity or position is not.
                if event.global_seq <= self.next_position {
                    return Err(backend("invalid recorded event order"));
                }
                let stream = stream_key(&event.stream()?);
                if event.version != self.events.head(&stream) + 1 {
                    return Err(backend("invalid recorded event order"));
                }
                self.next_position = event.global_seq;
                self.events.insert(stream, *event);
            }
            Op::Command {
                stream,
                key: id,
                digest,
                first,
                last,
            } => {
                self.commands.insert(
                    key(&[&stream_key(&stream), &id]),
                    (
                        digest,
                        GroupRange {
                            stream,
                            first_version: first,
                            last_version: last,
                        },
                    ),
                );
            }
            Op::Claim {
                tenant,
                scope,
                key: id,
                digest,
                range,
            } => {
                self.claims
                    .insert(key(&[tenant.as_str(), &scope, &id]), (digest, range));
            }
            Op::Group {
                tenant,
                key: id,
                digest,
                ranges,
                blobs,
            } => {
                self.groups
                    .insert(key(&[tenant.as_str(), &id]), (digest, ranges, blobs));
            }
            Op::Identity { tenant, id } => {
                self.identities.insert(tenant.as_str().into(), id);
            }
            Op::Generation { stream, id } => {
                self.generations.insert(stream_key(&stream), id);
            }
            Op::Projection { name, indexed } => {
                if self
                    .projections
                    .get(&name)
                    .is_some_and(|old| old != &indexed)
                {
                    return Err(backend("projection registration shape changed"));
                }
                self.projections.insert(name, indexed);
            }
            Op::Row {
                tenant,
                name,
                key,
                body,
            } => {
                let coordinate = (tenant.as_str().into(), name, key);
                if let Some(body) = body {
                    self.rows.insert(coordinate, body);
                } else {
                    self.rows.remove(&coordinate);
                }
            }
            Op::ClearRows { tenant, name } => {
                self.rows
                    .retain(|(t, n, _), _| t != tenant.as_str() || n != &name);
            }
            Op::Cursor {
                tenant,
                name,
                position,
            } => {
                self.cursors
                    .insert((tenant.as_str().into(), name), position);
            }
            Op::Blob {
                tenant,
                digest,
                object,
            } => {
                let coordinate = (tenant.as_str().into(), digest);
                if let Some(object) = object {
                    self.blobs.insert(coordinate, object);
                } else {
                    self.blobs.remove(&coordinate);
                }
            }
        }
        Ok(())
    }
}
impl Op {
    pub fn tenant(&self) -> Option<&TenantId> {
        match self {
            Self::Counter { tenant, .. } => tenant.as_ref(),
            Self::Event { event } => Some(&event.tenant),
            Self::Command { stream, .. } | Self::Generation { stream, .. } => Some(stream.tenant()),
            Self::Projection { .. } | Self::Watermark { .. } => None,
            Self::DirtyView { tenant, .. }
            | Self::Claim { tenant, .. }
            | Self::Group { tenant, .. }
            | Self::Identity { tenant, .. }
            | Self::Row { tenant, .. }
            | Self::ClearRows { tenant, .. }
            | Self::Cursor { tenant, .. }
            | Self::Blob { tenant, .. } => Some(tenant),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Events, State, stream_key, visits};
    use crate::{FileEventStore, journal::Journal};
    use eventlog_conformance::{event, meta};
    use eventlog_core::{
        AppendGroup, AtomicEventStore, EventStore, Expected, RecordedEvent, StreamAppend, StreamId,
        TenantId,
    };
    use serde_json::json;
    use std::{collections::BTreeMap, path::Path};

    /// The store these cases read: `ROUNDS` groups, each appending one event to every one of
    /// `STREAMS` streams, so every stream's events are spread across the whole store.
    const STREAMS: u64 = 40;
    const ROUNDS: u64 = 4;
    const STORE: u64 = STREAMS * ROUNDS;

    /// Each case's own tenant. The visit tally is keyed by tenant and the workspace gate runs
    /// these cases in parallel, so two cases sharing one would charge each other's reads.
    fn tenant(case: &str) -> TenantId {
        TenantId::new(format!("stream-index-{case}")).unwrap()
    }

    fn stream(tenant: &TenantId, index: u64) -> StreamId {
        StreamId::new(tenant.clone(), "item", format!("s{index:02}")).unwrap()
    }

    fn round(tenant: &TenantId, round: u64) -> AppendGroup {
        AppendGroup {
            tenant: tenant.clone(),
            meta: meta(&format!("round-{round}"), &json!({ "round": round })),
            appends: (0..STREAMS)
                .map(|index| StreamAppend {
                    stream: stream(tenant, index),
                    expected: Expected::Any,
                    events: vec![event("item.changed", 1)],
                })
                .collect(),
        }
    }

    async fn filled(root: &Path, tenant: &TenantId) -> FileEventStore {
        let store = FileEventStore::open(root).await.unwrap();
        for index in 0..ROUNDS {
            store.append_group(&round(tenant, index)).await.unwrap();
        }
        store
    }

    fn count(events: &[RecordedEvent]) -> u64 {
        u64::try_from(events.len()).unwrap()
    }

    /// The acceptance case: `stream_version` and a `read_stream` window visit the stream asked
    /// about, not the store.
    ///
    /// The feed read first is the control. It is a tenant-wide scan and is charged for every event
    /// it passes, which is what makes the zero and the window counts below a measurement rather
    /// than a counter nobody charges.
    #[tokio::test]
    async fn a_head_lookup_and_a_stream_window_visit_the_stream_not_the_store() {
        let root = tempfile::tempdir().unwrap();
        let tenant = tenant("window");
        let store = filled(root.path(), &tenant).await;
        let visited = || visits::of(&tenant);

        let before = visited();
        let feed = store.read_feed(&tenant, 0, 1_000).await.unwrap();
        assert_eq!(count(&feed.events), STORE);
        assert_eq!(visited() - before, STORE, "the feed is a scan of the store");

        for index in [0, STREAMS / 2, STREAMS - 1] {
            let asked = stream(&tenant, index);
            let before = visited();
            assert_eq!(store.stream_version(&asked).await.unwrap(), Some(ROUNDS));
            assert_eq!(visited() - before, 0, "stream_version of {asked:?}");
            for (after, limit) in [(0, 1), (0, 10), (1, 2), (ROUNDS - 1, 5), (ROUNDS, 5)] {
                let before = visited();
                let window = store.read_stream(&asked, after, limit).await.unwrap();
                let cost = visited() - before;
                let versions: Vec<_> = window.events.iter().map(|e| e.version).collect();
                let expected: Vec<_> = (after + 1..=ROUNDS).take(limit).collect();
                assert_eq!(
                    versions, expected,
                    "read_stream({asked:?}, {after}, {limit})"
                );
                // The window, plus the one event it reads past to know whether the stream ends.
                let window_cost = (ROUNDS - after).min(u64::try_from(limit).unwrap() + 1);
                assert_eq!(
                    cost, window_cost,
                    "read_stream({asked:?}, {after}, {limit}) visited {cost} of {STORE} events"
                );
            }
        }
    }

    /// The two other per-stream lookups: the receipt a retried command is answered with
    /// (`Transaction::result`), for one stream and for every member of a group, and the event a
    /// redaction rewrites. Each visits the events it names; the redaction's replay visits none.
    #[tokio::test]
    async fn a_retried_append_and_a_redaction_find_their_events_through_the_stream() {
        let root = tempfile::tempdir().unwrap();
        let tenant = tenant("receipt");
        let store = filled(root.path(), &tenant).await;
        let visited = || visits::of(&tenant);
        let asked = stream(&tenant, STREAMS - 1);
        let events = [event("item.changed", 2), event("item.changed", 3)];
        let command = meta("retried", &json!({ "retried": true }));

        let first = store
            .append(&asked, Expected::Any, &events, &command)
            .await
            .unwrap();
        let before = visited();
        let retried = store
            .append(&asked, Expected::Any, &events, &command)
            .await
            .unwrap();
        assert!(retried.deduplicated);
        assert_eq!(retried.events, first.events);
        assert_eq!(visited() - before, 2, "a retried append's receipt");

        let before = visited();
        let group = store.append_group(&round(&tenant, 0)).await.unwrap();
        assert!(group.deduplicated);
        assert_eq!(visited() - before, STREAMS, "a retried group's receipts");

        let before = visited();
        let redacted = store.redact(&asked, ROUNDS + 2, "privacy").await.unwrap();
        assert_eq!(redacted.version, ROUNDS + 2);
        assert_eq!(visited() - before, 1, "a redaction's find and replay");
    }

    /// The order check `apply` makes for every event asks for that event's stream head. Folding a
    /// history asks it once per event, so a head that scanned the store made a replay quadratic in
    /// the store: `STORE * (STORE - 1) / 2` visits for this one.
    #[tokio::test]
    async fn a_replay_visits_no_event_to_check_the_order_of_the_next() {
        let root = tempfile::tempdir().unwrap();
        let tenant = tenant("replay");
        drop(filled(root.path(), &tenant).await);
        let journal = Journal::open(root.path()).unwrap();

        let before = visits::of(&tenant);
        let state = State::replay(&journal.transactions).unwrap();
        assert_eq!(
            visits::of(&tenant) - before,
            0,
            "a replay of {STORE} events"
        );
        assert_eq!(state.head(&stream(&tenant, 0)), ROUNDS);
    }

    /// The index regrouped from the events it indexes, by the same key and to the same positions.
    fn regrouped(events: &Events) -> BTreeMap<String, BTreeMap<u64, u64>> {
        let mut index: BTreeMap<String, BTreeMap<u64, u64>> = BTreeMap::new();
        for event in events.by_position.values() {
            index
                .entry(stream_key(&event.stream().unwrap()))
                .or_default()
                .insert(event.version, event.global_seq);
        }
        index
    }

    /// Every write path leaves each stream's own read equal to the tenant's feed regrouped by
    /// stream, on the handle that folded the writes and on one that replays them from scratch.
    async fn agrees(store: &FileEventStore, tenants: &[TenantId]) {
        for tenant in tenants {
            let feed = store.read_feed(tenant, 0, 1_000).await.unwrap();
            assert!(!feed.has_more);
            let mut regrouped: BTreeMap<(String, String), Vec<RecordedEvent>> = BTreeMap::new();
            for event in feed.events {
                regrouped
                    .entry((event.stream_type.clone(), event.stream_id.clone()))
                    .or_default()
                    .push(event);
            }
            for ((stream_type, stream_id), events) in regrouped {
                let stream = StreamId::new(tenant.clone(), stream_type, stream_id).unwrap();
                let read = store.read_stream(&stream, 0, 1_000).await.unwrap();
                assert!(read.end_of_stream);
                assert_eq!(read.events, events, "{stream:?}");
                assert_eq!(
                    store.stream_version(&stream).await.unwrap(),
                    events.last().map(|e| e.version),
                    "{stream:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn every_write_path_keeps_each_streams_read_equal_to_its_events_in_the_feed() {
        let root = tempfile::tempdir().unwrap();
        let kept = tenant("kept");
        let erased = tenant("erased");
        let twin = |index: u64| stream(&erased, index);
        let stream = |index: u64| stream(&kept, index);
        let tenants = [kept.clone(), erased.clone()];
        let store = filled(root.path(), &kept).await;
        for index in 0..3 {
            store
                .append(
                    &twin(index),
                    Expected::NoStream,
                    &[event("item.changed", 4), event("item.changed", 5)],
                    &meta(&format!("twin-{index}"), &json!({ "twin": index })),
                )
                .await
                .unwrap();
        }
        agrees(&store, &tenants).await;

        assert!(
            store
                .append_group(&round(&kept, 1))
                .await
                .unwrap()
                .deduplicated
        );
        agrees(&store, &tenants).await;

        // A stream's head first: no later event of that stream would trip `apply`'s order check
        // if the replay left it out of the index, so only the comparison can see it.
        let head = store.redact(&stream(3), ROUNDS, "privacy").await.unwrap();
        agrees(&store, &tenants).await;
        let middle = store.redact(&stream(3), 2, "privacy").await.unwrap();
        agrees(&store, &tenants).await;
        let read = store.read_stream(&stream(3), 1, 1).await.unwrap();
        assert_eq!(read.events, vec![middle]);
        let read = store.read_stream(&stream(3), ROUNDS - 1, 1).await.unwrap();
        assert_eq!(read.events, vec![head]);

        store.forget_tenant(&erased).await.unwrap();
        agrees(&store, &tenants).await;
        for index in 0..3 {
            assert_eq!(store.stream_version(&twin(index)).await.unwrap(), None);
            let read = store.read_stream(&twin(index), 0, 10).await.unwrap();
            assert_eq!(read.events, Vec::<RecordedEvent>::new());
        }
        assert_eq!(
            store.stream_version(&stream(0)).await.unwrap(),
            Some(ROUNDS)
        );

        store
            .append(
                &stream(0),
                Expected::Exact(ROUNDS),
                &[event("item.changed", 6)],
                &meta("after-erasure", &json!({})),
            )
            .await
            .unwrap();
        agrees(&store, &tenants).await;
        drop(store);

        let reopened = FileEventStore::open(root.path()).await.unwrap();
        agrees(&reopened, &tenants).await;
        assert_eq!(
            reopened.stream_version(&stream(0)).await.unwrap(),
            Some(ROUNDS + 1)
        );
        drop(reopened);

        // The same history, folded one frame at a time as a resumed handle folds what it has not
        // seen, and replayed at once as an opener does: after every frame the index is exactly its
        // events regrouped, and both ends hold the same index.
        let journal = Journal::open(root.path()).unwrap();
        let mut folded = State::default();
        for (frame, transaction) in journal.transactions.iter().enumerate() {
            folded.fold(transaction).unwrap();
            assert_eq!(
                folded.events.by_stream,
                regrouped(&folded.events),
                "frame {frame}"
            );
        }
        let replayed = State::replay(&journal.transactions).unwrap();
        assert_eq!(replayed.events.by_stream, folded.events.by_stream);
        assert_eq!(
            replayed.events.by_position.len(),
            folded.events.by_position.len()
        );
    }
}
