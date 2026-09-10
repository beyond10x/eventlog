use crate::journal::backend;
use eventlog_core::{EventLogError, GroupRange, RecordedEvent, StreamId, TenantId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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
        event: RecordedEvent,
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Blob {
    pub id: String,
    pub hash: String,
}

#[derive(Default)]
pub(crate) struct State {
    pub events: BTreeMap<u64, RecordedEvent>,
    pub commands: BTreeMap<String, (String, GroupRange)>,
    pub claims: BTreeMap<String, (String, GroupRange)>,
    pub groups: BTreeMap<String, (String, Vec<GroupRange>)>,
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
            for op in serde_json::from_value::<Vec<Op>>(transaction.clone()).map_err(backend)? {
                state.apply(op)?;
            }
        }
        Ok(state)
    }
    pub fn head(&self, stream: &StreamId) -> u64 {
        self.events
            .values()
            .filter(|event| matches_stream(event, stream))
            .map(|event| event.version)
            .max()
            .unwrap_or(0)
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
                if event.global_seq <= self.next_position
                    || event.version != self.head(&event.stream()?) + 1
                {
                    return Err(backend("invalid recorded event order"));
                }
                self.next_position = event.global_seq;
                self.events.insert(event.global_seq, event);
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
            } => {
                self.groups
                    .insert(key(&[tenant.as_str(), &id]), (digest, ranges));
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
pub(crate) fn matches_stream(event: &RecordedEvent, stream: &StreamId) -> bool {
    &event.tenant == stream.tenant()
        && event.stream_type == stream.stream_type()
        && event.stream_id == stream.stream_id()
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
