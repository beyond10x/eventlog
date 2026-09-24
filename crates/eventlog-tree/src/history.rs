//! The two immutable records, reading them back, and the order history is replayed in.

use crate::layout::{
    EVENT_FORMAT, GROUP_FORMAT, backend, blob_path, canonical, corrupt, group_path, json_files,
    sha256_hex, subdirectories,
};
use eventlog_core::{CommandMeta, EventLogError, Expected, NewEvent, StreamId, TenantId};
use serde_json::{Map, Value, json};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs;
use std::path::{Path, PathBuf};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

fn format_time(value: OffsetDateTime) -> Result<String, EventLogError> {
    value.format(&Rfc3339).map_err(backend)
}

fn parse_time(value: &Value, what: &str) -> Result<OffsetDateTime, EventLogError> {
    OffsetDateTime::parse(text(value, what)?, &Rfc3339).map_err(|_| corrupt(what))
}

fn text<'a>(value: &'a Value, what: &str) -> Result<&'a str, EventLogError> {
    value.as_str().ok_or_else(|| corrupt(what))
}

fn field<'a>(map: &'a Map<String, Value>, name: &str) -> Result<&'a Value, EventLogError> {
    map.get(name).ok_or_else(|| corrupt(name))
}

fn closed(map: &Map<String, Value>, allowed: &[&str], what: &str) -> Result<(), EventLogError> {
    if map.keys().all(|key| allowed.contains(&key.as_str())) {
        Ok(())
    } else {
        Err(corrupt(&format!(
            "{what} carries a field this reader does not know"
        )))
    }
}

fn strings(value: &Value, what: &str) -> Result<Vec<String>, EventLogError> {
    value
        .as_array()
        .ok_or_else(|| corrupt(what))?
        .iter()
        .map(|item| text(item, what).map(str::to_owned))
        .collect()
}

/// Whether a group was one stream's append or a tenant's atomic group. They keep their command
/// receipts in different places, so a replay has to call the same entry point the writer did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Append,
    Group,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Group => "group",
        }
    }
}

/// One stream's share of a group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Member {
    pub stream_type: String,
    pub stream_id: String,
    /// What the engine was asked to expect when the group was written. A replay passes it again,
    /// because a group's request fingerprint covers it and a retry must still match.
    pub expected: Expected,
    /// The digests of the events this group appended to the stream, in order.
    pub events: Vec<String>,
}

pub(crate) fn expected_value(expected: Expected) -> Result<Value, EventLogError> {
    match expected {
        Expected::Any => Ok(json!("any")),
        Expected::NoStream => Ok(json!("no-stream")),
        Expected::Exact(version) => Ok(json!(["exact", version])),
        _ => Err(EventLogError::Invalid(
            "only a linear expectation reaches the engine".into(),
        )),
    }
}

fn parse_expected(value: &Value) -> Result<Expected, EventLogError> {
    match value {
        Value::String(text) if text == "any" => Ok(Expected::Any),
        Value::String(text) if text == "no-stream" => Ok(Expected::NoStream),
        Value::Array(items) if items.len() == 2 && items[0] == "exact" => items[1]
            .as_u64()
            .map(Expected::Exact)
            .ok_or_else(|| corrupt("member expectation")),
        _ => Err(corrupt("member expectation")),
    }
}

/// One command, as the file that commits it holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupRecord {
    pub tenant: String,
    pub kind: Kind,
    pub meta: CommandMeta,
    pub members: Vec<Member>,
    /// Blob digests the group committed with its events, sorted.
    pub blobs: Vec<String>,
    pub recorded_at: OffsetDateTime,
}

impl GroupRecord {
    /// The group file's name: a digest of where its receipt lives, so one command identity has
    /// exactly one file and two different ones never share it.
    pub fn key_digest(&self) -> String {
        let scope = match self.kind {
            Kind::Append => {
                let member = &self.members[0];
                json!(["append", self.tenant, member.stream_type, member.stream_id])
            }
            Kind::Group => json!(["group", self.tenant]),
        };
        sha256_hex(&canonical(&json!([scope, self.meta.idempotency_key])))
    }

    pub fn to_value(&self) -> Result<Value, EventLogError> {
        let meta = &self.meta;
        Ok(json!({
            "format": GROUP_FORMAT,
            "tenant": self.tenant,
            "kind": self.kind.as_str(),
            "idempotency_key": meta.idempotency_key,
            "request_hash": meta.request_hash,
            "subject": meta.subject,
            "actor": meta.actor,
            "request_id": meta.request_id,
            "trace_id": meta.trace_id,
            "causation_id": meta.causation_id,
            "causation_depth": meta.causation_depth,
            "occurred_at": format_time(meta.occurred_at)?,
            "members": self.members.iter().map(|member| Ok(json!({
                "stream_type": member.stream_type,
                "stream_id": member.stream_id,
                "expected": expected_value(member.expected)?,
                "events": member.events,
            }))).collect::<Result<Vec<_>, EventLogError>>()?,
            "blobs": self.blobs,
            "recorded_at": format_time(self.recorded_at)?,
        }))
    }

    pub fn from_value(value: &Value) -> Result<Self, EventLogError> {
        let map = value.as_object().ok_or_else(|| corrupt("group file"))?;
        closed(
            map,
            &[
                "format",
                "tenant",
                "kind",
                "idempotency_key",
                "request_hash",
                "subject",
                "actor",
                "request_id",
                "trace_id",
                "causation_id",
                "causation_depth",
                "occurred_at",
                "members",
                "blobs",
                "recorded_at",
            ],
            "a group file",
        )?;
        if text(field(map, "format")?, "group format")? != GROUP_FORMAT {
            return Err(corrupt("group format"));
        }
        let kind = match text(field(map, "kind")?, "group kind")? {
            "append" => Kind::Append,
            "group" => Kind::Group,
            _ => return Err(corrupt("group kind")),
        };
        let causation_id = match field(map, "causation_id")? {
            Value::Null => None,
            other => Some(text(other, "causation id")?.to_owned()),
        };
        let meta = CommandMeta {
            idempotency_key: text(field(map, "idempotency_key")?, "idempotency key")?.to_owned(),
            request_hash: text(field(map, "request_hash")?, "request hash")?.to_owned(),
            subject: text(field(map, "subject")?, "subject")?.to_owned(),
            actor: text(field(map, "actor")?, "actor")?.to_owned(),
            request_id: text(field(map, "request_id")?, "request id")?.to_owned(),
            trace_id: text(field(map, "trace_id")?, "trace id")?.to_owned(),
            causation_id,
            causation_depth: field(map, "causation_depth")?
                .as_u64()
                .and_then(|depth| u32::try_from(depth).ok())
                .ok_or_else(|| corrupt("causation depth"))?,
            occurred_at: parse_time(field(map, "occurred_at")?, "occurred at")?,
            claim: None,
        };
        let mut members = Vec::new();
        for member in field(map, "members")?
            .as_array()
            .ok_or_else(|| corrupt("group members"))?
        {
            let member = member.as_object().ok_or_else(|| corrupt("group member"))?;
            closed(
                member,
                &["stream_type", "stream_id", "expected", "events"],
                "a group member",
            )?;
            members.push(Member {
                stream_type: text(field(member, "stream_type")?, "stream type")?.to_owned(),
                stream_id: text(field(member, "stream_id")?, "stream id")?.to_owned(),
                expected: parse_expected(field(member, "expected")?)?,
                events: strings(field(member, "events")?, "member events")?,
            });
        }
        if members.is_empty() || (kind == Kind::Append && members.len() != 1) {
            return Err(corrupt("group members"));
        }
        Ok(Self {
            tenant: text(field(map, "tenant")?, "group tenant")?.to_owned(),
            kind,
            meta,
            members,
            blobs: strings(field(map, "blobs")?, "group blobs")?,
            recorded_at: parse_time(field(map, "recorded_at")?, "group recorded at")?,
        })
    }
}

/// One event, as its file holds it. Its version is not stored: it is the event's place in the
/// replay order, which a merge can change and a file must not have to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EventRecord {
    pub tenant: String,
    pub stream_type: String,
    pub stream_id: String,
    pub event_id: String,
    pub name: String,
    pub schema_version: u32,
    pub recorded_at: OffsetDateTime,
    pub parents: Vec<String>,
    /// The key digest of the group that committed this event, and its place among all of the
    /// group's events, member after member.
    pub group: String,
    pub index: u64,
    pub data_digest: String,
    pub data: Value,
    /// The reason, once the body has been redacted. The digest still covers the original body
    /// through `data_digest`, so redaction does not break the chain.
    pub redacted: Option<String>,
}

impl EventRecord {
    fn envelope(&self) -> Result<Value, EventLogError> {
        Ok(json!({
            "format": EVENT_FORMAT,
            "tenant": self.tenant,
            "stream_type": self.stream_type,
            "stream_id": self.stream_id,
            "event_id": self.event_id,
            "name": self.name,
            "schema_version": self.schema_version,
            "recorded_at": format_time(self.recorded_at)?,
            "parents": self.parents,
            "group": self.group,
            "index": self.index,
            "data_digest": self.data_digest,
        }))
    }

    /// The digest that names the file and that children name as their parent.
    pub fn digest(&self) -> Result<String, EventLogError> {
        Ok(sha256_hex(&canonical(&self.envelope()?)))
    }

    pub fn to_value(&self) -> Result<Value, EventLogError> {
        let mut value = self.envelope()?;
        let map = value.as_object_mut().expect("an envelope is an object");
        map.insert("data".into(), self.data.clone());
        if let Some(reason) = &self.redacted {
            map.insert("redacted".into(), json!({ "reason": reason }));
        }
        map.insert("digest".into(), Value::String(self.digest()?));
        Ok(value)
    }

    pub fn from_value(value: &Value) -> Result<(Self, String), EventLogError> {
        let map = value.as_object().ok_or_else(|| corrupt("event file"))?;
        closed(
            map,
            &[
                "format",
                "tenant",
                "stream_type",
                "stream_id",
                "event_id",
                "name",
                "schema_version",
                "recorded_at",
                "parents",
                "group",
                "index",
                "data_digest",
                "data",
                "redacted",
                "digest",
            ],
            "an event file",
        )?;
        if text(field(map, "format")?, "event format")? != EVENT_FORMAT {
            return Err(corrupt("event format"));
        }
        let redacted = match map.get("redacted") {
            None => None,
            Some(value) => {
                let reason = value.as_object().ok_or_else(|| corrupt("redaction"))?;
                closed(reason, &["reason"], "a redaction")?;
                Some(text(field(reason, "reason")?, "redaction reason")?.to_owned())
            }
        };
        let record = Self {
            tenant: text(field(map, "tenant")?, "event tenant")?.to_owned(),
            stream_type: text(field(map, "stream_type")?, "stream type")?.to_owned(),
            stream_id: text(field(map, "stream_id")?, "stream id")?.to_owned(),
            event_id: text(field(map, "event_id")?, "event id")?.to_owned(),
            name: text(field(map, "name")?, "event name")?.to_owned(),
            schema_version: field(map, "schema_version")?
                .as_u64()
                .and_then(|version| u32::try_from(version).ok())
                .ok_or_else(|| corrupt("schema version"))?,
            recorded_at: parse_time(field(map, "recorded_at")?, "event recorded at")?,
            parents: strings(field(map, "parents")?, "event parents")?,
            group: text(field(map, "group")?, "event group")?.to_owned(),
            index: field(map, "index")?
                .as_u64()
                .ok_or_else(|| corrupt("event index"))?,
            data_digest: text(field(map, "data_digest")?, "data digest")?.to_owned(),
            data: field(map, "data")?.clone(),
            redacted,
        };
        let stated = text(field(map, "digest")?, "event digest")?.to_owned();
        Ok((record, stated))
    }

    pub fn stream(&self) -> Result<StreamId, EventLogError> {
        StreamId::new(
            TenantId::new(self.tenant.clone())?,
            self.stream_type.clone(),
            self.stream_id.clone(),
        )
    }

    pub fn new_event(&self) -> Result<NewEvent, EventLogError> {
        NewEvent::new(self.name.clone(), self.schema_version, self.data.clone())
    }
}

/// A committed group with its events, in replay order.
#[derive(Clone, Debug)]
pub(crate) struct Committed {
    pub record: GroupRecord,
    /// One list per member, in the member's order.
    pub events: Vec<Vec<EventRecord>>,
    /// The bytes of each blob the group committed that is still present.
    pub blobs: Vec<(String, Vec<u8>)>,
}

/// One tenant's history as the tree holds it.
#[derive(Clone, Debug, Default)]
pub(crate) struct TenantHistory {
    pub identity: Option<String>,
    pub groups: Vec<Committed>,
    /// Every blob file, whether a group bound it or `put_blob` stored it alone.
    pub blobs: Vec<(String, Vec<u8>)>,
}

/// What an open found.
#[derive(Clone, Debug, Default)]
pub(crate) struct Loaded {
    pub tenants: BTreeMap<String, TenantHistory>,
    /// Event files no group commits: a write interrupted before its commit point.
    pub torn: Vec<PathBuf>,
    /// The name of every group file, to tell later whether another writer has added one.
    pub group_files: BTreeSet<PathBuf>,
}

fn read_json(path: &Path) -> Result<Value, EventLogError> {
    let bytes = fs::read(path).map_err(backend)?;
    serde_json::from_slice(&bytes).map_err(|_| corrupt("a file that is not JSON"))
}

fn tenant_of(directory: &Path) -> Result<String, EventLogError> {
    unescape(
        &directory
            .file_name()
            .ok_or_else(|| corrupt("tenant directory"))?
            .to_string_lossy(),
    )
}

/// Every group file name under `root`, for the change check a writer makes under the lock.
pub(crate) fn group_files(root: &Path) -> Result<BTreeSet<PathBuf>, EventLogError> {
    let mut found = BTreeSet::new();
    for tenant in subdirectories(&root.join("tenants"))? {
        for shard in subdirectories(&tenant.join("groups"))? {
            found.extend(json_files(&shard)?);
        }
    }
    Ok(found)
}

/// Read and verify everything under `root`, and put each tenant's groups in replay order.
///
/// # Errors
/// Refuses a file that does not parse, a digest that does not match its content, a group whose
/// member event is missing or does not name the group back, a parent that is not an earlier
/// event of the same stream, and a cycle.
pub(crate) fn load(root: &Path) -> Result<Loaded, EventLogError> {
    let mut loaded = Loaded::default();
    for tenant_directory in subdirectories(&root.join("tenants"))? {
        let segment_name = tenant_of(&tenant_directory)?;
        let mut history = TenantHistory::default();
        let identity_file = tenant_directory.join("identity.json");
        if identity_file.exists() {
            let value = read_json(&identity_file)?;
            history.identity = Some(
                value
                    .get("stream_identity")
                    .and_then(Value::as_str)
                    .ok_or_else(|| corrupt("tenant identity"))?
                    .to_owned(),
            );
        }

        // Every event file, by digest, verified against its content.
        let mut events: BTreeMap<String, (EventRecord, PathBuf)> = BTreeMap::new();
        for type_directory in subdirectories(&tenant_directory.join("streams"))? {
            for stream_directory in subdirectories(&type_directory)? {
                for path in json_files(&stream_directory)? {
                    let (record, stated) = EventRecord::from_value(&read_json(&path)?)?;
                    let digest = record.digest()?;
                    if digest != stated
                        || path
                            .file_stem()
                            .map(|stem| stem.to_string_lossy().into_owned())
                            != Some(digest.clone())
                    {
                        return Err(corrupt("an event file whose digest does not match it"));
                    }
                    if record.redacted.is_none()
                        && sha256_hex(&canonical(&record.data)) != record.data_digest
                    {
                        return Err(corrupt("an event body that does not match its digest"));
                    }
                    if crate::layout::stream_dir(
                        root,
                        &record.tenant,
                        &record.stream_type,
                        &record.stream_id,
                    ) != stream_directory
                    {
                        return Err(corrupt("an event file outside its stream's directory"));
                    }
                    events.insert(digest, (record, path));
                }
            }
        }

        // Every group, with the events it commits.
        let mut committed: BTreeMap<String, Committed> = BTreeMap::new();
        let mut owner: BTreeMap<String, String> = BTreeMap::new();
        for shard in subdirectories(&tenant_directory.join("groups"))? {
            for path in json_files(&shard)? {
                let record = GroupRecord::from_value(&read_json(&path)?)?;
                let key = record.key_digest();
                if group_path(root, &record.tenant, &key) != path {
                    return Err(corrupt("a group file whose name does not match it"));
                }
                let mut members = Vec::with_capacity(record.members.len());
                // An event's index is its place among all of the group's events, member after member.
                let mut index = 0_u64;
                for member in &record.members {
                    let mut list = Vec::with_capacity(member.events.len());
                    for digest in &member.events {
                        let (event, _) = events
                            .get(digest)
                            .ok_or_else(|| corrupt("a group whose event is missing"))?;
                        if event.group != key
                            || event.index != index
                            || event.stream_type != member.stream_type
                            || event.stream_id != member.stream_id
                            || event.tenant != record.tenant
                        {
                            return Err(corrupt("a group whose event names another group"));
                        }
                        if owner.insert(digest.clone(), key.clone()).is_some() {
                            return Err(corrupt("an event two groups commit"));
                        }
                        list.push(event.clone());
                        index += 1;
                    }
                    members.push(list);
                }
                let mut blobs = Vec::new();
                for digest in &record.blobs {
                    if let Ok(bytes) = fs::read(blob_path(root, &record.tenant, digest)) {
                        blobs.push((digest.clone(), bytes));
                    }
                }
                loaded.group_files.insert(path);
                committed.insert(
                    key,
                    Committed {
                        record,
                        events: members,
                        blobs,
                    },
                );
            }
        }
        for (digest, (_, path)) in &events {
            if !owner.contains_key(digest) {
                loaded.torn.push(path.clone());
            }
        }

        // Parents are committed events of the same stream.
        for (digest, group) in &owner {
            let (event, _) = &events[digest];
            for parent in &event.parents {
                let parent_group = owner
                    .get(parent)
                    .ok_or_else(|| corrupt("an event whose parent is not committed"))?;
                let (parent_event, _) = &events[parent];
                if parent_event.stream_type != event.stream_type
                    || parent_event.stream_id != event.stream_id
                {
                    return Err(corrupt("an event whose parent is on another stream"));
                }
                if parent_group == group && parent_event.index >= event.index {
                    return Err(corrupt("an event whose parent follows it in its group"));
                }
            }
        }
        history.groups = replay_order(committed, &owner, &events)?;

        for shard in subdirectories(&tenant_directory.join("blobs"))? {
            let entries = fs::read_dir(&shard).map_err(backend)?;
            let mut files = Vec::new();
            for entry in entries {
                let entry = entry.map_err(backend)?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.starts_with('.') && entry.file_type().map_err(backend)?.is_file() {
                    files.push((name, entry.path()));
                }
            }
            files.sort();
            for (_, path) in files {
                let digest = history_blob_digest(&path)?;
                history
                    .blobs
                    .push((digest, fs::read(&path).map_err(backend)?));
            }
        }
        loaded.tenants.insert(segment_name, history);
    }
    Ok(loaded)
}

fn history_blob_digest(path: &Path) -> Result<String, EventLogError> {
    let name = path
        .file_name()
        .ok_or_else(|| corrupt("blob file"))?
        .to_string_lossy()
        .into_owned();
    unescape(&name)
}

/// The inverse of [`crate::layout::segment`].
pub(crate) fn unescape(segment: &str) -> Result<String, EventLogError> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = segment
                .get(index + 1..index + 3)
                .ok_or_else(|| corrupt("an escaped name"))?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| corrupt("an escaped name"))?);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).map_err(|_| corrupt("an escaped name"))
}

/// Groups in causal order: a group follows every group holding a parent of one of its events.
/// Ties go to the earliest recorded instant, then the key digest, so the order is a function of
/// the files alone and two checkouts of one commit agree on it.
fn replay_order(
    mut committed: BTreeMap<String, Committed>,
    owner: &BTreeMap<String, String>,
    events: &BTreeMap<String, (EventRecord, PathBuf)>,
) -> Result<Vec<Committed>, EventLogError> {
    let mut depends: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut dependents: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for key in committed.keys() {
        depends.entry(key.clone()).or_default();
    }
    for (digest, group) in owner {
        for parent in &events[digest].0.parents {
            let parent_group = &owner[parent];
            if parent_group != group {
                depends
                    .get_mut(group)
                    .expect("every group")
                    .insert(parent_group.clone());
                dependents
                    .entry(parent_group.clone())
                    .or_default()
                    .insert(group.clone());
            }
        }
    }
    let rank = |key: &String, committed: &BTreeMap<String, Committed>| {
        let earliest = committed[key]
            .events
            .iter()
            .flatten()
            .map(|event| event.recorded_at)
            .min()
            .unwrap_or(committed[key].record.recorded_at);
        Reverse((earliest, key.clone()))
    };
    let mut ready: BinaryHeap<Reverse<(OffsetDateTime, String)>> = depends
        .iter()
        .filter(|(_, needs)| needs.is_empty())
        .map(|(key, _)| rank(key, &committed))
        .collect();
    let mut remaining: BTreeMap<String, usize> = depends
        .iter()
        .map(|(key, needs)| (key.clone(), needs.len()))
        .collect();
    let mut order = Vec::with_capacity(committed.len());
    while let Some(Reverse((_, key))) = ready.pop() {
        order.push(key.clone());
        for dependent in dependents.get(&key).into_iter().flatten() {
            let left = remaining.get_mut(dependent).expect("every group");
            *left -= 1;
            if *left == 0 {
                ready.push(rank(dependent, &committed));
            }
        }
    }
    if order.len() != committed.len() {
        return Err(corrupt("groups whose parents form a cycle"));
    }
    Ok(order
        .into_iter()
        .map(|key| committed.remove(&key).expect("every ordered group"))
        .collect())
}
