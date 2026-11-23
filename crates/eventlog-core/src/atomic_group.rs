//! Ordered multi-stream appends, independently selectable from the legacy single-stream port.
use crate::{
    AppendResult, BoxFuture, CommandMeta, EventLogError, EventStore, Expected, Guard, NewEvent,
    NoGuard, StreamId, TenantId, request_hash, validate_append,
};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use serde_json::{Map, Value, json};
use std::sync::Arc;

/// One ordered entry; repeated streams observe earlier entries in the same transaction.
#[derive(Clone, Debug)]
pub struct StreamAppend {
    pub stream: StreamId,
    pub expected: Expected,
    pub events: Vec<NewEvent>,
}

/// One tenant-scoped command identity covers the complete ordered request.
#[derive(Clone, Debug)]
pub struct AppendGroup {
    pub tenant: TenantId,
    pub appends: Vec<StreamAppend>,
    pub meta: CommandMeta,
}

impl AppendGroup {
    /// Validate all entries and hash actual content, not just the caller's claimed digest.
    ///
    /// # Errors
    /// Refuses empty groups, cross-tenant entries, invalid events/metadata and stream claims.
    /// A stream claim names one result range and cannot represent an atomic group.
    pub fn fingerprint(&self) -> Result<String, EventLogError> {
        self.meta.validate()?;
        if self.appends.is_empty() {
            return Err(EventLogError::Invalid(
                "an append group must contain entries".into(),
            ));
        }
        if self.meta.claim.is_some() {
            return Err(EventLogError::Invalid(
                "stream claims cannot name an append group".into(),
            ));
        }
        let mut entries = Vec::with_capacity(self.appends.len());
        for append in &self.appends {
            if append.stream.tenant() != &self.tenant {
                return Err(EventLogError::Invalid(
                    "append group crosses tenants".into(),
                ));
            }
            validate_append(&append.events, &self.meta)?;
            let expected = match append.expected {
                Expected::Any => json!(["any"]),
                Expected::NoStream => json!(["no-stream"]),
                Expected::Exact(version) => json!(["exact", version]),
                // The head set is part of the request: two merges over different heads are
                // different requests, even under one idempotency key.
                Expected::Merge(heads) => json!(["merge", heads.to_hex()]),
            };
            let events: Vec<_> = append.events.iter().map(event_entry).collect();
            entries.push(Spliced::new(
                json!({"stream":append.stream,"expected":expected,"events":null}),
                "events",
                events,
            ));
        }
        // Exhaustive destructuring makes an added metadata field require an explicit hash decision.
        let CommandMeta {
            idempotency_key,
            request_hash: digest,
            subject,
            actor,
            request_id,
            trace_id,
            causation_id,
            causation_depth,
            occurred_at,
            claim: _,
        } = &self.meta;
        request_hash(&Spliced::new(
            json!({"format":"eventlog-append-group/1","tenant":self.tenant,
            "entries":null,"meta":{"key":idempotency_key,"digest":digest,"subject":subject,
            "actor":actor,"request_id":request_id,"trace_id":trace_id,"causation_id":causation_id,
            "causation_depth":causation_depth,"occurred_at":occurred_at}}),
            "entries",
            entries,
        ))
    }
}

/// One event as the fingerprint has always written it, borrowing the body instead of copying it.
fn event_entry(event: &NewEvent) -> Spliced<&Value> {
    // Exhaustive destructuring: an added event field needs an explicit hash decision, as the
    // derived serialization this replaces would have hashed it.
    let NewEvent {
        name,
        schema_version,
        data,
    } = event;
    Spliced::new(
        json!({"name":name,"schema_version":schema_version,"data":null}),
        "data",
        data,
    )
}

/// A JSON object written in its skeleton's own key order, one value of which is borrowed.
///
/// The fingerprint hashes the bytes `serde_json` writes for an owned `Value`, whose key order is
/// the map's (sorted, or insertion order under `preserve_order`). Building the skeleton with the
/// same `json!` literal and writing the borrowed value in its slot keeps those bytes exactly, so
/// recorded group fingerprints still recognise their retries, without a second copy of the bodies.
struct Spliced<T> {
    skeleton: Map<String, Value>,
    key: &'static str,
    value: T,
}

impl<T: Serialize> Spliced<T> {
    fn new(skeleton: Value, key: &'static str, value: T) -> Self {
        let Value::Object(skeleton) = skeleton else {
            unreachable!("a json! object literal is an object")
        };
        Self {
            skeleton,
            key,
            value,
        }
    }
}

impl<T: Serialize> Serialize for Spliced<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.skeleton.len()))?;
        for (key, value) in &self.skeleton {
            if key == self.key {
                map.serialize_entry(key, &self.value)?;
            } else {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

/// Ordered entry results. A retry has the original IDs, positions and version ranges.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendGroupResult {
    pub appends: Vec<AppendResult>,
    pub deduplicated: bool,
}

/// Durable bookkeeping stores coordinates, never a second copy of event bodies.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupRange {
    pub stream: StreamId,
    pub first_version: u64,
    pub last_version: u64,
}

/// What [`AtomicEventStore::append_group_guarded_with_blobs`]'s default refuses with.
///
/// A stable phrase rather than a new error variant: a provider that has not implemented the
/// single-barrier form is refusing a capability, which is what [`EventLogError::Invalid`] already
/// carries elsewhere in this kit (`ProjectionStore::get_blob`'s default says "transaction blob
/// reads are unavailable" in the same shape). A caller matches on it to decide whether to fall
/// back to a blob at a time; the conformance exercise matches on it to decide which half of the
/// contract applies to the provider in front of it.
pub const UNAVAILABLE: &str = "guarded blob-bearing append groups are unavailable";

/// A provider must implement one transaction; there is deliberately no append-loop fallback.
pub trait AtomicEventStore: EventStore {
    fn append_group<'a>(
        &'a self,
        group: &'a AppendGroup,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        self.append_group_guarded(group, Arc::new(NoGuard))
    }

    /// Admission runs once, before entries, in the same transaction as their inline projectors.
    /// Retries return the original result without repeating admission or projections.
    fn append_group_guarded<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>>;

    /// Commit a guarded group together with the blobs it binds, under the group's own barrier.
    ///
    /// This is [`AtomicEventStore::append_group_guarded`] and [`EventStore::put_blob`] in one
    /// transaction, for the caller that has both a guard and a batch of blobs — a migration
    /// importing many boundaries at once is the case it exists for, and the reason it is on the
    /// port rather than on one provider is that such a caller holds a trait object and cannot
    /// name a provider's inherent method.
    ///
    /// **The guarantee is the same on every provider, because a provider that cannot make it
    /// refuses.** Admission runs before a byte of the batch is written, so a refused guard
    /// publishes neither the group nor a blob. A caller holding a trait object cannot name the
    /// provider underneath it, so a default that published first and asked afterwards would make
    /// this method mean one thing on one provider and the opposite on another — and
    /// content-addressed storage has no way to take a published blob back. A blob row is a
    /// binding, not scratch: nothing reference-counts it and nothing sweeps it.
    ///
    /// **The default therefore fails closed.** It writes nothing, commits nothing and refuses
    /// with [`UNAVAILABLE`] — this provider does not implement guarded blob-bearing groups. A
    /// caller that wants the slow path can still write each blob with [`EventStore::put_blob`]
    /// and then call [`AtomicEventStore::append_group_guarded`], which is the same sequence the
    /// old default ran; what it cannot do is get that sequence under a name that promises the
    /// batch was not published when the guard refused.
    ///
    /// **A retry that carries a batch is admitted again, so the guard must tolerate its own
    /// commit.** The batch is part of what such a retry is asking about, and a store that
    /// answered it before running the guard would let anyone replaying a committed key learn
    /// whether a digest is bound and whether bytes match. So admission runs first, which means a
    /// guard can be presented a second time with a group it already admitted and committed: it
    /// has to be idempotent over that, and refuse only what is genuinely somebody else's. A guard
    /// that reads "this subject is occupied" without checking *by what* will refuse the retry
    /// that idempotency exists to serve. A retry carrying **no** batch — every caller of
    /// [`AtomicEventStore::append_group_guarded`] — is unaffected and does not repeat admission.
    ///
    /// # Errors
    /// [`EventLogError::Invalid`] carrying [`UNAVAILABLE`] from the default. From a provider that
    /// implements it: whatever the group and each blob refuse on their own paths, publishing
    /// nothing on any refusing path. From a guard re-run on a batch-bearing retry: whatever that
    /// guard refuses.
    fn append_group_guarded_with_blobs<'a>(
        &'a self,
        group: &'a AppendGroup,
        admission: Arc<dyn Guard>,
        blobs: &'a [(String, Vec<u8>)],
    ) -> BoxFuture<'a, Result<AppendGroupResult, EventLogError>> {
        let _ = (group, admission, blobs);
        Box::pin(async { Err(EventLogError::Invalid(UNAVAILABLE.into())) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeadSetDigest;

    /// The fingerprint as 0.9.4 computed it: owned entries inside one owned value, hashed whole.
    /// Kept verbatim so a streaming implementation is held to the same bytes.
    fn legacy_fingerprint(group: &AppendGroup) -> String {
        let entries: Vec<Value> = group
            .appends
            .iter()
            .map(|append| {
                let expected = match append.expected {
                    Expected::Any => json!(["any"]),
                    Expected::NoStream => json!(["no-stream"]),
                    Expected::Exact(version) => json!(["exact", version]),
                    Expected::Merge(heads) => json!(["merge", heads.to_hex()]),
                };
                json!({"stream":append.stream,"expected":expected,"events":append.events})
            })
            .collect();
        let meta = &group.meta;
        request_hash(
            &json!({"format":"eventlog-append-group/1","tenant":group.tenant,
            "entries":entries,"meta":{"key":meta.idempotency_key,"digest":meta.request_hash,
            "subject":meta.subject,"actor":meta.actor,"request_id":meta.request_id,
            "trace_id":meta.trace_id,"causation_id":meta.causation_id,
            "causation_depth":meta.causation_depth,"occurred_at":meta.occurred_at}}),
        )
        .unwrap()
    }

    fn meta(causation_id: Option<&str>) -> CommandMeta {
        CommandMeta {
            idempotency_key: "fixture-key".into(),
            request_hash: "caller-hash".into(),
            subject: "subject-1".into(),
            actor: "actor-1".into(),
            request_id: "request-1".into(),
            trace_id: "trace-1".into(),
            causation_id: causation_id.map(str::to_owned),
            causation_depth: 3,
            occurred_at: time::OffsetDateTime::from_unix_timestamp_nanos(1_795_239_429_916_189_123)
                .unwrap()
                .to_offset(time::UtcOffset::from_hms(5, 30, 0).unwrap()),
            claim: None,
        }
    }

    /// Every expectation variant, keys written out of order, escapes, non-ASCII, numbers of each
    /// kind and nesting: the places a hand-written serialization could drift from the old one.
    fn mixed_group() -> AppendGroup {
        let tenant = TenantId::new("fixture-tenant").unwrap();
        let stream = |id: &str| StreamId::new(tenant.clone(), "item", id).unwrap();
        let event =
            |name: &str, version: u32, data: Value| NewEvent::new(name, version, data).unwrap();
        AppendGroup {
            tenant: tenant.clone(),
            meta: meta(Some("cause-1")),
            appends: vec![
                StreamAppend {
                    stream: stream("one"),
                    expected: Expected::NoStream,
                    events: vec![
                        event(
                            "item.created",
                            1,
                            json!({"zeta":1,"alpha":"quote \" slash \\ tab \t nl \n ctl \u{1}",
                                "mid":{"y":[1,-2,3.5,1e300,u64::MAX,i64::MIN],"x":null,"w":true}}),
                        ),
                        event(
                            "item.named",
                            7,
                            json!({"name":"Grüße, 世界 😀","empty":{},"list":[]}),
                        ),
                    ],
                },
                StreamAppend {
                    stream: stream("two"),
                    expected: Expected::Exact(42),
                    events: vec![event("item.moved", 2, json!({"to":"b"}))],
                },
                StreamAppend {
                    stream: stream("one"),
                    expected: Expected::Any,
                    events: vec![event("item.seen", 1, json!({"by":"c","at":0.1}))],
                },
                StreamAppend {
                    stream: stream("three"),
                    expected: Expected::Merge(HeadSetDigest::of(&["head-b", "head-a"])),
                    events: vec![event("item.merged", 1, json!({}))],
                },
            ],
        }
    }

    #[test]
    fn group_fingerprint_keeps_the_bytes_existing_requests_were_recorded_with() {
        let group = mixed_group();
        let fingerprint = group.fingerprint().unwrap();
        assert_eq!(fingerprint, legacy_fingerprint(&group));
        // Computed by eventlog 0.9.4 (b8582219) for this fixture. A changed digest makes every
        // stored group fingerprint stop recognizing its retry.
        assert_eq!(
            fingerprint,
            "2fbbb9631ad33fd0bdcc495f65e8059d2c9110d786ac585e90ffcfbfb2fccae0"
        );

        let mut without_cause = group.clone();
        without_cause.meta = meta(None);
        assert_eq!(
            without_cause.fingerprint().unwrap(),
            legacy_fingerprint(&without_cause)
        );
        assert_ne!(without_cause.fingerprint().unwrap(), fingerprint);
    }
}
