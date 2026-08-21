//! An event type is permanent. This is what that costs and how it is paid.

use eventlog_conformance::EventVector;
use eventlog_core::{Aggregate, Applied, DomainEvent, EventLogError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Written as `{"amount": n}` in schema version 1 and `{"delta": n, "reason": s}` in version 2.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Moved {
    delta: i64,
    reason: String,
}

impl DomainEvent for Moved {
    fn name(&self) -> &'static str {
        "ledger.moved"
    }

    fn schema_version(&self) -> u32 {
        2
    }

    fn to_data(&self) -> Result<Value, EventLogError> {
        Ok(json!({ "delta": self.delta, "reason": self.reason }))
    }

    fn from_data(name: &str, schema_version: u32, data: &Value) -> Result<Self, EventLogError> {
        if name != "ledger.moved" {
            return Err(EventLogError::Backend(format!("unknown event {name}")));
        }
        match schema_version {
            // The upcaster. Pure, run on read, and never deleted: the bytes it reads are still
            // out there and always will be.
            1 => Ok(Self {
                delta: data
                    .get("amount")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| EventLogError::Backend("v1 body has no amount".to_owned()))?,
                reason: "unrecorded".to_owned(),
            }),
            2 => Ok(Self {
                delta: data
                    .get("delta")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| EventLogError::Backend("v2 body has no delta".to_owned()))?,
                reason: data
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("unrecorded")
                    .to_owned(),
            }),
            other => Err(EventLogError::Backend(format!(
                "no upcaster for ledger.moved v{other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Ledger {
    id: String,
    balance: i64,
    reasons: Vec<String>,
}

impl Aggregate for Ledger {
    type Command = i64;
    type Event = Moved;
    type Error = EventLogError;

    const TYPE: &'static str = "ledger";
    const STATE_SCHEMA_VERSION: u32 = 1;

    fn empty(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            balance: 0,
            reasons: Vec::new(),
        }
    }

    fn apply(&mut self, applied: &Applied<'_, Self::Event>) {
        if let Applied::Happened { event, .. } = applied {
            self.balance += event.delta;
            self.reasons.push(event.reason.clone());
        }
    }

    fn decide(&self, command: &Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(vec![Moved {
            delta: *command,
            reason: "asked".to_owned(),
        }])
    }
}

fn vectors() -> Vec<EventVector> {
    vec![
        EventVector {
            name: "ledger.moved".to_owned(),
            schema_version: 1,
            data: json!({ "amount": 4 }),
        },
        EventVector {
            name: "ledger.moved".to_owned(),
            schema_version: 2,
            data: json!({ "delta": 6, "reason": "asked" }),
        },
    ]
}

#[test]
fn a_body_written_under_an_older_version_still_folds() {
    let expected = Ledger {
        id: "l-1".to_owned(),
        balance: 10,
        reasons: vec!["unrecorded".to_owned(), "asked".to_owned()],
    };
    eventlog_conformance::assert_vectors_fold::<Ledger>("l-1", &vectors(), &expected);
}

#[test]
fn a_version_with_no_upcaster_is_named_rather_than_guessed() {
    let orphan = vec![EventVector {
        name: "ledger.moved".to_owned(),
        schema_version: 3,
        data: json!({ "whatever": true }),
    }];
    let error = eventlog_conformance::fold_vectors::<Ledger>("l-1", &orphan)
        .expect_err("a version nobody wrote an upcaster for");
    assert!(
        format!("{error}").contains("no upcaster for ledger.moved v3"),
        "the failure says which version stopped folding: {error}"
    );
}

#[test]
fn a_committed_vector_still_folds_when_loaded_from_disk() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    for (index, vector) in vectors().iter().enumerate() {
        std::fs::write(
            directory.path().join(format!("{index:03}.json")),
            serde_json::to_vec_pretty(vector).expect("serialisable"),
        )
        .expect("written");
    }
    let loaded = eventlog_conformance::load_vectors(directory.path());
    assert_eq!(loaded.len(), 2);
    let folded = eventlog_conformance::fold_vectors::<Ledger>("l-1", &loaded).expect("folds");
    assert_eq!(folded.balance, 10);
}
