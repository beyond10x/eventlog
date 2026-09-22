//! Non-provisioning, non-recovering file history inspection.
use eventlog_core::{
    BoxFuture, HistoryInspection, InspectHistory, InspectionError, InspectionLimits, RecordedEvent,
    TenantId,
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

/// A source selector with no canonical writer authority.
#[derive(Clone, Debug)]
pub struct FileHistoryInspector {
    root: PathBuf,
}

impl FileHistoryInspector {
    /// Select an existing source. Construction performs no I/O; inspection admits it.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_owned(),
        }
    }
}

impl InspectHistory for FileHistoryInspector {
    fn inspect_history<'a>(
        &'a self,
        tenant: &'a TenantId,
        limits: InspectionLimits,
    ) -> BoxFuture<'a, Result<HistoryInspection, InspectionError>> {
        let root = self.root.clone();
        let tenant = tenant.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || inspect(&root, tenant, limits))
                .await
                .map_err(|_| InspectionError::SourceBusy)?
        })
    }
}

fn inspect(
    root: &Path,
    tenant: TenantId,
    limits: InspectionLimits,
) -> Result<HistoryInspection, InspectionError> {
    let journal = crate::journal::inspect(root, limits.source_bytes)?;
    validate_envelope_fields(&journal.transactions)?;
    let state = crate::state::State::replay(&journal.transactions)
        .map_err(|_| InspectionError::CorruptSource)?;
    let stream_identity = state.identities.get(tenant.as_str()).cloned();
    if stream_identity.as_ref().is_some_and(String::is_empty) {
        return Err(InspectionError::CorruptSource);
    }
    let mut events = Vec::new();
    let mut ids = BTreeSet::new();
    let mut envelope_bytes = 0_u64;
    for event in state
        .events
        .into_values()
        .filter(|event| event.tenant == tenant)
    {
        event.validate_for_inspection()?;
        if !ids.insert(event.event_id.clone()) {
            return Err(InspectionError::CorruptSource);
        }
        if u64::try_from(events.len()).map_err(|_| InspectionError::LimitExceeded)? >= limits.events
        {
            return Err(InspectionError::LimitExceeded);
        }
        let length = u64::try_from(
            serde_json::to_vec(&event)
                .map_err(|_| InspectionError::CorruptSource)?
                .len(),
        )
        .map_err(|_| InspectionError::LimitExceeded)?;
        envelope_bytes = envelope_bytes
            .checked_add(length)
            .ok_or(InspectionError::LimitExceeded)?;
        if envelope_bytes > limits.envelope_bytes {
            return Err(InspectionError::LimitExceeded);
        }
        events.push(event);
    }
    Ok(HistoryInspection {
        tenant,
        stream_identity,
        events,
    })
}

// The native fold already checks operation shape. RecordedEvent's general-purpose
// deserializer permits unknown fields; strict inspection must check the retained
// keys before that fold can discard them. User data remains an arbitrary object.
fn validate_envelope_fields(transactions: &[serde_json::Value]) -> Result<(), InspectionError> {
    for transaction in transactions {
        for operation in transaction
            .as_array()
            .ok_or(InspectionError::CorruptSource)?
        {
            if operation
                .get("operation")
                .and_then(serde_json::Value::as_str)
                != Some("event")
            {
                continue;
            }
            let raw = operation
                .get("event")
                .ok_or(InspectionError::CorruptSource)?;
            let event: RecordedEvent =
                serde_json::from_value(raw.clone()).map_err(|_| InspectionError::CorruptSource)?;
            let known = serde_json::to_value(event).map_err(|_| InspectionError::CorruptSource)?;
            if raw
                .as_object()
                .ok_or(InspectionError::CorruptSource)?
                .keys()
                .any(|key| known.get(key).is_none())
            {
                return Err(InspectionError::CorruptSource);
            }
        }
    }
    Ok(())
}
