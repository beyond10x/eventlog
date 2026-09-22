//! An opt-in history observation that grants no writer or recovery authority.
use crate::{
    BoxFuture, MAX_CAUSATION_DEPTH, RecordedEvent, TenantId, validate_field, validate_identity,
};

impl RecordedEvent {
    /// Check the retained envelope against the kit's append admission rules.
    ///
    /// Stream ordering and identity uniqueness still require the complete history.
    /// Schema version zero is legal, just as it is for a newly appended event.
    ///
    /// # Errors
    /// Returns a payload-free refusal for redaction or malformed metadata.
    pub fn validate_for_inspection(&self) -> Result<(), InspectionError> {
        if self.redacted_at.is_some() {
            return Err(InspectionError::RedactedHistory);
        }
        if self.global_seq == 0
            || self.version == 0
            || self.causation_depth > MAX_CAUSATION_DEPTH
            || !self.data.is_object()
        {
            return Err(InspectionError::CorruptSource);
        }
        self.stream().map_err(|_| InspectionError::CorruptSource)?;
        for (name, value) in [
            ("event id", self.event_id.as_str()),
            ("event name", self.name.as_str()),
            ("request id", self.request_id.as_str()),
            ("trace id", self.trace_id.as_str()),
        ] {
            validate_field(name, value).map_err(|_| InspectionError::CorruptSource)?;
        }
        for (name, value) in [("subject", &self.subject), ("actor", &self.actor)] {
            validate_identity(name, value).map_err(|_| InspectionError::CorruptSource)?;
        }
        if let Some(id) = &self.causation_id {
            validate_field("causation id", id).map_err(|_| InspectionError::CorruptSource)?;
        }
        Ok(())
    }
}

/// Explicit admission and result caps. Zero is a real limit, never a default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InspectionLimits {
    pub source_bytes: u64,
    pub events: u64,
    pub envelope_bytes: u64,
}

/// Complete decoded history for one tenant, not a physical store backup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryInspection {
    pub tenant: TenantId,
    /// Original stored identity, if any. Inspection never creates one.
    pub stream_identity: Option<String>,
    pub events: Vec<RecordedEvent>,
}

/// A refusal carries no partial history or retained payload in its diagnostic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InspectionError {
    #[error("inspection source is missing")]
    MissingSource,
    #[error("inspection source or platform is unsupported")]
    UnsupportedSource,
    #[error("inspection requires recovery or unsupported journal state")]
    RecoveryRequired,
    #[error("inspection source is corrupt")]
    CorruptSource,
    #[error("inspection history has redacted records")]
    RedactedHistory,
    #[error("inspection source is busy or unavailable")]
    SourceBusy,
    #[error("inspection source changed")]
    SourceChanged,
    #[error("inspection exceeds the explicit limit")]
    LimitExceeded,
}

/// Native consistent history inspection, with no ordinary-store fallback.
pub trait InspectHistory: Send + Sync {
    /// Inspect one tenant without changing persistent source bytes or entries.
    ///
    /// # Errors
    /// Returns a named refusal, with no partial value, when the source cannot be
    /// admitted unchanged or the complete history exceeds the supplied limits.
    fn inspect_history<'a>(
        &'a self,
        tenant: &'a TenantId,
        limits: InspectionLimits,
    ) -> BoxFuture<'a, Result<HistoryInspection, InspectionError>>;
}
