//! Administrative attachment and complete rebuilding of admitted inline projections.

use std::sync::Arc;

use crate::{BoxFuture, EventLogError, Projector, TenantId};

/// The result of one complete authoritative tenant fold.
///
/// This is a transient Rust value. It has no persisted or serialized representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InlineRebuildResult {
    /// Every authoritative tenant event successfully passed to the projector.
    pub applied: u64,
    /// The actual final global sequence, or zero when the tenant has no events.
    pub position: u64,
}

/// Provider-native administration for an already admitted inline projector.
///
/// Attachment validates durable structure and installs code only in this handle. Rebuild selects
/// that exact installed instance by name and atomically replaces its selected tenant rows.
pub trait InlineProjectionAdmin: Send + Sync + 'static {
    /// Attach code to projection tables that were admitted already, without changing durable state.
    fn attach_inline_existing(
        &self,
        projector: Arc<dyn Projector>,
    ) -> BoxFuture<'_, Result<(), EventLogError>>;

    /// Rebuild the named attached projector for one tenant from complete committed history.
    fn rebuild_inline_projection<'a>(
        &'a self,
        projector_name: &'a str,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<InlineRebuildResult, EventLogError>>;
}
