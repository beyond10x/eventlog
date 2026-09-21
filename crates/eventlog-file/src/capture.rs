//! One tenant's complete observation, read from committed files and nothing else.
//!
//! The ordinary store entry point is not usable for this. `FileEventStore::open` and every
//! `transaction` after it initialize a missing store, recover a pending append or privacy intent,
//! rewrite `events.jsonl`, drop stale snapshot caches and delete unbound blob objects — before the
//! read closure runs. An inspector that has to mutate a store to look at it has already changed
//! the thing it came to observe, so capture uses the strict reader instead and holds the same
//! runtime mutex and interprocess `writer.lock` that every writer holds.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use eventlog_core::{
    BoundBlobs, BoxFuture, CaptureBudget, CaptureError, CaptureLimits, CaptureMaterial,
    CapturedBlob, CapturedProjection, ConsistentTenantCapture, DeferredBlob, DeferredTenantCapture,
    EventLogError, ProjectionCaptureRefusal, ProjectionSpec, RecordedEvent, TenantCapture,
    TenantId, order_blobs, order_deferred_blobs, order_rows, validate_capture_request,
    validate_captured_digest, validate_captured_order,
};
use tokio::sync::Mutex;

use crate::{
    FileEventStore,
    journal::{self, Manifest},
    root_path,
    state::{Blob, State},
};

/// The committed history one handle has verified, as a *reader* verified it.
///
/// Deliberately not [`crate::Verified`]. That is a writer's view: it carries the frames a
/// transaction appends onto, and the promise that every object its fold binds has been hashed. A
/// reader makes neither promise — it hashes the content it hands out and nothing else, because a
/// capture has no business refusing over another tenant's object — and it needs neither the
/// frames nor the promise. Two types rather than a flag on one: a transaction cannot pick this
/// up, and that is enforced by the compiler rather than by a line somebody has to keep right.
pub(crate) struct Observed {
    /// The head these bytes were committed under, and the head this view is good for.
    manifest: Manifest,
    /// The fold of the whole committed history.
    state: State,
    /// The hash of the committed bytes the fold was built from. A resumed reader re-reads exactly
    /// those bytes and refuses to reuse the view when they are not them, so a committed frame
    /// damaged in place after a capture is never served from memory.
    content: journal::Content,
}

/// What one handle has observed: the committed head it last accepted, and — when it still has
/// one — the verified view behind that head, so the next capture pays for what changed.
///
/// The two travel together under one lock because they are one fact. The view is used only while
/// its own manifest is still the observed head, so any path that moves the head retires it.
#[derive(Default)]
struct Observation {
    observed: Option<Manifest>,
    view: Option<Observed>,
}

/// A read-only entry to an existing store.
///
/// Opening this handle and capturing through it both use the strict path, so an inspector never
/// has to call the mutating opener first just to look. It implements [`ConsistentTenantCapture`]
/// and nothing else: there is no method here that could write, so there is none to forget to guard.
pub struct FileTenantCapture {
    root: PathBuf,
    observation: Mutex<Observation>,
}

impl FileTenantCapture {
    /// Open an existing store without creating, recovering or cleaning anything.
    ///
    /// # Errors
    /// Returns [`CaptureError::Store`] when the root, the lock or the manifest is not there —
    /// never initialization — [`CaptureError::RecoveryRequired`] for a pending durable intent, and
    /// [`CaptureError::Corrupt`] for committed history that fails its own integrity checks.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, CaptureError> {
        let root = root_path(path.as_ref())?;
        let probe = root.clone();
        let manifest =
            blocking(move || journal::open_strict(&probe).map(|strict| strict.manifest)).await?;
        // No view yet: building one here would mean folding the history at open, which is work
        // this handle may never be asked for and a refusal this entry point does not own. The
        // first capture builds it from the strict read it does anyway.
        Ok(Self {
            root,
            observation: Mutex::new(Observation {
                observed: Some(manifest),
                view: None,
            }),
        })
    }
}

impl ConsistentTenantCapture for FileTenantCapture {
    fn capture_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<TenantCapture, CaptureError>> {
        Box::pin(async move {
            let observation = &mut *self.observation.lock().await;
            capture(
                &self.root,
                &mut observation.observed,
                &mut observation.view,
                tenant,
                projections,
                limits,
                observe,
            )
            .await
        })
    }

    fn capture_tenant_deferred<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<DeferredTenantCapture, CaptureError>> {
        Box::pin(async move {
            let observation = &mut *self.observation.lock().await;
            capture(
                &self.root,
                &mut observation.observed,
                &mut observation.view,
                tenant,
                projections,
                limits,
                observe_deferred,
            )
            .await
        })
    }
}

impl ConsistentTenantCapture for FileEventStore {
    fn capture_tenant<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<TenantCapture, CaptureError>> {
        Box::pin(async move {
            // The same runtime mutex every ordinary operation takes, then the same interprocess
            // lock, then the same read: one implementation serves both entry points.
            let runtime = &mut *self.runtime.lock().await;
            capture(
                &self.root,
                &mut runtime.observed,
                &mut runtime.captured,
                tenant,
                projections,
                limits,
                observe,
            )
            .await
        })
    }

    fn capture_tenant_deferred<'a>(
        &'a self,
        tenant: &'a TenantId,
        projections: &'a [ProjectionSpec],
        limits: CaptureLimits,
    ) -> BoxFuture<'a, Result<DeferredTenantCapture, CaptureError>> {
        Box::pin(async move {
            let runtime = &mut *self.runtime.lock().await;
            capture(
                &self.root,
                &mut runtime.observed,
                &mut runtime.captured,
                tenant,
                projections,
                limits,
                observe_deferred,
            )
            .await
        })
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, CaptureError> + Send + 'static,
) -> Result<T, CaptureError> {
    tokio::task::spawn_blocking(work).await.map_err(|_| {
        CaptureError::Store(EventLogError::Backend(
            "file store worker panicked".to_owned(),
        ))
    })?
}

/// How one tenant is observed inside a strict read, given the fold that read produced.
///
/// A function pointer and not a closure: the two observations differ only in what they do with
/// the bindings, and everything around them — the lock, the resumed view, the divergence guard
/// and the rule about which refusals may be answered ahead of it — is one body serving both.
/// There is no second copy of that reasoning to keep in step with this one.
type Observe<T> =
    fn(&Path, &State, &TenantId, &[ProjectionSpec], CaptureLimits) -> Result<T, CaptureError>;

async fn capture<T: Send + 'static>(
    root: &Path,
    observed: &mut Option<Manifest>,
    view: &mut Option<Observed>,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    limits: CaptureLimits,
    observe: Observe<T>,
) -> Result<T, CaptureError> {
    validate_capture_request(tenant, projections)?;
    let root = root.to_owned();
    let previous = observed.clone();
    // Taken, not borrowed, and filtered against the head this handle actually holds: a capture
    // that refuses leaves no view behind, so the next one reads everything again rather than
    // trusting a view assembled beside a refusal; and any entry point that moved the observed head
    // past what this view folded has already retired it by moving it. The rule and the reason are
    // `FileEventStore::transaction`'s.
    let reusable = view
        .take()
        .filter(|view| Some(&view.manifest) == previous.as_ref());
    let owner = tenant.clone();
    let requested = projections.to_vec();
    let (manifest, extended, verified, outcome) = blocking(move || {
        observed_history(
            &root,
            previous.as_ref(),
            reusable,
            &owner,
            &requested,
            limits,
            observe,
        )
    })
    .await?;
    // The design names exactly two refusals that may be answered from validated state ahead of the
    // per-handle guard: a tenant with no stored identity, and a history that was redacted. Both
    // are facts about the tenant rather than about this observation, and both stay true across the
    // rewrite that invalidated the handle. Everything else — a value, a crossed cap, an
    // unavailable projection, corruption — describes the content of one particular history, and a
    // handle whose observed history was replaced has no business describing the new one. Either
    // way no refusal lets this handle quietly adopt a history it never observed: `observed` moves
    // only when a value passes the guard.
    if matches!(
        outcome,
        Err(CaptureError::TenantIdentityMissing | CaptureError::RedactedHistory)
    ) {
        return outcome;
    }
    if !extended {
        return Err(CaptureError::Store(EventLogError::Backend(
            "file history diverged from this handle's observed history".to_owned(),
        )));
    }
    let value = outcome?;
    *observed = Some(manifest);
    *view = Some(verified);
    Ok(value)
}

/// Read the committed history under the writers' lock and observe one tenant in it.
///
/// With a verified view of `previous` in hand, [`journal::resume_strict`] compares
/// `manifest.json` with it and re-hashes the committed bytes behind it: an unchanged head costs
/// the lock, that comparison and one raw pass over those bytes, and a longer history costs the
/// same pass plus the frames past the observed length, chained from the observed digest and
/// folded onto the state the handle already had. Anything else — no view, a pending recovery
/// intent, a new epoch, a shorter or unchained file, or a committed prefix that is no longer the
/// bytes this handle verified — falls to [`journal::open_strict`], which rereads and rechains
/// everything and refuses exactly as it always has.
///
/// The resumed path answers the divergence guard without asking it. `resume_strict` establishes
/// strictly more than [`journal::extends_observed`] can: that the committed prefix is byte for
/// byte the history this handle verified, and that every frame past it chains from the head it
/// observed to the manifest that commits them.
fn observed_history<T>(
    root: &Path,
    previous: Option<&Manifest>,
    reusable: Option<Observed>,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    limits: CaptureLimits,
    observe: Observe<T>,
) -> Result<(Manifest, bool, Observed, Result<T, CaptureError>), CaptureError> {
    if let (Some(previous), Some(reusable)) = (previous, reusable)
        && let Some(resumed) = journal::resume_strict(root, previous, &reusable.content)
    {
        let mut state = reusable.state;
        #[cfg(test)]
        crate::cost::charge(root, |cost| {
            cost.frames_folded += resumed.fresh.len() as u64;
        });
        for transaction in &resumed.fresh {
            state.fold(transaction).map_err(|_| CaptureError::Corrupt {
                material: CaptureMaterial::Journal,
            })?;
        }
        // The observation runs inside the value that owns the lock, exactly as the strict reader
        // below does it: the blob objects it reads are read under the lock, and the lock is
        // released by `read_under_lock` returning, not by a statement anybody has to keep above
        // this one.
        let (outcome, manifest, _, content) =
            resumed.read_under_lock(|| observe(root, &state, tenant, projections, limits));
        return Ok((
            manifest.clone(),
            true,
            Observed {
                manifest,
                state,
                content,
            },
            outcome,
        ));
    }
    let strict = journal::open_strict(root)?;
    let extended = match previous {
        Some(previous) => {
            #[cfg(test)]
            crate::cost::charge(root, |cost| {
                cost.frames_reencoded += previous.sequence;
            });
            journal::extends_observed(&strict.manifest, &strict.transactions, previous)?
        }
        None => true,
    };
    #[cfg(test)]
    crate::cost::charge(root, |cost| {
        cost.frames_folded += strict.transactions.len() as u64;
    });
    let state = State::replay(&strict.transactions).map_err(|_| CaptureError::Corrupt {
        material: CaptureMaterial::Journal,
    })?;
    let (outcome, manifest, _, content) =
        strict.read_under_lock(|| observe(root, &state, tenant, projections, limits));
    Ok((
        manifest.clone(),
        extended,
        Observed {
            manifest,
            state,
            content,
        },
        outcome,
    ))
}

/// Everything one tenant's observation decides before it reaches the bindings.
///
/// The precedence here is the design's, in its order — identity, then redaction, then projection
/// availability, then the event cap — and it is one body rather than two so that the eager and
/// deferred observations cannot answer a refusal in different orders. What follows it differs;
/// this does not.
fn observed_prefix(
    state: &State,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    budget: &mut CaptureBudget,
) -> Result<(String, Vec<RecordedEvent>, Vec<ProjectionSpec>), CaptureError> {
    // Identity first: a tenant nobody provisioned has no observation to refuse on other grounds,
    // and ordinary append does not provision one, so append-then-redaction reaches exactly here.
    let Some(identity) = state.identities.get(tenant.as_str()) else {
        return Err(CaptureError::TenantIdentityMissing);
    };
    if identity.is_empty() {
        return Err(CaptureError::Corrupt {
            material: CaptureMaterial::Identity,
        });
    }
    if state
        .events
        .values()
        .any(|event| &event.tenant == tenant && event.is_redacted())
    {
        return Err(CaptureError::RedactedHistory);
    }

    let mut admitted = Vec::with_capacity(projections.len());
    for specification in projections {
        admit_projection(state, tenant, specification)?;
        admitted.push(*specification);
    }

    let mut events = Vec::new();
    for event in state
        .events
        .values()
        .filter(|event| &event.tenant == tenant)
    {
        budget.admit_event(event)?;
        events.push(event.clone());
    }
    validate_captured_order(tenant, &events)?;
    Ok((identity.clone(), events, admitted))
}

/// The requested materializations, charged after the bindings exactly as the design orders them.
fn observed_projections(
    state: &State,
    tenant: &TenantId,
    admitted: Vec<ProjectionSpec>,
    budget: &mut CaptureBudget,
) -> Result<Vec<CapturedProjection>, CaptureError> {
    let mut captured = Vec::with_capacity(admitted.len());
    for specification in admitted {
        let mut rows = Vec::new();
        for ((_, _, key), body) in state
            .rows
            .iter()
            .filter(|((owner, name, _), _)| owner == tenant.as_str() && name == specification.name)
        {
            budget.admit_projection_row(body)?;
            rows.push((key.clone(), body.clone()));
        }
        order_rows(&mut rows)?;
        captured.push(CapturedProjection {
            specification,
            rows,
        });
    }
    Ok(captured)
}

/// One tenant's bindings, by digest, as the committed history records them.
fn bound<'a>(
    state: &'a State,
    tenant: &'a TenantId,
) -> impl Iterator<Item = (&'a String, &'a Blob)> {
    state
        .blobs
        .iter()
        .filter(|((owner, _), _)| owner == tenant.as_str())
        .map(|((_, digest), blob)| (digest, blob))
}

fn observe(
    root: &Path,
    state: &State,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    limits: CaptureLimits,
) -> Result<TenantCapture, CaptureError> {
    let mut budget = CaptureBudget::new(limits);
    let (identity, events, admitted) = observed_prefix(state, tenant, projections, &mut budget)?;

    let mut blobs = Vec::new();
    for (digest, blob) in bound(state, tenant) {
        validate_captured_digest(digest)?;
        let bytes = read_blob(root, blob)?;
        budget.admit_blob(bytes.len() as u64)?;
        blobs.push(CapturedBlob {
            digest: digest.clone(),
            bytes,
        });
    }
    order_blobs(&mut blobs)?;

    let captured = observed_projections(state, tenant, admitted, &mut budget)?;
    Ok(TenantCapture {
        tenant: tenant.clone(),
        stream_identity: identity,
        events,
        blobs,
        projections: captured,
    })
}

/// The same observation, deciding every binding from the committed history and reading none.
///
/// This is where the authority's content stops being read on every capture. The committed record
/// already names the object behind each digest and the hash that object's content must have —
/// `put_blob` and `bind_blobs` wrote both — so *which* digests this tenant binds, and in what
/// order, and whether they cross the caller's caps, are all answerable without opening one of
/// them. What is not answerable without opening one is whether its content is still that content,
/// and that question is asked where it can be answered: in [`BoundObjects::read`], on the read
/// that hands the content out.
fn observe_deferred(
    root: &Path,
    state: &State,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    limits: CaptureLimits,
) -> Result<DeferredTenantCapture, CaptureError> {
    let mut budget = CaptureBudget::new(limits);
    let (identity, events, admitted) = observed_prefix(state, tenant, projections, &mut budget)?;

    let mut digests = Vec::new();
    let mut objects = BTreeMap::new();
    for (digest, blob) in bound(state, tenant) {
        validate_captured_digest(digest)?;
        budget.admit_blob(bound_length(root, blob)?)?;
        digests.push(digest.clone());
        objects.insert(digest.clone(), blob.clone());
    }
    let source: Arc<dyn BoundBlobs> = Arc::new(BoundObjects {
        root: root.to_owned(),
        objects,
    });
    let mut blobs: Vec<DeferredBlob> = digests
        .into_iter()
        .map(|digest| DeferredBlob::deferred(digest, Arc::clone(&source)))
        .collect();
    order_deferred_blobs(&mut blobs)?;

    let captured = observed_projections(state, tenant, admitted, &mut budget)?;
    Ok(DeferredTenantCapture {
        tenant: tenant.clone(),
        stream_identity: identity,
        events,
        blobs,
        projections: captured,
    })
}

/// The bindings one deferred observation made, and where their content lives.
///
/// What this holds is the size of the observation's *digests*, not of its content: one object id
/// and one recorded hash per binding. That is the whole reason the reads can be moved — a handle
/// that instead held the bytes to avoid re-reading them would hold the store.
///
/// It is read without the writers' lock, because the observation that made it has long since let
/// go. That is a real difference from the eager capture and it is bounded in one direction only:
/// the recorded hash decides every read, so a binding whose object a later writer replaced or
/// removed is refused. The content this answers with is the content the observation bound, or
/// there is no answer.
#[derive(Debug)]
struct BoundObjects {
    root: PathBuf,
    objects: BTreeMap<String, Blob>,
}

impl BoundBlobs for BoundObjects {
    fn read(&self, digest: &str) -> Result<Vec<u8>, CaptureError> {
        let Some(blob) = self.objects.get(digest) else {
            return Err(corrupt());
        };
        read_blob(&self.root, blob)
    }
}

/// How many bytes one binding's object holds, without reading or hashing any of them.
///
/// A deferred capture answers the caller's caps exactly as an eager one does, and the payload cap
/// is charged in bytes, so the bytes have to be counted. `stat` counts them — and it is also
/// where the half of the complete-content check that does not need the content already lives: an
/// admitted object name and a regular file. The other half is the hash, and it is charged where
/// the content is read.
///
/// The same split [`read_blob`] makes, for the same reason and through the same [`material`]: a
/// `stat` that fails because the process may not traverse `blobs/` is not the store telling
/// anybody its material is damaged. This one runs inside the strict read, where such a failure is
/// far less likely — but "less likely" is not a taxonomy, and two sites answering one question
/// two ways is how the answer drifts.
fn bound_length(root: &Path, blob: &Blob) -> Result<u64, CaptureError> {
    crate::validate_object(&blob.id).map_err(|_| corrupt())?;
    let metadata = std::fs::symlink_metadata(root.join("blobs").join(&blob.id))
        .map_err(|error| material(&error))?;
    if !metadata.is_file() {
        return Err(corrupt());
    }
    Ok(metadata.len())
}

fn admit_projection(
    state: &State,
    tenant: &TenantId,
    specification: &ProjectionSpec,
) -> Result<(), CaptureError> {
    let refuse = |reason| CaptureError::ProjectionUnavailable {
        projection: specification.name.to_owned(),
        reason,
    };
    let Some(declared) = state.projections.get(specification.name) else {
        return Err(refuse(ProjectionCaptureRefusal::Undeclared));
    };
    let indexed: Vec<String> = specification
        .indexed
        .iter()
        .map(|field| (*field).to_owned())
        .collect();
    if declared != &indexed {
        return Err(refuse(ProjectionCaptureRefusal::DeclarationMismatch));
    }
    // A redaction fences this view until a complete rebuild. Its rows may still hold the old body.
    if state
        .dirty_views
        .contains(&(tenant.as_str().to_owned(), specification.name.to_owned()))
    {
        return Err(refuse(ProjectionCaptureRefusal::Dirty));
    }
    Ok(())
}

/// The refusal one bound object is, and the only refusal any of them is.
fn corrupt() -> CaptureError {
    CaptureError::Corrupt {
        material: CaptureMaterial::Blob,
    }
}

/// Which refusal one filesystem error about a bound object is.
///
/// **`NotFound` is the only kind that says anything about the stored material**: the object the
/// committed record names is not there, which is the "the stored object is gone" half of
/// [`BoundBlobs::read`]'s contract. Everything else the operating system reports — a permission
/// change, an exhausted descriptor table, an `EIO` from the device — is a fact about this attempt
/// and not about the store. Answering those with [`CaptureError::Corrupt`] tells a consumer that
/// stored material failed its integrity check, and `CaptureMaterial`'s own documentation says the
/// variant is the whole diagnostic, so there is nothing else for that consumer to read: it cannot
/// tell a store it must stop trusting from one it should simply retry.
///
/// **This only became reachable when the read moved.** The eager capture read every object inside
/// the strict read, under the writers' lock, microseconds after the `stat` that admitted it — a
/// window in which essentially nothing but real damage happens. A [`DeferredBlob`] is read
/// whenever its holder gets round to it, outside every boundary this provider owns, which is
/// exactly where `EACCES`, `EMFILE` and `EIO` land. The read was moved and the taxonomy had to
/// follow it.
///
/// One function, because it is one rule: every filesystem error on the capture path comes through
/// here, so there is no second site at which to apply it differently.
fn material(error: &std::io::Error) -> CaptureError {
    if error.kind() == std::io::ErrorKind::NotFound {
        return corrupt();
    }
    CaptureError::Store(EventLogError::Backend(format!(
        "bound object could not be read: {:?}",
        error.kind()
    )))
}

/// The existing complete-content check: admitted object name, regular file, exact stored hash.
///
/// `clean_blobs` and `clear_snapshots` are not called. Unbound objects and stale caches belong to
/// whoever owns the write path; a reader that tidies them is a writer with better manners.
///
/// The three refusals are three different facts and [`material`] keeps them apart: a name no
/// writer here would have produced and a kind this provider does not write are the store's, a
/// content hash that disagrees with the record is the store's, and anything else the filesystem
/// says about the attempt is the attempt's.
fn read_blob(root: &Path, blob: &Blob) -> Result<Vec<u8>, CaptureError> {
    crate::validate_object(&blob.id).map_err(|_| corrupt())?;
    let path = root.join("blobs").join(&blob.id);
    if !std::fs::symlink_metadata(&path)
        .map_err(|error| material(&error))?
        .is_file()
    {
        return Err(corrupt());
    }
    let bytes = std::fs::read(&path).map_err(|error| material(&error))?;
    crate::verified(root, &bytes, &blob.hash).ok_or_else(corrupt)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Op;
    use eventlog_core::{EventStore, Expected, StreamId};
    use serde_json::json;

    fn limits() -> CaptureLimits {
        CaptureLimits {
            max_events: 64,
            max_blobs: 64,
            max_projection_rows: 64,
            max_payload_bytes: 65_536,
        }
    }

    /// Write one operation into committed history through the real framing.
    ///
    /// An integration test cannot reach this: the frames are hash-chained, so a stored identity
    /// nobody could mint through the public API is only constructible from inside the crate.
    fn commit(root: &Path, ops: &[Op]) {
        let mut journal = journal::Journal::open(root).expect("writable store");
        journal
            .append(serde_json::to_value(ops).expect("operations serialize"))
            .expect("committed frame");
    }

    /// The strict reader holds the writers' own lock for as long as its value lives.
    ///
    /// Acquiring the lock and holding it are two different facts, and the interprocess case can
    /// only see the first: it measures how long `open` waits, which is decided before the
    /// observation begins. A reader that released the lock the moment it had the manifest would
    /// still pass that case, and would still read blob objects with no lock at all. `flock`
    /// conflicts between open file descriptions, so a second handle in this same process settles
    /// the question with no timing in it.
    #[test]
    fn the_strict_reader_holds_the_writer_lock_for_its_whole_life() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        drop(journal::Journal::open(root).expect("initialized store"));
        let probe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join("writer.lock"))
            .expect("existing writer lock");
        probe
            .try_lock()
            .expect("the control: nothing holds the lock yet");
        probe.unlock().expect("released the control");
        let strict = journal::open_strict(root).expect("strict read");
        assert!(
            probe.try_lock().is_err(),
            "the strict reader handed back a value that is not holding the writers' lock"
        );
        drop(strict);
        probe
            .try_lock()
            .expect("the lock is free once the reader is finished with it");
    }

    /// Both readers that hand content back read it *inside* the lock, not beside it.
    ///
    /// `the_strict_reader_holds_the_writer_lock_for_its_whole_life` settles that the value holds
    /// the lock; it cannot see when its caller lets go. Releasing the lock one statement before
    /// `observe()` reads the blob objects it hands out left every case in this crate green
    /// (review 2, finding A3): the rule this module states was carried by statement order and by
    /// nothing else. `read_under_lock` takes the read as an argument of the value that owns the
    /// lock, so there is no order left to get wrong, and this case measures the lock from inside
    /// that read. `flock` conflicts between open file descriptions, so a second handle in this
    /// same process settles it with no timing between threads in it.
    ///
    /// The question is asked over a window rather than at an instant, and that is not a
    /// concession to timing. `Command::spawn` anywhere in this binary forks a child that carries
    /// every open descriptor until it execs, and an inherited descriptor keeps a released
    /// `flock` alive — measured here at around 700 spun `try_lock` calls, and the cause of the
    /// pre-existing flake in the sibling case above. A single probe therefore answers about
    /// somebody else's fork as readily as about this reader. Over a window it cannot: a lock the
    /// reader holds is held for the whole of it, and a transient that outlasts the whole of it is
    /// not something a fork can produce. The error is one-sided — a lost race reads as held,
    /// never as released — so this case cannot go red on a store that is correct.
    #[test]
    fn both_readers_read_the_content_they_hand_out_under_the_writers_lock() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        {
            let mut journal = journal::Journal::open(root).expect("initialized store");
            journal
                .append(json!({"value": "committed"}))
                .expect("committed frame");
        }
        let probe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join("writer.lock"))
            .expect("existing writer lock");
        // Two orders of magnitude over the longest inherited-descriptor window measured.
        let window = std::time::Duration::from_millis(100);
        let free_within = |window: std::time::Duration| {
            let until = std::time::Instant::now() + window;
            while std::time::Instant::now() < until {
                if probe.try_lock().is_ok() {
                    probe.unlock().expect("released the probe");
                    return true;
                }
            }
            false
        };

        assert!(
            free_within(window),
            "the control: nothing holds the writers' lock before a reader exists"
        );

        let strict = journal::open_strict(root).expect("strict read");
        let (free_during_read, manifest, _, content) =
            strict.read_under_lock(|| free_within(window));
        assert!(
            !free_during_read,
            "the strict reader read the content it hands out with the writers' lock released"
        );
        assert!(
            free_within(window),
            "the strict reader kept the writers' lock after it was finished with it"
        );

        let resumed =
            journal::resume_strict(root, &manifest, &content).expect("resumed onto the same head");
        let (free_during_read, _, _, _) = resumed.read_under_lock(|| free_within(window));
        assert!(
            !free_during_read,
            "the resumed reader read the content it hands out with the writers' lock released"
        );
        assert!(
            free_within(window),
            "the resumed reader kept the writers' lock after it was finished with it"
        );
    }

    /// Repeated captures on one handle verify the committed history once, not once per capture.
    ///
    /// No damage a case can do to the store tells a resumed reader from a strict one: both read
    /// the same bytes of `events.jsonl`. What separates them is work, so work is what this
    /// asserts — the frames decoded and chained, the frames re-encoded to answer the divergence
    /// guard, and the transactions folded into state. With no write between the captures, the
    /// first pays all of it and the nine after it pay none.
    ///
    /// The blob line is the boundary, and it is asserted rather than left implied: a capture hands
    /// the bytes to its caller, and bytes handed to a caller are hashed when they are read — the
    /// rule `Transaction::blob` follows for every read on the write path. Serving them from a
    /// cache instead would be a design change, not a refactor, and it would be unbounded in the
    /// size of the store's content.
    #[tokio::test(flavor = "multi_thread")]
    async fn repeated_captures_on_one_handle_verify_the_committed_history_once() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let tenant = TenantId::new("capture-cost-owner").unwrap();
        let store = FileEventStore::open(root).await.unwrap();
        store.stream_identity(&tenant).await.unwrap();
        for index in 0..8_i64 {
            store
                .append(
                    &StreamId::new(tenant.clone(), "item", format!("s{index}")).unwrap(),
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", index)],
                    &eventlog_conformance::meta(&format!("m{index}"), &json!({})),
                )
                .await
                .unwrap();
            store
                .put_blob(
                    &tenant,
                    &format!("d{index}"),
                    format!("bytes-{index}").as_bytes(),
                )
                .await
                .unwrap();
        }

        let before = crate::cost::of(root);
        let first = store.capture_tenant(&tenant, &[], limits()).await.unwrap();
        let one = crate::cost::of(root) - before;
        for round in 0..9 {
            assert_eq!(
                store.capture_tenant(&tenant, &[], limits()).await.unwrap(),
                first,
                "round {round}: a reused view serves the observation the strict read built"
            );
        }
        let ten = crate::cost::of(root) - before;
        println!("COST one={one:?} ten={ten:?}");

        assert!(
            one.frames_chained >= 17 && one.frames_folded >= 17 && one.blobs_hashed >= 8,
            "the fixture only means something with a history worth not reverifying: {one:?}"
        );
        assert_eq!(
            ten.frames_chained, one.frames_chained,
            "nine further captures decoded and chained committed frames again: {ten:?} against {one:?}"
        );
        assert_eq!(
            ten.frames_reencoded, one.frames_reencoded,
            "nine further captures re-encoded the observed prefix again: {ten:?} against {one:?}"
        );
        assert_eq!(
            ten.frames_folded, one.frames_folded,
            "nine further captures folded committed history again: {ten:?} against {one:?}"
        );
        assert_eq!(
            ten.blobs_hashed,
            10 * one.blobs_hashed,
            "the bytes a capture hands out are hashed on every capture: {ten:?} against {one:?}"
        );
    }

    /// A deferred capture says which digests are bound without opening one object to find out.
    ///
    /// The acceptance statement of this unit, and it is asserted as *work* rather than as wall
    /// clock, because wall clock cannot tell a fast read from no read: a machine with the whole
    /// store in page cache reads 7,815 objects in well under the budget a timing assertion would
    /// have to allow, and would go on passing after somebody put the reads back. `blobs_hashed`
    /// is the value the hash comparison produced, so it moves if and only if an object was opened
    /// and digested and found to be what the committed record says — which is what
    /// `a_binding_whose_content_is_not_the_record_charges_no_hashing` holds it to, because this
    /// case alone cannot tell "digested" from "read".
    ///
    /// The other half is the half that matters: the reads are *moved*, not removed. Asking one
    /// binding for its content costs exactly one, asking for all of them costs exactly what the
    /// capture used to cost, and what comes back is what the eager capture would have handed over
    /// — same digests, same bytes, same everything else. A capture that answered cheaply by
    /// answering with less would pass the first assertion and fail these.
    /// A read that found the wrong content charged no hashing, because the count *is* the check.
    ///
    /// What the sibling case above cannot see. Its three `blobs_hashed` assertions all read
    /// undamaged objects, so they cannot tell a charge produced by the hash comparison from one
    /// written beside it between the `read` and the comparison — and that is not a hypothetical:
    /// the charge was written beside it, and deleting the comparison left all three green while
    /// nothing was verified at all. `crate::synchronize` states the rule this crate already
    /// learned once, for `object_syncs`: *the count is produced by the synchronization, not
    /// written beside it.*
    ///
    /// A damaged object is the one input that separates the two. It is read — the object is a
    /// regular file of the admitted name and exactly the length the observation charged, so
    /// everything before the comparison succeeds — and it fails the comparison. A counter that
    /// measures reading moves; a counter that measures verifying does not. The undamaged control
    /// in the same case is what stops a permanently stuck counter from passing the first half.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_binding_whose_content_is_not_the_record_charges_no_hashing() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let tenant = TenantId::new("charge-on-verify-owner").unwrap();
        let store = FileEventStore::open(root).await.unwrap();
        store.stream_identity(&tenant).await.unwrap();
        for (digest, bytes) in [
            ("intact", b"intact-content"),
            ("damaged", b"damage-content"),
        ] {
            store.put_blob(&tenant, digest, bytes).await.unwrap();
        }
        let observed = store
            .capture_tenant_deferred(&tenant, &[], limits())
            .await
            .unwrap();

        // Same length, different bytes, written under the object's own name: the name is still
        // admitted, the entry is still a regular file and `bound_length` still agrees, so the
        // read reaches the comparison and nothing before it can refuse.
        let mut replaced = false;
        for entry in std::fs::read_dir(root.join("blobs")).unwrap() {
            let path = entry.unwrap().path();
            let mut bytes = std::fs::read(&path).unwrap();
            if bytes == b"damage-content" {
                bytes[0] ^= 0xff;
                std::fs::write(&path, &bytes).unwrap();
                replaced = true;
            }
        }
        assert!(replaced, "the fixture's content is stored under blobs/");

        let binding = |digest: &str| {
            observed
                .blobs
                .iter()
                .find(|blob| blob.digest == digest)
                .expect("the binding is in the observation")
        };

        let before = crate::cost::of(root);
        assert_eq!(
            binding("damaged").bytes(),
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Blob
            }),
            "the damaged object is refused, which is the precondition for what follows"
        );
        let damaged = crate::cost::of(root) - before;
        assert_eq!(
            damaged.blobs_hashed, 0,
            "an object read and found not to be the content the record names was charged as \
             hashing: the count is measuring the read rather than the comparison: {damaged:?}"
        );

        let control = crate::cost::of(root);
        assert_eq!(
            binding("intact").bytes().unwrap(),
            b"intact-content".to_vec(),
            "the untouched binding still hands out its content"
        );
        let verified = crate::cost::of(root) - control;
        assert_eq!(
            verified.blobs_hashed, 1,
            "a counter that never moves would pass the assertion above for the wrong reason: \
             {verified:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_deferred_capture_reads_no_blob_object_until_its_bytes_are_asked_for() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let tenant = TenantId::new("deferred-capture-owner").unwrap();
        let store = FileEventStore::open(root).await.unwrap();
        store.stream_identity(&tenant).await.unwrap();
        for index in 0..8_i64 {
            store
                .append(
                    &StreamId::new(tenant.clone(), "item", format!("s{index}")).unwrap(),
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", index)],
                    &eventlog_conformance::meta(&format!("m{index}"), &json!({})),
                )
                .await
                .unwrap();
            store
                .put_blob(
                    &tenant,
                    &format!("d{index}"),
                    format!("bytes-{index}").as_bytes(),
                )
                .await
                .unwrap();
        }

        let before = crate::cost::of(root);
        let deferred = store
            .capture_tenant_deferred(&tenant, &[], limits())
            .await
            .unwrap();
        let taking = crate::cost::of(root) - before;
        assert_eq!(
            deferred.blobs.len(),
            8,
            "a deferred capture names every bound blob: {:?}",
            deferred.blobs.len()
        );
        assert_eq!(
            deferred
                .blobs
                .iter()
                .map(|blob| blob.digest.clone())
                .collect::<Vec<_>>(),
            (0..8).map(|index| format!("d{index}")).collect::<Vec<_>>(),
            "the digests are the committed records', in the order every capture returns"
        );
        assert_eq!(
            taking.blobs_hashed, 0,
            "a deferred capture opened and digested blob objects to say which digests are bound: {taking:?}"
        );

        let asking = crate::cost::of(root);
        assert_eq!(
            deferred.blobs[3].bytes().unwrap(),
            b"bytes-3".to_vec(),
            "the binding hands out the content it names"
        );
        let asked = crate::cost::of(root) - asking;
        assert_eq!(
            asked.blobs_hashed, 1,
            "one binding asked for its content cost one object read, hashed on the way out: {asked:?}"
        );

        let loading = crate::cost::of(root);
        let loaded = deferred.load().unwrap();
        let whole = crate::cost::of(root) - loading;
        assert_eq!(
            whole.blobs_hashed, 8,
            "asking for every binding's content costs exactly what the capture used to cost: {whole:?}"
        );
        assert_eq!(
            loaded,
            store.capture_tenant(&tenant, &[], limits()).await.unwrap(),
            "a deferred capture read in full is the capture the caller would have been handed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stored_identity_bytes_survive_exactly_and_an_empty_one_is_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_owned();
        let legacy = TenantId::new("legacy-owner").unwrap();
        let empty = TenantId::new("empty-owner").unwrap();
        {
            let store = FileEventStore::open(&root).await.unwrap();
            store
                .append(
                    &StreamId::new(legacy.clone(), "item", "one").unwrap(),
                    Expected::NoStream,
                    &[eventlog_conformance::event("item.received", 1)],
                    &eventlog_conformance::meta("legacy", &json!({})),
                )
                .await
                .unwrap();
        }
        // Neither value has UUID syntax; one of them is not a value at all.
        let preserved = "  Legacy Identity/v0  ";
        {
            let path = root.clone();
            tokio::task::spawn_blocking(move || {
                commit(
                    &path,
                    &[
                        Op::Identity {
                            tenant: TenantId::new("legacy-owner").unwrap(),
                            id: preserved.to_owned(),
                        },
                        Op::Identity {
                            tenant: TenantId::new("empty-owner").unwrap(),
                            id: String::new(),
                        },
                    ],
                );
            })
            .await
            .unwrap();
        }

        let handle = FileTenantCapture::open(&root).await.unwrap();
        let captured = handle.capture_tenant(&legacy, &[], limits()).await.unwrap();
        assert_eq!(
            captured.stream_identity, preserved,
            "a stored identity is returned byte for byte: no trim, no normalization, no UUID rule"
        );
        assert_eq!(captured.events.len(), 1);
        assert_eq!(
            handle.capture_tenant(&empty, &[], limits()).await,
            Err(CaptureError::Corrupt {
                material: CaptureMaterial::Identity
            }),
            "an empty stored identity is corruption, not permission to mint a replacement"
        );
    }
}
