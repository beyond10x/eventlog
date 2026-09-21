//! One tenant's complete observation, read from committed files and nothing else.
//!
//! The ordinary store entry point is not usable for this. `FileEventStore::open` and every
//! `transaction` after it initialize a missing store, recover a pending append or privacy intent,
//! rewrite `events.jsonl`, drop stale snapshot caches and delete unbound blob objects — before the
//! read closure runs. An inspector that has to mutate a store to look at it has already changed
//! the thing it came to observe, so capture uses the strict reader instead and holds the same
//! runtime mutex and interprocess `writer.lock` that every writer holds.

use std::path::{Path, PathBuf};

use eventlog_core::{
    BoxFuture, CaptureBudget, CaptureError, CaptureLimits, CaptureMaterial, CapturedBlob,
    CapturedProjection, ConsistentTenantCapture, EventLogError, ProjectionCaptureRefusal,
    ProjectionSpec, TenantCapture, TenantId, order_blobs, order_rows, validate_capture_request,
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

async fn capture(
    root: &Path,
    observed: &mut Option<Manifest>,
    view: &mut Option<Observed>,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    limits: CaptureLimits,
) -> Result<TenantCapture, CaptureError> {
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
fn observed_history(
    root: &Path,
    previous: Option<&Manifest>,
    reusable: Option<Observed>,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    limits: CaptureLimits,
) -> Result<
    (
        Manifest,
        bool,
        Observed,
        Result<TenantCapture, CaptureError>,
    ),
    CaptureError,
> {
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
        // The lock is held for the whole observation, exactly as the strict reader holds it: the
        // blob objects below are read under it, not after it.
        let outcome = observe(root, &state, tenant, projections, limits);
        let (manifest, _, content) = resumed.into_parts();
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
    let outcome = observe(root, &state, tenant, projections, limits);
    let (manifest, _, content) = strict.into_parts();
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

fn observe(
    root: &Path,
    state: &State,
    tenant: &TenantId,
    projections: &[ProjectionSpec],
    limits: CaptureLimits,
) -> Result<TenantCapture, CaptureError> {
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

    let mut budget = CaptureBudget::new(limits);
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

    let mut blobs = Vec::new();
    for ((_, digest), blob) in state
        .blobs
        .iter()
        .filter(|((owner, _), _)| owner == tenant.as_str())
    {
        validate_captured_digest(digest)?;
        let bytes = read_blob(root, blob)?;
        budget.admit_blob(bytes.len() as u64)?;
        blobs.push(CapturedBlob {
            digest: digest.clone(),
            bytes,
        });
    }
    order_blobs(&mut blobs)?;

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

    Ok(TenantCapture {
        tenant: tenant.clone(),
        stream_identity: identity.clone(),
        events,
        blobs,
        projections: captured,
    })
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

/// The existing complete-content check: admitted object name, regular file, exact stored hash.
///
/// `clean_blobs` and `clear_snapshots` are not called. Unbound objects and stale caches belong to
/// whoever owns the write path; a reader that tidies them is a writer with better manners.
fn read_blob(root: &Path, blob: &Blob) -> Result<Vec<u8>, CaptureError> {
    let corrupt = || CaptureError::Corrupt {
        material: CaptureMaterial::Blob,
    };
    crate::validate_object(&blob.id).map_err(|_| corrupt())?;
    let path = root.join("blobs").join(&blob.id);
    if !std::fs::symlink_metadata(&path)
        .map_err(|_| corrupt())?
        .is_file()
    {
        return Err(corrupt());
    }
    let bytes = std::fs::read(&path).map_err(|_| corrupt())?;
    #[cfg(test)]
    crate::cost::charge(root, |cost| {
        cost.blobs_hashed += 1;
    });
    if journal::hash(&bytes) != blob.hash {
        return Err(corrupt());
    }
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
