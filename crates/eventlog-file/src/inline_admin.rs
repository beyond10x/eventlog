//! Native administration over the strict, nonrecovering file entry path.

use std::{collections::BTreeSet, sync::Arc};

use eventlog_core::{
    CaptureError, EventLogError, InlineProjectionAdmin, InlineRebuildResult, Projector, TenantId,
    validate_captured_order, validate_identifier,
};

use crate::{Transaction, blocking, journal, projection, state::Op, state::State};

fn capture_error(error: CaptureError) -> EventLogError {
    match error {
        CaptureError::Store(error) => error,
        CaptureError::RecoveryRequired => {
            EventLogError::Invalid("file store recovery is required before administration".into())
        }
        CaptureError::Corrupt { .. } => EventLogError::Backend(
            "file store committed history is corrupt; no history was repaired".into(),
        ),
        _ => EventLogError::Backend("file store strict administration refused".into()),
    }
}

fn validate_projector(projector: &dyn Projector) -> Result<(), EventLogError> {
    validate_identifier("projector name", projector.name())?;
    let mut names = BTreeSet::new();
    for specification in projector.projections() {
        specification.validate()?;
        if !names.insert(specification.name) {
            return Err(EventLogError::Invalid(
                "duplicate projection in one registration".into(),
            ));
        }
        let mut fields = BTreeSet::new();
        if specification
            .indexed
            .iter()
            .any(|field| !fields.insert(*field))
        {
            return Err(EventLogError::Invalid(
                "duplicate indexed field in one projection".into(),
            ));
        }
    }
    Ok(())
}

fn validate_admission(state: &State, projector: &dyn Projector) -> Result<(), EventLogError> {
    validate_projector(projector)?;
    for specification in projector.projections() {
        let indexed: Vec<String> = specification
            .indexed
            .iter()
            .map(|field| (*field).to_owned())
            .collect();
        if state.projections.get(specification.name) != Some(&indexed) {
            return Err(EventLogError::Invalid(
                "projection was not admitted with this exact shape".into(),
            ));
        }
    }
    Ok(())
}

impl InlineProjectionAdmin for crate::FileEventStore {
    fn attach_inline_existing(
        &self,
        projector: Arc<dyn Projector>,
    ) -> eventlog_core::BoxFuture<'_, Result<(), EventLogError>> {
        Box::pin(async move {
            validate_projector(projector.as_ref())?;
            let mut runtime = self.runtime.lock().await;
            if runtime.frozen {
                return Err(EventLogError::Invalid(
                    "inline registration is frozen after serving begins".into(),
                ));
            }
            if runtime.inline.iter().any(|existing| {
                existing.name() == projector.name()
                    || existing.projections().iter().any(|old| {
                        projector
                            .projections()
                            .iter()
                            .any(|new| new.name == old.name)
                    })
            }) {
                return Err(EventLogError::Invalid("duplicate inline projection".into()));
            }
            let root = self.root.clone();
            let strict =
                blocking(move || journal::open_strict(&root).map_err(capture_error)).await?;
            if let Some(observed) = &runtime.observed
                && !journal::extends_observed(&strict.manifest, &strict.transactions, observed)?
            {
                return Err(EventLogError::Backend(
                    "file history diverged from this handle's observed history".into(),
                ));
            }
            let state = State::replay(&strict.transactions)?;
            validate_admission(&state, projector.as_ref())?;
            runtime.observed = Some(strict.manifest.clone());
            runtime.inline.push(projector);
            Ok(())
        })
    }

    fn rebuild_inline_projection<'a>(
        &'a self,
        projector_name: &'a str,
        tenant: &'a TenantId,
    ) -> eventlog_core::BoxFuture<'a, Result<InlineRebuildResult, EventLogError>> {
        Box::pin(async move {
            validate_identifier("projector name", projector_name)?;
            let mut runtime = self.runtime.lock().await;
            let projector = runtime
                .inline
                .iter()
                .find(|projector| projector.name() == projector_name)
                .cloned()
                .ok_or_else(|| {
                    EventLogError::Invalid("inline projector is not attached to this handle".into())
                })?;
            validate_projector(projector.as_ref())?;

            // Strict acquisition comes before any ordinary opener that could recover or clean.
            let root = self.root.clone();
            let strict =
                blocking(move || journal::open_strict(&root).map_err(capture_error)).await?;
            if let Some(observed) = &runtime.observed
                && !journal::extends_observed(&strict.manifest, &strict.transactions, observed)?
            {
                return Err(EventLogError::Backend(
                    "file history diverged from this handle's observed history".into(),
                ));
            }
            let state = State::replay(&strict.transactions)?;
            validate_admission(&state, projector.as_ref())?;
            let mut transaction = Transaction {
                journal: strict.into_journal(&self.root),
                state,
                pending: Vec::new(),
                inline: runtime.inline.clone(),
                root: self.root.clone(),
                privacy: false,
                permit: self.permit.clone(),
            };
            for specification in projector.projections() {
                transaction.record(Op::ClearRows {
                    tenant: tenant.clone(),
                    name: specification.name.to_owned(),
                })?;
                transaction.record(Op::DirtyView {
                    tenant: tenant.clone(),
                    name: specification.name.to_owned(),
                    dirty: false,
                })?;
            }
            let events: Vec<_> = transaction
                .state
                .events
                .values()
                .filter(|event| &event.tenant == tenant)
                .cloned()
                .collect();
            validate_captured_order(tenant, &events).map_err(capture_error)?;
            let mut applied = 0_u64;
            let mut position = 0_u64;
            for event in &events {
                projector
                    .apply(
                        event,
                        &mut projection::View {
                            tx: &mut transaction,
                            tenant,
                            admission: false,
                            selected: Some(projector.projections()),
                        },
                    )
                    .await?;
                applied = applied.checked_add(1).ok_or_else(|| {
                    EventLogError::Backend("inline rebuild event count overflow".into())
                })?;
                position = event.global_seq;
            }
            transaction.record(Op::Cursor {
                tenant: tenant.clone(),
                name: projector.name().to_owned(),
                position,
            })?;
            transaction.pending.push(Op::Watermark {
                position: transaction.state.next_position,
            });
            let manifest = blocking(move || {
                transaction
                    .journal
                    .append(serde_json::to_value(transaction.pending).map_err(journal::backend)?)?;
                Ok(transaction.journal.manifest.clone())
            })
            .await?;
            runtime.observed = Some(manifest);
            Ok(InlineRebuildResult { applied, position })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, process::Command, sync::atomic::Ordering};

    use eventlog_core::{EventStore, Expected, Projector, StreamId};
    use serde_json::json;

    use super::*;

    const OWNER: &str = "inline-rebuild-crash-owner";

    // Invoked only by the parent test with an isolated directory and a journal failpoint.
    #[test]
    fn rebuild_crash_child() {
        let Ok(root) = std::env::var("EVENTLOG_FILE_INLINE_REBUILD_CRASH_ROOT") else {
            return;
        };
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = crate::FileEventStore::open(root).await.unwrap();
            let projector = Arc::new(eventlog_conformance::AdminProjector::default());
            projector.generation.store(2, Ordering::Release);
            store
                .attach_inline_existing(projector.clone())
                .await
                .unwrap();
            store
                .rebuild_inline_projection(projector.name(), &TenantId::new(OWNER).unwrap())
                .await
                .unwrap();
        });
        panic!("inline rebuild failpoint was not reached");
    }

    fn crash_rebuild(root: &Path, point: &str) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "inline_admin::tests::rebuild_crash_child",
                "--nocapture",
            ])
            .env("EVENTLOG_FILE_INLINE_REBUILD_CRASH_ROOT", root)
            .env("EVENTLOG_FILE_CRASH_AT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_death_at_each_inline_rebuild_publication_boundary() {
        for point in [
            "append-prepared",
            "append-torn",
            "append-synced",
            "append-committed",
        ] {
            let root = tempfile::tempdir().unwrap();
            let tenant = TenantId::new(OWNER).unwrap();
            let stream = StreamId::new(tenant.clone(), "item", "one").unwrap();
            let original_position = {
                let store = crate::FileEventStore::open(root.path()).await.unwrap();
                let projector = Arc::new(eventlog_conformance::AdminProjector::default());
                store.create_projections(projector).await.unwrap();
                let appended = store
                    .append(
                        &stream,
                        Expected::NoStream,
                        &[eventlog_conformance::event("item.recorded", 1)],
                        &eventlog_conformance::meta("crash-rebuild", &json!({})),
                    )
                    .await
                    .unwrap();
                store.redact(&stream, 1, "privacy").await.unwrap();
                appended.events[0].global_seq
            };

            crash_rebuild(root.path(), point);

            let journal = journal::Journal::open(root.path()).unwrap();
            let state = State::replay(&journal.transactions).unwrap();
            let committed = point == "append-committed";
            for name in [
                eventlog_conformance::ADMIN_LEDGER.name,
                eventlog_conformance::ADMIN_SIDECAR.name,
            ] {
                let dirty = state
                    .dirty_views
                    .contains(&(OWNER.to_owned(), name.to_owned()));
                assert_eq!(dirty, !committed, "{point}: dirty marker for {name}");
                let rows = state
                    .rows
                    .iter()
                    .filter(|((owner, table, _), _)| owner == OWNER && table == name)
                    .collect::<Vec<_>>();
                assert_eq!(rows.len(), usize::from(committed), "{point}: {name}");
                if committed {
                    assert_eq!(rows[0].1["generation"], 2);
                }
            }
            assert_eq!(
                state
                    .cursors
                    .get(&(OWNER.to_owned(), "admin_projector".to_owned()))
                    .copied(),
                committed.then_some(original_position),
                "{point}: cursor must publish with rows and dirty cleanup"
            );
            assert!(!root.path().join("append.json").exists(), "{point}");
        }
    }
}
