//! Native inline administration inside one owned blocking worker.

use std::{
    collections::BTreeSet,
    sync::{Arc, atomic::AtomicBool},
};

use eventlog_core::{
    CaptureError, CapturedOrder, EventLogError, InlineProjectionAdmin, InlineRebuildResult,
    MAX_READ_LIMIT, Projector, RecordedEvent, TenantId, validate_identifier,
};
use rusqlite::{Connection, params};
use time::OffsetDateTime;

use crate::{
    COLUMNS, Inner, REBUILD_STAGING, SqliteEventStore, SqliteProjections, backend, begin_immediate,
    capture, drive, durable_projection_table, ensure_callback_integrity, finish_transaction,
    format_time, joined, poisoned, projection_table, read_event, run_blocking, to_i64,
};

/// Events one rebuild page reads and retains at a time: the store's bounded batch, and two in unit
/// tests so a short history spans several pages.
const REBUILD_PAGE: usize = if cfg!(test) { 2 } else { MAX_READ_LIMIT };

/// One page of the tenant's history after `after`, in position order.
fn read_page(
    connection: &Connection,
    prefix: &str,
    tenant: &TenantId,
    after: Option<i64>,
) -> Result<Vec<RecordedEvent>, EventLogError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT {COLUMNS} FROM {prefix}_events WHERE tenant_id=?1 AND (?2 IS NULL OR global_seq>?2) \
             ORDER BY global_seq LIMIT ?3"
        ))
        .map_err(backend)?;
    let rows = statement
        .query_map(
            params![tenant.as_str(), after, to_i64(REBUILD_PAGE as u64)?],
            read_event,
        )
        .map_err(backend)?;
    let mut page = Vec::new();
    for row in rows {
        page.push(row.map_err(backend)??);
    }
    Ok(page)
}

fn capture_error(error: CaptureError) -> EventLogError {
    match error {
        CaptureError::Store(error) => error,
        CaptureError::ProjectionUnavailable { .. } | CaptureError::InvalidRequest { .. } => {
            EventLogError::Invalid("projection admission does not match durable structure".into())
        }
        _ => EventLogError::Backend("projection admission encountered corrupt storage".into()),
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

impl InlineProjectionAdmin for SqliteEventStore {
    fn attach_inline_existing(
        &self,
        projector: Arc<dyn Projector>,
    ) -> eventlog_core::BoxFuture<'_, Result<(), EventLogError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(run_blocking(move || {
            inner.attach_inline_existing(projector)
        }))
    }

    fn rebuild_inline_projection<'a>(
        &'a self,
        projector_name: &'a str,
        tenant: &'a TenantId,
    ) -> eventlog_core::BoxFuture<'a, Result<InlineRebuildResult, EventLogError>> {
        let inner = Arc::clone(&self.inner);
        let name = projector_name.to_owned();
        let tenant = tenant.clone();
        Box::pin(run_blocking(move || {
            inner.rebuild_inline_projection(&name, &tenant)
        }))
    }
}

impl Inner {
    fn attach_inline_existing(&self, projector: Arc<dyn Projector>) -> Result<(), EventLogError> {
        validate_projector(projector.as_ref())?;
        let registration = self.registration.lock().map_err(poisoned)?;
        if *registration {
            return Err(EventLogError::Invalid(
                "inline registration is frozen after serving begins".into(),
            ));
        }
        let mut projectors = self.inline.lock().map_err(poisoned)?;
        let mut names = self.inline_names.lock().map_err(poisoned)?;
        if projectors
            .iter()
            .any(|existing| existing.name() == projector.name())
            || projector
                .projections()
                .iter()
                .any(|specification| names.contains(specification.name))
        {
            return Err(EventLogError::Invalid("duplicate inline projection".into()));
        }
        self.refuse_catch_up_driven(projector.name())?;

        let connection = self.connection.lock().map_err(poisoned)?;
        begin_immediate(&connection)?;
        let validation = (|| {
            for specification in projector.projections() {
                capture::admit_projection(&connection, &self.prefix, specification)
                    .map_err(capture_error)?;
            }
            Ok(())
        })();
        finish_transaction(&connection, validation)?;

        // No fallible work remains between the two local updates.
        let mut installed = names.clone();
        installed.extend(
            projector
                .projections()
                .iter()
                .map(|specification| specification.name.to_owned()),
        );
        let mut next = projectors.clone();
        next.push(projector);
        *names = installed;
        *projectors = next;
        Ok(())
    }

    fn rebuild_inline_projection(
        &self,
        projector_name: &str,
        tenant: &TenantId,
    ) -> Result<InlineRebuildResult, EventLogError> {
        validate_identifier("projector name", projector_name)?;
        let _registration = self.registration.lock().map_err(poisoned)?;
        let projector = self
            .inline
            .lock()
            .map_err(poisoned)?
            .iter()
            .find(|projector| projector.name() == projector_name)
            .cloned()
            .ok_or_else(|| {
                EventLogError::Invalid("inline projector is not attached to this handle".into())
            })?;
        validate_projector(projector.as_ref())?;

        let mut connection = self.connection.lock().map_err(poisoned)?;
        begin_immediate(&connection)?;
        let callback_failed = Arc::new(AtomicBool::new(false));
        let result = (|| {
            for specification in projector.projections() {
                capture::admit_projection(&connection, &self.prefix, specification)
                    .map_err(capture_error)?;
                let shadow = projection_table(REBUILD_STAGING, specification.name);
                let columns = joined(specification.indexed.len(), |position| {
                    format!(",idx_{position} TEXT")
                });
                connection
                    .execute_batch(&format!(
                        "CREATE TEMP TABLE {shadow}(tenant_id TEXT NOT NULL,row_key TEXT NOT NULL,\
                         body TEXT NOT NULL{columns},PRIMARY KEY(tenant_id,row_key))"
                    ))
                    .map_err(backend)?;
            }

            // Two bounded passes inside this one immediate transaction, one page retained at a
            // time: the whole history is validated before the first callback, as when it was
            // held in one vector, and then folded page by page. The writer lock keeps both
            // passes on the same rows.
            let mut order = CapturedOrder::default();
            let mut validated = 0_u64;
            let mut after: Option<i64> = None;
            loop {
                let page = read_page(&connection, &self.prefix, tenant, after)?;
                if page.is_empty() {
                    break;
                }
                #[cfg(feature = "kpi-counters")]
                crate::kpi::rebuild_retained(page.len());
                for event in &page {
                    order.observe(tenant, event).map_err(capture_error)?;
                    after = Some(to_i64(event.global_seq)?);
                    validated += 1;
                }
            }
            let validated_through = after;
            let mut projections = SqliteProjections {
                connection: &mut connection,
                blob_prefix: &self.prefix,
                verified: &self.verified,
                tracked: &self.tracked,
                projection_prefix: REBUILD_STAGING,
                inline: &self.inline_names,
                tenant,
                admission: None,
                callback_failed: Arc::clone(&callback_failed),
                selected: Some(projector.projections()),
            };
            let mut position = 0_i64;
            let mut applied = 0_u64;
            let mut after: Option<i64> = None;
            loop {
                let page = read_page(projections.connection, &self.prefix, tenant, after)?;
                if page.is_empty() {
                    break;
                }
                #[cfg(test)]
                tests::note_retained(&self.prefix, page.len());
                #[cfg(feature = "kpi-counters")]
                crate::kpi::rebuild_retained(page.len());
                for event in &page {
                    let outcome = drive(projector.apply(event, &mut projections));
                    ensure_callback_integrity(&callback_failed)?;
                    outcome?;
                    position = to_i64(event.global_seq)?;
                    after = Some(position);
                    applied = applied.checked_add(1).ok_or_else(|| {
                        EventLogError::Backend("inline rebuild event count overflow".into())
                    })?;
                }
            }
            if applied != validated || after != validated_through {
                return Err(EventLogError::Backend(
                    "inline rebuild history changed between its passes".into(),
                ));
            }
            for specification in projector.projections() {
                let active = durable_projection_table(&self.prefix, specification.name);
                let shadow = projection_table(REBUILD_STAGING, specification.name);
                connection
                    .execute(
                        &format!("DELETE FROM {active} WHERE tenant_id=?1"),
                        params![tenant.as_str()],
                    )
                    .map_err(backend)?;
                connection
                    .execute(
                        &format!("INSERT INTO {active} SELECT * FROM {shadow} WHERE tenant_id=?1"),
                        params![tenant.as_str()],
                    )
                    .map_err(backend)?;
                connection
                    .execute_batch(&format!("DROP TABLE {shadow}"))
                    .map_err(backend)?;
            }
            ensure_callback_integrity(&callback_failed)?;
            connection.execute(&format!("INSERT INTO {}_projection_cursors(projection,tenant_id,global_seq,updated_at) VALUES(?1,?2,?3,?4) ON CONFLICT(projection,tenant_id) DO UPDATE SET global_seq=excluded.global_seq,updated_at=excluded.updated_at",self.prefix),params![projector.name(),tenant.as_str(),position,format_time(OffsetDateTime::now_utc())?]).map_err(backend)?;
            Ok(InlineRebuildResult {
                applied,
                position: u64::try_from(position)
                    .map_err(|_| EventLogError::Backend("stored value is negative".into()))?,
            })
        })();
        finish_transaction(&connection, result)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    };

    use eventlog_core::{
        BoxFuture, EventLogError, EventStore as _, Expected, InlineProjectionAdmin as _,
        ProjectionSpec, ProjectionStore, Projector, RecordedEvent, StreamId, TenantId,
    };

    use super::REBUILD_PAGE;
    use crate::SqliteEventStore;

    /// The most events one rebuild of a prefix held decoded at once, by prefix.
    static RETAINED: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

    /// Called by the rebuild with the events it holds while it folds them.
    pub(crate) fn note_retained(prefix: &str, events: usize) {
        let mut retained = RETAINED.lock().unwrap();
        match retained.iter_mut().find(|(seen, _)| seen == prefix) {
            Some((_, most)) => *most = (*most).max(events),
            None => retained.push((prefix.to_owned(), events)),
        }
    }

    fn most_retained(prefix: &str) -> usize {
        RETAINED
            .lock()
            .unwrap()
            .iter()
            .find(|(seen, _)| seen == prefix)
            .map_or(0, |(_, most)| *most)
    }

    const VERSIONS: ProjectionSpec = ProjectionSpec {
        name: "paged_versions",
        indexed: &[],
    };

    /// Records each stream's latest version and counts its calls.
    #[derive(Default)]
    struct Versions {
        calls: AtomicU64,
    }

    impl Projector for Versions {
        fn name(&self) -> &'static str {
            "paged_versions"
        }

        fn projections(&self) -> &'static [ProjectionSpec] {
            std::slice::from_ref(&VERSIONS)
        }

        fn apply<'a>(
            &'a self,
            event: &'a RecordedEvent,
            store: &'a mut dyn ProjectionStore,
        ) -> BoxFuture<'a, Result<(), EventLogError>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::AcqRel);
                store
                    .upsert(
                        &VERSIONS,
                        &event.tenant,
                        &event.stream_id,
                        &serde_json::json!({ "version": event.version }),
                    )
                    .await
            })
        }
    }

    /// A rebuild of a history longer than one page holds one page at a time, folds every event,
    /// and still validates the whole history before its first callback: a corrupt event on the
    /// last page refuses the rebuild with no callback made.
    #[tokio::test]
    async fn inline_rebuild_folds_bounded_pages_after_validating_every_page() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("paged.db");
        let path = path.to_str().unwrap();
        let prefix = "paged_rebuild";
        let tenant = TenantId::new("paged").unwrap();
        let store = SqliteEventStore::open(path, prefix).await.unwrap();
        let projector = Arc::new(Versions::default());
        store.create_projections(projector.clone()).await.unwrap();
        store
            .attach_inline_existing(projector.clone())
            .await
            .unwrap();
        for (stream, events) in [("one", 3_i64), ("two", 4)] {
            store
                .append(
                    &StreamId::new(tenant.clone(), "item", stream).unwrap(),
                    Expected::NoStream,
                    &(1..=events)
                        .map(|value| eventlog_conformance::event("item.recorded", value))
                        .collect::<Vec<_>>(),
                    &eventlog_conformance::meta(stream, &serde_json::json!({})),
                )
                .await
                .unwrap();
        }
        projector.calls.store(0, Ordering::Release);
        let rebuilt = store
            .rebuild_inline_projection(projector.name(), &tenant)
            .await
            .unwrap();
        assert_eq!(rebuilt.applied, 7);
        assert_eq!(projector.calls.load(Ordering::Acquire), 7);
        assert!(
            most_retained(prefix) <= REBUILD_PAGE,
            "the rebuild held {} events at once; a page is {REBUILD_PAGE}",
            most_retained(prefix)
        );
        let latest = store
            .projection_get(&VERSIONS, &tenant, "two")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest["version"], 4);

        let tamper = rusqlite::Connection::open(path).unwrap();
        assert_eq!(
            tamper
                .execute(
                    &format!(
                        "UPDATE {prefix}_events SET version=9 WHERE global_seq=\
                         (SELECT MAX(global_seq) FROM {prefix}_events WHERE tenant_id=?1)"
                    ),
                    [tenant.as_str()],
                )
                .unwrap(),
            1
        );
        drop(tamper);
        projector.calls.store(0, Ordering::Release);
        let refused = store
            .rebuild_inline_projection(projector.name(), &tenant)
            .await;
        assert!(
            matches!(&refused, Err(EventLogError::Backend(reason)) if reason.contains("corrupt")),
            "a version gap on the last page refuses as corrupt storage: {refused:?}"
        );
        assert_eq!(
            projector.calls.load(Ordering::Acquire),
            0,
            "no callback runs before the whole history is validated"
        );
    }
}
