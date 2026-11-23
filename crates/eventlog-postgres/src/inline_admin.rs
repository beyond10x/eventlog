//! Native administration behind PostgreSQL's publication and registration coordinates.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use eventlog_core::{
    CaptureError, CapturedOrder, EventLogError, InlineProjectionAdmin, InlineRebuildResult,
    MAX_READ_LIMIT, Projector, TenantId, validate_identifier,
};
use time::OffsetDateTime;
use tokio_postgres::IsolationLevel;

use crate::{
    ADVISORY_KEY, COLUMNS, PostgresEventStore, PostgresProjections, backend, capture,
    ensure_callback_integrity, poisoned, projection_table, publication_identity, read_event,
    to_i64,
};

/// Events one rebuild page reads and retains at a time: the store's bounded batch, and two in unit
/// tests so a short history spans several pages.
const REBUILD_PAGE: usize = if cfg!(test) { 2 } else { MAX_READ_LIMIT };

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

impl InlineProjectionAdmin for PostgresEventStore {
    fn attach_inline_existing(
        &self,
        projector: Arc<dyn Projector>,
    ) -> eventlog_core::BoxFuture<'_, Result<(), EventLogError>> {
        self.bounded(async move {
            validate_projector(projector.as_ref())?;
            let _registration = self.registration.lock().await;
            if self.frozen.load(Ordering::Acquire) {
                return Err(EventLogError::Invalid(
                    "inline registration is frozen after serving begins".into(),
                ));
            }
            {
                let projectors = self.inline.lock().map_err(poisoned)?;
                let names = self.inline_names.lock().map_err(poisoned)?;
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
            }

            let mut client = self.pool.acquire().await?;
            client.quarantine();
            let transaction = client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await
                .map_err(backend)?;
            let validation = async {
                for specification in projector.projections() {
                    capture::admit_projection(&transaction, &self.prefix, specification)
                        .await
                        .map_err(capture_error)?;
                }
                Ok::<(), EventLogError>(())
            }
            .await;
            match validation {
                Ok(()) => transaction.commit().await.map_err(backend)?,
                Err(error) => {
                    transaction.rollback().await.map_err(backend)?;
                    return Err(error);
                }
            }
            client.settled();

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
        })
    }

    fn rebuild_inline_projection<'a>(
        &'a self,
        projector_name: &'a str,
        tenant: &'a TenantId,
    ) -> eventlog_core::BoxFuture<'a, Result<InlineRebuildResult, EventLogError>> {
        Box::pin(async move {
            validate_identifier("projector name", projector_name)?;
            let commit_started = AtomicBool::new(false);
            match tokio::time::timeout(
                self.pool.options.transaction_timeout,
                self.rebuild_inline_bounded(projector_name, tenant, &commit_started),
            )
            .await
            {
                Ok(result) => result,
                Err(_) if commit_started.load(Ordering::Acquire) => {
                    Err(EventLogError::UnknownCommit)
                }
                Err(_) => Err(EventLogError::Deadline {
                    operation: "store operation",
                }),
            }
        })
    }
}

impl PostgresEventStore {
    async fn rebuild_inline_bounded(
        &self,
        projector_name: &str,
        tenant: &TenantId,
        commit_started: &AtomicBool,
    ) -> Result<InlineRebuildResult, EventLogError> {
        let _registration = self.registration.lock().await;
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

        let publication = publication_identity(&self.prefix)?;
        let mut client = self.pool.acquire().await?;
        client.quarantine();
        client
            .execute(
                &format!("SELECT pg_advisory_lock({ADVISORY_KEY})"),
                &[&publication],
            )
            .await
            .map_err(backend)?;

        let (outcome, transaction_ended) = match client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .start()
            .await
        {
            Err(error) => (Err(backend(error)), true),
            Ok(transaction) => {
                let folded = async {
                    // Same transaction lock coordinate used by catch-up and ordinary rebuild.
                    let identity = serde_json::to_string(&[
                        self.prefix.as_str(),
                        "projector",
                        projector.name(),
                        tenant.as_str(),
                    ])
                    .map_err(|_| EventLogError::Invalid("invalid lock coordinates".into()))?;
                    transaction.query_one("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || $1,0))", &[&identity]).await.map_err(backend)?;
                    for specification in projector.projections() {
                        capture::admit_projection(&transaction, &self.prefix, specification)
                            .await
                            .map_err(capture_error)?;
                        let active = projection_table(&self.prefix, specification.name);
                        let shadow = projection_table("eventlog_rebuild", specification.name);
                        transaction
                            .batch_execute(&format!(
                                "CREATE TEMP TABLE {shadow} (LIKE {active} INCLUDING ALL) ON COMMIT DROP"
                            ))
                            .await
                            .map_err(backend)?;
                    }

                    let callback_failed = Arc::new(AtomicBool::new(false));
                    // Two bounded passes over one REPEATABLE READ snapshot, one page retained at
                    // a time: the whole history is validated before the first callback, as when
                    // it was held in one vector, and then folded page by page. The exclusive
                    // publication lock and the snapshot make both passes read the same rows.
                    let page_statement = transaction
                        .prepare(&format!(
                            "SELECT {COLUMNS} FROM {}_events WHERE tenant_id=$1 AND \
                             ($2::bigint IS NULL OR global_seq>$2) ORDER BY global_seq LIMIT $3",
                            self.prefix
                        ))
                        .await
                        .map_err(backend)?;
                    let page_limit = to_i64(REBUILD_PAGE as u64)?;
                    let mut order = CapturedOrder::default();
                    let mut validated = 0_u64;
                    let mut after: Option<i64> = None;
                    loop {
                        let rows = transaction
                            .query(&page_statement, &[&tenant.as_str(), &after, &page_limit])
                            .await
                            .map_err(backend)?;
                        if rows.is_empty() {
                            break;
                        }
                        for row in &rows {
                            let event = read_event(row)?;
                            order.observe(tenant, &event).map_err(capture_error)?;
                            after = Some(to_i64(event.global_seq)?);
                            validated += 1;
                        }
                    }
                    let validated_through = after;
                    let mut projections = PostgresProjections {
                        client: &transaction,
                        blob_prefix: &self.prefix,
                        projection_prefix: "eventlog_rebuild",
                        lock_prefix: &self.prefix,
                        inline: &self.inline_names,
                        tenant,
                        admission: None,
                        reservation_pending: false,
                        callback_failed: Arc::clone(&callback_failed),
                        verified: &self.verified,
                        selected: Some(projector.projections()),
                    };
                    let mut position = 0_i64;
                    let mut applied = 0_u64;
                    let mut after: Option<i64> = None;
                    loop {
                        let rows = transaction
                            .query(&page_statement, &[&tenant.as_str(), &after, &page_limit])
                            .await
                            .map_err(backend)?;
                        if rows.is_empty() {
                            break;
                        }
                        #[cfg(test)]
                        tests::note_retained(&self.prefix, rows.len());
                        for row in &rows {
                            let event = read_event(row)?;
                            let result = projector.apply(&event, &mut projections).await;
                            ensure_callback_integrity(&callback_failed)?;
                            result?;
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
                        let active = projection_table(&self.prefix, specification.name);
                        let shadow = projection_table("eventlog_rebuild", specification.name);
                        transaction
                            .execute(
                                &format!("DELETE FROM {active} WHERE tenant_id=$1"),
                                &[&tenant.as_str()],
                            )
                            .await
                            .map_err(backend)?;
                        transaction
                            .execute(
                                &format!(
                                    "INSERT INTO {active} SELECT * FROM {shadow} WHERE tenant_id=$1"
                                ),
                                &[&tenant.as_str()],
                            )
                            .await
                            .map_err(backend)?;
                    }
                    ensure_callback_integrity(&callback_failed)?;
                    transaction.execute(&format!("INSERT INTO {}_projection_cursors(projection,tenant_id,global_seq,updated_at) VALUES($1,$2,$3,$4) ON CONFLICT(projection,tenant_id) DO UPDATE SET global_seq=EXCLUDED.global_seq,updated_at=EXCLUDED.updated_at",self.prefix), &[&projector.name(),&tenant.as_str(),&position,&OffsetDateTime::now_utc()]).await.map_err(backend)?;
                    Ok::<InlineRebuildResult, EventLogError>(InlineRebuildResult {
                        applied,
                        position: u64::try_from(position).map_err(|_| {
                            EventLogError::Backend("stored value is negative".into())
                        })?,
                    })
                }
                .await;
                match folded {
                    Ok(value) => {
                        commit_started.store(true, Ordering::Release);
                        match transaction.commit().await {
                            Ok(()) => (Ok(value), true),
                            Err(_) => (Err(EventLogError::UnknownCommit), false),
                        }
                    }
                    Err(error) => match transaction.rollback().await {
                        Ok(()) => (Err(error), true),
                        Err(rollback) => (Err(backend(rollback)), false),
                    },
                }
            }
        };

        let released = client
            .query_one(
                &format!("SELECT pg_advisory_unlock({ADVISORY_KEY})"),
                &[&publication],
            )
            .await
            .map(|row| row.get::<_, bool>(0));
        match (outcome, transaction_ended, released) {
            (Ok(value), true, Ok(true)) => {
                client.settled();
                Ok(value)
            }
            (Ok(_), _, _) => Err(EventLogError::UnknownCommit),
            (Err(error), true, Ok(true)) => {
                client.settled();
                Err(error)
            }
            (Err(error), _, _) => Err(error),
        }
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
    use crate::PostgresEventStore;

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

    fn prefix(case: &str) -> String {
        let mut nanos = time::OffsetDateTime::now_utc()
            .unix_timestamp_nanos()
            .unsigned_abs();
        let mut suffix = String::new();
        for _ in 0..12 {
            suffix.push(char::from(b'a' + (nanos % 26) as u8));
            nanos /= 26;
        }
        format!("paged_{case}_{suffix}")
    }

    /// A rebuild of a history longer than one page holds one page at a time, folds every event,
    /// and still validates the whole history before its first callback: a corrupt event on the
    /// last page refuses the rebuild with no callback made.
    #[tokio::test]
    async fn inline_rebuild_folds_bounded_pages_after_validating_every_page() {
        let Ok(url) = std::env::var("EVENTLOG_TEST_POSTGRES_URL") else {
            panic!("paged rebuild proof requires EVENTLOG_TEST_POSTGRES_URL");
        };
        let prefix = prefix("rebuild");
        let tenant = TenantId::new("paged").unwrap();
        let store = PostgresEventStore::connect(&url, &prefix).await.unwrap();
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
            most_retained(&prefix) <= REBUILD_PAGE,
            "the rebuild held {} events at once; a page is {REBUILD_PAGE}",
            most_retained(&prefix)
        );
        let latest = store
            .projection_get(&VERSIONS, &tenant, "two")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest["version"], 4);

        let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        assert_eq!(
            sql.execute(
                &format!(
                    "UPDATE {prefix}_events SET version=9 WHERE tenant_id=$1 AND global_seq=\
                     (SELECT MAX(global_seq) FROM {prefix}_events WHERE tenant_id=$1)"
                ),
                &[&tenant.as_str()],
            )
            .await
            .unwrap(),
            1
        );
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
        store.drop_tables().await.unwrap();
    }
}
