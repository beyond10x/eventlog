//! Native administration behind PostgreSQL's publication and registration coordinates.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use eventlog_core::{
    CaptureError, EventLogError, InlineProjectionAdmin, InlineRebuildResult, MAX_READ_LIMIT,
    Projector, TenantId, validate_captured_order, validate_identifier,
};
use time::OffsetDateTime;
use tokio_postgres::IsolationLevel;

use crate::{
    ADVISORY_KEY, COLUMNS, PostgresEventStore, PostgresProjections, backend, capture,
    ensure_callback_integrity, poisoned, projection_table, publication_identity, read_event,
    to_i64,
};

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
                    let mut events = Vec::new();
                    let mut after: Option<i64> = None;
                    loop {
                        let rows = transaction
                            .query(
                                &format!(
                                    "SELECT {COLUMNS} FROM {}_events WHERE tenant_id=$1 AND \
                                     ($2::bigint IS NULL OR global_seq>$2) ORDER BY global_seq LIMIT $3",
                                    self.prefix
                                ),
                                &[
                                    &tenant.as_str(),
                                    &after,
                                    &to_i64(MAX_READ_LIMIT as u64)?,
                                ],
                            )
                            .await
                            .map_err(backend)?;
                        if rows.is_empty() {
                            break;
                        }
                        for row in &rows {
                            let event = read_event(row)?;
                            after = Some(to_i64(event.global_seq)?);
                            events.push(event);
                        }
                    }
                    validate_captured_order(tenant, &events).map_err(capture_error)?;
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
                        selected: Some(projector.projections()),
                    };
                    let mut position = 0_i64;
                    let mut applied = 0_u64;
                    for event in &events {
                        let result = projector.apply(event, &mut projections).await;
                        ensure_callback_integrity(&callback_failed)?;
                        result?;
                        position = to_i64(event.global_seq)?;
                        applied = applied.checked_add(1).ok_or_else(|| {
                            EventLogError::Backend("inline rebuild event count overflow".into())
                        })?;
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
