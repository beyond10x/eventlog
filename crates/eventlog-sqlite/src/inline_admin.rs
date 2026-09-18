//! Native inline administration inside one owned blocking worker.

use std::{
    collections::BTreeSet,
    sync::{Arc, atomic::AtomicBool},
};

use eventlog_core::{
    CaptureError, EventLogError, InlineProjectionAdmin, InlineRebuildResult, MAX_READ_LIMIT,
    Projector, TenantId, validate_captured_order, validate_identifier,
};
use rusqlite::params;
use time::OffsetDateTime;

use crate::{
    COLUMNS, Inner, SqliteEventStore, SqliteProjections, backend, begin_immediate, capture, drive,
    ensure_callback_integrity, finish_transaction, format_time, joined, poisoned, projection_table,
    read_event, run_blocking, to_i64,
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
                let shadow = projection_table("eventlog_rebuild", specification.name);
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

            let mut events = Vec::new();
            let mut after: Option<i64> = None;
            loop {
                let page = {
                    let mut statement = connection
                        .prepare(&format!(
                            "SELECT {COLUMNS} FROM {}_events WHERE tenant_id=?1 AND (?2 IS NULL OR global_seq>?2) \
                             ORDER BY global_seq LIMIT ?3",
                            self.prefix
                        ))
                        .map_err(backend)?;
                    let rows = statement
                        .query_map(
                            params![tenant.as_str(), after, to_i64(MAX_READ_LIMIT as u64)?],
                            read_event,
                        )
                        .map_err(backend)?;
                    let mut page = Vec::new();
                    for row in rows {
                        page.push(row.map_err(backend)??);
                    }
                    page
                };
                if page.is_empty() {
                    break;
                }
                for event in page {
                    after = Some(to_i64(event.global_seq)?);
                    events.push(event);
                }
            }
            validate_captured_order(tenant, &events).map_err(capture_error)?;
            let mut projections = SqliteProjections {
                connection: &mut connection,
                blob_prefix: &self.prefix,
                projection_prefix: "eventlog_rebuild",
                inline: &self.inline_names,
                tenant,
                admission: None,
                callback_failed: Arc::clone(&callback_failed),
                selected: Some(projector.projections()),
            };
            let mut position = 0_i64;
            let mut applied = 0_u64;
            for event in &events {
                let outcome = drive(projector.apply(event, &mut projections));
                ensure_callback_integrity(&callback_failed)?;
                outcome?;
                position = to_i64(event.global_seq)?;
                applied = applied.checked_add(1).ok_or_else(|| {
                    EventLogError::Backend("inline rebuild event count overflow".into())
                })?;
            }
            for specification in projector.projections() {
                let active = projection_table(&self.prefix, specification.name);
                let shadow = projection_table("eventlog_rebuild", specification.name);
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
