use crate::{Transaction, state::Op};
use eventlog_core::{
    AdmissionPermit, AdmissionScope, BoxFuture, EventLogError, ProjectionSpec, ProjectionStore,
    Reservation, TenantId, bounded_limit, indexed_value, ordered_reservations,
};
use serde_json::Value;

pub(crate) struct View<'a> {
    pub tx: &'a mut Transaction,
    pub tenant: &'a TenantId,
    pub admission: bool,
}
impl View<'_> {
    pub fn validate(&self, spec: &ProjectionSpec, tenant: &TenantId) -> Result<(), EventLogError> {
        spec.validate()?;
        if tenant != self.tenant {
            return Err(EventLogError::Invalid(
                "projection crosses tenant boundary".into(),
            ));
        }
        let indexed: Vec<String> = spec.indexed.iter().map(|s| (*s).into()).collect();
        if self
            .tx
            .state
            .dirty_views
            .contains(&(tenant.as_str().into(), spec.name.into()))
        {
            return Err(EventLogError::Invalid(
                "projection requires a complete rebuild after redaction".into(),
            ));
        }
        if self.tx.state.projections.get(spec.name) != Some(&indexed) {
            return Err(EventLogError::Invalid(
                "projection was not admitted with this shape".into(),
            ));
        }
        Ok(())
    }
}
impl ProjectionStore for View<'_> {
    fn reserve<'a>(
        &'a mut self,
        permit: &'a AdmissionPermit,
        reservations: &'a [Reservation],
    ) -> BoxFuture<'a, Result<Vec<i64>, EventLogError>> {
        Box::pin(async move {
            if !self.admission || !self.tx.permit.same_authority(permit) {
                return Err(EventLogError::Invalid(
                    "admission requires the trusted guard's permit".into(),
                ));
            }
            let ordered = ordered_reservations(reservations, self.tenant)?;
            let mut writes = Vec::new();
            let mut values = Vec::new();
            for (coordinate, reservation) in ordered {
                let old = self
                    .tx
                    .state
                    .counters
                    .get(&coordinate)
                    .copied()
                    .unwrap_or(0);
                let value = old.checked_add(reservation.delta).ok_or_else(|| {
                    EventLogError::Invalid("admission ceiling or release bound refused".into())
                })?;
                if value < 0 || value > reservation.ceiling {
                    return Err(EventLogError::Invalid(
                        "admission ceiling or release bound refused".into(),
                    ));
                }
                let tenant = match &reservation.scope {
                    AdmissionScope::Tenant { tenant, .. } => Some(tenant.clone()),
                    AdmissionScope::Deployment { .. } => None,
                };
                writes.push(Op::Counter {
                    tenant,
                    coordinate,
                    value,
                });
                values.push(value);
            }
            for op in writes {
                self.tx.record(op)?;
            }
            Ok(values)
        })
    }
    fn get_blob<'a>(
        &'a mut self,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, EventLogError>> {
        Box::pin(async move { self.tx.blob(self.tenant, digest) })
    }
    fn upsert<'a>(
        &'a mut self,
        spec: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
        body: &'a Value,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.validate(spec, tenant)?;
            self.tx.record(Op::Row {
                tenant: tenant.clone(),
                name: spec.name.into(),
                key: key.into(),
                body: Some(body.clone()),
            })
        })
    }
    fn delete<'a>(
        &'a mut self,
        spec: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            self.validate(spec, tenant)?;
            self.tx.record(Op::Row {
                tenant: tenant.clone(),
                name: spec.name.into(),
                key: key.into(),
                body: None,
            })
        })
    }
    fn get<'a>(
        &'a mut self,
        spec: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        Box::pin(async move {
            self.validate(spec, tenant)?;
            Ok(self
                .tx
                .state
                .rows
                .get(&(tenant.as_str().into(), spec.name.into(), key.into()))
                .cloned())
        })
    }
    fn get_for_update<'a>(
        &'a mut self,
        spec: &'a ProjectionSpec,
        tenant: &'a TenantId,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Value>, EventLogError>> {
        Box::pin(async move {
            if !self
                .tx
                .inline
                .iter()
                .any(|p| p.projections().iter().any(|s| s.name == spec.name))
            {
                return Err(EventLogError::Invalid(
                    "guard reads a projection not driven inline".into(),
                ));
            }
            self.get(spec, tenant, key).await
        })
    }
    fn find<'a>(
        &'a mut self,
        spec: &'a ProjectionSpec,
        tenant: &'a TenantId,
        field: &'a str,
        value: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Value>, EventLogError>> {
        Box::pin(async move {
            self.validate(spec, tenant)?;
            if spec.field_position(field).is_none() {
                return Err(EventLogError::Invalid(
                    "field was not declared indexed".into(),
                ));
            }
            Ok(self
                .tx
                .state
                .rows
                .iter()
                .filter(|((t, n, _), body)| {
                    t == tenant.as_str()
                        && n == spec.name
                        && indexed_value(body, field).as_deref() == Some(value)
                })
                .take(bounded_limit(limit))
                .map(|(_, body)| body.clone())
                .collect())
        })
    }
}
