//! Transaction-internal admission coordinates. These are storage mechanics, not domain policy.
use crate::{EventLogError, TenantId, validate_field};
use std::sync::Arc;

/// An unforgeable-in-process grant issued by the concrete store to its trusted host.
/// Domain applications receive only `dyn EventStore`; neither that port nor projector callbacks
/// expose this grant. A newly constructed grant cannot impersonate another store's grant.
#[derive(Clone, Debug)]
pub struct AdmissionPermit(Arc<()>);
impl Default for AdmissionPermit {
    fn default() -> Self {
        Self(Arc::new(()))
    }
}
impl AdmissionPermit {
    /// Backends compare grants by allocation identity, never by caller-supplied names.
    pub fn same_authority(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Exact typed coordinates within one owner database/schema. Deployment is not a tenant value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionScope {
    Tenant { tenant: TenantId, key: String },
    Deployment { key: String },
}
impl AdmissionScope {
    /// Collision-safe encoding used by the durable `ScopeCounter` record.
    /// # Errors
    /// Refuses invalid opaque keys; never normalizes tenant or service coordinate bytes.
    pub fn coordinate(&self) -> Result<String, EventLogError> {
        let encode = |value: &str| format!("{}:{value}", value.len());
        match self {
            Self::Tenant { tenant, key } => {
                validate_field("admission key", key)?;
                Ok(format!("t{}{}", encode(tenant.as_str()), encode(key)))
            }
            Self::Deployment { key } => {
                validate_field("admission key", key)?;
                Ok(format!("d{}", encode(key)))
            }
        }
    }
    /// Tenant scopes may only address the current append's exact tenant.
    pub fn belongs_to(&self, tenant: &TenantId) -> bool {
        match self {
            Self::Tenant { tenant: scoped, .. } => scoped == tenant,
            Self::Deployment { .. } => true,
        }
    }
}

/// One counter delta checked against a host-selected ceiling in the append transaction.
/// A zero delta checks a policy change/read under the same locks; a negative delta releases.
#[derive(Clone, Debug)]
pub struct Reservation {
    pub scope: AdmissionScope,
    pub delta: i64,
    pub ceiling: i64,
}

/// Validate and deterministically order one atomic reservation request.
/// # Errors
/// Refuses duplicate coordinates, cross-tenant references, negative ceilings and unbounded lists.
pub fn ordered_reservations<'a>(
    reservations: &'a [Reservation],
    tenant: &TenantId,
) -> Result<Vec<(String, &'a Reservation)>, EventLogError> {
    if reservations.is_empty() || reservations.len() > 64 {
        return Err(EventLogError::Invalid(
            "admission requires 1..64 scopes".into(),
        ));
    }
    let mut ordered = Vec::with_capacity(reservations.len());
    for reservation in reservations {
        if !reservation.scope.belongs_to(tenant) || reservation.ceiling < 0 {
            return Err(EventLogError::Invalid(
                "invalid or cross-tenant admission scope".into(),
            ));
        }
        ordered.push((reservation.scope.coordinate()?, reservation));
    }
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(EventLogError::Invalid("duplicate admission scope".into()));
    }
    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn coordinates_preserve_boundaries_and_grants_cannot_be_reconstructed() {
        let a = AdmissionScope::Tenant {
            tenant: TenantId::new("a:b").unwrap(),
            key: "c".into(),
        };
        let b = AdmissionScope::Tenant {
            tenant: TenantId::new("a").unwrap(),
            key: "b:c".into(),
        };
        let d = AdmissionScope::Deployment {
            key: "a:b:c".into(),
        };
        assert_ne!(a.coordinate().unwrap(), b.coordinate().unwrap());
        assert_ne!(a.coordinate().unwrap(), d.coordinate().unwrap());
        let permit = AdmissionPermit::default();
        assert!(permit.same_authority(&permit.clone()));
        assert!(!permit.same_authority(&AdmissionPermit::default()));
    }
}
