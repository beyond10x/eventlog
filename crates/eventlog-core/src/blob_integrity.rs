use sha2::{Digest, Sha256};

use crate::EventLogError;

/// How an SQL provider handles a populated blob table from the predecessor edition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LegacyBlobMigration {
    /// Refuse a populated predecessor until its owner explicitly accepts the observed bytes.
    #[default]
    RefusePopulated,
    /// Hash every observed predecessor row inside the provider's fenced migration transaction.
    TrustObservedBytes,
}

/// The finite result of one acknowledged SQL blob migration attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobMigrationReport {
    pub upgraded: bool,
    pub trusted_legacy_rows: u64,
}

#[must_use]
pub fn blob_integrity_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Validate the predecessor's count before trusting its observed bytes.
///
/// # Errors
/// Returns [`EventLogError::Backend`] for corrupt persisted metadata.
pub fn validate_legacy_blob_count(bytes: &[u8], byte_count: i64) -> Result<(), EventLogError> {
    let byte_count = usize::try_from(byte_count)
        .map_err(|_| EventLogError::Backend("stored blob integrity metadata is invalid".into()))?;
    if byte_count != bytes.len() {
        return Err(EventLogError::Backend(
            "stored blob integrity metadata is invalid".into(),
        ));
    }
    Ok(())
}

/// Validate and return bytes from the current SQL blob edition.
///
/// # Errors
/// Returns [`EventLogError::Backend`] for corrupt persisted metadata or bytes.
pub fn validate_stored_blob(
    bytes: Vec<u8>,
    byte_count: i64,
    integrity_sha256: Option<String>,
    integrity_v1: i64,
) -> Result<Vec<u8>, EventLogError> {
    validate_legacy_blob_count(&bytes, byte_count)?;
    let hash = integrity_sha256.ok_or_else(|| {
        EventLogError::Backend("stored blob integrity metadata is invalid".into())
    })?;
    if integrity_v1 != 1
        || hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || blob_integrity_sha256(&bytes) != hash
    {
        return Err(EventLogError::Backend(
            "stored blob integrity metadata is invalid".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validator_is_fallible_and_accepts_only_the_closed_first_edition() {
        let bytes = b"observed".to_vec();
        let hash = blob_integrity_sha256(&bytes);
        assert_eq!(
            validate_stored_blob(bytes.clone(), 8, Some(hash.clone()), 1).unwrap(),
            bytes
        );
        for result in [
            validate_stored_blob(bytes.clone(), -1, Some(hash.clone()), 1),
            validate_stored_blob(bytes.clone(), 7, Some(hash.clone()), 1),
            validate_stored_blob(bytes.clone(), 8, None, 1),
            validate_stored_blob(bytes.clone(), 8, Some(hash.to_uppercase()), 1),
            validate_stored_blob(bytes.clone(), 8, Some("0".repeat(64)), 1),
            validate_stored_blob(bytes.clone(), 8, Some(hash), 2),
        ] {
            assert!(matches!(result, Err(EventLogError::Backend(_))));
        }
    }
}
