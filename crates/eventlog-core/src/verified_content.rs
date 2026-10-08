//! Blob content one handle has already verified: `eventlog.blobs.VerifiedContent` in
//! `ess/blob-integrity/domains/blobs.yaml`, keyed by its integrity SHA-256.
//!
//! Every provider checks blob content before returning it: the SQL providers validate the stored
//! length, edition and SHA-256 (`docs/design/sql-blob-read-integrity.md`), and the File provider
//! hashes an object against the hash its committed history recorded
//! (`docs/design/file-provider.md`). That check is a pure function of what was just read, so a
//! handle may answer it **once per distinct content, not once per read.**
//!
//! A handle keeps each `(integrity_sha256, bytes)` pair it has verified, holding the exact bytes.
//! A later read naming that hash is accepted without a SHA-256 only if the bytes it just read are
//! *equal* to the remembered ones, compared in full. Equal inputs give the check's equal answer,
//! so the answer is the one the full check would give — the only thing saved is the hash. Bytes or
//! a hash changed after a verified read are therefore not a hit and are checked in full, and a
//! read is still a read: nothing here returns bytes the provider did not just read from storage.
//!
//! **What is remembered, and for how long.** Entries come only from content the handle read and
//! verified, or wrote and hashed itself. They are bounded by [`VERIFIED_CONTENT_BUDGET`] bytes;
//! past it the memory is cleared, not grown. A provider calls [`VerifiedContent::forget`] when its
//! handle deletes a blob, erases a tenant, or returns an error from a write that bound blobs, so
//! content of a write that returned an error is not kept and a deletion or erasure through the
//! handle leaves none of its bytes here. A callback that **panics** after a batch was bound
//! unwinds past that clearing, and the batch's bytes may stay until the budget, a later clearing
//! or the handle is dropped. A deletion or erasure through **another** handle or process does not
//! reach this one; such an entry is never returned — a hit requires storage to hold identical
//! bytes — but it is still a copy in this process's memory until one of those events.

use std::{collections::HashMap, sync::Mutex};

use crate::{EventLogError, validate_legacy_blob_count, validate_stored_blob};

/// The most content bytes one handle remembers. Past it the memory is cleared, not grown.
pub const VERIFIED_CONTENT_BUDGET: usize = 32 << 20;

/// What one handle has verified. One per handle; never shared between handles.
#[derive(Default)]
pub struct VerifiedContent {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Verified content by its SHA-256, exactly as it was verified.
    by_hash: HashMap<String, Vec<u8>>,
    /// The sum of the lengths in `by_hash`.
    held: usize,
    /// Full checks this memory could not answer and therefore ran.
    hashed: u64,
}

impl State {
    fn known(&self, integrity_sha256: &str, bytes: &[u8]) -> bool {
        self.by_hash
            .get(integrity_sha256)
            .is_some_and(|verified| verified.as_slice() == bytes)
    }

    fn insert(&mut self, integrity_sha256: &str, bytes: &[u8]) {
        if bytes.len() > VERIFIED_CONTENT_BUDGET || self.by_hash.contains_key(integrity_sha256) {
            return;
        }
        if self.held + bytes.len() > VERIFIED_CONTENT_BUDGET {
            self.by_hash.clear();
            self.held = 0;
        }
        self.held += bytes.len();
        self.by_hash
            .insert(integrity_sha256.to_owned(), bytes.to_vec());
    }
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> EventLogError {
    EventLogError::Backend("the store lock was poisoned by a panic".to_owned())
}

impl VerifiedContent {
    /// Validate one stored SQL row — [`validate_stored_blob`] — and return its bytes, skipping
    /// the SHA-256 when its hash names remembered content equal to `bytes` in full.
    ///
    /// A hit still requires `integrity_v1 == 1` and `byte_count` equal to the length, the checks
    /// the full validator makes without hashing; a row that passes in full is remembered.
    ///
    /// # Errors
    /// [`EventLogError::Backend`] for corrupt persisted metadata or bytes.
    pub fn check_stored(
        &self,
        bytes: Vec<u8>,
        byte_count: i64,
        integrity_sha256: Option<String>,
        integrity_v1: i64,
    ) -> Result<Vec<u8>, EventLogError> {
        let mut state = self.state.lock().map_err(poisoned)?;
        if integrity_v1 == 1
            && integrity_sha256
                .as_deref()
                .is_some_and(|hash| state.known(hash, &bytes))
        {
            validate_legacy_blob_count(&bytes, byte_count)?;
            return Ok(bytes);
        }
        state.hashed += 1;
        let hash = integrity_sha256.clone();
        let bytes = validate_stored_blob(bytes, byte_count, integrity_sha256, integrity_v1)?;
        if let Some(hash) = hash {
            state.insert(&hash, &bytes);
        }
        Ok(bytes)
    }

    /// Whether `bytes` are the content `integrity_sha256` names: `true` without hashing when they
    /// equal remembered content in full, otherwise `full`'s answer, remembered when it is `true`.
    ///
    /// `full` is the provider's complete check: it must hash `bytes` and compare the result with
    /// `integrity_sha256`, and answer `true` only when they agree.
    ///
    /// # Errors
    /// [`EventLogError::Backend`] when the memory's lock was poisoned.
    pub fn check_recorded(
        &self,
        bytes: &[u8],
        integrity_sha256: &str,
        full: impl FnOnce(&[u8]) -> bool,
    ) -> Result<bool, EventLogError> {
        let mut state = self.state.lock().map_err(poisoned)?;
        if state.known(integrity_sha256, bytes) {
            return Ok(true);
        }
        state.hashed += 1;
        let matched = full(bytes);
        if matched {
            state.insert(integrity_sha256, bytes);
        }
        Ok(matched)
    }

    /// Record that this handle computed `integrity_sha256` over `bytes` itself, at write time.
    ///
    /// Only a hash this handle computed from these exact bytes belongs here. The pair is true
    /// whether or not the write commits; a write that does not commit calls [`Self::forget`] so
    /// its bytes are not kept.
    pub fn remember(&self, integrity_sha256: &str, bytes: &[u8]) {
        if let Ok(mut state) = self.state.lock() {
            state.insert(integrity_sha256, bytes);
        }
    }

    /// Drop everything remembered, so deleted or erased content is not kept in memory.
    pub fn forget(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.by_hash.clear();
            state.held = 0;
        }
    }

    /// Content bytes remembered now; never more than [`VERIFIED_CONTENT_BUDGET`].
    #[must_use]
    pub fn held(&self) -> usize {
        self.state.lock().map_or(0, |state| state.held)
    }

    /// Distinct contents remembered now.
    #[must_use]
    pub fn entries(&self) -> usize {
        self.state.lock().map_or(0, |state| state.by_hash.len())
    }

    /// Full checks run through this memory so far: the reads it could not answer by comparison.
    #[must_use]
    pub fn hashed(&self) -> u64 {
        self.state.lock().map_or(0, |state| state.hashed)
    }
}

#[cfg(test)]
mod tests {
    use super::{VERIFIED_CONTENT_BUDGET, VerifiedContent};
    use crate::{EventLogError, blob_integrity_sha256};

    #[test]
    fn remembered_content_stays_within_its_budget_and_forget_drops_it() {
        let verified = VerifiedContent::default();
        let chunk = vec![7u8; VERIFIED_CONTENT_BUDGET / 3 + 1];
        for index in 0..8u8 {
            let mut bytes = chunk.clone();
            bytes[0] = index;
            verified.remember(&blob_integrity_sha256(&bytes), &bytes);
            assert!(verified.held() <= VERIFIED_CONTENT_BUDGET, "after {index}");
        }
        let oversized = vec![1u8; VERIFIED_CONTENT_BUDGET + 1];
        verified.remember(&blob_integrity_sha256(&oversized), &oversized);
        assert!(verified.held() <= VERIFIED_CONTENT_BUDGET);
        verified.forget();
        assert_eq!((verified.held(), verified.entries()), (0, 0));
    }

    #[test]
    fn a_stored_row_is_hashed_once_and_any_change_is_checked_in_full() {
        let verified = VerifiedContent::default();
        let bytes = b"observed".to_vec();
        let hash = blob_integrity_sha256(&bytes);
        for _ in 0..5 {
            assert_eq!(
                verified
                    .check_stored(bytes.clone(), 8, Some(hash.clone()), 1)
                    .unwrap(),
                bytes
            );
        }
        assert_eq!(verified.hashed(), 1);
        for result in [
            verified.check_stored(b"observeD".to_vec(), 8, Some(hash.clone()), 1),
            verified.check_stored(bytes.clone(), 7, Some(hash.clone()), 1),
            verified.check_stored(bytes.clone(), 8, Some(hash.clone()), 2),
            verified.check_stored(bytes.clone(), 8, None, 1),
        ] {
            assert!(matches!(result, Err(EventLogError::Backend(_))));
        }
        assert_eq!(
            verified.hashed(),
            4,
            "a changed length is refused by the length check, without hashing"
        );
    }

    #[test]
    fn recorded_content_is_hashed_once_and_changed_bytes_are_hashed_and_refused() {
        let verified = VerifiedContent::default();
        let bytes = b"object".to_vec();
        let hash = blob_integrity_sha256(&bytes);
        let full = |candidate: &[u8]| blob_integrity_sha256(candidate) == hash;
        for _ in 0..5 {
            assert!(verified.check_recorded(&bytes, &hash, full).unwrap());
        }
        assert_eq!(verified.hashed(), 1);
        for _ in 0..3 {
            assert!(!verified.check_recorded(b"objecT", &hash, full).unwrap());
        }
        assert_eq!(verified.hashed(), 4, "a refusal is never remembered");
    }
}
