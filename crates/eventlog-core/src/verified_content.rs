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
//! past it the least recently used entries are dropped until a new one fits, so one new content
//! does not evict everything else. A provider calls [`VerifiedContent::forget`] when its
//! handle deletes a blob, erases a tenant, or returns an error from a write that bound blobs, so
//! content of a write that returned an error is not kept and a deletion or erasure through the
//! handle leaves none of its bytes here. A callback that **panics** after a batch was bound
//! unwinds past that clearing, and the batch's bytes may stay until the budget, a later clearing
//! or the handle is dropped. A deletion or erasure through **another** handle or process does not
//! reach this one; such an entry is never returned — a hit requires storage to hold identical
//! bytes — but it is still a copy in this process's memory until one of those events.
//!
//! **Ordering.** A provider forgets both before a deletion or erasure and after it commits. Every
//! read notes the memory's generation when it looks up, hashes and compares **without** holding the
//! memory, and is remembered only if no [`VerifiedContent::forget`] happened in between. So a read
//! through the same handle that raced a deletion or erasure — and saw the row before the commit —
//! cannot put the erased bytes back once the post-commit forget has run, and reads that miss hash
//! in parallel rather than one at a time. A read that fetches, awaits (a statement, a commit) and
//! only then checks notes the generation before its fetch ([`VerifiedContent::generation`],
//! [`VerifiedContent::check_stored_since`]): a deletion that settles entirely inside that interval
//! would otherwise bump the generation before the check looked it up.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use crate::{EventLogError, validate_legacy_blob_count, validate_stored_blob};

/// The most content bytes one handle remembers. Past it the least recently used entries go first.
pub const VERIFIED_CONTENT_BUDGET: usize = 32 << 20;

/// What one handle has verified. One per handle; never shared between handles.
#[derive(Default)]
pub struct VerifiedContent {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Verified content by its SHA-256, exactly as it was verified, with its last use.
    by_hash: HashMap<String, Entry>,
    /// Every entry's hash by its last use, oldest first: the eviction order.
    by_use: BTreeMap<u64, String>,
    /// The next use stamp; only grows.
    clock: u64,
    /// The sum of the lengths in `by_hash`.
    held: usize,
    /// Full checks this memory could not answer and therefore ran.
    hashed: u64,
    /// Bumped by every [`VerifiedContent::forget`]; a check that began under an older value is
    /// not remembered.
    generation: u64,
}

struct Entry {
    bytes: Arc<[u8]>,
    used: u64,
}

impl State {
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// The content under `integrity_sha256`, marked as used now.
    fn touch(&mut self, integrity_sha256: &str) -> Option<Arc<[u8]>> {
        let used = self.tick();
        let entry = self.by_hash.get_mut(integrity_sha256)?;
        let previous = std::mem::replace(&mut entry.used, used);
        let bytes = Arc::clone(&entry.bytes);
        if let Some(hash) = self.by_use.remove(&previous) {
            self.by_use.insert(used, hash);
        }
        Some(bytes)
    }

    fn insert(&mut self, integrity_sha256: &str, bytes: &[u8]) {
        if bytes.len() > VERIFIED_CONTENT_BUDGET || self.by_hash.contains_key(integrity_sha256) {
            return;
        }
        // Drop the least recently used entries until the new one fits, not everything.
        while self.held + bytes.len() > VERIFIED_CONTENT_BUDGET {
            let Some((_, oldest)) = self.by_use.pop_first() else {
                break;
            };
            if let Some(evicted) = self.by_hash.remove(&oldest) {
                self.held -= evicted.bytes.len();
            }
        }
        let used = self.tick();
        self.held += bytes.len();
        self.by_use.insert(used, integrity_sha256.to_owned());
        self.by_hash.insert(
            integrity_sha256.to_owned(),
            Entry {
                bytes: Arc::from(bytes),
                used,
            },
        );
    }

    fn clear(&mut self) {
        self.by_hash.clear();
        self.by_use.clear();
        self.held = 0;
    }
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> EventLogError {
    EventLogError::Backend("the store lock was poisoned by a panic".to_owned())
}

/// The memory's generation, noted by a read **before** it fetches anything from storage.
///
/// A read whose fetch and check are separated by awaits — a statement, a commit — notes this
/// first and hands it to [`VerifiedContent::check_stored_since`]. Content is then remembered only
/// if no [`VerifiedContent::forget`] happened since the fetch began, so a same-handle deletion
/// that finished between the fetch and the check cannot leave the deleted bytes remembered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentGeneration(u64);

/// What one lookup saw: the remembered content under the hash, if any, and the generation.
struct Lookup {
    remembered: Option<Arc<[u8]>>,
    generation: u64,
}

impl Lookup {
    /// Whether `bytes` equal the remembered content in full. Compared outside the memory's lock.
    fn hit(&self, bytes: &[u8]) -> bool {
        self.remembered
            .as_deref()
            .is_some_and(|verified| verified == bytes)
    }
}

impl VerifiedContent {
    fn look_up(&self, integrity_sha256: Option<&str>) -> Result<Lookup, EventLogError> {
        let mut state = self.state.lock().map_err(poisoned)?;
        Ok(Lookup {
            remembered: integrity_sha256.and_then(|hash| state.touch(hash)),
            generation: state.generation,
        })
    }

    /// Count one full check before it runs, so a check that refuses is counted too.
    fn count_full_check(&self) -> Result<(), EventLogError> {
        self.state.lock().map_err(poisoned)?.hashed += 1;
        Ok(())
    }

    /// Remember content a full check accepted, unless a forget happened since its lookup.
    fn settle(&self, lookup: &Lookup, integrity_sha256: &str, bytes: &[u8]) {
        if let Ok(mut state) = self.state.lock()
            && state.generation == lookup.generation
        {
            state.insert(integrity_sha256, bytes);
        }
    }

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
        self.check_stored_from(None, bytes, byte_count, integrity_sha256, integrity_v1)
    }

    /// The memory's generation now. Note it before fetching a row whose check comes later.
    ///
    /// # Errors
    /// [`EventLogError::Backend`] when the memory's lock was poisoned.
    pub fn generation(&self) -> Result<ContentGeneration, EventLogError> {
        Ok(ContentGeneration(
            self.state.lock().map_err(poisoned)?.generation,
        ))
    }

    /// [`Self::check_stored`] for a row fetched after `since` was noted: the row is remembered
    /// only if no [`Self::forget`] happened since then, not merely since this check began.
    ///
    /// # Errors
    /// [`EventLogError::Backend`] for corrupt persisted metadata or bytes.
    pub fn check_stored_since(
        &self,
        since: ContentGeneration,
        bytes: Vec<u8>,
        byte_count: i64,
        integrity_sha256: Option<String>,
        integrity_v1: i64,
    ) -> Result<Vec<u8>, EventLogError> {
        self.check_stored_from(
            Some(since),
            bytes,
            byte_count,
            integrity_sha256,
            integrity_v1,
        )
    }

    fn check_stored_from(
        &self,
        since: Option<ContentGeneration>,
        bytes: Vec<u8>,
        byte_count: i64,
        integrity_sha256: Option<String>,
        integrity_v1: i64,
    ) -> Result<Vec<u8>, EventLogError> {
        let mut lookup = self.look_up(integrity_sha256.as_deref())?;
        if let Some(ContentGeneration(noted)) = since {
            lookup.generation = noted;
        }
        if integrity_v1 == 1 && lookup.hit(&bytes) {
            validate_legacy_blob_count(&bytes, byte_count)?;
            return Ok(bytes);
        }
        self.count_full_check()?;
        let hash = integrity_sha256.clone();
        let bytes = validate_stored_blob(bytes, byte_count, integrity_sha256, integrity_v1)?;
        if let Some(hash) = hash {
            self.settle(&lookup, &hash, &bytes);
        }
        Ok(bytes)
    }

    /// Whether `bytes` are the content `integrity_sha256` names: `true` without hashing when they
    /// equal remembered content in full, otherwise `full`'s answer, remembered when it is `true`.
    ///
    /// `full` is the provider's complete check: it must hash `bytes` and compare the result with
    /// `integrity_sha256`, and answer `true` only when they agree. It runs without the memory's
    /// lock held.
    ///
    /// # Errors
    /// [`EventLogError::Backend`] when the memory's lock was poisoned.
    pub fn check_recorded(
        &self,
        bytes: &[u8],
        integrity_sha256: &str,
        full: impl FnOnce(&[u8]) -> bool,
    ) -> Result<bool, EventLogError> {
        let lookup = self.look_up(Some(integrity_sha256))?;
        if lookup.hit(bytes) {
            return Ok(true);
        }
        self.count_full_check()?;
        let matched = full(bytes);
        if matched {
            self.settle(&lookup, integrity_sha256, bytes);
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

    /// Drop everything remembered, so deleted or erased content is not kept in memory, and refuse
    /// to remember any check that began before this call.
    pub fn forget(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.clear();
            state.generation += 1;
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

    /// Whether another thread can use the memory within two seconds.
    fn usable_from_another_thread(verified: &std::sync::Arc<VerifiedContent>) -> bool {
        let (sent, received) = std::sync::mpsc::channel();
        let other = std::sync::Arc::clone(verified);
        std::thread::spawn(move || {
            let _ = sent.send(other.entries());
        });
        received
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok()
    }

    /// The SHA-256 runs without the handle-wide lock, so reads through one handle that miss hash
    /// in parallel rather than one at a time.
    #[test]
    fn a_full_check_runs_without_holding_the_memory() {
        let verified = std::sync::Arc::new(VerifiedContent::default());
        let bytes = b"object".to_vec();
        let hash = blob_integrity_sha256(&bytes);
        let mut free = false;
        assert!(
            verified
                .check_recorded(&bytes, &hash, |candidate| {
                    free = usable_from_another_thread(&verified);
                    blob_integrity_sha256(candidate) == hash
                })
                .unwrap()
        );
        assert!(free, "the full check ran while holding the memory");
    }

    /// Content whose check began before a `forget` is not remembered after it: a read racing a
    /// deletion or erasure through the same handle cannot put the erased bytes back.
    #[test]
    fn a_check_that_began_before_a_forget_remembers_nothing() {
        let verified = VerifiedContent::default();
        let bytes = b"object".to_vec();
        let hash = blob_integrity_sha256(&bytes);
        assert!(
            verified
                .check_recorded(&bytes, &hash, |candidate| {
                    verified.forget();
                    blob_integrity_sha256(candidate) == hash
                })
                .unwrap()
        );
        assert_eq!((verified.held(), verified.entries()), (0, 0));
        assert_eq!(verified.hashed(), 1);
    }

    /// A row fetched before a `forget` and checked after it is returned but not remembered when
    /// the read noted the generation before its fetch; an unnoted check would remember it.
    #[test]
    fn a_row_fetched_before_a_forget_is_not_remembered_by_a_later_check() {
        let verified = VerifiedContent::default();
        let bytes = b"fetched".to_vec();
        let hash = blob_integrity_sha256(&bytes);
        let noted = verified.generation().unwrap();
        verified.forget();
        verified.forget();
        assert_eq!(
            verified
                .check_stored_since(noted, bytes.clone(), 7, Some(hash.clone()), 1)
                .unwrap(),
            bytes,
            "the read still answers with the bytes it fetched"
        );
        assert_eq!((verified.held(), verified.entries()), (0, 0));
        let current = verified.generation().unwrap();
        verified
            .check_stored_since(current, bytes.clone(), 7, Some(hash), 1)
            .unwrap();
        assert_eq!(
            verified.entries(),
            1,
            "a fetch after the forget is remembered"
        );
    }

    /// A full memory makes room for one new content by dropping the least recently used entry,
    /// not everything: the contents still in use are answered without another full check, and a
    /// `forget` still drops them all.
    #[test]
    fn one_new_content_evicts_only_the_least_recently_used_entry() {
        const MIB: usize = 1 << 20;
        let verified = VerifiedContent::default();
        let blobs: Vec<(String, Vec<u8>)> = (0..=VERIFIED_CONTENT_BUDGET / MIB)
            .map(|index| {
                (
                    format!("hash-{index}"),
                    vec![u8::try_from(index).unwrap(); MIB],
                )
            })
            .collect();
        let (cold, active) = blobs.split_last().unwrap();
        // The full check is not what this case measures; counting it is.
        let full = |_: &[u8]| true;
        for (hash, bytes) in active {
            assert!(verified.check_recorded(bytes, hash, full).unwrap());
        }
        assert_eq!(verified.held(), VERIFIED_CONTENT_BUDGET);
        for (hash, bytes) in &active[1..] {
            assert!(verified.check_recorded(bytes, hash, full).unwrap());
        }
        let filled = verified.hashed();
        assert!(verified.check_recorded(&cold.1, &cold.0, full).unwrap());
        assert_eq!(
            verified.entries(),
            active.len(),
            "exactly one entry made room"
        );
        for (hash, bytes) in &active[1..] {
            assert!(verified.check_recorded(bytes, hash, full).unwrap());
        }
        assert_eq!(
            verified.hashed(),
            filled + 1,
            "only the new content was checked in full; the recently used ones stayed"
        );
        assert!(
            verified
                .check_recorded(&active[0].1, &active[0].0, full)
                .unwrap()
        );
        assert_eq!(
            verified.hashed(),
            filled + 2,
            "the least recently used entry was the one dropped"
        );
        verified.forget();
        assert_eq!((verified.held(), verified.entries()), (0, 0));
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
