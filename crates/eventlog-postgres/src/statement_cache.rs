//! The prepared statements of one physical connection.
//!
//! A prepared statement is state of the server session that holds it, so the cache is a field of
//! the pooled connection and dies with it: a retired, closed or cancelled connection takes its
//! statements with it and no other connection is ever handed a name it did not prepare. The cache
//! is keyed by the exact SQL text and knows nothing about what the text means.
//!
//! The rules, each pinned by a test below:
//!
//! - The first use of a text on a connection runs unprepared and is only remembered; the second
//!   use prepares it and executes it prepared. A connection that serves one find never pays the
//!   extra prepare round trip.
//! - At most [`CAPACITY`] statements are prepared per connection, 64. A prepared statement is
//!   never evicted to make room: eviction closes a statement, and that close lands on the next,
//!   unrelated operation of the connection. A text whose second use arrives when 64 are prepared
//!   is never prepared and always runs unprepared; past the cap a store pays the parse on every
//!   call of the extra texts, silently, and the documentation of the pool says so.
//! - At most [`CAPACITY`] texts that were used once are remembered, apart from the prepared ones.
//!   They hold no server state. When that set is full, a new text replaces the newest of them, so
//!   the older ones keep their place and reach their second use, and 64 one-off texts never keep
//!   a hot text from being prepared: it takes the slot of the newest one-off text.
//! - A statement the server reports stale (a changed result type, a dropped object) is forgotten
//!   and its text starts again as first use.
//! - A connection whose server session does not keep named statements is refused for good: after
//!   [`StatementCache::refuse`] no text is ever prepared again on it.
use std::collections::HashMap;

/// The most distinct SQL texts one connection remembers. A constant, not a knob.
pub(crate) const CAPACITY: usize = 64;

/// What the caller does with a text it is about to run.
pub(crate) enum Step<S> {
    /// Run the text unprepared.
    Unprepared,
    /// Prepare the text now, report the statement with [`StatementCache::prepared`], run it.
    Prepare,
    /// Run this statement.
    Use(S),
}

/// How an error of a prepared statement's execution is handled.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Execution {
    /// The statement no longer fits the database: forget it and run the text once unprepared.
    Stale,
    /// The session does not keep named statements: refuse the connection and run the text once
    /// unprepared.
    Refused,
    /// Any other error is returned as it is.
    Other,
}

/// Executing a cached statement. Raised at Bind: 0A000 (the cached plan must not change its
/// result type), 42703 and 42P01 (an object the statement names changed) are stale; 26000 (the
/// session has no such statement) and 08P01 (a pooler in front of the session does not keep
/// them) refuse the connection.
pub(crate) fn classify_execution(code: Option<&str>) -> Execution {
    match code {
        Some("0A000" | "42703" | "42P01") => Execution::Stale,
        Some("26000" | "08P01") => Execution::Refused,
        _ => Execution::Other,
    }
}

/// Preparing a statement. These codes mean the session or a pooler in front of it does not keep
/// named statements (0A000, 08P01, 42P05 a name already present on a multiplexed server session,
/// 26000). Any other error is a real failure: a cancelled or timed out prepare (57014, 55P03),
/// an exhausted server (53300), a closed connection, a missing table.
pub(crate) fn refuses_prepare(code: Option<&str>) -> bool {
    matches!(code, Some("0A000" | "08P01" | "42P05" | "26000"))
}

/// What the cache did, for tests. Not public: a production counter nothing reads is debt.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counts {
    pub(crate) prepare_attempts: usize,
    pub(crate) prepared: usize,
    pub(crate) evicted: usize,
    pub(crate) refused: usize,
}

pub(crate) struct StatementCache<S> {
    prepared: HashMap<String, S>,
    /// Texts used once, each with the order in which it was first used.
    seen: HashMap<String, u64>,
    next_order: u64,
    refused: bool,
    #[cfg(test)]
    counts: Counts,
}

impl<S> Default for StatementCache<S> {
    fn default() -> Self {
        Self {
            prepared: HashMap::new(),
            seen: HashMap::new(),
            next_order: 0,
            refused: false,
            #[cfg(test)]
            counts: Counts::default(),
        }
    }
}

impl<S: Clone> StatementCache<S> {
    /// What to do with `sql` now.
    pub(crate) fn step(&mut self, sql: &str) -> Step<S> {
        if self.refused {
            return Step::Unprepared;
        }
        if let Some(statement) = self.prepared.get(sql) {
            return Step::Use(statement.clone());
        }
        if self.seen.contains_key(sql) {
            if self.prepared.len() >= CAPACITY {
                return Step::Unprepared;
            }
            #[cfg(test)]
            {
                self.counts.prepare_attempts += 1;
            }
            return Step::Prepare;
        }
        if self.seen.len() >= CAPACITY {
            let newest = self
                .seen
                .iter()
                .max_by_key(|(_, order)| **order)
                .map(|(text, _)| text.clone());
            if let Some(text) = newest {
                self.seen.remove(&text);
            }
        }
        self.seen.insert(sql.to_owned(), self.next_order);
        self.next_order += 1;
        Step::Unprepared
    }

    /// `sql` was prepared as `statement`.
    pub(crate) fn prepared(&mut self, sql: &str, statement: S) {
        #[cfg(test)]
        {
            self.counts.prepared += 1;
        }
        self.seen.remove(sql);
        self.prepared.insert(sql.to_owned(), statement);
    }

    /// The server reported the statement of `sql` stale. The text starts again as first use.
    pub(crate) fn stale(&mut self, sql: &str) {
        #[cfg(test)]
        {
            self.counts.evicted += 1;
        }
        self.prepared.remove(sql);
        self.seen.remove(sql);
    }

    /// The session does not keep named statements. Final for this connection.
    pub(crate) fn refuse(&mut self) {
        #[cfg(test)]
        if !self.refused {
            self.counts.refused += 1;
        }
        self.refused = true;
        self.prepared.clear();
        self.seen.clear();
    }

    #[cfg(test)]
    pub(crate) fn counts(&self) -> Counts {
        self.counts
    }

    #[cfg(test)]
    pub(crate) fn is_refused(&self) -> bool {
        self.refused
    }

    #[cfg(test)]
    pub(crate) fn prepared_texts(&self) -> usize {
        self.prepared.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_use(step: &Step<u32>, expected: u32) -> bool {
        matches!(step, Step::Use(statement) if *statement == expected)
    }

    #[test]
    fn warm_up_then_prepare_then_use() {
        let mut cache = StatementCache::<u32>::default();
        assert!(matches!(cache.step("a"), Step::Unprepared));
        assert!(matches!(cache.step("a"), Step::Prepare));
        cache.prepared("a", 7);
        for _ in 0..3 {
            assert!(is_use(&cache.step("a"), 7));
        }
        assert_eq!(cache.counts().prepare_attempts, 1);
        assert_eq!(cache.counts().prepared, 1);
        assert_eq!(cache.prepared_texts(), 1);
    }

    fn texts(count: usize) -> Vec<String> {
        (0..count).map(|n| format!("text {n}")).collect()
    }

    #[test]
    fn the_65th_text_is_not_cached_and_nothing_is_evicted() {
        let mut cache = StatementCache::<u32>::default();
        let texts = texts(CAPACITY + 1);
        for (n, text) in texts.iter().take(CAPACITY).enumerate() {
            assert!(matches!(cache.step(text), Step::Unprepared));
            assert!(matches!(cache.step(text), Step::Prepare));
            cache.prepared(text, u32::try_from(n).unwrap());
        }
        let beyond = &texts[CAPACITY];
        for _ in 0..3 {
            assert!(matches!(cache.step(beyond), Step::Unprepared));
        }
        assert_eq!(cache.prepared_texts(), CAPACITY);
        assert_eq!(cache.counts().evicted, 0);
        for (n, text) in texts.iter().take(CAPACITY).enumerate() {
            assert!(is_use(&cache.step(text), u32::try_from(n).unwrap()));
        }
    }

    #[test]
    fn texts_seen_once_never_keep_a_hot_text_from_being_prepared() {
        let mut cache = StatementCache::<u32>::default();
        let texts = texts(CAPACITY + 1);
        for text in texts.iter().take(CAPACITY) {
            assert!(matches!(cache.step(text), Step::Unprepared));
        }
        let hot = &texts[CAPACITY];
        assert!(matches!(cache.step(hot), Step::Unprepared));
        assert!(matches!(cache.step(hot), Step::Prepare));
        cache.prepared(hot, 1);
        assert!(is_use(&cache.step(hot), 1));
        assert_eq!(cache.prepared_texts(), 1);
        assert!(cache.seen.len() <= CAPACITY);
    }

    #[test]
    fn the_older_texts_seen_once_keep_their_place_and_reach_their_second_use() {
        let mut cache = StatementCache::<u32>::default();
        let texts = texts(CAPACITY + 3);
        for text in &texts {
            assert!(matches!(cache.step(text), Step::Unprepared));
        }
        assert_eq!(cache.seen.len(), CAPACITY);
        for text in texts.iter().take(CAPACITY - 1) {
            assert!(matches!(cache.step(text), Step::Prepare), "{text}");
        }
    }

    #[test]
    fn a_text_whose_second_use_finds_64_prepared_is_never_prepared() {
        let mut cache = StatementCache::<u32>::default();
        let texts = texts(CAPACITY + 1);
        for (n, text) in texts.iter().take(CAPACITY).enumerate() {
            cache.step(text);
            assert!(matches!(cache.step(text), Step::Prepare));
            cache.prepared(text, u32::try_from(n).unwrap());
        }
        let beyond = &texts[CAPACITY];
        for _ in 0..4 {
            assert!(matches!(cache.step(beyond), Step::Unprepared));
        }
        assert_eq!(cache.counts().prepare_attempts, CAPACITY);
        assert_eq!(cache.prepared_texts(), CAPACITY);
    }

    #[test]
    fn texts_that_differ_only_in_their_tail_have_their_own_slots() {
        let mut cache = StatementCache::<u32>::default();
        let plain = "SELECT body FROM t WHERE a = $1";
        let checked = "SELECT body FROM t WHERE a = $1 AND b = $2";
        cache.step(plain);
        assert!(matches!(cache.step(plain), Step::Prepare));
        cache.prepared(plain, 1);
        assert!(matches!(cache.step(checked), Step::Unprepared));
        assert!(matches!(cache.step(checked), Step::Prepare));
        cache.prepared(checked, 2);
        assert!(is_use(&cache.step(plain), 1));
        assert!(is_use(&cache.step(checked), 2));
    }

    #[test]
    fn refused_is_final_for_the_connection() {
        let mut cache = StatementCache::<u32>::default();
        cache.step("a");
        assert!(matches!(cache.step("a"), Step::Prepare));
        cache.prepared("a", 1);
        cache.refuse();
        cache.refuse();
        for _ in 0..3 {
            assert!(matches!(cache.step("a"), Step::Unprepared));
            assert!(matches!(cache.step("b"), Step::Unprepared));
        }
        assert!(cache.is_refused());
        assert_eq!(cache.prepared_texts(), 0);
        assert_eq!(
            cache.counts(),
            Counts {
                prepare_attempts: 1,
                prepared: 1,
                evicted: 0,
                refused: 1,
            }
        );
    }

    #[test]
    fn a_stale_text_starts_again_as_first_use() {
        let mut cache = StatementCache::<u32>::default();
        cache.step("a");
        cache.step("a");
        cache.prepared("a", 1);
        cache.stale("a");
        assert_eq!(cache.counts().evicted, 1);
        assert!(!cache.is_refused());
        assert!(matches!(cache.step("a"), Step::Unprepared));
        assert!(matches!(cache.step("a"), Step::Prepare));
    }

    #[test]
    fn execution_errors_are_stale_refusing_or_returned() {
        for code in ["0A000", "42703", "42P01"] {
            assert_eq!(classify_execution(Some(code)), Execution::Stale, "{code}");
        }
        for code in ["26000", "08P01"] {
            assert_eq!(classify_execution(Some(code)), Execution::Refused, "{code}");
        }
        for code in ["57014", "55P03", "53300", "42P05", "40001", "25P02"] {
            assert_eq!(classify_execution(Some(code)), Execution::Other, "{code}");
        }
        assert_eq!(classify_execution(None), Execution::Other);
    }

    #[test]
    fn only_the_session_refusing_codes_refuse_a_prepare() {
        for code in ["0A000", "08P01", "42P05", "26000"] {
            assert!(refuses_prepare(Some(code)), "{code}");
        }
        for code in ["57014", "55P03", "53300", "42P01", "42703"] {
            assert!(!refuses_prepare(Some(code)), "{code}");
        }
        assert!(!refuses_prepare(None));
    }
}
