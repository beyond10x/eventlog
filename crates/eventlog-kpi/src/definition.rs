//! `kpi/kpis.json`, the definitions, and `kpi/baseline.json`, the recorded numbers.
use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
    Failure, Measured, Outcome,
    compare::{Verdict, judge_counter},
    environment, timing,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Counter,
    Timing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Lower,
    Higher,
}

/// How far a counter may move before the gate calls it a change: the larger of the two bounds.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tolerance {
    pub pct: f64,
    pub abs: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Kpi {
    pub id: String,
    pub kind: Kind,
    /// The review finding whose fix this KPI shows, when there is one.
    pub finding: Option<String>,
    pub provider: String,
    pub measures: String,
    pub unit: String,
    pub direction: Direction,
    pub scenario: String,
    /// Counters only.
    pub tolerance: Option<Tolerance>,
    /// Timings only: the worst head/base ratio of medians `ab` accepts.
    pub ab_max_ratio: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AbSettings {
    pub rounds: usize,
    pub max_ratio: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Definitions {
    pub format: String,
    pub ab: AbSettings,
    pub kpis: Vec<Kpi>,
}

const DEFINITIONS_FORMAT: &str = "eventlog-kpis/1";
const BASELINE_FORMAT: &str = "eventlog-kpi-baseline/1";

impl Definitions {
    pub fn load(path: &Path) -> Outcome<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| Failure(format!("{}: {error}", path.display())))?;
        let definitions: Self = serde_json::from_str(&text)
            .map_err(|error| Failure(format!("{}: {error}", path.display())))?;
        definitions.validate()?;
        Ok(definitions)
    }

    fn validate(&self) -> Outcome<()> {
        if self.format != DEFINITIONS_FORMAT {
            return Err(Failure(format!(
                "definitions format {} is not {DEFINITIONS_FORMAT}",
                self.format
            )));
        }
        let mut seen = std::collections::BTreeSet::new();
        for kpi in &self.kpis {
            if !seen.insert(kpi.id.as_str()) {
                return Err(Failure(format!("KPI {} is defined twice", kpi.id)));
            }
            let shaped = match kpi.kind {
                Kind::Counter => kpi.tolerance.is_some() && kpi.ab_max_ratio.is_none(),
                Kind::Timing => kpi.tolerance.is_none(),
            };
            if !shaped {
                return Err(Failure(format!(
                    "KPI {}: a counter has a tolerance, a timing an optional ab_max_ratio",
                    kpi.id
                )));
            }
        }
        Ok(())
    }

    pub fn of_kind(&self, kind: Kind) -> impl Iterator<Item = &Kpi> {
        self.kpis.iter().filter(move |kpi| kpi.kind == kind)
    }

    pub fn get(&self, id: &str) -> Option<&Kpi> {
        self.kpis.iter().find(|kpi| kpi.id == id)
    }

    /// Refuse a measurement nobody defined: a number without a definition cannot be gated.
    pub fn require_known<'a>(
        &self,
        ids: impl Iterator<Item = &'a String>,
        kind: Kind,
    ) -> Outcome<()> {
        let unknown: Vec<_> = ids
            .filter(|id| self.get(id).is_none_or(|kpi| kpi.kind != kind))
            .cloned()
            .collect();
        if unknown.is_empty() {
            Ok(())
        } else {
            Err(Failure(format!(
                "measured but not defined as {kind:?} in kpi/kpis.json: {}",
                unknown.join(", ")
            )))
        }
    }

    pub fn ab_ratio(&self, kpi: &Kpi) -> f64 {
        kpi.ab_max_ratio.unwrap_or(self.ab.max_ratio)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Baseline {
    pub format: String,
    /// The commit the numbers were measured on, and whether its tree had local changes.
    pub revision: String,
    pub recorded_at: String,
    pub environment: environment::Environment,
    pub counters: BTreeMap<String, f64>,
    /// Local numbers for the report only; `ab` never compares against them.
    pub timings: BTreeMap<String, timing::Summary>,
    pub timing_rounds: usize,
}

impl Baseline {
    pub fn load(path: &Path) -> Outcome<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| Failure(format!("{}: {error}", path.display())))?;
        let baseline: Self = serde_json::from_str(&text)
            .map_err(|error| Failure(format!("{}: {error}", path.display())))?;
        if baseline.format != BASELINE_FORMAT {
            return Err(Failure(format!(
                "baseline format {} is not {BASELINE_FORMAT}",
                baseline.format
            )));
        }
        Ok(baseline)
    }

    /// The new baseline. A KPI this run skipped (PostgreSQL without a URL) keeps its previous
    /// value rather than vanishing from the ratchet.
    pub fn new(
        root: &Path,
        definitions: &Definitions,
        counters: &Measured,
        timings: &timing::Rounds,
        rounds: usize,
        previous: Option<&Self>,
    ) -> Self {
        let recorded_counters = Self::merged_counters(definitions, counters, previous, false);
        let mut recorded_timings = timings.summary();
        for kpi in definitions.of_kind(Kind::Timing) {
            if !recorded_timings.contains_key(&kpi.id)
                && let Some(value) = previous.and_then(|p| p.timings.get(&kpi.id))
            {
                recorded_timings.insert(kpi.id.clone(), *value);
            }
        }
        Self {
            format: BASELINE_FORMAT.to_owned(),
            revision: environment::revision(root),
            recorded_at: environment::now(),
            environment: environment::Environment::observe(
                counters.values.keys().any(|id| id.starts_with("postgres.")),
            ),
            counters: recorded_counters,
            timings: recorded_timings,
            timing_rounds: rounds,
        }
    }

    /// The counters to write. A counter this run did not measure keeps its previous value.
    /// With `settled`, a counter whose previous value still judges `Ok` against the measurement
    /// keeps that value too, so a re-record moves only what moved beyond its tolerance.
    fn merged_counters(
        definitions: &Definitions,
        counters: &Measured,
        previous: Option<&Self>,
        settled: bool,
    ) -> BTreeMap<String, f64> {
        let mut recorded = BTreeMap::new();
        for kpi in definitions.of_kind(Kind::Counter) {
            let before = previous.and_then(|p| p.counters.get(&kpi.id)).copied();
            match (counters.values.get(&kpi.id).copied(), before) {
                (Some(now), Some(before))
                    if settled
                        && judge_counter(
                            kpi.direction,
                            kpi.tolerance.unwrap_or(Tolerance { pct: 0.0, abs: 0.0 }),
                            before,
                            now,
                        ) == Verdict::Ok =>
                {
                    recorded.insert(kpi.id.clone(), before);
                }
                (Some(now), _) => {
                    recorded.insert(kpi.id.clone(), now);
                }
                (None, Some(before)) => {
                    println!("eventlog-kpi: {} kept from the previous baseline", kpi.id);
                    recorded.insert(kpi.id.clone(), before);
                }
                (None, None) => {}
            }
        }
        recorded
    }

    /// The previous baseline with its counters re-measured and nothing else rewritten: the
    /// timings, their round count and the environment they were recorded in stay as they were.
    /// Counters are deterministic work counts, so the machine that measured them does not matter,
    /// while a timing recorded on one machine is never mixed with another's. A counter within
    /// its tolerance of the new measurement keeps its recorded value; a new one, or one that moved
    /// beyond its tolerance, takes the measurement. Only the revision the counters were measured
    /// on and the time move with them.
    pub fn with_counters(
        root: &Path,
        definitions: &Definitions,
        counters: &Measured,
        previous: &Self,
    ) -> Self {
        Self {
            format: BASELINE_FORMAT.to_owned(),
            revision: environment::revision(root),
            recorded_at: environment::now(),
            environment: previous.environment.clone(),
            counters: Self::merged_counters(definitions, counters, Some(previous), true),
            timings: previous.timings.clone(),
            timing_rounds: previous.timing_rounds,
        }
    }

    pub fn write(&self, path: &Path) -> Outcome<()> {
        let mut text = serde_json::to_string_pretty(self)?;
        text.push('\n');
        std::fs::write(path, text)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counter(id: &str, abs: f64) -> String {
        format!(
            r#"{{ "id": "{id}", "kind": "counter", "finding": null, "provider": "x", "unit": "u",
               "direction": "lower", "measures": "m", "scenario": "s",
               "tolerance": {{ "pct": 0.0, "abs": {abs} }} }}"#
        )
    }

    fn definitions() -> Definitions {
        let kpis = [
            counter("x.settled", 2.0),
            counter("x.moved", 2.0),
            counter("x.new", 0.0),
            counter("x.unmeasured", 0.0),
            r#"{ "id": "x.latency", "kind": "timing", "finding": null, "provider": "x",
                 "unit": "us", "direction": "lower", "measures": "m", "scenario": "s",
                 "ab_max_ratio": null }"#
                .to_owned(),
        ]
        .join(",");
        let text = format!(
            r#"{{ "format": "eventlog-kpis/1", "ab": {{ "rounds": 5, "max_ratio": 1.5 }},
                 "kpis": [{kpis}] }}"#
        );
        serde_json::from_str(&text).expect("test definitions")
    }

    fn previous() -> Baseline {
        serde_json::from_str(
            r#"{ "format": "eventlog-kpi-baseline/1", "revision": "old", "recorded_at": "then",
                 "environment": { "cpu": "a recording machine", "cpus": 20, "kernel": "k",
                                  "load_average": "1 1 1", "rustc": "r", "profile": "release",
                                  "postgres": true },
                 "counters": { "x.settled": 100.0, "x.moved": 100.0, "x.unmeasured": 7.0 },
                 "timings": { "x.latency": { "p50": 11.0, "p95": 22.0 } },
                 "timing_rounds": 3 }"#,
        )
        .expect("test baseline")
    }

    #[test]
    fn the_definitions_file_holds_the_projection_find_measurement_kit() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let definitions = Definitions::load(&root.join("kpi/kpis.json")).expect("kpi/kpis.json");
        for (id, kind) in [
            ("postgres.projection_find_skewed.latency", Kind::Timing),
            ("postgres.atomic_group_large.latency", Kind::Timing),
            ("postgres.projection_find_skewed.tuples_read", Kind::Counter),
            ("postgres.projection_find.round_trips", Kind::Counter),
            ("postgres.projection_find.parses", Kind::Counter),
        ] {
            assert_eq!(definitions.get(id).map(|kpi| kpi.kind), Some(kind), "{id}");
        }
    }

    #[test]
    fn record_counters_adds_and_moves_counters_and_rewrites_no_timing() {
        let mut measured = Measured::default();
        measured.put("x.settled", 101.0);
        measured.put("x.moved", 90.0);
        measured.put("x.new", 8.0);
        let next = Baseline::with_counters(Path::new("."), &definitions(), &measured, &previous());
        assert!(
            (next.counters["x.settled"] - 100.0).abs() < f64::EPSILON,
            "within tolerance"
        );
        assert!(
            (next.counters["x.moved"] - 90.0).abs() < f64::EPSILON,
            "beyond tolerance"
        );
        assert!(
            (next.counters["x.new"] - 8.0).abs() < f64::EPSILON,
            "a new counter"
        );
        assert!(
            (next.counters["x.unmeasured"] - 7.0).abs() < f64::EPSILON,
            "a counter this run did not measure"
        );
        assert!((next.timings["x.latency"].p50 - 11.0).abs() < f64::EPSILON);
        assert!((next.timings["x.latency"].p95 - 22.0).abs() < f64::EPSILON);
        assert_eq!(next.timing_rounds, 3);
        assert_eq!(next.environment.cpu, "a recording machine");
    }
}
