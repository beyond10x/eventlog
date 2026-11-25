//! `kpi/kpis.json`, the definitions, and `kpi/baseline.json`, the recorded numbers.
use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::{Failure, Measured, Outcome, environment, timing};

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
    /// With `only_new`, a counter that already has a recorded value keeps it, whatever this run
    /// measured, so a re-record adds the counters the baseline lacks and moves nothing else.
    fn merged_counters(
        definitions: &Definitions,
        counters: &Measured,
        previous: Option<&Self>,
        only_new: bool,
    ) -> BTreeMap<String, f64> {
        let mut recorded = BTreeMap::new();
        for kpi in definitions.of_kind(Kind::Counter) {
            let before = previous.and_then(|p| p.counters.get(&kpi.id)).copied();
            match (counters.values.get(&kpi.id).copied(), before) {
                (Some(_), Some(before)) if only_new => {
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
    /// while a timing recorded on one machine is never mixed with another's. A counter that is
    /// already recorded keeps its value (an improvement is ratcheted, and a regression absorbed,
    /// only by a decision of its own); a counter the baseline lacks takes the measurement. Only the
    /// revision the counters were measured on and the time move with them.
    ///
    /// `ratchet` names the counters whose recorded value takes this run's measurement: the
    /// decision of its own. Each must be a defined counter that is recorded and measured, and
    /// its measurement must not be worse than the recorded value in the counter's direction, so
    /// a ratchet never absorbs a regression.
    pub fn with_counters(
        root: &Path,
        definitions: &Definitions,
        counters: &Measured,
        previous: &Self,
        ratchet: &[String],
    ) -> Outcome<Self> {
        let mut recorded = Self::merged_counters(definitions, counters, Some(previous), true);
        for id in ratchet {
            let kpi = definitions
                .get(id)
                .filter(|kpi| kpi.kind == Kind::Counter)
                .ok_or_else(|| Failure(format!("--ratchet {id}: not a defined counter")))?;
            let now =
                counters.values.get(id).copied().ok_or_else(|| {
                    Failure(format!("--ratchet {id}: this run did not measure it"))
                })?;
            let before = previous.counters.get(id).copied().ok_or_else(|| {
                Failure(format!(
                    "--ratchet {id}: not recorded yet, so nothing to ratchet"
                ))
            })?;
            if crate::compare::worse_by(kpi.direction, before, now) > 0.0 {
                return Err(Failure(format!(
                    "--ratchet {id}: measured {now} is worse than the recorded {before}; a \
                     ratchet never absorbs a regression"
                )));
            }
            recorded.insert(id.clone(), now);
        }
        Ok(Self {
            format: BASELINE_FORMAT.to_owned(),
            revision: environment::revision(root),
            recorded_at: environment::now(),
            environment: previous.environment.clone(),
            counters: recorded,
            timings: previous.timings.clone(),
            timing_rounds: previous.timing_rounds,
        })
    }

    pub fn write(&self, path: &Path) -> Outcome<()> {
        let mut text = serde_json::to_string_pretty(self)?;
        text.push('\n');
        std::fs::write(path, text)?;
        Ok(())
    }

    /// Writes this baseline over the file at `path` by replacing only its revision, its
    /// recording time and its counters in the file's own text. Everything else, the timings
    /// above all, keeps the digits it was written with: a parse and a rewrite of a long decimal
    /// can land one unit in the last place off, and a record that adds counters has no reason to
    /// move a timing.
    pub fn write_counters_into(&self, path: &Path) -> Outcome<()> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| Failure(format!("{}: {error}", path.display())))?;
        std::fs::write(path, self.splice_counters(&text)?)?;
        Ok(())
    }

    fn splice_counters(&self, text: &str) -> Outcome<String> {
        let text = replace_string_field(text, "revision", &self.revision)?;
        let text = replace_string_field(&text, "recorded_at", &self.recorded_at)?;
        let opening = "\n  \"counters\": ";
        let start = text
            .find(opening)
            .ok_or_else(|| Failure("the baseline has no counters object".to_owned()))?
            + opening.len();
        let closing = ",\n  \"timings\"";
        let end = text[start..]
            .find(closing)
            .ok_or_else(|| Failure("the baseline has no timings after its counters".to_owned()))?
            + start;
        let block = serde_json::to_string_pretty(&self.counters)?.replace('\n', "\n  ");
        Ok(format!("{}{block}{}", &text[..start], &text[end..]))
    }
}

fn replace_string_field(text: &str, key: &str, value: &str) -> Outcome<String> {
    let opening = format!("\n  \"{key}\": \"");
    let start = text
        .find(&opening)
        .ok_or_else(|| Failure(format!("the baseline has no {key} line")))?
        + opening.len();
    let length = text[start..]
        .find('"')
        .ok_or_else(|| Failure(format!("the baseline's {key} line is not closed")))?;
    let quoted = serde_json::to_string(value)?;
    Ok(format!(
        "{}{quoted}{}",
        &text[..start - 1],
        &text[start + length + 1..]
    ))
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
    fn a_counters_only_write_changes_the_revision_the_time_and_the_counters_and_no_other_digit() {
        let before = r#"{
  "format": "eventlog-kpi-baseline/1",
  "revision": "old",
  "recorded_at": "then",
  "environment": { "cpu": "c" },
  "counters": {
    "x.settled": 100.0
  },
  "timings": {
    "x.latency": {
      "p50": 93.80199999999999,
      "p95": 989.6550000000001
    }
  },
  "timing_rounds": 3
}
"#;
        let mut next = previous();
        next.revision = "new".to_owned();
        next.recorded_at = "now".to_owned();
        next.counters =
            BTreeMap::from([("x.new".to_owned(), 8.0), ("x.settled".to_owned(), 100.0)]);
        let after = next.splice_counters(before).expect("a splice");
        let expected = before
            .replace("\"old\"", "\"new\"")
            .replace("\"then\"", "\"now\"")
            .replace(
                "\"x.settled\": 100.0\n  }",
                "\"x.new\": 8.0,\n    \"x.settled\": 100.0\n  }",
            );
        assert_eq!(after, expected);
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
    fn record_counters_adds_new_counters_and_moves_no_recorded_value_or_timing() {
        let mut measured = Measured::default();
        measured.put("x.settled", 101.0);
        measured.put("x.moved", 90.0);
        measured.put("x.new", 8.0);
        let next =
            Baseline::with_counters(Path::new("."), &definitions(), &measured, &previous(), &[])
                .expect("a record without a ratchet");
        assert!(
            (next.counters["x.settled"] - 100.0).abs() < f64::EPSILON,
            "a recorded counter keeps its value"
        );
        assert!(
            (next.counters["x.moved"] - 100.0).abs() < f64::EPSILON,
            "an improvement beyond tolerance is not ratcheted"
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

    #[test]
    fn a_ratchet_takes_the_measurement_of_the_named_counter_only_and_never_a_regression() {
        fn ratchet(measured: &Measured, ids: &[&str]) -> Outcome<Baseline> {
            let ids: Vec<String> = ids.iter().map(|id| (*id).to_owned()).collect();
            Baseline::with_counters(Path::new("."), &definitions(), measured, &previous(), &ids)
        }

        let mut measured = Measured::default();
        measured.put("x.settled", 40.0);
        measured.put("x.moved", 90.0);
        measured.put("x.new", 8.0);

        let next = ratchet(&measured, &["x.settled"]).expect("an improvement is ratcheted");
        assert!((next.counters["x.settled"] - 40.0).abs() < f64::EPSILON);
        assert!(
            (next.counters["x.moved"] - 100.0).abs() < f64::EPSILON,
            "a counter that is not named keeps its value"
        );

        measured.put("x.settled", 101.0);
        let refused = ratchet(&measured, &["x.settled"]).expect_err("a regression is not absorbed");
        assert!(
            refused.0.contains("never absorbs a regression"),
            "{}",
            refused.0
        );

        for (id, why) in [
            ("x.latency", "not a defined counter"),
            ("x.unmeasured", "did not measure"),
            ("x.new", "not recorded yet"),
            ("x.nowhere", "not a defined counter"),
        ] {
            let refused = ratchet(&measured, &[id]).expect_err(id);
            assert!(refused.0.contains(why), "{id}: {}", refused.0);
        }
    }
}
