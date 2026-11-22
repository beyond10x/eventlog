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
        let mut recorded_counters = BTreeMap::new();
        for kpi in definitions.of_kind(Kind::Counter) {
            if let Some(value) = counters.values.get(&kpi.id) {
                recorded_counters.insert(kpi.id.clone(), *value);
            } else if let Some(value) = previous.and_then(|p| p.counters.get(&kpi.id)) {
                println!("eventlog-kpi: {} kept from the previous baseline", kpi.id);
                recorded_counters.insert(kpi.id.clone(), *value);
            }
        }
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

    pub fn write(&self, path: &Path) -> Outcome<()> {
        let mut text = serde_json::to_string_pretty(self)?;
        text.push('\n');
        std::fs::write(path, text)?;
        Ok(())
    }
}
