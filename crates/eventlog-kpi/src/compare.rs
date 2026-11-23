//! Verdicts: a counter against its baseline, a head timing against its base, and the tables.
use crate::{
    Measured,
    definition::{Baseline, Definitions, Direction, Kind, Kpi, Tolerance},
};

#[derive(Clone, Debug, PartialEq)]
pub enum Verdict {
    Ok,
    Improved,
    Regressed,
    Skipped(String),
    /// Defined, measured, but absent from the baseline: record it explicitly.
    Unrecorded,
    /// Defined, not skipped, and not measured: the runner lost a scenario.
    Missing,
}

impl Verdict {
    pub fn fails(&self) -> bool {
        matches!(self, Self::Regressed | Self::Unrecorded | Self::Missing)
    }

    fn label(&self) -> String {
        match self {
            Self::Ok => "ok".into(),
            Self::Improved => "IMPROVED (re-record to ratchet)".into(),
            Self::Regressed => "REGRESSED".into(),
            Self::Skipped(reason) => format!("skipped: {reason}"),
            Self::Unrecorded => "UNRECORDED (run record)".into(),
            Self::Missing => "MISSING (scenario did not run)".into(),
        }
    }
}

/// How much worse `now` is than `baseline`, signed so that positive is worse.
fn worse_by(direction: Direction, baseline: f64, now: f64) -> f64 {
    match direction {
        Direction::Lower => now - baseline,
        Direction::Higher => baseline - now,
    }
}

pub fn judge_counter(
    direction: Direction,
    tolerance: Tolerance,
    baseline: f64,
    now: f64,
) -> Verdict {
    let allowed = tolerance.abs.max(baseline.abs() * tolerance.pct / 100.0);
    let worse = worse_by(direction, baseline, now);
    if worse > allowed {
        Verdict::Regressed
    } else if -worse > allowed {
        Verdict::Improved
    } else {
        Verdict::Ok
    }
}

/// The head/base ratio oriented so that above 1 is worse, and its verdict against `max_ratio`.
pub fn judge_ratio(direction: Direction, base: f64, head: f64, max_ratio: f64) -> (f64, Verdict) {
    let ratio = match direction {
        Direction::Lower => head / base,
        Direction::Higher => base / head,
    };
    let verdict = if !ratio.is_finite() || ratio > max_ratio {
        Verdict::Regressed
    } else if ratio < 1.0 / max_ratio {
        Verdict::Improved
    } else {
        Verdict::Ok
    };
    (ratio, verdict)
}

pub struct Row {
    pub id: String,
    pub finding: String,
    pub baseline: Option<f64>,
    pub now: Option<f64>,
    pub verdict: Verdict,
}

pub struct Report {
    pub rows: Vec<Row>,
}

impl Report {
    pub fn passed(&self) -> bool {
        !self.rows.iter().any(|row| row.verdict.fails())
    }

    pub fn print(&self) {
        println!(
            "{:<44} {:>6} {:>14} {:>14} {:>14} {:>8}  verdict",
            "KPI", "review", "baseline", "now", "delta", "delta%"
        );
        for row in &self.rows {
            let (delta, percent) = match (row.baseline, row.now) {
                (Some(baseline), Some(now)) => (
                    format!("{:+.0}", now - baseline),
                    if baseline == 0.0 {
                        "-".to_owned()
                    } else {
                        format!("{:+.1}", (now - baseline) / baseline * 100.0)
                    },
                ),
                _ => ("-".to_owned(), "-".to_owned()),
            };
            println!(
                "{:<44} {:>6} {:>14} {:>14} {:>14} {:>8}  {}",
                row.id,
                row.finding,
                number(row.baseline),
                number(row.now),
                delta,
                percent,
                row.verdict.label()
            );
        }
        let count = |wanted: fn(&Verdict) -> bool| {
            self.rows.iter().filter(|row| wanted(&row.verdict)).count()
        };
        println!(
            "eventlog-kpi: {} ok, {} improved, {} regressed, {} skipped, {} unrecorded or missing",
            count(|verdict| *verdict == Verdict::Ok),
            count(|verdict| *verdict == Verdict::Improved),
            count(|verdict| *verdict == Verdict::Regressed),
            count(|verdict| matches!(verdict, Verdict::Skipped(_))),
            count(|verdict| matches!(verdict, Verdict::Unrecorded | Verdict::Missing)),
        );
        if self.rows.iter().any(|row| row.verdict == Verdict::Improved) {
            println!(
                "eventlog-kpi: improvements stay unratcheted until `record` writes them in the same change"
            );
        }
    }
}

fn number(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{value:.0}"))
}

fn finding(kpi: &Kpi) -> String {
    kpi.finding.clone().unwrap_or_else(|| "-".to_owned())
}

pub fn check(definitions: &Definitions, baseline: &Baseline, measured: &Measured) -> Report {
    let rows = definitions
        .of_kind(Kind::Counter)
        .map(|kpi| {
            let recorded = baseline.counters.get(&kpi.id).copied();
            let now = measured.values.get(&kpi.id).copied();
            let verdict = match (recorded, now) {
                (_, None) => measured
                    .skip_reason(&kpi.id)
                    .map_or(Verdict::Missing, |reason| {
                        Verdict::Skipped(reason.to_owned())
                    }),
                (None, Some(_)) => Verdict::Unrecorded,
                (Some(recorded), Some(now)) => judge_counter(
                    kpi.direction,
                    kpi.tolerance.unwrap_or(Tolerance { pct: 0.0, abs: 0.0 }),
                    recorded,
                    now,
                ),
            };
            Row {
                id: kpi.id.clone(),
                finding: finding(kpi),
                baseline: recorded,
                now,
                verdict,
            }
        })
        .collect();
    Report { rows }
}

pub fn print_record(definitions: &Definitions, baseline: &Baseline, measured: &Measured) {
    for kpi in definitions.of_kind(Kind::Counter) {
        let value = baseline.counters.get(&kpi.id);
        let note = measured
            .skip_reason(&kpi.id)
            .map_or(String::new(), |reason| format!("  ({reason})"));
        println!(
            "{:<44} {:>6} {:>14} {}{note}",
            kpi.id,
            finding(kpi),
            number(value.copied()),
            kpi.unit
        );
    }
    for kpi in definitions.of_kind(Kind::Timing) {
        if let Some(value) = baseline.timings.get(&kpi.id) {
            println!(
                "{:<44} {:>6} p50 {:>10.1} p95 {:>10.1} {}",
                kpi.id,
                finding(kpi),
                value.p50,
                value.p95,
                kpi.unit
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIGHT: Tolerance = Tolerance { pct: 2.0, abs: 1.0 };

    #[test]
    fn counter_within_tolerance_is_ok_and_beyond_it_regresses_or_improves() {
        assert_eq!(
            judge_counter(Direction::Lower, TIGHT, 1000.0, 1019.0),
            Verdict::Ok
        );
        assert_eq!(
            judge_counter(Direction::Lower, TIGHT, 1000.0, 1021.0),
            Verdict::Regressed
        );
        assert_eq!(
            judge_counter(Direction::Lower, TIGHT, 1000.0, 979.0),
            Verdict::Improved
        );
        assert_eq!(
            judge_counter(Direction::Lower, TIGHT, 0.0, 1.0),
            Verdict::Ok,
            "abs floor"
        );
        assert_eq!(
            judge_counter(Direction::Lower, TIGHT, 0.0, 2.0),
            Verdict::Regressed
        );
    }

    #[test]
    fn higher_is_better_counters_regress_downward() {
        let exact = Tolerance { pct: 0.0, abs: 0.0 };
        assert_eq!(
            judge_counter(Direction::Higher, exact, 1.0, 0.0),
            Verdict::Regressed
        );
        assert_eq!(
            judge_counter(Direction::Higher, exact, 0.0, 1.0),
            Verdict::Improved
        );
        assert_eq!(
            judge_counter(Direction::Higher, exact, 1.0, 1.0),
            Verdict::Ok
        );
    }

    #[test]
    fn timing_ratio_is_oriented_so_above_one_is_worse() {
        let (ratio, verdict) = judge_ratio(Direction::Lower, 100.0, 140.0, 1.3);
        assert!((ratio - 1.4).abs() < 1e-9);
        assert_eq!(verdict, Verdict::Regressed);
        let (ratio, verdict) = judge_ratio(Direction::Higher, 1000.0, 800.0, 1.3);
        assert!((ratio - 1.25).abs() < 1e-9);
        assert_eq!(verdict, Verdict::Ok);
        assert_eq!(
            judge_ratio(Direction::Lower, 100.0, 70.0, 1.3).1,
            Verdict::Improved
        );
        assert_eq!(
            judge_ratio(Direction::Lower, 0.0, 70.0, 1.3).1,
            Verdict::Regressed
        );
    }

    #[test]
    fn only_regressions_unrecorded_and_missing_fail_the_gate() {
        assert!(Verdict::Regressed.fails());
        assert!(Verdict::Unrecorded.fails());
        assert!(Verdict::Missing.fails());
        assert!(!Verdict::Improved.fails());
        assert!(!Verdict::Skipped("no PostgreSQL".into()).fails());
        assert!(!Verdict::Ok.fails());
    }
}
