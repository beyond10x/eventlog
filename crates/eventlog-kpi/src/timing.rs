//! The timings: latency and throughput of the common paths, per provider. Small fixtures and
//! few samples; this is a measurement lane, not a load test.
use std::{collections::BTreeMap, path::Path, time::Instant};

use serde::{Deserialize, Serialize};

use crate::{
    Outcome, ensure,
    fixture::{self, Place, Provider, ROWS},
    large_group, skewed,
};

/// The p50 and p95 of one KPI's samples (microseconds, or events per second for throughput).
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Summary {
    pub p50: f64,
    pub p95: f64,
}

/// Several runs of every timing, summarized by the median of each run's summary.
#[derive(Default)]
pub struct Rounds {
    runs: BTreeMap<String, Vec<Summary>>,
    pub skipped: BTreeMap<String, String>,
}

impl Rounds {
    pub fn add(&mut self, round: Round) {
        for (id, summary) in round.values {
            self.runs.entry(id).or_default().push(summary);
        }
        self.skipped.extend(round.skipped);
    }

    pub fn ids(&self) -> impl Iterator<Item = &String> {
        self.runs.keys()
    }

    pub fn summary(&self) -> BTreeMap<String, Summary> {
        self.runs
            .iter()
            .map(|(id, runs)| {
                let p50: Vec<f64> = runs.iter().map(|summary| summary.p50).collect();
                let p95: Vec<f64> = runs.iter().map(|summary| summary.p95).collect();
                (
                    id.clone(),
                    Summary {
                        p50: median(&p50),
                        p95: median(&p95),
                    },
                )
            })
            .collect()
    }
}

#[derive(Default)]
pub struct Round {
    values: BTreeMap<String, Summary>,
    skipped: BTreeMap<String, String>,
}

pub fn median(values: &[f64]) -> f64 {
    percentile(values, 50)
}

/// Nearest-rank percentile; 0 for no values.
pub fn percentile(values: &[f64], rank: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = (rank * sorted.len()).div_ceil(100).clamp(1, sorted.len()) - 1;
    sorted[index]
}

fn micros(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e6
}

impl Round {
    fn latency(&mut self, provider: Provider, name: &str, samples: &[f64]) {
        self.values.insert(
            format!("{}.{name}.latency", provider.name()),
            Summary {
                p50: median(samples),
                p95: percentile(samples, 95),
            },
        );
    }

    fn throughput(&mut self, provider: Provider, name: &str, samples: &[f64]) {
        let value = median(samples);
        self.values.insert(
            format!("{}.{name}.throughput", provider.name()),
            Summary {
                p50: value,
                p95: value,
            },
        );
    }
}

/// One run of every timing scenario on every provider available.
pub async fn measure(scratch: &Path, url: Option<&str>) -> Outcome<Round> {
    let mut round = Round::default();
    for provider in Provider::ALL {
        if provider == Provider::Postgres && url.is_none() {
            round.skipped.insert(
                "postgres.".into(),
                "EVENTLOG_TEST_POSTGRES_URL is not set; PostgreSQL timings were not measured"
                    .into(),
            );
            continue;
        }
        provider_round(provider, scratch, url, &mut round).await?;
    }
    Ok(round)
}

async fn provider_round(
    provider: Provider,
    scratch: &Path,
    url: Option<&str>,
    round: &mut Round,
) -> Outcome<()> {
    let tenant = fixture::tenant()?;

    // Writes: single events, guarded groups with blobs, batches of 32.
    let place = Place::new(provider, scratch, url)?;
    let store = place.open().await?;
    let mut samples = Vec::new();
    for index in 0..32 {
        let stream = fixture::stream(&tenant, "single", &index.to_string())?;
        let start = Instant::now();
        fixture::append_one(store.events(), &stream, &format!("single-{index}")).await?;
        samples.push(micros(start));
    }
    round.latency(provider, "append_single", &samples);

    let mut samples = Vec::new();
    for index in 0..16 {
        let (group, blobs) = fixture::blob_group(&tenant, &format!("group-{index}"), 2, 2, 1024)?;
        let start = Instant::now();
        fixture::guarded_blob_group(store.atomic(), &group, &blobs).await?;
        samples.push(micros(start));
    }
    round.latency(provider, "group_blobs", &samples);

    let mut samples = Vec::new();
    for index in 0..4 {
        let stream = fixture::stream(&tenant, "batch", &index.to_string())?;
        let start = Instant::now();
        fixture::seed(store.events(), &stream, 32, &format!("batch-{index}")).await?;
        samples.push(32.0 / start.elapsed().as_secs_f64());
    }
    round.throughput(provider, "batch_append", &samples);
    store.discard().await;

    // Reads and reopen over a 128-event stream.
    let place = Place::new(provider, scratch, url)?;
    let store = place.open().await?;
    let stream = fixture::stream(&tenant, "item", "first")?;
    fixture::seed(store.events(), &stream, 128, "seed").await?;
    let mut samples = Vec::new();
    for _ in 0..32 {
        let start = Instant::now();
        let slice = store.events().read_stream(&stream, 0, 1000).await?;
        samples.push(micros(start));
        ensure(slice.events.len() == 128, "read_stream returned the stream")?;
    }
    round.latency(provider, "read_stream", &samples);

    let mut samples = Vec::new();
    for _ in 0..2 {
        let mut cursor = 0;
        loop {
            let start = Instant::now();
            let page = store.events().read_feed(&tenant, cursor, 8).await?;
            samples.push(micros(start));
            cursor = page.next_position;
            if !page.has_more {
                break;
            }
        }
    }
    round.latency(provider, "read_feed_page", &samples);
    store.close().await;

    let mut samples = Vec::new();
    for _ in 0..8 {
        let start = Instant::now();
        let store = place.open().await?;
        samples.push(micros(start));
        store.close().await;
    }
    round.latency(provider, "open_128", &samples);
    place.open().await?.discard().await;

    // Indexed reads over 1,001 projected rows.
    let place = Place::new(provider, scratch, url)?;
    let store = fixture::projected(&place).await?;
    let mut samples = Vec::new();
    for _ in 0..32 {
        let start = Instant::now();
        let found = store
            .events()
            .projection_find(&ROWS, &tenant, "kind", "item", 10)
            .await?;
        samples.push(micros(start));
        ensure(found.len() == 10, "projection_find returned the ten items")?;
    }
    round.latency(provider, "projection_find", &samples);
    store.discard().await;

    // Skewed tenants and a large atomic group (PostgreSQL only; a seeded table and 6,500 streams
    // are too much to repeat for every provider, and the plan question is PostgreSQL's).
    if let (Provider::Postgres, Some(url)) = (provider, url) {
        let samples = skewed::time_finds(scratch, url).await?;
        round.latency(provider, "projection_find_skewed", &samples);
        let samples = large_group::time_groups(scratch, url).await?;
        round.latency(provider, "atomic_group_large", &samples);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{median, percentile};

    #[test]
    fn nearest_rank_percentiles_pick_a_sample() {
        let samples = [5.0, 1.0, 4.0, 2.0, 3.0];
        assert!((median(&samples) - 3.0).abs() < f64::EPSILON);
        assert!((percentile(&samples, 95) - 5.0).abs() < f64::EPSILON);
        assert!((percentile(&samples, 1) - 1.0).abs() < f64::EPSILON);
        assert!(median(&[]).abs() < f64::EPSILON);
    }
}
