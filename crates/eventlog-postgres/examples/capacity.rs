//! One immutable workload shared by original and candidate adapters; orchestration supplies scope.
use clap::Parser;
use eventlog_core::{
    BoxFuture, CatchUpRunner, EventLogError, EventStore, Expected, Guard, ProjectionSpec,
    ProjectionStore, Projector, RecordedEvent, StreamId, TenantId,
};
use eventlog_postgres::PostgresEventStore;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
const HOT: ProjectionSpec = ProjectionSpec {
    name: "capacity_hot",
    indexed: &[],
};
struct HotCounter;
impl Projector for HotCounter {
    fn name(&self) -> &'static str {
        "capacity_hot"
    }
    fn projections(&self) -> &'static [ProjectionSpec] {
        std::slice::from_ref(&HOT)
    }
    fn apply<'a>(
        &'a self,
        event: &'a RecordedEvent,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let held = store
                .get(&HOT, &event.tenant, "shared")
                .await?
                .and_then(|value| value["held"].as_u64())
                .unwrap_or(0);
            store
                .upsert(&HOT, &event.tenant, "shared", &json!({"held":held+1}))
                .await
        })
    }
}
struct HotGuard {
    lock_read_us: AtomicU64,
    calls: AtomicU64,
}
impl Guard for HotGuard {
    fn check<'a>(
        &'a self,
        store: &'a mut dyn ProjectionStore,
    ) -> BoxFuture<'a, Result<(), EventLogError>> {
        Box::pin(async move {
            let started = Instant::now();
            let row = store
                .get_for_update(&HOT, &TenantId::new("capacity-tenant")?, "shared")
                .await?;
            self.lock_read_us
                .fetch_add(micros(started.elapsed()), Ordering::Relaxed);
            self.calls.fetch_add(1, Ordering::Relaxed);
            if row
                .and_then(|value| value["held"].as_u64())
                .is_none_or(|held| held >= 1_000_000)
            {
                return Err(EventLogError::Invalid(
                    "preinitialized laboratory admission ceiling".into(),
                ));
            }
            Ok(())
        })
    }
}
fn micros(value: Duration) -> u64 {
    u64::try_from(value.as_micros()).expect("bounded duration")
}
#[derive(Parser)]
struct Args {
    prefix: String,
    worker: usize,
    concurrency: usize,
    samples: usize,
    #[arg(value_parser=["uniform","hot"])]
    mode: String,
}
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL").expect("required database");
    let prefix = &args.prefix;
    let worker = args.worker;
    let concurrency = args.concurrency;
    let samples = args.samples;
    let hot = args.mode == "hot";
    let store = Arc::new(
        PostgresEventStore::connect(&url, prefix)
            .await
            .expect("store"),
    );
    if hot {
        store
            .register_inline(Arc::new(HotCounter))
            .await
            .expect("hot projection");
    }
    let erased: Arc<dyn EventStore> = store.clone();
    let projector: Arc<dyn Projector> = Arc::new(eventlog_conformance::Tally);
    let runner = Arc::new(
        CatchUpRunner::new(erased, projector)
            .await
            .expect("catch-up"),
    );
    let tenant = TenantId::new("capacity-tenant").expect("tenant");
    if samples == 0 {
        if hot {
            let stream = StreamId::new(tenant.clone(), "capacity", "seed").expect("stream");
            store
                .append(
                    &stream,
                    Expected::NoStream,
                    &[eventlog_conformance::event("capacity.seeded", 0)],
                    &eventlog_conformance::meta("seed", &json!({"seed":73491})),
                )
                .await
                .expect("seeded shared admission row");
        }
        println!("{}", json!({"initialized":true,"prefix":prefix}));
        return;
    }
    let hot_guard = Arc::new(HotGuard {
        lock_read_us: AtomicU64::new(0),
        calls: AtomicU64::new(0),
    });
    // Warm the same process, pool and dataset before the timed steady interval.
    for n in 0..16 {
        let stream =
            StreamId::new(tenant.clone(), "capacity", format!("warmup-w{worker}")).expect("stream");
        let event = eventlog_conformance::event("capacity.warmup", n);
        let meta = eventlog_conformance::meta(
            &format!("warmup-w{worker}-{n}"),
            &json!({"seed":73491,"n":n}),
        );
        if hot {
            store
                .append_guarded(&stream, Expected::Any, &[event], &meta, hot_guard.clone())
                .await
                .expect("warm-up");
        } else {
            store
                .append(&stream, Expected::Any, &[event], &meta)
                .await
                .expect("warm-up");
        }
        store
            .read_stream(&stream, 0, 16)
            .await
            .expect("warm-up read");
    }
    hot_guard.lock_read_us.store(0, Ordering::Relaxed);
    hot_guard.calls.store(0, Ordering::Relaxed);
    let steady_started_at = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    let start = Instant::now();
    let mut tasks = Vec::new();
    for lane in 0..concurrency {
        let store = Arc::clone(&store);
        let hot_guard = Arc::clone(&hot_guard);
        tasks.push(tokio::spawn(async move {
            let tenant = TenantId::new("capacity-tenant").expect("tenant");
            let mut latencies = Vec::new();
            let mut append_latencies = Vec::new();
            let mut succeeded = 0;
            let mut conflicts = 0;
            let mut refused = 0;
            let mut deduplicated = 0;
            let mut violations = 0;
            let mut last_position = 0;
            for sample in 0..samples {
                // Streams remain independent in the hot case; only the admission counter is shared.
                let stream = StreamId::new(
                    tenant.clone(),
                    "capacity",
                    format!("w{worker}_l{lane}_s{}", sample % 64),
                )
                .expect("stream");
                let key = format!("w{worker}_l{lane}_n{sample}");
                let payload = json!({"seed":73491,"value":sample,"padding":"x".repeat(128)});
                let event = eventlog_core::NewEvent::new("capacity.appended", 1, payload.clone())
                    .expect("event");
                let meta = eventlog_conformance::meta(&key, &payload);
                let began = Instant::now();
                let result = if hot {
                    store
                        .append_guarded(&stream, Expected::Any, &[event], &meta, hot_guard.clone())
                        .await
                } else {
                    store.append(&stream, Expected::Any, &[event], &meta).await
                };
                append_latencies.push(micros(began.elapsed()));
                match result {
                    Ok(result) => {
                        succeeded += 1;
                        deduplicated += usize::from(result.deduplicated);
                        last_position =
                            last_position.max(result.events.last().expect("event").global_seq);
                        match store.read_stream(&stream, 0, 16).await {
                            Ok(read) => {
                                if !read
                                    .events
                                    .iter()
                                    .any(|event| event.event_id == result.events[0].event_id)
                                {
                                    violations += 1;
                                }
                            }
                            Err(_) => violations += 1,
                        }
                    }
                    Err(EventLogError::Conflict { .. }) => conflicts += 1,
                    Err(_) => refused += 1,
                }
                latencies.push(micros(began.elapsed()));
            }
            (
                succeeded,
                conflicts,
                refused,
                deduplicated,
                violations,
                last_position,
                latencies,
                append_latencies,
            )
        }));
    }
    let mut succeeded = 0;
    let mut conflicts = 0;
    let mut refused = 0;
    let mut deduplicated = 0;
    let mut violations = 0;
    let mut last_position = 0;
    let mut latencies = Vec::new();
    let mut append_latencies = Vec::new();
    for task in tasks {
        let (
            ok_count,
            conflict_count,
            refusal_count,
            dedup_count,
            violation_count,
            position,
            sample_latencies,
            sample_appends,
        ) = task.await.expect("worker");
        succeeded += ok_count;
        conflicts += conflict_count;
        refused += refusal_count;
        deduplicated += dedup_count;
        violations += violation_count;
        last_position = last_position.max(position);
        latencies.extend(sample_latencies);
        append_latencies.extend(sample_appends);
    }
    let workload_elapsed_us = micros(start.elapsed());
    let tail_started = Instant::now();
    let mut cursor = 0;
    while cursor < last_position && tail_started.elapsed() < Duration::from_secs(10) {
        let page = store.read_feed(&tenant, cursor, 1000).await.expect("feed");
        cursor = page.next_position;
        if page.events.is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    let feed_lag_us = micros(tail_started.elapsed());
    if cursor < last_position {
        violations += 1;
    }
    let projection_started = Instant::now();
    let mut projection_position = 0;
    while projection_position < last_position
        && projection_started.elapsed() < Duration::from_secs(10)
    {
        let progress = runner.run_once(&tenant).await.expect("projector");
        projection_position = projection_position.max(progress.position);
        if progress.applied == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    let projection_lag_us = micros(projection_started.elapsed());
    if projection_position < last_position {
        violations += 1;
    }
    latencies.sort_unstable();
    append_latencies.sort_unstable();
    let quantile = |values: &[u64], n: usize| values[(values.len() - 1) * n / 100];
    println!(
        "{}",
        json!({"worker":worker,"concurrency":concurrency,"samples":latencies.len(),"mode":if hot{"hot"}else{"uniform"},"steady_started_unix_ns":steady_started_at,"elapsed_us":workload_elapsed_us,"succeeded":succeeded,"conflicts":conflicts,"refused":refused,"deduplicated":deduplicated,"correctness_violations":violations,"last_position":last_position,"safe_feed_delay_us":feed_lag_us,"projection_lag_us":projection_lag_us,"guard_lock_and_read_total_us":hot_guard.lock_read_us.load(Ordering::Relaxed),"guard_calls":hot_guard.calls.load(Ordering::Relaxed),"p50_us":quantile(&latencies,50),"p95_us":quantile(&latencies,95),"p99_us":quantile(&latencies,99),"append_p99_us":quantile(&append_latencies,99),"max_us":latencies.last(),"latencies_us":latencies,"append_latencies_us":append_latencies})
    );
    if violations != 0 {
        std::process::exit(1);
    }
}
