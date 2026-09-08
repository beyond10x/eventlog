//! Exact retained envelopes and partial projection replay, shared by both adapter revisions.
use clap::Parser;
use eventlog_core::{CatchUpRunner, EventStore, Expected, StreamId, TenantId};
use eventlog_postgres::PostgresEventStore;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
#[derive(Parser)]
struct Args {
    prefix: String,
    #[arg(value_parser=["prepare","recover"])]
    phase: String,
    manifest: PathBuf,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let started = Instant::now();
    let url = std::env::var("EVENTLOG_TEST_POSTGRES_URL")?;
    let store = Arc::new(PostgresEventStore::connect(&url, &args.prefix).await?);
    let tenant = TenantId::new("recovery-tenant")?;
    let stream = StreamId::new(tenant.clone(), "item", "retained")?;
    let runner = CatchUpRunner::new(store.clone(), Arc::new(eventlog_conformance::Tally))
        .await?
        .with_batch(10);
    if args.phase == "prepare" {
        for index in 0..20 {
            store
                .append(
                    &stream,
                    Expected::Any,
                    &[eventlog_conformance::event("item.received", index)],
                    &eventlog_conformance::meta(
                        &format!("command-{index}"),
                        &serde_json::json!({"number":index}),
                    ),
                )
                .await?;
        }
        let events = store.read_stream(&stream, 0, 100).await?.events;
        std::fs::write(&args.manifest, serde_json::to_vec_pretty(&events)?)?;
        assert_eq!(runner.run_once(&tenant).await?.applied, 10);
        println!(
            "{}",
            serde_json::json!({"prepared_events":20,"projected_before_restart":10,"elapsed_ms":started.elapsed().as_millis()})
        );
    } else {
        let expected: Vec<eventlog_core::RecordedEvent> =
            serde_json::from_slice(&std::fs::read(&args.manifest)?)?;
        assert_eq!(
            store.read_stream(&stream, 0, 100).await?.events,
            expected,
            "exact retained envelopes"
        );
        let retry = store
            .append(
                &stream,
                Expected::Any,
                &[eventlog_conformance::event("item.received", 19)],
                &eventlog_conformance::meta("command-19", &serde_json::json!({"number":19})),
            )
            .await?;
        assert!(retry.deduplicated);
        assert_eq!(retry.events, expected[19..]);
        let competing = Arc::new(PostgresEventStore::connect(&url, &args.prefix).await?);
        let competitor = CatchUpRunner::new(competing, Arc::new(eventlog_conformance::Tally))
            .await?
            .with_batch(3);
        let replay_started = Instant::now();
        loop {
            let (first, second) =
                tokio::join!(runner.run_once(&tenant), competitor.run_once(&tenant));
            let (first, second) = (first?, second?);
            if first.position.max(second.position) >= expected.last().expect("events").global_seq {
                break;
            }
            assert!(
                replay_started.elapsed() < Duration::from_secs(15),
                "replay bound"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            store
                .projection_get(&eventlog_conformance::TALLY, &tenant, "item/retained")
                .await?
                .expect("view")["count"],
            20
        );
        println!(
            "{}",
            serde_json::json!({"recovered_events":20,"projection_count":20,"receipt_deduplicated":true,"correctness_violations":0,"replay_ms":replay_started.elapsed().as_millis(),"reconnect_and_replay_ms":started.elapsed().as_millis()})
        );
    }
    Ok(())
}
