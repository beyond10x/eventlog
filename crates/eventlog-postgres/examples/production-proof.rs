//! Required real-backend runner. Missing prerequisites and selected-zero lanes are refusals.
use clap::Parser;
use serde_json::json;
#[derive(Parser)]
struct Args {}
use sha2::{Digest, Sha256};
use std::{
    process::{Command, ExitCode},
    time::Instant,
};

fn main() -> ExitCode {
    let _ = Args::parse();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("production proof refused: {error}");
            ExitCode::FAILURE
        }
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    for required in [
        "EVENTLOG_TEST_POSTGRES_URL",
        "EVENTLOG_TEST_POSTGRES_CA",
        "EVENTLOG_TEST_HOSTED_POSTGRES_URL",
    ] {
        if std::env::var(required)
            .ok()
            .is_none_or(|value| value.trim().is_empty())
        {
            return Err(format!("required fixture {required} is absent").into());
        }
    }
    let started = time::OffsetDateTime::now_utc();
    let elapsed = Instant::now();
    let output = Command::new("cargo")
        .args(["test", "--workspace", "--locked", "--", "--nocapture"])
        .env("EVENTLOG_REQUIRE_POSTGRES", "1")
        .env("RUST_TEST_THREADS", "1")
        .output()?;
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    if let Ok(path) = std::env::var("EVENTLOG_PROOF_RAW") {
        std::fs::write(path, format!("{stdout}\n{stderr}"))?;
    }
    print!("{stdout}");
    eprint!("{stderr}");
    let mut passed = 0_usize;
    let mut failed = 0_usize;
    let mut ignored = 0_usize;
    let mut summaries = Vec::new();
    for line in stdout
        .lines()
        .filter(|line| line.starts_with("test result:"))
    {
        summaries.push(line);
        for part in line.split(';') {
            let words: Vec<_> = part.split_whitespace().collect();
            for pair in words.windows(2) {
                if let Ok(count) = pair[0].parse::<usize>() {
                    match pair[1] {
                        "passed" => passed += count,
                        "failed" => failed += count,
                        "ignored" => ignored += count,
                        _ => {}
                    }
                }
            }
        }
    }
    let required = [
        "file_storage_contract",
        "file_groups_contract",
        "file_claims_contract",
        "file_snapshots_contract",
        "file_scopes_contract",
        "file_callback_failure_contract",
        "file_projections_contract",
        "file_inline_contract",
        "file_paging_contract",
        "file_rebuild_contract",
        "file_public_inputs_contract",
        "journal::tests::process_death_at_each_append_boundary",
        "journal::tests::process_death_at_each_privacy_boundary",
        "journal::tests::privacy_crash_cleans_cached_bodies_and_blobs_on_open",
        "journal::tests::committed_damage_and_unproven_suffix_are_never_repaired",
        "journal::tests::divergent_longer_history_does_not_extend_observed_head",
        "journal::tests::mixed_recovery_intents_refuse_before_changing_authority",
        "independent_processes_serialize_groups_and_duplicate_keys",
        "conflicting_groups_commit_exactly_one_complete_request",
        "blob_damage_and_missing_manifest_refuse_reopen",
        "reopen_preserves_receipts_and_cache_deletion_preserves_authority",
        "privacy_removes_active_bytes_and_never_reuses_feed_positions",
        "redaction_fences_projection_reads_and_new_writes_until_complete_rebuild",
        "ordered_groups_commit_and_rollback_as_one_unit",
        "concurrent_groups_preserve_order_without_partial_commits",
        "group_retry_survives_database_reopen",
        "schema::group_migration_tests::snapshot_edition_migrates_to_groups_and_foreign_group_shape_refuses",
        "snapshot_history_and_repository_privacy_interleavings",
        "schema::snapshot_tests::snapshot_schema_upgrade_retains_history_and_refuses_partial_metadata",
        "committed_commands_survive_snapshot_storage_failure",
        "snapshot_capture_waits_for_complete_tenant_erasure",
        "independent_first_appends_return_contract_outcomes",
        "isolated_transport_cannot_hide_a_remote_address_behind_localhost",
        "lost_commit_response_reconnects_to_exact_durable_receipt",
        "missing_scope_bootstrap_is_atomic_across_independent_processes",
        "caught_reservation_cancellation_cannot_commit_an_unchecked_append",
        "inline_failure_preserves_all_atomic_state_and_callback_authority",
        "killed_projector_restarts_competing_workers_without_partial_view_or_cursor",
        "a_reader_never_skips_an_event_that_committed_late",
        "a_feed_cursor_cannot_pass_an_inflight_lower_position_with_a_newer_xid",
        "unrelated_xmin_cannot_make_committed_positions_noncontiguous",
        "schema_admission_refuses_inherited_event_children",
        "legacy_projection_rows_are_erased_without_reregistering_retired_projectors",
        "legacy_erasure_matches_literal_underscores_and_preserves_other_owners",
        "ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure",
        "absent_deployment_scope_races_across_tenants_and_clients",
        "registration_freezes_and_pool_refuses_bounded_overload",
        "pool_observation_preserves_two_connections_and_four_waiters",
        "shutdown_cancellation_preserves_public_pool_lifetimes",
        "public_queue_cancellation_and_broken_idle_reclaim_exact_capacity",
        "empty_catch_up_reuses_the_same_settled_session",
        "contended_catch_up_reuses_the_same_settled_session",
        "unsettled_catch_up_rollback_never_recycles_a_session",
        "pool::tests::reusable_retirement_preserves_two_connection_four_waiter_snapshot",
        "pool::tests::quarantine_retirement_cannot_count_a_replacement_twice",
        "pool::tests::idle_checkout_has_one_coherent_owner",
        "pool::tests::queued_cancellation_timeout_and_granted_cancellation_release_capacity",
        "pool::tests::zero_waiter_pool_reuses_and_refuses_without_queueing",
        "pool::tests::shutdown_cancels_waiters_and_drains_quarantine",
        "pool::tests::closed_idle_driver_is_joined_before_replacement_connects",
        "pool::tests::shutdown_does_not_recycle_a_returning_connection_after_close",
        "verified_tls_requires_matching_server_and_separate_application_role",
        "legacy_populated_schema_migrates_atomically_and_unknown_checksums_refuse",
        "schema_admission_refuses_triggers_policies_generation_and_foreign_sequences",
        "schema_admission_refuses_rules_that_suppress_command_receipts",
        "hosted_schema_admission_refuses_rewrite_rules",
        "rewrite_rules_are_rejected_on_every_durable_table",
        "rewrite_rules_are_rejected_during_projection_migration_and_registration",
        "public_input_validation_is_atomic_on_postgresql",
        "public_input_validation_is_atomic_on_sqlite",
        "append_refuses_deserialized_events_that_bypass_constructor_checks",
        "append_refuses_deserialized_streams_that_bypass_constructor_checks",
        "append_refuses_claim_fields_that_bypass_constructor_checks",
        "tests::input_validation_preserves_valid_wire_and_exact_field_limits",
        "scope_reservations_are_atomic_and_confined",
        "rebuild_preserves_other_tenants_and_previous_view_on_failure",
    ];
    let missing: Vec<_> = required
        .iter()
        .filter(|name| {
            !stdout
                .lines()
                .any(|line| line.starts_with(&format!("test {name} ...")) && line.ends_with("ok"))
        })
        .collect();
    let valid = output.status.success()
        && passed > 0
        && failed == 0
        && ignored == 0
        && missing.is_empty()
        && !stderr.contains("skipped:")
        && !stdout.contains("skipped:");
    let revision = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()?;
    let binary = std::fs::read(std::env::current_exe()?)?;
    let server = tokio::runtime::Runtime::new()?.block_on(async {
        let configuration: tokio_postgres::Config =
            std::env::var("EVENTLOG_TEST_POSTGRES_URL")?.parse()?;
        let (client, connection) = configuration.connect(tokio_postgres::NoTls).await?;
        let driver = tokio::spawn(connection);
        let row = client
            .query_one(
                "SELECT current_setting('server_version'), current_setting('server_version_num')",
                &[],
            )
            .await?;
        let server = json!({"version":row.get::<_,String>(0),"version_num":row.get::<_,String>(1)});
        drop(client);
        driver.await??;
        Ok::<_, Box<dyn std::error::Error>>(server)
    })?;
    let report = json!({"format":"eventlog-production-proof/1","backend_server":server,"schema_setup":"test-owned exact prefixes and hosted_owner schema; additive checksum admission exercised","default_pool":{"connections":4,"waiters":32,"acquisition_ms":2000,"transaction_ms":10000},"source_revision":String::from_utf8(revision.stdout)?.trim(),"source_dirty":!dirty.stdout.is_empty(),"binary_sha256":format!("{:x}",Sha256::digest(binary)),"started_at":started.to_string(),"finished_at":time::OffsetDateTime::now_utc().to_string(),"duration_ms":elapsed.elapsed().as_millis(),"passed":passed,"failed":failed,"skipped":ignored,"missing_required_cases":missing,"runner_summaries":summaries,"conformance_valid":valid,"capacity_admitted":false,"capacity_requirement":"comparative laboratory artifact is separately required; this runner does not manufacture capacity evidence","owner_fixture_handoff":["SDK current authority and exact realm/service bindings","SDK original and generated stream/feed/cursor/view/effect vectors"]});
    if let Ok(path) = std::env::var("EVENTLOG_PROOF_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    println!("{report}");
    if !valid {
        return Err("required backend suite did not execute completely and pass".into());
    }
    Ok(())
}
