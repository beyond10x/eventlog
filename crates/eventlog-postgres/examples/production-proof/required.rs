//! Existing required cases, qualified by their actual Cargo provider and target.
use super::admission::RequiredCase;
macro_rules! cases {
    ($package:literal, $kind:literal, $target:literal; $($name:literal),+ $(,)?) => {
        [$(RequiredCase { package: $package, kind: $kind, target: $target, name: $name }),+]
    };
}
pub fn required_cases() -> Vec<RequiredCase> {
    let mut required = Vec::new();
    required.extend(cases!("eventlog-file", "test", "conformance";
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
    ));
    required.extend(cases!("eventlog-file", "lib", "eventlog_file";
        "journal::tests::process_death_at_each_append_boundary",
        "journal::tests::process_death_at_each_privacy_boundary",
        "journal::tests::privacy_crash_cleans_cached_bodies_and_blobs_on_open",
        "journal::tests::committed_damage_and_unproven_suffix_are_never_repaired",
        "journal::tests::divergent_longer_history_does_not_extend_observed_head",
        "journal::tests::mixed_recovery_intents_refuse_before_changing_authority",
    ));
    required.extend(cases!("eventlog-file", "test", "durability";
        "independent_processes_serialize_groups_and_duplicate_keys",
        "conflicting_groups_commit_exactly_one_complete_request",
        "blob_damage_and_missing_manifest_refuse_reopen",
        "reopen_preserves_receipts_and_cache_deletion_preserves_authority",
        "privacy_removes_active_bytes_and_never_reuses_feed_positions",
        "redaction_fences_projection_reads_and_new_writes_until_complete_rebuild",
    ));
    required.extend(cases!("eventlog-postgres", "test", "atomic_groups";
        "ordered_groups_commit_and_rollback_as_one_unit",
        "concurrent_groups_preserve_order_without_partial_commits",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "atomic_groups";
        "ordered_groups_commit_and_rollback_as_one_unit",
        "concurrent_groups_preserve_order_without_partial_commits",
        "group_retry_survives_database_reopen",
    ));
    required.extend(cases!("eventlog-postgres", "lib", "eventlog_postgres";
        "schema::blob_integrity_tests::historical_editions_and_old_reader_are_frozen",
        "blob_migration_tests::acknowledged_report_survives_cleanup_failure",
        "schema::group_migration_tests::snapshot_edition_migrates_to_groups_and_foreign_group_shape_refuses",
        "schema::snapshot_tests::snapshot_schema_upgrade_retains_history_and_refuses_partial_metadata",
        "pool::tests::reusable_retirement_preserves_two_connection_four_waiter_snapshot",
        "pool::tests::quarantine_retirement_cannot_count_a_replacement_twice",
        "pool::tests::idle_checkout_has_one_coherent_owner",
        "pool::tests::queued_cancellation_timeout_and_granted_cancellation_release_capacity",
        "pool::tests::zero_waiter_pool_reuses_and_refuses_without_queueing",
        "pool::tests::shutdown_cancels_waiters_and_drains_quarantine",
        "pool::tests::closed_idle_driver_is_joined_before_replacement_connects",
        "pool::tests::shutdown_does_not_recycle_a_returning_connection_after_close",
    ));
    required.extend(cases!("eventlog-postgres", "test", "conformance";
        "postgres_blob_integrity_covers_all_read_boundaries_and_binding_controls",
        "postgres_callback_blob_corruption_poison_rolls_back_every_owner",
        "postgres_blob_migration_is_exact_explicit_atomic_and_fenced",
        "postgres_blob_migration_unknown_commit_has_no_report",
        "snapshot_history_and_repository_privacy_interleavings",
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
        "absent_deployment_scope_races_across_tenants_and_clients",
        "registration_freezes_and_pool_refuses_bounded_overload",
        "pool_observation_preserves_two_connections_and_four_waiters",
        "shutdown_cancellation_preserves_public_pool_lifetimes",
        "public_queue_cancellation_and_broken_idle_reclaim_exact_capacity",
        "empty_catch_up_reuses_the_same_settled_session",
        "contended_catch_up_reuses_the_same_settled_session",
        "unsettled_catch_up_rollback_never_recycles_a_session",
        "verified_tls_requires_matching_server_and_separate_application_role",
        "legacy_populated_schema_migrates_atomically_and_unknown_checksums_refuse",
        "schema_admission_refuses_triggers_policies_generation_and_foreign_sequences",
        "schema_admission_refuses_rules_that_suppress_command_receipts",
        "hosted_schema_admission_refuses_rewrite_rules",
        "rewrite_rules_are_rejected_on_every_durable_table",
        "rewrite_rules_are_rejected_during_projection_migration_and_registration",
        "scope_reservations_are_atomic_and_confined",
        "rebuild_preserves_other_tenants_and_previous_view_on_failure",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "repository";
        "snapshot_history_and_repository_privacy_interleavings",
        "committed_commands_survive_snapshot_storage_failure",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "conformance";
        "sqlite_blob_integrity_covers_all_read_boundaries_and_binding_controls",
        "sqlite_callback_blob_corruption_poison_rolls_back_every_owner",
        "sqlite_blob_migration_is_exact_explicit_atomic_and_fenced",
        "review_caught_malformed_blob_metadata_poisons_sqlite_inline_append",
        "review_sqlite_admission_requires_an_enforced_integrity_check",
        "inline_failure_preserves_all_atomic_state_and_callback_authority",
        "rebuild_preserves_other_tenants_and_previous_view_on_failure",
        "scope_reservations_are_atomic_and_confined_in_memory_and_file",
    ));
    required.extend(cases!("eventlog-postgres", "test", "adversary";
        "unrelated_xmin_cannot_make_committed_positions_noncontiguous",
        "schema_admission_refuses_inherited_event_children",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "adversary";
        "legacy_projection_rows_are_erased_without_reregistering_retired_projectors",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "legacy_namespaces";
        "legacy_erasure_matches_literal_underscores_and_preserves_other_owners",
        "ambiguous_legacy_namespace_refuses_and_rolls_back_all_erasure",
    ));
    required.extend(cases!("eventlog-postgres", "test", "input_validation";
        "public_input_validation_is_atomic_on_postgresql",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "input_validation_review";
        "public_input_validation_is_atomic_on_sqlite",
        "append_refuses_deserialized_events_that_bypass_constructor_checks",
        "append_refuses_deserialized_streams_that_bypass_constructor_checks",
        "append_refuses_claim_fields_that_bypass_constructor_checks",
    ));
    required.extend(cases!("eventlog-core", "lib", "eventlog_core";
        "tests::input_validation_preserves_valid_wire_and_exact_field_limits",
    ));
    required
}
