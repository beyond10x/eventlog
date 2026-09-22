//! Existing required cases, qualified by their actual Cargo provider and target.
use super::admission::RequiredCase;
macro_rules! cases {
    ($package:literal, $kind:literal, $target:literal; $($name:literal),+ $(,)?) => {
        [$(RequiredCase { package: $package, kind: $kind, target: $target, name: $name }),+]
    };
}
pub fn required_cases() -> Vec<RequiredCase> {
    let mut required = Vec::new();
    required.extend(
        cases!("eventlog-sqlite", "test", "atomic_blob_integrity_review";
            "corrupt_reuse_precedes_byte_conflict_and_all_guard_modes",
            "malformed_sqlite_storage_classes_refuse_without_partial_publication",
            "retained_receipt_wins_before_corrupt_row_decode_after_head_advance",
            "foreign_corruption_and_guard_refusal_preserve_healthy_reuse",
        ),
    );
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
    required.extend(cases!("eventlog-file", "test", "consistent_capture";
        "file_consistent_capture_contract",
        "a_read_only_handle_inspects_a_store_nobody_opened_for_writing",
        "opening_and_capturing_change_no_stored_entry_or_byte",
        "a_pending_intent_refuses_without_recovery_cleanup_or_initialization",
        "a_dangling_pending_intent_entry_refuses_strict_open_and_capture",
        "missing_store_files_refuse_and_create_nothing",
        "capture_queues_behind_another_process_holding_the_writer_lock",
        "another_process_rewriting_history_invalidates_an_observing_handle",
        "only_missing_identity_and_redacted_history_answer_ahead_of_the_divergence_guard",
        "every_key_an_existing_writer_admits_round_trips_including_an_embedded_nul",
    ));
    required.extend(cases!("eventlog-file", "test", "inline_admin";
        "file_inline_admin_contract",
        "attach_and_attached_rebuild_refuse_each_pending_intent_without_changing_a_byte",
        "dirty_markers_survive_reopen_and_attach_until_the_selected_rebuild_publishes",
        "replay_holds_registration_coordination_and_cancelled_waiter_leaves_no_deadlock",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "consistent_capture";
        "sqlite_consistent_capture_contract",
        "capture_orders_against_append_and_erasure_across_separate_handles",
        "a_rebuild_paused_before_replacement_never_exposes_mixed_rows",
        "a_stored_identity_is_preserved_exactly_or_refused_as_corruption",
        "registry_and_physical_shape_drift_refuse_capture",
        "exact_non_text_tenant_coordinates_are_corruption_for_every_captured_material",
        "admitted_projection_keys_include_an_embedded_nul",
        "a_paged_coordinate_outside_text_is_corruption_in_both_reads",
        "a_stored_blob_length_is_proven_against_the_bytes_before_any_payload_cap",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "inline_admin";
        "sqlite_inline_admin_contract",
        "structural_attach_changes_neither_catalog_nor_registry_and_drift_installs_nothing",
        "corrupt_active_blob_aborts_the_complete_admin_fold",
        "paused_admin_rebuild_keeps_rows_cursor_registration_and_other_tenant_atomic",
        "cancelled_waiter_continues_as_one_worker_after_replay_releases_registration",
        "cancelled_rebuild_caller_leaves_one_continuing_worker_and_whole_publication",
    ));
    required.extend(cases!("eventlog-postgres", "test", "consistent_capture";
        "postgres_consistent_capture_contract",
        "capture_returns_committed_history_the_feed_watermark_withholds",
        "capture_observes_an_erasure_that_completed_during_its_lock_wait",
        "cancellation_and_deadline_retire_the_lease_and_recover_capacity",
        "a_rebuild_paused_before_replacement_never_exposes_mixed_rows",
        "registry_and_physical_shape_drift_refuse_capture",
        "a_stored_identity_is_preserved_exactly_or_refused_as_corruption",
        "a_stored_blob_length_is_proven_against_the_bytes_before_any_payload_cap",
    ));
    required.extend(cases!("eventlog-postgres", "test", "inline_admin";
        "postgres_inline_admin_contract",
        "restricted_application_role_attaches_existing_shapes_without_ddl",
        "rebuild_reads_a_committed_event_the_feed_watermark_withholds",
        "cancelled_registration_waiter_cannot_cross_replay_and_capacity_recovers",
        "cancellation_while_waiting_for_publication_retires_the_session_and_writer_recovers",
        "cancellation_during_replay_retires_the_session_and_later_rebuild_proceeds",
        "commit_and_unlock_response_loss_are_unknown_and_retire_before_capacity_returns",
        "replay_holds_publishers_and_keeps_one_active_blob_snapshot",
    ));
    required.extend(cases!("eventlog-file", "lib", "eventlog_file";
        "native_group_crash::every_native_group_boundary_recovers_one_complete_outcome",
        "inline_admin::tests::process_death_at_each_inline_rebuild_publication_boundary",
        "capture::tests::stored_identity_bytes_survive_exactly_and_an_empty_one_is_corruption",
        "capture::tests::the_strict_reader_holds_the_writer_lock_for_its_whole_life",
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
    required.extend(cases!("eventlog-sqlite", "lib", "eventlog_sqlite";
        "atomic_group::native_group_crash::every_native_group_boundary_recovers_one_complete_outcome",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "atomic_groups";
        "ordered_groups_commit_and_rollback_as_one_unit",
        "concurrent_groups_preserve_order_without_partial_commits",
        "group_retry_survives_database_reopen",
    ));
    required.extend(cases!("eventlog-postgres", "lib", "eventlog_postgres";
        "atomic_group::native_group_crash::every_native_group_boundary_recovers_one_complete_outcome",
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
        "review2_postgres_admission_refuses_unenforced_check_and_unmigrated_legacy_shape",
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
        "review2_sqlite_admission_refuses_hidden_collation_and_conflict_clauses",
        "review2_sqlite_nocase_digest_table_must_not_serve_another_identity",
        "review2_sqlite_hidden_clauses_change_binding_semantics_once_admitted",
        "review2_sqlite_similar_legacy_predecessors_are_refused_before_any_persistent_change",
        "review2_sqlite_legacy_admission_refuses_hidden_clauses_before_additive_migration",
        "review2_sqlite_admission_refuses_unrecognized_blob_table_semantics",
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
        "capture::tests::every_cap_is_exact_including_zero_and_never_saturates",
        "capture::tests::stored_order_identity_and_request_shapes_are_checked",
        "capture::tests::captured_content_is_ordered_bytewise_and_never_repeats_a_coordinate",
    ));
    required.extend(cases!("eventlog-core", "lib", "eventlog_core";
        "atomic_blob::tests::atomic_blob_fingerprint_binds_actual_bytes_not_caller_hashes",
        "atomic_blob::tests::atomic_blob_fingerprint_freezes_legacy_and_explicit_format",
        "atomic_blob::tests::atomic_blob_fingerprint_sorts_keys_and_refuses_empty_duplicate_or_invalid_input",
    ));
    required.extend(cases!("eventlog-file", "lib", "eventlog_file";
        "journal::tests::atomic_blob_crash_boundaries_recover_only_complete_publication",
        "journal::tests::atomic_blob_postcommit_cleanup_failure_keeps_committed_content",
    ));
    required.extend(cases!("eventlog-file", "test", "adversary_atomic_blob";
        "file_adversary_cancelled_guard_preserves_existing_content_and_receipt",
        "file_adversary_equal_length_collision_rolls_back",
        "file_adversary_rebound_retry_preserves_receipt_and_current_binding",
    ));
    required.extend(
        cases!("eventlog-file", "test", "adversary_strict_inspection";
            "adversary_file_duplicate_event_identity_is_corruption",
            "adversary_file_empty_identity_refuses_and_zero_result_caps_allow_absence",
            "adversary_file_inadmissible_recorded_envelope_is_corruption",
            "adversary_file_unknown_recorded_envelope_field_is_not_discarded",
        ),
    );
    required.extend(cases!("eventlog-file", "test", "atomic_blob";
        "file_atomic_blob_cleanup_failure_reports_retained_unbound_artifact",
        "file_atomic_blob_content_contract",
        "file_atomic_blob_independent_writers_keep_only_complete_winner",
        "file_atomic_blob_known_abort_cleans_only_owned_staging",
        "file_atomic_blob_reopen_retry_preserves_erasure_and_receipt",
    ));
    required.extend(cases!("eventlog-file", "test", "strict_inspection";
        "file_inspection_body_fields_and_zero_schema_are_preserved",
        "file_inspection_concurrent_append_is_one_observation",
        "file_inspection_history_preserves_source",
        "file_inspection_identity_redaction_and_unknown_format",
        "file_inspection_missing_sources_never_create",
        "file_inspection_native_lock_is_nonblocking",
        "file_inspection_recovery_and_corruption_preserve_source",
    ));
    required.extend(cases!("eventlog-postgres", "test", "adversary_atomic_blob";
        "postgres_adversary_cancelled_guard_preserves_existing_content_and_receipt",
        "postgres_adversary_equal_length_collision_rolls_back",
        "postgres_adversary_rebound_retry_preserves_receipt_and_current_binding",
    ));
    required.extend(cases!("eventlog-postgres", "test", "atomic_blob";
        "postgres_atomic_blob_content_contract",
        "postgres_atomic_blob_disjoint_streams_share_reversed_blob_sets",
        "postgres_atomic_blob_independent_writers_keep_only_complete_winner",
        "postgres_atomic_blob_lost_commit_response_resolves_without_resurrection",
        "postgres_atomic_blob_reopen_retry_preserves_erasure_and_receipt",
        "postgres_atomic_blob_reuse_serializes_standalone_delete",
    ));
    required.extend(cases!("eventlog-sqlite", "lib", "eventlog_sqlite";
        "atomic_group::tests::atomic_blob_native_commit_failure_is_unknown_and_retry_resolves",
        "inspection::linux::tests::inspection_concurrent_call_cannot_unlock_another_observation",
        "inspection::linux::tests::inspection_descriptor_exhaustion_never_opens_or_closes_source",
        "inspection::linux::tests::inspection_ofd_blocks_native_writer_process",
        "inspection::linux::tests::inspection_ofd_blocks_native_writers_in_process",
        "inspection::linux::tests::inspection_ofd_detects_replaced_source",
        "inspection::linux::tests::inspection_refusal_preserves_existing_process_reader_lock",
        "inspection::linux::tests::inspection_refusal_preserves_existing_process_writer_lock",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "adversary_atomic_blob";
        "sqlite_adversary_equal_length_collision_rolls_back",
        "sqlite_adversary_rebound_retry_preserves_receipt_and_current_binding",
    ));
    required.extend(
        cases!("eventlog-sqlite", "test", "adversary_strict_inspection";
            "adversary_sqlite_empty_identity_and_exact_source_cap",
            "adversary_sqlite_inadmissible_recorded_envelope_is_corruption",
            "adversary_sqlite_public_descriptor_bound_keeps_prior_writer_lock",
            "adversary_sqlite_success_preserves_preexisting_reader_lock",
        ),
    );
    required.extend(cases!("eventlog-sqlite", "test", "atomic_blob";
        "sqlite_atomic_blob_content_contract",
        "sqlite_atomic_blob_independent_writers_keep_only_complete_winner",
        "sqlite_atomic_blob_reopen_retry_preserves_erasure_and_receipt",
        "sqlite_atomic_blob_reuse_refuses_hash_mismatch_and_rolls_back",
        "sqlite_atomic_blob_reuse_refuses_malformed_hash_and_rolls_back",
        "sqlite_atomic_blob_reuse_refuses_missing_hash_and_rolls_back",
        "sqlite_atomic_blob_reuse_refuses_length_mismatch_and_rolls_back",
        "sqlite_atomic_blob_reuse_refuses_negative_length_and_rolls_back",
        "sqlite_atomic_blob_reuse_refuses_unknown_edition_and_rolls_back",
        "sqlite_atomic_blob_integrity_preserves_healthy_reuse_and_byte_conflicts",
        "sqlite_atomic_blob_receipt_retry_precedes_integrity_and_never_restores_erasure",
    ));
    required.extend(cases!("eventlog-sqlite", "test", "strict_inspection";
        "sqlite_inspection_history_preserves_source",
        "sqlite_inspection_identity_corruption_schema_and_uri",
        "sqlite_inspection_missing_sources_never_create",
        "sqlite_inspection_native_writer_refuses_without_changes",
        "sqlite_inspection_wal_and_journal_refusals_preserve_source",
    ));
    required
}
