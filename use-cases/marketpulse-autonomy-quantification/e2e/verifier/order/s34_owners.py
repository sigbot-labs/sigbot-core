from common.rust import RustIntegration


def verify(context):
    environment = {"MARKETPULSE_DATABASE_URL": context.secret("database_url_env")}
    migration = RustIntegration.execute(context, "sigbot-order", "owner_migration", [
        "migration_keeps_completed_dedup_splits_same_node_workflows_and_preserves_suffixes",
        "migration_refuses_conflicting_facts_instead_of_resetting_deduplication",
        "migration_refuses_invalid_or_unrecoverable_workflow_binding",
    ], environment)
    delivery = RustIntegration.execute(context, "sigbot-order", "venue_polling", [
        "account_broadcast_other_workflow_commits_quarantine_before_ack",
        "restart_restores_cursor_and_repeated_trades_do_not_duplicate_execution_or_fees",
        "missing_fees_and_invalid_pages_leave_facts_outbox_and_cursor_uncommitted",
        "leases_fence_expired_owners_and_scope_node_filters_select_only_owned_intents",
    ], environment)
    return {"migration": migration, "delivery": delivery}
