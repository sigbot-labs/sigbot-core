from common.rust import RustIntegration


def verify(context):
    environment = {"MARKETPULSE_DATABASE_URL": context.secret("database_url_env")}
    delivery = RustIntegration.execute(context, "sigbot-order", "venue_polling", [
        "account_broadcast_other_workflow_commits_quarantine_before_ack",
        "restart_restores_cursor_and_repeated_trades_do_not_duplicate_execution_or_fees",
        "missing_fees_and_invalid_pages_leave_facts_outbox_and_cursor_uncommitted",
        "leases_fence_expired_owners_and_scope_node_filters_select_only_owned_intents",
        "wallet_mode_mismatch_blocks_venue_observation_without_advancing_facts_or_cursor",
    ], environment)
    reconciliation = RustIntegration.execute(context, "sigbot-order", "order_reconciliation", [
        "unknown_submission_only_reconciles_and_preparation_is_immutable",
    ], environment)
    return {"delivery": delivery, "reconciliation_and_mode_fence": reconciliation}
