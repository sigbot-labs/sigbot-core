from common.rust import RustIntegration
from common.storage import ArchiveStorage


def verify(context):
    ArchiveStorage(context).bootstrap()
    environment = {"MARKETPULSE_DATABASE_URL": context.secret("database_url_env")}
    contracts = [
        ("sigbot-http", "http", [
            "signed_gateway_context_is_scoped_through_real_http_workflow_transactions"]),
        ("sigbot-core", "mgmt_health", [
            "real_http_requires_ready_business_and_reachable_database_and_detects_task_exit"]),
        ("sigbot-logger", "archive_persistence", ["durable_restart_late_cursor_dedup_and_scope"]),
        ("sigbot-notifier", "notifier_runtime", [
            "postgres_in_app_duplicate_scope_single_owner_and_crash_recovery"]),
        ("sigbot-strategy-runner", "strategy_routing", [
            "previous_run_is_durably_quarantined_per_instance_before_clock_or_callback",
            "routing_quarantine_cannot_bypass_scope_or_workflow",
            "same_topic_other_targets_and_edges_are_durable_without_consuming_the_intended_inbox"]),
    ]
    result = {target: RustIntegration.execute(context, package, target, tests, environment)
              for package, target, tests in contracts}
    return {"contracts": result,
            "identity_boundary": "official signed-context fixture, actual TCP and PG; Hosted Login remains a separate gate"}
