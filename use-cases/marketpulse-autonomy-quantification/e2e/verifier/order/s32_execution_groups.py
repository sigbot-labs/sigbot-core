from common.rust import RustIntegration


def verify(context):
    return RustIntegration.execute(context, "sigbot-order", "execution_groups", [
        "completed_group_requires_wallet_booking_and_workflow_owner",
        "compensation_is_reduce_only_once_and_survives_coordinator_restart",
        "unknown_and_partial_fills_wait_for_terminal_cumulative_quantity",
        "hold_policy_never_emits_compensation",
        "rejected_compensation_is_a_visible_terminal_failure",
        "immutable_leg_identity_and_admission_deadline_are_enforced",
    ], {"MARKETPULSE_DATABASE_URL": context.secret("database_url_env")})
