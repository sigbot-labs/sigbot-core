from common.rust import RustIntegration


def verify(context):
    return RustIntegration.execute(context, "sigbot-strategy-runner", "service_effects", [
        "account_callbacks_commit_requests_once_and_service_results_return_through_on_data",
        "lost_checkpoint_owner_rolls_back_inbox_and_all_service_requests",
    ], {"MARKETPULSE_DATABASE_URL": context.secret("database_url_env")})
