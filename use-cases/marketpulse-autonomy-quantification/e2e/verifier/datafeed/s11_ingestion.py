from common.rust import RustIntegration


def verify(context):
    return RustIntegration.execute(context, "sigbot-datafeed", "contracts", [
        "real_rest_mcp_a2a_transport_equivalence",
        "postgres_late_revision_future_exclusion_and_partition_equivalence",
        "source_revisions_preserve_canonical_delivery_and_allocate_distinct_sequences",
        "as_of_uses_only_persisted_facts_and_duplicate_request_keeps_canonical_reply",
        "active_latest_commits_two_real_sources_and_reply_once_across_restart",
        "independent_symbol_checkpoints_and_reversed_queries_preserve_results",
    ], {"MARKETPULSE_TWITTER_URL": context.endpoint("twitter"),
        "MARKETPULSE_TRUTHSOCIAL_URL": context.endpoint("truthsocial"),
        "MARKETPULSE_DATABASE_URL": context.secret("database_url_env")})
