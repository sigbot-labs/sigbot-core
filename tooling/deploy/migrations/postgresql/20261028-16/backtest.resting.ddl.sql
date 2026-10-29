-- Accepted limit orders retain the original business envelope and the exact
-- matching terms. A restart must not guess an owner or silently change fees.
ALTER TABLE simulated_order
    ADD COLUMN original_message JSONB CHECK(original_message IS NULL OR NOT(original_message ? 'requestSpanId')),
    ADD COLUMN matching_node_id BIGINT CHECK(matching_node_id IS NULL OR matching_node_id>0),
    ADD COLUMN matching_configuration JSONB;
CREATE INDEX simulated_order_resting_idx
    ON simulated_order(env,tenant_id,space_id,matching_node_id,run_id,accepted_at,producer_node_id,seq,account_id)
    WHERE state='accepted';
-- Pre-migration accepted rows remain visible for reconciliation. Their missing
-- owner/configuration cannot safely be inferred from an exchange acceptance ID.
