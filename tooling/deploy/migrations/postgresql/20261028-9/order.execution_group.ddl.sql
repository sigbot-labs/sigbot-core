-- Existing group rows remain readable but cannot run without an explicit owner.
ALTER TABLE execution_group ADD COLUMN IF NOT EXISTS producer_node_id BIGINT NOT NULL DEFAULT 0;
ALTER TABLE execution_group ADD COLUMN IF NOT EXISTS workflow_id BIGINT;
ALTER TABLE execution_group ADD COLUMN IF NOT EXISTS coordinator_node_id BIGINT;
ALTER TABLE execution_group ADD COLUMN IF NOT EXISTS leg_count INTEGER;
ALTER TABLE execution_group ADD COLUMN IF NOT EXISTS deadline TIMESTAMPTZ;
ALTER TABLE execution_group ADD COLUMN IF NOT EXISTS failure_policy TEXT;
ALTER TABLE execution_group ADD COLUMN IF NOT EXISTS traceparent TEXT;
ALTER TABLE execution_group ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE execution_group DROP CONSTRAINT IF EXISTS execution_group_pkey;
ALTER TABLE execution_group ADD PRIMARY KEY(env,tenant_id,space_id,producer_node_id,run_id,seq);
CREATE TABLE IF NOT EXISTS execution_group_leg (
    env TEXT NOT NULL,tenant_id BIGINT NOT NULL,space_id BIGINT NOT NULL,
    producer_node_id BIGINT NOT NULL,run_id TEXT NOT NULL,group_seq BIGINT NOT NULL,leg_index INTEGER NOT NULL,
    order_node_id BIGINT NOT NULL,wallet_id BIGINT NOT NULL REFERENCES s_wallets(id),
    intent_seq BIGINT NOT NULL,intent JSONB NOT NULL,rejected BOOLEAN NOT NULL DEFAULT FALSE,
    compensation_seq BIGINT,compensation_intent JSONB,
    PRIMARY KEY(env,tenant_id,space_id,producer_node_id,run_id,group_seq,leg_index),
    FOREIGN KEY(env,tenant_id,space_id,producer_node_id,run_id,group_seq)
        REFERENCES execution_group(env,tenant_id,space_id,producer_node_id,run_id,seq)
);
CREATE INDEX IF NOT EXISTS execution_group_owner_pending ON execution_group(env,tenant_id,space_id,coordinator_node_id,updated_at)
    WHERE state NOT IN ('completed','held','compensated','compensation_failed');
