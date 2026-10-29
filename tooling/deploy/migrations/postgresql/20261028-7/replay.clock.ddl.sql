-- Freeze finite historical datasets and retain the actual time AI output became available.
ALTER TABLE market_event ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE evaluation_snapshot ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
UPDATE evaluation_snapshot e SET available_at=GREATEST(e.as_of,COALESCE((
    SELECT MIN(o.created_at) FROM messager_outbox o WHERE o.env=e.env AND o.tenant_id=e.tenant_id
    AND o.space_id=e.space_id AND o.node_id=e.node_id AND o.run_id=e.run_id
    AND o.topic LIKE '%/evaluation/snapshot' AND (o.payload->'payload'->>'version')::bigint=e.version
),NOW())) WHERE e.available_at IS NULL;
ALTER TABLE evaluation_snapshot ALTER COLUMN available_at SET NOT NULL;
ALTER TABLE evaluation_snapshot ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS market_event_replay ON market_event(env,tenant_id,space_id,run_id,available_at,source,source_event_id,revision);
CREATE INDEX IF NOT EXISTS evaluation_snapshot_replay ON evaluation_snapshot(env,tenant_id,space_id,run_id,available_at,node_id,version);
CREATE TABLE IF NOT EXISTS workflow_timer (
    env TEXT NOT NULL, tenant_id BIGINT NOT NULL, space_id BIGINT NOT NULL,
    workflow_id BIGINT NOT NULL REFERENCES s_workflow(id), node_id BIGINT NOT NULL,
    run_id TEXT NOT NULL REFERENCES workflow_run(run_id), seq BIGINT NOT NULL CHECK(seq>=0),
    scheduled_at TIMESTAMPTZ NOT NULL, payload JSONB NOT NULL, traceparent TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','delivered')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env,tenant_id,space_id,node_id,run_id,seq)
);
CREATE INDEX IF NOT EXISTS workflow_timer_due ON workflow_timer(env,tenant_id,space_id,run_id,scheduled_at,node_id,seq) WHERE state='pending';
CREATE TABLE IF NOT EXISTS backtest_checkpoint (
    env TEXT NOT NULL, tenant_id BIGINT NOT NULL, space_id BIGINT NOT NULL,
    run_id TEXT NOT NULL REFERENCES workflow_run(run_id), configuration JSONB NOT NULL,
    frozen_at TIMESTAMPTZ NOT NULL, cursor JSONB, virtual_time TIMESTAMPTZ NOT NULL,
    steps BIGINT NOT NULL DEFAULT 0 CHECK(steps>=0),
    state TEXT NOT NULL DEFAULT 'running' CHECK(state IN ('running','complete','failed')),
    PRIMARY KEY(env,tenant_id,space_id,run_id)
);
