-- Market sequence belongs to the external source; message sequence belongs to a
-- producer/node/run stream. A shared fact can have several Workflow deliveries.
CREATE TABLE IF NOT EXISTS market_event_delivery (
    env TEXT NOT NULL, tenant_id BIGINT NOT NULL, space_id BIGINT NOT NULL,
    workflow_id BIGINT NOT NULL REFERENCES s_workflow(id), node_id BIGINT NOT NULL,
    run_id TEXT NOT NULL REFERENCES workflow_run(run_id), source TEXT NOT NULL,
    source_event_id TEXT NOT NULL, revision INTEGER NOT NULL, message_seq BIGINT NOT NULL CHECK(message_seq>=0),
    payload JSONB NOT NULL CHECK(NOT (payload ? 'requestSpanId')), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env,tenant_id,space_id,node_id,run_id,source,source_event_id,revision),
    UNIQUE(env,tenant_id,space_id,node_id,run_id,message_seq),
    FOREIGN KEY(env,tenant_id,space_id,source,source_event_id,revision)
        REFERENCES market_event(env,tenant_id,space_id,source,source_event_id,revision)
);
INSERT INTO market_event_delivery(env,tenant_id,space_id,workflow_id,node_id,run_id,source,source_event_id,revision,message_seq,payload,created_at)
SELECT o.env,o.tenant_id,o.space_id,(o.payload->>'workflow_id')::bigint,o.node_id,o.run_id,m.source,m.source_event_id,m.revision,o.seq,o.payload,o.created_at
FROM messager_outbox o JOIN market_event m ON m.env=o.env AND m.tenant_id=o.tenant_id AND m.space_id=o.space_id
    AND m.source=o.payload->'payload'->>'source' AND m.source_event_id=o.payload->'payload'->>'source_event_id' AND m.revision=0
WHERE o.topic LIKE '%/feed/market' OR o.topic LIKE '%/feed/news' ON CONFLICT DO NOTHING;
-- Existing wire IDs are preserved and the next shared sequence cannot collide with them.
INSERT INTO workflow_sequence(env,tenant_id,space_id,node_id,run_id,next_seq)
SELECT env,tenant_id,space_id,node_id,run_id,MAX(seq)+1 FROM messager_outbox
GROUP BY env,tenant_id,space_id,node_id,run_id
ON CONFLICT(env,tenant_id,space_id,node_id,run_id)
DO UPDATE SET next_seq=GREATEST(workflow_sequence.next_seq,EXCLUDED.next_seq);
CREATE INDEX IF NOT EXISTS market_event_delivery_history ON market_event_delivery(env,tenant_id,space_id,run_id,source,source_event_id,revision);
