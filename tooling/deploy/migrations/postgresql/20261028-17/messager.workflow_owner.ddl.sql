-- Pause Order/Exporter writers before applying, then restart with workflow-scoped
-- owners. Preserve completed Inbox and sent Outbox records: they are dedup facts.
LOCK TABLE messager_inbox, messager_outbox IN ACCESS EXCLUSIVE MODE;
CREATE TEMP TABLE workflow_owner_migration (
    kind TEXT NOT NULL, env TEXT NOT NULL, tenant_id BIGINT NOT NULL,
    space_id BIGINT NOT NULL, old_owner TEXT NOT NULL, new_owner TEXT NOT NULL,
    node_id BIGINT NOT NULL, run_id TEXT NOT NULL, seq BIGINT NOT NULL,
    PRIMARY KEY(kind,env,tenant_id,space_id,old_owner,node_id,run_id,seq)
) ON COMMIT DROP;

CREATE FUNCTION pg_temp.workflow_owner_name(owner TEXT, environment TEXT,
    tenant BIGINT, space BIGINT, message JSONB, run TEXT) RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE parts TEXT[]; message_workflow BIGINT; stored_workflow BIGINT;
BEGIN
    parts := regexp_match(owner,
        '^(order(?:[.]execution|[.]status)?|export):([A-Za-z0-9_-]+):([0-9]+):([0-9]+):([0-9]+)(:(?:reservation|venue))?$');
    IF parts IS NULL THEN RETURN owner; END IF;
    IF parts[2] <> environment OR parts[3]::BIGINT <> tenant OR parts[4]::BIGINT <> space THEN
        RAISE EXCEPTION 'legacy message owner differs from its persisted scope';
    END IF;
    IF message->>'workflow_id' IS NOT NULL AND NOT message->>'workflow_id' ~ '^[1-9][0-9]*$' THEN
        RAISE EXCEPTION 'legacy message has an invalid explicit workflow binding';
    END IF;
    IF message->>'workflow_id' ~ '^[1-9][0-9]*$' THEN
        message_workflow := (message->>'workflow_id')::BIGINT;
    END IF;
    SELECT workflow_id INTO stored_workflow FROM workflow_run
        WHERE run_id=run AND env=environment AND tenant_id=tenant AND space_id=space;
    IF message_workflow IS NOT NULL AND stored_workflow IS NOT NULL
        AND message_workflow <> stored_workflow THEN
        RAISE EXCEPTION 'message workflow differs from its durable run';
    END IF;
    message_workflow := COALESCE(message_workflow,stored_workflow);
    IF message_workflow IS NULL THEN
        RAISE EXCEPTION 'legacy order/export message has no recoverable workflow binding; reconcile before migration';
    END IF;
    RETURN format('%s:%s:%s:%s:%s:%s%s',parts[1],environment,tenant,space,
        message_workflow,parts[5],COALESCE(parts[6],''));
END $$;

-- Only missing routing metadata is backfilled from a proven Workflow binding.
-- Financial domain payloads and immutable wallet journals are never changed.
CREATE FUNCTION pg_temp.workflow_owner_payload(message JSONB, owner TEXT) RETURNS JSONB
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN message->>'workflow_id' IS NULL
        THEN jsonb_set(message,'{workflow_id}',to_jsonb(split_part(owner,':',5)::BIGINT),true)
        ELSE message END
$$;

INSERT INTO workflow_owner_migration
SELECT 'inbox',env,tenant_id,space_id,consumer,new_owner,node_id,run_id,seq
FROM (SELECT *,pg_temp.workflow_owner_name(consumer,env,tenant_id,space_id,payload,run_id) new_owner
      FROM messager_inbox) mapped WHERE consumer<>new_owner;
INSERT INTO workflow_owner_migration
SELECT 'outbox',env,tenant_id,space_id,producer,new_owner,node_id,run_id,seq
FROM (SELECT *,pg_temp.workflow_owner_name(producer,env,tenant_id,space_id,payload,run_id) new_owner
      FROM messager_outbox) mapped WHERE producer<>new_owner;

-- Partial rollouts may have both spellings. Only identical canonical facts merge.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM workflow_owner_migration m
        JOIN messager_inbox old ON m.kind='inbox' AND (old.env,old.tenant_id,old.space_id,old.consumer,old.node_id,old.run_id,old.seq)=
            (m.env,m.tenant_id,m.space_id,m.old_owner,m.node_id,m.run_id,m.seq)
        JOIN messager_inbox current ON (current.env,current.tenant_id,current.space_id,current.consumer,current.node_id,current.run_id,current.seq)=
            (m.env,m.tenant_id,m.space_id,m.new_owner,m.node_id,m.run_id,m.seq)
        WHERE old.topic<>current.topic OR (pg_temp.workflow_owner_payload(old.payload,m.new_owner)-ARRAY['attempt_no','traceparent','produced_at'])<>(pg_temp.workflow_owner_payload(current.payload,m.new_owner)-ARRAY['attempt_no','traceparent','produced_at'])
    ) OR EXISTS (
        SELECT 1 FROM workflow_owner_migration m
        JOIN messager_outbox old ON m.kind='outbox' AND (old.env,old.tenant_id,old.space_id,old.producer,old.node_id,old.run_id,old.seq)=
            (m.env,m.tenant_id,m.space_id,m.old_owner,m.node_id,m.run_id,m.seq)
        JOIN messager_outbox current ON (current.env,current.tenant_id,current.space_id,current.producer,current.node_id,current.run_id,current.seq)=
            (m.env,m.tenant_id,m.space_id,m.new_owner,m.node_id,m.run_id,m.seq)
        WHERE old.topic<>current.topic OR (pg_temp.workflow_owner_payload(old.payload,m.new_owner)-ARRAY['attempt_no','traceparent','produced_at'])<>(pg_temp.workflow_owner_payload(current.payload,m.new_owner)-ARRAY['attempt_no','traceparent','produced_at'])
    ) THEN RAISE EXCEPTION 'legacy owner migration found conflicting immutable message facts'; END IF;
END $$;

INSERT INTO messager_inbox(env,tenant_id,space_id,consumer,node_id,run_id,seq,topic,payload,attempt_no,state,received_at,completed_at,quarantine_reason)
SELECT old.env,old.tenant_id,old.space_id,m.new_owner,old.node_id,old.run_id,old.seq,old.topic,pg_temp.workflow_owner_payload(old.payload,m.new_owner),
       old.attempt_no,old.state,old.received_at,old.completed_at,old.quarantine_reason
FROM workflow_owner_migration m JOIN messager_inbox old ON m.kind='inbox'
    AND (old.env,old.tenant_id,old.space_id,old.consumer,old.node_id,old.run_id,old.seq)=
        (m.env,m.tenant_id,m.space_id,m.old_owner,m.node_id,m.run_id,m.seq)
ON CONFLICT(env,tenant_id,space_id,consumer,node_id,run_id,seq) DO UPDATE SET
    payload=pg_temp.workflow_owner_payload(messager_inbox.payload,EXCLUDED.consumer),
    state=CASE WHEN messager_inbox.state='done' OR EXCLUDED.state='done' THEN 'done'
               WHEN messager_inbox.state='quarantined' OR EXCLUDED.state='quarantined' THEN 'quarantined' ELSE 'pending' END,
    attempt_no=GREATEST(messager_inbox.attempt_no,EXCLUDED.attempt_no),
    received_at=LEAST(messager_inbox.received_at,EXCLUDED.received_at),
    completed_at=GREATEST(messager_inbox.completed_at,EXCLUDED.completed_at),
    quarantine_reason=COALESCE(messager_inbox.quarantine_reason,EXCLUDED.quarantine_reason);
DELETE FROM messager_inbox old USING workflow_owner_migration m WHERE m.kind='inbox'
    AND (old.env,old.tenant_id,old.space_id,old.consumer,old.node_id,old.run_id,old.seq)=
        (m.env,m.tenant_id,m.space_id,m.old_owner,m.node_id,m.run_id,m.seq);

INSERT INTO messager_outbox(env,tenant_id,space_id,producer,node_id,run_id,seq,topic,payload,attempt_no,state,available_at,created_at,sent_at)
SELECT old.env,old.tenant_id,old.space_id,m.new_owner,old.node_id,old.run_id,old.seq,old.topic,pg_temp.workflow_owner_payload(old.payload,m.new_owner),
       old.attempt_no,old.state,old.available_at,old.created_at,old.sent_at
FROM workflow_owner_migration m JOIN messager_outbox old ON m.kind='outbox'
    AND (old.env,old.tenant_id,old.space_id,old.producer,old.node_id,old.run_id,old.seq)=
        (m.env,m.tenant_id,m.space_id,m.old_owner,m.node_id,m.run_id,m.seq)
ON CONFLICT(env,tenant_id,space_id,producer,node_id,run_id,seq) DO UPDATE SET
    payload=pg_temp.workflow_owner_payload(messager_outbox.payload,EXCLUDED.producer),
    state=CASE WHEN messager_outbox.state='sent' OR EXCLUDED.state='sent' THEN 'sent'
               WHEN messager_outbox.state='quarantined' OR EXCLUDED.state='quarantined' THEN 'quarantined' ELSE 'pending' END,
    attempt_no=GREATEST(messager_outbox.attempt_no,EXCLUDED.attempt_no),
    available_at=GREATEST(messager_outbox.available_at,EXCLUDED.available_at),
    created_at=LEAST(messager_outbox.created_at,EXCLUDED.created_at),
    sent_at=GREATEST(messager_outbox.sent_at,EXCLUDED.sent_at);
DELETE FROM messager_outbox old USING workflow_owner_migration m WHERE m.kind='outbox'
    AND (old.env,old.tenant_id,old.space_id,old.producer,old.node_id,old.run_id,old.seq)=
        (m.env,m.tenant_id,m.space_id,m.old_owner,m.node_id,m.run_id,m.seq);
DROP FUNCTION pg_temp.workflow_owner_name(TEXT,TEXT,BIGINT,BIGINT,JSONB,TEXT);
DROP FUNCTION pg_temp.workflow_owner_payload(JSONB,TEXT);
