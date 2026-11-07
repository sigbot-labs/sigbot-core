CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE FUNCTION uuid_v7() RETURNS UUID LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    unix_millis BIGINT;
    value BYTEA;
BEGIN
    unix_millis := floor(extract(epoch FROM clock_timestamp()) * 1000);
    value := decode(lpad(to_hex(unix_millis), 12, '0'), 'hex') || gen_random_bytes(10);
    value := set_byte(value, 6, (get_byte(value, 6) & 15) | 112);
    value := set_byte(value, 8, (get_byte(value, 8) & 63) | 128);
    RETURN encode(value, 'hex')::UUID;
END $$;

CREATE FUNCTION uuid_is_v7(value UUID) RETURNS BOOLEAN
LANGUAGE SQL IMMUTABLE STRICT AS $$
    SELECT (get_byte(uuid_send(value), 6) >> 4) = 7
$$;

-- Ten domain roots. Workflow DAGs are atomic JSON; provider channels need no child tables.
CREATE TABLE tenant_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    name TEXT NOT NULL,
    description TEXT,
    configuration JSONB NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL DEFAULT '',
    updated_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE datasource_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenant_info(id),
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    transport TEXT NOT NULL,
    configuration JSONB NOT NULL DEFAULT '{}',
    secret_ref TEXT,
    version BIGINT NOT NULL DEFAULT 1 CHECK(version > 0),
    created_by TEXT NOT NULL DEFAULT '',
    updated_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(env, tenant_id, name),
    UNIQUE(env, tenant_id, id)
);

CREATE TABLE exchange_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenant_info(id),
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    transport TEXT NOT NULL,
    configuration JSONB NOT NULL DEFAULT '{}',
    secret_ref TEXT,
    version BIGINT NOT NULL DEFAULT 1 CHECK(version > 0),
    created_by TEXT NOT NULL DEFAULT '',
    updated_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(env, tenant_id, name),
    UNIQUE(env, tenant_id, id)
);

CREATE TABLE strategy_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenant_info(id),
    name TEXT NOT NULL,
    code TEXT NOT NULL DEFAULT '',
    default_parameters JSONB NOT NULL DEFAULT '{}',
    configuration JSONB NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL DEFAULT '',
    updated_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(env, tenant_id, name),
    UNIQUE(env, tenant_id, id)
);

CREATE TABLE notifier_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenant_info(id),
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    transport TEXT NOT NULL,
    configuration JSONB NOT NULL DEFAULT '{}',
    secret_ref TEXT,
    version BIGINT NOT NULL DEFAULT 1 CHECK(version > 0),
    created_by TEXT NOT NULL DEFAULT '',
    updated_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(env, tenant_id, name),
    UNIQUE(env, tenant_id, id)
);

CREATE TABLE exporter_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenant_info(id),
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    transport TEXT NOT NULL,
    configuration JSONB NOT NULL DEFAULT '{}',
    secret_ref TEXT,
    version BIGINT NOT NULL DEFAULT 1 CHECK(version > 0),
    created_by TEXT NOT NULL DEFAULT '',
    updated_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(env, tenant_id, name),
    UNIQUE(env, tenant_id, id)
);

CREATE TABLE workflow_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'STOPPED'
        CHECK(status IN ('PENDING','STARTING','RUNNING','STOPPING','STOPPED','SUCCESS','FAILED')),
    dag_config JSONB NOT NULL DEFAULT '{"nodes":[],"connections":[]}',
    properties JSONB,
    secrets JSONB,
    description TEXT,
    created_by TEXT,
    updated_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    del_flag SMALLINT NOT NULL DEFAULT 0,
    UNIQUE(env, tenant_id, id)
);

CREATE TABLE cluster_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenant_info(id) CHECK(uuid_is_v7(tenant_id)),
    workflow_id UUID,
    name TEXT NOT NULL,
    provider TEXT NOT NULL CHECK(provider IN ('docker','kubernetes')),
    conn_url TEXT NOT NULL,
    auth_config JSONB NOT NULL CHECK(auth_config ? 'auth_type'),
    configuration JSONB NOT NULL CHECK(configuration ? 'runtime_type'),
    middleware JSONB NOT NULL DEFAULT '{"redis":null,"pgvector":null,"timescaledb":null,"emqx":null}',
    labels JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'offline' CHECK(status IN ('online','draining','offline','error')),
    priority INTEGER NOT NULL DEFAULT 100 CHECK(priority BETWEEN 0 AND 10000),
    max_concurrent_workflows INTEGER NOT NULL DEFAULT 16 CHECK(max_concurrent_workflows BETWEEN 1 AND 10000),
    heartbeat_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT NOT NULL,
    UNIQUE(env, tenant_id, id),
    FOREIGN KEY(env, tenant_id, workflow_id)
        REFERENCES workflow_info(env, tenant_id, id)
);

CREATE TABLE ledger_info (
    id UUID PRIMARY KEY DEFAULT uuid_v7() CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    account_id UUID NOT NULL CHECK(uuid_is_v7(account_id)),
    exchange TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('live', 'paper', 'backtest')),
    account_type TEXT,
    portfolio_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    del_flag BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(env, tenant_id, id),
    UNIQUE(env, tenant_id, account_id)
);

CREATE TABLE order_info (
    id UUID NOT NULL DEFAULT uuid_v7() UNIQUE CHECK(uuid_is_v7(id)),
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    wallet_id UUID NOT NULL,
    producer_node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    seq BIGINT NOT NULL CHECK(seq >= 0),
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    intent JSONB NOT NULL,
    reserve_asset TEXT NOT NULL,
    reserve_amount NUMERIC(40,18) NOT NULL CHECK(reserve_amount > 0),
    state TEXT NOT NULL CHECK(state IN (
        'pending','reserved','submitting','acknowledged','unknown','rejected',
        'partially_filled','filled','cancelled'
    )),
    exchange_order_id TEXT,
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    wallet_request_seq BIGINT CHECK(wallet_request_seq >= 0),
    wallet_release_seq BIGINT CHECK(wallet_release_seq >= 0),
    prepared_submission JSONB CHECK(
        prepared_submission IS NULL OR (
            prepared_submission ? 'venue_order_id' AND
            prepared_submission ? 'payload' AND
            octet_length(prepared_submission::text) <= 1048576
        )
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env, tenant_id, wallet_id, producer_node_id, run_id, seq),
    FOREIGN KEY(env, tenant_id, wallet_id)
        REFERENCES ledger_info(env, tenant_id, id),
    UNIQUE(wallet_id, exchange, exchange_order_id)
);

-- Internal execution, recovery and immutable audit storage.

-- Controller-owned remote compute targets. Authentication values are never
-- persisted here: auth_config contains only host environment-variable names.
CREATE UNIQUE INDEX cluster_info_scope_name_idx
    ON cluster_info(env, tenant_id, COALESCE(workflow_id, '00000000-0000-0000-0000-000000000000'::UUID), name);
CREATE INDEX cluster_info_available_idx
    ON cluster_info(env, tenant_id, status, priority, workflow_id)
    WHERE status='online';

CREATE TABLE wf_run (
    run_id UUID PRIMARY KEY CHECK(uuid_is_v7(run_id)),
    workflow_id UUID NOT NULL,
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    cluster_id UUID,
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    state TEXT NOT NULL CHECK(state IN ('queued','running','success','failed','stopped')),
    params JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    replay_of UUID REFERENCES wf_run(run_id),
    FOREIGN KEY(env,tenant_id,cluster_id) REFERENCES cluster_info(env,tenant_id,id),
    UNIQUE(env,tenant_id,run_id),
    FOREIGN KEY(env, tenant_id, workflow_id)
        REFERENCES workflow_info(env, tenant_id, id),
    UNIQUE(env, tenant_id, workflow_id, run_id)
);

ALTER TABLE order_info ADD CONSTRAINT order_run_fk FOREIGN KEY(env,tenant_id,run_id) REFERENCES wf_run(env,tenant_id,run_id);

CREATE TABLE wf_checkpoint (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    workflow_id UUID NOT NULL,
    node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    partition_key TEXT NOT NULL,
    seq BIGINT NOT NULL CHECK(seq >= 0),
    state JSONB NOT NULL,
    watermark TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK(version > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env, tenant_id, workflow_id, node_id, run_id, partition_key),
    FOREIGN KEY(env, tenant_id, workflow_id, run_id)
        REFERENCES wf_run(env, tenant_id, workflow_id, run_id)
);

CREATE TABLE wf_sequence (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    node_id BIGINT NOT NULL,
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    next_seq BIGINT NOT NULL DEFAULT 0 CHECK(next_seq >= 0),
    PRIMARY KEY(env, tenant_id, node_id, run_id)
);

CREATE TABLE wf_timer (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    workflow_id UUID NOT NULL,
    node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    seq BIGINT NOT NULL CHECK(seq >= 0),
    scheduled_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    traceparent TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','delivered')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env, tenant_id, node_id, run_id, seq),
    FOREIGN KEY(env, tenant_id, workflow_id, run_id)
        REFERENCES wf_run(env, tenant_id, workflow_id, run_id)
);
CREATE INDEX wf_timer_due_idx
    ON wf_timer(env, tenant_id, run_id, scheduled_at, node_id, seq)
    WHERE state='pending';

-- Wallet is the only writer of financial facts and projections.

-- Immutable external execution facts retained as the public ledger projection.
CREATE TABLE wlt_ledger (
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    order_id UUID NOT NULL,
    trade_id UUID NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price NUMERIC(40,18) NOT NULL,
    qty NUMERIC(40,18) NOT NULL,
    fee NUMERIC(40,18) NOT NULL DEFAULT 0,
    fee_asset TEXT,
    exchange_order_id TEXT,
    exchange_trade_id TEXT,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(wallet_id, order_id, trade_id),
    CHECK(uuid_is_v7(order_id) AND uuid_is_v7(trade_id))
);
CREATE UNIQUE INDEX wlt_ledger_exchange_execution_idx
    ON wlt_ledger(wallet_id, exchange_trade_id) WHERE exchange_trade_id IS NOT NULL;

-- Rebuildable wallet projections.
CREATE TABLE wlt_balance (
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    asset TEXT NOT NULL,
    available NUMERIC(40,18) NOT NULL DEFAULT 0,
    locked NUMERIC(40,18) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(wallet_id, asset),
    CHECK(available >= 0 AND locked >= 0)
);
CREATE TABLE wlt_position (
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    size NUMERIC(40,18) NOT NULL DEFAULT 0,
    entry_price NUMERIC(40,18) NOT NULL DEFAULT 0,
    realized_pnl NUMERIC(40,18) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(wallet_id, symbol, side)
);

CREATE TABLE wlt_reservation (
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    producer_node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    seq BIGINT NOT NULL CHECK(seq >= 0),
    asset TEXT NOT NULL,
    amount NUMERIC(40,18) NOT NULL CHECK(amount > 0),
    remaining NUMERIC(40,18) NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('reserved', 'settled', 'released')),
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(wallet_id, producer_node_id, run_id, seq),
    CHECK(remaining >= 0 AND remaining <= amount)
);
CREATE TABLE wlt_journal (
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    producer_node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    seq BIGINT NOT NULL CHECK(seq >= 0),
    source TEXT NOT NULL,
    fact_ref TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reversal_of TEXT,
    payload JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY(wallet_id, producer_node_id, run_id, seq),
    UNIQUE(wallet_id, source, fact_ref)
);
CREATE TABLE wlt_posting (
    wallet_id UUID NOT NULL,
    producer_node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    seq BIGINT NOT NULL,
    line_no INTEGER NOT NULL,
    account TEXT NOT NULL,
    asset TEXT NOT NULL,
    amount NUMERIC(40,18) NOT NULL CHECK(amount <> 0),
    PRIMARY KEY(wallet_id, producer_node_id, run_id, seq, line_no),
    FOREIGN KEY(wallet_id, producer_node_id, run_id, seq)
        REFERENCES wlt_journal(wallet_id, producer_node_id, run_id, seq)
);

CREATE FUNCTION wlt_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'immutable wallet fact; append a compensating journal';
END $$;
CREATE TRIGGER wlt_ledger_immutable BEFORE UPDATE OR DELETE ON wlt_ledger
    FOR EACH ROW EXECUTE FUNCTION wlt_reject_mutation();
CREATE TRIGGER wlt_journal_immutable BEFORE UPDATE OR DELETE ON wlt_journal
    FOR EACH ROW EXECUTE FUNCTION wlt_reject_mutation();
CREATE TRIGGER wlt_posting_immutable BEFORE UPDATE OR DELETE ON wlt_posting
    FOR EACH ROW EXECUTE FUNCTION wlt_reject_mutation();

CREATE FUNCTION wlt_check_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM wlt_posting WHERE wallet_id=NEW.wallet_id
        AND producer_node_id=NEW.producer_node_id AND run_id=NEW.run_id AND seq=NEW.seq
    ) OR EXISTS (
        SELECT asset FROM wlt_posting WHERE wallet_id=NEW.wallet_id
        AND producer_node_id=NEW.producer_node_id AND run_id=NEW.run_id AND seq=NEW.seq
        GROUP BY asset HAVING SUM(amount) <> 0
    ) THEN
        RAISE EXCEPTION 'wallet journal must balance per asset';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER wlt_journal_balanced AFTER INSERT ON wlt_journal
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wlt_check_balanced();
CREATE CONSTRAINT TRIGGER wlt_posting_balanced AFTER INSERT ON wlt_posting
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wlt_check_balanced();

CREATE INDEX wlt_wallet_tenant_idx ON ledger_info(tenant_id, env);
CREATE INDEX wlt_wallet_exchange_idx ON ledger_info(exchange, mode);
CREATE INDEX wlt_ledger_order_idx ON wlt_ledger(wallet_id, order_id);
CREATE INDEX wlt_ledger_symbol_time_idx ON wlt_ledger(wallet_id, symbol, ts);
CREATE INDEX wlt_position_symbol_idx ON wlt_position(symbol);

CREATE TABLE msg_inbox (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    consumer TEXT NOT NULL,
    node_id BIGINT NOT NULL,
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    seq BIGINT NOT NULL CHECK(seq >= 0),
    topic TEXT NOT NULL,
    payload JSONB NOT NULL CHECK(NOT(payload ? 'requestSpanId')),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','done','quarantined')),
    quarantine_reason TEXT CHECK(quarantine_reason IS NULL OR octet_length(quarantine_reason) <= 1024),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY(env, tenant_id, consumer, node_id, run_id, seq)
);
CREATE INDEX msg_inbox_pending_idx
    ON msg_inbox(consumer, received_at) WHERE state='pending';

CREATE TABLE msg_outbox (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    producer TEXT NOT NULL,
    node_id BIGINT NOT NULL,
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    seq BIGINT NOT NULL CHECK(seq >= 0),
    topic TEXT NOT NULL,
    payload JSONB NOT NULL CHECK(NOT(payload ? 'requestSpanId')),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','sent','quarantined')),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    PRIMARY KEY(env, tenant_id, producer, node_id, run_id, seq)
);
CREATE INDEX msg_outbox_pending_idx
    ON msg_outbox(producer, available_at) WHERE state='pending';

CREATE TABLE mkt_event (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('market','news')),
    event_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    seq BIGINT NOT NULL CHECK(seq >= 0),
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
    payload JSONB NOT NULL,
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    PRIMARY KEY(env, tenant_id, run_id, source, source_event_id, revision),
    CHECK(available_at >= event_at)
);
CREATE INDEX mkt_event_history_idx
    ON mkt_event(env, tenant_id, source, symbol, event_at, available_at);
CREATE INDEX mkt_event_run_idx
    ON mkt_event(env, tenant_id, run_id, available_at, source, source_event_id, revision);

CREATE TABLE mkt_delivery (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    workflow_id UUID NOT NULL,
    node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    message_seq BIGINT NOT NULL CHECK(message_seq >= 0),
    payload JSONB NOT NULL CHECK(NOT(payload ? 'requestSpanId')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env, tenant_id, node_id, run_id, source, source_event_id, revision),
    UNIQUE(env, tenant_id, node_id, run_id, message_seq),
    FOREIGN KEY(env, tenant_id, workflow_id, run_id)
        REFERENCES wf_run(env, tenant_id, workflow_id, run_id),
    FOREIGN KEY(env, tenant_id, run_id, source, source_event_id, revision)
        REFERENCES mkt_event(env, tenant_id, run_id, source, source_event_id, revision)
);
CREATE INDEX mkt_delivery_history_idx
    ON mkt_delivery(env, tenant_id, run_id, source, source_event_id, revision);

CREATE TABLE eval_case (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    source TEXT NOT NULL,
    case_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    facts JSONB NOT NULL,
    inference JSONB NOT NULL,
    provenance JSONB NOT NULL,
    model_version TEXT NOT NULL,
    PRIMARY KEY(env, tenant_id, source, case_id),
    CHECK(available_at >= observed_at)
);

CREATE TABLE eval_snapshot (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    workflow_id UUID NOT NULL,
    node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    version BIGINT NOT NULL CHECK(version > 0),
    as_of TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    probabilities JSONB NOT NULL,
    parameters JSONB NOT NULL,
    model_version TEXT NOT NULL,
    case_refs JSONB NOT NULL,
    facts JSONB NOT NULL DEFAULT '[]',
    inferences JSONB NOT NULL DEFAULT '[]',
    classification_version TEXT NOT NULL,
    PRIMARY KEY(env, tenant_id, workflow_id, node_id, run_id, version),
    FOREIGN KEY(env, tenant_id, workflow_id, run_id)
        REFERENCES wf_run(env, tenant_id, workflow_id, run_id),
    CHECK(valid_until > as_of),
    CHECK(available_at >= as_of)
);
CREATE INDEX eval_snapshot_replay_idx
    ON eval_snapshot(env, tenant_id, run_id, available_at, node_id, version);

CREATE TABLE bt_checkpoint (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    configuration JSONB NOT NULL,
    frozen_at TIMESTAMPTZ NOT NULL,
    cursor JSONB,
    pending_message JSONB,
    virtual_time TIMESTAMPTZ NOT NULL,
    steps BIGINT NOT NULL DEFAULT 0 CHECK(steps >= 0),
    state TEXT NOT NULL DEFAULT 'running' CHECK(state IN ('running','complete','failed')),
    PRIMARY KEY(env, tenant_id, run_id)
);

CREATE TABLE bt_order (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    account_id UUID NOT NULL CHECK(uuid_is_v7(account_id)),
    producer_node_id BIGINT NOT NULL,
    matching_node_id BIGINT NOT NULL CHECK(matching_node_id > 0),
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    seq BIGINT NOT NULL CHECK(seq >= 0),
    exchange_order_id TEXT NOT NULL,
    intent JSONB NOT NULL,
    original_message JSONB NOT NULL CHECK(NOT(original_message ? 'requestSpanId')),
    matching_configuration JSONB NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('accepted','filled','cancelled','rejected')),
    accepted_at TIMESTAMPTZ NOT NULL,
    matched_at TIMESTAMPTZ,
    PRIMARY KEY(env, tenant_id, account_id, producer_node_id, run_id, seq),
    UNIQUE(env, tenant_id, account_id, exchange_order_id)
);
CREATE INDEX bt_order_resting_idx
    ON bt_order(env, tenant_id, matching_node_id, run_id, accepted_at, producer_node_id, seq, account_id)
    WHERE state='accepted';

CREATE UNIQUE INDEX ord_order_prepared_venue_idx
    ON order_info(wallet_id, exchange, (prepared_submission->>'venue_order_id'))
    WHERE prepared_submission IS NOT NULL;

CREATE FUNCTION ord_reject_prepared_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.prepared_submission IS NOT NULL
       AND NEW.prepared_submission IS DISTINCT FROM OLD.prepared_submission THEN
        RAISE EXCEPTION 'prepared venue submission is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER ord_order_prepared_immutable BEFORE UPDATE OF prepared_submission ON order_info
    FOR EACH ROW EXECUTE FUNCTION ord_reject_prepared_mutation();

CREATE TABLE ord_execution (
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    exchange TEXT NOT NULL,
    exchange_trade_id TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL,
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    seq BIGINT NOT NULL CHECK(seq >= 0),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
    price NUMERIC(40,18) NOT NULL CHECK(price > 0),
    quantity NUMERIC(40,18) NOT NULL CHECK(quantity > 0),
    fee NUMERIC(40,18) NOT NULL CHECK(fee >= 0),
    fee_asset TEXT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL,
    producer_node_id BIGINT NOT NULL CHECK(producer_node_id > 0),
    reservation_node_id BIGINT NOT NULL CHECK(reservation_node_id > 0),
    reservation_seq BIGINT NOT NULL CHECK(reservation_seq >= 0),
    order_complete BOOLEAN NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY(wallet_id, exchange, exchange_trade_id)
);
CREATE INDEX ord_execution_intent_idx
    ON ord_execution(wallet_id, reservation_node_id, run_id, reservation_seq);
CREATE TRIGGER ord_execution_immutable BEFORE UPDATE OR DELETE ON ord_execution
    FOR EACH ROW EXECUTE FUNCTION wlt_reject_mutation();

CREATE TABLE ord_execution_group (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    producer_node_id BIGINT NOT NULL,
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    seq BIGINT NOT NULL CHECK(seq >= 0),
    workflow_id UUID NOT NULL,
    coordinator_node_id BIGINT NOT NULL,
    leg_count INTEGER NOT NULL CHECK(leg_count > 0),
    deadline TIMESTAMPTZ NOT NULL,
    failure_policy TEXT NOT NULL CHECK(failure_policy IN ('hold','compensate_filled')),
    traceparent TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'pending','executing','reconciling','compensating','completed','held',
        'compensated','compensation_failed'
    )),
    legs JSONB NOT NULL DEFAULT '{}',
    compensation JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env, tenant_id, producer_node_id, run_id, seq),
    FOREIGN KEY(env, tenant_id, workflow_id, run_id)
        REFERENCES wf_run(env, tenant_id, workflow_id, run_id)
);
CREATE TABLE ord_execution_leg (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    producer_node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    group_seq BIGINT NOT NULL,
    leg_index INTEGER NOT NULL,
    order_node_id BIGINT NOT NULL,
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    intent_seq BIGINT NOT NULL,
    intent JSONB NOT NULL,
    rejected BOOLEAN NOT NULL DEFAULT FALSE,
    compensation_seq BIGINT,
    compensation_intent JSONB,
    PRIMARY KEY(env, tenant_id, producer_node_id, run_id, group_seq, leg_index),
    FOREIGN KEY(env, tenant_id, producer_node_id, run_id, group_seq)
        REFERENCES ord_execution_group(env, tenant_id, producer_node_id, run_id, seq)
);
CREATE INDEX ord_execution_group_pending_idx
    ON ord_execution_group(env, tenant_id, coordinator_node_id, updated_at)
    WHERE state NOT IN ('completed','held','compensated','compensation_failed');

CREATE TABLE ord_venue_checkpoint (
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    producer_node_id BIGINT NOT NULL,
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    seq BIGINT NOT NULL CHECK(seq >= 0),
    version BIGINT NOT NULL DEFAULT 0 CHECK(version >= 0),
    cursor TEXT,
    complete BOOLEAN NOT NULL DEFAULT FALSE,
    next_poll_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(wallet_id, producer_node_id, run_id, seq)
);
CREATE TABLE ord_venue_observation (
    wallet_id UUID NOT NULL REFERENCES ledger_info(id),
    exchange TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('execution','status')),
    venue_reference TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL,
    payload JSONB NOT NULL CHECK(octet_length(payload::text) <= 262144),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(wallet_id, exchange, kind, venue_reference)
);
CREATE INDEX ord_venue_observation_order_idx
    ON ord_venue_observation(wallet_id, exchange, exchange_order_id, kind);
CREATE FUNCTION ord_reject_venue_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'venue observations are immutable; append an authoritative correction';
END $$;
CREATE TRIGGER ord_venue_observation_immutable
    BEFORE UPDATE OR DELETE ON ord_venue_observation
    FOR EACH ROW EXECUTE FUNCTION ord_reject_venue_mutation();

CREATE TABLE log_chunk (
    cursor BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    workflow_id UUID NOT NULL,
    node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    first_seq BIGINT NOT NULL CHECK(first_seq >= 0),
    last_seq BIGINT NOT NULL,
    object_key TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL,
    byte_length BIGINT NOT NULL CHECK(byte_length > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env, tenant_id, workflow_id, node_id, run_id, first_seq),
    FOREIGN KEY(env, tenant_id, workflow_id, run_id)
        REFERENCES wf_run(env, tenant_id, workflow_id, run_id),
    CHECK(last_seq >= first_seq)
);
CREATE TABLE ntf_delivery (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    workflow_id UUID NOT NULL,
    owner_node_id BIGINT NOT NULL,
    producer_node_id BIGINT NOT NULL,
    run_id UUID NOT NULL,
    seq BIGINT NOT NULL CHECK(seq >= 0),
    channel TEXT NOT NULL CHECK(channel IN ('email','telegram','in_app')),
    recipient TEXT NOT NULL,
    template_version TEXT NOT NULL,
    payload JSONB NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','sending','delivered','rejected','unknown')),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,
    provider_receipt TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env, tenant_id, producer_node_id, run_id, seq, channel, recipient),
    FOREIGN KEY(env, tenant_id, workflow_id, run_id)
        REFERENCES wf_run(env, tenant_id, workflow_id, run_id)
);
CREATE INDEX ntf_delivery_pending_idx
    ON ntf_delivery(env, tenant_id, workflow_id, owner_node_id, state, available_at);

CREATE TABLE exp_checkpoint (
    env TEXT NOT NULL,
    tenant_id UUID NOT NULL CHECK(uuid_is_v7(tenant_id)),
    connection_id UUID NOT NULL,
    run_id UUID NOT NULL REFERENCES wf_run(run_id),
    partition_key TEXT NOT NULL,
    seq BIGINT NOT NULL CHECK(seq >= 0),
    cursor JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(env, tenant_id, connection_id, run_id, partition_key),
    FOREIGN KEY(env, tenant_id, connection_id)
        REFERENCES exporter_info(env, tenant_id, id)
);
