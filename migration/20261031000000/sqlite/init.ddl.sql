PRAGMA foreign_keys = ON;

CREATE TABLE cfg_connection (
    id TEXT PRIMARY KEY CHECK(length(id)=36 AND substr(id,15,1)='7'),
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL CHECK(length(tenant_id)=36 AND substr(tenant_id,15,1)='7'),
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    transport TEXT NOT NULL,
    configuration TEXT NOT NULL CHECK(json_extract(configuration,'$.kind') IS NOT NULL),
    secret_ref TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(env, tenant_id, name),
    UNIQUE(env, tenant_id, id)
);

CREATE TABLE wf_workflow (
    id TEXT PRIMARY KEY CHECK(length(id)=36 AND substr(id,15,1)='7'),
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL CHECK(length(tenant_id)=36 AND substr(tenant_id,15,1)='7'),
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'STOPPED'
        CHECK(status IN ('PENDING','STARTING','RUNNING','STOPPING','STOPPED','SUCCESS','FAILED')),
    flow_json TEXT NOT NULL DEFAULT '{"nodes":[],"connections":[]}',
    properties TEXT,
    secrets TEXT,
    description TEXT,
    created_by TEXT,
    updated_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    del_flag INTEGER NOT NULL DEFAULT 0,
    UNIQUE(env, tenant_id, id)
);

CREATE TABLE ctl_worker (
    id TEXT PRIMARY KEY CHECK(length(id)=36 AND substr(id,15,1)='7'),
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL CHECK(length(tenant_id)=36 AND substr(tenant_id,15,1)='7'),
    workflow_id TEXT CHECK(workflow_id IS NULL OR (length(workflow_id)=36 AND substr(workflow_id,15,1)='7')),
    name TEXT NOT NULL,
    provider TEXT NOT NULL CHECK(provider IN ('docker','kubernetes')),
    conn_url TEXT NOT NULL,
    auth_config TEXT NOT NULL CHECK(json_extract(auth_config,'$.auth_type') IS NOT NULL),
    configuration TEXT NOT NULL CHECK(json_extract(configuration,'$.runtime_type') IS NOT NULL),
    middleware TEXT NOT NULL DEFAULT '{"redis":null,"pgvector":null,"timescaledb":null,"emqx":null}' CHECK(json_valid(middleware)),
    labels TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'offline' CHECK(status IN ('online','draining','offline','error')),
    priority INTEGER NOT NULL DEFAULT 100 CHECK(priority BETWEEN 0 AND 10000),
    max_concurrent_workflows INTEGER NOT NULL DEFAULT 16 CHECK(max_concurrent_workflows BETWEEN 1 AND 10000),
    heartbeat_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT NOT NULL,
    FOREIGN KEY(env, tenant_id, workflow_id)
        REFERENCES wf_workflow(env, tenant_id, id)
);
CREATE UNIQUE INDEX ctl_worker_scope_name_idx
    ON ctl_worker(env, tenant_id, IFNULL(workflow_id, ''), name);
CREATE INDEX ctl_worker_available_idx
    ON ctl_worker(env, tenant_id, status, priority, workflow_id)
    WHERE status='online';

CREATE TABLE wf_run (
    run_id TEXT PRIMARY KEY CHECK(length(run_id)=36 AND substr(run_id,15,1)='7'),
    workflow_id TEXT NOT NULL CHECK(length(workflow_id)=36 AND substr(workflow_id,15,1)='7'),
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL CHECK(length(tenant_id)=36 AND substr(tenant_id,15,1)='7'),
    worker_id TEXT REFERENCES ctl_worker(id),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    state TEXT NOT NULL CHECK(state IN ('queued','running','success','failed','stopped')),
    params TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at TEXT,
    replay_of TEXT REFERENCES wf_run(run_id),
    FOREIGN KEY(env, tenant_id, workflow_id)
        REFERENCES wf_workflow(env, tenant_id, id),
    UNIQUE(env, tenant_id, workflow_id, run_id)
);
CREATE INDEX wf_run_scope_idx
    ON wf_run(env, tenant_id, workflow_id, started_at);


CREATE TABLE wlt_wallet (
    id TEXT PRIMARY KEY CHECK(length(id)=36 AND substr(id,15,1)='7'),
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL CHECK(length(tenant_id)=36 AND substr(tenant_id,15,1)='7'),
    account_id TEXT NOT NULL CHECK(length(account_id)=36 AND substr(account_id,15,1)='7'),
    exchange TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('live','paper','backtest')),
    account_type TEXT,
    portfolio_version INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    del_flag INTEGER NOT NULL DEFAULT 0,
    UNIQUE(env, tenant_id, id),
    UNIQUE(env, tenant_id, account_id)
);
CREATE TABLE wlt_ledger (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    order_id TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
    price TEXT NOT NULL,
    qty TEXT NOT NULL,
    fee TEXT NOT NULL DEFAULT '0',
    fee_asset TEXT,
    exchange_order_id TEXT,
    exchange_trade_id TEXT,
    ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(wallet_id, order_id, trade_id),
    CHECK(length(order_id)=36 AND substr(order_id,15,1)='7'),
    CHECK(length(trade_id)=36 AND substr(trade_id,15,1)='7')
);
CREATE TABLE wlt_balance (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    asset TEXT NOT NULL,
    available TEXT NOT NULL DEFAULT '0',
    locked TEXT NOT NULL DEFAULT '0',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(wallet_id, asset)
);
CREATE TABLE wlt_position (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('LONG','SHORT')),
    size TEXT NOT NULL DEFAULT '0',
    entry_price TEXT NOT NULL DEFAULT '0',
    realized_pnl TEXT NOT NULL DEFAULT '0',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(wallet_id, symbol, side)
);
CREATE TABLE wlt_equity_snapshot (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    equity TEXT NOT NULL,
    ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(wallet_id, ts)
);
CREATE INDEX wlt_wallet_tenant_idx ON wlt_wallet(tenant_id, env);
CREATE INDEX wlt_wallet_exchange_idx ON wlt_wallet(exchange, mode);
CREATE INDEX wlt_ledger_order_idx ON wlt_ledger(wallet_id, order_id);
CREATE INDEX wlt_ledger_symbol_time_idx ON wlt_ledger(wallet_id, symbol, ts);
CREATE INDEX wlt_position_symbol_idx ON wlt_position(symbol);
CREATE INDEX wlt_equity_snapshot_time_idx ON wlt_equity_snapshot(wallet_id, ts);

CREATE TABLE wf_checkpoint (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK(seq >= 0),
    state TEXT NOT NULL CHECK(json_valid(state)),
    watermark TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version > 0),
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,workflow_id,node_id,run_id,partition_key),
    FOREIGN KEY(env,tenant_id,workflow_id,run_id) REFERENCES wf_run(env,tenant_id,workflow_id,run_id)
);

CREATE TABLE wf_sequence (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    next_seq INTEGER NOT NULL DEFAULT 0 CHECK(next_seq >= 0),
    PRIMARY KEY(env,tenant_id,node_id,run_id)
);

CREATE TABLE wf_timer (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK(seq >= 0),
    scheduled_at TEXT NOT NULL,
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    traceparent TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','delivered')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,node_id,run_id,seq),
    FOREIGN KEY(env,tenant_id,workflow_id,run_id) REFERENCES wf_run(env,tenant_id,workflow_id,run_id)
);
CREATE INDEX wf_timer_due_idx ON wf_timer(env,tenant_id,run_id,scheduled_at,node_id,seq) WHERE state='pending';

CREATE TABLE wlt_reservation (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    producer_node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK(seq >= 0),
    asset TEXT NOT NULL,
    amount NUMERIC NOT NULL CHECK(amount > 0),
    remaining NUMERIC NOT NULL CHECK(remaining >= 0 AND remaining <= amount),
    state TEXT NOT NULL CHECK(state IN ('reserved','settled','released')),
    expires_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(wallet_id,producer_node_id,run_id,seq)
);

CREATE TABLE wlt_journal (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    producer_node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK(seq >= 0),
    source TEXT NOT NULL,
    fact_ref TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reversal_of TEXT,
    payload TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(payload)),
    PRIMARY KEY(wallet_id,producer_node_id,run_id,seq),
    UNIQUE(wallet_id,source,fact_ref)
);

CREATE TABLE wlt_posting (
    wallet_id TEXT NOT NULL,
    producer_node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    line_no INTEGER NOT NULL,
    account TEXT NOT NULL,
    asset TEXT NOT NULL,
    amount NUMERIC NOT NULL CHECK(amount <> 0),
    PRIMARY KEY(wallet_id,producer_node_id,run_id,seq,line_no),
    FOREIGN KEY(wallet_id,producer_node_id,run_id,seq)
        REFERENCES wlt_journal(wallet_id,producer_node_id,run_id,seq)
);

CREATE TRIGGER wlt_ledger_update_immutable BEFORE UPDATE ON wlt_ledger BEGIN
    SELECT RAISE(ABORT,'immutable wallet fact; append a compensating journal');
END;
CREATE TRIGGER wlt_ledger_delete_immutable BEFORE DELETE ON wlt_ledger BEGIN
    SELECT RAISE(ABORT,'immutable wallet fact; append a compensating journal');
END;
CREATE TRIGGER wlt_journal_update_immutable BEFORE UPDATE ON wlt_journal BEGIN
    SELECT RAISE(ABORT,'immutable wallet fact; append a compensating journal');
END;
CREATE TRIGGER wlt_journal_delete_immutable BEFORE DELETE ON wlt_journal BEGIN
    SELECT RAISE(ABORT,'immutable wallet fact; append a compensating journal');
END;
CREATE TRIGGER wlt_posting_update_immutable BEFORE UPDATE ON wlt_posting BEGIN
    SELECT RAISE(ABORT,'immutable wallet fact; append a compensating journal');
END;
CREATE TRIGGER wlt_posting_delete_immutable BEFORE DELETE ON wlt_posting BEGIN
    SELECT RAISE(ABORT,'immutable wallet fact; append a compensating journal');
END;

CREATE TABLE msg_inbox (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    consumer TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    topic TEXT NOT NULL,
    payload TEXT NOT NULL CHECK(json_valid(payload) AND json_extract(payload,'$.requestSpanId') IS NULL),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','done','quarantined')),
    quarantine_reason TEXT CHECK(quarantine_reason IS NULL OR length(CAST(quarantine_reason AS BLOB)) <= 1024),
    received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    PRIMARY KEY(env,tenant_id,consumer,node_id,run_id,seq)
);
CREATE INDEX msg_inbox_pending_idx ON msg_inbox(consumer,received_at) WHERE state='pending';

CREATE TABLE msg_outbox (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    producer TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    topic TEXT NOT NULL,
    payload TEXT NOT NULL CHECK(json_valid(payload) AND json_extract(payload,'$.requestSpanId') IS NULL),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','sent','quarantined')),
    available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TEXT,
    PRIMARY KEY(env,tenant_id,producer,node_id,run_id,seq)
);
CREATE INDEX msg_outbox_pending_idx ON msg_outbox(producer,available_at) WHERE state='pending';

CREATE TABLE mkt_event (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('market','news')),
    event_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    seq INTEGER NOT NULL CHECK(seq >= 0),
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    PRIMARY KEY(env,tenant_id,run_id,source,source_event_id,revision),
    CHECK(available_at >= event_at)
);
CREATE INDEX mkt_event_history_idx ON mkt_event(env,tenant_id,source,symbol,event_at,available_at);
CREATE INDEX mkt_event_run_idx ON mkt_event(env,tenant_id,run_id,available_at,source,source_event_id,revision);

CREATE TABLE mkt_delivery (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    message_seq INTEGER NOT NULL CHECK(message_seq >= 0),
    payload TEXT NOT NULL CHECK(json_valid(payload) AND json_extract(payload,'$.requestSpanId') IS NULL),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,node_id,run_id,source,source_event_id,revision),
    UNIQUE(env,tenant_id,node_id,run_id,message_seq),
    FOREIGN KEY(env,tenant_id,workflow_id,run_id) REFERENCES wf_run(env,tenant_id,workflow_id,run_id),
    FOREIGN KEY(env,tenant_id,run_id,source,source_event_id,revision)
        REFERENCES mkt_event(env,tenant_id,run_id,source,source_event_id,revision)
);
CREATE INDEX mkt_delivery_history_idx ON mkt_delivery(env,tenant_id,run_id,source,source_event_id,revision);

CREATE TABLE eval_case (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source TEXT NOT NULL,
    case_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    facts TEXT NOT NULL CHECK(json_valid(facts)),
    inference TEXT NOT NULL CHECK(json_valid(inference)),
    provenance TEXT NOT NULL CHECK(json_valid(provenance)),
    model_version TEXT NOT NULL,
    PRIMARY KEY(env,tenant_id,source,case_id),
    CHECK(available_at >= observed_at)
);

CREATE TABLE eval_snapshot (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version > 0),
    as_of TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    available_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    probabilities TEXT NOT NULL CHECK(json_valid(probabilities)),
    parameters TEXT NOT NULL CHECK(json_valid(parameters)),
    model_version TEXT NOT NULL,
    case_refs TEXT NOT NULL CHECK(json_valid(case_refs)),
    facts TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(facts)),
    inferences TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(inferences)),
    classification_version TEXT NOT NULL,
    PRIMARY KEY(env,tenant_id,workflow_id,node_id,run_id,version),
    FOREIGN KEY(env,tenant_id,workflow_id,run_id) REFERENCES wf_run(env,tenant_id,workflow_id,run_id),
    CHECK(valid_until > as_of AND available_at >= as_of)
);
CREATE INDEX eval_snapshot_replay_idx ON eval_snapshot(env,tenant_id,run_id,available_at,node_id,version);

CREATE TABLE bt_checkpoint (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    configuration TEXT NOT NULL CHECK(json_valid(configuration)),
    frozen_at TEXT NOT NULL,
    cursor TEXT CHECK(cursor IS NULL OR json_valid(cursor)),
    pending_message TEXT CHECK(pending_message IS NULL OR json_valid(pending_message)),
    virtual_time TEXT NOT NULL,
    steps INTEGER NOT NULL DEFAULT 0 CHECK(steps >= 0),
    state TEXT NOT NULL DEFAULT 'running' CHECK(state IN ('running','complete','failed')),
    PRIMARY KEY(env,tenant_id,run_id)
);

CREATE TABLE bt_order (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    producer_node_id INTEGER NOT NULL,
    matching_node_id INTEGER NOT NULL CHECK(matching_node_id > 0),
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    exchange_order_id TEXT NOT NULL,
    intent TEXT NOT NULL CHECK(json_valid(intent)),
    original_message TEXT NOT NULL CHECK(json_valid(original_message) AND json_extract(original_message,'$.requestSpanId') IS NULL),
    matching_configuration TEXT NOT NULL CHECK(json_valid(matching_configuration)),
    state TEXT NOT NULL CHECK(state IN ('accepted','filled','cancelled','rejected')),
    accepted_at TEXT NOT NULL,
    matched_at TEXT,
    PRIMARY KEY(env,tenant_id,account_id,producer_node_id,run_id,seq),
    UNIQUE(env,tenant_id,account_id,exchange_order_id)
);
CREATE INDEX bt_order_resting_idx
    ON bt_order(env,tenant_id,matching_node_id,run_id,accepted_at,producer_node_id,seq,account_id)
    WHERE state='accepted';

CREATE TABLE ord_order (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    producer_node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    intent TEXT NOT NULL CHECK(json_valid(intent)),
    reserve_asset TEXT NOT NULL,
    reserve_amount NUMERIC NOT NULL CHECK(reserve_amount > 0),
    state TEXT NOT NULL CHECK(state IN ('pending','reserved','submitting','acknowledged','unknown','rejected','partially_filled','filled','cancelled')),
    exchange_order_id TEXT,
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    wallet_request_seq INTEGER CHECK(wallet_request_seq >= 0),
    wallet_release_seq INTEGER CHECK(wallet_release_seq >= 0),
    prepared_submission TEXT CHECK(prepared_submission IS NULL OR (
        json_valid(prepared_submission)
        AND json_extract(prepared_submission,'$.venue_order_id') IS NOT NULL
        AND json_extract(prepared_submission,'$.payload') IS NOT NULL
        AND length(CAST(prepared_submission AS BLOB)) <= 1048576
    )),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,wallet_id,producer_node_id,run_id,seq),
    FOREIGN KEY(env,tenant_id,wallet_id) REFERENCES wlt_wallet(env,tenant_id,id),
    UNIQUE(wallet_id,exchange,exchange_order_id)
);
CREATE UNIQUE INDEX ord_order_prepared_venue_idx
    ON ord_order(wallet_id,exchange,json_extract(prepared_submission,'$.venue_order_id'))
    WHERE prepared_submission IS NOT NULL;
CREATE TRIGGER ord_order_prepared_immutable BEFORE UPDATE OF prepared_submission ON ord_order
WHEN OLD.prepared_submission IS NOT NULL AND NEW.prepared_submission IS NOT OLD.prepared_submission BEGIN
    SELECT RAISE(ABORT,'prepared venue submission is immutable');
END;

CREATE TABLE ord_execution (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    exchange TEXT NOT NULL,
    exchange_trade_id TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
    price NUMERIC NOT NULL CHECK(price > 0),
    quantity NUMERIC NOT NULL CHECK(quantity > 0),
    fee NUMERIC NOT NULL CHECK(fee >= 0),
    fee_asset TEXT NOT NULL,
    executed_at TEXT NOT NULL,
    producer_node_id INTEGER NOT NULL CHECK(producer_node_id > 0),
    reservation_node_id INTEGER NOT NULL CHECK(reservation_node_id > 0),
    reservation_seq INTEGER NOT NULL CHECK(reservation_seq >= 0),
    order_complete INTEGER NOT NULL CHECK(order_complete IN (0,1)),
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    PRIMARY KEY(wallet_id,exchange,exchange_trade_id)
);
CREATE INDEX ord_execution_intent_idx ON ord_execution(wallet_id,reservation_node_id,run_id,reservation_seq);
CREATE TRIGGER ord_execution_update_immutable BEFORE UPDATE ON ord_execution BEGIN
    SELECT RAISE(ABORT,'immutable execution fact');
END;
CREATE TRIGGER ord_execution_delete_immutable BEFORE DELETE ON ord_execution BEGIN
    SELECT RAISE(ABORT,'immutable execution fact');
END;

CREATE TABLE ord_execution_group (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    producer_node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    workflow_id TEXT NOT NULL,
    coordinator_node_id INTEGER NOT NULL,
    leg_count INTEGER NOT NULL CHECK(leg_count > 0),
    deadline TEXT NOT NULL,
    failure_policy TEXT NOT NULL CHECK(failure_policy IN ('hold','compensate_filled')),
    traceparent TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','executing','reconciling','compensating','completed','held','compensated','compensation_failed')),
    legs TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(legs)),
    compensation TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(compensation)),
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,producer_node_id,run_id,seq),
    FOREIGN KEY(env,tenant_id,workflow_id,run_id) REFERENCES wf_run(env,tenant_id,workflow_id,run_id)
);

CREATE TABLE ord_execution_leg (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    producer_node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    group_seq INTEGER NOT NULL,
    leg_index INTEGER NOT NULL,
    order_node_id INTEGER NOT NULL,
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    intent_seq INTEGER NOT NULL,
    intent TEXT NOT NULL CHECK(json_valid(intent)),
    rejected INTEGER NOT NULL DEFAULT 0 CHECK(rejected IN (0,1)),
    compensation_seq INTEGER,
    compensation_intent TEXT CHECK(compensation_intent IS NULL OR json_valid(compensation_intent)),
    PRIMARY KEY(env,tenant_id,producer_node_id,run_id,group_seq,leg_index),
    FOREIGN KEY(env,tenant_id,producer_node_id,run_id,group_seq)
        REFERENCES ord_execution_group(env,tenant_id,producer_node_id,run_id,seq)
);
CREATE INDEX ord_execution_group_pending_idx
    ON ord_execution_group(env,tenant_id,coordinator_node_id,updated_at)
    WHERE state NOT IN ('completed','held','compensated','compensation_failed');

CREATE TABLE ord_venue_checkpoint (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    producer_node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    cursor TEXT,
    complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1)),
    next_poll_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(wallet_id,producer_node_id,run_id,seq)
);

CREATE TABLE ord_venue_observation (
    wallet_id TEXT NOT NULL REFERENCES wlt_wallet(id),
    exchange TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('execution','status')),
    venue_reference TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL,
    payload TEXT NOT NULL CHECK(json_valid(payload) AND length(CAST(payload AS BLOB)) <= 262144),
    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(wallet_id,exchange,kind,venue_reference)
);
CREATE INDEX ord_venue_observation_order_idx ON ord_venue_observation(wallet_id,exchange,exchange_order_id,kind);
CREATE TRIGGER ord_venue_observation_update_immutable BEFORE UPDATE ON ord_venue_observation BEGIN
    SELECT RAISE(ABORT,'venue observations are immutable');
END;
CREATE TRIGGER ord_venue_observation_delete_immutable BEFORE DELETE ON ord_venue_observation BEGIN
    SELECT RAISE(ABORT,'venue observations are immutable');
END;

CREATE TABLE log_chunk (
    cursor INTEGER NOT NULL UNIQUE,
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    first_seq INTEGER NOT NULL CHECK(first_seq >= 0),
    last_seq INTEGER NOT NULL CHECK(last_seq >= first_seq),
    object_key TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL,
    byte_length INTEGER NOT NULL CHECK(byte_length > 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,workflow_id,node_id,run_id,first_seq),
    FOREIGN KEY(env,tenant_id,workflow_id,run_id) REFERENCES wf_run(env,tenant_id,workflow_id,run_id)
);

CREATE TABLE ntf_delivery (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    owner_node_id INTEGER NOT NULL,
    producer_node_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK(seq >= 0),
    channel TEXT NOT NULL CHECK(channel IN ('email','telegram','in_app')),
    recipient TEXT NOT NULL,
    template_version TEXT NOT NULL,
    payload TEXT NOT NULL CHECK(json_valid(payload)),
    state TEXT NOT NULL CHECK(state IN ('pending','sending','delivered','rejected','unknown')),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK(attempt_no >= 0),
    available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivered_at TEXT,
    provider_receipt TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,producer_node_id,run_id,seq,channel,recipient),
    FOREIGN KEY(env,tenant_id,workflow_id,run_id) REFERENCES wf_run(env,tenant_id,workflow_id,run_id)
);
CREATE INDEX ntf_delivery_pending_idx
    ON ntf_delivery(env,tenant_id,workflow_id,owner_node_id,state,available_at);

CREATE TABLE ntf_read (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    producer_node_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    channel TEXT NOT NULL,
    recipient TEXT NOT NULL,
    read_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,run_id,producer_node_id,seq,channel,recipient),
    FOREIGN KEY(env,tenant_id,producer_node_id,run_id,seq,channel,recipient)
        REFERENCES ntf_delivery(env,tenant_id,producer_node_id,run_id,seq,channel,recipient)
);

CREATE TABLE exp_checkpoint (
    env TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES wf_run(run_id),
    partition_key TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK(seq >= 0),
    cursor TEXT NOT NULL CHECK(json_valid(cursor)),
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(env,tenant_id,connection_id,run_id,partition_key),
    FOREIGN KEY(env,tenant_id,connection_id) REFERENCES cfg_connection(env,tenant_id,id)
);
