-- Core owns user read receipts; Notification remains the sole delivery-state writer.
CREATE TABLE IF NOT EXISTS notification_read (
    env TEXT NOT NULL, tenant_id BIGINT NOT NULL, space_id BIGINT NOT NULL,
    run_id TEXT NOT NULL, producer_node_id BIGINT NOT NULL, seq BIGINT NOT NULL, channel TEXT NOT NULL,
    recipient TEXT NOT NULL, read_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(env,tenant_id,space_id,run_id,producer_node_id,seq,channel,recipient),
    FOREIGN KEY(env,tenant_id,space_id,run_id,producer_node_id,seq,channel,recipient)
        REFERENCES notification_delivery(env,tenant_id,space_id,run_id,producer_node_id,seq,channel,recipient)
);
