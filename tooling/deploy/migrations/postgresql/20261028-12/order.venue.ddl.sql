-- Order owns venue polling progress; accepted orders survive host restarts.
CREATE TABLE order_venue_checkpoint (
    wallet_id BIGINT NOT NULL REFERENCES s_wallets(id),
    producer_node_id BIGINT NOT NULL,
    run_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    cursor TEXT,
    complete BOOLEAN NOT NULL DEFAULT FALSE,
    next_poll_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(wallet_id, producer_node_id, run_id, seq)
);
CREATE TABLE order_venue_observation (
    wallet_id BIGINT NOT NULL REFERENCES s_wallets(id),
    exchange TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('execution','status')),
    venue_reference TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL,
    payload JSONB NOT NULL CHECK(octet_length(payload::text)<=262144),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(wallet_id, exchange, kind, venue_reference)
);
CREATE INDEX order_venue_observation_order_idx
    ON order_venue_observation(wallet_id,exchange,exchange_order_id,kind);
