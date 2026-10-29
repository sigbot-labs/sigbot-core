-- Persist the actual Wallet request sequence, not an independent correlation ID.
ALTER TABLE order_record ADD COLUMN IF NOT EXISTS wallet_request_seq BIGINT;
ALTER TABLE order_record ADD COLUMN IF NOT EXISTS wallet_release_seq BIGINT;
