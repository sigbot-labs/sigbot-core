-- Restart reuses the admitted input's existing node/run/sequence identity.
-- Source cursor identifies an existing historical fact; no correlation ID is minted.
ALTER TABLE backtest_checkpoint ADD COLUMN IF NOT EXISTS pending_message JSONB;
