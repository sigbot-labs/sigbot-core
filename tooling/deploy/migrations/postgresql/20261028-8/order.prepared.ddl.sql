-- Venue SDK preparation is saved before any order submission. Payloads that contain
-- signing material are encrypted by the adapter using a host-only key.
ALTER TABLE order_record ADD COLUMN IF NOT EXISTS prepared_submission JSONB
    CHECK(prepared_submission IS NULL OR (prepared_submission ? 'venue_order_id'
        AND prepared_submission ? 'payload' AND octet_length(prepared_submission::text) <= 1048576));
CREATE UNIQUE INDEX IF NOT EXISTS order_prepared_venue_id
    ON order_record(wallet_id,exchange,(prepared_submission->>'venue_order_id'))
    WHERE prepared_submission IS NOT NULL;
CREATE OR REPLACE FUNCTION order_reject_prepared_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.prepared_submission IS NOT NULL AND NEW.prepared_submission IS DISTINCT FROM OLD.prepared_submission THEN
        RAISE EXCEPTION 'prepared venue submission is immutable';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS order_prepared_immutable ON order_record;
CREATE TRIGGER order_prepared_immutable BEFORE UPDATE OF prepared_submission ON order_record
    FOR EACH ROW EXECUTE FUNCTION order_reject_prepared_mutation();
