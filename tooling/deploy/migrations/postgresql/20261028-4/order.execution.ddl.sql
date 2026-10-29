-- Preserve existing execution records; add the original intent and producer references.
ALTER TABLE order_execution ADD COLUMN IF NOT EXISTS producer_node_id BIGINT;
ALTER TABLE order_execution ADD COLUMN IF NOT EXISTS reservation_node_id BIGINT;
ALTER TABLE order_execution ADD COLUMN IF NOT EXISTS reservation_seq BIGINT;
ALTER TABLE order_execution ADD COLUMN IF NOT EXISTS order_complete BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE order_execution ADD COLUMN IF NOT EXISTS payload JSONB;
CREATE INDEX IF NOT EXISTS order_execution_intent ON order_execution(wallet_id,reservation_node_id,run_id,reservation_seq);
-- Historical rows are retained with NULL provenance and require explicit reconciliation.
-- New writes require all identity fields and an immutable canonical fact.
CREATE OR REPLACE FUNCTION order_execution_validate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.producer_node_id IS NULL OR NEW.producer_node_id <= 0 OR NEW.reservation_node_id IS NULL
       OR NEW.reservation_node_id <= 0 OR NEW.reservation_seq IS NULL OR NEW.reservation_seq < 0
       OR NEW.payload IS NULL THEN RAISE EXCEPTION 'execution provenance is required'; END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS order_execution_provenance ON order_execution;
CREATE TRIGGER order_execution_provenance BEFORE INSERT ON order_execution
    FOR EACH ROW EXECUTE FUNCTION order_execution_validate();
DROP TRIGGER IF EXISTS order_execution_immutable ON order_execution;
CREATE TRIGGER order_execution_immutable BEFORE UPDATE OR DELETE ON order_execution
    FOR EACH ROW EXECUTE FUNCTION wallet_reject_mutation();
