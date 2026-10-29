-- PostgreSQL NOW()/created_at is the transaction start, which can precede the LLM
-- response. Legacy rows backfilled from created_at must never expose AI output early.
UPDATE evaluation_snapshot e SET available_at=GREATEST(e.available_at,(
    SELECT MAX((o.payload->>'produced_at')::timestamptz) FROM messager_outbox o
    WHERE o.env=e.env AND o.tenant_id=e.tenant_id AND o.space_id=e.space_id AND o.node_id=e.node_id
      AND o.run_id=e.run_id AND o.topic LIKE '%/evaluation/snapshot'
      AND (o.payload->'payload'->>'version')::bigint=e.version AND e.available_at<=o.created_at
)) WHERE EXISTS(SELECT 1 FROM messager_outbox o WHERE o.env=e.env AND o.tenant_id=e.tenant_id
    AND o.space_id=e.space_id AND o.node_id=e.node_id AND o.run_id=e.run_id AND o.topic LIKE '%/evaluation/snapshot'
    AND (o.payload->'payload'->>'version')::bigint=e.version AND e.available_at<=o.created_at);
DO $$ BEGIN
    IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='evaluation_available_after_observation' AND conrelid='evaluation_snapshot'::regclass) THEN
        ALTER TABLE evaluation_snapshot ADD CONSTRAINT evaluation_available_after_observation CHECK(available_at>=as_of);
    END IF;
END $$;
