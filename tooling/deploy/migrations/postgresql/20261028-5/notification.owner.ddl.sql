-- Notification is the sole delivery-state writer; historical rows remain readable.
ALTER TABLE notification_delivery ADD COLUMN IF NOT EXISTS owner_node_id BIGINT;
ALTER TABLE notification_delivery ADD COLUMN IF NOT EXISTS workflow_id BIGINT REFERENCES s_workflow(id);
ALTER TABLE notification_delivery ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS notification_delivery_owner_pending_idx ON notification_delivery
    (env,tenant_id,space_id,workflow_id,owner_node_id,state,available_at);
