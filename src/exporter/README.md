# Exporter runtime

The existing Exporter service enables v2 with `runtime_v2=true` and a JSON `runtime_config` containing scope, Workflow/node/input IDs, a scoped `connection_id`, `database_url_env` and `sink`. It consumes the canonical Workflow export request and publishes the export result. Strategy returns an `Effects.requests` entry with `kind="export"`; its query contains a bounded row batch and a fixed Sheets range when applicable.

Kafka uses `provider="kafka"`, `brokers`, `topic`, `properties` and optional `secret_env`. Secret values belong only in the host environment; for example `secret_env={"sasl.password":"SIGBOT_KAFKA_PASSWORD"}`. Plaintext credential properties are rejected. The factory forces idempotent production, `acks=all`, bounded queues and delivery timeouts. Successful delivery precedes the Inbox/checkpoint/Outbox commit. A committed duplicate does not reach Kafka again; a crash between broker ACK and database commit can redeliver the same scoped message key, so downstream consumers still deduplicate by that key.

Google Sheets uses `provider="google_sheets"`, `spreadsheet_id` and `token_env`. It sends an official `ValueRange` DTO with a fixed-range PUT and checks the acknowledged row count. Retrying writes the same range; append is not used. Reusing a destination range for different business batches is a configuration error the workflow author must avoid.

The existing producer/node/run/sequence determines the export key; a client-supplied key cannot override it. The checkpoint advances monotonically while late batches are still delivered once. `requestSpanId` stays in the transient reply cache and is absent from persistent payloads. Broker failures retain uncommitted work for transport retry and backpressure.

Run `cargo test -p sigbot-exporter --test export_delivery -- --include-ignored --test-threads=1` with `MARKETPULSE_DATABASE_URL` and `MARKETPULSE_KAFKA_BROKERS`. This test uses actual Kafka and PostgreSQL for delivery, restart, duplicate identity rejection, late cursors and scope checks. It does not claim a live Google Sheets write or Kafka multi-broker failover.

The durable owner is `export:{env}:{tenant_id}:{space_id}:{workflow_id}:{node_id}`. Reusing a node number in another Workflow cannot consume its Inbox or dispatch its Outbox. Migration 17 preserves completed legacy deduplication entries; stop old Order/Exporter writers before applying it and start only the new workers afterwards. Replay barriers normalize legacy owner names in memory while keeping their frozen checkpoint configuration unchanged.
