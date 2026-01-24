---
name: exporter
description: Exporter service module coding guidelines. Related modules: src/exporter/**/*.rs (exporter service implementation), src/types/src/modules/exporter/**/*.rs (exporter type definitions)
---
You are working on the Exporter Service module (`SigbotExporterServer`). This microservice exports trading data and logs to external systems, supporting both batch and stream export modes.

**Architecture & Data Flow**:
1. **External System Classification**: Each `exporter_xx.rs` represents a complete data export microservice for a specific external system (e.g., `exporter_googlestreet.rs` for Google Sheets, `exporter_kafka.rs` for Kafka).
2. **Topic Subscription**: Each exporter manager subscribes to its own data source topics from messager (e.g., EMQX). Currently supports order placed data (`TOPIC_WF_TRADING_PLACED`), future support for logs and config changes.
3. **Export Modes**: Batch export (e.g., Google Sheets for order analysis) and stream export (e.g., Kafka for real-time streaming to external systems).

**Key Responsibilities**:
1. **Exporter Managers** (`exporter_googlestreet.rs`, `exporter_kafka.rs`): Each manager handles subscription to specific topics, parses events, and exports to its external system. Currently supports order data export, designed for future data types.
2. **Factory Pattern**: Use `SigbotExporterManagerFactory::init()`, support `ExporterMgrProvider` (GOOGLESHEETS, KAFKA), CLI args `--exporter-manager-provider/configuration`.
3. **Server Integration**: `SigbotExporterServer` initializes manager and messager, delegates subscription to manager's `subscribe()` method.

**Coding Guidelines**:
- **Self-Contained**: Each exporter manager handles its own topic subscription in `subscribe()` method.
- **Async Processing**: Process exports asynchronously, don't block message handlers.
- **Error Handling**: Log failures but continue processing, handle writer errors gracefully.
- **Extensibility**: Add new exporters by creating `exporter_xx.rs` and implementing `ISigbotExporterManager` trait.
- **Configuration**: Support user-configurable targets via UI/config (spreadsheet_id, kafka_brokers, etc.).
- **Future**: Design for multiple data source types (orders, logs, config changes) with different topic subscriptions per exporter.
