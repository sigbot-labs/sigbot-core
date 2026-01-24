---
name: exporter
description: 导出服务模块编码指南。相关模块：src/exporter/**/*.rs (导出服务实现), src/types/src/modules/exporter/**/*.rs (导出类型定义)
---
您正在开发导出服务模块 (`SigbotExporterServer`)。该微服务将交易数据和日志导出到外部系统，支持批量和流式导出模式。

**架构与数据流**：
1. **外部系统分类**：每个 `exporter_xx.rs` 代表针对特定外部系统的完整数据导出微服务（例如，`exporter_googlestreet.rs` 用于 Google Sheets，`exporter_kafka.rs` 用于 Kafka）。
2. **主题订阅**：每个导出管理器订阅来自消息器（例如 EMQX）的自己的数据源主题。当前支持订单放置数据（`TOPIC_WF_TRADING_PLACED`），未来支持日志和配置更改。
3. **导出模式**：批量导出（例如，用于订单分析的 Google Sheets）和流式导出（例如，用于实时流式传输到外部系统的 Kafka）。

**主要职责**：
1. **导出管理器**（`exporter_googlestreet.rs`、`exporter_kafka.rs`）：每个管理器处理对特定主题的订阅，解析事件，并导出到其外部系统。当前支持订单数据导出，设计用于未来的数据类型。
2. **工厂模式**：使用 `SigbotExporterManagerFactory::init()`，支持 `ExporterMgrProvider`（GOOGLESHEETS、KAFKA），CLI 参数 `--exporter-manager-provider/configuration`。
3. **服务器集成**：`SigbotExporterServer` 初始化管理器和消息器，将订阅委托给管理器的 `subscribe()` 方法。

**编码指南**：
- **自包含**：每个导出管理器在 `subscribe()` 方法中处理自己的主题订阅。
- **异步处理**：异步处理导出，不要阻塞消息处理器。
- **错误处理**：记录失败但继续处理，优雅地处理写入器错误。
- **可扩展性**：通过创建 `exporter_xx.rs` 并实现 `ISigbotExporterManager` trait 来添加新的导出器。
- **配置**：通过 UI/配置支持用户可配置的目标（spreadsheet_id、kafka_brokers 等）。
- **未来**：设计用于多种数据源类型（订单、日志、配置更改），每个导出器具有不同的主题订阅。
