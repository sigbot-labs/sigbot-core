---
name: datafeed
description: Datafeed ingestor module coding guidelines. Related modules: src/datafeed/**/*.rs (datafeed ingestor service), src/core/src/modules/datafeed/**/*.rs (datafeed core modules)
---
You are working on the Datafeed Ingestor module (SigbotDatafeedIngestor). This microservice is responsible for fetching/subscribing market and news data from external sources and publishing to EMQX message broker.

**Architecture & Responsibilities**:
- **Datafeed Ingestor**: Fetches market/news data from external APIs (Binance, Twitter, TruthSocial, Coinmarketcap, etc.) and publishes to EMQX (ISigbotMessagerClient) as the first step in the trading workflow (real-time event-driven middleware before strategy analysis)

**Key Responsibilities**:
1. **External Data Fetching**:
   - Fetch market data from exchange APIs (Binance, etc.)
   - Subscribe to real-time market streams (websockets, REST APIs)
   - Fetch news data from social media APIs (Twitter, TruthSocial)
   - Handle multiple data sources concurrently

2. **Data Normalization**:
   - Normalize data from different sources to common format
   - Convert timestamps to UTC
   - Standardize symbol formats
   - Validate data integrity before publishing

3. **Message Publishing**:
   - Publish normalized data to EMQX topics (TOPIC_MARKET_STREAMS)
   - Use appropriate MQTT QoS levels for reliability
   - Handle publish failures with retry logic
   - Support both market data and news data streams

**Coding Guidelines**:

1. **External API Integration**:
   - Implement rate limiting to respect API limits
   - Handle API authentication (API keys, OAuth tokens)
   - Implement exponential backoff for retries
   - Use connection pooling for HTTP clients
   - Support both REST and WebSocket connections

2. **Data Source Abstraction**:
   - Use factory pattern for different data sources
   - Implement `ISigbotDatafeedClient` trait for all sources
   - Support pluggable data source implementations
   - Handle source-specific error types

3. **Message Broker Integration**:
   - Use `ISigbotMessagerClient` trait for message publishing
   - Publish to appropriate topics based on data type
   - Ensure message serialization is efficient (JSON)
   - Handle message broker connection failures gracefully

4. **Error Handling**:
   - Map external API errors to internal error types
   - Log errors with full context (source, timestamp, data)
   - Implement circuit breaker pattern for failing sources
   - Continue operation if one source fails

5. **Performance**:
   - Use async/await for all I/O operations
   - Process multiple data sources concurrently
   - Batch messages when appropriate
   - Monitor data ingestion latency

6. **Data Quality**:
   - Validate data before publishing
   - Handle missing or corrupted data gracefully
   - Log data quality metrics
   - Support data filtering/transformation

7. **Testing**:
   - Mock external APIs in unit tests
   - Test data normalization logic
   - Test message publishing with mock broker
   - Test error scenarios (API failures, network issues)
   - Test concurrent data source handling

8. **Configuration**:
   - Support per-source configuration
   - Allow enabling/disabling specific sources
   - Configure retry policies and timeouts
   - Support environment-specific settings
