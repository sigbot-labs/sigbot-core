---
name: messager
description: Messager client module coding guidelines. Related modules: src/messager/**/*.rs (messager client implementations: MQTT, Local), src/core/src/modules/messager/**/*.rs (messager core modules)
---
You are working on the Messager module. This module provides message broker abstraction for EMQX (MQTT) and local messaging.

**Architecture & Responsibilities**:
- Provides `ISigbotMessagerClient` trait for message broker abstraction
- Supports MQTT (EMQX) and Local message brokers
- Used by all microservices for pub/sub messaging
- Enables event-driven architecture across the system

**Key Responsibilities**:
1. **Message Broker Abstraction**:
   - Abstract message broker operations (publish, subscribe)
   - Support multiple broker implementations (MQTT, Local)
   - Provide unified interface for all microservices

2. **MQTT Integration**:
   - Connect to EMQX MQTT broker
   - Handle MQTT connection lifecycle
   - Support QoS levels (0, 1, 2)
   - Handle reconnection logic

3. **Local Messaging**:
   - Provide in-memory message broker for testing
   - Support local pub/sub operations
   - Useful for development and testing

4. **Topic Management**:
   - Support topic wildcards (single-level +, multi-level #)
   - Handle topic subscriptions and unsubscriptions
   - Support multi-tenant topics

**Coding Guidelines**:

1. **Trait Design**:
   - Use `ISigbotMessagerClient` trait for abstraction
   - Keep trait methods async
   - Support Send + Sync bounds for concurrent use
   - Return Result types for error handling

2. **Factory Pattern**:
   - Use factory pattern for broker creation
   - Support runtime broker selection
   - Register broker implementations dynamically
   - Support singleton instances

3. **MQTT Implementation**:
   - Use rumqttc or paho-mqtt crate
   - Handle connection lifecycle (init, close)
   - Support QoS levels appropriately
   - Implement reconnection with exponential backoff
   - Handle MQTT-specific errors

4. **Message Handling**:
   - Support message serialization (JSON)
   - Handle message deserialization errors
   - Support binary and text messages
   - Provide message handlers with proper signatures

5. **Subscription Management**:
   - Track active subscriptions
   - Support multiple handlers per topic
   - Handle subscription failures gracefully
   - Support wildcard subscriptions

6. **Error Handling**:
   - Map broker-specific errors to common error types
   - Handle connection failures gracefully
   - Implement retry logic for transient failures
   - Log errors with full context

7. **Performance**:
   - Use async/await for all operations
   - Support concurrent publish/subscribe
   - Use connection pooling when applicable
   - Monitor message throughput

8. **Testing**:
   - Mock message broker in tests
   - Test publish/subscribe operations
   - Test error scenarios (connection failures)
   - Test reconnection logic
   - Test wildcard subscriptions

9. **Configuration**:
   - Support broker-specific configuration
   - Configure connection parameters (host, port, credentials)
   - Configure QoS and other MQTT settings
   - Support environment-based configuration
