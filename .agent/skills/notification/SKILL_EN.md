---
name: notification
description: Notification forwarder module coding guidelines. Related modules: src/notification/**/*.rs (notification forwarder service), src/core/src/modules/notification/**/*.rs (notification core modules)
---
You are working on the Notification Forwarder module (SigbotNotificationForwarder). This microservice subscribes to notification messages from EMQX and forwards them to various channels.

**Architecture & Responsibilities**:
- **2.1.6 Notification Forwarder**: Subscribes to notification messages from EMQX (`ISigbotMessagerClient`) and forwards them to Email, Telegram, WeChat, and other platforms based on notification type

**Key Responsibilities**:
1. **Message Subscription**:
   - Subscribe to notification topics from EMQX
   - Handle different notification types (trading alerts, system notifications, etc.)
   - Parse notification messages and extract routing information

2. **Multi-Channel Forwarding**:
   - Forward to Email (SMTP)
   - Forward to Telegram (Bot API)
   - Forward to WeChat (WeChat API)
   - Support extensible channel architecture

3. **Notification Routing**:
   - Route notifications based on user preferences
   - Support multiple channels per notification
   - Handle channel-specific formatting

**Coding Guidelines**:

1. **Channel Abstraction**:
   - Use trait-based design (`ISigbotNotificationClient`)
   - Implement factory pattern for channel creation
   - Support pluggable channel implementations
   - Handle channel-specific configurations

2. **Message Processing**:
   - Deserialize notification messages from JSON
   - Validate notification structure
   - Extract recipient information
   - Format messages for target channel

3. **Error Handling**:
   - Handle channel failures gracefully (don't block other channels)
   - Retry failed notifications with exponential backoff
   - Log notification failures with context
   - Support dead letter queue for failed notifications

4. **Rate Limiting**:
   - Respect channel rate limits (e.g., Telegram API limits)
   - Implement per-channel rate limiting
   - Queue notifications when rate limit exceeded
   - Monitor rate limit usage

5. **Security**:
   - Never log sensitive notification content
   - Encrypt credentials (API keys, tokens)
   - Validate notification sources
   - Support authentication for channels

6. **Performance**:
   - Process notifications asynchronously
   - Batch notifications when possible
   - Use connection pooling for HTTP clients
   - Monitor notification delivery latency

7. **Testing**:
   - Mock notification channels in tests
   - Test message routing logic
   - Test error scenarios (channel failures)
   - Test rate limiting behavior
   - Test concurrent notification handling

8. **Configuration**:
   - Support per-channel configuration
   - Allow enabling/disabling specific channels
   - Configure retry policies
   - Support notification templates
