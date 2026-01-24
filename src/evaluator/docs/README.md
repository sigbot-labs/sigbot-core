# Sigbot Evaluator Module

## Overview

The Evaluator module provides LLM-powered hyperparameter evaluation and adjustment for static Pycode Strategy nodes. It operates as a **sidecar** (bypass) component in the trading workflow, similar to traditional web2 WAF's asynchronous traffic attack identification mechanism.

## Architecture

The module follows a **workflow orchestrator** pattern with multi-agent coordination, similar to google adk-go's architecture:

```
Orchestrator
    ├── BootAgent (LLM agent - collects market statistics)
    ├── LoaderAgent (LLM agent - loads warm data)
    └── LoopAgent (Loop agent - orchestrates AlphaAgent + AuditorAgent with retry)
        ├── AlphaAgent (LLM agent - calculates hyperparameters)
        └── AuditorAgent (LLM agent - validates compliance)
```

### Core Components

- **`core/agent.rs`**: Unified `ISigbotAgent` trait definition and execution context
- **`core/orchestrator.rs`**: Workflow orchestrator that coordinates agents sequentially
- **`agents/`**: Individual agent implementations
  - **LLM Agents**: BootAgent, LoaderAgent, AlphaAgent, AuditorAgent
  - **Loop Agent**: SigbotLoopAgent (handles retry logic for multiple agents)
- **`evaluator_runner.rs`**: Main entry point and service runner

### Agent Types

1. **LLM Agents**: Agents that use LLM for analysis and decision-making
   - **BootAgent**: Collects basic market statistics as bootstrap information
   - **LoaderAgent**: Loads warm data (historical klines, Twitter, TruthSocial news, etc.) based on bootstrap info
   - **AlphaAgent**: Analyzes warm data and calculates hyperparameters (support/resistance levels, stop loss, take profit, etc.)
   - **AuditorAgent**: Performs compliance boundary checks on generated hyperparameters

2. **Loop Agent**: Orchestrates multiple agents with retry logic
   - **SigbotLoopAgent**: Executes multiple agents sequentially, retries on failure
   - Supports feedback mechanism (feeds failure reasons back to starting agent on retry)
   - Configurable max retries and retry delay
   - **Agent Rules**: Configure success/failure conditions and goto behavior for each subagent
     - `success_condition`: Custom function to determine if agent result is success
     - `failure_condition`: Custom function to determine if agent result is failure (triggers retry)
     - `goto_on_failure`: Which agent index to goto on failure (None = restart from first agent)

### Unified Agent Interface

All agents implement the `ISigbotAgent` trait with a single `execute()` method:

```rust
pub trait ISigbotAgent: Send + Sync {
    fn name(&self) -> &'static str;
    async fn execute(&self, ctx: &SigbotAgentContext) -> Result<SigbotAgentResult, Error>;
}
```

This unified interface allows:
- **Orchestrator** to treat all agents uniformly
- **LoopAgent** to wrap any combination of agents
- Easy composition and extension of agent workflows

## Trigger Mechanisms

The evaluator supports dual triggering:

1. **Cron Job**: Scheduled periodic evaluation (default: every 6 hours)
2. **Market Data Events**: Event-driven evaluation based on specific market data rules

## Usage

### Start Evaluator Runner

```bash
sigbot evaluator \
  --messager-provider=mqtt \
  --evaluator-runner-provider=default \
  --evaluator-runner-configuration=<base64_encoded_json>
```

### Configuration

The evaluator configuration should include:
- Messager configuration (for publishing hyperparameter updates)
- LLM configuration (for agent operations)

### Topics

- **Input**: `/internal/v1/{TENANT_ID}/evaluator/trigger` - Subscribe to trigger events
- **Output**: `/internal/v1/{TENANT_ID}/{WORKFLOW_ID}/hyperparameter/update` - Publish hyperparameter updates

### Integration

1. **Strategy Runner**: Subscribes to `TOPIC_WF_HYPERPARAMETER_UPDATE` to receive and apply hyperparameter updates
2. **Notification Forwarder**: Can subscribe to hyperparameter updates for notifications

## Design Principles

- **Unified Agent Interface**: All agents implement the `Agent` trait for consistency
- **Orchestrator Pattern**: Central orchestrator coordinates agent execution and handles retry logic
- **Context Passing**: Agents communicate through `AgentContext` with shared data
- **Error Handling**: Each agent returns `AgentResult` with success/failure status

## Agent Rule Configuration Example

```rust
// Configure LoopAgent with agent rules
let loop_agent = SigbotLoopAgent::new(vec![alpha_agent, auditor_agent])
    .with_max_retries(3)
    .with_retry_delay(Duration::from_millis(500))
    .with_feedback(true)
    // Configure AuditorAgent (index 1) rule
    .configure_agent(1, |rule| {
        rule.with_goto_on_failure(Some(0)) // On failure, goto AlphaAgent (index 0) to retry
            .with_success_condition(|result| result.success) // Success: result.success == true
            .with_failure_condition(|result| !result.success) // Failure: result.success == false
    });
```

This configuration means:
- When AuditorAgent fails, retry from AlphaAgent (index 0)
- AuditorAgent success condition: `result.success == true`
- AuditorAgent failure condition: `result.success == false`

## Future Enhancements

- [ ] Database integration for market statistics collection
- [ ] Actual datafeed integration for warm data loading
- [ ] Enhanced LLM prompts and tool calling
- [ ] Strategy-specific hyperparameter templates
- [ ] Performance metrics and monitoring
- [ ] More sophisticated agent rule conditions (e.g., check specific data fields)
