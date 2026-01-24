# Evaluator Module Architecture

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SigbotEvaluatorManager                       │
│                                                                 │
│  ┌──────────────────┐              ┌──────────────────┐        │
│  │  Time-Driven     │              │  Event-Driven    │        │
│  │  (Cron Job)      │              │  (Messager)      │        │
│  │                  │              │                  │        │
│  │  Scan t_workflow │              │  Subscribe to    │        │
│  │  table every 6h  │              │  market events   │        │
│  └────────┬─────────┘              └────────┬─────────┘        │
│           │                                 │                  │
│           └────────────┬────────────────────┘                  │
│                        │                                       │
│                        ▼                                       │
│           ┌────────────────────────┐                          │
│           │    Job Registry        │                          │
│           │  workflow_id → Job     │                          │
│           └────────────┬───────────┘                          │
│                        │                                       │
│                        ▼                                       │
│           ┌────────────────────────┐                          │
│           │  SigbotEvaluationJob   │                          │
│           └────────────┬───────────┘                          │
└────────────────────────┼────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              GenericMultiAgentOrchestrator                      │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Step 1: Parallel Datafeed Agents (Concurrent)           │  │
│  │  ┌──────────────┐         ┌──────────────┐              │  │
│  │  │  BootAgent   │         │ LoaderAgent  │              │  │
│  │  │              │         │              │              │  │
│  │  │ Collect base │         │ Fetch warm   │              │  │
│  │  │ statistics   │         │ market data  │              │  │
│  │  └──────────────┘         └──────────────┘              │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                       │
│                         ▼                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Step 2: Sequential Strategy Agents (Ordered)            │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │           LoopAgent (Retry Logic)                  │  │  │
│  │  │  ┌──────────────┐         ┌──────────────┐        │  │  │
│  │  │  │ AlphaAgent   │◄────────┤ AuditorAgent │        │  │  │
│  │  │  │              │  retry  │              │        │  │  │
│  │  │  │ Calculate    │         │ Audit &      │        │  │  │
│  │  │  │ hyperparams  │         │ validate     │        │  │  │
│  │  │  └──────────────┘         └──────────────┘        │  │  │
│  │  │                                                    │  │  │
│  │  │  Max Retries: 3                                   │  │  │
│  │  │  Retry Delay: 500ms                               │  │  │
│  │  │  Feedback: Enabled                                │  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                         │                                       │
└─────────────────────────┼───────────────────────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Hyperparameter       │
              │  Update Event         │
              │                       │
              │  Published to         │
              │  Messager (EMQX)      │
              └───────────┬───────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
              ▼                       ▼
    ┌─────────────────┐     ┌─────────────────┐
    │ Strategy Runner │     │  Notification   │
    │                 │     │  Forwarder      │
    │ Apply hyperparams│     │                 │
    │ to Python code  │     │ Email/Telegram  │
    └─────────────────┘     └─────────────────┘
```

## Component Responsibilities

### SigbotEvaluatorManager
- **Purpose**: Manage evaluation job lifecycle
- **Responsibilities**:
  - Time-driven: Scan t_workflow table periodically
  - Event-driven: Subscribe to market data events
  - Track running jobs in registry
  - Prevent duplicate job starts
  - Cleanup completed jobs

### SigbotEvaluationJob
- **Purpose**: Execute single evaluation workflow
- **Responsibilities**:
  - Create and configure agents
  - Execute orchestrator
  - Publish results to messager
  - Handle job lifecycle (start/stop)

### GenericMultiAgentOrchestrator
- **Purpose**: Coordinate multi-agent execution
- **Responsibilities**:
  - Execute datafeed agents in parallel
  - Execute strategy agents sequentially
  - Manage agent context and data flow
  - Handle errors and retries

### Agents

#### BootAgent (Datafeed)
- Collect base market statistics
- Provide guidance information

#### LoaderAgent (Datafeed)
- Fetch warm market data (past 1d/4h/1h/30m)
- Load Twitter/TruthSocial news
- Use tools to fetch data

#### AlphaAgent (Strategy)
- Analyze market data
- Calculate hyperparameters (support/resistance levels, RSI thresholds)
- Use math tools for precise calculations

#### AuditorAgent (Strategy)
- Audit calculated hyperparameters
- Validate compliance boundaries
- Retry if validation fails (loop back to AlphaAgent)

## Data Flow

```
1. Trigger (Time/Event)
   │
   ▼
2. Create Evaluation Job
   │
   ▼
3. Parallel Datafeed Collection
   ├─ BootAgent → Base Statistics
   └─ LoaderAgent → Warm Market Data
   │
   ▼
4. Sequential Strategy Processing
   ├─ AlphaAgent → Calculate Hyperparameters
   └─ AuditorAgent → Validate ──┐
                                 │ (if failed)
                                 └─→ Retry AlphaAgent
   │
   ▼
5. Publish Hyperparameters
   ├─ Strategy Runner (apply to Python code)
   └─ Notification Forwarder (Email/Telegram)
```

## Key Design Patterns

### 1. **Parallel Execution Pattern**
```rust
// Datafeed agents run concurrently
let mut handles = Vec::new();
for agent in datafeed_agents {
    let handle = tokio::spawn(async move {
        agent.execute(&ctx).await
    });
    handles.push(handle);
}
// Wait for all to complete
for handle in handles {
    handle.await?;
}
```

### 2. **Sequential Execution Pattern**
```rust
// Strategy agents run in order
for agent in strategy_agents {
    let result = agent.execute(&ctx).await?;
    ctx = ctx.with_data(result.data);
}
```

### 3. **Retry Loop Pattern**
```rust
// LoopAgent handles retry logic
let alpha_auditor_loop = LoopAgent::new(vec![alpha_agent, auditor_agent])
    .configure_agent(1, |rule| {
        rule.with_goto_on_failure(Some(0)) // Retry AlphaAgent on failure
    });
```

### 4. **Job Registry Pattern**
```rust
// Track running jobs
job_registry.insert(workflow_id, job);
// ... execute job ...
job_registry.remove(&workflow_id);
```

## Comparison with Go Reference

### Similarities
- ✅ Dual trigger mechanism (time + event)
- ✅ Job registry for tracking
- ✅ Distributed lock support (prepared)
- ✅ Proper lifecycle management
- ✅ Parallel + Sequential execution

### Differences
- 🔄 Rust uses Arc/Mutex instead of channels
- 🔄 Rust uses ConcurrentMap instead of map + RWMutex
- 🔄 Rust uses tokio::spawn instead of goroutines
- 🔄 Rust uses async/await instead of callbacks

## Future Enhancements

1. **Database Integration**: Query t_workflow table
2. **Distributed Lock**: Implement Redis/etcd lock
3. **Status Tracking**: Update workflow status in DB
4. **Configuration**: Load from config file
5. **Metrics**: Add Prometheus metrics
6. **Tracing**: Add OpenTelemetry tracing
