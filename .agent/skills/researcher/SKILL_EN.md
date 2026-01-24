---
name: researcher
description: SigBot Researcher module coding guidelines (for sigbot-researcher subproject). Related modules: **/researcher/**/*.rs, **/research/**/*.rs (researcher subproject code)
---
You are working on the SigBot Researcher module. This is a higher-level AI-powered strategy research subproject built on top of sigbot-core.

**Architecture & Responsibilities**:
- **SigBot Researcher**: AI-powered autonomous strategy research system (virtual strategy developer)
- **SigbotResearchManager**: Microservice that scans s_research_case table for pending cases and launches research jobs
- **Multi-Agent Workflow**: Orchestrator executes multi-agent workflow:
  1. Datafeed Agent: Fetches data using agent tools from sigbot-core API or third-party APIs (supports a2a+x402 protocol for automatic payment)
  2. Alpha Gen Agent: Generates strategy code
  3. Backtest Agent: Executes backtest and generates performance report
  4. Fin Auditor Agent: Reviews backtest report (checks Sharpe ratio, max drawdown, win rate thresholds), audits strategy code safety (extreme market conditions), generates comprehensive safety assessment report
  5. Portfolio Mgr Agent: Comprehensive evaluation of backtest performance report and strategy logic/capital safety assessment report, final approval decision

**Key Responsibilities**:
1. **Research Case Management**:
   - Provide research case list API
   - Provide create new research case API
   - Track research case status (pending, running, completed, failed)
   - Store research case configuration and parameters

2. **Research Job Execution**:
   - Launch research jobs for pending cases
   - Use distributed lock to prevent concurrent job execution
   - Track job execution history
   - Store multi-agent workflow progress

3. **Multi-Agent Orchestration**:
   - Coordinate multiple agents in workflow
   - Pass data between agents
   - Handle agent failures and retries
   - Track agent execution progress

4. **Agent Implementations**:
   - Datafeed Agent: Fetch market data using tools
   - Alpha Gen Agent: Generate strategy code using LLM
   - Backtest Agent: Execute backtest via sigbot-core API
   - Fin Auditor Agent: Analyze backtest results and code safety
   - Portfolio Mgr Agent: Make final approval decision

5. **Deployment Integration**:
   - Provide deploy button/API to deploy approved strategies to sigbot-core
   - Transfer strategy code and configuration
   - Update research case status after deployment

**Coding Guidelines**:

1. **Research Case Management**:
   - Store research cases in PostgreSQL (s_research_case table)
   - Support CRUD operations via API
   - Track case status and history
   - Support case filtering and search

2. **Distributed Locking**:
   - Use distributed lock (Redis, PostgreSQL advisory locks) for job scanning
   - Prevent multiple instances from processing same case
   - Handle lock acquisition failures gracefully
   - Implement lock timeout and renewal

3. **Job Execution**:
   - Launch research jobs asynchronously
   - Track job progress and status
   - Store job execution logs
   - Handle job failures and retries

4. **Agent Architecture**:
   - Use trait-based design for agents (`IAgent`)
   - Support pluggable agent implementations
   - Provide agent tools for external API access
   - Handle agent communication and data passing

5. **Orchestrator Design**:
   - Implement workflow state machine
   - Handle workflow transitions
   - Support conditional branching based on agent results
   - Handle workflow failures and rollback

6. **LLM Integration**:
   - Use LangChain for LLM interactions
   - Provide agent tools for LLM agents
   - Handle LLM API errors and retries
   - Cache LLM responses when appropriate

7. **Datafeed Agent**:
   - Use agent tools to call sigbot-core API
   - Support third-party market data APIs
   - Support a2a+x402 protocol for automatic payment
   - Handle API authentication and errors

8. **Alpha Gen Agent**:
   - Generate strategy code using LLM
   - Validate generated code syntax
   - Ensure code follows SDK patterns
   - Store generated code for review

9. **Backtest Agent**:
   - Call sigbot-core backtest API
   - Monitor backtest progress
   - Retrieve backtest results
   - Parse performance metrics

10. **Fin Auditor Agent**:
    - Analyze backtest performance metrics
    - Check thresholds (Sharpe ratio, max drawdown, win rate)
    - Audit strategy code for safety issues
    - Test extreme market conditions
    - Generate comprehensive safety report

11. **Portfolio Mgr Agent**:
    - Evaluate backtest performance report
    - Evaluate strategy safety assessment
    - Make final approval/rejection decision
    - Provide decision reasoning

12. **Error Handling**:
    - Handle agent failures gracefully
    - Support workflow retry mechanisms
    - Log all agent actions and decisions
    - Store error details for debugging

13. **Testing**:
    - Mock agent implementations in tests
    - Test workflow orchestration logic
    - Test distributed locking behavior
    - Test agent communication
    - Test error scenarios

14. **Configuration**:
    - Support per-agent configuration
    - Configure LLM models and parameters
    - Configure threshold values for approval
    - Support environment-specific settings
