export type PageName =
  | "overview"
  | "market"
  | "workflows"
  | "sources"
  | "exchanges";
export type Environment = "live" | "paper" | "simulated" | "backtest";
export interface Identity {
  principal_id: string;
  tenant_id: string;
  action: string;
}
export interface RunEvent {
  run_id: string;
  workflow_id?: string;
  workflow_name?: string;
  trace_id?: string;
  span_id?: string;
  parent_span_id?: string;
  status: string;
  event_time?: string;
  message?: string;
  seq?: number;
  attempt_no?: number;
}
export interface Overview {
  workflows_total?: number;
  active_workflows?: number;
  realized_pnl?: string;
  equity?: string;
  run_success_rate?: number;
  executions_count?: number;
  equity_series?: { time: string; value: number }[];
  recent_runs?: RunEvent[];
  updated_at?: string;
}
export interface Notification {
  seq: number;
  producer_node_id: number;
  channel: string;
  read: boolean;
  title: string;
  body: string;
  status: string;
  event_time: string;
  run_id: string;
  kind?: string;
}
export interface MarketRecord {
  source_event_id?: string;
  revision?: number;
  source: string;
  category: string;
  symbol?: string;
  value?: string;
  title?: string;
  url?: string;
  event_time: string;
  available_time: string;
  sequence?: number;
  points?: { time: number; price: number; intensity: number }[];
}
export interface Connection {
  id: string;
  name: string;
  kind: "datafeed" | "exchange";
  provider: string;
  mode: string;
  status: string;
  endpoint?: string;
  secret_ref?: string;
  updated_at?: string;
}
export interface WorkflowNode {
  id: number;
  name: string;
  stage: string;
  position: { x: number; y: number };
  status?: string;
  config?: Record<string, unknown>;
}
export interface WorkflowEdge {
  kind?: string;
  id: string;
  source: string;
  target: string;
}
export interface Workflow {
  id: number;
  name: string;
  status: string;
  env: Environment;
  space_id: number;
  flow_json?: { nodes: WorkflowNode[]; connections: WorkflowEdge[] };
  description?: string;
  updated_at?: string;
  properties?: Record<string, string>;
}
export interface WorkflowList {
  data: Workflow[] | null;
  page?: { total?: number; num?: number; limit?: number };
}
