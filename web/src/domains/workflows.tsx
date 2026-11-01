import { useCallback, useEffect, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  Handle,
  Position,
  addEdge,
  useEdgesState,
  useNodesState,
  useNodesInitialized,
  useReactFlow,
  type Connection,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import {
  ArrowLeft,
  Check,
  ChevronRight,
  Code2,
  Download,
  GitBranch,
  Play,
  Plus,
  Radio,
  Save,
  Square,
  Trash2,
} from "lucide-react";

import "@xyflow/react/dist/style.css";
import { Api, useRemote } from "../lib/api";
import type {
  DeploymentEnvironment,
  RunEvent,
  Workflow,
  WorkflowList,
  WorkflowNode,
} from "../lib/types";
import { useScope } from "../lib/workspace";
import {
  Badge,
  ConnectionState,
  DateText,
  Empty,
  PageHeader,
  SectionHeading,
} from "../components/shared";

function FitWorkflowViewport({ nodeCount }: { nodeCount: number }) {
  const initialized = useNodesInitialized();
  const { fitView } = useReactFlow();
  useEffect(() => {
    if (initialized && nodeCount > 0) {
      void fitView({ maxZoom: 1, padding: 0.2, duration: 120 });
    }
  }, [initialized, nodeCount, fitView]);
  return null;
}

interface FlowData extends Record<string, unknown> {
  label: string;
  module: string;
  stage: string;
  config: Record<string, unknown>;
}
type FlowNode = Node<FlowData>;
const palette = [
  {
    module: "datafeed",
    name: "Datafeed",
    stage: "TWITTER@INPUT",
    description: "行情与新闻",
  },
  {
    module: "evaluator",
    name: "Evaluator",
    stage: "MAS@EVALUATION",
    description: "态势与超参",
  },
  {
    module: "strategy",
    name: "Strategy",
    stage: "PYCODE@EVALUATION",
    description: "策略与意图",
  },
  {
    module: "order",
    name: "Order",
    stage: "DEFAULT@TRANSACTION",
    description: "风控与执行",
  },
  {
    module: "wallet",
    name: "Wallet",
    stage: "POST",
    description: "账本与组合",
  },
  {
    module: "notifier",
    name: "Notification",
    stage: "EMAIL@POST",
    description: "通知与投递",
  },
  {
    module: "exporter",
    name: "Exporter",
    stage: "OUTPUT",
    description: "外部导出",
  },
];
function StageNode({ data, selected }: NodeProps<FlowNode>) {
  return (
    <div className={`stage-node ${data.module} ${selected ? "selected" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="stage-icon">
        {data.module === "strategy" ? (
          <Code2 size={17} />
        ) : data.module === "datafeed" ? (
          <Radio size={17} />
        ) : (
          <GitBranch size={17} />
        )}
      </div>
      <div>
        <strong>{data.label}</strong>
        <small>
          {palette.find((stage) => stage.module === data.module)?.description ??
            data.module}
        </small>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
const nodeTypes = { stage: StageNode };

export function Workflows() {
  const scope = useScope();
  const remote = useRemote<WorkflowList>(
    `/api/v1/workflows?${scope.query}&num=1&limit=100`,
    { data: [] },
  );
  const [editing, setEditing] = useState<Workflow | "new" | null>(null);
  if (editing)
    return (
      <WorkflowEditor
        workflow={editing === "new" ? null : editing}
        close={() => {
          setEditing(null);
          remote.refresh();
        }}
      />
    );
  return (
    <>
      <PageHeader
        eyebrow="AUTONOMOUS WORKFLOWS"
        title="Workflow"
        description="连接数据、态势、策略与执行，用清晰的边界编排每一步。"
        action={
          <button className="primary-button" onClick={() => setEditing("new")}>
            <Plus size={16} />
            新建 Workflow
          </button>
        }
      />
      <ConnectionState {...remote} />
      <section className="panel">
        <SectionHeading
          title="我的 Workflow"
          subtitle={`${scope.env.toUpperCase()} · 当前认证租户`}
        />
        {remote.data.data?.length ? (
          <div className="workflow-grid">
            {remote.data.data.map((workflow) => (
              <button
                className="workflow-card"
                key={workflow.id}
                onClick={() => setEditing(workflow)}
              >
                <div className="workflow-card-head">
                  <span className="workflow-glyph">
                    <GitBranch size={22} />
                  </span>
                  <Badge value={workflow.status} />
                </div>
                <h3>{workflow.name}</h3>
                <p>{workflow.description ?? "市场信息驱动的策略工作流"}</p>
                <div className="workflow-card-pipeline">
                  {(workflow.flow_json?.nodes ?? [])
                    .slice(0, 5)
                    .map((node, index) => (
                      <span key={node.id}>
                        {index > 0 && <ChevronRight size={12} />}
                        <span>{node.name}</span>
                      </span>
                    ))}
                </div>
                <footer>
                  <span>
                    <DateText value={workflow.updated_at} />
                  </span>
                  <span>打开画布 →</span>
                </footer>
              </button>
            ))}
          </div>
        ) : (
          <Empty
            title="从一条清晰的策略链路开始"
            detail="添加节点并连接数据流，先在 Paper 或 Backtest 模式验证你的想法。"
          />
        )}
      </section>
      <div className="info-strip">
        <GitBranch size={18} />
        <p>
          节点通过异步消息通信。每次运行的事件、重试与执行结果，都可以追溯到原始
          Run。
        </p>
      </div>
    </>
  );
}

function WorkflowEditor({
  workflow,
  close,
}: {
  workflow: Workflow | null;
  close: () => void;
}) {
  const scope = useScope();
  const [id, setId] = useState<string | undefined>(workflow?.id);
  const [name, setName] = useState(workflow?.name ?? "");
  const [env, setEnv] = useState<DeploymentEnvironment>(
    workflow?.env ?? scope.env,
  );
  const [mode, setMode] = useState(workflow?.properties?.mode ?? "paper");
  const [status, setStatus] = useState(workflow?.status ?? "STOPPED");
  const [nodes, setNodes, onNodesChange] = useNodesState<FlowNode>(
    (workflow?.flow_json?.nodes ?? []).map((node) => ({
      id: String(node.id),
      type: "stage",
      position: node.position,
      data: {
        label: node.name,
        module: String(
          node.config?.module ??
            palette.find((stage) => stage.stage === node.stage)?.module ??
            "strategy",
        ),
        stage: node.stage,
        config: node.config ?? {},
      },
    })),
  );
  const [edges, setEdges, onEdgesChange] = useEdgesState(
    (workflow?.flow_json?.connections ?? []).map((edge) => ({
      ...edge,
      animated: false,
    })),
  );
  const [selected, setSelected] = useState<string | null>(null);
  const [source, setSource] = useState("");
  const [target, setTarget] = useState("");
  const [edgeKind, setEdgeKind] = useState("");
  const [config, setConfig] = useState("{}");
  const [feedback, setFeedback] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [dirty, setDirty] = useState(!workflow);
  const connect = useCallback(
    (connection: Connection) => {
      if (
        connection.source === connection.target ||
        edges.some(
          (edge) =>
            edge.source === connection.source &&
            edge.target === connection.target,
        )
      )
        return;
      setEdges((current) =>
        addEdge(
          {
            ...connection,
            id: `${connection.source}-${connection.target}`,
            kind: edgeKind || undefined,
          },
          current,
        ),
      );
      setDirty(true);
    },
    [edges, setEdges, edgeKind],
  );
  function addNode(stage: (typeof palette)[number]) {
    const nodeId = String(
      Math.max(0, ...nodes.map((node) => Number(node.id))) + 1,
    );
    setNodes((current) => [
      ...current,
      {
        id: nodeId,
        type: "stage",
        position: {
          x: 50 + (current.length % 3) * 245,
          y: 90 + Math.floor(current.length / 3) * 140,
        },
        data: {
          label: stage.name,
          module: stage.module,
          stage: stage.stage,
          config: { module: stage.module },
        },
      },
    ]);
    setDirty(true);
  }
  function applyConfiguration() {
    try {
      const value: unknown = JSON.parse(config);
      if (value === null || Array.isArray(value) || typeof value !== "object")
        throw new Error("配置需要是 JSON 对象");
      setNodes((current) =>
        current.map((node) =>
          node.id === selected
            ? {
                ...node,
                data: {
                  ...node.data,
                  config: { ...value, module: node.data.module },
                },
              }
            : node,
        ),
      );
      setDirty(true);
      setError(null);
      setFeedback("节点配置已更新，保存后生效");
    } catch (error) {
      setError(error instanceof Error ? error.message : "配置格式错误");
    }
  }
  async function save() {
    if (!name.trim() || nodes.length === 0) {
      setError("请填写名称并添加至少一个节点");
      return;
    }
    setBusy(true);
    setError(null);
    setFeedback(null);
    try {
      const graph: { nodes: WorkflowNode[]; connections: typeof edges } = {
        nodes: nodes.map((node) => ({
          id: Number(node.id),
          name: node.data.label,
          stage: node.data.stage,
          position: node.position,
          config: node.data.config,
        })),
        connections: edges,
      };
      const result = await Api.save<{ id: string }>("/api/v1/workflows", {
        id,
        name: name.trim(),
        env,
        status,
        plain_configuration: { ...workflow?.properties, mode },
        flow_info: {
          nodes: graph.nodes,
          connections: graph.connections.map(
            ({ id, source, target, kind }) => ({
              id,
              source,
              target,
              kind,
            }),
          ),
        },
      });
      setId(result.id);
      setDirty(false);
      setFeedback("Workflow 已保存");
    } catch (error) {
      setError(error instanceof Error ? error.message : "保存失败");
    } finally {
      setBusy(false);
    }
  }
  async function transition(action: "start" | "stop") {
    if (!id || dirty) {
      setError("请先保存 Workflow");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await Api.save(`/api/v1/workflows/${id}/${action}`, {});
      setStatus(action === "start" ? "PENDING" : "STOPPING");
      setFeedback(
        action === "start"
          ? "运行请求已接受，等待调度与执行事件"
          : "停止请求已接受",
      );
    } catch (error) {
      setError(error instanceof Error ? error.message : "请求失败");
    } finally {
      setBusy(false);
    }
  }
  function removeNode() {
    setNodes((current) => current.filter((node) => node.id !== selected));
    setEdges((current) =>
      current.filter(
        (edge) => edge.source !== selected && edge.target !== selected,
      ),
    );
    setSelected(null);
    setDirty(true);
  }
  return (
    <>
      <div className="editor-top">
        <button className="text-button" onClick={close}>
          <ArrowLeft size={16} />
          所有 Workflow
        </button>
        <div className="editor-actions">
          <Badge value={status} />
          <button className="secondary-button" disabled={busy} onClick={save}>
            <Save size={15} />
            保存 Workflow
          </button>
          <button
            className="primary-button"
            disabled={busy || !id || dirty}
            onClick={() => transition("start")}
          >
            <Play size={15} />
            运行 Workflow
          </button>
          <button
            className="icon-button"
            title="停止 Workflow"
            aria-label="停止 Workflow"
            disabled={busy || !id}
            onClick={() => transition("stop")}
          >
            <Square size={16} />
          </button>
        </div>
      </div>
      <div className="editor-fields">
        <label>
          Workflow 名称
          <input
            value={name}
            onChange={(event) => {
              setName(event.target.value);
              setDirty(true);
            }}
            placeholder="为策略起一个名字"
          />
        </label>
        <label>
          部署环境
          <select
            disabled={!!id}
            value={env}
            onChange={(event) => {
              setEnv(event.target.value as DeploymentEnvironment);
              setDirty(true);
            }}
          >
            <option value="dev">DEV</option>
            <option value="sit">SIT</option>
            <option value="uat">UAT</option>
            <option value="prod">PROD</option>
          </select>
        </label>
        <label>
          交易模式
          <select
            aria-label="交易模式"
            value={mode}
            onChange={(event) => {
              setMode(event.target.value);
              setDirty(true);
            }}
          >
            <option value="paper">Paper</option>
            <option value="backtest">Backtest</option>
            <option value="live">Live</option>
          </select>
        </label>
        <span className="draft-label">
          {dirty ? "有未保存的更改" : "已保存"}
        </span>
      </div>
      {error && (
        <div role="alert" className="form-error">
          {error}
        </div>
      )}
      {feedback && (
        <div role="status" className="success-note">
          <Check size={15} />
          {feedback}
        </div>
      )}
      <div className="workflow-editor">
        <aside className="node-palette">
          <div className="eyebrow">NODE LIBRARY</div>
          <h3>添加节点</h3>
          {palette.map((stage) => (
            <button
              key={stage.module}
              onClick={() => addNode(stage)}
              aria-label={`添加 ${stage.name}`}
            >
              <span className={`node-color ${stage.module}`} />
              <span>
                <strong>{stage.name}</strong>
                <small>{stage.description}</small>
              </span>
              <Plus size={14} />
            </button>
          ))}
        </aside>
        <div className="flow-canvas" data-testid="workflow-canvas">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={(changes) => {
              onNodesChange(changes);
              if (
                changes.some((change) =>
                  ["position", "add", "replace", "remove"].includes(
                    change.type,
                  ),
                )
              )
                setDirty(true);
            }}
            onEdgesChange={(changes) => {
              onEdgesChange(changes);
              if (changes.some((change) => change.type !== "select"))
                setDirty(true);
            }}
            onConnect={connect}
            nodeTypes={nodeTypes}
            onNodeClick={(_, node) => {
              setSelected(node.id);
              setConfig(JSON.stringify(node.data.config, null, 2));
            }}
            fitView
            fitViewOptions={{ maxZoom: 1, padding: 0.2 }}
          >
            <FitWorkflowViewport nodeCount={nodes.length} />
            <Background gap={22} size={1} />
            <Controls showInteractive={false} />
          </ReactFlow>
          {nodes.length === 0 && (
            <div className="canvas-hint">
              <GitBranch size={30} />
              <strong>从左侧添加第一个节点</strong>
              <p>通过节点端点，或下方表单连接异步数据流。</p>
            </div>
          )}
        </div>
      </div>
      <div className="edge-builder">
        <strong>连接节点</strong>
        <label>
          起点节点
          <select
            aria-label="起点节点"
            value={source}
            onChange={(event) => setSource(event.target.value)}
          >
            <option value="">选择起点</option>
            {nodes.map((node) => (
              <option key={node.id} value={node.id}>
                {node.data.label} #{node.id}
              </option>
            ))}
          </select>
        </label>
        <ChevronRight size={16} />
        <label>
          终点节点
          <select
            aria-label="终点节点"
            value={target}
            onChange={(event) => setTarget(event.target.value)}
          >
            <option value="">选择终点</option>
            {nodes.map((node) => (
              <option key={node.id} value={node.id}>
                {node.data.label} #{node.id}
              </option>
            ))}
          </select>
        </label>
        <label>
          消息类型
          <select
            aria-label="消息类型"
            value={edgeKind}
            onChange={(event) => setEdgeKind(event.target.value)}
          >
            <option value="">按节点模块路由</option>
            {[
              "feed_market",
              "feed_news",
              "feed_status",
              "evaluation_snapshot",
              "strategy_signal",
              "feed_snapshot_request",
              "feed_snapshot_result",
              "compute_request",
              "compute_result",
              "deployment_request",
              "deployment_result",
              "notification_request",
              "notification_result",
              "export_request",
              "export_result",
              "runtime_event",
              "log_event",
            ].map((kind) => (
              <option key={kind} value={kind}>
                {kind}
              </option>
            ))}
          </select>
        </label>
        <button
          className="secondary-button"
          disabled={!source || !target || source === target}
          onClick={() =>
            connect({ source, target, sourceHandle: null, targetHandle: null })
          }
        >
          添加连线
        </button>
        <span data-testid="workflow-edge-count">{edges.length} 条连线</span>
      </div>
      {selected && (
        <section className="panel node-config">
          <SectionHeading
            title={`节点配置 · ${nodes.find((node) => node.id === selected)?.data.label}`}
            subtitle="连接与凭据使用服务端引用；禁止在策略中直接下单。"
            action={
              <button className="text-button danger" onClick={removeNode}>
                <Trash2 size={14} />
                删除节点
              </button>
            }
          />
          <label>
            节点 JSON 配置
            <textarea
              value={config}
              onChange={(event) => setConfig(event.target.value)}
              rows={6}
            />
          </label>
          <button className="secondary-button" onClick={applyConfiguration}>
            应用节点配置
          </button>
        </section>
      )}
      {id ? (
        <RunEvidence workflowId={id} env={env} />
      ) : (
        <section className="panel">
          <SectionHeading
            title="运行与事件"
            subtitle="Run / Trace / Span · 来自持久化事件"
          />
          <Empty
            title="保存并运行后，在此查看事件"
            detail="订单接受与实际成交分别记录；重试沿用原 run_id。"
          />
        </section>
      )}
    </>
  );
}

function RunEvidence({
  workflowId,
  env,
}: {
  workflowId: string;
  env: DeploymentEnvironment;
}) {
  const remote = useRemote<{ data: RunEvent[] }>(
    `/api/v1/workflows/${workflowId}/events?env=${env}`,
    { data: [] },
  );
  useEffect(() => {
    const timer = setInterval(remote.refresh, 5000);
    return () => clearInterval(timer);
  }, [remote.refresh]);
  const [selected, setSelected] = useState<RunEvent | null>(null);
  const event = selected ?? remote.data.data[0];
  return (
    <section className="panel">
      <SectionHeading
        title="运行与事件"
        subtitle="每 5 秒同步持久化事件 · 保留重试与序列信息"
        action={
          <button className="text-button" onClick={remote.refresh}>
            刷新事件
          </button>
        }
      />
      {remote.error && <p className="inline-error">{remote.error}</p>}
      {event && (
        <div className="trace-evidence">
          {[
            ["run-id", event.run_id],
            ["trace-id", event.trace_id],
            ["span-id", event.span_id],
            ["parent-span-id", event.parent_span_id],
          ].map(([key, value]) => (
            <div key={key}>
              <span>{key?.replaceAll("-", "_")}</span>
              <code data-testid={`workflow-${key}`}>{value ?? "—"}</code>
            </div>
          ))}
        </div>
      )}
      {remote.data.data.length ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>序列</th>
                <th>Run / 事件</th>
                <th>状态</th>
                <th>时间</th>
                <th>重试</th>
              </tr>
            </thead>
            <tbody>
              {remote.data.data.map((event, index) => (
                <tr
                  key={`${event.run_id}-${event.seq}-${index}`}
                  onClick={() => setSelected(event)}
                  tabIndex={0}
                  onKeyDown={(key) => {
                    if (key.key === "Enter") setSelected(event);
                  }}
                >
                  <td>{event.seq ?? "—"}</td>
                  <td>
                    <span className="mono">{event.run_id}</span>
                    <small>{event.message}</small>
                  </td>
                  <td>
                    <Badge value={event.status} />
                  </td>
                  <td>
                    <DateText value={event.event_time} />
                  </td>
                  <td>{event.attempt_no ?? 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Empty
          title="暂无运行事件"
          detail="这里只展示已持久化的真实消息与执行记录。"
        />
      )}
    </section>
  );
}
