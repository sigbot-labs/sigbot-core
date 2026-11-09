import { useState, type FormEvent } from "react";
import { Boxes, Code2, Pencil, Plus, X } from "lucide-react";
import { parse, stringify } from "yaml";
import { Badge, ConnectionState, DateText, Empty, PageHeader } from "../components/shared";
import { Api, useRemote } from "../lib/api";
import type { Identity, TenantMiddleware, Worker } from "../lib/types";
import { useScope } from "../lib/workspace";

const middlewareNames = ["redis", "pgvector", "timescaledb", "emqx"] as const;
export function Workers() {
  const scope = useScope();
  const remote = useRemote<{ data: Worker[] }>(`/api/v1/clusters?${scope.query}`, { data: [] });
  const identity = useRemote<Identity | null>("/api/v1/identity", null);
  const defaults: {data: TenantMiddleware} = {data: {}};
  const [editing, setEditing] = useState<Worker | null | undefined>();
  return (
    <>
      <PageHeader
        eyebrow="TENANT DATA PLANE"
        title="租户运行资源"
        description="注册远程 Docker / Kubernetes Worker，并为九个数据面组件配置隔离的 Redis、pgvector、TimescaleDB 与 EMQX。"
        action={<button className="primary-button" onClick={() => setEditing(null)}><Plus size={16} />注册 Worker</button>}
      />
      <ConnectionState {...remote} />
      <section className="panel">
        {remote.data.data.length ? (
          <div className="table-wrap"><table><thead><tr><th>Worker</th><th>运行环境</th><th>中间件</th><th>状态</th><th>更新</th><th /></tr></thead>
            <tbody>{remote.data.data.map((worker) => <tr key={worker.id}>
              <td>{worker.name}<small>{worker.conn_url}</small></td>
              <td>{worker.provider}<small>{worker.env}</small></td>
              <td>{middlewareNames.filter((name) => worker.middleware[name]).join(" · ") || "外部托管"}</td>
              <td><Badge value={worker.status} /></td>
              <td><DateText value={worker.updated_at} /></td>
              <td><button className="icon-button" aria-label={`编辑 ${worker.name}`} onClick={() => setEditing(worker)}><Pencil size={15} /></button></td>
            </tr>)}</tbody>
          </table></div>
        ) : <Empty title="尚未注册租户 Worker" detail="身份租户来自 Authguard；这里只保存其数据面计算连接与受限中间件部署规格。" />}
      </section>
      {editing !== undefined && <WorkerEditor worker={editing} defaults={defaults.data} env={scope.env} tenantId={identity.data?.tenant_id ?? ""} close={() => setEditing(undefined)} saved={() => { setEditing(undefined); remote.refresh(); }} />}
    </>
  );
}

function WorkerEditor({ worker, defaults, env, tenantId, close, saved }: { worker: Worker | null; defaults: TenantMiddleware; env: string; tenantId: string; close: () => void; saved: () => void }) {
  const [mode, setMode] = useState<"form" | "yaml">("form");
  const [provider, setProvider] = useState(worker?.provider ?? "kubernetes");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const initial = worker ?? defaultWorker(env, defaults);
  const [document, setDocument] = useState(stringify(editable(initial)));
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); setError(null); setSaving(true);
    try {
      const payload = mode === "yaml" ? parse(document) : fromForm(new FormData(event.currentTarget), worker?.id, env, provider);
      validatePayload(payload);
      const {id, env: targetEnv, ...fields} = payload;
      await Api.entity("clusters", String(targetEnv), fields, typeof id === "string" ? id : undefined); saved();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Worker 配置无效");
    } finally { setSaving(false); }
  }
  return <div className="modal-backdrop"><section role="dialog" aria-modal="true" aria-labelledby="worker-title" className="modal wide-modal">
    <div className="section-heading"><div><h2 id="worker-title">{worker ? "编辑" : "注册"}租户 Worker</h2><p>表单与 YAML 使用相同的服务端强类型校验</p></div><button className="icon-button" aria-label="关闭" onClick={close}><X size={18} /></button></div>
    <div className="mode-switch"><button className={mode === "form" ? "active" : ""} onClick={() => setMode("form")}><Boxes size={14} />表单</button><button className={mode === "yaml" ? "active" : ""} onClick={() => setMode("yaml")}><Code2 size={14} />YAML</button></div>
    <form onSubmit={submit}>
      {mode === "yaml" ? <label>Worker YAML<textarea className="yaml-editor" value={document} onChange={(event) => setDocument(event.target.value)} spellCheck={false} /></label> : <WorkerFields worker={worker} defaults={defaults} env={env} tenantId={tenantId} provider={provider} setProvider={setProvider} />}
      {error && <p className="form-error" role="alert">{error}</p>}
      <button className="primary-button" disabled={saving}>{saving ? "正在保存…" : "保存 Worker"}</button>
    </form>
  </section></div>;
}

function WorkerFields({ worker, defaults, env, tenantId, provider, setProvider }: { worker: Worker | null; defaults: TenantMiddleware; env: string; tenantId: string; provider: string; setProvider: (value: "docker" | "kubernetes") => void }) {
  return <>
    <div className="form-grid"><label>名称<input name="name" required defaultValue={worker?.name ?? "tenant-worker"} /></label><label>类型<select name="provider" value={provider} onChange={(event) => setProvider(event.target.value as "docker" | "kubernetes")}><option value="kubernetes">Kubernetes</option><option value="docker">Docker</option></select></label></div>
    <input type="hidden" name="env" value={env} />
    <label>连接地址<input name="conn_url" required defaultValue={worker?.conn_url ?? (provider === "kubernetes" ? "https://127.0.0.1:6443" : "unix:///var/run/docker.sock")} /></label>
    {provider === "kubernetes" ? <div className="form-grid"><label>Namespace<input name="namespace" required defaultValue={String(worker?.configuration.namespace ?? "sigbot-tenant")} /></label><label>JWT Secret 引用<input name="token_secret" required defaultValue={String(worker?.auth_config.token_secret ?? "worker_kubernetes_token")} /></label><label>Runtime Secret<input name="runtime_secret" required defaultValue={String(worker?.configuration.runtime_secret ?? "sigbot-runtime")} /></label><label>Runtime ConfigMap<input name="runtime_config_map" required defaultValue={String(worker?.configuration.runtime_config_map ?? "sigbot-runtime")} /></label><label>ServiceAccount<input name="service_account" required defaultValue={String(worker?.configuration.service_account ?? "sigbot-worker")} /></label></div>
      : <div className="form-grid"><label>隔离网络<input name="network" required defaultValue={String(worker?.configuration.network ?? "sigbot-tenant")} /></label><label>宿主环境文件名<input name="environment_file" required defaultValue={String(worker?.configuration.environment_file ?? `tenant-${tenantId}-runtime`)} /></label></div>}
    <h3 className="form-subtitle">租户动态中间件</h3>
    <div className="middleware-grid">{middlewareNames.map((name) => { const current = worker?.middleware[name] ?? defaults[name]; return <fieldset key={name}><legend>{name}</legend><label>Image<input name={`${name}_image`} required defaultValue={current?.image ?? ""} /></label><div className="form-grid"><label>CPU m<input name={`${name}_cpu`} type="number" min="50" required defaultValue={current?.resources.cpu_millis ?? 500} /></label><label>Memory MB<input name={`${name}_memory`} type="number" min="64" required defaultValue={current?.resources.memory_mb ?? 512} /></label><label>Storage GB<input name={`${name}_storage`} type="number" min="1" required defaultValue={current?.resources.storage_gb ?? 8} /></label></div></fieldset>; })}</div>
  </>;
}

function fromForm(fields: FormData, id: string | undefined, env: string, provider: string) {
  const value = (name: string) => String(fields.get(name) ?? "").trim();
  const middleware = Object.fromEntries(middlewareNames.map((name) => [name, { image: value(`${name}_image`), resources: { cpu_millis: Number(value(`${name}_cpu`)), memory_mb: Number(value(`${name}_memory`)), storage_gb: Number(value(`${name}_storage`)) } }])) as TenantMiddleware;
  const docker = provider === "docker";
  return { id: id ?? null, env, workflow_id: null, name: value("name"), provider, conn_url: value("conn_url"), auth_config: docker ? { auth_type: "docker_anonymous" } : { auth_type: "kubernetes_jwt", token_secret: value("token_secret"), certificate_authority_secret: null }, configuration: docker ? { runtime_type: "docker", network: value("network"), environment_file: value("environment_file"), log_volume_prefix: "" } : { runtime_type: "kubernetes", namespace: value("namespace"), runtime_secret: value("runtime_secret"), runtime_config_map: value("runtime_config_map"), service_account: value("service_account"), secret_revision: "1", log_pvc_prefix: "", egress_cidrs: [], egress_ports: [443, 1883, 5432, 6379] }, middleware, labels: {}, priority: 100, max_concurrent_workflows: 16 };
}

function defaultWorker(env: string, defaults: TenantMiddleware): Worker {
  return { id: "", env: env as Worker["env"], workflow_id: null, name: "tenant-worker", provider: "kubernetes", conn_url: "https://127.0.0.1:6443", auth_config: { auth_type: "kubernetes_jwt", token_secret: "worker_kubernetes_token", certificate_authority_secret: null }, configuration: { runtime_type: "kubernetes", namespace: "sigbot-tenant", runtime_secret: "sigbot-runtime", runtime_config_map: "sigbot-runtime", service_account: "sigbot-worker", secret_revision: "1", log_pvc_prefix: "", egress_cidrs: [], egress_ports: [443, 1883, 5432, 6379] }, middleware: defaults, labels: {}, status: "offline", priority: 100, max_concurrent_workflows: 16, updated_at: "" };
}

function editable(worker: Worker) {
  return {
    id: worker.id || null,
    env: worker.env,
    workflow_id: worker.workflow_id ?? null,
    name: worker.name,
    provider: worker.provider,
    conn_url: worker.conn_url,
    auth_config: worker.auth_config,
    configuration: worker.configuration,
    middleware: worker.middleware,
    labels: worker.labels,
    priority: worker.priority,
    max_concurrent_workflows: worker.max_concurrent_workflows,
  };
}

function validatePayload(value: unknown): asserts value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("YAML 顶层必须是 Worker 对象");
  const object = value as Record<string, unknown>;
  const allowed = new Set(["id", "env", "workflow_id", "name", "provider", "conn_url", "auth_config", "configuration", "middleware", "labels", "priority", "max_concurrent_workflows"]);
  const unknown = Object.keys(object).filter((key) => !allowed.has(key));
  if (unknown.length) throw new Error(`不允许的字段：${unknown.join(", ")}`);
}
