import { useScope } from "../lib/workspace";
import { useState, type FormEvent } from "react";
import { Link2, Plus, ShieldCheck, X } from "lucide-react";
import { Api, useRemote } from "../lib/api";
import type { Connection } from "../lib/types";
import {
  Badge,
  ConnectionState,
  DateText,
  Empty,
  PageHeader,
  SectionHeading,
} from "../components/shared";

export function Connections({ kind }: { kind: "datafeed" | "exchange" }) {
  const scope = useScope();
  const remote = useRemote<{ data: Connection[] }>(
    `/api/v1/${kind === "datafeed" ? "datasources" : "exchanges"}?${scope.query}`,
    { data: [] },
  );
  const [showForm, setShowForm] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const sources =
    kind === "datafeed"
      ? [
          "X",
          "TruthSocial",
          "Binance",
          "Hyperliquid",
          "FederalReserve",
          "Treasury",
        ]
      : ["Binance", "IBKR", "Polymarket"];
  const label = kind === "datafeed" ? "数据源" : "交易所";
  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setSaving(true);
    const fields = new FormData(event.currentTarget);
    try {
      await Api.entity(kind === "datafeed" ? "datasources" : "exchanges", String(fields.get("env")), {
        name: fields.get("name"),
        provider: fields.get("provider"),
        transport: fields.get("mode"),
        configuration: {endpoint: fields.get("endpoint")},
        secret_ref: fields.get("secret_ref"),
      });
      setShowForm(false);
      remote.refresh();
    } catch (error) {
      setError(error instanceof Error ? error.message : "连接保存失败");
    } finally {
      setSaving(false);
    }
  }
  return (
    <>
      <PageHeader
        eyebrow={kind === "datafeed" ? "DATA CONNECTIONS" : "EXECUTION VENUES"}
        title={label}
        description={
          kind === "datafeed"
            ? "统一管理行情与新闻来源，通过 MCP、A2A 或 API Key 接入。"
            : "在账户与环境边界内连接交易场所，以统一接口处理订单与成交。"
        }
        action={
          <button className="primary-button" onClick={() => setShowForm(true)}>
            <Plus size={16} />
            连接{label}
          </button>
        }
      />
      <ConnectionState {...remote} />
      <div className="connection-cards">
        {sources.map((provider) => {
          const connections = (remote.data.data ?? []).filter(
            (connection) =>
              connection.provider.toLowerCase() === provider.toLowerCase(),
          );
          return (
            <section className="panel provider-card" key={provider}>
              <div className="provider-head">
                <span className={`provider-logo ${provider.toLowerCase()}`}>
                  {provider === "Binance"
                    ? "◇"
                    : provider === "X"
                      ? "𝕏"
                      : provider.slice(0, 1)}
                </span>
                <Badge
                  value={
                    connections.length
                      ? `${connections.length} 个连接`
                      : "未连接"
                  }
                />
              </div>
              <h2>{provider}</h2>
              <p>
                {kind === "exchange"
                  ? "行情 / 订单 / 执行事实"
                  : /X|TruthSocial|Federal|Treasury/.test(provider)
                    ? "新闻 / 社交动态 / 政策"
                    : "行情 / 清算 / 资金费率"}
              </p>
              <button
                className="provider-action"
                onClick={() => setShowForm(true)}
              >
                <Link2 size={15} />
                管理连接
              </button>
            </section>
          );
        })}
      </div>
      <section className="panel">
        <SectionHeading
          title="已配置的连接"
          subtitle="凭据由服务端 Secret 管理，浏览器仅保存引用"
        />
        {remote.data.data?.length ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>名称 / 平台</th>
                  <th>模式</th>
                  <th>状态</th>
                  <th>最近更新</th>
                </tr>
              </thead>
              <tbody>
                {remote.data.data.map((connection) => (
                  <tr key={connection.id}>
                    <td>
                      {connection.name}
                      <small>{connection.provider}</small>
                    </td>
                    <td>{connection.transport}</td>
                    <td>
                      <Badge value="configured" />
                    </td>
                    <td>
                      <DateText value={connection.updated_at} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty
            title={`还没有已配置的${label}连接`}
            detail="连接信息经过身份与空间权限校验，密钥不会保存在本地浏览器。"
          />
        )}
      </section>
      {showForm && (
        <div className="modal-backdrop">
          <section
            role="dialog"
            aria-modal="true"
            aria-labelledby="connection-title"
            className="modal"
          >
            <div className="section-heading">
              <h2 id="connection-title">连接{label}</h2>
              <button
                className="icon-button"
                aria-label="关闭连接窗口"
                onClick={() => setShowForm(false)}
              >
                <X size={18} />
              </button>
            </div>
            <form onSubmit={save}>
              <label>
                连接名称
                <input
                  required
                  name="name"
                  autoFocus
                  placeholder="例如：市场观察数据源"
                />
              </label>
              <div className="form-grid">
                <label>
                  平台
                  <select name="provider">
                    {sources.map((source) => (
                      <option key={source}>{source}</option>
                    ))}
                  </select>
                </label>
                <label>
                  接入模式
                  <select name="mode">
                    {(kind === "datafeed"
                      ? ["MCP", "A2A", "API_KEY"]
                      : ["live", "paper", "backtest"]
                    ).map((mode) => (
                      <option key={mode}>{mode}</option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="form-grid">
                <label>
                  部署环境
                  <select name="env">
                    <option value="dev">DEV</option>
                    <option value="sit">SIT</option>
                    <option value="uat">UAT</option>
                    <option value="prod">PROD</option>
                  </select>
                </label>
              </div>
              <label>
                服务地址
                <input
                  name="endpoint"
                  type="url"
                  placeholder="https://…"
                  required
                />
              </label>
              <label>
                凭据 Secret 引用
                <input
                  name="secret_ref"
                  placeholder="secret://名称/键"
                  required
                />
              </label>
              <p className="form-note">
                <ShieldCheck size={15} />
                请填写已由运维配置的 Secret 引用。
              </p>
              {error && (
                <p className="form-error" role="alert">
                  {error}
                </p>
              )}
              <button className="primary-button" disabled={saving}>
                {saving ? "正在保存…" : "保存连接"}
              </button>
            </form>
          </section>
        </div>
      )}
    </>
  );
}
