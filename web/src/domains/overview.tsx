import { useScope } from "../lib/workspace";
import {
  Activity,
  ArrowUpRight,
  GitBranch,
  Layers3,
  Wallet,
} from "lucide-react";
import { useRemote } from "../lib/api";
import type { Overview as OverviewData } from "../lib/types";
import {
  Badge,
  ConnectionState,
  DateText,
  Empty,
  LinkButton,
  PageHeader,
  SectionHeading,
} from "../components/shared";

export function Overview() {
  const scope = useScope();
  const remote = useRemote<OverviewData>(`/api/v1/overview?${scope.query}`, {});
  const metrics = [
    {
      label: "组合净值",
      value: remote.data.equity,
      unit: "USDT",
      icon: Wallet,
    },
    {
      label: "已实现收益",
      value: remote.data.realized_pnl,
      unit: "USDT",
      icon: ArrowUpRight,
    },
    {
      label: "运行中的 Workflow",
      value: remote.data.active_workflows,
      unit: "/ " + (remote.data.workflows_total ?? "—"),
      icon: GitBranch,
    },
    {
      label: "真实执行笔数",
      value: remote.data.executions_count,
      unit: "笔",
      icon: Activity,
    },
  ];
  const series = remote.data.equity_series ?? [];
  const values = series.map((point) => point.value);
  const low = Math.min(...values),
    high = Math.max(...values);
  const points = series
    .map(
      (point, index) =>
        `${30 + (index / Math.max(series.length - 1, 1)) * 740},${190 - ((point.value - low) / Math.max(high - low, 1)) * 150}`,
    )
    .join(" ");
  return (
    <>
      <PageHeader
        eyebrow="YOUR QUANT WORKSPACE"
        title="Overview"
        description="从市场洞察到交易执行，让每一次决策有迹可循。"
        action={
          <a className="primary-button" href="#workflows">
            <GitBranch size={16} />
            管理 Workflow
          </a>
        }
      />
      <ConnectionState {...remote} />
      <div className="metrics">
        {metrics.map(({ label, value, unit, icon: Icon }) => (
          <article className="metric" key={label}>
            <div>
              <span>{label}</span>
              <Icon size={18} />
            </div>
            <strong>
              {value ?? "—"}
              <small>{unit}</small>
            </strong>
            <p>{value == null ? "等待组合与运行数据" : "来自账本与执行记录"}</p>
          </article>
        ))}
      </div>
      <div className="overview-grid">
        <section className="panel">
          <SectionHeading
            title="组合净值"
            subtitle="按组合版本记录 · 已实现与未实现收益"
            action={<span className="pill">近期表现</span>}
          />
          {series.length > 1 ? (
            <svg
              className="equity-chart"
              viewBox="0 0 800 230"
              role="img"
              aria-label="组合净值趋势"
            >
              <path
                d="M30 40H770M30 90H770M30 140H770M30 190H770"
                stroke="#e7eae5"
                fill="none"
              />
              <polyline
                points={points}
                fill="none"
                stroke="#248360"
                strokeWidth="3"
              />
            </svg>
          ) : (
            <Empty
              title="你的第一条净值曲线，从一次真实执行开始"
              detail="连接交易账户并运行 Workflow，组合快照会在此持续更新。"
            />
          )}
          <div className="panel-foot">
            <span>最近更新</span>
            <DateText value={remote.data.updated_at} />
          </div>
        </section>
        <section className="panel getting-started">
          <div className="eyebrow">BUILD WITH CONFIDENCE</div>
          <Layers3 size={34} />
          <h2>
            把想法变成
            <br />
            可追踪的策略
          </h2>
          <p>连接市场数据，评估态势，以明确的风控边界执行你的策略。</p>
          <a href="#sources">
            01 <span>连接一个数据源</span>
            <ArrowUpRight size={16} />
          </a>
          <a href="#workflows">
            02 <span>编排你的 Workflow</span>
            <ArrowUpRight size={16} />
          </a>
          <a href="#exchanges">
            03 <span>选择交易环境</span>
            <ArrowUpRight size={16} />
          </a>
        </section>
      </div>
      <section className="panel">
        <SectionHeading
          title="近期 Workflow 运行"
          subtitle="从一次运行，追溯完整决策链路"
          action={<LinkButton to="#workflows">查看全部</LinkButton>}
        />
        {(remote.data.recent_runs ?? []).length ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Workflow / Run</th>
                  <th>状态</th>
                  <th>执行时间</th>
                  <th>重试</th>
                </tr>
              </thead>
              <tbody>
                {remote.data.recent_runs!.map((run) => (
                  <tr key={run.run_id}>
                    <td>
                      <a
                        href={`#workflows?run=${encodeURIComponent(run.run_id)}`}
                      >
                        {run.workflow_name ?? run.workflow_id ?? "Workflow"}
                      </a>
                      <small className="mono">{run.run_id}</small>
                    </td>
                    <td>
                      <Badge value={run.status} />
                    </td>
                    <td>
                      <DateText value={run.event_time} />
                    </td>
                    <td>{run.attempt_no ?? 0}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty
            title="还没有运行记录"
            detail="已完成、进行中和需要关注的运行，会统一显示在这里。"
          />
        )}
      </section>
    </>
  );
}
