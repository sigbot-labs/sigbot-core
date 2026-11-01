import { ArrowUpRight, Radio, RefreshCw } from "lucide-react";
import type { ReactNode } from "react";

export function PageHeader({
  eyebrow,
  title,
  description,
  action,
}: {
  eyebrow: string;
  title: string;
  description: string;
  action?: ReactNode;
}) {
  return (
    <div className="page-heading">
      <div>
        <div className="eyebrow">{eyebrow}</div>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      {action}
    </div>
  );
}
export function Empty({
  title = "等待真实数据",
  detail = "连接数据源后，这里的数据将自动更新。",
}: {
  title?: string;
  detail?: string;
}) {
  return (
    <div className="empty">
      <div className="empty-icon">
        <Radio size={22} />
      </div>
      <strong>{title}</strong>
      <p>{detail}</p>
    </div>
  );
}
export function ConnectionState({
  error,
  loading,
  refresh,
}: {
  error: string | null;
  loading: boolean;
  refresh: () => void;
}) {
  return (
    <div className={`connection-state ${error ? "warning" : ""}`} role="status">
      <span className={`status-dot ${error ? "offline" : ""}`} />
      <span>{loading ? "正在同步…" : (error ?? "已连接 · 数据已同步")}</span>
      <button className="text-button" onClick={refresh} aria-label="刷新数据">
        <RefreshCw size={14} />
        刷新
      </button>
    </div>
  );
}
export function Badge({ value }: { value: string }) {
  return (
    <span
      className={`badge ${/RUNNING|SUCCESS|connected|delivered/i.test(value) ? "green" : /FAILED|error/i.test(value) ? "red" : ""}`}
    >
      {value}
    </span>
  );
}
export function DateText({ value }: { value?: string }) {
  return (
    <>
      {value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—"}
    </>
  );
}
export function SectionHeading({
  title,
  subtitle,
  action,
}: {
  title: string;
  subtitle?: string;
  action?: ReactNode;
}) {
  return (
    <div className="section-heading">
      <div>
        <h2>{title}</h2>
        {subtitle && <p>{subtitle}</p>}
      </div>
      {action}
    </div>
  );
}
export function LinkButton({
  children,
  to,
}: {
  children: ReactNode;
  to: string;
}) {
  return (
    <a className="text-button" href={to}>
      {children}
      <ArrowUpRight size={14} />
    </a>
  );
}
