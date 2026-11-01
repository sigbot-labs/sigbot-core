import { useEffect, useState } from "react";
import {
  Activity,
  ArrowUpRight,
  Bell,
  Blocks,
  ChevronDown,
  CircleHelp,
  Database,
  GitBranch,
  LayoutDashboard,
  LogOut,
  Menu,
  ShieldCheck,
  Wallet,
  X,
} from "lucide-react";
import { Api, useRemote } from "./lib/api";
import { WorkspaceContext, useScope } from "./lib/workspace";
import type {
  DeploymentEnvironment,
  Identity,
  Notification,
  PageName,
} from "./lib/types";
import { Overview } from "./domains/overview";
import { Market } from "./domains/market";
import { Workflows } from "./domains/workflows";
import { Connections } from "./domains/connections";
import { Workers } from "./domains/workers";
import { Badge, DateText } from "./components/shared";

const navigation = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "market", label: "市场行情", icon: Activity },
  { id: "workflows", label: "Workflow", icon: GitBranch },
  { id: "sources", label: "数据源", icon: Database },
  { id: "exchanges", label: "交易所", icon: Wallet },
] as const;
export default function App() {
  const [page, setPage] = useState<PageName>(
    (location.hash.slice(1).split("?")[0] || "overview") as PageName,
  );
  const [env, setEnv] = useState<DeploymentEnvironment>("dev");
  const [mobileNav, setMobileNav] = useState(false);
  useEffect(() => {
    const navigate = () => {
      const next = location.hash.slice(1).split("?")[0];
      setPage(
        navigation.some((item) => item.id === next) || next === "workers"
          ? (next as PageName)
          : "overview",
      );
      setMobileNav(false);
    };
    window.addEventListener("hashchange", navigate);
    return () => window.removeEventListener("hashchange", navigate);
  }, []);
  const current = navigation.find((item) => item.id === page) ?? { id: "workers", label: "租户运行资源", icon: Blocks };
  return (
    <WorkspaceContext.Provider value={{ env, setEnv }}>
      <div className="app-shell">
        <aside className={`sidebar ${mobileNav ? "open" : ""}`}>
          <a className="brand" href="#overview" aria-label="Sigbot 首页">
            <span className="brand-symbol">
              <Blocks size={25} />
            </span>
            <span>
              sigbot<span className="brand-period">.</span>
            </span>
          </a>
          <div className="workspace-switch">
            <span className="workspace-avatar">S</span>
            <div>
              <strong>量化工作台</strong>
              <small>Research · Trade · Observe</small>
            </div>
            <ChevronDown size={14} />
          </div>
          <div className="nav-caption">WORKSPACE</div>
          <nav aria-label="主导航">
            {navigation.map(({ id, label, icon: Icon }) => (
              <a
                className={page === id ? "active" : ""}
                href={`#${id}`}
                aria-current={page === id ? "page" : undefined}
                key={id}
              >
                <Icon size={18} />
                <span>{label}</span>
                {page === id && <span className="nav-active-dot" />}
              </a>
            ))}
          </nav>
          <div className="sidebar-bottom">
            <div className="boundary-note">
              <ShieldCheck size={18} />
              <strong>明确边界，稳健执行</strong>
              <p>
                身份、环境与账户隔离
                <br />
                贯穿每次策略运行
              </p>
            </div>
            <a
              className="help-link"
              href="https://github.com/wl4g/sigbot"
              rel="noreferrer"
              target="_blank"
            >
              <CircleHelp size={16} />
              文档与帮助
              <ArrowUpRight size={13} />
            </a>
            <div className="sidebar-version">
              <span className="status-dot" />
              SIGBOT CORE <span>v2</span>
            </div>
          </div>
        </aside>
        <div className="workspace-main">
          <header className="topbar">
            <button
              className="icon-button mobile-menu"
              aria-label="展开导航"
              onClick={() => setMobileNav((value) => !value)}
            >
              <Menu size={20} />
            </button>
            <div className="breadcrumb">
              Workspace <span>/</span>
              <strong>{current.label}</strong>
            </div>
            <div className="topbar-right">
              <label className="scope-control">
                部署环境
                <select
                  aria-label="当前部署环境"
                  value={env}
                  onChange={(event) =>
                    setEnv(event.target.value as DeploymentEnvironment)
                  }
                >
                  <option value="dev">DEV</option>
                  <option value="sit">SIT</option>
                  <option value="uat">UAT</option>
                  <option value="prod">PROD</option>
                </select>
              </label>
              <HeaderMenus />
            </div>
          </header>
          <main id="main-content" key={page}>
            {page === "overview" ? (
              <Overview />
            ) : page === "market" ? (
              <Market />
            ) : page === "workflows" ? (
              <Workflows />
            ) : page === "sources" ? (
              <Connections kind="datafeed" />
            ) : page === "exchanges" ? (
              <Connections kind="exchange" />
            ) : (
              <Workers />
            )}
            <footer className="workspace-footer">
              <span>Designed for thoughtful decisions.</span>
              <span>可追溯 · 可重放 · 可验证</span>
            </footer>
          </main>
        </div>
      </div>
    </WorkspaceContext.Provider>
  );
}

function HeaderMenus() {
  const scope = useScope();
  const identity = useRemote<Identity | null>("/api/v1/identity", null);
  const notifications = useRemote<{ data: Notification[] }>(
    `/api/v1/notifications?${scope.query}`,
    { data: [] },
  );
  const [menu, setMenu] = useState<"notifications" | "profile" | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMenu(null);
    };
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, []);
  async function read(notification: Notification) {
    try {
      await Api.save(`/api/v1/notifications/read?${scope.query}`, {
        run_id: notification.run_id,
        seq: notification.seq,
        producer_node_id: notification.producer_node_id,
      });
      notifications.refresh();
    } catch (error) {
      setError(error instanceof Error ? error.message : "更新失败");
    }
  }
  async function logout() {
    try {
      const response = await fetch("/auth/logout", {
        method: "POST",
        credentials: "same-origin",
      });
      if (!response.ok) throw new Error("退出登录失败，请重试");
      location.assign("/auth/login");
    } catch (error) {
      setError(error instanceof Error ? error.message : "退出登录失败");
    }
  }
  return (
    <div className="header-menus">
      <button
        className={`icon-button bell ${menu === "notifications" ? "selected" : ""}`}
        aria-label="通知"
        aria-expanded={menu === "notifications"}
        onClick={() => {
          setMenu(menu === "notifications" ? null : "notifications");
          notifications.refresh();
        }}
      >
        <Bell size={19} />
        {notifications.data.data.some((item) => !item.read) && (
          <span className="notification-dot" />
        )}
      </button>
      <button
        className="profile-button"
        aria-label="个人菜单"
        aria-expanded={menu === "profile"}
        onClick={() => setMenu(menu === "profile" ? null : "profile")}
      >
        <span>
          {identity.data?.principal_id?.slice(0, 1).toUpperCase() ?? "S"}
        </span>
        <ChevronDown size={13} />
      </button>
      {menu === "notifications" && (
        <section
          role="dialog"
          aria-label="通知中心"
          className="header-popover notifications"
        >
          <div className="section-heading">
            <div>
              <h2>通知中心</h2>
              <p>开平仓、清算与运行状态</p>
            </div>
            <button
              className="icon-button"
              aria-label="关闭通知"
              onClick={() => setMenu(null)}
            >
              <X size={16} />
            </button>
          </div>
          {notifications.error ? (
            <p className="inline-error">{notifications.error}</p>
          ) : notifications.data.data.length ? (
            notifications.data.data.map((item) => (
              <article
                className="notification-item"
                key={`${item.run_id}-${item.producer_node_id}-${item.seq}-${item.channel}`}
              >
                <div>
                  <strong>{item.title}</strong>
                  <Badge value={item.status} />
                </div>
                <p>{item.body}</p>
                <small>
                  <DateText value={item.event_time} />
                </small>
                {!item.read && (
                  <button className="text-button" onClick={() => read(item)}>
                    标为已读
                  </button>
                )}
              </article>
            ))
          ) : (
            <div className="notification-empty">
              <Bell size={26} />
              <strong>暂无通知</strong>
              <p>新的执行与风险事件会显示在这里。</p>
            </div>
          )}
        </section>
      )}
      {menu === "profile" && (
        <section
          className="header-popover profile-menu"
          role="menu"
          aria-label="账户菜单"
        >
          <div className="profile-summary">
            <strong>{identity.data?.principal_id ?? "尚未登录"}</strong>
            <small>
              {identity.data
                ? `租户 ${identity.data.tenant_id}`
                : "通过 Authguard 访问工作台"}
            </small>
          </div>
          <a role="menuitem" href="/auth/account/security">
            <ShieldCheck size={16} />
            账户与安全
          </a>
          <a role="menuitem" href="#workers" onClick={() => setMenu(null)}>
            <Blocks size={16} />
            租户运行资源
          </a>
          {identity.data ? (
            <button role="menuitem" onClick={logout}>
              <LogOut size={16} />
              退出登录
            </button>
          ) : (
            <a role="menuitem" href="/auth/login">
              <LogOut size={16} />
              登录工作台
            </a>
          )}
          {error && <p className="form-error">{error}</p>}
        </section>
      )}
    </div>
  );
}
