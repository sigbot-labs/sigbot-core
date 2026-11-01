import { useScope } from "../lib/workspace";
import { useState } from "react";
import { ExternalLink, Layers3, Newspaper } from "lucide-react";
import { useRemote } from "../lib/api";
import type { MarketRecord } from "../lib/types";
import {
  ConnectionState,
  DateText,
  Empty,
  PageHeader,
  SectionHeading,
} from "../components/shared";

const sources = [
  { id: "all", label: "全部" },
  { id: "binance", label: "Binance" },
  { id: "hyperliquid", label: "Hyperliquid" },
  { id: "twitter", label: "X" },
  { id: "truthsocial", label: "TruthSocial" },
  { id: "fed", label: "美联储" },
  { id: "treasury", label: "Treasury" },
];
const categories = [
  { id: "liquidation", label: "清算热力图" },
  { id: "funding", label: "借贷 / 资金费率" },
  { id: "positioning", label: "多空比 / 成交量" },
  { id: "news", label: "新闻与政策" },
];

export function Market() {
  const scope = useScope();
  const [source, setSource] = useState("all");
  const [category, setCategory] = useState("liquidation");
  const remote = useRemote<{ data: MarketRecord[] }>(
    `/api/v1/market?${scope.query}&source=${encodeURIComponent(source)}&category=${category}`,
    { data: [] },
  );
  const records = remote.data.data ?? [];
  return (
    <>
      <PageHeader
        eyebrow="MARKET INTELLIGENCE"
        title="市场行情"
        description="把行情、资金与政策放在同一个视野，保留每条信息的来源和时间。"
      />
      <ConnectionState {...remote} />
      <div className="filter-row" aria-label="行情来源">
        {sources.map((item) => (
          <button
            className={source === item.id ? "filter active" : "filter"}
            aria-pressed={source === item.id}
            onClick={() => setSource(item.id)}
            key={item.id}
          >
            {item.label}
          </button>
        ))}
      </div>
      <div className="tabs" aria-label="行情类别">
        {categories.map((item) => (
          <button
            onClick={() => setCategory(item.id)}
            aria-pressed={category === item.id}
            className={category === item.id ? "active" : ""}
            key={item.id}
          >
            {item.label}
          </button>
        ))}
      </div>
      <section className="panel">
        <SectionHeading
          title={categories.find((item) => item.id === category)?.label ?? ""}
          subtitle={
            category === "liquidation"
              ? "Binance / Hyperliquid · 价格与流动性分布"
              : "所有时间来自数据源；可得时间用于策略与回放"
          }
        />
        {records.length === 0 ? (
          <div className="market-empty">
            <div className="chart-grid" aria-hidden="true" />
            <Empty
              title="尚未接入这一类行情"
              detail="完成数据源连接后，展示实际采集的行情；历史数据可用于查询与回放。"
            />
            <a className="secondary-button" href="#sources">
              <Layers3 size={15} />
              配置数据源
            </a>
          </div>
        ) : (
          <div className="market-records">
            {records.map((record, index) => (
              <article
                className="market-record"
                key={`${record.source}-${record.source_event_id ?? index}-${record.revision ?? 0}`}
              >
                <div className="record-title">
                  <span className="source-mark">
                    {record.source.slice(0, 1)}
                  </span>
                  <div>
                    <strong>
                      {record.title ?? record.symbol ?? record.category}
                    </strong>
                    <small>
                      {record.source} · {record.category}
                    </small>
                  </div>
                  {record.value != null && <b>{record.value}</b>}
                </div>
                {record.points?.length ? (
                  <div
                    className="heatmap"
                    aria-label={`${record.source}清算热力图`}
                  >
                    {record.points.map((point, i) => (
                      <div
                        key={i}
                        title={`${point.price} · ${point.intensity}`}
                        style={{
                          background: `rgba(36,131,96,${Math.max(0.08, Math.min(1, point.intensity))})`,
                        }}
                      >
                        {point.price}
                      </div>
                    ))}
                  </div>
                ) : null}
                <div className="record-times">
                  <span>
                    事件时间 <DateText value={record.event_time} />
                  </span>
                  <span>
                    可得时间 <DateText value={record.available_time} />
                  </span>
                  {record.url && /^https:\/\//.test(record.url) && (
                    <a href={record.url} rel="noreferrer" target="_blank">
                      阅读来源
                      <ExternalLink size={12} />
                    </a>
                  )}
                </div>
              </article>
            ))}
          </div>
        )}
      </section>
      <div className="info-strip">
        <Newspaper size={18} />
        <p>
          新闻事实与模型推断分别记录。态势评估是策略的输入，交易由独立风控流程批准。
        </p>
      </div>
    </>
  );
}
